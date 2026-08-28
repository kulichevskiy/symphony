import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { Icon } from "@/components/ui/icon";
import { formatEventTime, formatTokens, localDay } from "@/lib/format";
import {
  fetchRunEvents,
  foldTokenTick,
  streamRun,
  type FeedEvent,
  type TokenTick,
} from "@/lib/live";
import { cn } from "@/lib/utils";

const RECONNECT_DELAY_MS = 2000;
// Bounds how many live-arrived events `tailItems` holds before the oldest
// overflow into `history`. Matches the backend's page size so a chatty,
// long-open run never grows the tail without limit.
const MAX_TAIL_ITEMS = 100;

type FeedStatus = "connecting" | "live" | "ended" | "error";

/** A feed event plus a render key that stays stable as newer events arrive
 *  above it. History events key off their `seq` (stable per run); tail events
 *  get a local counter, since the stream carries no sequence. */
type FeedEntry = { key: string; event: FeedEvent };

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const id = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(id);
      resolve();
    });
  });
}

/**
 * A run's activity feed, newest first.
 *
 * Opens on one bounded page of the run's newest visible events (the history
 * endpoint pages backwards by 100), appends older pages on demand, and — in
 * `live` mode — tails the log from the byte offset that page was read at,
 * inserting fresh events at the top. Because history is paged rather than
 * replayed from byte 0, opening a long run no longer builds its whole DOM up
 * front. Live arrivals past `MAX_TAIL_ITEMS` fold into `history` instead of
 * being dropped, so a run left open for a long, chatty stretch keeps a
 * bounded tail without losing any events or opening a gap.
 *
 * In non-live mode (a finished run) the tail drains the log once and never
 * reconnects. Token ticks fold into a single running total instead of feed
 * lines.
 */
function useActivityFeed(runId: string, enabled: boolean, live: boolean) {
  const [history, setHistory] = useState<FeedEntry[]>([]);
  const [tailItems, setTailItems] = useState<FeedEntry[]>([]);
  const [nextBefore, setNextBefore] = useState<number | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [tokens, setTokens] = useState<TokenTick | null>(null);
  const [status, setStatus] = useState<FeedStatus>("connecting");
  const [attempt, setAttempt] = useState(0);
  // Where the tail resumes from. Set once the first page lands (so streamed
  // events start exactly where that page ended) and advanced by `cursor`
  // frames, so a reconnect or a live→finished flip never re-reads a line.
  const [tailFrom, setTailFrom] = useState<{ runId: string; offset: number } | null>(null);
  const streamState = useRef({ runId: "", offset: 0 });
  const localKey = useRef(0);
  // Authoritative copy of `tailItems`, kept in sync so the tail-bounding
  // check below has the current length without waiting on a state update.
  const tailRef = useRef<FeedEntry[]>([]);
  // Owns the lifetime of `loadMore`'s in-flight request: recreated whenever
  // `runId` changes, aborting whatever page fetch was still pending for the
  // previous run.
  const loadMoreControllerRef = useRef<AbortController | null>(null);
  useEffect(() => {
    const controller = new AbortController();
    loadMoreControllerRef.current = controller;
    return () => controller.abort();
  }, [runId]);

  const entries = useCallback((events: FeedEvent[]): FeedEntry[] => {
    return events.map((event) => ({
      key: event.seq !== undefined ? `seq-${event.seq}` : `local-${localKey.current++}`,
      event,
    }));
  }, []);

  // First page per run. `attempt` is a dep so `retry` also recovers a failed
  // page load; the guard keeps a successful one from being re-fetched (and its
  // loaded older pages discarded) when the run finishes or retry fires.
  useEffect(() => {
    if (!enabled || !runId) return;
    if (streamState.current.runId === runId) return;
    let cancelled = false;
    const controller = new AbortController();
    setHistory([]);
    setTailItems([]);
    tailRef.current = [];
    setTokens(null);
    setNextBefore(null);
    setStatus("connecting");

    void (async () => {
      try {
        const page = await fetchRunEvents(runId, { signal: controller.signal });
        if (cancelled) return;
        setHistory(entries(page.events));
        setNextBefore(page.nextBefore);
        // Usage the skipped prefix accounts for; live ticks fold onto it.
        if (page.tokens) setTokens(page.tokens);
        streamState.current = { runId, offset: page.offset };
        setTailFrom({ runId, offset: page.offset });
      } catch {
        if (!cancelled) setStatus("error");
      }
    })();

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [runId, enabled, attempt, entries]);

  useEffect(() => {
    if (!enabled || !runId || tailFrom === null || tailFrom.runId !== runId) return;
    let cancelled = false;
    const controller = new AbortController();
    let offset = streamState.current.offset;
    setStatus("connecting");

    void (async () => {
      while (!cancelled) {
        try {
          setStatus("live");
          const result = await streamRun(runId, {
            offset,
            signal: controller.signal,
            onCursor: (o) => {
              if (cancelled) return;
              offset = o;
              streamState.current.offset = o;
            },
            onEvent: (event) => {
              if (cancelled) return;
              if (event.kind === "tokens") {
                setTokens((prev) => foldTokenTick(prev, event));
              } else if (event.kind !== "cursor" && event.kind !== "end") {
                const next = [
                  { key: `local-${localKey.current++}`, event },
                  ...tailRef.current,
                ];
                if (next.length > MAX_TAIL_ITEMS) {
                  const overflow = next.splice(MAX_TAIL_ITEMS);
                  setHistory((prev) => [...overflow, ...prev]);
                }
                tailRef.current = next;
                setTailItems(next);
              }
            },
          });
          offset = result.offset;
          streamState.current.offset = result.offset;
          if (result.ended) {
            if (!cancelled) setStatus("ended");
            return;
          }
          if (!live) {
            // Drain returned without an `end` frame — a dropped connection
            // or proxy/server restart cut it short. Surface as retryable
            // rather than claiming the final log is complete.
            if (!cancelled) setStatus("error");
            return;
          }
        } catch {
          if (cancelled) return;
          setStatus("error");
          // Non-live: surface the error but never retry — a past run's log
          // is drained in one pass (the retry button can re-trigger it).
          if (!live) return;
        }
        if (cancelled) return;
        // Dropped or errored while the run is still live — resume from offset.
        await sleep(RECONNECT_DELAY_MS, controller.signal);
      }
    })();

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [runId, enabled, live, attempt, tailFrom]);

  const loadMore = useCallback(async () => {
    if (nextBefore === null || loadingMore) return;
    setLoadingMore(true);
    try {
      const page = await fetchRunEvents(runId, {
        before: nextBefore,
        signal: loadMoreControllerRef.current?.signal,
      });
      setHistory((prev) => [...prev, ...entries(page.events)]);
      setNextBefore(page.nextBefore);
    } catch {
      // Keep the action visible so the operator can try the page again.
    } finally {
      setLoadingMore(false);
    }
  }, [runId, nextBefore, loadingMore, entries]);

  return {
    // `tailItems` is exposed separately (not just folded into `items`) so a
    // caller can anchor scroll-restoration to top-prepends alone, without
    // reacting to `history` growing from `loadMore`'s bottom appends.
    tailItems,
    items: [...tailItems, ...history],
    tokens,
    status,
    hasMore: nextBefore !== null,
    loadingMore,
    loadMore,
    reconnect: () => setAttempt((n) => n + 1),
  };
}

function EventBody({ event }: { event: FeedEvent }) {
  if (event.kind === "message") {
    return (
      <>
        <span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-blue-500" />
        <p className="whitespace-pre-wrap break-words text-sm leading-relaxed text-foreground">
          {event.text}
        </p>
      </>
    );
  }
  if (event.kind === "file_edit") {
    return (
      <>
        <span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-violet-500" />
        <p className="break-words text-sm leading-relaxed">
          <span className="font-medium text-violet-600 dark:text-violet-400">
            edited
          </span>{" "}
          <span className="font-mono text-xs text-muted-foreground">
            {event.files.length ? event.files.join(", ") : event.tool ?? "file"}
          </span>
        </p>
      </>
    );
  }
  // tool_call
  return (
    <>
      <span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-amber-500" />
      <p className="break-words text-sm leading-relaxed">
        <span className="font-medium text-amber-600 dark:text-amber-400">
          {event.tool}
        </span>
        {event.detail ? (
          <span className="ml-1.5 font-mono text-xs text-muted-foreground">
            {event.detail}
          </span>
        ) : null}
      </p>
    </>
  );
}

function EventRow({
  event,
  withDate,
  rowRef,
}: {
  event: FeedEvent;
  withDate: boolean;
  rowRef?: (el: HTMLDivElement | null) => void;
}) {
  return (
    <div ref={rowRef} className="flex gap-2 py-1">
      <span className="mt-0.5 shrink-0 font-mono text-[11px] tabular-nums text-muted-foreground">
        {formatEventTime(event.ts, withDate)}
      </span>
      <EventBody event={event} />
    </div>
  );
}

const STATUS_LABEL: Record<FeedStatus, string> = {
  connecting: "connecting",
  live: "live",
  ended: "run finished",
  error: "reconnecting",
};

/** Parsed view of an agent run — messages, tool calls, file edits and a running
 *  token total, newest first. In `live` mode (default) it follows a running run
 *  and reconnects on drops; with `live={false}` it drains a finished run's log
 *  once (no reconnect loop) as a final log. `label` overrides the header text
 *  (e.g. "final log — implement, failed"). Older history loads on demand, so
 *  new output never scrolls the operator away from what they were reading. */
export function LiveFeed({
  runId,
  active,
  live = true,
  label,
}: {
  runId: string;
  active: boolean;
  live?: boolean;
  label?: ReactNode;
}) {
  const { items, tailItems, tokens, status, hasMore, loadingMore, loadMore, reconnect } =
    useActivityFeed(runId, active, live);

  // New tail events are prepended above whatever the operator is reading, so
  // restore their scroll offset after each insertion — relying on implicit
  // browser scroll anchoring here would break on browsers that don't
  // implement it (e.g. Safari); native anchoring is disabled on the
  // scrollable container below (`[overflow-anchor:none]`) so this manual
  // compensation is the only mechanism in play. Only `tailItems` (top
  // inserts) trigger this; `loadMore`'s bottom appends need no compensation
  // since they don't shift content already in view.
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const rowRefs = useRef(new Map<string, HTMLDivElement>());
  const setRowRef = useCallback(
    (key: string) => (el: HTMLDivElement | null) => {
      if (el) rowRefs.current.set(key, el);
      else rowRefs.current.delete(key);
    },
    [],
  );
  // Tracks the row that was first before the latest commit, so the
  // compensating scroll is derived from that single row's `offsetTop` shift
  // — not the container's whole `scrollHeight` delta, which would also pick
  // up an unrelated `loadMore` bottom append landing in the same commit.
  const anchorKeyRef = useRef<string | null>(null);
  const anchorOffsetRef = useRef<number | null>(null);
  const prevTailRef = useRef<typeof tailItems>(tailItems);
  useLayoutEffect(() => {
    anchorKeyRef.current = null;
    anchorOffsetRef.current = null;
    prevTailRef.current = tailItems;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId]);
  // Runs after every render (not just tail insertions), since `history`
  // growing from a first page load or `loadMore` also moves the anchor row
  // and must refresh the baseline — otherwise the next tail insertion
  // compensates against a stale position and overshoots. Only an actual
  // top-prepend (a new `tailItems` identity) applies the compensating
  // scroll; `loadMore`'s bottom appends must not.
  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (el === null) return;
    const prepended = prevTailRef.current !== tailItems;
    if (
      prepended &&
      anchorKeyRef.current !== null &&
      anchorOffsetRef.current !== null &&
      el.scrollTop > 0
    ) {
      const anchorEl = rowRefs.current.get(anchorKeyRef.current);
      if (anchorEl) {
        el.scrollTop += anchorEl.offsetTop - anchorOffsetRef.current;
      }
    }
    const firstKey = items[0]?.key ?? null;
    const firstEl = firstKey !== null ? (rowRefs.current.get(firstKey) ?? null) : null;
    anchorKeyRef.current = firstKey;
    anchorOffsetRef.current = firstEl ? firstEl.offsetTop : null;
    prevTailRef.current = tailItems;
  });

  const dot =
    status === "live"
      ? "bg-blue-500"
      : status === "ended"
        ? "bg-green-500"
        : "bg-amber-500";

  // Newest first, so a row shows its date only where the day changes going
  // down the feed. Rows without a receipt time never open a day.
  let lastDay: string | null = null;
  const rows = items.map(({ key, event }) => {
    const day = localDay(event.ts);
    const withDate = day !== null && lastDay !== null && day !== lastDay;
    if (day !== null) lastDay = day;
    return { key, event, withDate };
  });

  return (
    <div>
      <div className="mb-2 flex flex-wrap items-center gap-2 text-xs">
        <span className="inline-flex items-center gap-1.5 font-medium text-muted-foreground">
          <span
            className={cn(
              "h-2 w-2 rounded-full",
              dot,
              status === "live" && "animate-pulse",
            )}
          />
          {label ?? STATUS_LABEL[status]}
        </span>
        {tokens ? (
          <span className="font-mono text-muted-foreground">
            · in {formatTokens(tokens.input_tokens)} · out{" "}
            {formatTokens(tokens.output_tokens)}
          </span>
        ) : null}
        {status === "error" ? (
          <button
            type="button"
            onClick={reconnect}
            className="ml-auto inline-flex items-center gap-1 text-muted-foreground hover:text-foreground"
          >
            <Icon name="rotate" size={12} /> retry
          </button>
        ) : null}
      </div>
      <div
        ref={scrollRef}
        className="relative max-h-[420px] overflow-y-auto overscroll-contain [overflow-anchor:none] rounded-md border border-border bg-secondary/20 px-3 py-2"
      >
        {rows.length === 0 ? (
          <p className="py-6 text-center text-sm text-muted-foreground">
            {status === "ended"
              ? live
                ? "No live output."
                : "No output recorded for this run."
              : "Waiting for output…"}
          </p>
        ) : (
          <div className="divide-y divide-border/40">
            {rows.map(({ key, event, withDate }) => (
              <EventRow key={key} event={event} withDate={withDate} rowRef={setRowRef(key)} />
            ))}
          </div>
        )}
        {hasMore ? (
          <div className="pt-2 text-center">
            <button
              type="button"
              onClick={() => void loadMore()}
              disabled={loadingMore}
              className="text-xs font-medium text-muted-foreground hover:text-foreground disabled:opacity-50"
            >
              {loadingMore ? "Загрузка…" : "Загрузить ещё"}
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
}
