import { useEffect, useRef, useState } from "react";

import { Icon } from "@/components/ui/icon";
import { formatTokens } from "@/lib/format";
import { streamRun, type LiveEvent } from "@/lib/live";
import { cn } from "@/lib/utils";

/** Cap the retained feed so a long run can't grow the DOM without bound. */
const MAX_FEED = 300;
const RECONNECT_DELAY_MS = 2000;

type FeedItem = Exclude<LiveEvent, { kind: "tokens" | "cursor" | "end" }>;
type TokenTick = Extract<LiveEvent, { kind: "tokens" }>;
type FeedStatus = "connecting" | "live" | "ended" | "error";

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
 * Subscribe to a run's live event stream. Reconnects from the last byte offset
 * if the connection drops before the run finishes, and stops cleanly on the
 * server's `end` frame. Token ticks are folded into a single running total
 * rather than appended as feed lines.
 */
function useLiveFeed(runId: string, enabled: boolean) {
  const [items, setItems] = useState<FeedItem[]>([]);
  const [tokens, setTokens] = useState<TokenTick | null>(null);
  const [status, setStatus] = useState<FeedStatus>("connecting");
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    if (!enabled || !runId) return;
    let cancelled = false;
    const controller = new AbortController();
    let offset = 0;
    setItems([]);
    setTokens(null);
    setStatus("connecting");

    void (async () => {
      while (!cancelled) {
        try {
          setStatus("live");
          const result = await streamRun(runId, {
            offset,
            signal: controller.signal,
            onCursor: (o) => {
              offset = o;
            },
            onEvent: (event) => {
              if (event.kind === "tokens") {
                setTokens(event);
              } else if (
                event.kind !== "cursor" &&
                event.kind !== "end"
              ) {
                setItems((prev) => [...prev, event].slice(-MAX_FEED));
              }
            },
          });
          offset = result.offset;
          if (result.ended) {
            if (!cancelled) setStatus("ended");
            return;
          }
        } catch {
          if (cancelled) return;
          setStatus("error");
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
  }, [runId, enabled, attempt]);

  return { items, tokens, status, reconnect: () => setAttempt((n) => n + 1) };
}

function EventRow({ event }: { event: FeedItem }) {
  if (event.kind === "message") {
    return (
      <div className="flex gap-2 py-1">
        <span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-blue-500" />
        <p className="whitespace-pre-wrap break-words text-sm leading-relaxed text-foreground">
          {event.text}
        </p>
      </div>
    );
  }
  if (event.kind === "file_edit") {
    return (
      <div className="flex gap-2 py-1">
        <span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-violet-500" />
        <p className="break-words text-sm leading-relaxed">
          <span className="font-medium text-violet-600 dark:text-violet-400">
            edited
          </span>{" "}
          <span className="font-mono text-xs text-muted-foreground">
            {event.files.length ? event.files.join(", ") : event.tool ?? "file"}
          </span>
        </p>
      </div>
    );
  }
  // tool_call
  return (
    <div className="flex gap-2 py-1">
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
    </div>
  );
}

const STATUS_LABEL: Record<FeedStatus, string> = {
  connecting: "connecting",
  live: "live",
  ended: "run finished",
  error: "reconnecting",
};

/** Live, parsed view of a running agent — messages, tool calls, file edits and
 *  a running token total, tailed from the run log. Renders nothing until the
 *  first event arrives. Scrolls with new output; readable on mobile. */
export function LiveFeed({ runId, active }: { runId: string; active: boolean }) {
  const { items, tokens, status, reconnect } = useLiveFeed(runId, active);
  const scrollRef = useRef<HTMLDivElement>(null);
  const pinnedRef = useRef(true);

  useEffect(() => {
    const el = scrollRef.current;
    if (el && pinnedRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [items]);

  function onScroll() {
    const el = scrollRef.current;
    if (!el) return;
    pinnedRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  }

  const dot =
    status === "live"
      ? "bg-blue-500"
      : status === "ended"
        ? "bg-green-500"
        : "bg-amber-500";

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
          {STATUS_LABEL[status]}
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
        onScroll={onScroll}
        className="max-h-[420px] overflow-y-auto overscroll-contain rounded-md border border-border bg-secondary/20 px-3 py-2"
      >
        {items.length === 0 ? (
          <p className="py-6 text-center text-sm text-muted-foreground">
            {status === "ended" ? "No live output." : "Waiting for output…"}
          </p>
        ) : (
          <div className="divide-y divide-border/40">
            {items.map((event, i) => (
              <EventRow key={i} event={event} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
