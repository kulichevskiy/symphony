import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { authHeaders } from "@/lib/auth";
import { cn } from "@/lib/utils";

export type TimelineEvent = {
  ts: string;
  kind: string;
  payload: Record<string, unknown>;
};

// Default page size; must match the backend `limit` default.
export const TIMELINE_PAGE_LIMIT = 200;

type TimelineVariant = "line" | "table";
type SortDirection = "asc" | "desc";

const TIMELINE_KINDS = [
  "run_started",
  "run_ended",
  "pr_opened",
  "pr_merged",
  "comment_seen",
  "activity_comment_posted",
  "review_state_changed",
  "operator_wait_started",
  "operator_wait_ended",
  "external_observed",
  "external_cleared",
  "external_state_change",
] as const;

const KIND_LABELS: Record<string, string> = {
  run_started: "run started",
  run_ended: "run ended",
  pr_opened: "PR opened",
  pr_merged: "PR merged",
  comment_seen: "comment seen",
  activity_comment_posted: "activity comment",
  review_state_changed: "review state",
  operator_wait_started: "operator wait started",
  operator_wait_ended: "operator wait ended",
  external_observed: "external observed",
  external_cleared: "external cleared",
  external_state_change: "external state",
};

const KIND_COLORS: Record<string, string> = {
  run_started: "bg-emerald-500",
  run_ended: "bg-slate-500",
  pr_opened: "bg-blue-500",
  pr_merged: "bg-violet-500",
  comment_seen: "bg-amber-500",
  activity_comment_posted: "bg-cyan-500",
  review_state_changed: "bg-fuchsia-500",
  operator_wait_started: "bg-orange-500",
  operator_wait_ended: "bg-lime-600",
  external_observed: "bg-sky-600",
  external_cleared: "bg-teal-600",
  external_state_change: "bg-indigo-600",
};

export function buildTimelineUrl(id: string, before?: string): string {
  const params = new URLSearchParams({ limit: String(TIMELINE_PAGE_LIMIT) });
  if (before) {
    params.set("before", before);
  }
  return `/api/issues/${encodeURIComponent(id)}/timeline?${params.toString()}`;
}

// Events come back oldest-first, so the oldest loaded ts (the paging cursor) is
// at index 0 and older pages prepend ahead of what we already have.
export function oldestLoadedTs(events: TimelineEvent[]): string | undefined {
  return events[0]?.ts;
}

// A full page (>= limit; the backend may exceed it to keep a tie group intact)
// means older events may still exist.
export function hasEarlierEvents(pageLength: number): boolean {
  return pageLength >= TIMELINE_PAGE_LIMIT;
}

// Sources don't all format `ts` the same way (e.g. `2026-05-17T10:00:00Z`
// from comments vs `2026-05-17T10:00:00.500000+00:00` from internally
// generated ones), so raw string comparison isn't reliably order-preserving
// across pages. Always compare/sort on the parsed instant instead.
function tsValue(ts: string): number {
  return new Date(ts).getTime();
}

function isTsBefore(a: string, b: string): boolean {
  return tsValue(a) < tsValue(b);
}

// Stable identity for timeline rows backed by a current-state table (i.e. a
// single row per key that can move to a new ts), independent of their ts and
// payload. Used to drop a stale copy of the same row even when it now lives
// outside the fresh page's ts window. Append-only sources return undefined
// and keep the default ts+kind+payload identity.
function timelineEntityKey(event: TimelineEvent): string | undefined {
  if (event.kind === "activity_comment_posted") {
    return `activity_comment_posted:${String(event.payload.run_id)}`;
  }
  return undefined;
}

// Merges pages keyed on ts+kind+payload and re-sorts, so a moving "newest"
// window (refetched on the interval) can never drift apart from loaded
// earlier pages and silently drop the events between them.
export function mergeTimelineEvents(
  ...pages: TimelineEvent[][]
): TimelineEvent[] {
  const byKey = new Map<string, TimelineEvent>();
  for (const page of pages) {
    for (const event of page) {
      byKey.set(`${event.ts}:${event.kind}:${JSON.stringify(event.payload)}`, event);
    }
  }
  return [...byKey.values()].sort((left, right) => tsValue(left.ts) - tsValue(right.ts));
}

// The newest-page fetch is authoritative for everything at or after its
// oldest ts: some sources are current-state rows that collapse to a single
// row per key (e.g. activity_comment_marks), so a value that changed or
// disappeared there must be dropped, not merged in on top of the stale one.
// A current-state row can also have moved *into* the fresh window from a ts
// that used to sit outside it (loaded from an earlier page before it moved),
// so entity identity - not just the ts window - decides what's stale.
export function mergeNewestPage(
  prev: TimelineEvent[],
  page: TimelineEvent[],
): TimelineEvent[] {
  const windowStart = oldestLoadedTs(page);
  const freshEntityKeys = new Set(
    page.map(timelineEntityKey).filter((key): key is string => key !== undefined),
  );
  const kept = prev.filter((event) => {
    if (windowStart !== undefined && !isTsBefore(event.ts, windowStart)) {
      return false;
    }
    const entityKey = timelineEntityKey(event);
    return entityKey === undefined || !freshEntityKeys.has(entityKey);
  });
  return mergeTimelineEvents(kept, page);
}

async function fetchIssueTimeline(id: string, before?: string): Promise<TimelineEvent[]> {
  const response = await fetch(buildTimelineUrl(id, before), {
    headers: await authHeaders(),
  });
  if (!response.ok) {
    throw new Error(response.status === 404 ? "Issue not found" : "Failed to load timeline");
  }
  return (await response.json()) as TimelineEvent[];
}

function useRelativeClock() {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const interval = window.setInterval(() => setNow(Date.now()), 10000);
    return () => window.clearInterval(interval);
  }, []);

  return now;
}

function kindLabel(kind: string) {
  return KIND_LABELS[kind] ?? kind.split("_").join(" ");
}

function kindDotClass(kind: string) {
  return KIND_COLORS[kind] ?? "bg-muted-foreground";
}

function isExternalKind(kind: string) {
  return (
    kind === "external_observed" ||
    kind === "external_cleared" ||
    kind === "external_state_change"
  );
}

function formatUtc(ts: string) {
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) {
    return ts;
  }
  return `${date.toISOString().slice(0, 19)}Z`;
}

function formatRelative(ts: string, now: number) {
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) {
    return ts;
  }

  const diffSeconds = Math.round((date.getTime() - now) / 1000);
  const absSeconds = Math.abs(diffSeconds);
  const units: Array<[number, string]> = [
    [60 * 60 * 24, "d"],
    [60 * 60, "h"],
    [60, "m"],
  ];

  let value = absSeconds;
  let unit = "s";
  for (const [seconds, label] of units) {
    if (absSeconds >= seconds) {
      value = Math.round(absSeconds / seconds);
      unit = label;
      break;
    }
  }

  if (value < 10 && unit === "s") {
    return "now";
  }
  return diffSeconds > 0 ? `in ${value}${unit}` : `${value}${unit} ago`;
}

function RelativeTime({ ts, now }: { ts: string; now: number }) {
  return (
    <time className="font-mono" dateTime={ts} title={formatRelative(ts, now)}>
      {formatUtc(ts)}
    </time>
  );
}

function KindBadge({ kind }: { kind: string }) {
  return (
    <span className="inline-flex items-center gap-2 whitespace-nowrap rounded-md border px-2 py-1 text-xs font-medium">
      {isExternalKind(kind) ? (
        <svg
          aria-hidden="true"
          className="h-3 w-3 text-sky-700 dark:text-sky-400"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth="2"
        >
          <path
            d="M10 13a5 5 0 0 0 7.07 0l2.12-2.12a5 5 0 0 0-7.07-7.07L11 4.93"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          <path
            d="M14 11a5 5 0 0 0-7.07 0L4.81 13.12a5 5 0 0 0 7.07 7.07L13 19.07"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      ) : (
        <span className={cn("h-2 w-2 rounded-full", kindDotClass(kind))} />
      )}
      {kindLabel(kind)}
    </span>
  );
}

function PayloadList({ payload }: { payload: Record<string, unknown> }) {
  const entries = Object.entries(payload);
  if (entries.length === 0) {
    return <span className="text-muted-foreground">(empty)</span>;
  }

  return (
    <dl className="grid grid-cols-[max-content_minmax(0,1fr)] gap-x-3 gap-y-1">
      {entries.map(([key, value]) => (
        <div key={key} className="contents">
          <dt className="text-muted-foreground">{key}</dt>
          <dd className="min-w-0 break-words font-mono text-xs">{String(value)}</dd>
        </div>
      ))}
    </dl>
  );
}

function TimelineLine({ events, now }: { events: TimelineEvent[]; now: number }) {
  return (
    <ol className="relative ml-3 border-l pl-6">
      {events.map((event, index) => (
        <li key={`${event.ts}:${event.kind}:${index}`} className="relative pb-4 last:pb-0">
          <span
            className={cn(
              "absolute -left-[31px] top-1 h-3 w-3 rounded-full border-2 border-background",
              kindDotClass(event.kind),
            )}
          />
          <div className="rounded-md border bg-background p-3">
            <div className="mb-2 flex flex-wrap items-center gap-2">
              <KindBadge kind={event.kind} />
              <span className="text-xs text-muted-foreground">
                <RelativeTime ts={event.ts} now={now} />
              </span>
            </div>
            <PayloadList payload={event.payload} />
          </div>
        </li>
      ))}
    </ol>
  );
}

function TimelineTable({ events, now }: { events: TimelineEvent[]; now: number }) {
  const [sortDirection, setSortDirection] = useState<SortDirection>("asc");
  const [selectedKinds, setSelectedKinds] = useState<Set<string>>(
    () => new Set(TIMELINE_KINDS),
  );
  const eventKinds = useMemo(() => {
    const seen = new Set(events.map((event) => event.kind));
    const knownKinds = TIMELINE_KINDS.filter((kind) => seen.has(kind));
    const extraKinds = [...seen].filter(
      (kind) => !(TIMELINE_KINDS as readonly string[]).includes(kind),
    );
    return [...knownKinds, ...extraKinds.sort()];
  }, [events]);
  const visibleEvents = useMemo(() => {
    const filteredEvents = events.filter((event) => selectedKinds.has(event.kind));
    return [...filteredEvents].sort((left, right) => {
      const comparison = new Date(left.ts).getTime() - new Date(right.ts).getTime();
      return sortDirection === "asc" ? comparison : -comparison;
    });
  }, [events, selectedKinds, sortDirection]);

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-end gap-3">
        <label className="grid gap-1 text-xs font-medium text-muted-foreground">
          Kinds
          <select
            multiple
            className="min-h-24 rounded-md border bg-background px-2 py-1 text-sm text-foreground"
            value={[...selectedKinds]}
            onChange={(event) =>
              setSelectedKinds(
                new Set(
                  [...event.currentTarget.selectedOptions].map((option) => option.value),
                ),
              )
            }
          >
            {eventKinds.map((kind) => (
              <option key={kind} value={kind}>
                {kindLabel(kind)}
              </option>
            ))}
          </select>
        </label>
        <Button
          variant="secondary"
          type="button"
          onClick={() => setSortDirection((current) => (current === "asc" ? "desc" : "asc"))}
        >
          {sortDirection === "asc" ? "Oldest first" : "Newest first"}
        </Button>
      </div>
      {visibleEvents.length === 0 ? (
        <p className="text-sm text-muted-foreground">(none)</p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Time</TableHead>
              <TableHead>Kind</TableHead>
              <TableHead>Payload</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {visibleEvents.map((event, index) => (
              <TableRow key={`${event.ts}:${event.kind}:${index}`}>
                <TableCell className="whitespace-nowrap font-mono text-xs">
                  <RelativeTime ts={event.ts} now={now} />
                </TableCell>
                <TableCell>
                  <KindBadge kind={event.kind} />
                </TableCell>
                <TableCell className="max-w-[620px] break-words font-mono text-xs">
                  {JSON.stringify(event.payload)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}

function VariantToggle({
  variant,
  onVariantChange,
}: {
  variant: TimelineVariant;
  onVariantChange: (variant: TimelineVariant) => void;
}) {
  return (
    <div className="flex items-center gap-2">
      <Button
        type="button"
        variant={variant === "line" ? "default" : "secondary"}
        onClick={() => onVariantChange("line")}
      >
        Line
      </Button>
      <Button
        type="button"
        variant={variant === "table" ? "default" : "secondary"}
        onClick={() => onVariantChange("table")}
      >
        Table
      </Button>
    </div>
  );
}

export function IssueTimeline({ issueId }: { issueId: string }) {
  const [variant, setVariant] = useState<TimelineVariant>("line");
  const now = useRelativeClock();
  const { data, error, isLoading, isFetching } = useQuery({
    queryKey: ["issue-timeline", issueId],
    queryFn: () => fetchIssueTimeline(issueId),
    enabled: issueId.length > 0,
    refetchInterval: 5000,
    refetchOnWindowFocus: true,
    staleTime: 0,
  });

  // All loaded events (auto-refreshing newest page + any earlier pages),
  // merged and re-sorted on every update so the moving newest window and the
  // on-demand earlier pages can never drift apart and drop events between them.
  const [events, setEvents] = useState<TimelineEvent[]>([]);
  const [hasMoreEarlier, setHasMoreEarlier] = useState(false);
  const [loadingEarlier, setLoadingEarlier] = useState(false);
  const [earlierError, setEarlierError] = useState<string | null>(null);
  const [earlierLoadedOnce, setEarlierLoadedOnce] = useState(false);
  const [gapDetected, setGapDetected] = useState(false);

  // Lets in-flight requests notice they were issued for an issue the user has
  // since navigated away from, so their responses can be dropped instead of
  // being merged into the now-current issue's timeline.
  const issueIdRef = useRef(issueId);

  // Newest ts covered by the last-folded newest-page fetch, so the next
  // refetch can tell whether its window still overlaps what came before.
  const newestCoveredTsRef = useRef<string | undefined>(undefined);

  useEffect(() => {
    issueIdRef.current = issueId;
    setEvents([]);
    setHasMoreEarlier(false);
    setLoadingEarlier(false);
    setEarlierError(null);
    setEarlierLoadedOnce(false);
    setGapDetected(false);
    newestCoveredTsRef.current = undefined;
  }, [issueId]);

  // Fold each refetch of the newest page into the accumulator instead of
  // replacing it, so events already merged in from earlier pages survive.
  useEffect(() => {
    if (!data) {
      return;
    }
    const windowStart = oldestLoadedTs(data);
    const previousNewestTs = newestCoveredTsRef.current;
    // If more than a full page of events arrived since the last refetch, the
    // fresh newest-page window no longer overlaps what we'd already loaded:
    // there's a gap of events we never fetched and can't recover with only a
    // `before` cursor, so surface it instead of pretending the accumulation
    // is still contiguous.
    const gapped =
      previousNewestTs !== undefined &&
      windowStart !== undefined &&
      isTsBefore(previousNewestTs, windowStart);
    if (gapped) {
      setEvents(data);
      setEarlierLoadedOnce(false);
      setGapDetected(true);
      setHasMoreEarlier(hasEarlierEvents(data.length));
    } else {
      setEvents((prev) => mergeNewestPage(prev, data));
      // Seed the load-earlier control from the newest page's size, without
      // clobbering it once the user has started paging back manually.
      if (!earlierLoadedOnce) {
        setHasMoreEarlier(hasEarlierEvents(data.length));
      }
    }
    const windowEnd = data[data.length - 1]?.ts;
    if (windowEnd !== undefined) {
      newestCoveredTsRef.current = windowEnd;
    }
  }, [data, earlierLoadedOnce]);

  async function loadEarlier() {
    const cursor = oldestLoadedTs(events);
    if (!cursor || loadingEarlier) {
      return;
    }
    const requestIssueId = issueId;
    setLoadingEarlier(true);
    setEarlierError(null);
    try {
      const page = await fetchIssueTimeline(requestIssueId, cursor);
      if (issueIdRef.current !== requestIssueId) {
        return;
      }
      setEvents((prev) => mergeTimelineEvents(prev, page));
      setHasMoreEarlier(hasEarlierEvents(page.length));
      setEarlierLoadedOnce(true);
      setLoadingEarlier(false);
    } catch (loadError) {
      if (issueIdRef.current !== requestIssueId) {
        return;
      }
      setEarlierError((loadError as Error).message);
      setLoadingEarlier(false);
    }
  }

  return (
    <section className="border-t py-5">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold tracking-normal">Timeline</h2>
          <p className="text-sm text-muted-foreground">
            {isFetching ? "Refreshing" : `${events.length} events`}
          </p>
        </div>
        <VariantToggle variant={variant} onVariantChange={setVariant} />
      </div>
      {isLoading ? <p className="text-sm text-muted-foreground">Loading</p> : null}
      {error ? (
        <p className="text-sm text-red-600 dark:text-red-400">{(error as Error).message}</p>
      ) : null}
      {!isLoading && !error && events.length === 0 ? (
        <p className="text-sm text-muted-foreground">(none)</p>
      ) : null}
      {gapDetected ? (
        <p className="mb-3 text-sm text-amber-600 dark:text-amber-400">
          Missed some events while this view was idle - showing only the latest window.
        </p>
      ) : null}
      {events.length > 0 && hasMoreEarlier ? (
        <div className="mb-3 flex items-center gap-3">
          <Button type="button" variant="secondary" onClick={loadEarlier} disabled={loadingEarlier}>
            {loadingEarlier ? "Loading" : "Load earlier events"}
          </Button>
          {earlierError ? (
            <span className="text-sm text-red-600 dark:text-red-400">{earlierError}</span>
          ) : null}
        </div>
      ) : null}
      {events.length > 0 && variant === "line" ? (
        <TimelineLine events={events} now={now} />
      ) : null}
      {events.length > 0 && variant === "table" ? (
        <TimelineTable events={events} now={now} />
      ) : null}
    </section>
  );
}
