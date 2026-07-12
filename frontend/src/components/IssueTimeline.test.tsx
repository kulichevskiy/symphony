// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildTimelineUrl,
  hasEarlierEvents,
  IssueTimeline,
  mergeNewestPage,
  mergeTimelineEvents,
  oldestLoadedTs,
  TIMELINE_PAGE_LIMIT,
  type TimelineEvent,
} from "./IssueTimeline";

function event(
  ts: string,
  kind = "run_started",
  payload: Record<string, unknown> = {},
): TimelineEvent {
  return { ts, kind, payload };
}

describe("buildTimelineUrl", () => {
  it("requests the default limit without a cursor", () => {
    expect(buildTimelineUrl("iss-1")).toBe(
      `/api/issues/iss-1/timeline?limit=${TIMELINE_PAGE_LIMIT}`,
    );
  });

  it("passes the before cursor and encodes the id", () => {
    const url = buildTimelineUrl("a b", "2026-05-17T10:00:00Z");
    expect(url).toContain("/api/issues/a%20b/timeline");
    expect(url).toContain(`limit=${TIMELINE_PAGE_LIMIT}`);
    expect(url).toContain("before=2026-05-17T10%3A00%3A00Z");
  });
});

describe("oldestLoadedTs", () => {
  it("returns the first (oldest) ts as the next before cursor", () => {
    const events = [event("2026-05-17T10:00:00Z"), event("2026-05-17T10:05:00Z")];
    expect(oldestLoadedTs(events)).toBe("2026-05-17T10:00:00Z");
  });

  it("is undefined when nothing is loaded", () => {
    expect(oldestLoadedTs([])).toBeUndefined();
  });
});

describe("hasEarlierEvents", () => {
  it("offers loading earlier only when a full page came back", () => {
    expect(hasEarlierEvents(TIMELINE_PAGE_LIMIT - 1)).toBe(false);
    expect(hasEarlierEvents(TIMELINE_PAGE_LIMIT)).toBe(true);
    // Backend may exceed the limit to keep a tie group intact.
    expect(hasEarlierEvents(TIMELINE_PAGE_LIMIT + 3)).toBe(true);
  });
});

describe("mergeTimelineEvents", () => {
  it("dedupes on ts+kind across pages and re-sorts ascending", () => {
    const pageA = [event("2026-05-17T10:03:00Z"), event("2026-05-17T10:04:30Z")];
    const pageB = [event("2026-05-17T10:04:30Z"), event("2026-05-17T10:05:00Z")];
    const merged = mergeTimelineEvents(pageA, pageB);
    expect(merged.map((e) => e.ts)).toEqual([
      "2026-05-17T10:03:00Z",
      "2026-05-17T10:04:30Z",
      "2026-05-17T10:05:00Z",
    ]);
  });

  it("keeps distinct events that share a ts+kind (e.g. two comment_seen rows with GitHub's second-precision ts)", () => {
    const ts = "2026-05-17T10:04:30Z";
    const first = event(ts, "comment_seen", { comment_id: 1 });
    const second = event(ts, "comment_seen", { comment_id: 2 });
    const merged = mergeTimelineEvents([first, second]);
    expect(merged.length).toBe(2);
  });

  it("orders mixed ts shapes chronologically, not lexicographically", () => {
    // A second-precision `Z` ts string-sorts *after* a later sub-second
    // `+00:00` one ('.' < 'Z'), even though it happened first.
    const earlierZ = event("2026-05-17T10:00:00Z");
    const laterOffset = event("2026-05-17T10:00:00.500000+00:00");
    const merged = mergeTimelineEvents([laterOffset, earlierZ]);
    expect(merged.map((e) => e.ts)).toEqual([
      "2026-05-17T10:00:00Z",
      "2026-05-17T10:00:00.500000+00:00",
    ]);
  });
});

describe("mergeNewestPage", () => {
  it("drops a stale prior value for a current-state row the newest page no longer reports", () => {
    // activity_comment_marks collapses to one row per run: an older
    // last_posted_at/fingerprint for the same run must not survive once the
    // mark has moved on, unlike append-only rows.
    const anchor = event("2026-05-17T10:00:00Z");
    const stalePost = event("2026-05-17T10:03:00Z", "activity_comment_posted", {
      run_id: "run-1",
      fingerprint: "fp-old",
    });
    const prev = [anchor, stalePost];
    const refreshedPost = event("2026-05-17T10:06:00Z", "activity_comment_posted", {
      run_id: "run-1",
      fingerprint: "fp-new",
    });
    // The fresh page's window starts back at `anchor`'s ts, so it's
    // authoritative over the whole [anchor, now) range - including the ts
    // where `stalePost` used to live.
    const merged = mergeNewestPage(prev, [anchor, refreshedPost]);
    expect(merged).toEqual([anchor, refreshedPost]);
  });

  it("drops a stale current-state row even when it sits older than the fresh window", () => {
    // The mark was loaded from an earlier page while its last_posted_at was
    // still old; it has since moved into the fresh page's window under a new
    // ts, but the old copy's ts (older than the window) means a plain ts
    // check wouldn't touch it - identity must.
    const older = event("2026-05-17T09:00:00Z");
    const staleMark = event("2026-05-17T09:30:00Z", "activity_comment_posted", {
      run_id: "run-1",
      fingerprint: "fp-old",
    });
    const prev = [older, staleMark];
    const windowAnchor = event("2026-05-17T10:00:00Z");
    const freshMark = event("2026-05-17T10:05:00Z", "activity_comment_posted", {
      run_id: "run-1",
      fingerprint: "fp-new",
    });
    const merged = mergeNewestPage(prev, [windowAnchor, freshMark]);
    expect(merged).toEqual([older, windowAnchor, freshMark]);
  });

  it("keeps earlier events outside the newest page's window", () => {
    const older = event("2026-05-17T09:00:00Z");
    const prev = [older, event("2026-05-17T10:00:00Z")];
    const page = [event("2026-05-17T10:00:00Z"), event("2026-05-17T10:05:00Z")];
    const merged = mergeNewestPage(prev, page);
    expect(merged.map((e) => e.ts)).toEqual([
      "2026-05-17T09:00:00Z",
      "2026-05-17T10:00:00Z",
      "2026-05-17T10:05:00Z",
    ]);
  });
});

// Minute-spaced timestamps so page windows are easy to reason about.
const BASE_MS = Date.UTC(2026, 0, 1, 0, 0, 0);
function tsAt(minute: number): string {
  return new Date(BASE_MS + minute * 60_000).toISOString();
}
function eventAt(minute: number): TimelineEvent {
  return event(tsAt(minute));
}

describe("IssueTimeline", () => {
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("loads earlier pages and survives a newest-page refetch without dropping the events between them", async () => {
    // Newest page on mount: a full page, minutes 800..999.
    const newestPage = Array.from({ length: TIMELINE_PAGE_LIMIT }, (_, i) => eventAt(800 + i));
    // Earlier page fetched on demand: minutes 750..800 (ties with the newest
    // page's oldest event at minute 800 — must dedupe, not double-count).
    const earlierPage = Array.from({ length: 51 }, (_, i) => eventAt(750 + i));
    // Simulated refetch of the newest page 5s later: the window has slid
    // forward to minutes 850..1049, no longer covering 800..849 — those must
    // still render from what was already merged in from `newestPage`.
    const refetchedPage = Array.from({ length: TIMELINE_PAGE_LIMIT }, (_, i) => eventAt(850 + i));

    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      const before = new URL(url, "http://localhost").searchParams.get("before");
      const body = before === tsAt(800) ? earlierPage : newestPage;
      return Promise.resolve({
        ok: true,
        status: 200,
        json: () => Promise.resolve(body),
      } as Response);
    });
    vi.stubGlobal("fetch", fetchMock);

    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <IssueTimeline issueId="iss-1" />
      </QueryClientProvider>,
    );

    const loadEarlierButton = await screen.findByRole("button", { name: "Load earlier events" });
    await act(async () => {
      fireEvent.click(loadEarlierButton);
      await Promise.resolve();
      await Promise.resolve();
    });

    // Cursor for the earlier fetch was the newest page's oldest ts.
    expect(
      fetchMock.mock.calls.some((call) => {
        const url = typeof call[0] === "string" ? call[0] : call[0].toString();
        return url.includes(`before=${encodeURIComponent(tsAt(800))}`);
      }),
    ).toBe(true);

    // 200 + 51 - 1 (tie at minute 800) = 250 unique events, prepended in order.
    await screen.findByText("250 events");

    // Simulate the interval refetch landing with a shifted window.
    await act(async () => {
      client.setQueryData(["issue-timeline", "iss-1"], refetchedPage);
      await Promise.resolve();
    });

    // 250 + 50 new (minutes 1000..1049) = 300; minutes 800..849 survive from
    // the earlier merge even though `refetchedPage` no longer contains them.
    await screen.findByText("300 events");
    const renderedTimes = [
      ...document.querySelectorAll("time[datetime]"),
    ].map((node) => node.getAttribute("datetime"));
    expect(renderedTimes).toContain(tsAt(810));
    expect(renderedTimes).toEqual([...renderedTimes].sort());
    expect(new Set(renderedTimes).size).toBe(renderedTimes.length);
  });

  it("detects a gap when the newest page jumps ahead with no overlap", async () => {
    // Mount: minutes 800..999.
    const newestPage = Array.from({ length: TIMELINE_PAGE_LIMIT }, (_, i) => eventAt(800 + i));
    // Refetch lands with a window that shares no ts with what's loaded: more
    // than a full page of events must have arrived since the last refetch.
    const jumpedPage = Array.from({ length: TIMELINE_PAGE_LIMIT }, (_, i) => eventAt(5000 + i));

    const fetchMock = vi.fn(() =>
      Promise.resolve({
        ok: true,
        status: 200,
        json: () => Promise.resolve(newestPage),
      } as Response),
    );
    vi.stubGlobal("fetch", fetchMock);

    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <IssueTimeline issueId="iss-1" />
      </QueryClientProvider>,
    );

    await screen.findByText("200 events");
    expect(screen.queryByText(/Missed some events/)).toBeNull();

    await act(async () => {
      client.setQueryData(["issue-timeline", "iss-1"], jumpedPage);
      await Promise.resolve();
    });

    // Reset to just the fresh page instead of pretending the stale
    // accumulation is still contiguous with it.
    await screen.findByText("200 events");
    await screen.findByText(/Missed some events/);
    const renderedTimes = [
      ...document.querySelectorAll("time[datetime]"),
    ].map((node) => node.getAttribute("datetime"));
    expect(renderedTimes).not.toContain(tsAt(810));
    expect(renderedTimes).toContain(tsAt(5010));
  });

  it("drops a load-earlier response for an issue the user navigated away from before it resolved", async () => {
    const iss1Newest = Array.from({ length: TIMELINE_PAGE_LIMIT }, (_, i) => eventAt(800 + i));
    const iss1Earlier = Array.from({ length: 5 }, (_, i) => eventAt(795 + i));
    const iss2Newest = [eventAt(1)];

    let resolveIss1Earlier: ((events: TimelineEvent[]) => void) | undefined;
    const iss1EarlierPromise = new Promise<TimelineEvent[]>((resolve) => {
      resolveIss1Earlier = resolve;
    });

    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      const parsed = new URL(url, "http://localhost");
      const before = parsed.searchParams.get("before");
      const respond = (body: TimelineEvent[]) =>
        Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(body) } as Response);

      if (parsed.pathname === "/api/issues/iss-1/timeline") {
        return before ? iss1EarlierPromise.then(respond) : respond(iss1Newest);
      }
      return respond(iss2Newest);
    });
    vi.stubGlobal("fetch", fetchMock);

    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <IssueTimeline issueId="iss-1" />
      </QueryClientProvider>,
    );

    const loadEarlierButton = await screen.findByRole("button", { name: "Load earlier events" });
    fireEvent.click(loadEarlierButton);

    // Navigate to a different issue before the in-flight "load earlier"
    // request for iss-1 resolves.
    await act(async () => {
      rerender(
        <QueryClientProvider client={client}>
          <IssueTimeline issueId="iss-2" />
        </QueryClientProvider>,
      );
      await Promise.resolve();
    });
    await screen.findByText("1 events");

    // The stale iss-1 response lands after navigation; it must be dropped
    // rather than merged into iss-2's timeline.
    await act(async () => {
      resolveIss1Earlier?.(iss1Earlier);
      await Promise.resolve();
      await Promise.resolve();
    });
    await screen.findByText("1 events");
  });
});
