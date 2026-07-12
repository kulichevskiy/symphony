// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildTimelineUrl,
  hasEarlierEvents,
  IssueTimeline,
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
});
