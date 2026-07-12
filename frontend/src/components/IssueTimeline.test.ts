import { describe, expect, it } from "vitest";

import {
  buildTimelineUrl,
  hasEarlierEvents,
  oldestLoadedTs,
  TIMELINE_PAGE_LIMIT,
  type TimelineEvent,
} from "./IssueTimeline";

function event(ts: string, kind = "run_started"): TimelineEvent {
  return { ts, kind, payload: {} };
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

describe("prepending earlier pages", () => {
  it("keeps events ascending with no gaps or duplicates across pages", () => {
    // Simulate the component: newest page loaded, then an earlier page fetched
    // with before = oldest loaded ts, prepended ahead of it.
    const newest = [event("2026-05-17T10:04:45Z"), event("2026-05-17T10:05:00Z")];
    const cursor = oldestLoadedTs(newest);
    expect(cursor).toBe("2026-05-17T10:04:45Z");

    // Backend returns strictly-older events for that cursor.
    const earlier = [event("2026-05-17T10:03:00Z"), event("2026-05-17T10:04:30Z")];
    const merged = [...earlier, ...newest];

    const timestamps = merged.map((e) => e.ts);
    expect(timestamps).toEqual([...timestamps].sort());
    expect(new Set(timestamps).size).toBe(timestamps.length);
  });
});
