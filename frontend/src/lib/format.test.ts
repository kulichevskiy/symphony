import { describe, expect, it } from "vitest";

import { formatEventTime } from "./format";

describe("formatEventTime", () => {
  it("renders the six-fractional-digit receipt timestamp the backend emits", () => {
    // `datetime.now(UTC).isoformat()` (src/symphony/agent/run_log.py) always
    // emits six fractional digits, unlike `toISOString()`'s three — this
    // pins that the format contract still parses via `new Date()`.
    expect(formatEventTime("2026-08-27T10:00:00.123456+00:00")).toMatch(/^\d{2}:\d{2}:\d{2}$/);
  });
});
