import { describe, expect, it } from "vitest";

import { buildGrid, buildHeatThresholds, heatLevel, isInWindow } from "./Heatmap";

describe("buildHeatThresholds", () => {
  it("derives quantile cut points from non-zero days only", () => {
    const days = [0, 0, 100, 200, 300, 400].map((output_tokens) => ({
      output_tokens,
    }));
    const thresholds = buildHeatThresholds(days);
    // Three ascending cut points splitting the four positive values.
    expect(thresholds).toHaveLength(3);
    expect(thresholds[0]).toBeLessThanOrEqual(thresholds[1]);
    expect(thresholds[1]).toBeLessThanOrEqual(thresholds[2]);
    expect(thresholds[0]).toBeGreaterThan(0);
  });

  it("recomputes a smaller scale for a quieter slice", () => {
    const big = buildHeatThresholds(
      [1_000_000, 5_000_000, 12_000_000, 20_000_000].map((output_tokens) => ({
        output_tokens,
      })),
    );
    const small = buildHeatThresholds(
      [10_000, 50_000, 120_000, 200_000].map((output_tokens) => ({
        output_tokens,
      })),
    );
    expect(small[2]).toBeLessThan(big[2]);
  });

  it("stays usable when there is no activity", () => {
    const thresholds = buildHeatThresholds([
      { output_tokens: 0 },
      { output_tokens: 0 },
    ]);
    expect(heatLevel(0, thresholds)).toBe(0);
  });
});

describe("buildGrid", () => {
  it("aligns weeks Monday (top row) to Sunday (bottom row)", () => {
    const { weeks } = buildGrid([], "2026-05-01", "2026-05-31");
    // Row di maps to a Monday-indexed weekday: di=0 -> Mon (getUTCDay 1),
    // di=6 -> Sun (getUTCDay 0).
    for (const week of weeks) {
      week.forEach((cell, di) => {
        if (cell) expect(cell.date.getUTCDay()).toBe((di + 1) % 7);
      });
    }
  });
});

describe("heatLevel", () => {
  const thresholds = [100, 200, 300];

  it("maps zero tokens to the empty level", () => {
    expect(heatLevel(0, thresholds)).toBe(0);
  });

  it("buckets positive tokens across the four shaded levels", () => {
    expect(heatLevel(50, thresholds)).toBe(1);
    expect(heatLevel(150, thresholds)).toBe(2);
    expect(heatLevel(250, thresholds)).toBe(3);
    expect(heatLevel(999, thresholds)).toBe(4);
  });
});

describe("isInWindow", () => {
  it("includes every cell when bounds are open (all-time)", () => {
    expect(isInWindow("2026-01-01", null, null)).toBe(true);
  });

  it("honors inclusive lower and upper day bounds", () => {
    expect(isInWindow("2026-05-10", "2026-05-11", "2026-05-17")).toBe(false);
    expect(isInWindow("2026-05-11", "2026-05-11", "2026-05-17")).toBe(true);
    expect(isInWindow("2026-05-17", "2026-05-11", "2026-05-17")).toBe(true);
    expect(isInWindow("2026-05-18", "2026-05-11", "2026-05-17")).toBe(false);
  });

  it("treats a single missing bound as open on that side", () => {
    expect(isInWindow("1999-01-01", null, "2026-05-17")).toBe(true);
    expect(isInWindow("2030-01-01", "2026-05-11", null)).toBe(true);
  });
});
