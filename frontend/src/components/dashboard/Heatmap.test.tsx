import { describe, expect, it } from "vitest";

import { buildHeatThresholds, heatLevel } from "./Heatmap";

describe("buildHeatThresholds", () => {
  it("derives quantile cut points from non-zero days only", () => {
    const days = [0, 0, 100, 200, 300, 400].map((tokens) => ({ tokens }));
    const thresholds = buildHeatThresholds(days);
    // Three ascending cut points splitting the four positive values.
    expect(thresholds).toHaveLength(3);
    expect(thresholds[0]).toBeLessThanOrEqual(thresholds[1]);
    expect(thresholds[1]).toBeLessThanOrEqual(thresholds[2]);
    expect(thresholds[0]).toBeGreaterThan(0);
  });

  it("recomputes a smaller scale for a quieter slice", () => {
    const big = buildHeatThresholds(
      [1_000_000, 5_000_000, 12_000_000, 20_000_000].map((tokens) => ({ tokens })),
    );
    const small = buildHeatThresholds(
      [10_000, 50_000, 120_000, 200_000].map((tokens) => ({ tokens })),
    );
    expect(small[2]).toBeLessThan(big[2]);
  });

  it("stays usable when there is no activity", () => {
    const thresholds = buildHeatThresholds([{ tokens: 0 }, { tokens: 0 }]);
    expect(heatLevel(0, thresholds)).toBe(0);
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
