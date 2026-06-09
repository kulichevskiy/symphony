import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { StageSeries } from "@/lib/api";

import {
  buildStageColumns,
  seriesMonthMarks,
  StageTrend,
  stageBars,
} from "./StageTrend";

const series: StageSeries = {
  bucket: "day",
  start: "2026-05-14",
  end: "2026-05-16",
  // Stage keys arrive in arbitrary (alphabetical) order from the server.
  stages: ["merge", "implement", "review"],
  buckets: [
    { start: "2026-05-14", output_tokens: { implement: 100, merge: 0 } },
    { start: "2026-05-15", output_tokens: {} },
    { start: "2026-05-16", output_tokens: { implement: 20, merge: 80 } },
  ],
};

describe("buildStageColumns", () => {
  it("orders stages by pipeline rank and totals each bucket", () => {
    const { stages, columns, maxTotal } = buildStageColumns(series);
    // implement → review → merge (pipeline order), not the server's order.
    expect(stages).toEqual(["implement", "review", "merge"]);
    expect(columns.map((c) => c.total)).toEqual([100, 0, 100]);
    expect(maxTotal).toBe(100);
  });
});

describe("seriesMonthMarks", () => {
  it("labels the first bucket of each new month", () => {
    const marks = seriesMonthMarks([
      "2026-04-29",
      "2026-04-30",
      "2026-05-01",
      "2026-05-02",
    ]);
    expect(marks).toEqual([
      { index: 0, label: "Apr" },
      { index: 2, label: "May" },
    ]);
  });
});

describe("stageBars", () => {
  it("returns pipeline-ordered non-zero stages with labels for a bucket", () => {
    const { stages, columns } = buildStageColumns(series);
    const bars = stageBars(columns[2], stages);
    expect(bars).toEqual([
      { key: "implement", label: "Implement", value: 20 },
      { key: "merge", label: "Merge", value: 80 },
    ]);
  });
});

describe("StageTrend", () => {
  it("renders stacked columns with the shared stage palette, defaults to Tokens", () => {
    const markup = renderToStaticMarkup(<StageTrend series={series} />);
    // Both metric modes offered; Tokens is the active default.
    expect(markup).toContain("Tokens");
    expect(markup).toContain("% share");
    // Stage palette segments (implement=blue, merge=emerald) are drawn.
    expect(markup).toContain("bg-blue-500");
    expect(markup).toContain("bg-emerald-500");
    // Legend lists stages in pipeline order.
    expect(markup.indexOf("Implement")).toBeLessThan(markup.indexOf("Merge"));
    // Month axis label rendered.
    expect(markup).toContain("May");
    // Daily granularity hint.
    expect(markup).toContain("daily");
    // No event/prompt-change markers are drawn.
    expect(markup.toLowerCase()).not.toContain("marker");
  });

  it("renders an empty-state when the window has no buckets", () => {
    const empty: StageSeries = {
      bucket: "day",
      start: null,
      end: null,
      stages: [],
      buckets: [],
    };
    const markup = renderToStaticMarkup(<StageTrend series={empty} />);
    expect(markup).toContain("No stage activity");
  });
});
