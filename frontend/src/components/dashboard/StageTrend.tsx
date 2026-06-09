import { useRef, useState } from "react";

import type { StageSeries } from "@/lib/api";
import { formatLongDate, formatTokens } from "@/lib/format";
import { cn } from "@/lib/utils";

import { STAGE_LABEL, STAGE_TINT, stageRank } from "./atoms";

const MONTHS = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

export type StageColumn = {
  start: string;
  total: number;
  perStage: Record<string, number>;
};

/** Pipeline-order the window's stage keys (unknown stages sort last). */
function orderedSeriesStages(stages: string[]): string[] {
  return [...stages].sort((a, b) => stageRank(a) - stageRank(b));
}

/** How a trend dimension (stage / team / model) renders its keys: the stack
 *  order, the legend/tooltip label, and the segment color. Defaults below give
 *  the by-stage behavior; the by-team / by-model views pass their own. */
export type SeriesAdapter = {
  order: (keys: string[]) => string[];
  label: (key: string) => string;
  tint: (key: string) => string;
};

const STAGE_ADAPTER: SeriesAdapter = {
  order: orderedSeriesStages,
  label: (key) => STAGE_LABEL[key] ?? key,
  tint: (key) => STAGE_TINT[key] ?? "bg-slate-400",
};

/** Reduce the series into ordered keys, per-bucket columns with their output
 *  total, and the largest column total (for magnitude scaling). */
export function buildStageColumns(
  series: StageSeries,
  order: (keys: string[]) => string[] = orderedSeriesStages,
): {
  stages: string[];
  columns: StageColumn[];
  maxTotal: number;
} {
  const stages = order(series.stages);
  const columns: StageColumn[] = series.buckets.map((b) => ({
    start: b.start,
    total: Object.values(b.output_tokens).reduce((s, v) => s + v, 0),
    perStage: b.output_tokens,
  }));
  const maxTotal = columns.reduce((m, c) => Math.max(m, c.total), 0);
  return { stages, columns, maxTotal };
}

/** Axis labels: the index + month abbrev of the first bucket of each new month
 *  (January carries a 2-digit year so spans crossing a year stay legible). */
export function seriesMonthMarks(
  starts: string[],
): Array<{ index: number; label: string }> {
  const marks: Array<{ index: number; label: string }> = [];
  let last = "";
  starts.forEach((start, index) => {
    const d = new Date(`${start}T00:00:00Z`);
    const key = `${d.getUTCFullYear()}-${d.getUTCMonth()}`;
    if (key === last) return;
    last = key;
    const month = MONTHS[d.getUTCMonth()];
    const label =
      d.getUTCMonth() === 0
        ? `${month} '${String(d.getUTCFullYear()).slice(2)}`
        : month;
    marks.push({ index, label });
  });
  return marks;
}

/** Ordered, non-zero stacked bars for one bucket — used by the tooltip. */
export function stageBars(
  column: StageColumn,
  stages: string[],
  label: (key: string) => string = STAGE_ADAPTER.label,
): Array<{ key: string; label: string; value: number }> {
  return stages
    .map((key) => ({
      key,
      label: label(key),
      value: column.perStage[key] ?? 0,
    }))
    .filter((b) => b.value > 0);
}

/**
 * The Trend chart: output tokens over time as stacked columns, one per bucket,
 * keys stacked in the adapter's order with its palette. Drives the by-stage,
 * by-team, and by-model trends — only the `adapter` differs.
 *
 * - Tokens mode (default): a column's height encodes its output total relative
 *   to the busiest bucket; segments within split by each key's share.
 * - % share mode: every column fills the track; only the segment split varies.
 *
 * Both modes derive purely from output tokens. No event/prompt-change markers.
 * `mode` (Tokens / % share) is owned by the parent so its toggle can sit in the
 * shared Breakdown header row alongside Totals / Trend.
 */
export function StageTrend({
  series,
  mode,
  adapter = STAGE_ADAPTER,
}: {
  series: StageSeries;
  mode: "tokens" | "share";
  adapter?: SeriesAdapter;
}) {
  const [hover, setHover] = useState<
    { column: StageColumn; left: number; top: number } | null
  >(null);
  const wrapRef = useRef<HTMLDivElement>(null);
  const { stages, columns, maxTotal } = buildStageColumns(series, adapter.order);
  const monthMarks = seriesMonthMarks(columns.map((c) => c.start));
  const markByIndex = new Map(monthMarks.map((m) => [m.index, m.label]));

  function onEnter(
    e: React.MouseEvent<HTMLDivElement>,
    column: StageColumn,
  ) {
    const wrap = wrapRef.current?.getBoundingClientRect();
    if (!wrap) return;
    const rect = e.currentTarget.getBoundingClientRect();
    setHover({
      column,
      left: rect.left - wrap.left + rect.width / 2,
      top: rect.top - wrap.top,
    });
  }

  return (
    <div>
      {columns.length === 0 ? (
        <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
          No stage activity in this window
        </div>
      ) : (
        <div className="relative" ref={wrapRef}>
          <div className="flex h-40 items-end gap-px">
            {columns.map((c) => {
              const fillPct =
                mode === "tokens"
                  ? maxTotal > 0
                    ? (c.total / maxTotal) * 100
                    : 0
                  : c.total > 0
                    ? 100
                    : 0;
              return (
                <div
                  key={c.start}
                  className="relative flex h-full min-w-[2px] flex-1 items-end"
                  onMouseEnter={(e) => onEnter(e, c)}
                  onMouseLeave={() => setHover(null)}
                >
                  <div
                    className="flex w-full flex-col-reverse overflow-hidden rounded-sm"
                    style={{ height: `${fillPct}%` }}
                  >
                    {stages.map((st) => {
                      const value = c.perStage[st] ?? 0;
                      if (value <= 0 || c.total <= 0) return null;
                      return (
                        <div
                          key={st}
                          className={adapter.tint(st)}
                          style={{ height: `${(value / c.total) * 100}%` }}
                        />
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>

          {/* Month axis: a label cell under the first bucket of each month. */}
          <div className="mt-1 flex gap-px">
            {columns.map((c, i) => (
              <div
                key={c.start}
                className="min-w-[2px] flex-1 whitespace-nowrap text-[9px] leading-3 text-muted-foreground"
              >
                {markByIndex.get(i) ?? ""}
              </div>
            ))}
          </div>

          <div className="mt-3 flex flex-wrap items-center gap-x-3 gap-y-1">
            {stages.map((st) => (
              <span
                key={st}
                className="flex items-center gap-1.5 text-[11px] text-muted-foreground"
              >
                <span
                  className={cn("h-2 w-2 rounded-sm", adapter.tint(st))}
                />
                {adapter.label(st)}
              </span>
            ))}
          </div>

          {hover ? (
            <div
              className="pointer-events-none absolute z-30 -translate-x-1/2 -translate-y-full whitespace-nowrap rounded-md border border-border bg-popover px-2.5 py-1.5 text-xs shadow-lg"
              style={{ left: hover.left, top: hover.top - 6 }}
            >
              <div className="font-medium text-foreground">
                {formatLongDate(hover.column.start)}
              </div>
              <div className="mt-1 grid grid-cols-[auto_auto] gap-x-4 gap-y-0.5 font-mono text-[11px] text-muted-foreground">
                {stageBars(hover.column, stages, adapter.label).map((b) => (
                  <span key={b.key} className="flex items-center gap-1.5">
                    <span
                      className={cn("h-2 w-2 rounded-sm", adapter.tint(b.key))}
                    />
                    {b.label} {formatTokens(b.value)}
                  </span>
                ))}
              </div>
              <div className="mt-1 border-t border-border pt-1 font-mono text-[11px] text-foreground">
                total {formatTokens(hover.column.total)}
              </div>
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}
