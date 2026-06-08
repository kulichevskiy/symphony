import { useEffect, useRef, useState } from "react";

import type { HeatmapDay } from "@/lib/api";
import { formatLongDate } from "@/lib/format";
import { cn } from "@/lib/utils";

import { Tk } from "./atoms";

const HEAT_LEVELS = [
  "bg-slate-100 dark:bg-slate-800/70",
  "bg-blue-200 dark:bg-blue-900",
  "bg-blue-300 dark:bg-blue-700",
  "bg-blue-500 dark:bg-blue-600",
  "bg-blue-700 dark:bg-blue-400",
];

/**
 * Quantile cut points for the four shaded levels, derived from the non-zero
 * output of the days in the current slice so the gradation scales to the max
 * daily output in the visible range whatever provider is selected. Returns
 * three ascending thresholds; an all-zero slice yields a trivial scale where
 * every positive day lands in the top level.
 */
export function buildHeatThresholds(days: { output_tokens: number }[]): number[] {
  const vals = days
    .map((d) => d.output_tokens)
    .filter((t) => t > 0)
    .sort((a, b) => a - b);
  if (vals.length === 0) return [1, 1, 1];
  const q = (p: number) =>
    vals[Math.min(vals.length - 1, Math.floor(p * vals.length))];
  return [q(0.25), q(0.5), q(0.75)];
}

export function heatLevel(output: number, thresholds: number[]): number {
  if (!output) return 0;
  if (output < thresholds[0]) return 1;
  if (output < thresholds[1]) return 2;
  if (output < thresholds[2]) return 3;
  return 4;
}

const MONTHS = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

type Cell = {
  iso: string;
  date: Date;
  issues: number;
  input: number;
  output: number;
  cacheWrite: number;
  cacheRead: number;
};

/**
 * Build a dense Sun..Sat-aligned grid of weeks ending on the Saturday of the
 * end date's week, zero-filling days with no recorded spend. Days after the
 * end date (trailing in the final week) are rendered as gaps.
 */
function buildGrid(days: HeatmapDay[], start: string, end: string): {
  weeks: (Cell | null)[][];
  monthMarks: { wi: number; label: string }[];
} {
  const byIso = new Map(days.map((d) => [d.date, d]));
  const startDate = new Date(`${start}T00:00:00Z`);
  const endDate = new Date(`${end}T00:00:00Z`);
  // Pad to whole weeks: back to Sunday, forward to Saturday.
  const gridStart = new Date(startDate);
  gridStart.setUTCDate(gridStart.getUTCDate() - gridStart.getUTCDay());
  const gridEnd = new Date(endDate);
  gridEnd.setUTCDate(gridEnd.getUTCDate() + (6 - gridEnd.getUTCDay()));

  const cells: (Cell | null)[] = [];
  for (
    let d = new Date(gridStart);
    d <= gridEnd;
    d.setUTCDate(d.getUTCDate() + 1)
  ) {
    if (d < startDate || d > endDate) {
      cells.push(null);
      continue;
    }
    const iso = d.toISOString().slice(0, 10);
    const hit = byIso.get(iso);
    cells.push({
      iso,
      date: new Date(d),
      issues: hit?.issues ?? 0,
      input: hit?.input_tokens ?? 0,
      output: hit?.output_tokens ?? 0,
      cacheWrite: hit?.cache_write_tokens ?? 0,
      cacheRead: hit?.cache_read_tokens ?? 0,
    });
  }

  const weeks: (Cell | null)[][] = [];
  for (let i = 0; i < cells.length; i += 7) {
    weeks.push(cells.slice(i, i + 7));
  }

  const monthMarks: { wi: number; label: string }[] = [];
  let lastMonth = -1;
  weeks.forEach((w, wi) => {
    const first = w.find((c) => c !== null);
    if (first && first.date.getUTCMonth() !== lastMonth) {
      lastMonth = first.date.getUTCMonth();
      monthMarks.push({ wi, label: MONTHS[lastMonth] });
    }
  });

  return { weeks, monthMarks };
}

const CELL = 12;
const GAP = 3;
const COL_W = CELL + GAP;

export function Heatmap({
  days,
  start,
  end,
}: {
  days: HeatmapDay[];
  start: string;
  end: string;
}) {
  const [hover, setHover] = useState<{ cell: Cell; left: number; top: number } | null>(
    null,
  );
  const wrapRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const { weeks, monthMarks } = buildGrid(days, start, end);
  const thresholds = buildHeatThresholds(days);

  // The 53-week grid overflows its column; the newest weeks (where activity
  // lives) are on the right, so start scrolled to the end like GitHub does.
  useEffect(() => {
    const el = scrollRef.current;
    if (el) {
      el.scrollLeft = el.scrollWidth;
    }
  }, [weeks.length]);

  function onEnter(e: React.MouseEvent<HTMLDivElement>, cell: Cell) {
    const wrap = wrapRef.current?.getBoundingClientRect();
    if (!wrap) return;
    const rect = e.currentTarget.getBoundingClientRect();
    setHover({
      cell,
      left: rect.left - wrap.left + rect.width / 2,
      top: rect.top - wrap.top,
    });
  }

  return (
    <div className="relative" ref={wrapRef}>
      <div className="flex gap-1.5">
        <div
          className="flex shrink-0 flex-col pt-[18px]"
          style={{ gap: `${GAP}px` }}
        >
          {["", "Mon", "", "Wed", "", "Fri", ""].map((d, i) => (
            <div
              key={i}
              className="h-3 text-[9px] leading-3 text-muted-foreground"
              style={{ width: 22 }}
            >
              {d}
            </div>
          ))}
        </div>
        <div ref={scrollRef} className="min-w-0 overflow-x-auto pb-1">
          <div className="relative mb-1 h-3" style={{ width: weeks.length * COL_W }}>
            {monthMarks.map((m) => (
              <span
                key={m.wi}
                className="absolute text-[9px] leading-3 text-muted-foreground"
                style={{ left: m.wi * COL_W }}
              >
                {m.label}
              </span>
            ))}
          </div>
          <div className="flex" style={{ gap: `${GAP}px` }}>
            {weeks.map((week, wi) => (
              <div key={wi} className="flex flex-col" style={{ gap: `${GAP}px` }}>
                {Array.from({ length: 7 }).map((_, di) => {
                  const cell = week[di];
                  if (!cell) {
                    return <div key={di} style={{ width: CELL, height: CELL }} />;
                  }
                  return (
                    <div
                      key={di}
                      className={cn(
                        "rounded-sm ring-1 ring-inset ring-black/[0.04] transition-colors dark:ring-white/[0.04]",
                        HEAT_LEVELS[heatLevel(cell.output, thresholds)],
                      )}
                      style={{ width: CELL, height: CELL }}
                      onMouseEnter={(e) => onEnter(e, cell)}
                      onMouseLeave={() => setHover(null)}
                    />
                  );
                })}
              </div>
            ))}
          </div>
        </div>
      </div>
      <div className="mt-2 flex items-center gap-1.5 pl-[30px] text-[10px] text-muted-foreground">
        <span>Less</span>
        {HEAT_LEVELS.map((c, i) => (
          <span key={i} className={cn("h-2.5 w-2.5 rounded-sm", c)} />
        ))}
        <span>More</span>
      </div>
      {hover ? (
        <div
          className="pointer-events-none absolute z-30 -translate-x-1/2 -translate-y-full whitespace-nowrap rounded-md border border-border bg-popover px-2.5 py-1.5 text-xs shadow-lg"
          style={{ left: hover.left, top: hover.top - 6 }}
        >
          <div className="font-medium text-foreground">
            {formatLongDate(hover.cell.iso)}
          </div>
          <div className="mt-0.5 font-mono text-muted-foreground">
            {hover.cell.issues} {hover.cell.issues === 1 ? "issue" : "issues"}
          </div>
          <div className="mt-1 grid grid-cols-[auto_auto] gap-x-4 gap-y-0.5 border-t border-border pt-1 font-mono text-[11px] text-muted-foreground">
            <span>in <Tk value={hover.cell.input} /></span>
            <span>out <Tk value={hover.cell.output} /></span>
            <span>cache-write <Tk value={hover.cell.cacheWrite} /></span>
            <span>cache-read <Tk value={hover.cell.cacheRead} /></span>
          </div>
        </div>
      ) : null}
    </div>
  );
}
