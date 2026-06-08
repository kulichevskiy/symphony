import { useState } from "react";

import { Icon } from "@/components/ui/icon";
import {
  dateTriggerLabel,
  isDefaultDate,
  useFilters,
  type DateFilter as DateFilterValue,
  type DatePreset,
} from "@/lib/filters";
import { cn } from "@/lib/utils";

import { FilterTrigger, Popover } from "./Popover";

const PRESET_OPTIONS: { value: DatePreset; label: string }[] = [
  { value: "12mo", label: "Last 12 months" },
  { value: "90d", label: "Last 90 days" },
  { value: "30d", label: "Last 30 days" },
  { value: "7d", label: "Last 7 days" },
  { value: "yesterday", label: "Yesterday" },
  { value: "today", label: "Today" },
];

const WEEKDAYS = ["S", "M", "T", "W", "T", "F", "S"];

function ymd(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function todayYmd(): string {
  return new Date().toISOString().slice(0, 10);
}

/** A month's days laid out as Sun..Sat weeks, with leading/trailing pad cells. */
export function buildMonthGrid(year: number, month: number): (string | null)[][] {
  const firstDow = new Date(Date.UTC(year, month, 1)).getUTCDay();
  const daysInMonth = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
  const cells: (string | null)[] = [];
  for (let i = 0; i < firstDow; i++) cells.push(null);
  for (let day = 1; day <= daysInMonth; day++) {
    cells.push(ymd(new Date(Date.UTC(year, month, day))));
  }
  while (cells.length % 7 !== 0) cells.push(null);
  const weeks: (string | null)[][] = [];
  for (let i = 0; i < cells.length; i += 7) weeks.push(cells.slice(i, i + 7));
  return weeks;
}

function monthLabel(year: number, month: number): string {
  return new Date(Date.UTC(year, month, 1)).toLocaleDateString("en-US", {
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  });
}

/** A two-click UTC-day range calendar: first click sets the start, second the
 *  end (clicking before the start restarts the range). Apply commits a custom
 *  range to the filter and dismisses the popover. */
function CustomRangePicker({
  value,
  onApply,
}: {
  value: DateFilterValue;
  onApply: (from: string, to: string) => void;
}) {
  const anchor = value.kind === "custom" ? value.from : todayYmd();
  const [year, setYear] = useState(() => Number(anchor.slice(0, 4)));
  const [month, setMonth] = useState(() => Number(anchor.slice(5, 7)) - 1);
  const [from, setFrom] = useState<string | null>(
    value.kind === "custom" ? value.from : null,
  );
  const [to, setTo] = useState<string | null>(
    value.kind === "custom" ? value.to : null,
  );

  function pick(iso: string) {
    if (!from || to) {
      setFrom(iso);
      setTo(null);
    } else if (iso < from) {
      setFrom(iso);
    } else {
      setTo(iso);
    }
  }

  function shiftMonth(delta: number) {
    const d = new Date(Date.UTC(year, month + delta, 1));
    setYear(d.getUTCFullYear());
    setMonth(d.getUTCMonth());
  }

  const weeks = buildMonthGrid(year, month);

  return (
    <div className="w-[15rem] px-1.5 pb-1.5">
      <div className="mb-1.5 flex items-center justify-between">
        <button
          type="button"
          aria-label="Previous month"
          onClick={() => shiftMonth(-1)}
          className="rounded p-1 text-muted-foreground hover:bg-secondary/60 hover:text-foreground"
        >
          <Icon name="chevronRight" size={14} style={{ transform: "rotate(180deg)" }} />
        </button>
        <span className="text-xs font-medium">{monthLabel(year, month)}</span>
        <button
          type="button"
          aria-label="Next month"
          onClick={() => shiftMonth(1)}
          className="rounded p-1 text-muted-foreground hover:bg-secondary/60 hover:text-foreground"
        >
          <Icon name="chevronRight" size={14} />
        </button>
      </div>
      <div className="grid grid-cols-7 gap-0.5 text-center text-[10px] text-muted-foreground">
        {WEEKDAYS.map((d, i) => (
          <span key={i} className="py-0.5">
            {d}
          </span>
        ))}
      </div>
      <div className="mt-0.5 grid grid-cols-7 gap-0.5">
        {weeks.flat().map((iso, i) => {
          if (!iso) return <span key={i} />;
          const inRange = !!from && !!to && iso >= from && iso <= to;
          const isEnd = iso === from || iso === to;
          return (
            <button
              key={i}
              type="button"
              onClick={() => pick(iso)}
              className={cn(
                "h-7 rounded text-center text-xs tabular-nums transition-colors",
                isEnd
                  ? "bg-primary font-medium text-primary-foreground"
                  : inRange
                    ? "bg-primary/15 text-foreground"
                    : "text-foreground hover:bg-secondary/60",
              )}
            >
              {Number(iso.slice(8, 10))}
            </button>
          );
        })}
      </div>
      <div className="mt-2 flex items-center justify-between">
        <span className="font-mono text-[11px] text-muted-foreground">
          {from ? `${from} → ${to ?? "…"}` : "Pick a range"}
        </span>
        <button
          type="button"
          disabled={!from || !to}
          onClick={() => from && to && onApply(from, to)}
          className="rounded-md bg-primary px-2.5 py-1 text-xs font-medium text-primary-foreground transition-opacity disabled:opacity-40"
        >
          Apply
        </button>
      </div>
    </div>
  );
}

/** The Date filter: a preset list plus a custom calendar range, written to the
 *  `dates`/`from`/`to` URL params via the shared filter store. */
export function DateFilter() {
  const { date, setDate } = useFilters();

  return (
    <Popover
      align="start"
      trigger={({ open, toggle }) => (
        <FilterTrigger
          label="Date"
          value={dateTriggerLabel(date)}
          active={!isDefaultDate(date)}
          open={open}
          onClick={toggle}
        />
      )}
    >
      {({ close }) => (
        <div className="w-[15rem]">
          <div className="flex flex-col">
            {PRESET_OPTIONS.map((opt) => {
              const active = date.kind === "preset" && date.preset === opt.value;
              return (
                <button
                  key={opt.value}
                  type="button"
                  onClick={() => {
                    setDate({ kind: "preset", preset: opt.value });
                    close();
                  }}
                  className={cn(
                    "flex items-center justify-between rounded px-2 py-1.5 text-left text-xs transition-colors",
                    active
                      ? "bg-secondary text-foreground"
                      : "text-muted-foreground hover:bg-secondary/60 hover:text-foreground",
                  )}
                >
                  {opt.label}
                  {active ? <Icon name="check" size={13} /> : null}
                </button>
              );
            })}
          </div>
          <div className="my-1.5 border-t border-border" />
          <div className="px-1 pb-0.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            Custom range
          </div>
          <CustomRangePicker
            value={date}
            onApply={(from, to) => {
              setDate({ kind: "custom", from, to });
              close();
            }}
          />
        </div>
      )}
    </Popover>
  );
}
