export { formatTokens } from "./formatTokens";

export function exactInt(value: number | null | undefined): string {
  return String(Math.round(value ?? 0));
}

/**
 * Weighted "effective" token total — the single unit the per-issue token
 * budget gates on (SYM-130). Cache writes cost more than fresh input, cache
 * reads far less. Mirrors the backend helper in `src/symphony/tokens.py`;
 * keep the weights (1.25 / 0.1) in sync.
 */
export function effectiveTokens(t: {
  input_tokens: number;
  output_tokens: number;
  cache_write_tokens: number;
  cache_read_tokens: number;
}): number {
  return (
    t.input_tokens +
    t.output_tokens +
    t.cache_write_tokens * 1.25 +
    t.cache_read_tokens * 0.1
  );
}

export function formatUtc(ts: string | null | undefined): string {
  if (!ts) {
    return "null";
  }
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) {
    return String(ts);
  }
  return `${d.toISOString().slice(0, 19)}Z`;
}

export function formatRelative(
  ts: string | null | undefined,
  nowMs?: number,
): string {
  if (!ts) {
    return "unknown";
  }
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) {
    return String(ts);
  }
  const now = nowMs ?? Date.now();
  const diff = Math.round((now - d.getTime()) / 1000);
  const abs = Math.abs(diff);
  const units: Array<[number, string]> = [
    [86400, "d"],
    [3600, "h"],
    [60, "m"],
  ];
  let value = abs;
  let unit = "s";
  for (const [secs, label] of units) {
    if (abs >= secs) {
      value = Math.floor(abs / secs);
      unit = label;
      break;
    }
  }
  if (value < 10 && unit === "s") {
    return "now";
  }
  return diff < 0 ? `in ${value}${unit}` : `${value}${unit} ago`;
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}

/** Browser-local calendar day of a receipt time, for spotting day boundaries
 *  in the activity feed. `null` when there is no usable timestamp. */
export function localDay(ts: string | null | undefined): string | null {
  if (!ts) {
    return null;
  }
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) {
    return null;
  }
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

/** Browser-local `HH:mm:ss` for an activity event, prefixed with the local
 *  date when `withDate` (a day boundary in the feed). Renders `—` for legacy
 *  log lines with no recoverable receipt time rather than inventing one. */
export function formatEventTime(
  ts: string | null | undefined,
  withDate = false,
): string {
  const day = localDay(ts);
  if (!ts || day === null) {
    return "—";
  }
  const d = new Date(ts);
  const time = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  return withDate ? `${day} ${time}` : time;
}

export function formatLongDate(ts: string): string {
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) {
    return String(ts);
  }
  return d.toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  });
}
