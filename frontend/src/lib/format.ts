export { formatTokens } from "./formatTokens";

export function exactInt(value: number | null | undefined): string {
  return String(Math.round(value ?? 0));
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
