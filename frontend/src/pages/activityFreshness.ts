export const ACTIVITY_TINT_CLASSES = {
  fresh: "",
  amber: "bg-amber-50/60 hover:bg-amber-50/80",
  orange: "bg-orange-50/65 hover:bg-orange-50/85",
  red: "bg-red-50/70 hover:bg-red-50/90",
} as const;

const HOUR_SECONDS = 60 * 60;
const DAY_SECONDS = 24 * HOUR_SECONDS;

export function activityAgeSeconds(
  activityTs: string | null,
  fallbackAgeSecs: number | null,
  nowMs: number,
): number | null {
  if (activityTs !== null) {
    const activityMs = new Date(activityTs).getTime();
    if (!Number.isNaN(activityMs)) {
      return Math.max(0, Math.floor((nowMs - activityMs) / 1000));
    }
  }
  return fallbackAgeSecs;
}

export function activityTintClass(ageSecs: number | null): string {
  if (ageSecs === null || ageSecs < HOUR_SECONDS) {
    return ACTIVITY_TINT_CLASSES.fresh;
  }
  if (ageSecs < 6 * HOUR_SECONDS) {
    return ACTIVITY_TINT_CLASSES.amber;
  }
  if (ageSecs < DAY_SECONDS) {
    return ACTIVITY_TINT_CLASSES.orange;
  }
  return ACTIVITY_TINT_CLASSES.red;
}

export function formatUtcTimestamp(ts: string): string {
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) {
    return ts;
  }
  return date.toISOString().replace(".000Z", "Z");
}

export function formatRelativeTimestamp(ts: string, nowMs: number): string {
  const activityMs = new Date(ts).getTime();
  if (Number.isNaN(activityMs)) {
    return ts;
  }

  const diffSeconds = Math.round((activityMs - nowMs) / 1000);
  const absSeconds = Math.abs(diffSeconds);
  const units: Array<[number, string]> = [
    [DAY_SECONDS, "d"],
    [HOUR_SECONDS, "h"],
    [60, "m"],
  ];

  let value = absSeconds;
  let unit = "s";
  for (const [unitSeconds, unitLabel] of units) {
    if (absSeconds >= unitSeconds) {
      value = Math.floor(absSeconds / unitSeconds);
      unit = unitLabel;
      break;
    }
  }

  if (value < 10 && unit === "s") {
    return "now";
  }
  return diffSeconds > 0 ? `in ${value}${unit}` : `${value}${unit} ago`;
}
