import { useEffect, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { CanonicalStatus, CanonicalStatusState, IssueWarning } from "@/lib/api";

const STATE_LABELS: Record<CanonicalStatusState, string> = {
  drift_detected: "drift detected",
  halted: "halted",
  paused: "paused",
  awaiting_merge: "awaiting merge",
  running: "running",
  failed: "failed",
  awaiting_review_trigger: "awaiting review trigger",
  pr_open: "PR open",
  done: "done",
  idle: "idle",
};

const STATE_CLASSES: Record<CanonicalStatusState, string> = {
  drift_detected: "border-red-500 bg-red-100 text-red-950",
  halted: "border-red-300 bg-red-50 text-red-900",
  paused: "border-amber-300 bg-amber-50 text-amber-900",
  awaiting_merge: "border-blue-300 bg-blue-50 text-blue-900",
  running: "border-blue-300 bg-blue-50 text-blue-900",
  failed: "border-red-300 bg-red-50 text-red-900",
  awaiting_review_trigger: "border-violet-300 bg-violet-50 text-violet-900",
  pr_open: "border-cyan-300 bg-cyan-50 text-cyan-900",
  done: "border-green-300 bg-green-50 text-green-900",
  idle: "border-gray-300 bg-gray-50 text-gray-700",
};

function useRelativeClock() {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const interval = window.setInterval(() => setNow(Date.now()), 10000);
    return () => window.clearInterval(interval);
  }, []);

  return now;
}

function formatDuration(seconds: number) {
  const units: Array<[number, string]> = [
    [60 * 60 * 24, "d"],
    [60 * 60, "h"],
    [60, "m"],
  ];

  for (const [unitSeconds, label] of units) {
    if (seconds >= unitSeconds) {
      return `${Math.floor(seconds / unitSeconds)}${label}`;
    }
  }
  return `${Math.max(0, seconds)}s`;
}

function formatUtc(ts: string) {
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) {
    return ts;
  }
  return `${date.toISOString().slice(0, 19)}Z`;
}

function formatRelative(ts: string, now: number) {
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) {
    return ts;
  }

  const diffSeconds = Math.round((date.getTime() - now) / 1000);
  const absSeconds = Math.abs(diffSeconds);
  const units: Array<[number, string]> = [
    [60 * 60 * 24, "d"],
    [60 * 60, "h"],
    [60, "m"],
  ];

  let value = absSeconds;
  let unit = "s";
  for (const [seconds, label] of units) {
    if (absSeconds >= seconds) {
      value = Math.round(absSeconds / seconds);
      unit = label;
      break;
    }
  }

  if (value < 10 && unit === "s") {
    return "now";
  }
  return diffSeconds > 0 ? `in ${value}${unit}` : `${value}${unit} ago`;
}

export function CanonicalStatusBadge({ status }: { status: CanonicalStatus }) {
  return (
    <Badge className={cn("gap-1 capitalize", STATE_CLASSES[status.state])}>
      {status.state === "drift_detected" ? <span aria-hidden="true">⚠</span> : null}
      {STATE_LABELS[status.state]}
    </Badge>
  );
}

export function StuckOverlay({ status }: { status: CanonicalStatus }) {
  if (status.stuck_for === null) {
    return null;
  }

  return (
    <span className="whitespace-nowrap text-xs font-semibold text-red-600">
      ⚠ stuck {formatDuration(status.stuck_for)}
    </span>
  );
}

function NoProgressChip({
  warnings,
  latestActivityAgeSecs,
}: {
  warnings?: IssueWarning[];
  latestActivityAgeSecs?: number | null;
}) {
  if (!warnings?.includes("no_progress")) {
    return null;
  }

  const duration =
    latestActivityAgeSecs === null || latestActivityAgeSecs === undefined
      ? null
      : formatDuration(latestActivityAgeSecs);
  return (
    <span className="whitespace-nowrap rounded border border-amber-300 bg-amber-50 px-1.5 py-0.5 text-xs font-semibold text-amber-900">
      ⏸ no progress{duration ? ` ${duration}` : ""}
    </span>
  );
}

export function StatusCluster({
  status,
  warnings,
  latestActivityAgeSecs,
}: {
  status: CanonicalStatus;
  warnings?: IssueWarning[];
  latestActivityAgeSecs?: number | null;
}) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <CanonicalStatusBadge status={status} />
      <NoProgressChip
        warnings={warnings}
        latestActivityAgeSecs={latestActivityAgeSecs}
      />
      {status.subtitle ? (
        <span className="font-mono text-xs text-muted-foreground">{status.subtitle}</span>
      ) : null}
      <StuckOverlay status={status} />
    </div>
  );
}

export function StatusSinceLine({ status }: { status: CanonicalStatus }) {
  const now = useRelativeClock();

  if (!status.since) {
    return null;
  }

  return (
    <p className="text-sm text-muted-foreground">
      since <span className="font-mono" title={formatRelative(status.since, now)}>{formatUtc(status.since)}</span>
    </p>
  );
}
