import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export const STATE_LABELS: Record<string, string> = {
  drift_detected: "drift detected",
  halted: "halted",
  paused: "paused",
  awaiting_merge: "awaiting merge",
  running: "running",
  failed: "failed",
  awaiting_review_trigger: "awaiting review",
  pr_open: "PR open",
  done: "done",
  idle: "idle",
  todo: "todo",
  waiting: "waiting",
};

export const STATE_CLASSES: Record<string, string> = {
  drift_detected:
    "border-red-500 bg-red-100 text-red-950 dark:border-red-500 dark:bg-red-950/60 dark:text-red-100",
  halted:
    "border-red-300 bg-red-50 text-red-900 dark:border-red-700 dark:bg-red-950/40 dark:text-red-200",
  paused:
    "border-amber-300 bg-amber-50 text-amber-900 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-200",
  awaiting_merge:
    "border-blue-300 bg-blue-50 text-blue-900 dark:border-blue-700 dark:bg-blue-950/40 dark:text-blue-200",
  running:
    "border-blue-300 bg-blue-50 text-blue-900 dark:border-blue-700 dark:bg-blue-950/40 dark:text-blue-200",
  failed:
    "border-red-300 bg-red-50 text-red-900 dark:border-red-700 dark:bg-red-950/40 dark:text-red-200",
  awaiting_review_trigger:
    "border-violet-300 bg-violet-50 text-violet-900 dark:border-violet-700 dark:bg-violet-950/40 dark:text-violet-200",
  pr_open:
    "border-cyan-300 bg-cyan-50 text-cyan-900 dark:border-cyan-700 dark:bg-cyan-950/40 dark:text-cyan-200",
  done: "border-green-300 bg-green-50 text-green-900 dark:border-green-700 dark:bg-green-950/40 dark:text-green-200",
  idle: "border-slate-300 bg-slate-50 text-slate-700 dark:border-slate-700 dark:bg-slate-800/60 dark:text-slate-300",
  todo: "border-slate-300 bg-slate-50 text-slate-700 dark:border-slate-700 dark:bg-slate-800/60 dark:text-slate-300",
  waiting:
    "border-slate-300 bg-slate-100 text-slate-600 dark:border-slate-700 dark:bg-slate-800/40 dark:text-slate-400",
};

export function LiveDot({ tone = "current" }: { tone?: string }) {
  const dot = tone === "current" ? "bg-current" : tone;
  return (
    <span className="relative flex h-2 w-2">
      <span
        className={cn(
          "absolute inline-flex h-full w-full animate-ping rounded-full opacity-60",
          dot,
        )}
      />
      <span className={cn("relative inline-flex h-2 w-2 rounded-full", dot)} />
    </span>
  );
}

export function StatusBadge({
  status,
  live = false,
}: {
  status: string;
  live?: boolean;
}) {
  return (
    <Badge
      className={cn("gap-1.5 capitalize", STATE_CLASSES[status] ?? STATE_CLASSES.idle)}
    >
      {live && status === "running" ? <LiveDot /> : null}
      {status === "drift_detected" ? <span aria-hidden="true">⚠</span> : null}
      {STATE_LABELS[status] ?? status}
    </Badge>
  );
}
