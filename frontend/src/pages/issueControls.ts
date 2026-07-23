// Operator command metadata + per-status applicability. Mirrors the daemon's
// SlashKind vocabulary; the web buttons post the same commands that Linear
// `$`-comments do.

export type CommandId =
  | "approve"
  | "reject"
  | "skip-review"
  | "skip-acceptance"
  | "retry-acceptance"
  | "retry"
  | "stop";

export type CommandMeta = {
  label: string;
  cmd: string;
  icon: string;
  group: "review" | "acceptance" | "lifecycle";
  primary?: boolean;
  destructive?: boolean;
};

export const COMMANDS: Record<CommandId, CommandMeta> = {
  approve: { label: "Approve", cmd: "$approve", icon: "thumbsUp", group: "review", primary: true },
  reject: { label: "Reject", cmd: "$reject", icon: "ban", group: "review", destructive: true },
  "skip-review": { label: "Skip review", cmd: "$skip-review", icon: "skip", group: "review" },
  "skip-acceptance": { label: "Skip acceptance", cmd: "$skip-acceptance", icon: "skip", group: "acceptance" },
  "retry-acceptance": { label: "Retry acceptance", cmd: "$retry-acceptance", icon: "rotate", group: "acceptance" },
  retry: { label: "Retry", cmd: "$retry", icon: "rotate", group: "lifecycle" },
  stop: { label: "Stop", cmd: "$stop", icon: "square", group: "lifecycle", destructive: true },
};

/** Display order for the single flat controls row: primary action first,
 *  skips/retries in the middle, destructive last. */
export const COMMAND_ORDER: CommandId[] = [
  "approve", "reject", "skip-review", "skip-acceptance", "retry-acceptance", "retry", "stop",
];

const ALL_CMDS: CommandId[] = COMMAND_ORDER;

export type Applicability = {
  en: Record<CommandId, boolean>;
  why: Record<CommandId, string>;
};

export function applicability(status: string, waitingOn?: string | null): Applicability {
  const en = {} as Record<CommandId, boolean>;
  const why = {} as Record<CommandId, string>;
  for (const c of ALL_CMDS) {
    en[c] = false;
    why[c] = "Not applicable right now";
  }
  const on = (c: CommandId) => {
    en[c] = true;
  };
  const off = (c: CommandId, reason: string) => {
    en[c] = false;
    why[c] = reason;
  };

  switch (status) {
    case "awaiting_review_trigger":
    case "pr_open":
      on("approve");
      on("reject");
      on("skip-review");
      on("stop");
      off("skip-acceptance", "No acceptance run in progress");
      off("retry-acceptance", "Acceptance has not run yet");
      off("retry", "Nothing has failed");
      break;
    case "awaiting_merge":
      on("approve");
      on("stop");
      // A review-cap park (Needs Input) only honors $approve/$reject — the
      // backend routes it through the merge-needs-approval handler, which
      // silently no-ops skip-acceptance/retry-acceptance for this wait kind.
      if (waitingOn === "review_cap") {
        on("reject");
        off("skip-acceptance", "Not supported for a review-cap park");
        off("retry-acceptance", "Not supported for a review-cap park");
      } else {
        off("reject", "Already past review");
        on("skip-acceptance");
        on("retry-acceptance");
      }
      off("skip-review", "Review already complete");
      off("retry", "Nothing has failed");
      break;
    case "running":
      on("stop");
      off("approve", "Nothing to approve — not awaiting review");
      off("reject", "Nothing to reject — not awaiting review");
      off("skip-review", "Not at a review gate");
      off("skip-acceptance", "Not in acceptance");
      off("retry-acceptance", "Acceptance is not running");
      off("retry", "Run is still in progress");
      break;
    case "failed":
      on("retry");
      on("retry-acceptance");
      off("stop", "Run already stopped");
      off("approve", "Nothing to approve — run failed");
      off("reject", "Nothing to reject — run failed");
      off("skip-review", "Not at a review gate");
      off("skip-acceptance", "Acceptance did not start");
      break;
    case "paused":
      on("retry");
      on("stop");
      off("approve", "Paused — resume first");
      off("reject", "Paused — resume first");
      off("skip-review", "Paused — resume first");
      off("skip-acceptance", "Paused — resume first");
      off("retry-acceptance", "Paused — resume first");
      break;
    case "drift_detected":
      on("stop");
      on("retry");
      off("approve", "Resolve drift before approving");
      off("reject", "Resolve drift first");
      off("skip-review", "Resolve drift first");
      off("skip-acceptance", "Resolve drift first");
      off("retry-acceptance", "Resolve drift first");
      break;
    case "halted":
      on("retry");
      off("stop", "Already halted");
      off("approve", "Halted — retry to resume");
      off("reject", "Halted");
      off("skip-review", "Halted");
      off("skip-acceptance", "Halted");
      off("retry-acceptance", "Halted");
      break;
    default: // done, idle
      for (const c of ALL_CMDS) {
        off(c, "No actions available for this issue");
      }
      break;
  }
  return { en, why };
}

export function waitLabel(kind: string): string {
  if (kind === "merge") return "merge approval";
  if (kind.startsWith("acceptance")) return "acceptance sign-off";
  if (kind.startsWith("review")) return "your review";
  if (kind === "implement_failed") return "a failed run decision";
  return kind.replace(/_/g, " ");
}
