export type CanonicalStatusState =
  | "drift_detected"
  | "halted"
  | "paused"
  | "awaiting_merge"
  | "running"
  | "failed"
  | "awaiting_review_trigger"
  | "pr_open"
  | "done"
  | "idle";

export type CanonicalStatus = {
  state: CanonicalStatusState;
  since: string | null;
  subtitle: string | null;
  stuck_for: number | null;
};

export type IssueScope = "active" | "done";
export type IssueWarning = "no_progress";

export interface IssueSummary {
  id: string;
  identifier: string;
  title: string;
  team_key: string;
  input_tokens: number;
  output_tokens: number;
  cache_write_tokens: number;
  cache_read_tokens: number;
  latest_activity_ts: string | null;
  latest_activity_age_secs: number | null;
  canonical_status: CanonicalStatus;
  warnings?: IssueWarning[];
  completed_at?: string | null;
}

export interface TokenSplit {
  input_tokens: number;
  output_tokens: number;
  cache_write_tokens: number;
  cache_read_tokens: number;
}

export interface SpendTotals extends TokenSplit {
  issues: number;
}

export interface TeamSpend extends TokenSplit {
  key: string;
  issues: number;
}

export interface ModelSpend extends TokenSplit {
  model: string;
  issues: number;
}

export interface ProviderSpend extends TokenSplit {
  provider: string;
  issues: number;
  per_model: ModelSpend[];
}

export interface SpendSummary {
  totals: SpendTotals;
  per_team: TeamSpend[];
  per_provider: ProviderSpend[];
}

export interface HeatmapDay {
  date: string;
  input_tokens: number;
  output_tokens: number;
  cache_write_tokens: number;
  cache_read_tokens: number;
  issues: number;
}

export interface SpendHeatmap {
  days: HeatmapDay[];
  start: string;
  end: string;
}

export interface CommandAccepted {
  status: string;
  command_id: string;
  command: string;
}

export interface TokenModelUsage extends TokenSplit {
  provider: string;
  model: string;
}

export type IssueDetail = {
  issue: {
    id: string;
    identifier: string;
    title: string;
    team_key: string;
  };
  tokens_by_model: TokenModelUsage[];
  canonical_status: CanonicalStatus;
  latest_activity_ts?: string | null;
  latest_activity_age_secs?: number | null;
  warnings?: IssueWarning[];
  external_snapshot?: IssueExternalSnapshot;
  runs: Array<{
    id: string;
    stage: string;
    status: string;
    pid: number | null;
    started_at: string;
    ended_at: string | null;
    input_tokens: number;
    output_tokens: number;
    cache_write_tokens: number;
    cache_read_tokens: number;
    termination_kind: string;
    termination_detail: string;
    exit_returncode: number | null;
  }>;
  issue_prs: Array<{
    github_repo: string;
    binding_key: string;
    pr_number: number;
    pr_url: string;
    created_at: string;
    merged_at: string | null;
  }>;
  operator_waits: Array<{
    run_id: string;
    kind: string;
    linear_team_key: string;
    github_repo: string;
    issue_label: string;
    created_at: string;
  }>;
  review_state: {
    iteration: number;
    last_trigger_signature: string;
    ci_fetch_failures: number;
    pr_number: number | null;
    pr_url: string;
    github_repo: string;
    issue_label: string;
    codex_lgtm_comment_id: string;
  } | null;
  comment_events: Array<{
    comment_id: string;
    seen_at: string;
  }>;
  activity_comment_marks: Array<{
    run_id: string;
    first_unpublished_at: string | null;
    last_event_at: string | null;
    event_count_since_post: number;
    last_posted_at: string | null;
    last_fingerprint: string;
  }>;
};

export type DriftSeverity = "drift" | "warning";

export type DriftFlag = {
  field: string;
  sqlite_value: string | null;
  source_value: string | null;
  source_name: string;
  severity: DriftSeverity;
  flagged_at?: string | null;
};

export type ExternalComment = {
  author: string;
  ts: string;
  body: string;
  comment_id: string | number;
  url: string | null;
  truncated?: boolean;
};

export type LinearSnapshot = {
  state?: string | null;
  updated_at?: string | null;
  comments?: ExternalComment[];
  labels?: string[];
  error?: string;
  stale?: boolean;
  stale_fetched_at?: string;
};

export type GithubPrSnapshot = {
  pr_number?: number | null;
  github_repo?: string | null;
  state?: string | null;
  url?: string | null;
  mergeable?: string | boolean | null;
  merge_state_status?: string | null;
  merged_at?: string | null;
  merged_by?: string | null;
  check_summary?: {
    passing: number;
    failing: number;
    pending: number;
    total: number;
  };
  comments?: ExternalComment[];
  comments_error?: string;
  error?: string;
  stale?: boolean;
  stale_fetched_at?: string;
};

export type IssueExternalSnapshot = {
  fetched_at: string;
  linear: LinearSnapshot;
  github: GithubPrSnapshot;
  drift_flags: DriftFlag[];
};

export type ExternalObservation = {
  id: number;
  issue_id: string;
  source: "linear" | "github" | string;
  observed_at: string;
  payload_json: string;
  drift_kind: string | null;
  action_taken: string;
};

async function fetchJson<T>(
  path: string,
  notFoundMessage: string,
  fallbackMessage: string,
): Promise<T> {
  const response = await fetch(path, {
    headers: { Accept: "application/json" },
  });

  if (!response.ok) {
    throw new Error(response.status === 404 ? notFoundMessage : fallbackMessage);
  }

  return (await response.json()) as T;
}

export function fetchIssues({
  q,
  scope = "active",
  withinSecs,
}: {
  q?: string;
  scope?: IssueScope;
  withinSecs?: number;
} = {}): Promise<IssueSummary[]> {
  const params = new URLSearchParams({ scope });
  const normalizedQ = q?.trim();
  if (normalizedQ) {
    params.set("q", normalizedQ);
  }
  if (withinSecs != null) {
    params.set("within_secs", String(withinSecs));
  }

  return fetchJson<IssueSummary[]>(
    `/api/issues?${params.toString()}`,
    "Issue list not found",
    "Failed to load issues",
  );
}

export function fetchSpendSummary(): Promise<SpendSummary> {
  return fetchJson<SpendSummary>(
    "/api/spend/summary",
    "Spend summary not found",
    "Failed to load spend summary",
  );
}

export function fetchSpendHeatmap(
  days = 371,
  provider?: string,
): Promise<SpendHeatmap> {
  const query = provider
    ? `/api/spend/heatmap?days=${days}&provider=${encodeURIComponent(provider)}`
    : `/api/spend/heatmap?days=${days}`;
  return fetchJson<SpendHeatmap>(
    query,
    "Spend heatmap not found",
    "Failed to load spend heatmap",
  );
}

export async function postIssueCommand(
  id: string,
  command: string,
): Promise<CommandAccepted> {
  const response = await fetch(`/api/issues/${encodeURIComponent(id)}/command`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ command }),
  });
  if (!response.ok) {
    let detail = "Failed to apply command";
    try {
      const body = (await response.json()) as { detail?: string };
      if (body?.detail) {
        detail = body.detail;
      }
    } catch {
      // keep fallback
    }
    throw new Error(detail);
  }
  return (await response.json()) as CommandAccepted;
}

export function fetchIssueDetail(
  id: string,
  { includeExternal = false }: { includeExternal?: boolean } = {},
): Promise<IssueDetail> {
  const params = new URLSearchParams();
  if (includeExternal) {
    params.set("include_external", "1");
  }
  const query = params.toString();
  return fetchJson<IssueDetail>(
    `/api/issues/${encodeURIComponent(id)}${query ? `?${query}` : ""}`,
    "Issue not found",
    "Failed to load issue",
  );
}

export function fetchIssueExternal(
  id: string,
  { refresh = false }: { refresh?: boolean } = {},
): Promise<IssueExternalSnapshot> {
  const params = new URLSearchParams({ refresh: refresh ? "1" : "0" });
  return fetchJson<IssueExternalSnapshot>(
    `/api/issues/${encodeURIComponent(id)}/external?${params.toString()}`,
    "Issue not found",
    "Failed to load external issue state",
  );
}

export function fetchIssueObservations(id: string): Promise<ExternalObservation[]> {
  return fetchJson<ExternalObservation[]>(
    `/api/issues/${encodeURIComponent(id)}/observations`,
    "Issue not found",
    "Failed to load observations",
  );
}
