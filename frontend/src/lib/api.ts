import { authHeaders } from "@/lib/auth";

/** A non-2xx `/api/*` response. Carries the HTTP `status` so callers can
 *  distinguish an allowlist rejection (403) from other failures. */
export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

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

export interface Meta {
  /** Public origin the webhook receiver is reachable at (dev tunnel), or null. */
  tunnel_url?: string | null;
  /** Paste-ready Linear webhook URL (tunnel origin + /linear/webhook), or null. */
  linear_webhook_url?: string | null;
}

/** The daemon's live Auth0 tenant config, as configured by its own
 *  `AUTH0_DOMAIN`/`AUTH0_CLIENT_ID` env vars — the source of truth for
 *  whether/how to run the Auth0 login flow, since those can differ from
 *  whatever was baked into this static bundle at build time. */
export interface AuthConfig {
  enabled: boolean;
  domain?: string;
  client_id?: string;
}

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

export interface StageSpend extends TokenSplit {
  key: string;
  issues: number;
}

/** A provider-qualified model, as surfaced by the always-unscoped `models`
 *  list on /spend/summary to populate the Models filter popover. */
export interface ModelRef {
  provider: string;
  model: string;
}

export interface SpendSummary {
  totals: SpendTotals;
  per_team: TeamSpend[];
  per_provider: ProviderSpend[];
  /** One row per distinct runs.stage in the filtered window; reconciles with
   *  per_team / per_provider. */
  per_stage: StageSpend[];
  /** Always-unscoped team keys from config, for the Teams filter popover. */
  teams: string[];
  /** Always-unscoped (provider, model) pairs, for the Models filter popover. */
  models: ModelRef[];
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

/** One time bucket of the by-stage trend: stage key -> summed output tokens
 *  (non-zero stages only; missing stages are zero). `start` is the bucket's
 *  UTC day — the calendar day for daily buckets, the Monday for weekly ones. */
export interface StageSeriesBucket {
  start: string;
  output_tokens: Record<string, number>;
}

export interface StageSeries {
  buckets: StageSeriesBucket[];
  /** "day" for short windows (≤ ~6 weeks), "week" beyond. */
  bucket: "day" | "week";
  /** Distinct stage keys present in the window (incl. zero-output ones). */
  stages: string[];
  start: string | null;
  end: string | null;
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
    headers: { Accept: "application/json", ...(await authHeaders()) },
  });

  if (!response.ok) {
    throw new ApiError(
      response.status === 404 ? notFoundMessage : fallbackMessage,
      response.status,
    );
  }

  return (await response.json()) as T;
}

/** Join a key list into a comma-separated param; empty → omit. Shared by the
 *  `teams` and (provider-qualified) `models` filters. */
function applyList(params: URLSearchParams, key: string, values?: string[]): void {
  if (values && values.length) {
    params.set(key, values.join(","));
  }
}

export function fetchIssues({
  q,
  scope = "active",
  from,
  to,
  provider,
  teams,
  models,
}: {
  q?: string;
  scope?: IssueScope;
  /** Inclusive UTC-day lower bound (`YYYY-MM-DD`); omitted = open-ended. */
  from?: string;
  /** Inclusive UTC-day upper bound (`YYYY-MM-DD`); omitted = open-ended. */
  to?: string;
  provider?: string;
  teams?: string[];
  models?: string[];
} = {}): Promise<IssueSummary[]> {
  const params = new URLSearchParams({ scope });
  const normalizedQ = q?.trim();
  if (normalizedQ) {
    params.set("q", normalizedQ);
  }
  if (from) {
    params.set("from", from);
  }
  if (to) {
    params.set("to", to);
  }
  if (provider) {
    params.set("provider", provider);
  }
  applyList(params, "teams", teams);
  applyList(params, "models", models);

  return fetchJson<IssueSummary[]>(
    `/api/issues?${params.toString()}`,
    "Issue list not found",
    "Failed to load issues",
  );
}

export function fetchMeta(): Promise<Meta> {
  return fetchJson<Meta>("/api/meta", "Meta not found", "Failed to load meta");
}

/** Daemon-level dispatch kill-switch state. When `paused`, the daemon starts
 *  no new runs; in-flight runs continue. Resets to running on daemon restart. */
export interface PauseState {
  paused: boolean;
}

export function fetchPauseState(): Promise<PauseState> {
  return fetchJson<PauseState>(
    "/api/pause",
    "Pause state not found",
    "Failed to load pause state",
  );
}

export async function setPauseState(paused: boolean): Promise<PauseState> {
  const response = await fetch("/api/pause", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      ...(await authHeaders()),
    },
    body: JSON.stringify({ paused }),
  });
  if (!response.ok) {
    throw new ApiError("Failed to update pause state", response.status);
  }
  return (await response.json()) as PauseState;
}

/** One resolved pipeline role in the read-only config view. */
export interface RoleView {
  agent: string;
  model?: string | null;
  effort?: string | null;
}

/** One tracker-project ↔ GitHub-repo binding (non-sensitive fields only). */
export interface BindingView {
  provider: string;
  project_key: string;
  github_repo: string;
  max_concurrent: number;
  roles: Record<string, RoleView>;
}

/** The effective loaded daemon config, redacted for read-only display. */
export interface ConfigView {
  read_only: boolean;
  global_max_concurrent: number;
  poll_interval_secs: number;
  bindings: BindingView[];
}

export function fetchConfigView(): Promise<ConfigView> {
  return fetchJson<ConfigView>(
    "/api/config",
    "Config not found",
    "Failed to load config",
  );
}

export function fetchAuthConfig(): Promise<AuthConfig> {
  return fetchJson<AuthConfig>(
    "/api/auth-config",
    "Auth config not found",
    "Failed to load auth config",
  );
}

export function fetchSpendSummary(
  provider?: string,
  teams?: string[],
  models?: string[],
  from?: string,
  to?: string,
): Promise<SpendSummary> {
  const params = new URLSearchParams();
  if (provider) {
    params.set("provider", provider);
  }
  applyList(params, "teams", teams);
  applyList(params, "models", models);
  if (from) {
    params.set("from", from);
  }
  if (to) {
    params.set("to", to);
  }
  const query = params.toString();
  return fetchJson<SpendSummary>(
    query ? `/api/spend/summary?${query}` : "/api/spend/summary",
    "Spend summary not found",
    "Failed to load spend summary",
  );
}

export function fetchSpendHeatmap(
  days = 371,
  provider?: string,
  teams?: string[],
  models?: string[],
): Promise<SpendHeatmap> {
  const params = new URLSearchParams({ days: String(days) });
  if (provider) {
    params.set("provider", provider);
  }
  applyList(params, "teams", teams);
  applyList(params, "models", models);
  return fetchJson<SpendHeatmap>(
    `/api/spend/heatmap?${params.toString()}`,
    "Spend heatmap not found",
    "Failed to load spend heatmap",
  );
}

/** The trend series: output-token time buckets grouped by `by` (stage / team /
 *  model). Window + bucket granularity follow the active date filter (all
 *  history when unfiltered). `series.stages` carries the dimension's keys. */
export function fetchSpendStageSeries(
  by: "stage" | "team" | "model",
  provider?: string,
  teams?: string[],
  models?: string[],
  from?: string,
  to?: string,
): Promise<StageSeries> {
  const params = new URLSearchParams();
  if (by !== "stage") {
    params.set("by", by);
  }
  if (provider) {
    params.set("provider", provider);
  }
  applyList(params, "teams", teams);
  applyList(params, "models", models);
  if (from) {
    params.set("from", from);
  }
  if (to) {
    params.set("to", to);
  }
  const query = params.toString();
  return fetchJson<StageSeries>(
    query ? `/api/spend/stage-series?${query}` : "/api/spend/stage-series",
    "Stage series not found",
    "Failed to load stage series",
  );
}

export async function postIssueCommand(
  id: string,
  command: string,
): Promise<CommandAccepted> {
  const response = await fetch(`/api/issues/${encodeURIComponent(id)}/command`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      ...(await authHeaders()),
    },
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
