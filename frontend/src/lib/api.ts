export type CanonicalStatusState =
  | "awaiting_operator"
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

export type IssueScope = "active" | "recent" | "all";

export interface IssueSummary {
  id: string;
  identifier: string;
  title: string;
  team_key: string;
  canonical_status: CanonicalStatus;
}

export type IssueDetail = {
  issue: {
    id: string;
    identifier: string;
    title: string;
    team_key: string;
  };
  canonical_status: CanonicalStatus;
  runs: Array<{
    id: string;
    stage: string;
    status: string;
    pid: number | null;
    started_at: string;
    ended_at: string | null;
    cost_usd: number;
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
  issue_cost_marks: {
    warning_posted_at: string | null;
  } | null;
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
}: {
  q?: string;
  scope?: IssueScope;
} = {}): Promise<IssueSummary[]> {
  const params = new URLSearchParams({ scope });
  const normalizedQ = q?.trim();
  if (normalizedQ) {
    params.set("q", normalizedQ);
  }

  return fetchJson<IssueSummary[]>(
    `/api/issues?${params.toString()}`,
    "Issue list not found",
    "Failed to load issues",
  );
}

export function fetchIssueDetail(id: string): Promise<IssueDetail> {
  return fetchJson<IssueDetail>(
    `/api/issues/${encodeURIComponent(id)}`,
    "Issue not found",
    "Failed to load issue",
  );
}
