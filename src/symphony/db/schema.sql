-- Symphony persistence schema. Applied at startup; safe to re-apply.
--
-- Status values used in `runs.status`:
--   running      live (subprocess attached or dispatched)
--   completed    finished cleanly
--   done         terminal pipeline success
--   needs_approval terminal operator handoff after an unrecoverable stage failure
--   failed       finished with non-zero exit / spawn failure
--   interrupted  marked dead by startup reconcile (host restarted)

CREATE TABLE IF NOT EXISTS repos (
    linear_team_key TEXT PRIMARY KEY,
    github_repo     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS issues (
    id          TEXT PRIMARY KEY,
    identifier  TEXT NOT NULL,
    title       TEXT NOT NULL,
    team_key    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id          TEXT PRIMARY KEY,
    issue_id    TEXT NOT NULL REFERENCES issues(id),
    stage       TEXT NOT NULL,
    status      TEXT NOT NULL,
    pid         INTEGER,
    started_at  TEXT NOT NULL,
    ended_at    TEXT,
    cost_usd    REAL NOT NULL DEFAULT 0
);

-- Active-run lookup: dedupe in poll (status='running') and reconcile
-- (status='running' AND pid IS NOT NULL).
CREATE INDEX IF NOT EXISTS idx_runs_status_pid ON runs(status, pid);

-- Per-issue cost aggregation (cost_cap_per_issue_usd enforcement).
CREATE INDEX IF NOT EXISTS idx_runs_issue_cost ON runs(issue_id, cost_usd);

-- PR opened for an issue. The row bridges the async Review/Merge ticks:
-- Implement creates the PR and Review handoff, later ticks poll the same PR
-- until Review + CI are green, then Merge marks it merged.
CREATE TABLE IF NOT EXISTS issue_prs (
    issue_id    TEXT NOT NULL REFERENCES issues(id),
    github_repo TEXT NOT NULL,
    pr_number   INTEGER NOT NULL,
    pr_url      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    merged_at   TEXT,
    PRIMARY KEY (issue_id, github_repo)
);

CREATE INDEX IF NOT EXISTS idx_issue_prs_unmerged
    ON issue_prs(merged_at, created_at);

-- `last_seen_ids` is a JSON array of comment IDs that share `last_seen_at`.
-- Combined with a `gte` filter on the next fetch, this prevents losing
-- comments tied at the boundary timestamp (e.g. bursty creation, pagination
-- splitting a same-millisecond batch) without re-firing already-handled ones.
CREATE TABLE IF NOT EXISTS comment_cursors (
    issue_id      TEXT PRIMARY KEY REFERENCES issues(id),
    last_seen_at  TEXT NOT NULL,
    last_seen_ids TEXT NOT NULL DEFAULT '[]'
);

-- Review-stage state per issue.
--   iteration              fix-runs dispatched so far (capped at 12).
--   last_trigger_signature stable signature of the most recent
--                          review_classifier verdict; used to dedup
--                          consecutive fix-runs against the same trigger.
CREATE TABLE IF NOT EXISTS review_state (
    issue_id               TEXT PRIMARY KEY REFERENCES issues(id),
    iteration              INTEGER NOT NULL DEFAULT 0,
    last_trigger_signature TEXT NOT NULL DEFAULT ''
);
