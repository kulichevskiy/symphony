-- Symphony persistence schema. Applied at startup; safe to re-apply.
--
-- Status values used in `runs.status`:
--   running      live (subprocess attached or dispatched)
--   completed    finished cleanly
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

CREATE TABLE IF NOT EXISTS comment_cursors (
    issue_id     TEXT PRIMARY KEY REFERENCES issues(id),
    last_seen_at TEXT NOT NULL
);

-- Per-issue cost-warning idempotency. The cost warning template fires
-- exactly once per issue — when the cumulative cost first crosses the
-- configured threshold. Persisting the post timestamp lets a restarted
-- orchestrator skip the warning even if cumulative cost is already past
-- threshold from prior runs.
CREATE TABLE IF NOT EXISTS issue_cost_marks (
    issue_id            TEXT PRIMARY KEY REFERENCES issues(id),
    warning_posted_at   TEXT
);
