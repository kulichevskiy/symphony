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
    binding_key TEXT NOT NULL DEFAULT '',
    pr_number   INTEGER NOT NULL,
    pr_url      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    merged_at   TEXT,
    PRIMARY KEY (issue_id, github_repo)
);

CREATE INDEX IF NOT EXISTS idx_issue_prs_unmerged
    ON issue_prs(merged_at, created_at);

-- A successful merge-conflict rebase fix-run may produce a clean PR head
-- without a fresh review signal. This marker lets the next merge poll bypass
-- the no-signal verdict for the same PR cycle and reviewed head only, and
-- survives restarts.
CREATE TABLE IF NOT EXISTS merge_conflict_fix_marks (
    issue_id      TEXT NOT NULL REFERENCES issues(id),
    github_repo   TEXT NOT NULL,
    pr_number     INTEGER NOT NULL,
    pr_created_at TEXT NOT NULL,
    head_sha      TEXT NOT NULL,
    marked_at     TEXT NOT NULL,
    PRIMARY KEY (issue_id, github_repo)
);

-- `last_seen_ids` is a JSON array of comment IDs that share `last_seen_at`.
-- Combined with a `gte` filter on the next fetch, this prevents losing
-- comments tied at the boundary timestamp (e.g. bursty creation, pagination
-- splitting a same-millisecond batch) without re-firing already-handled ones.
CREATE TABLE IF NOT EXISTS comment_cursors (
    issue_id      TEXT PRIMARY KEY REFERENCES issues(id),
    last_seen_at  TEXT NOT NULL,
    last_seen_ids TEXT NOT NULL DEFAULT '[]'
);

-- Comment IDs handled by either webhook or poll delivery. This lets webhook
-- delivery order differ from comment creation order without dropping an older
-- slash command merely because the cursor has already moved past it.
CREATE TABLE IF NOT EXISTS comment_events (
    comment_id TEXT PRIMARY KEY,
    issue_id   TEXT NOT NULL REFERENCES issues(id),
    seen_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_comment_events_issue ON comment_events(issue_id);

-- Webhook delivery dedupe. `received_at` is ISO-8601 UTC; old rows are
-- pruned opportunistically before each insert based on the configured TTL.
-- `status` remains pending until the handler succeeds, so retries are not
-- acknowledged as duplicates before their side effects are durable.
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id          TEXT PRIMARY KEY,
    received_at TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'handled'))
);

-- Review-stage state per issue.
--   iteration              fix-runs dispatched so far (capped at 12).
--   last_trigger_signature stable signature of the most recent
--                          review_classifier verdict; used to dedup
--                          consecutive fix-runs against the same trigger.
--   ci_fetch_failures      consecutive `gh pr checks` fetch failures.
--   pr_number/pr_url       active PR under Review.
--   github_repo/issue_label binding selected when Review started.
CREATE TABLE IF NOT EXISTS review_state (
    issue_id               TEXT PRIMARY KEY REFERENCES issues(id),
    iteration              INTEGER NOT NULL DEFAULT 0,
    last_trigger_signature TEXT NOT NULL DEFAULT '',
    ci_fetch_failures      INTEGER NOT NULL DEFAULT 0,
    pr_number              INTEGER,
    pr_url                 TEXT NOT NULL DEFAULT '',
    github_repo            TEXT NOT NULL DEFAULT '',
    issue_label            TEXT NOT NULL DEFAULT '',
    codex_lgtm_comment_id  TEXT NOT NULL DEFAULT ''
);

-- Acceptance-stage state per issue. The current code_only runner records
-- Claude verdicts here; later dev/preview slices can add artifact-backed
-- criteria without changing schema or poll-loop ownership.
CREATE TABLE IF NOT EXISTS acceptance_state (
    issue_id            TEXT PRIMARY KEY REFERENCES issues(id),
    iteration           INTEGER NOT NULL DEFAULT 0,
    pr_number           INTEGER,
    pr_url              TEXT NOT NULL DEFAULT '',
    pr_head_sha         TEXT NOT NULL DEFAULT '',
    mode                TEXT NOT NULL DEFAULT 'off',
    preview_url         TEXT NOT NULL DEFAULT '',
    extracted_criteria  TEXT NOT NULL DEFAULT '',
    last_verdict        TEXT NOT NULL DEFAULT '',
    last_artifacts_url  TEXT NOT NULL DEFAULT '',
    infra_retries       INTEGER NOT NULL DEFAULT 0
);

-- Resurrected Review monitor rows whose opportunistic `@codex review`
-- re-arm was inconclusive and must be retried by the live monitor task.
CREATE TABLE IF NOT EXISTS review_rearm_retries (
    run_id TEXT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE
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

-- Rate-limit/dedupe state for Codex activity comments. The full activity
-- stream is not stored here; raw JSONL remains in per-run log files.
CREATE TABLE IF NOT EXISTS activity_comment_marks (
    run_id                 TEXT PRIMARY KEY REFERENCES runs(id),
    first_unpublished_at   TEXT,
    last_event_at          TEXT,
    event_count_since_post INTEGER NOT NULL DEFAULT 0,
    last_posted_at         TEXT,
    last_fingerprint       TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS activity_command_marks (
    run_id             TEXT NOT NULL REFERENCES runs(id),
    item_id            TEXT NOT NULL,
    last_heartbeat_at  TEXT NOT NULL,
    PRIMARY KEY (run_id, item_id)
);

-- Runs waiting for an explicit operator slash command after the runner has
-- stopped. Cost-cap breaches and manually stopped review monitors use this so
-- resume/reject slash commands remain actionable after an orchestrator restart.
CREATE TABLE IF NOT EXISTS operator_waits (
    issue_id        TEXT PRIMARY KEY REFERENCES issues(id),
    run_id          TEXT NOT NULL REFERENCES runs(id),
    kind            TEXT NOT NULL,
    linear_team_key TEXT NOT NULL,
    github_repo     TEXT NOT NULL,
    issue_label     TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operator_waits_run ON operator_waits(run_id);

-- Low-volume audit trail for state mutations that are otherwise only visible
-- as the latest row value. Used by the UI timeline for race-condition debugging.
CREATE TABLE IF NOT EXISTS state_transitions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id    TEXT NOT NULL REFERENCES issues(id),
    table_name  TEXT NOT NULL,
    field       TEXT NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    ts          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_state_transitions_issue_ts
    ON state_transitions(issue_id, ts);

-- Periodic and webhook-triggered observations of external truth. Observation
-- rows are append-only; when active auto-clear is enabled, the reconciler
-- records the observation and monotonic local mutation in the same transaction.
CREATE TABLE IF NOT EXISTS external_observations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id      TEXT NOT NULL REFERENCES issues(id),
    source        TEXT NOT NULL,
    observed_at   TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    drift_kind    TEXT,
    action_taken  TEXT NOT NULL DEFAULT 'observed'
);

CREATE INDEX IF NOT EXISTS idx_external_observations_issue_ts
    ON external_observations(issue_id, observed_at);

CREATE INDEX IF NOT EXISTS idx_external_observations_drift
    ON external_observations(drift_kind) WHERE drift_kind IS NOT NULL;
