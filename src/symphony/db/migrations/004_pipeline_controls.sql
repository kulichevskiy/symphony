-- 004_pipeline_controls: durable issue-level pipeline control state (SYM-244).
--
-- One row per issue holds what the operator surface reads: the pipeline mode,
-- the stage the pipeline sits on, that stage's latest attempt outcome, and the
-- diagnostic reason behind it. `reason` is data only — the actions an operator
-- may take are derived from mode + outcome, so a new failure string can never
-- reach a new command handler.
CREATE TABLE pipeline_controls (
    issue_id   TEXT PRIMARY KEY REFERENCES issues(id),
    mode       TEXT NOT NULL,
    stage      TEXT,
    outcome    TEXT NOT NULL,
    reason     TEXT,
    run_id     TEXT,
    actor      TEXT,
    updated_at TEXT NOT NULL
);

-- Every *accepted* action, written in the same transaction as the row above so
-- a restart can never find a dispatched action that left no record (or a state
-- change no action explains). `action_id` is the ingress's own identity for the
-- request — the tracker comment id for a `$retry`, the queued command id for a
-- web button — which makes the primary key the idempotency key: a replayed
-- command inserts nothing and dispatches nothing.
CREATE TABLE pipeline_control_actions (
    issue_id     TEXT NOT NULL REFERENCES issues(id),
    action_id    TEXT NOT NULL,
    action       TEXT NOT NULL,
    actor        TEXT NOT NULL,
    from_mode    TEXT NOT NULL,
    to_mode      TEXT NOT NULL,
    from_outcome TEXT NOT NULL,
    to_outcome   TEXT NOT NULL,
    stage        TEXT,
    run_id       TEXT,
    ts           TEXT NOT NULL,
    PRIMARY KEY (issue_id, action_id)
);

CREATE INDEX idx_pipeline_control_actions_issue_ts
    ON pipeline_control_actions(issue_id, ts);
