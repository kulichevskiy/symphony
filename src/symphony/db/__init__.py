"""SQLite persistence layer.

Schema lives in versioned `migrations/NNN_*.sql|py` files applied by the
runner in `schema.py` at startup (Config v2 1/9). Each table has its own DAO
module exposing typed read/write functions:

    from symphony import db
    conn = await db.connect(cfg.db_path)
    await db.issues.upsert(conn, ...)
    await db.runs.create(conn, ...)
    await db.comment_cursors.set(conn, ...)
    await db.issue_prs.upsert(conn, ...)
"""

from __future__ import annotations

from . import (
    acceptance_state,
    activity_comments,
    comment_cursors,
    comment_events,
    config_bindings,
    config_globals,
    config_repo_secrets,
    external_observations,
    issue_prs,
    issues,
    notifications,
    oauth_connections,
    operator_waits,
    review_state,
    run_model_usage,
    runs,
    state_transitions,
    tracker_queue,
    webhook_deliveries,
)
from .schema import apply_migrations, connect

__all__ = [
    "acceptance_state",
    "activity_comments",
    "apply_migrations",
    "comment_cursors",
    "comment_events",
    "config_bindings",
    "config_globals",
    "config_repo_secrets",
    "connect",
    "external_observations",
    "issue_prs",
    "issues",
    "notifications",
    "oauth_connections",
    "operator_waits",
    "review_state",
    "run_model_usage",
    "runs",
    "state_transitions",
    "tracker_queue",
    "webhook_deliveries",
]
