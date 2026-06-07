"""SQLite persistence layer.

Schema lives in `schema.sql` (checked-in, applied idempotently at startup).
Each table has its own DAO module exposing typed read/write functions:

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
    external_observations,
    issue_prs,
    issues,
    operator_waits,
    review_state,
    runs,
    state_transitions,
    webhook_deliveries,
)
from .schema import apply_schema, connect

__all__ = [
    "acceptance_state",
    "activity_comments",
    "apply_schema",
    "comment_cursors",
    "comment_events",
    "connect",
    "external_observations",
    "issue_prs",
    "issues",
    "operator_waits",
    "review_state",
    "runs",
    "state_transitions",
    "webhook_deliveries",
]
