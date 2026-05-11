"""SQLite persistence layer.

Schema lives in `schema.sql` (checked-in, applied idempotently at startup).
Each table has its own DAO module exposing typed read/write functions:

    from symphony import db
    conn = await db.connect(cfg.db_path)
    await db.issues.upsert(conn, ...)
    await db.runs.create(conn, ...)
    await db.comment_cursors.set(conn, ...)
"""

from __future__ import annotations

from . import comment_cursors, comment_events, issues, review_state, runs, webhook_deliveries
from .schema import apply_schema, connect

__all__ = [
    "apply_schema",
    "comment_cursors",
    "comment_events",
    "connect",
    "issues",
    "review_state",
    "runs",
    "webhook_deliveries",
]
