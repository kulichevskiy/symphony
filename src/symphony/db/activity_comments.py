"""Publish/rate-limit marks for activity comments.

The raw Codex activity stream stays in per-run log files. These rows only
record what has already been published, which keeps restart/dedupe state small
and avoids duplicating the full event stream in SQLite.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


@dataclass(frozen=True)
class ActivityCommentMark:
    run_id: str
    first_unpublished_at: str | None
    last_event_at: str | None
    event_count_since_post: int
    last_posted_at: str | None
    last_fingerprint: str


def _row_to_mark(row: aiosqlite.Row) -> ActivityCommentMark:
    return ActivityCommentMark(
        run_id=row["run_id"],
        first_unpublished_at=row["first_unpublished_at"],
        last_event_at=row["last_event_at"],
        event_count_since_post=row["event_count_since_post"],
        last_posted_at=row["last_posted_at"],
        last_fingerprint=row["last_fingerprint"],
    )


async def get(conn: aiosqlite.Connection, run_id: str) -> ActivityCommentMark | None:
    cur = await conn.execute(
        """
        SELECT run_id, first_unpublished_at, last_event_at, event_count_since_post,
               last_posted_at, last_fingerprint
        FROM activity_comment_marks
        WHERE run_id = ?
        """,
        (run_id,),
    )
    row = await cur.fetchone()
    return _row_to_mark(row) if row is not None else None


async def record_event(conn: aiosqlite.Connection, *, run_id: str, occurred_at: str) -> None:
    await conn.execute(
        """
        INSERT INTO activity_comment_marks (
            run_id, first_unpublished_at, last_event_at, event_count_since_post
        )
        VALUES (?, ?, ?, 1)
        ON CONFLICT(run_id) DO UPDATE SET
            first_unpublished_at = COALESCE(
                activity_comment_marks.first_unpublished_at,
                excluded.first_unpublished_at
            ),
            last_event_at = excluded.last_event_at,
            event_count_since_post = activity_comment_marks.event_count_since_post + 1
        """,
        (run_id, occurred_at, occurred_at),
    )
    await conn.commit()


async def mark_published(
    conn: aiosqlite.Connection,
    *,
    run_id: str,
    posted_at: str,
    fingerprint: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO activity_comment_marks (
            run_id, first_unpublished_at, last_event_at, event_count_since_post,
            last_posted_at, last_fingerprint
        )
        VALUES (?, NULL, NULL, 0, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            first_unpublished_at = NULL,
            event_count_since_post = 0,
            last_posted_at = excluded.last_posted_at,
            last_fingerprint = excluded.last_fingerprint
        """,
        (run_id, posted_at, fingerprint),
    )
    await conn.commit()


async def last_heartbeat_at(conn: aiosqlite.Connection, *, run_id: str, item_id: str) -> str | None:
    cur = await conn.execute(
        """
        SELECT last_heartbeat_at
        FROM activity_command_marks
        WHERE run_id = ? AND item_id = ?
        """,
        (run_id, item_id),
    )
    row = await cur.fetchone()
    return str(row["last_heartbeat_at"]) if row is not None else None


async def heartbeat_marks(conn: aiosqlite.Connection, *, run_id: str) -> dict[str, str]:
    cur = await conn.execute(
        """
        SELECT item_id, last_heartbeat_at
        FROM activity_command_marks
        WHERE run_id = ?
        """,
        (run_id,),
    )
    rows = await cur.fetchall()
    return {str(row["item_id"]): str(row["last_heartbeat_at"]) for row in rows}


async def mark_heartbeat(
    conn: aiosqlite.Connection,
    *,
    run_id: str,
    item_id: str,
    posted_at: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO activity_command_marks (run_id, item_id, last_heartbeat_at)
        VALUES (?, ?, ?)
        ON CONFLICT(run_id, item_id) DO UPDATE SET
            last_heartbeat_at = excluded.last_heartbeat_at
        """,
        (run_id, item_id, posted_at),
    )
    await conn.commit()
