"""DAO for the `comment_cursors` table.

One row per Linear issue: the RFC3339 timestamp of the most recent comment
we've reacted to. The poll loop's inbound-steering query passes this as the
`after` cursor so we don't re-process old comments after a restart.
"""

from __future__ import annotations

import aiosqlite


async def get(conn: aiosqlite.Connection, issue_id: str) -> str | None:
    cur = await conn.execute(
        "SELECT last_seen_at FROM comment_cursors WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    last_seen: str = row["last_seen_at"]
    return last_seen


async def set(conn: aiosqlite.Connection, issue_id: str, last_seen_at: str) -> None:
    await conn.execute(
        """
        INSERT INTO comment_cursors (issue_id, last_seen_at)
        VALUES (?, ?)
        ON CONFLICT(issue_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
        """,
        (issue_id, last_seen_at),
    )
    await conn.commit()
