"""DAO for Linear comment idempotency across webhook and poll delivery."""

from __future__ import annotations

import aiosqlite


async def seen(conn: aiosqlite.Connection, comment_id: str) -> bool:
    cur = await conn.execute(
        "SELECT 1 FROM comment_events WHERE comment_id = ? LIMIT 1",
        (comment_id,),
    )
    return await cur.fetchone() is not None


async def claim(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    comment_id: str,
    seen_at: str,
) -> bool:
    cur = await conn.execute(
        """
        INSERT OR IGNORE INTO comment_events (comment_id, issue_id, seen_at)
        VALUES (?, ?, ?)
        """,
        (comment_id, issue_id, seen_at),
    )
    await conn.commit()
    return (cur.rowcount or 0) > 0


async def forget(conn: aiosqlite.Connection, comment_id: str) -> None:
    await conn.execute("DELETE FROM comment_events WHERE comment_id = ?", (comment_id,))
    await conn.commit()
