"""DAO for the `sent_notifications` / `pending_notifications` tables.

One `sent_notifications` row per notification event that has already been
pushed, keyed by an opaque `event_key` the caller composes (e.g.
`pr_merged:<issue>:<run>`). `claim` is the dedupe primitive: it commits
immediately and returns True exactly once per key, so a repeated poll that
re-derives the same key gets False and stays quiet. Committing right away
(rather than holding the insert open across the outbound HTTP call) keeps
the claim from riding on the same transaction as unrelated writes on a
shared connection.

`pending_notifications` holds the rendered text for a claimed event whose
send failed, so a later poll tick can retry it without re-deriving the
original call site (most of which fire once, on a state transition).
"""

from __future__ import annotations

import aiosqlite


async def claim(conn: aiosqlite.Connection, event_key: str, now: str) -> bool:
    """Record `event_key` as sent; return True only the first time."""
    cur = await conn.execute(
        "INSERT OR IGNORE INTO sent_notifications (event_key, sent_at) VALUES (?, ?)",
        (event_key, now),
    )
    inserted = (cur.rowcount or 0) > 0
    await conn.commit()
    return inserted


async def queue_retry(conn: aiosqlite.Connection, event_key: str, text: str) -> None:
    """Persist a claimed event's rendered text for a later retry."""
    await conn.execute(
        """
        INSERT INTO pending_notifications (event_key, text) VALUES (?, ?)
        ON CONFLICT(event_key) DO UPDATE SET text = excluded.text
        """,
        (event_key, text),
    )
    await conn.commit()


async def list_pending(conn: aiosqlite.Connection) -> list[tuple[str, str]]:
    cur = await conn.execute("SELECT event_key, text FROM pending_notifications")
    rows = await cur.fetchall()
    return [(row["event_key"], row["text"]) for row in rows]


async def clear_pending(conn: aiosqlite.Connection, event_key: str) -> None:
    await conn.execute("DELETE FROM pending_notifications WHERE event_key = ?", (event_key,))
    await conn.commit()


__all__ = ["claim", "clear_pending", "list_pending", "queue_retry"]
