"""DAO for the `sent_notifications` table.

One row per notification event that has already been pushed, keyed by an
opaque `event_key` the caller composes (e.g. `pr_merged:<issue>:<run>`).
`claim` is the dedupe primitive: it returns True exactly once per key, so a
repeated poll that re-derives the same key gets False and stays quiet.
`release` undoes a claim when the send that followed it failed, so a later
poll retries instead of the event staying permanently (and wrongly) claimed.
"""

from __future__ import annotations

import aiosqlite


async def claim(
    conn: aiosqlite.Connection,
    event_key: str,
    now: str,
    *,
    commit: bool = True,
) -> bool:
    """Record `event_key` as sent; return True only the first time."""
    cur = await conn.execute(
        "INSERT OR IGNORE INTO sent_notifications (event_key, sent_at) VALUES (?, ?)",
        (event_key, now),
    )
    inserted = (cur.rowcount or 0) > 0
    if commit:
        await conn.commit()
    return inserted


async def release(
    conn: aiosqlite.Connection,
    event_key: str,
    *,
    commit: bool = True,
) -> None:
    """Undo a `claim` so the event is retried on a later poll."""
    await conn.execute("DELETE FROM sent_notifications WHERE event_key = ?", (event_key,))
    if commit:
        await conn.commit()


__all__ = ["claim", "release"]
