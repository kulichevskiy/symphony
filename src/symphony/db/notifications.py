"""DAO for the `sent_notifications` table.

One row per notification event that has already been pushed, keyed by an
opaque `event_key` the caller composes (e.g. `pr_merged:<issue>:<run>`).
`claim` is the dedupe primitive: it returns True exactly once per key, so a
repeated poll that re-derives the same key gets False and stays quiet.
Callers that need to retry a failed send should pass `commit=False` and
roll back the connection rather than committing the claim.
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


__all__ = ["claim"]
