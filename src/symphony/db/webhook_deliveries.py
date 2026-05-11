"""DAO for Linear webhook delivery idempotency."""

from __future__ import annotations

from datetime import datetime, timedelta

import aiosqlite


async def claim(
    conn: aiosqlite.Connection,
    delivery_id: str,
    *,
    received_at: datetime,
    ttl_secs: int,
) -> bool:
    """Return True iff `delivery_id` was not seen within the TTL window."""
    cutoff = received_at - timedelta(seconds=ttl_secs)
    await conn.execute(
        "DELETE FROM webhook_deliveries WHERE received_at < ?",
        (cutoff.isoformat(),),
    )
    cur = await conn.execute(
        """
        INSERT OR IGNORE INTO webhook_deliveries (id, received_at)
        VALUES (?, ?)
        """,
        (delivery_id, received_at.isoformat()),
    )
    await conn.commit()
    return (cur.rowcount or 0) > 0


async def forget(conn: aiosqlite.Connection, delivery_id: str) -> None:
    await conn.execute("DELETE FROM webhook_deliveries WHERE id = ?", (delivery_id,))
    await conn.commit()
