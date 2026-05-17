"""DAO for webhook delivery idempotency."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

import aiosqlite

ClaimState = Literal["new", "duplicate", "pending"]


async def begin(
    conn: aiosqlite.Connection,
    delivery_id: str,
    *,
    received_at: datetime,
    ttl_secs: int,
) -> ClaimState:
    """Start handling `delivery_id`, returning its current dedupe state."""
    cutoff = received_at - timedelta(seconds=ttl_secs)
    await conn.execute(
        "DELETE FROM webhook_deliveries WHERE received_at < ?",
        (cutoff.isoformat(),),
    )
    cur = await conn.execute(
        """
        INSERT OR IGNORE INTO webhook_deliveries (id, received_at, status)
        VALUES (?, ?, 'pending')
        """,
        (delivery_id, received_at.isoformat()),
    )
    await conn.commit()
    if (cur.rowcount or 0) > 0:
        return "new"

    cur = await conn.execute(
        "SELECT status FROM webhook_deliveries WHERE id = ?",
        (delivery_id,),
    )
    row = await cur.fetchone()
    if row is not None and row[0] == "handled":
        return "duplicate"
    return "pending"


async def finish(conn: aiosqlite.Connection, delivery_id: str) -> None:
    await conn.execute(
        "UPDATE webhook_deliveries SET status = 'handled' WHERE id = ?",
        (delivery_id,),
    )
    await conn.commit()


async def forget(conn: aiosqlite.Connection, delivery_id: str) -> None:
    await conn.execute("DELETE FROM webhook_deliveries WHERE id = ?", (delivery_id,))
    await conn.commit()
