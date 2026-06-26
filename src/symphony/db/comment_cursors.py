"""DAO for the `comment_cursors` table.

One row per Linear issue: the RFC3339 timestamp of the most recent comment
we've reacted to plus the IDs of all comments at that exact timestamp. The
poll loop's inbound-steering query passes the timestamp as a `gte` cursor
and locally drops any comment whose ID appears in `last_seen_ids`, so we
neither replay handled comments after a restart nor lose tied-timestamp
comments at a boundary.
"""

from __future__ import annotations

import json
from collections.abc import Iterable

import aiosqlite


async def get(conn: aiosqlite.Connection, issue_id: str) -> tuple[str, list[str]] | None:
    cur = await conn.execute(
        "SELECT last_seen_at, last_seen_ids FROM comment_cursors WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    last_seen_at: str = row["last_seen_at"]
    raw_ids: str = row["last_seen_ids"] or "[]"
    try:
        ids = json.loads(raw_ids)
    except json.JSONDecodeError:
        ids = []
    if not isinstance(ids, list):
        ids = []
    return last_seen_at, [str(x) for x in ids]


async def set(
    conn: aiosqlite.Connection,
    issue_id: str,
    last_seen_at: str,
    last_seen_ids: Iterable[str] = (),
) -> None:
    payload = json.dumps(sorted({str(x) for x in last_seen_ids}))
    await conn.execute(
        """
        INSERT INTO comment_cursors (issue_id, last_seen_at, last_seen_ids)
        VALUES (?, ?, ?)
        ON CONFLICT(issue_id) DO UPDATE SET
            last_seen_at = excluded.last_seen_at,
            last_seen_ids = excluded.last_seen_ids
        """,
        (issue_id, last_seen_at, payload),
    )
    await conn.commit()
