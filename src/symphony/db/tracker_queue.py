"""DAO for the `tracker_queue` table.

A per-(team, binding scope) snapshot of the tracker's dispatch queue: issues
currently in the binding's ready ("Todo") and waiting states. The poll loop
rewrites a scope's rows wholesale on every successful scan, so the table
always mirrors the last scan and never accumulates stale entries. An issue's
`seen_at` survives rewrites while it stays in the same queue, so the UI can
show how long it has really been waiting rather than the last poll time.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


@dataclass(frozen=True)
class QueueRow:
    issue_id: str
    identifier: str
    title: str
    queue: str  # 'ready' | 'waiting'
    state_name: str
    blocked_by: str = ""


async def replace_scan(
    conn: aiosqlite.Connection,
    *,
    team_key: str,
    scope: str,
    rows: list[QueueRow],
    seen_at: str,
) -> None:
    """Replace a binding scope's queue snapshot with the latest scan result."""

    cur = await conn.execute(
        "SELECT issue_id, queue, seen_at FROM tracker_queue WHERE team_key = ? AND scope = ?",
        (team_key, scope),
    )
    first_seen = {
        (str(r["issue_id"]), str(r["queue"])): str(r["seen_at"]) for r in await cur.fetchall()
    }
    await conn.execute(
        "DELETE FROM tracker_queue WHERE team_key = ? AND scope = ?",
        (team_key, scope),
    )
    if rows:
        await conn.executemany(
            """
            INSERT INTO tracker_queue (
                team_key, scope, issue_id, identifier, title, queue,
                state_name, blocked_by, seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    team_key,
                    scope,
                    row.issue_id,
                    row.identifier,
                    row.title,
                    row.queue,
                    row.state_name,
                    row.blocked_by,
                    first_seen.get((row.issue_id, row.queue), seen_at),
                )
                for row in rows
            ],
        )
    await conn.commit()
