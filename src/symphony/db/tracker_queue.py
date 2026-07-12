"""DAO for the `tracker_queue` table.

A per-team snapshot of the tracker's dispatch queue: issues currently in the
binding's ready ("Todo") and waiting states. The poll loop rewrites a team's
rows wholesale on every successful scan, so the table always mirrors the last
scan and never accumulates stale entries.
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


async def replace_team_scan(
    conn: aiosqlite.Connection,
    *,
    team_key: str,
    rows: list[QueueRow],
    seen_at: str,
) -> None:
    """Replace a team's queue snapshot with the latest scan result."""

    await conn.execute("DELETE FROM tracker_queue WHERE team_key = ?", (team_key,))
    if rows:
        await conn.executemany(
            """
            INSERT INTO tracker_queue (
                team_key, issue_id, identifier, title, queue,
                state_name, blocked_by, seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    team_key,
                    row.issue_id,
                    row.identifier,
                    row.title,
                    row.queue,
                    row.state_name,
                    row.blocked_by,
                    seen_at,
                )
                for row in rows
            ],
        )
    await conn.commit()
