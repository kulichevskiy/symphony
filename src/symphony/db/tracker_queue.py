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


async def prune_scopes(
    conn: aiosqlite.Connection,
    *,
    keep: list[tuple[str, str]],
) -> None:
    """Drop rows from binding scopes that are no longer configured.

    `replace_scan` only rewrites scopes that still get scanned; a removed or
    renamed binding (repo, label, provider, site) would otherwise leave its
    queue rows visible forever. Called once at daemon startup.
    """
    if not keep:
        await conn.execute("DELETE FROM tracker_queue")
    else:
        conds = " OR ".join("(team_key = ? AND scope = ?)" for _ in keep)
        await conn.execute(
            f"DELETE FROM tracker_queue WHERE NOT ({conds})",
            tuple(value for pair in keep for value in pair),
        )
    await conn.commit()


async def mark_waiting(
    conn: aiosqlite.Connection,
    *,
    team_key: str,
    scope: str,
    issue_id: str,
    state_name: str,
    blocked_by: str,
    seen_at: str,
) -> None:
    """Reflect a same-tick park to Waiting without waiting for the next scan."""

    await conn.execute(
        """
        UPDATE tracker_queue
           SET queue = 'waiting', state_name = ?, blocked_by = ?, seen_at = ?
         WHERE team_key = ? AND scope = ? AND issue_id = ? AND queue != 'waiting'
        """,
        (state_name, blocked_by, seen_at, team_key, scope, issue_id),
    )
    await conn.commit()


async def remove(
    conn: aiosqlite.Connection,
    *,
    team_key: str,
    scope: str,
    issue_id: str,
) -> None:
    """Drop an issue that a guard just moved out of the queue lanes entirely."""

    await conn.execute(
        "DELETE FROM tracker_queue WHERE team_key = ? AND scope = ? AND issue_id = ?",
        (team_key, scope, issue_id),
    )
    await conn.commit()


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
