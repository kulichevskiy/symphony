"""DAO for the `runs` table.

A run row is created when the orchestrator hands an issue to a runner. It
moves through `running` → `completed` | `failed` | `interrupted`. The
startup reconcile walks `running` rows with non-null PIDs and flips dead
ones to `interrupted`.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite

# Statuses that mean "this run is supposed to be alive". Kept as a tuple so
# callers (poll dedupe, reconcile) share a single source of truth.
LIVE_STATUSES: tuple[str, ...] = ("running",)


@dataclass
class Run:
    id: str
    issue_id: str
    stage: str
    status: str
    pid: int | None
    started_at: str
    ended_at: str | None
    cost_usd: float


def _row_to_run(row: aiosqlite.Row) -> Run:
    return Run(
        id=row["id"],
        issue_id=row["issue_id"],
        stage=row["stage"],
        status=row["status"],
        pid=row["pid"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        cost_usd=row["cost_usd"],
    )


async def create(
    conn: aiosqlite.Connection,
    *,
    id: str,
    issue_id: str,
    stage: str,
    status: str,
    pid: int | None,
    started_at: str,
    cost_usd: float = 0.0,
) -> None:
    await conn.execute(
        """
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, cost_usd)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (id, issue_id, stage, status, pid, started_at, cost_usd),
    )
    await conn.commit()


async def update_status(
    conn: aiosqlite.Connection,
    run_id: str,
    status: str,
    *,
    ended_at: str | None = None,
) -> None:
    await conn.execute(
        "UPDATE runs SET status = ?, ended_at = COALESCE(?, ended_at) WHERE id = ?",
        (status, ended_at, run_id),
    )
    await conn.commit()


async def has_active(conn: aiosqlite.Connection, issue_id: str) -> bool:
    """True if `issue_id` has any run in a live status. Replaces the
    in-memory `_dispatched` dict as the dedupe oracle for the poll loop."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"SELECT 1 FROM runs WHERE issue_id = ? AND status IN ({placeholders}) LIMIT 1",
        (issue_id, *LIVE_STATUSES),
    )
    row = await cur.fetchone()
    return row is not None


async def list_live_with_pid(conn: aiosqlite.Connection) -> list[Run]:
    """Live runs with a non-null PID — the input set for startup reconcile."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd
        FROM runs
        WHERE status IN ({placeholders}) AND pid IS NOT NULL
        """,
        LIVE_STATUSES,
    )
    rows = await cur.fetchall()
    return [_row_to_run(r) for r in rows]


async def cost_for_issue(conn: aiosqlite.Connection, issue_id: str) -> float:
    cur = await conn.execute(
        "SELECT COALESCE(SUM(cost_usd), 0.0) FROM runs WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return 0.0
    return float(row[0])
