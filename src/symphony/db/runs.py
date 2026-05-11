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


async def create_if_no_active(
    conn: aiosqlite.Connection,
    *,
    id: str,
    issue_id: str,
    stage: str,
    status: str,
    pid: int | None,
    started_at: str,
    cost_usd: float = 0.0,
) -> bool:
    """Atomic dedupe: insert iff no `LIVE_STATUSES` row exists for `issue_id`.

    Closes the TOCTOU window between `has_active` and `create`: a separate
    `has_active` check followed by an unconditional INSERT lets two callers
    (poll loop and `dispatch` CLI, or two manual dispatches) both observe
    "no active run" and both write a `running` row. Doing it in one
    statement makes SQLite's write-side serialization carry the guarantee.

    Returns True if the row was inserted, False if a live run already
    existed and the insert was skipped.
    """
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, cost_usd)
        SELECT ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM runs WHERE issue_id = ? AND status IN ({placeholders})
        )
        """,
        (id, issue_id, stage, status, pid, started_at, cost_usd, issue_id, *LIVE_STATUSES),
    )
    await conn.commit()
    return (cur.rowcount or 0) > 0


async def create_if_not_dispatched(
    conn: aiosqlite.Connection,
    *,
    id: str,
    issue_id: str,
    stage: str,
    status: str,
    pid: int | None,
    started_at: str,
    cost_usd: float = 0.0,
) -> bool:
    """Atomic dispatch dedupe: insert iff no live run exists."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, cost_usd)
        SELECT ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM runs WHERE issue_id = ? AND status IN ({placeholders})
        )
        """,
        (
            id,
            issue_id,
            stage,
            status,
            pid,
            started_at,
            cost_usd,
            issue_id,
            *LIVE_STATUSES,
        ),
    )
    await conn.commit()
    return (cur.rowcount or 0) > 0


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


async def update_pid(conn: aiosqlite.Connection, run_id: str, pid: int) -> None:
    await conn.execute("UPDATE runs SET pid = ? WHERE id = ?", (pid, run_id))
    await conn.commit()


async def has_active(conn: aiosqlite.Connection, issue_id: str) -> bool:
    """True if `issue_id` has any run in a live status."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"SELECT 1 FROM runs WHERE issue_id = ? AND status IN ({placeholders}) LIMIT 1",
        (issue_id, *LIVE_STATUSES),
    )
    row = await cur.fetchone()
    return row is not None


async def has_running_or_completed(conn: aiosqlite.Connection, issue_id: str) -> bool:
    """Dedupe oracle for the poll loop.

    Historical name retained for compatibility, but dispatch dedupe is
    intentionally live-only: completed runs must not block legitimate reruns.
    """
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


@dataclass
class RunWithIssue:
    run: Run
    identifier: str


def _row_to_run_with_issue(row: aiosqlite.Row) -> RunWithIssue:
    return RunWithIssue(run=_row_to_run(row), identifier=row["identifier"])


async def list_recent(
    conn: aiosqlite.Connection, *, limit: int = 50
) -> list[RunWithIssue]:
    """Inspection-side view: every live run + the most recent terminated
    runs up to `limit`, joined with their issue identifier, ordered by
    `started_at` DESC.

    A naive `ORDER BY started_at DESC LIMIT ?` over all rows hides a
    long-running live run if the newest `limit` terminated runs started
    after it — exactly the wrong thing during incident triage. So we
    always return every `LIVE_STATUSES` row regardless of `limit`, and
    use the limit only for the terminated tail.
    """
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT r.id, r.issue_id, r.stage, r.status, r.pid, r.started_at,
               r.ended_at, r.cost_usd, i.identifier
        FROM runs r
        JOIN issues i ON i.id = r.issue_id
        WHERE r.status IN ({placeholders})
           OR r.id IN (
                SELECT id FROM runs
                WHERE status NOT IN ({placeholders})
                ORDER BY started_at DESC
                LIMIT ?
           )
        ORDER BY r.started_at DESC
        """,
        (*LIVE_STATUSES, *LIVE_STATUSES, limit),
    )
    rows = await cur.fetchall()
    return [_row_to_run_with_issue(r) for r in rows]


async def get_with_issue(
    conn: aiosqlite.Connection, run_id: str
) -> RunWithIssue | None:
    cur = await conn.execute(
        """
        SELECT r.id, r.issue_id, r.stage, r.status, r.pid, r.started_at,
               r.ended_at, r.cost_usd, i.identifier
        FROM runs r
        JOIN issues i ON i.id = r.issue_id
        WHERE r.id = ?
        """,
        (run_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_run_with_issue(row)


async def history_for_issue(
    conn: aiosqlite.Connection, issue_id: str
) -> list[Run]:
    """Stage history: all runs for an issue, oldest first."""
    cur = await conn.execute(
        """
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd
        FROM runs
        WHERE issue_id = ?
        ORDER BY started_at ASC
        """,
        (issue_id,),
    )
    rows = await cur.fetchall()
    return [_row_to_run(r) for r in rows]


async def latest_for_issue_stage(
    conn: aiosqlite.Connection, *, issue_id: str, stage: str
) -> Run | None:
    cur = await conn.execute(
        """
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd
        FROM runs
        WHERE issue_id = ? AND stage = ?
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (issue_id, stage),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_run(row)


async def cost_for_issue(conn: aiosqlite.Connection, issue_id: str) -> float:
    cur = await conn.execute(
        "SELECT COALESCE(SUM(cost_usd), 0.0) FROM runs WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return 0.0
    return float(row[0])
