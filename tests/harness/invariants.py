"""`assert_consistent(sim, conn)` — the shared drift invariant.

Run at the end of every scenario. It asserts Symphony's SQLite view has not
drifted from the canonical `Sim` reality (and is internally coherent). Passes
trivially on empty state.
"""

from __future__ import annotations

import aiosqlite

from symphony import db
from symphony.db.runs import LIVE_STATUSES

from .sim import Sim


async def _live_run_rows(conn: aiosqlite.Connection) -> list[aiosqlite.Row]:
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"SELECT id, issue_id, status, ended_at FROM runs "
        f"WHERE status IN ({placeholders})",
        LIVE_STATUSES,
    )
    return list(await cur.fetchall())


async def assert_consistent(sim: Sim, conn: aiosqlite.Connection) -> None:
    live = await _live_run_rows(conn)

    # 1. No zombie running runs: a live run must not already have an end time.
    zombies = [row["id"] for row in live if row["ended_at"] is not None]
    assert not zombies, f"zombie running runs (running but ended): {zombies}"

    # 2. One active run per issue.
    by_issue: dict[str, int] = {}
    for row in live:
        by_issue[row["issue_id"]] = by_issue.get(row["issue_id"], 0) + 1
    overloaded = {iid: n for iid, n in by_issue.items() if n > 1}
    assert not overloaded, f"multiple active runs per issue: {overloaded}"

    # 3. No orphan operator_waits: every wait points at an existing run.
    run_ids = {row["id"] for row in await (
        await conn.execute("SELECT id FROM runs")
    ).fetchall()}
    for wait in await db.operator_waits.list_all(conn):
        assert wait.run_id in run_ids, (
            f"orphan operator_wait {wait.kind} for issue {wait.issue_id}: "
            f"run {wait.run_id} does not exist"
        )

    # 4. Linear lane matches Sim PR/merge reality: a merged PR means its issue
    #    is in a completed (Done) lane.
    for pr in sim.prs.values():
        if pr.merged and pr.issue_id:
            issue = sim.issues.get(pr.issue_id)
            if issue is not None:
                assert issue.state_type == "completed", (
                    f"PR #{pr.number} merged but issue {pr.issue_id} lane is "
                    f"{issue.state_name!r} (type {issue.state_type!r}), not Done"
                )

    # 5. issue_prs agrees with Sim: every tracked PR exists in Sim with a
    #    matching merge status.
    cur = await conn.execute(
        "SELECT issue_id, github_repo, pr_number, merged_at FROM issue_prs"
    )
    for row in await cur.fetchall():
        key = (row["github_repo"], row["pr_number"])
        sim_pr = sim.prs.get(key)
        assert sim_pr is not None, (
            f"issue_prs tracks PR {key} that does not exist in Sim"
        )
        db_merged = row["merged_at"] is not None
        assert db_merged == sim_pr.merged, (
            f"issue_prs merge state for PR {key} ({db_merged}) disagrees with "
            f"Sim ({sim_pr.merged})"
        )
