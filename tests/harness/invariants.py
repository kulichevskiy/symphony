"""`assert_consistent(sim, conn)` — the shared drift invariant.

Run at the end of every scenario. It asserts Symphony's SQLite view has not
drifted from the canonical `Sim` reality (and is internally coherent). Passes
trivially on empty state.
"""

from __future__ import annotations

import aiosqlite

from symphony import db
from symphony.db.runs import LIVE_STATUSES

from .sim import PR_CLOSED, Sim


async def _live_run_rows(conn: aiosqlite.Connection) -> list[aiosqlite.Row]:
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"SELECT id, issue_id, status, stage, ended_at FROM runs "
        f"WHERE status IN ({placeholders})",
        LIVE_STATUSES,
    )
    return list(await cur.fetchall())


async def assert_consistent(sim: Sim, conn: aiosqlite.Connection) -> None:
    live = await _live_run_rows(conn)

    # 1. No zombie running runs: a live run must not already have an end time.
    zombies = [row["id"] for row in live if row["ended_at"] is not None]
    assert not zombies, f"zombie running runs (running but ended): {zombies}"

    # 2. One active run per issue — except a review monitor + review_fix pair is
    #    intentional: the monitor row stays running while the fix row runs.
    by_issue: dict[str, list[str]] = {}
    for row in live:
        by_issue.setdefault(row["issue_id"], []).append(row["stage"])
    overloaded = {
        iid: stages
        for iid, stages in by_issue.items()
        if len(stages) > 1 and sorted(stages) != ["review", "review_fix"]
    }
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
    #    is in a completed (Done) lane. When Sim has no issue_id (e.g. the test
    #    used the default empty SimIssue.url), fall back to the DB's issue_prs.
    for pr in sim.prs.values():
        if not pr.merged:
            continue
        issue_id = pr.issue_id
        if not issue_id:
            db_cur = await conn.execute(
                "SELECT issue_id FROM issue_prs WHERE github_repo=? AND pr_number=?",
                (pr.repo, pr.number),
            )
            db_row = await db_cur.fetchone()
            if db_row:
                issue_id = db_row["issue_id"]
        if issue_id:
            issue = sim.issues.get(issue_id)
            if issue is not None:
                assert issue.state_type == "completed", (
                    f"PR #{pr.number} merged but issue {issue_id} lane is "
                    f"{issue.state_name!r} (type {issue.state_type!r}), not Done"
                )

    # 5. issue_prs agrees with Sim: every tracked PR exists in Sim with a
    #    matching merge status.
    cur = await conn.execute(
        "SELECT issue_id, github_repo, pr_number, merged_at FROM issue_prs"
    )
    db_pr_rows = await cur.fetchall()
    for row in db_pr_rows:
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

    # 5b. Every Sim PR with an issue_id must be recorded in issue_prs under the
    #     correct issue — a PR filed under the wrong issue would pass check 5 but
    #     silently corrupt issue ownership.
    db_pr_issue: dict[tuple[str, int], str] = {
        (row["github_repo"], row["pr_number"]): row["issue_id"] for row in db_pr_rows
    }
    for (repo, number), sim_pr in sim.prs.items():
        if sim_pr.issue_id and sim_pr.state != PR_CLOSED:
            assert (repo, number) in db_pr_issue, (
                f"Sim PR ({repo!r}, {number}) for issue {sim_pr.issue_id!r} "
                f"is not recorded in issue_prs"
            )
            db_issue_id = db_pr_issue[(repo, number)]
            assert db_issue_id == sim_pr.issue_id, (
                f"issue_prs records PR ({repo!r}, {number}) under issue "
                f"{db_issue_id!r} but Sim has it under {sim_pr.issue_id!r}"
            )

    # 5c. No duplicate PR ownership: the same (github_repo, pr_number) must not
    #     appear under two different issues. The schema allows this (PK is
    #     (issue_id, github_repo)) but it would corrupt ownership invariants.
    dup_cur = await conn.execute(
        "SELECT github_repo, pr_number, COUNT(DISTINCT issue_id) AS n "
        "FROM issue_prs "
        "GROUP BY github_repo, pr_number "
        "HAVING n > 1"
    )
    duplicates = await dup_cur.fetchall()
    assert not duplicates, (
        "issue_prs has duplicate ownership rows for: "
        + ", ".join(
            f"({row['github_repo']!r}, {row['pr_number']})" for row in duplicates
        )
    )
