"""Tests for the in-memory test harness rig (tests/harness/).

The harness is the deterministic Linear/GitHub rehearsal all later
pipeline-timing scenarios run against. These tests pin its skeleton:
construction, a no-op tick, the shared clock, and the drift invariant.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.tracker import IssueTracker
from tests.harness import (
    FakeGitHub,
    FakeLinear,
    Harness,
    ManualClock,
    Sim,
    SimIssue,
    assert_consistent,
)


@pytest.mark.asyncio
async def test_harness_constructs_on_temp_sqlite(tmp_path: Path) -> None:
    harness = await Harness.create(tmp_path)
    try:
        assert harness.orch is not None
        assert isinstance(harness.sim, Sim)
        assert isinstance(harness.clock, ManualClock)
        # Real SQLite file on disk.
        assert (tmp_path / "symphony.sqlite").exists()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_fakes_implement_orchestrator_interface(tmp_path: Path) -> None:
    harness = await Harness.create(tmp_path)
    try:
        assert isinstance(harness.linear, FakeLinear)
        assert isinstance(harness.github, FakeGitHub)
        # FakeLinear satisfies the tracker protocol the orchestrator calls.
        assert isinstance(harness.linear, IssueTracker)
        # Both fakes are backed by the single canonical Sim.
        assert harness.linear._sim is harness.sim  # noqa: SLF001
        assert harness.github._sim is harness.sim  # noqa: SLF001
        # And they are the instances the orchestrator/reconciler hold.
        assert harness.orch._gh is harness.github  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_empty_step_is_noop_and_consistent(tmp_path: Path) -> None:
    harness = await Harness.create(tmp_path)
    try:
        await harness.warmup()
        scheduled = await harness.step()
        assert scheduled == []
        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_shared_clock_threaded_into_orchestrator_and_reconciler(
    tmp_path: Path,
) -> None:
    harness = await Harness.create(tmp_path)
    try:
        before = harness.orch._now()  # noqa: SLF001
        recon_before = harness.orch._reconciler._now()  # noqa: SLF001
        sim_before = harness.sim.now()
        assert before == recon_before == sim_before

        harness.advance(120)

        after = harness.orch._now()  # noqa: SLF001
        recon_after = harness.orch._reconciler._now()  # noqa: SLF001
        sim_after = harness.sim.now()
        assert after == recon_after == sim_after
        assert (after - before).total_seconds() == 120
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_restart_reopens_db_and_recovers_orphaned_run(tmp_path: Path) -> None:
    """A `running` run whose pid the Sim reports dead is an orphan from a host
    crash. `restart()` closes + reopens the DB, builds a fresh Orchestrator on
    the same Sim + clock, and reconcile flips the orphan `interrupted` and
    comments — without drift."""
    harness = await Harness.create(tmp_path)
    try:
        # Seed the issue in the Sim (so the Linear comment lands) and the DB
        # (so reconcile finds the run's issue), plus a running implement run
        # whose pid the Sim reports dead.
        harness.sim.issues["iss-dead"] = SimIssue(
            id="iss-dead", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.issues.upsert(
            harness.conn, id="iss-dead", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            harness.conn,
            id="dead",
            issue_id="iss-dead",
            stage="implement",
            status="running",
            pid=4242,
            started_at="2026-01-01T00:00:00+00:00",
        )
        harness.sim.kill_process(4242)

        old_conn = harness.conn
        await harness.restart()
        # The conn was reopened on the same file, not reused.
        assert harness.conn is not old_conn

        cur = await harness.conn.execute(
            "SELECT status, ended_at, termination_kind FROM runs WHERE id=?",
            ("dead",),
        )
        row = await cur.fetchone()
        assert row is not None
        assert row["status"] == db.runs.INTERRUPTED_STATUS
        assert row["ended_at"] is not None
        assert row["termination_kind"] == "orphaned"

        comments = harness.sim.comments.get("iss-dead", [])
        assert len(comments) == 1
        assert "Host restarted" in comments[0].body

        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_merge_pr_enqueues_github_webhook_without_delivering(
    tmp_path: Path,
) -> None:
    """`sim.merge_pr()` mutates reality and ENQUEUES the merge webhook; nothing
    is delivered until the harness chooses to. `deliver_github_webhook()` then
    routes the queued event into the real orchestrator handler."""
    harness = await Harness.create(tmp_path)
    try:
        await harness.warmup()
        url = await harness.github.ensure_pr(
            title="t", body="", head="b", repo="org/repo"
        )
        assert url
        pr = next(iter(harness.sim.prs.values()))

        sim_pr = harness.sim.merge_pr(pr.number, repo=pr.repo)
        # Mutation applied to canonical reality; webhook queued, not delivered.
        assert sim_pr.merged
        assert len(harness.sim.github_webhooks) == 1
        assert harness.sim.github_webhooks[0].merged is True

        result = await harness.deliver_github_webhook()
        assert result.handled is True
        await harness.drain()
        assert harness.sim.github_webhooks == []
        # Nothing else queued → delivering again is a hard error.
        with pytest.raises(AssertionError):
            await harness.deliver_github_webhook()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_operator_comment_enqueues_linear_webhook_for_explicit_delivery(
    tmp_path: Path,
) -> None:
    """The Linear analog: `sim.operator_comment()` records the comment and
    ENQUEUES the Linear webhook; `deliver_linear_webhook()` routes it into the
    real handler. With no active run the handler reports `handled=False`, which
    still proves the enqueue → deliver plumbing."""
    harness = await Harness.create(tmp_path)
    try:
        await harness.warmup()
        harness.sim.issues["iss-1"] = SimIssue(
            id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )

        harness.sim.operator_comment("iss-1", "$stop")
        assert harness.sim.comments["iss-1"][-1].body == "$stop"
        assert len(harness.sim.linear_webhooks) == 1

        result = await harness.deliver_linear_webhook()
        assert result.kind == "comment"
        assert harness.sim.linear_webhooks == []
        with pytest.raises(AssertionError):
            await harness.deliver_linear_webhook()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_assert_consistent_passes_on_empty_state(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "empty.sqlite")
    try:
        sim = Sim(ManualClock())
        await assert_consistent(sim, conn)
    finally:
        await conn.close()


async def _seed_issue(conn: object, *, ident: str = "ENG-1") -> str:
    return await db.issues.upsert(
        conn,  # type: ignore[arg-type]
        id=ident,
        identifier=ident,
        title="t",
        team_key="ENG",
    )


async def _seed_run(
    conn: object, *, run_id: str, issue_id: str, stage: str, pid: int | None
) -> None:
    await db.runs.create(
        conn,  # type: ignore[arg-type]
        id=run_id,
        issue_id=issue_id,
        stage=stage,
        status="running",
        pid=pid,
        started_at="2026-01-01T00:00:00+00:00",
    )


@pytest.mark.asyncio
async def test_assert_consistent_flags_zombie_running_run(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "z.sqlite")
    try:
        sim = Sim(ManualClock())
        issue_id = await _seed_issue(conn)
        await _seed_run(
            conn, run_id="r1", issue_id=issue_id, stage="implement", pid=None
        )
        # A run marked running but already ended is a zombie.
        await db.runs.update_status(
            conn, "r1", "running", ended_at="2026-01-01T00:00:00+00:00"
        )
        with pytest.raises(AssertionError):
            await assert_consistent(sim, conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_assert_consistent_flags_two_active_runs_for_one_issue(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "two.sqlite")
    try:
        sim = Sim(ManualClock())
        issue_id = await _seed_issue(conn)
        await _seed_run(
            conn, run_id="r1", issue_id=issue_id, stage="implement", pid=1
        )
        await _seed_run(conn, run_id="r2", issue_id=issue_id, stage="review", pid=2)
        with pytest.raises(AssertionError):
            await assert_consistent(sim, conn)
    finally:
        await conn.close()
