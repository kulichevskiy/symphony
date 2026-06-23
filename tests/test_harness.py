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
