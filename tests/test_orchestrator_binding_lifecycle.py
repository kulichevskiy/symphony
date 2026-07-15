"""Binding lifecycle: enabled toggle, launch gate, binding-key stamp (SYM-193).

A disabled binding starts no new issues and drops its tracker-queue lanes, but
stays loaded (visible to review/merge pollers and operator-wait resolution).
The launch gate — the single authoritative pre-spawn check — aborts a queued
first dispatch when the binding is disabled or occupancy meets the current cap,
while letting follow-up stages (fix-runs) proceed so in-flight work drains.
Runs carry the binding key they were dispatched under.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config
from symphony.effective_config import assemble_effective_config
from symphony.orchestrator.poll import _binding_storage_key
from tests.harness import Harness

TEAM = "ENG"
REPO = "org/repo"
READY = "Todo"
DONE = "Done"


def _base(tmp_path: Path) -> Config:
    return Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        db_path=tmp_path / "symphony.sqlite",
    )


def _payload(**extra) -> dict:
    payload: dict = {
        "linear_team_key": TEAM,
        "github_repo": REPO,
        "local_review": False,
        "remote_review": False,
        "linear_states": {
            "ready": READY,
            "in_progress": "In Progress",
            "code_review": "Needs Approval",
            "done": DONE,
        },
    }
    payload.update(extra)
    return payload


async def _insert(conn, *, enabled: bool = True, **extra) -> None:
    await db.config_bindings.insert(
        conn,
        payload=_payload(**extra),
        key=(TEAM, REPO, "", "linear", "default"),
        enabled=enabled,
    )


async def _queue_rows(conn, scope_team: str) -> int:
    cur = await conn.execute("SELECT COUNT(*) FROM tracker_queue WHERE team_key = ?", (scope_team,))
    row = await cur.fetchone()
    return int(row[0])


@pytest.mark.asyncio
async def test_disabled_binding_skips_dispatch_and_stays_loaded(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, enabled=False)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        issue = harness.sim.seed_issue(
            identifier="ENG-1", team_key=TEAM, state_name=READY, title="paused"
        )
        await harness.warmup()
        scheduled = await harness.step()
        # No new dispatch for a disabled binding.
        assert scheduled == []
        assert harness.sim.issues[issue.id].state_name == READY
        # No tracker-queue lanes were written for the disabled scope.
        assert await _queue_rows(harness.conn, TEAM) == 0
        # The binding stays loaded so review/merge pollers can still resolve it.
        assert TEAM in {b.linear_team_key for b in harness.orch.config.repos}
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_disable_mid_run_clears_existing_lanes(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    # `max_concurrent=0` mirrors the Ready lane into `tracker_queue` without
    # dispatching a full pipeline run.
    await _insert(conn, enabled=True, max_concurrent=0)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        harness.sim.seed_issue(identifier="ENG-1", team_key=TEAM, state_name=READY, title="queued")
        await harness.warmup()
        await harness.step()
        assert await _queue_rows(harness.conn, TEAM) > 0

        # Disable the binding in the DB; the next tick reloads it and clears
        # the lanes.
        await harness.conn.execute("UPDATE config_bindings SET enabled = 0")
        await harness.conn.commit()
        await harness.step()
        assert await _queue_rows(harness.conn, TEAM) == 0
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_run_carries_binding_key_from_dispatch(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, enabled=True)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg)
    try:
        harness.sim.seed_issue(
            identifier="ENG-1", team_key=TEAM, state_name=READY, title="stamp me"
        )
        await harness.warmup()
        scheduled = await harness.step()
        assert len(scheduled) == 1
        binding = harness.orch.config.repos[0]
        cur = await harness.conn.execute("SELECT binding_key FROM runs WHERE stage = 'implement'")
        rows = await cur.fetchall()
        assert rows
        assert all(str(r["binding_key"]) == _binding_storage_key(binding) for r in rows)
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_launch_gate_disabled_aborts_first_dispatch_not_followups(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, enabled=True)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg)
    try:
        await harness.warmup()
        binding = harness.orch.config.repos[0]
        # Enabled: both first dispatch and follow-ups are admitted.
        assert await harness.orch._launch_gate_admits(binding, first_dispatch=True)
        assert await harness.orch._launch_gate_admits(binding, first_dispatch=False)

        # Disable it in the live config (as a mid-run reload would).
        disabled = binding.model_copy(update={"enabled": False})
        harness.orch.config = harness.orch.config.model_copy(update={"repos": [disabled]})
        # First dispatch aborts; follow-up (drain) still proceeds.
        assert not await harness.orch._launch_gate_admits(disabled, first_dispatch=True)
        assert await harness.orch._launch_gate_admits(disabled, first_dispatch=False)
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_ready_binding_for_issue_skips_disabled_binding(tmp_path: Path) -> None:
    """The webhook path's dispatch resolver must skip a disabled binding
    itself, matching the poll scan's pause — relying solely on the launch
    gate would still let a disabled binding reserve a scheduled slot and
    contend for a dispatch semaphore before the gate aborts the spawn
    (SYM-193 review)."""
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, enabled=False)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        issue = harness.sim.seed_issue(
            identifier="ENG-1", team_key=TEAM, state_name=READY, title="paused"
        )
        await harness.warmup()
        assert harness.orch._ready_binding_for_issue(issue) is None  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_launch_gate_blocks_spawn_over_lowered_cap(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, enabled=True, max_concurrent=2)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg)
    try:
        await harness.warmup()
        binding = harness.orch.config.repos[0]
        key = _binding_storage_key(binding)
        # One live run already occupies the binding.
        issue_id = await db.issues.upsert(
            harness.conn,
            id="i1",
            provider="linear",
            site="default",
            identifier="ENG-1",
            title="live",
            team_key=TEAM,
        )
        await db.runs.create(
            harness.conn,
            id="r1",
            issue_id=issue_id,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-01-01T00:00:00Z",
            binding_key=key,
        )
        # cap=2, occupancy=1 → still admits.
        assert await harness.orch._launch_gate_admits(binding, first_dispatch=True)

        # Lower the cap to 1 in the live config: occupancy (1) now meets the
        # cap, so nothing new is admitted even though a queued task waited on
        # the old (capacity-2) semaphore.
        lowered = binding.model_copy(update={"max_concurrent": 1})
        harness.orch.config = harness.orch.config.model_copy(update={"repos": [lowered]})
        assert not await harness.orch._launch_gate_admits(lowered, first_dispatch=True)
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_review_monitor_does_not_count_against_cap(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, enabled=True, max_concurrent=1)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg)
    try:
        await harness.warmup()
        binding = harness.orch.config.repos[0]
        key = _binding_storage_key(binding)
        issue_id = await db.issues.upsert(
            harness.conn,
            id="i1",
            provider="linear",
            site="default",
            identifier="ENG-1",
            title="in review",
            team_key=TEAM,
        )
        # A passive review monitor is live but never consumes dispatch capacity,
        # so a new first dispatch is still admitted at cap=1.
        await db.runs.create(
            harness.conn,
            id="r-review",
            issue_id=issue_id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-01-01T00:00:00Z",
            binding_key=key,
        )
        assert await harness.orch._launch_gate_admits(binding, first_dispatch=True)
    finally:
        await harness.close()
