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


async def _update_stored_binding(conn, **fields) -> None:
    """Edit the sole `config_bindings` row directly in the DB, leaving any
    already-loaded `self.config` untouched — mirrors an operator edit landing
    while a task sits behind a semaphore, before the next tick's reload."""
    stored = (await db.config_bindings.list_all(conn))[0]
    payload = dict(stored.payload)
    enabled = fields.pop("enabled", stored.enabled)
    payload.update(fields)
    await db.config_bindings.update(
        conn,
        stored.id,
        payload=payload,
        key=(
            stored.project_key,
            stored.github_repo,
            stored.issue_label,
            stored.tracker_provider,
            stored.tracker_site,
        ),
        enabled=enabled,
        priority=stored.priority,
        expected_version=stored.version,
    )


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

        # Disable it directly in the DB — `self.config` stays stale (no
        # reload ran), which is exactly the window a task queued behind a
        # semaphore can sit in (SYM-193).
        await _update_stored_binding(harness.conn, enabled=False)
        # First dispatch aborts; follow-up (drain) still proceeds.
        assert not await harness.orch._launch_gate_admits(binding, first_dispatch=True)
        assert await harness.orch._launch_gate_admits(binding, first_dispatch=False)
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

        # Lower the cap to 1 directly in the DB (self.config stays stale, as
        # it would while a task sits behind the old capacity-2 semaphore):
        # occupancy (1) now meets the cap, so nothing new is admitted.
        await _update_stored_binding(harness.conn, max_concurrent=1)
        assert not await harness.orch._launch_gate_admits(binding, first_dispatch=True)
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_current_binding_row_preserves_resolved_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The launch gate's fresh re-read must not clobber fields the sparse DB
    payload never carries resolved — `env:` names are resolved to secrets at
    load time, not stored resolved in `config_bindings` (SYM-193 review)."""
    monkeypatch.setenv("MY_SECRET", "shh")
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, enabled=True, env={"FOO": "MY_SECRET"})
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()
    assert cfg.repos[0].env == {"FOO": "shh"}

    harness = await Harness.create(tmp_path, config=cfg)
    try:
        await harness.warmup()
        binding = harness.orch.config.repos[0]
        current = await harness.orch._current_binding_row(binding)  # noqa: SLF001
        assert current is not None
        assert current.env == {"FOO": "shh"}
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_current_binding_row_does_not_revive_deleted_binding(
    tmp_path: Path,
) -> None:
    """Once the DB owns topology, a binding deleted down to zero rows must
    read as gone — not fall back to the daemon's stale `self.config.repos`,
    which would silently revive it for scheduling/gate checks until the next
    reload (SYM-193 review)."""
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, enabled=True)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()
        binding = harness.orch.config.repos[0]

        stored = (await db.config_bindings.list_all(harness.conn))[0]
        await db.config_bindings.delete(harness.conn, stored.id, expected_version=stored.version)

        # `self.config.repos` is stale (no reload ran yet) and still lists
        # `binding`, but the DB owns topology and now has zero rows for it.
        assert await harness.orch._current_binding_row(binding) is None  # noqa: SLF001
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
