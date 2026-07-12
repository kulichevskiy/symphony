"""Orchestrator seam: a binding stored in the config DB is assembled and then
scanned/dispatched identically to its YAML equivalent (SYM-188)."""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.effective_config import assemble_effective_config
from tests.harness import Harness, ManualClock

TEAM = "ENG"
REPO = "org/repo"
READY = "Todo"
DONE = "Done"


def _base(tmp_path: Path) -> Config:
    return Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        db_path=tmp_path / "config.sqlite",
    )


@pytest.mark.asyncio
async def test_db_binding_is_scanned_and_dispatched(tmp_path: Path) -> None:
    # Store a binding in the config DB, then assemble the effective config from
    # it — no YAML `repos:` involved.
    conn = await db.connect(tmp_path / "config.sqlite")
    await db.config_bindings.insert(
        conn,
        payload={
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
        },
        key=(TEAM, REPO, "", "linear", "default"),
        priority=0,
    )
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    assert [b.project_key for b in cfg.repos] == [TEAM]

    # Run the assembled config through the orchestrator harness and confirm the
    # ready issue is dispatched on the first tick.
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=cfg, clock=clock)
    try:
        issue = harness.sim.seed_issue(
            identifier="ENG-1", team_key=TEAM, state_name=READY, title="from db"
        )
        await harness.warmup()
        scheduled = await harness.step()
        assert len(scheduled) == 1
        # The seeded issue left the ready lane (it was picked up).
        assert harness.sim.issues[issue.id].state_name != READY
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_disabled_binding_recovers_pidless_local_review_run(tmp_path: Path) -> None:
    """A binding disabled while it has a pidless `local_review` run must
    still recover it: startup reconcile moves the issue back to Ready using
    the disabled binding (kept in `cfg.repos` for exactly this), and the next
    poll tick must notice and dispatch it rather than stranding it in Ready
    forever."""
    config = Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        db_path=tmp_path / "config.sqlite",
        repos=[
            RepoBinding(
                linear_team_key=TEAM,
                github_repo=REPO,
                enabled=False,
                local_review=False,
                remote_review=False,
                linear_states=LinearStates(
                    ready=READY,
                    in_progress="In Progress",
                    code_review="Needs Approval",
                    done=DONE,
                ),
            )
        ],
    )

    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=config, clock=clock)
    try:
        issue = harness.sim.seed_issue(
            identifier="ENG-1",
            team_key=TEAM,
            state_name="Local Code Review",
            title="crash recovery",
        )
        storage_id = await db.issues.upsert(
            harness.conn, id=issue.id, identifier=issue.identifier, team_key=TEAM, title=issue.title
        )
        await db.runs.create(
            harness.conn,
            id="pidless-local-review",
            issue_id=storage_id,
            stage="local_review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        # warmup() runs startup reconcile: the pidless run has no PID, so it
        # is flipped interrupted and the issue moved back to Ready using the
        # disabled binding.
        await harness.warmup()
        assert harness.sim.issues[issue.id].state_name == READY

        scheduled = await harness.step()
        assert len(scheduled) == 1
        assert harness.sim.issues[issue.id].state_name != READY
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_tick_skips_disabled_binding_with_no_registered_tracker(tmp_path: Path) -> None:
    """A disabled binding with no live DB work gets no tracker registered at
    boot (`cli._configured_tracker_registry` skips it) but stays loaded in
    `cfg.repos`. `_tick()`'s recovery scan over that binding must not crash
    the whole tick with `KeyError` — every existing disabled-binding test
    builds the Orchestrator with a single tracker, which registers one for
    every binding (including disabled ones) and so never exercises this."""
    from unittest.mock import AsyncMock, MagicMock

    from symphony.orchestrator.poll import Orchestrator
    from symphony.tracker import TrackerRegistry

    disabled_binding = RepoBinding(
        linear_team_key=TEAM,
        github_repo=REPO,
        enabled=False,
        linear_states=LinearStates(ready=READY, code_review="Needs Approval"),
    )
    conn = await db.connect(tmp_path / "config.sqlite")
    try:
        orch = Orchestrator(
            Config(repos=[disabled_binding]),
            TrackerRegistry(),
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )
        orch._restore_operator_waits = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._poll_merge_candidates = AsyncMock(return_value=[])  # type: ignore[method-assign]  # noqa: SLF001
        orch._poll_review_runs = AsyncMock(return_value=[])  # type: ignore[method-assign]  # noqa: SLF001
        orch._resurrect_review_runs = AsyncMock(return_value=[])  # type: ignore[method-assign]  # noqa: SLF001
        orch._poll_slash_commands = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001

        scheduled = await orch._tick()  # noqa: SLF001

        assert scheduled == []
    finally:
        await conn.close()
