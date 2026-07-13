"""Orchestrator seam: a binding stored in the config DB is assembled and then
scanned/dispatched identically to its YAML equivalent (SYM-188)."""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config
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
