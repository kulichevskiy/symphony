"""Effective-config assembly + boot gates (SYM-188).

Assembly composes YAML system knobs + DB bindings + DB global roles. The two
zero-binding boot gates refuse a start that would orphan live work or silently
dispatch nothing; a true fresh install boots.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config
from symphony.effective_config import ConfigBootError, assemble_effective_config


def _base(tmp_path: Path, *, repos: bool = False) -> Config:
    repos_block = (
        [
            {
                "linear_team_key": "ENG",
                "github_repo": "org/repo",
                "linear_states": {"ready": "Todo", "code_review": "In Review"},
            }
        ]
        if repos
        else []
    )
    return Config(
        workspace_root=tmp_path / "ws",
        log_root=tmp_path / "logs",
        db_path=tmp_path / "state.sqlite",
        repos=Config.model_validate({"repos": repos_block}).repos,
    )


@pytest.mark.asyncio
async def test_fresh_install_boots(tmp_path: Path) -> None:
    """No bindings, no unresolved work, no YAML topology → boots with empty."""
    conn = await db.connect(tmp_path / "state.sqlite")
    cfg = await assemble_effective_config(conn, _base(tmp_path, repos=False))
    assert cfg.repos == []
    assert cfg.roles == {}
    await conn.close()


@pytest.mark.asyncio
async def test_zero_bindings_with_yaml_repos_refuses(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    with pytest.raises(ConfigBootError, match="repos:"):
        await assemble_effective_config(conn, _base(tmp_path, repos=True))
    await conn.close()


@pytest.mark.asyncio
async def test_zero_bindings_over_live_work_refuses(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    storage_id = await db.issues.upsert(
        conn, id="I1", identifier="ENG-1", title="t", team_key="ENG"
    )
    await db.runs.create(
        conn,
        id="R1",
        issue_id=storage_id,
        stage="implement",
        status="running",
        pid=None,
        started_at="2026-07-12T00:00:00+00:00",
    )
    with pytest.raises(ConfigBootError, match="unresolved work"):
        await assemble_effective_config(conn, _base(tmp_path, repos=False))
    await conn.close()


@pytest.mark.asyncio
async def test_bindings_from_db_assembled_in_priority_order(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "WEB",
            "github_repo": "org/web",
            "linear_states": {"ready": "Backlog", "code_review": "In Review"},
        },
        key=("WEB", "org/web", "", "linear", "default"),
        priority=1,
    )
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "ENG",
            "github_repo": "org/api",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
        },
        key=("ENG", "org/api", "", "linear", "default"),
        priority=0,
    )
    await db.config_globals.set_globals(
        conn, roles={"implement": {"agent": "codex"}}, migrated_at="t"
    )
    # YAML still has repos — ignored because the DB has bindings (not an error).
    cfg = await assemble_effective_config(conn, _base(tmp_path, repos=True))
    assert [b.project_key for b in cfg.repos] == ["ENG", "WEB"]
    # Global matrix flows through and resolves.
    assert cfg.repos[0].resolved_role("implement", cfg.roles).agent == "codex"
    await conn.close()


@pytest.mark.asyncio
async def test_disabled_bindings_not_dispatched(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "ENG",
            "github_repo": "org/api",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
        },
        key=("ENG", "org/api", "", "linear", "default"),
        enabled=False,
    )
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "WEB",
            "github_repo": "org/web",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
        },
        key=("WEB", "org/web", "", "linear", "default"),
        enabled=True,
    )
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    assert [b.project_key for b in cfg.repos] == ["WEB"]
    await conn.close()
