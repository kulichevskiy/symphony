"""Effective-config assembly + boot gates (SYM-188).

Assembly composes env system knobs + DB bindings + DB global roles (Config v2
9/9 — YAML is gone). The zero-binding boot gate refuses a daemon start that
would orphan live work; a true fresh install boots with an empty topology.
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
    # `base` is `from_env`-shaped: its `repos` are always empty (the DB owns
    # topology now), so the assembled config's bindings come entirely from the
    # DB, in priority order.
    cfg = await assemble_effective_config(conn, _base(tmp_path, repos=False))
    assert [b.project_key for b in cfg.repos] == ["ENG", "WEB"]
    # Global matrix flows through and resolves.
    assert cfg.repos[0].resolved_role("implement", cfg.roles).agent == "codex"
    await conn.close()


@pytest.mark.asyncio
async def test_enabled_column_stamped_but_inert(tmp_path: Path) -> None:
    """The row's `enabled` column is stamped onto the binding, but this slice
    gives it no semantics — every row loads the same. The lifecycle (dispatch
    skip, launch gate, drain guard) ships in SYM-193."""
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
    assert {b.project_key: b.enabled for b in cfg.repos} == {"ENG": False, "WEB": True}
    await conn.close()


@pytest.mark.asyncio
async def test_invalid_db_global_role_enum_refuses_boot(tmp_path: Path) -> None:
    """A corrupt/hand-edited/restored DB row with a bad enum value must raise
    `ConfigBootError`, not a raw `pydantic.ValidationError` — the callers in
    `cli._run`/`_preflight`/`_dispatch` only catch `ConfigBootError`."""
    conn = await db.connect(tmp_path / "state.sqlite")
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "ENG",
            "github_repo": "org/api",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
        },
        key=("ENG", "org/api", "", "linear", "default"),
    )
    await db.config_globals.set_globals(
        conn, roles={"implement": {"agent": "bogus"}}, migrated_at="t"
    )
    with pytest.raises(ConfigBootError):
        await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()


@pytest.mark.asyncio
async def test_unknown_db_global_role_name_refuses_boot(tmp_path: Path) -> None:
    """A typo'd role-name key (e.g. `review-find`, restored/UI-written) must
    raise `ConfigBootError` — the same rejection an unknown key gets in YAML
    via the `RoleName` Literal, not a silent no-op that boots with the typo'd
    role never actually configured."""
    conn = await db.connect(tmp_path / "state.sqlite")
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "ENG",
            "github_repo": "org/api",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
        },
        key=("ENG", "org/api", "", "linear", "default"),
    )
    await db.config_globals.set_globals(
        conn, roles={"review-find": {"agent": "codex"}}, migrated_at="t"
    )
    with pytest.raises(ConfigBootError):
        await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()


@pytest.mark.asyncio
async def test_db_binding_env_is_resolved_not_left_as_key_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Assembly resolves `env:` key names to values — an agent spawned against
    an unresolved binding would otherwise get the literal `.env` key *name*
    instead of the secret value."""
    monkeypatch.setenv("AGENT_TOKEN", "the-real-secret")
    conn = await db.connect(tmp_path / "state.sqlite")
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "ENG",
            "github_repo": "org/api",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
            "env": {"AGENT_TOKEN": "AGENT_TOKEN"},
        },
        key=("ENG", "org/api", "", "linear", "default"),
    )
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    (binding,) = cfg.repos
    assert binding.env == {"AGENT_TOKEN": "the-real-secret"}
    await conn.close()


@pytest.mark.asyncio
async def test_no_legacy_synthesis_resolved_role_is_authoritative(tmp_path: Path) -> None:
    """DB payloads are legacy-free and (SYM-192) every per-stage consumer reads
    `resolved_role`, so assembly must NOT back-synthesize the legacy top-level
    fields: the binding keeps its default `agent`/`codex_model`, while the
    resolved matrix carries the real per-role values."""
    conn = await db.connect(tmp_path / "state.sqlite")
    # What the importer emits for a legacy `agent: codex` +
    # `codex_model: gpt-5.1-codex-max` binding: agent/model cells on every
    # builder role, plus explicit claude cells on the review roles.
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "ENG",
            "github_repo": "org/api",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
            "roles": {
                "implement": {"agent": "codex", "model": "gpt-5.1-codex-max"},
                "fix": {"agent": "codex", "model": "gpt-5.1-codex-max"},
                "accept": {"agent": "codex", "model": "gpt-5.1-codex-max"},
                "review_find": {"agent": "claude", "model": "sonnet"},
                "review_verify": {"agent": "claude", "model": "opus"},
            },
        },
        key=("ENG", "org/api", "", "linear", "default"),
    )
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    (binding,) = cfg.repos
    # Legacy fields are never synthesized — they stay at model defaults.
    assert binding.agent == "claude"
    assert binding.reviewer_agent is None
    assert binding.local_review_claude_model is None
    # The resolved matrix is the single source of truth for every consumer.
    assert binding.resolved_role("implement", cfg.roles).agent == "codex"
    assert binding.resolved_role("implement", cfg.roles).model == "gpt-5.1-codex-max"
    assert binding.resolved_role("review_find", cfg.roles).agent == "claude"
    assert binding.resolved_role("review_find", cfg.roles).model == "sonnet"
    assert binding.resolved_role("review_verify", cfg.roles).model == "opus"
    await conn.close()


@pytest.mark.asyncio
async def test_mixed_matrix_resolves_per_role_without_legacy_fields(tmp_path: Path) -> None:
    """A per-role mixed matrix (codex implement over inherited claude fix)
    resolves each role independently; no legacy field is written, so the
    binding's default `agent` stays `claude` while `implement` is codex."""
    conn = await db.connect(tmp_path / "state.sqlite")
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "ENG",
            "github_repo": "org/api",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
            "roles": {"implement": {"agent": "codex"}},
        },
        key=("ENG", "org/api", "", "linear", "default"),
    )
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    (binding,) = cfg.repos
    assert binding.agent == "claude"
    assert binding.resolved_role("implement", cfg.roles).agent == "codex"
    assert binding.resolved_role("fix", cfg.roles).agent == "claude"
    await conn.close()


@pytest.mark.asyncio
async def test_invalid_db_role_combo_refuses_boot(tmp_path: Path) -> None:
    """DB roles bypass `Config`'s constructor validators via `model_copy`;
    assembly must re-run the same family check YAML `repos:`/`roles:` gets."""
    conn = await db.connect(tmp_path / "state.sqlite")
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "ENG",
            "github_repo": "org/api",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
        },
        key=("ENG", "org/api", "", "linear", "default"),
    )
    # `codex` agent paired with a Claude-only model alias — invalid combo the
    # YAML loader's `_validate_roles` would reject at construction time.
    await db.config_globals.set_globals(
        conn, roles={"implement": {"agent": "codex", "model": "opus"}}, migrated_at="t"
    )
    with pytest.raises(ConfigBootError, match="invalid role configuration"):
        await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()


@pytest.mark.asyncio
async def test_malformed_binding_payload_json_refuses_boot_cleanly(tmp_path: Path) -> None:
    """A hand-edited or restored row with malformed payload JSON must surface
    as `ConfigBootError` — the same clean failure every other config problem
    gets — not a raw `json.JSONDecodeError` traceback out of `list_all`."""
    conn = await db.connect(tmp_path / "state.sqlite")
    await conn.execute(
        """
        INSERT INTO config_bindings (
            payload, version, enabled, priority, updated_at, updated_by,
            project_key, github_repo, issue_label, tracker_provider, tracker_site
        )
        VALUES ('{not json', 1, 1, 0, '', '', 'ENG', 'org/api', '', 'linear', 'default')
        """
    )
    await conn.commit()
    with pytest.raises(ConfigBootError, match="malformed config binding payload"):
        await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()


@pytest.mark.asyncio
async def test_malformed_globals_roles_json_refuses_boot_cleanly(tmp_path: Path) -> None:
    """A hand-edited or restored `config_globals.roles` with malformed JSON
    must surface as `ConfigBootError`, not a raw `json.JSONDecodeError`
    traceback out of `db.config_globals.get`."""
    conn = await db.connect(tmp_path / "state.sqlite")
    await db.config_bindings.insert(
        conn,
        payload={
            "linear_team_key": "ENG",
            "github_repo": "org/api",
            "linear_states": {"ready": "Todo", "code_review": "In Review"},
        },
        key=("ENG", "org/api", "", "linear", "default"),
    )
    await conn.execute(
        "INSERT INTO config_globals (id, roles, migrated_at, version) "
        "VALUES (1, '{not json', 't', 1)"
    )
    await conn.commit()
    with pytest.raises(ConfigBootError, match="malformed config globals payload"):
        await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()
