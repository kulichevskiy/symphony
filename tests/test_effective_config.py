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
        await assemble_effective_config(
            conn, _base(tmp_path, repos=False), yaml_has_repos_topology=True
        )
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
async def test_bindings_from_db_assembled_in_priority_order(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
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
    # Production shape once the DB owns bindings: `base` is loaded with
    # `resolve_repos=False`, so `base.repos` is always empty — the leftover
    # YAML topology is only knowable via `yaml_has_repos_topology`.
    with caplog.at_level("WARNING"):
        cfg = await assemble_effective_config(
            conn, _base(tmp_path, repos=False), yaml_has_repos_topology=True
        )
    assert [b.project_key for b in cfg.repos] == ["ENG", "WEB"]
    # Global matrix flows through and resolves.
    assert cfg.repos[0].resolved_role("implement", cfg.roles).agent == "codex"
    assert "YAML `repos:`/`roles:` are ignored" in caplog.text
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
    """Assembly resolves `env:` key names to values, mirroring `Config.load`
    for YAML repos — an agent spawned against an unresolved binding would get
    the literal `.env` key *name* instead of the secret value."""
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
async def test_legacy_role_fields_synthesized_for_codex_binding(tmp_path: Path) -> None:
    """DB payloads are legacy-free, but several runtime paths still read the
    legacy top-level fields directly (`binding.agent` for codex resume and
    usage-cost attribution, `binding.codex_model`, ...). Assembly must
    synthesize them from the resolved matrix so a migrated codex binding
    doesn't run those paths with claude defaults (SYM-192 retires this)."""
    conn = await db.connect(tmp_path / "state.sqlite")
    # What the importer emits for a legacy `agent: codex` +
    # `codex_model: gpt-5.1-codex-max` binding: agent/model cells on every
    # builder role, plus explicit claude cells on the review roles (the
    # legacy-free baseline would otherwise flip their opposite-family
    # default to codex).
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
                "review_verify": {"agent": "claude"},
            },
        },
        key=("ENG", "org/api", "", "linear", "default"),
    )
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    (binding,) = cfg.repos
    assert binding.agent == "codex"
    assert binding.codex_model == "gpt-5.1-codex-max"
    assert binding.reviewer_agent == "claude"
    assert binding.local_review_claude_model == "sonnet"
    # Synthesis must not have changed any resolved role.
    assert binding.resolved_role("implement", cfg.roles).agent == "codex"
    assert binding.resolved_role("review_find", cfg.roles).agent == "claude"
    await conn.close()


@pytest.mark.asyncio
async def test_mixed_matrix_skips_legacy_synthesis_keeping_resolution(tmp_path: Path) -> None:
    """A per-role mixed matrix (codex implement over inherited claude fix)
    can't be represented by one legacy `agent` value without changing the
    fallback resolution — synthesis must leave the binding untouched and the
    resolved roles identical to what the matrix alone dictates."""
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
async def test_warns_on_leftover_yaml_repos_via_real_load_path(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reproduces the actual production shape end-to-end: a YAML file with a
    `repos:` block, loaded with `resolve_repos=False` (as every caller does
    once the DB has bindings), leaves `base.repos == []` — the warning must
    still fire from `Config.peek_repos_topology`, not from `base.repos`."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    p = tmp_path / "cfg.yaml"
    p.write_text(
        f"""
workspace_root: {tmp_path / "ws"}
log_root: {tmp_path / "logs"}
db_path: {tmp_path / "state.sqlite"}
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    linear_states: {{ready: Todo, code_review: In Review}}
"""
    )
    base = Config.load(p, resolve_repos=False)
    assert base.repos == []

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
    with caplog.at_level("WARNING"):
        await assemble_effective_config(
            conn, base, yaml_has_repos_topology=Config.peek_repos_topology(p)
        )
    assert "YAML `repos:`/`roles:` are ignored" in caplog.text
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
