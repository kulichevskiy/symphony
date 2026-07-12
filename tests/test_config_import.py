"""Importer seam: YAML fixture in → DB rows out (SYM-188).

Asserts legacy resolution (incl. the codex-reviewer inheritance case), priority
stamped from YAML order, full Claude model IDs normalized/preserved, sparse
legacy-free payloads, and double-import refusal without the replace flag.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.config import RepoBinding
from symphony.config_import import ConfigImportError, import_config

_STATES = """
    linear_states:
      ready: Todo
      code_review: In Review
"""


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(body)
    return p


async def _import(tmp_path: Path, body: str, **kw):  # type: ignore[no-untyped-def]
    conn = await db.connect(tmp_path / "state.sqlite")
    path = _write(tmp_path, body)
    result = await import_config(path, conn, now="2026-07-12T00:00:00+00:00", **kw)
    rows = await db.config_bindings.list_all(conn)
    globals_row = await db.config_globals.get(conn)
    return conn, result, rows, globals_row


@pytest.mark.asyncio
async def test_rows_out_with_priority_from_yaml_order(tmp_path: Path) -> None:
    conn, result, rows, globals_row = await _import(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
{_STATES}
  - linear_team_key: WEB
    github_repo: org/web
{_STATES}
""",
    )
    assert result.bindings == 2
    assert [r.priority for r in rows] == [0, 1]
    assert [r.project_key for r in rows] == ["ENG", "WEB"]
    # Migration marker stamped.
    assert globals_row is not None and globals_row.migrated_at
    await conn.close()


@pytest.mark.asyncio
async def test_payload_is_sparse_and_legacy_free(tmp_path: Path) -> None:
    conn, _result, rows, _g = await _import(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    agent: codex
{_STATES}
""",
    )
    payload = rows[0].payload
    # No legacy top-level role field survives.
    assert "agent" not in payload
    assert "codex_model" not in payload
    # No defaults materialized: only operator-set non-role fields + the matrix.
    assert set(payload) <= {"linear_team_key", "github_repo", "linear_states", "roles"}
    # The codex implementer was captured in the matrix.
    assert payload["roles"]["implement"]["agent"] == "codex"
    await conn.close()


@pytest.mark.asyncio
async def test_codex_reviewer_inherits_binding_codex_model(tmp_path: Path) -> None:
    """`reviewer_agent: codex` + `codex_model` (no reviewer model pinned): the
    reviewer inherits the binding codex model, and that inheritance is baked
    into the matrix rather than dropped."""
    conn, _result, rows, globals_row = await _import(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    reviewer_agent: codex
    codex_model: gpt-5.1-codex-max
{_STATES}
""",
    )
    payload = rows[0].payload
    roles = payload["roles"]
    assert roles["review_find"]["model"] == "gpt-5.1-codex-max"
    assert roles["review_verify"]["model"] == "gpt-5.1-codex-max"
    # And it resolves back identically on reload.
    global_roles = globals_row.roles if globals_row else {}
    from symphony.config import RoleConfig

    gr = {k: RoleConfig.model_validate(v) for k, v in global_roles.items()}
    binding = RepoBinding.model_validate(payload)
    assert binding.resolved_role("review_find", gr).agent == "codex"
    assert binding.resolved_role("review_find", gr).model == "gpt-5.1-codex-max"
    await conn.close()


@pytest.mark.asyncio
async def test_full_claude_model_normalized_to_alias(tmp_path: Path) -> None:
    conn, _result, rows, _g = await _import(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    agent: codex
    local_review_claude_model: claude-sonnet-4-6
{_STATES}
""",
    )
    roles = rows[0].payload["roles"]
    assert roles["review_find"]["model"] == "sonnet"
    await conn.close()


@pytest.mark.asyncio
async def test_reload_matches_operator_resolution(tmp_path: Path) -> None:
    """The sparse matrix reproduces the operator's resolved roles bit-for-bit."""
    body = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    agent: codex
    codex_model: gpt-5.1-codex-max
    local_review_verifier_claude_model: opus
    roles:
      review_find:
        effort: high
        model: sonnet
{_STATES}
"""
    from symphony.config import Config, RoleConfig

    operator = Config.model_validate(__import__("yaml").safe_load(body)).repos[0]
    op_global = Config.model_validate(__import__("yaml").safe_load(body)).roles

    conn, _result, rows, globals_row = await _import(tmp_path, body)
    raw_globals = globals_row.roles if globals_row else {}
    gr = {k: RoleConfig.model_validate(v) for k, v in raw_globals.items()}
    reloaded = RepoBinding.model_validate(rows[0].payload)
    for name in ("implement", "review_find", "review_verify", "fix", "accept"):
        assert reloaded.resolved_role(name, gr) == operator.resolved_role(name, op_global), name
    await conn.close()


@pytest.mark.asyncio
async def test_double_import_refused_without_replace(tmp_path: Path) -> None:
    body = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
{_STATES}
"""
    conn = await db.connect(tmp_path / "state.sqlite")
    path = _write(tmp_path, body)
    await import_config(path, conn, now="t1")
    with pytest.raises(ConfigImportError, match="already imported"):
        await import_config(path, conn, now="t2")
    # replace=True succeeds and does not duplicate rows.
    result = await import_config(path, conn, replace=True, now="t3")
    assert result.replaced is True
    assert await db.config_bindings.count(conn) == 1
    await conn.close()
