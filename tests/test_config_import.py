"""Importer seam: YAML fixture in → DB rows out (SYM-188).

Asserts legacy resolution (incl. the codex-reviewer inheritance case), priority
stamped from YAML order, full Claude model IDs normalized/preserved, sparse
legacy-free payloads, and double-import refusal without the replace flag.

SYM-195 adds the cutover binding-key backfill (unambiguous run/PR rows stamped;
ambiguous rows refused unless an explicit mapping is passed) and the
export→import round-trip through replace mode.
"""

from __future__ import annotations

import json
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
async def test_webhook_secrets_land_in_repo_secret_table(tmp_path: Path) -> None:
    """Per-binding YAML webhook secrets move into the repo-scoped table so
    verification survives cutover, and never remain in the (legacy-free,
    secret-free) binding payload (SYM-194)."""
    conn, _result, rows, _g = await _import(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    webhook_enabled: true
    webhook_secret: yaml-secret
{_STATES}
""",
    )
    assert "webhook_secret" not in rows[0].payload
    sec = await db.config_repo_secrets.get(conn, "org/api")
    assert sec is not None and sec.secret == "yaml-secret"
    assert sec.version == 1
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
async def test_review_strategy_stripped_and_materialized(tmp_path: Path) -> None:
    """A legacy `review_strategy:` key is not in `_LEGACY_ROLE_FIELDS` (it's not
    a role field), so it must be dropped explicitly rather than surviving
    verbatim into the "legacy-free" payload and re-firing its deprecation
    warning on every reload."""
    conn, _result, rows, _g = await _import(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    review_strategy: local
{_STATES}
""",
    )
    payload = rows[0].payload
    assert "review_strategy" not in payload
    assert payload["local_review"] is True
    assert payload["remote_review"] is False
    # Reload never re-triggers the review_strategy deprecation warning.
    RepoBinding.model_validate(payload)
    await conn.close()


@pytest.mark.asyncio
async def test_codex_reviewer_inherits_binding_codex_model(tmp_path: Path) -> None:
    """`reviewer_agent: codex` + `codex_model` (no reviewer model pinned): the
    finder (`review_find`) inherits the binding codex model, and that
    inheritance is baked into the matrix rather than dropped. `reviewer_agent`
    only ever governed the finder, not the verifier — `review_verify` defaults
    to the implementer's own family (claude, unset here) to stay opposite
    `review_find`, so it resolves identically before and after the legacy
    fields are stripped and gets no matrix cell."""
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
    assert "review_verify" not in roles
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


@pytest.mark.asyncio
async def test_import_refused_after_ui_roles_put_on_fresh_db(tmp_path: Path) -> None:
    """A UI `PUT /api/config/roles` on a fresh, never-migrated DB creates the
    `config_globals` row with `migrated_at=""` (config_globals.update_roles).
    A later import without `replace=True` must still refuse — not silently
    overwrite the operator's globals — even though no migration marker was
    ever stamped."""
    conn = await db.connect(tmp_path / "state.sqlite")
    await db.config_globals.update_roles(
        conn, roles={"implement": {"agent": "codex"}}, expected_version=0
    )
    body = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
{_STATES}
"""
    path = _write(tmp_path, body)
    with pytest.raises(ConfigImportError, match="already imported"):
        await import_config(path, conn, now="t1")
    globals_row = await db.config_globals.get(conn)
    assert globals_row is not None
    assert globals_row.roles == {"implement": {"agent": "codex"}}
    await conn.close()


@pytest.mark.asyncio
async def test_import_stores_disabled_binding(tmp_path: Path) -> None:
    """`enabled: false` input imports as a disabled row: the `enabled` column
    has runtime semantics (dispatch skip, launch gate, drain guard —
    SYM-193), so the importer no longer needs to refuse it."""
    conn = await db.connect(tmp_path / "state.sqlite")
    path = _write(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    enabled: false
{_STATES}
""",
    )
    await import_config(path, conn, now="t1")
    rows = await db.config_bindings.list_all(conn)
    assert len(rows) == 1
    assert rows[0].enabled is False
    await conn.close()


@pytest.mark.asyncio
async def test_imported_codex_binding_resolves_roles_at_runtime(tmp_path: Path) -> None:
    """End-to-end: a migrated legacy codex binding, reloaded through
    `assemble_effective_config`, exposes its codex agent/model via
    `resolved_role` — the single source every per-stage consumer reads
    (SYM-192). Legacy top-level fields are never back-synthesized."""
    from symphony.config import Config
    from symphony.effective_config import assemble_effective_config

    conn, _result, _rows, _g = await _import(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    agent: codex
    codex_model: gpt-5.1-codex-max
{_STATES}
""",
    )
    cfg = await assemble_effective_config(
        conn,
        Config(
            workspace_root=tmp_path / "ws",
            log_root=tmp_path / "logs",
            db_path=tmp_path / "state.sqlite",
        ),
    )
    (binding,) = cfg.repos
    # No legacy back-synthesis: the payload stays at model defaults.
    assert binding.agent == "claude"
    # The resolved matrix carries the migrated codex builder + opposite-family
    # (claude) reviewer roles.
    impl = binding.resolved_role("implement", cfg.roles)
    assert impl.agent == "codex" and impl.model == "gpt-5.1-codex-max"
    assert binding.resolved_role("fix", cfg.roles).agent == "codex"
    assert binding.resolved_role("review_find", cfg.roles).agent == "claude"
    await conn.close()


def _storage_key(*parts: str) -> str:
    return json.dumps(list(parts), separators=(",", ":"))


async def _seed_active_work(
    conn,  # type: ignore[no-untyped-def]
    *,
    identifier: str,
    team_key: str,
    github_repo: str | None,
) -> str:
    issue_id = await db.issues.upsert(
        conn, id=identifier, identifier=identifier, title="t", team_key=team_key
    )
    await db.runs.create(
        conn,
        id=f"run-{identifier}",
        issue_id=issue_id,
        stage="implement",
        status="running",
        pid=123,
        started_at="t0",
    )
    if github_repo is not None:
        await db.issue_prs.upsert(
            conn,
            issue_id=issue_id,
            github_repo=github_repo,
            pr_number=1,
            pr_url="u",
            created_at="t0",
        )
    return issue_id


@pytest.mark.asyncio
async def test_backfill_unambiguous_run_and_pr(tmp_path: Path) -> None:
    """A cutover with a single candidate binding stamps its natural key onto
    still-active runs and open-PR rows that carry an empty binding key."""
    conn = await db.connect(tmp_path / "state.sqlite")
    issue_id = await _seed_active_work(
        conn, identifier="ENG-1", team_key="ENG", github_repo="org/api"
    )
    path = _write(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
{_STATES}
""",
    )
    result = await import_config(path, conn, now="t1")
    assert result.runs_backfilled == 1
    assert result.prs_backfilled == 1
    expected = _storage_key("ENG", "org/api", "", "linear", "default")
    run = await conn.execute_fetchall(
        "SELECT binding_key FROM runs WHERE issue_id = ?", (issue_id,)
    )
    assert run[0]["binding_key"] == expected
    pr = await conn.execute_fetchall(
        "SELECT binding_key FROM issue_prs WHERE issue_id = ?", (issue_id,)
    )
    assert pr[0]["binding_key"] == expected
    await conn.close()


@pytest.mark.asyncio
async def test_backfill_ambiguous_refused_with_list(tmp_path: Path) -> None:
    """Two bindings on one repo (labeled + catch-all) make the row's binding
    ambiguous; the importer refuses the whole cutover, listing the row, rather
    than guessing from the issue's mutable labels — and the delete/insert roll
    back, so the DB is untouched."""
    conn = await db.connect(tmp_path / "state.sqlite")
    await _seed_active_work(conn, identifier="ENG-1", team_key="ENG", github_repo="org/api")
    path = _write(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    issue_label: urgent
{_STATES}
  - linear_team_key: ENG
    github_repo: org/api
{_STATES}
""",
    )
    with pytest.raises(ConfigImportError, match="ENG-1"):
        await import_config(path, conn, now="t1")
    # Rolled back: no bindings landed.
    assert await db.config_bindings.count(conn) == 0
    await conn.close()


@pytest.mark.asyncio
async def test_backfill_explicit_mapping_honored(tmp_path: Path) -> None:
    """An explicit issue→binding mapping resolves an otherwise-ambiguous row."""
    conn = await db.connect(tmp_path / "state.sqlite")
    issue_id = await _seed_active_work(
        conn, identifier="ENG-1", team_key="ENG", github_repo="org/api"
    )
    path = _write(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    issue_label: urgent
{_STATES}
  - linear_team_key: ENG
    github_repo: org/api
{_STATES}
""",
    )
    result = await import_config(
        path,
        conn,
        now="t1",
        issue_bindings={"ENG-1": ["ENG", "org/api", "urgent", "linear", "default"]},
    )
    assert result.runs_backfilled == 1
    assert result.prs_backfilled == 1
    expected = _storage_key("ENG", "org/api", "urgent", "linear", "default")
    pr = await conn.execute_fetchall(
        "SELECT binding_key FROM issue_prs WHERE issue_id = ?", (issue_id,)
    )
    assert pr[0]["binding_key"] == expected
    await conn.close()


@pytest.mark.asyncio
async def test_backfill_mapping_outside_candidates_rejected(tmp_path: Path) -> None:
    """A mapping entry naming a binding outside the row's own
    team/provider/site/repo candidates is a stale or mistyped natural key, not
    a disambiguation of this row — the importer refuses rather than trusting
    it and stamping the row with an unrelated binding."""
    conn = await db.connect(tmp_path / "state.sqlite")
    await _seed_active_work(conn, identifier="ENG-1", team_key="ENG", github_repo="org/api")
    path = _write(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    issue_label: urgent
{_STATES}
  - linear_team_key: ENG
    github_repo: org/api
{_STATES}
  - linear_team_key: WEB
    github_repo: org/web
{_STATES}
""",
    )
    with pytest.raises(ConfigImportError, match="ENG-1"):
        await import_config(
            path,
            conn,
            now="t1",
            issue_bindings={"ENG-1": ["WEB", "org/web", "", "linear", "default"]},
        )
    # Rolled back: no bindings landed, and nothing was stamped with the
    # out-of-scope binding's key.
    assert await db.config_bindings.count(conn) == 0
    await conn.close()


@pytest.mark.asyncio
async def test_round_trip_export_restore(tmp_path: Path) -> None:
    """export (restore) → import (replace) reproduces the DB config exactly,
    including a disabled binding, priorities, and the global roles matrix."""
    from symphony.config_export import export_config

    body = f"""
roles:
  review_find:
    agent: codex
    model: gpt-5.1-codex-max
repos:
  - linear_team_key: WEB
    github_repo: org/web
    max_concurrent: 4
{_STATES}
  - linear_team_key: ENG
    github_repo: org/api
    enabled: false
{_STATES}
"""
    conn = await db.connect(tmp_path / "state.sqlite")
    path = _write(tmp_path, body)
    await import_config(path, conn, now="t1")
    before = await db.config_bindings.list_all(conn)
    before_globals = await db.config_globals.get(conn)

    rows = await db.config_bindings.list_all(conn)
    yaml_text = export_config(
        rows,
        before_globals.roles if before_globals else {},
        set(),
        mode="restore",
    )
    export_path = tmp_path / "export.yaml"
    export_path.write_text(yaml_text)
    await import_config(export_path, conn, replace=True, now="t2")

    after = await db.config_bindings.list_all(conn)
    after_globals = await db.config_globals.get(conn)
    assert [(r.project_key, r.payload, r.enabled, r.priority) for r in after] == [
        (r.project_key, r.payload, r.enabled, r.priority) for r in before
    ]
    assert after_globals.roles == before_globals.roles  # type: ignore[union-attr]
    await conn.close()


@pytest.mark.asyncio
async def test_import_skips_webhook_secret_placeholder(tmp_path: Path) -> None:
    """An un-edited restore export carries the export placeholder, not a real
    secret — importing it must not persist the placeholder as a real webhook
    secret (SYM-195)."""
    from symphony.config_export import WEBHOOK_SECRET_PLACEHOLDER

    conn, _result, _rows, _globals_row = await _import(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    webhook_secret: {WEBHOOK_SECRET_PLACEHOLDER}
{_STATES}
""",
    )
    secret = await db.config_repo_secrets.get(conn, "org/api")
    assert secret is None
    await conn.close()


@pytest.mark.asyncio
async def test_import_rejects_redacted_mcp_placeholder(tmp_path: Path) -> None:
    """An un-edited restore export carries `true` in place of an `mcp_servers`
    env/headers credential — unlike `webhook_secret`, there's no field to
    skip, so the importer must refuse rather than install the literal `True`
    as a broken credential (SYM-195 review)."""
    conn = await db.connect(tmp_path / "state.sqlite")
    path = _write(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
    mcp_servers:
      supabase:
        command: npx
        env:
          API_KEY: true
{_STATES}
""",
    )
    with pytest.raises(ConfigImportError, match="mcp_servers.supabase.env.API_KEY"):
        await import_config(path, conn, now="t1")
    assert await db.config_bindings.count(conn) == 0
    await conn.close()


@pytest.mark.asyncio
async def test_failed_replace_does_not_delete_existing_bindings(tmp_path: Path) -> None:
    """A `--replace` import that fails partway (duplicate natural key across
    two unlabeled bindings on the same scope) must roll back the delete too —
    not leave the DB with the old bindings gone and nothing written back."""
    conn, _result, _rows, _globals_row = await _import(
        tmp_path,
        f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api
{_STATES}
""",
    )
    bad_path = _write(
        tmp_path,
        f"""
repos:
  - linear_team_key: WEB
    github_repo: org/web
{_STATES}
  - linear_team_key: WEB
    github_repo: org/web
{_STATES}
""",
    )
    with pytest.raises(Exception):  # noqa: B017 — sqlite3.IntegrityError, not wrapped
        await import_config(bad_path, conn, replace=True, now="t2")
    rows = await db.config_bindings.list_all(conn)
    assert [r.project_key for r in rows] == ["ENG"]
    await conn.close()
