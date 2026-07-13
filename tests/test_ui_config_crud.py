"""Binding CRUD over the app harness — real FastAPI app, temp SQLite (SYM-190).

Asserts external behavior at the HTTP seam: CRUD round-trips, duplicate-selector
rejection, optimistic-lock conflicts, field-path validation errors, `updated_by`
stamping, secret redaction, and the options payload.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from symphony import db
from symphony.app import create_app
from symphony.config import Config
from symphony.db import config_bindings

from .test_auth import JWKS_URI, _jwks, _settings, _token
from .test_webhook import _Handler


def _payload(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "project_key": "ENG",
        "github_repo": "org/repo",
        "states": {"ready": "Todo"},
    }
    base.update(overrides)
    return base


def _app(
    conn: Any,
    db_path: Path,
    *,
    auth: bool = False,
    github_webhook_secret: str = "test-global-secret",
    linear_api_key: str = "test-linear-key",
) -> Any:
    # A global secret/key is set by default so tests unrelated to a specific
    # check don't trip it; the tests for each check override the relevant
    # kwarg to empty to exercise it directly.
    return create_app(
        _Handler(),
        conn,
        ui_enabled=True,
        ui_db_path=db_path,
        ui_external_config=Config(
            github_webhook_secret=github_webhook_secret,
            linear_api_key=linear_api_key,
        ),
        auth0_settings=_settings() if auth else None,
    )


async def _open(tmp_path: Path) -> tuple[Any, Path]:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    return conn, db_path


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_crud_round_trip(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(max_concurrent=3), "priority": 2},
            )
            assert created.status_code == 201, created.text
            rec = created.json()
            bid = rec["id"]
            assert rec["version"] == 1
            assert rec["updated_by"] == "local"
            assert rec["priority"] == 2
            assert rec["payload"]["max_concurrent"] == 3

            listed = await client.get("/api/config/bindings")
            assert [b["id"] for b in listed.json()] == [bid]

            got = await client.get(f"/api/config/bindings/{bid}")
            assert got.status_code == 200 and got.json()["id"] == bid

            updated = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": _payload(max_concurrent=5),
                    "version": 1,
                },
            )
            assert updated.status_code == 200, updated.text
            assert updated.json()["version"] == 2
            assert updated.json()["enabled"] is True
            assert updated.json()["payload"]["max_concurrent"] == 5

            deleted = await client.delete(f"/api/config/bindings/{bid}?version=2")
            assert deleted.status_code == 204
            assert (await client.get("/api/config/bindings")).json() == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_binding_crud_survives_shared_connection_being_closed(
    tmp_path: Path,
) -> None:
    """The CRUD router must write through its own connection (`WriteDbPool`),
    never the daemon's shared `conn` — a `commit()` on a connection the
    orchestrator also writes through would flush whatever unrelated,
    not-yet-committed statements it had pending at that moment (SYM-190).
    Closing `conn` before any CRUD request proves the router never touches
    it: with the old shared-connection wiring this would fail with
    "Cannot operate on a closed database"."""
    conn, db_path = await _open(tmp_path)
    app = _app(conn, db_path)
    await conn.close()
    async with _client(app) as client:
        created = await client.post("/api/config/bindings", json={"payload": _payload()})
        assert created.status_code == 201, created.text
        listed = await client.get("/api/config/bindings")
        assert [b["id"] for b in listed.json()] == [created.json()["id"]]


@pytest.mark.asyncio
async def test_duplicate_selector_rejected_including_unlabeled(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            # Unlabeled catch-all on repo-a.
            first = await client.post(
                "/api/config/bindings", json={"payload": _payload(github_repo="org/a")}
            )
            assert first.status_code == 201
            # Same scope + (empty) label + ready state, different repo → exact
            # duplicate selector, rejected.
            dup = await client.post(
                "/api/config/bindings", json={"payload": _payload(github_repo="org/b")}
            )
            assert dup.status_code == 422
            assert dup.json()["detail"][0]["loc"] == ["issue_label"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_same_label_different_ready_state_accepted(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            a = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        github_repo="org/a", issue_label="bug", states={"ready": "Backlog"}
                    )
                },
            )
            assert a.status_code == 201, a.text
            # Same tracker scope + same label but a different ready lane — a
            # legitimate two-lane setup, accepted.
            b = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        github_repo="org/b", issue_label="bug", states={"ready": "Todo"}
                    )
                },
            )
            assert b.status_code == 201, b.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_same_scope_different_ready_state_same_repo_rolls_back(tmp_path: Path) -> None:
    """Same project/repo/label/site with a different ready state passes the
    selector duplicate check (ready state makes it a legitimate two-lane
    setup) but the DB's unique index — keyed on natural-key columns only, no
    ready state — still catches it. The resulting `IntegrityError` must roll
    back the write transaction the failed `INSERT` opened, or it holds the
    write lock until some later, unrelated commit closes it."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            first = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(issue_label="bug", states={"ready": "Backlog"})},
            )
            assert first.status_code == 201, first.text
            dup = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(issue_label="bug", states={"ready": "Todo"})},
            )
            assert dup.status_code == 422
            assert dup.json()["detail"][0]["loc"] == ["github_repo"]
            assert not conn.in_transaction

            # The connection must still be writable afterwards.
            follow_up = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(github_repo="org/other")},
            )
            assert follow_up.status_code == 201, follow_up.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_disabled_write_rejected(tmp_path: Path) -> None:
    """`enabled: false` has no runtime effect until SYM-193 — the write path
    rejects it outright rather than let an operator believe a binding is
    disabled when the daemon still dispatches it (see effective_config.py)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created_disabled = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(), "enabled": False},
            )
            assert created_disabled.status_code == 422
            assert created_disabled.json()["detail"][0]["loc"] == ["enabled"]

            created = await client.post("/api/config/bindings", json={"payload": _payload()})
            assert created.status_code == 201, created.text
            bid = created.json()["id"]

            updated_disabled = await client.put(
                f"/api/config/bindings/{bid}",
                json={"payload": _payload(), "version": 1, "enabled": False},
            )
            assert updated_disabled.status_code == 422
            assert updated_disabled.json()["detail"][0]["loc"] == ["enabled"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_version_conflict(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            bid = (await client.post("/api/config/bindings", json={"payload": _payload()})).json()[
                "id"
            ]
            stale = await client.put(
                f"/api/config/bindings/{bid}",
                json={"payload": _payload(max_concurrent=4), "version": 99},
            )
            assert stale.status_code == 409
            assert stale.json()["detail"]["current_version"] == 1
            # DELETE with a stale version conflicts too.
            del_stale = await client.delete(f"/api/config/bindings/{bid}?version=99")
            assert del_stale.status_code == 409
            # Neither conflict left a write transaction open on the shared
            # connection (both roll back before raising).
            assert conn.in_transaction is False

            # A same-connection write right after the conflicts still lands
            # cleanly — proof the dangling transaction isn't blocking it.
            retry = await client.put(
                f"/api/config/bindings/{bid}",
                json={"payload": _payload(max_concurrent=4), "version": 1},
            )
            assert retry.status_code == 200, retry.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_field_validation_error_carries_loc(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            # Missing required `states.ready`.
            bad_states = await client.post(
                "/api/config/bindings",
                json={"payload": {"project_key": "ENG", "github_repo": "org/repo", "states": {}}},
            )
            assert bad_states.status_code == 422
            locs = [tuple(err["loc"]) for err in bad_states.json()["detail"]]
            assert ("states", "ready") in locs

            # Bad merge strategy → the exact field.
            bad_merge = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(merge_strategy="fast-forward")},
            )
            assert bad_merge.status_code == 422
            assert any(err["loc"][-1] == "merge_strategy" for err in bad_merge.json()["detail"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_legacy_role_field_rejected_with_loc(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings", json={"payload": _payload(agent="codex")}
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["agent"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_unknown_field_rejected_with_loc(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings", json={"payload": _payload(max_concurent=3)}
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["max_concurent"]

            # Persisted `.env`/YAML-alias spellings are legal, not unknown.
            aliased = await client.post(
                "/api/config/bindings",
                json={
                    "payload": {
                        "linear_team_key": "ENG",
                        "github_repo": "org/repo2",
                        "linear_states": {"ready": "Todo"},
                    }
                },
            )
            assert aliased.status_code == 201
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_matrix_error_shows_roles_path(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        roles={"implement": {"agent": "codex", "model": "not-a-model"}}
                    )
                },
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["roles"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_unknown_env_key_fails_closed(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(env={"MY_TOKEN": "DEFINITELY_NOT_A_REAL_ENV_KEY_XZ"})},
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["env"]
            assert "DEFINITELY_NOT_A_REAL_ENV_KEY_XZ" in resp.json()["detail"][0]["msg"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_secret_redacted_on_read(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_secret="s3cr3t-should-never-leak")},
            )
            assert created.status_code == 201
            assert "s3cr3t-should-never-leak" not in created.text
            assert created.json()["webhook_secret_set"] is True
            assert "webhook_secret" not in created.json()["payload"]

            listed = await client.get("/api/config/bindings")
            assert "s3cr3t-should-never-leak" not in listed.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_secret_survives_edit_of_redacted_payload(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_secret="s3cr3t-should-never-leak")},
            )
            assert created.status_code == 201, created.text
            bid = created.json()["id"]

            got = await client.get(f"/api/config/bindings/{bid}")
            assert got.json()["webhook_secret_set"] is True
            assert "webhook_secret" not in got.json()["payload"]

            # Edit re-sends exactly the redacted GET payload (as the form
            # does) — the stored secret must survive, not be cleared.
            put = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": {**got.json()["payload"], "max_concurrent": 7},
                    "enabled": got.json()["enabled"],
                    "priority": got.json()["priority"],
                    "version": got.json()["version"],
                },
            )
            assert put.status_code == 200, put.text
            assert put.json()["webhook_secret_set"] is True
            assert "s3cr3t-should-never-leak" not in put.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_mcp_server_secrets_redacted_on_read(tmp_path: Path) -> None:
    """`mcp_servers` has no `resolve_env`-style name indirection (unlike the
    top-level `env` field), so an operator embeds literal credentials
    straight into a server's `env`/`headers` — those must never appear in a
    response."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        mcp_servers={
                            "supabase": {
                                "command": "npx",
                                "env": {"API_KEY": "sk-should-never-leak"},
                            }
                        }
                    )
                },
            )
            assert created.status_code == 201, created.text
            assert "sk-should-never-leak" not in created.text
            assert created.json()["payload"]["mcp_servers"] == {
                "supabase": {"command": "npx", "env": {"API_KEY": True}}
            }

            listed = await client.get("/api/config/bindings")
            assert "sk-should-never-leak" not in listed.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_mcp_server_secrets_survive_edit_of_redacted_payload(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        mcp_servers={
                            "supabase": {
                                "command": "npx",
                                "env": {"API_KEY": "sk-should-never-leak"},
                            }
                        }
                    )
                },
            )
            bid = created.json()["id"]
            got = await client.get(f"/api/config/bindings/{bid}")

            # Edit re-sends exactly the redacted GET payload (as the
            # collapsible raw-JSON section does) plus an unrelated change —
            # the stored secret must survive, not be overwritten with the
            # `true` redaction placeholder.
            put = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": {**got.json()["payload"], "max_concurrent": 7},
                    "enabled": got.json()["enabled"],
                    "priority": got.json()["priority"],
                    "version": got.json()["version"],
                },
            )
            assert put.status_code == 200, put.text
            assert "sk-should-never-leak" not in put.text

            stored = await config_bindings.get(conn, bid)
            assert stored is not None
            assert stored.payload["mcp_servers"]["supabase"]["env"] == {
                "API_KEY": "sk-should-never-leak"
            }

            # A real edit (a literal string, not the `true` placeholder)
            # still overrides the stored secret.
            rotated = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": {
                        **put.json()["payload"],
                        "mcp_servers": {
                            "supabase": {"command": "npx", "env": {"API_KEY": "sk-rotated"}}
                        },
                    },
                    "enabled": put.json()["enabled"],
                    "priority": put.json()["priority"],
                    "version": put.json()["version"],
                },
            )
            assert rotated.status_code == 200, rotated.text
            stored = await config_bindings.get(conn, bid)
            assert stored is not None
            assert stored.payload["mcp_servers"]["supabase"]["env"] == {"API_KEY": "sk-rotated"}
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_diff_logged_without_secret_values(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    conn, db_path = await _open(tmp_path)
    caplog.set_level(logging.INFO, logger="symphony.ui.config_crud")
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        webhook_secret="topsecret",
                        max_concurrent=9,
                        mcp_servers={"supabase": {"env": {"API_KEY": "mcp-topsecret"}}},
                    )
                },
            )
            bid = created.json()["id"]
            await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": _payload(
                        webhook_secret="rotated",
                        max_concurrent=1,
                        mcp_servers={"supabase": {"env": {"API_KEY": "mcp-rotated"}}},
                    ),
                    "version": 1,
                },
            )
    finally:
        await conn.close()
    text = caplog.text
    assert "topsecret" not in text and "rotated" not in text
    assert "mcp-topsecret" not in text and "mcp-rotated" not in text
    assert "webhook_secret" in text  # the flag, not the value
    assert "max_concurrent" in text
    assert "mcp_servers" in text  # which servers changed, not their contents


@pytest.mark.asyncio
async def test_same_family_review_returns_nonblocking_warning(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            # Both implement and review on codex → save succeeds but warns.
            resp = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        roles={
                            "implement": {"agent": "codex"},
                            "review_find": {"agent": "codex"},
                            "review_verify": {"agent": "codex"},
                        }
                    )
                },
            )
            assert resp.status_code == 201, resp.text
            assert any("diversity" in w for w in resp.json()["warnings"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_enabled_without_secret_rejected(tmp_path: Path) -> None:
    """`webhook_enabled: true` with no per-binding secret and no global
    `GITHUB_WEBHOOK_SECRET` must fail closed at save time — otherwise the
    daemon's hot-reload path (`cli._live_github_webhook_settings`) swallows
    the misconfiguration and silently disables *every* repo's webhook
    verification, not just this one."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, github_webhook_secret="")
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_enabled=True)},
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["webhook_secret"]

            # A per-binding secret satisfies it.
            with_secret = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_enabled=True, webhook_secret="s3cr3t")},
            )
            assert with_secret.status_code == 201, with_secret.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_linear_binding_without_api_key_rejected(tmp_path: Path) -> None:
    """A fresh DB-owned install boots with zero bindings, so `cli._run`'s
    boot-time `LINEAR_API_KEY` check never fires before the first linear
    binding is created via the UI — this write path must be the fail-closed
    gate instead, or the binding saves cleanly and then silently never
    dispatches (`Orchestrator._reload_bindings` swallows the resulting
    `for_binding` `ValueError` and just logs it)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, linear_api_key="")
        async with _client(app) as client:
            resp = await client.post("/api/config/bindings", json={"payload": _payload()})
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["provider"]
            assert "LINEAR_API_KEY" in resp.json()["detail"][0]["msg"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_jira_binding_without_credentials_rejected(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(github_webhook_secret="test-global-secret"),
        )
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings", json={"payload": _payload(provider="jira")}
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["provider"]
            msg = resp.json()["detail"][0]["msg"]
            assert "JIRA_BASE_URL" in msg
            assert "JIRA_EMAIL" in msg
            assert "JIRA_API_TOKEN" in msg
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_jira_tracker_site_normalized_from_global_base_url(tmp_path: Path) -> None:
    """A Jira binding with no per-binding `base_url` keys on the global
    `jira_base_url`, matching `assemble_effective_config`'s resolution — not
    the "default" placeholder `RepoBinding.model_validate` alone would leave
    it with."""
    conn, db_path = await _open(tmp_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(
                jira_base_url="https://issues.example.com",
                jira_email="jira@example.com",
                jira_api_token="test-jira-token",
                github_webhook_secret="test-global-secret",
            ),
        )
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(provider="jira")},
            )
            assert resp.status_code == 201, resp.text
            assert resp.json()["tracker_site"] == "https://issues.example.com"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_duplicate_rejected_across_jira_bindings_relying_on_global_base_url(
    tmp_path: Path,
) -> None:
    """The duplicate-selector check must normalize *existing* rows the same
    way it normalizes the candidate: a stored Jira binding with no per-binding
    `base_url` has no `tracker_site` of its own in its payload, and without
    re-deriving it from the global `jira_base_url` it would compare against
    the "default" placeholder instead of the site it actually resolves to —
    letting an exact duplicate slip through."""
    conn, db_path = await _open(tmp_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(
                jira_base_url="https://issues.example.com",
                jira_email="jira@example.com",
                jira_api_token="test-jira-token",
                github_webhook_secret="test-global-secret",
            ),
        )
        async with _client(app) as client:
            first = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(provider="jira", github_repo="org/a")},
            )
            assert first.status_code == 201, first.text
            dup = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(provider="jira", github_repo="org/b")},
            )
            assert dup.status_code == 422
            assert dup.json()["detail"][0]["loc"] == ["issue_label"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_options_payload(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return None

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/config/options")
        body = resp.json()
        assert body["agent_families"] == ["claude", "codex"]
        assert body["merge_strategies"] == ["squash", "merge", "rebase"]
        assert set(body["claude_aliases"]) == {"opus", "sonnet", "haiku"}
        assert "gpt-5.1-codex" in body["codex_models"]
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_updated_by_uses_auth_email(tmp_path: Path) -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, auth=True)
        async with _client(app) as client:
            headers = {"Authorization": f"Bearer {_token()}"}
            created = await client.post(
                "/api/config/bindings", json={"payload": _payload()}, headers=headers
            )
            assert created.status_code == 201, created.text
            assert created.json()["updated_by"] == "alice@example.com"

            # Unauthenticated write is rejected by the shared gate.
            unauth = await client.post("/api/config/bindings", json={"payload": _payload()})
            assert unauth.status_code == 401
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_crud_router_not_mounted_when_yaml_owns_topology(tmp_path: Path) -> None:
    # A legacy YAML topology not yet imported keeps `reload_bindings_from_db`
    # off, so the daemon never applies a write made here — the router must not
    # mount rather than accept writes it will silently ignore.
    conn, db_path = await _open(tmp_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(github_webhook_secret="test-global-secret"),
            ui_db_owns_topology=False,
        )
        async with _client(app) as client:
            options = await client.get("/api/config/options")
            bindings = await client.get("/api/config/bindings")
            created = await client.post("/api/config/bindings", json={"payload": _payload()})
        assert options.status_code == 404
        assert bindings.status_code == 404
        assert created.status_code == 404
    finally:
        await conn.close()


# --- global roles matrix (SYM-191) -----------------------------------------


@pytest.mark.asyncio
async def test_roles_get_put_round_trip(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            # Fresh DB: no globals row yet → empty matrix at version 0.
            got = await client.get("/api/config/roles")
            assert got.status_code == 200
            assert got.json() == {"roles": {}, "version": 0}

            put = await client.put(
                "/api/config/roles",
                json={"roles": {"implement": {"agent": "codex"}}, "version": 0},
            )
            assert put.status_code == 200, put.text
            assert put.json()["version"] == 1
            assert put.json()["roles"] == {"implement": {"agent": "codex"}}

            reread = await client.get("/api/config/roles")
            assert reread.json() == {
                "roles": {"implement": {"agent": "codex"}},
                "version": 1,
            }
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_put_rejects_bad_effort_with_zero_bindings(tmp_path: Path) -> None:
    """A fresh DB has no bindings for the family/effort loop to walk — the
    global matrix must still be validated on its own (SYM-191 review)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            put = await client.put(
                "/api/config/roles",
                json={
                    "roles": {"implement": {"agent": "claude", "effort": "turbo"}},
                    "version": 0,
                },
            )
            assert put.status_code == 422, put.text
            assert put.json()["detail"][0]["loc"] == ["roles"]

            # Rejected, not persisted — a reread still shows the empty matrix.
            reread = await client.get("/api/config/roles")
            assert reread.json() == {"roles": {}, "version": 0}
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_put_version_conflict(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            first = await client.put(
                "/api/config/roles",
                json={"roles": {"implement": {"agent": "codex"}}, "version": 0},
            )
            assert first.status_code == 200
            # A second write still carrying the stale version 0 → 409.
            stale = await client.put(
                "/api/config/roles",
                json={"roles": {"implement": {"agent": "claude"}}, "version": 0},
            )
            assert stale.status_code == 409
            assert stale.json()["detail"]["current_version"] == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_put_diversity_warning_non_blocking(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post("/api/config/bindings", json={"payload": _payload()})
            assert created.status_code == 201, created.text
            # Bindings default implement→claude; forcing the reviewer to claude
            # too loses cross-family diversity — a non-blocking warning.
            put = await client.put(
                "/api/config/roles",
                json={"roles": {"review_find": {"agent": "claude"}}, "version": 0},
            )
            assert put.status_code == 200, put.text
            assert any("diversity" in w for w in put.json()["warnings"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_effort_with_inherited_model_saves(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """An effort override with no explicit model saves — it is family-checked
    against the resolved role, not rejected for lacking a model (SYM-191)."""

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return None

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(roles={"implement": {"effort": "high"}})},
            )
            assert created.status_code == 201, created.text
            assert created.json()["payload"]["roles"] == {"implement": {"effort": "high"}}
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_options_claude_efforts_per_model(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return {"opus": ["low", "high"], "sonnet": ["medium"]}.get(model, ["low"])

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/config/options")
        body = resp.json()
        assert body["claude_efforts_by_model"]["opus"] == ["low", "high"]
        assert body["claude_efforts_by_model"]["sonnet"] == ["medium"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_options_claude_efforts_fall_back_without_key(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """No ANTHROPIC_API_KEY → the capability fetch returns None; the endpoint
    falls back to the family-wide effort set rather than an empty list."""

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return None

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/config/options")
        body = resp.json()
        assert set(body["claude_efforts_by_model"]["opus"]) == {
            "low",
            "medium",
            "high",
            "xhigh",
            "max",
        }
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_save_rejects_unsupported_claude_effort(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A claude (model, effort) pair the live capability check rejects fails at
    save with a `roles` field path — not silently at dispatch."""

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return ["low", "medium"]

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        roles={"implement": {"agent": "claude", "model": "opus", "effort": "xhigh"}}
                    )
                },
            )
            assert resp.status_code == 422, resp.text
            assert resp.json()["detail"][0]["loc"] == ["roles"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_effort_on_unrelated_binding_does_not_block_save(
    tmp_path: Path, monkeypatch  # type: ignore[no-untyped-def]
) -> None:
    """A now-unsupported claude (model, effort) on binding A (e.g. saved
    fail-open before ANTHROPIC_API_KEY was configured) must not fail a save on
    unrelated binding B: `_reject_unsupported_efforts` is scoped to the
    candidate for a single-binding save."""

    caps: list[str] = ["low", "medium", "high", "xhigh", "max"]

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return caps

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created_a = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        project_key="ENG-A",
                        github_repo="org/repo-a",
                        roles={
                            "implement": {"agent": "claude", "model": "opus", "effort": "xhigh"}
                        },
                    )
                },
            )
            assert created_a.status_code == 201, created_a.text

            # Capability check now rejects binding A's stored effort — as if
            # the key/model capabilities changed after A was saved.
            caps[:] = ["low", "medium"]

            created_b = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        project_key="ENG-B",
                        github_repo="org/repo-b",
                        roles={"implement": {"agent": "claude", "model": "opus", "effort": "low"}},
                    )
                },
            )
            assert created_b.status_code == 201, created_b.text
    finally:
        await conn.close()
