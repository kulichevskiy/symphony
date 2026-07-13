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


def _app(conn: Any, db_path: Path, *, auth: bool = False) -> Any:
    return create_app(
        _Handler(),
        conn,
        ui_enabled=True,
        ui_db_path=db_path,
        ui_external_config=Config(),
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
                    "enabled": False,
                },
            )
            assert updated.status_code == 200, updated.text
            assert updated.json()["version"] == 2
            assert updated.json()["enabled"] is False
            assert updated.json()["payload"]["max_concurrent"] == 5

            deleted = await client.delete(f"/api/config/bindings/{bid}?version=2")
            assert deleted.status_code == 204
            assert (await client.get("/api/config/bindings")).json() == []
    finally:
        await conn.close()


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
async def test_disabled_binding_exempt_from_selector_check(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            await client.post("/api/config/bindings", json={"payload": _payload()})
            staged = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(github_repo="org/other"), "enabled": False},
            )
            assert staged.status_code == 201, staged.text
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
                json={"payload": _payload(webhook_secret="topsecret", max_concurrent=9)},
            )
            bid = created.json()["id"]
            await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": _payload(webhook_secret="rotated", max_concurrent=1),
                    "version": 1,
                },
            )
    finally:
        await conn.close()
    text = caplog.text
    assert "topsecret" not in text and "rotated" not in text
    assert "webhook_secret" in text  # the flag, not the value
    assert "max_concurrent" in text


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
async def test_options_payload(tmp_path: Path) -> None:
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
