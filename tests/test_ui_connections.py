"""Read-only Connections API (OAuth in UI 1/7) over the real app harness.

Asserts the HTTP seam: four providers all `not connected` on a fresh DB, no
credential material ever serialized, and the Auth0 gate applies.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from symphony import db
from symphony.app import create_app
from symphony.config import Config
from symphony.crypto import KEY_FILE_NAME, CredentialCipher, key_fingerprint

from .test_auth import JWKS_URI, _jwks, _settings, _token
from .test_webhook import _Handler


def _app(conn: Any, db_path: Path, *, auth: bool = False) -> Any:
    return create_app(
        _Handler(),
        conn,
        ui_enabled=True,
        ui_db_path=db_path,
        ui_external_config=Config(linear_api_key="test-linear-key"),
        auth0_settings=_settings() if auth else None,
    )


async def _open(tmp_path: Path) -> tuple[Any, Path]:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    return conn, db_path


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_fresh_db_reports_four_providers_not_connected(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/connections")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert [c["provider"] for c in body] == ["github", "linear", "claude", "codex"]
        assert all(c["status"] == "not_connected" for c in body)
        assert all(c["expires_at"] is None for c in body)
        # No credential material is ever serialized.
        for card in body:
            assert "credential" not in card
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_status_and_expiry_surface_without_credential(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn,
            provider="github",
            credential="gho_super_secret",
            cipher=CredentialCipher("k"),
            status="connected",
            expires_at="2026-08-01T00:00:00Z",
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/connections")
        assert resp.status_code == 200, resp.text
        github = next(c for c in resp.json() if c["provider"] == "github")
        assert github["status"] == "connected"
        assert github["expires_at"] == "2026-08-01T00:00:00Z"
        # The secret never appears anywhere in the response body.
        assert "gho_super_secret" not in resp.text
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_connections_gated_behind_auth(tmp_path: Path) -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, auth=True)
        async with _client(app) as client:
            unauth = await client.get("/api/connections")
            assert unauth.status_code == 401
            authed = await client.get(
                "/api/connections", headers={"Authorization": f"Bearer {_token()}"}
            )
            assert authed.status_code == 200
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_key_fingerprint_endpoint_and_auto_provisioned_key(tmp_path: Path) -> None:
    """Config v2 2/9: with no explicit key configured, `create_app` auto-
    provisions one next to the DB and the API serves its fingerprint —
    never the key itself."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)  # ui_external_config has no encryption key set
        key_path = tmp_path / KEY_FILE_NAME
        assert key_path.exists()  # auto-provisioned at app construction
        key = key_path.read_text(encoding="utf-8").strip()
        async with _client(app) as client:
            resp = await client.get("/api/connections/key")
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"fingerprint": key_fingerprint(key)}
        assert key not in resp.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_key_fingerprint_reflects_explicit_key(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(
                linear_api_key="test-linear-key",
                symphony_encryption_key="explicit-secret",
            ),
        )
        async with _client(app) as client:
            resp = await client.get("/api/connections/key")
        assert resp.json() == {"fingerprint": key_fingerprint("explicit-secret")}
        # An explicit key never writes the key file.
        assert not (tmp_path / KEY_FILE_NAME).exists()
    finally:
        await conn.close()
