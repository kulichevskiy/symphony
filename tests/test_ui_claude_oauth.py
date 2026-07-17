"""Claude code-paste login over the real app harness (OAuth in UI 5/7).

`start` spawns the (faked) `claude` login and returns its OAuth URL + a
login-session id; `submit-code` feeds the pasted code and stores the produced
credentials *encrypted*; `test` reports live/expired off the stored `expiresAt`;
`disconnect` clears the row. The credential blob never appears in a response.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from symphony import db
from symphony.app import create_app
from symphony.crypto import CredentialCipher

from .test_auth import JWKS_URI, _jwks, _settings, _token
from .test_webhook import _Handler

_KEY = "deployment-secret"
_FUTURE_MS = 4102444800000  # 2100-01-01
_PAST_MS = 1000000000000  # 2001-09-09


def _credential(expires_ms: int = _FUTURE_MS) -> str:
    return json.dumps(
        {"claudeAiOauth": {"accessToken": "sk-ant-oat-secret", "expiresAt": expires_ms}}
    )


class _FakeLogin:
    def __init__(self, credential: str) -> None:
        self._credential = credential
        self.submitted: str | None = None

    async def start(self) -> str:
        return "https://claude.ai/oauth/authorize?code=1&state=xyz"

    async def submit_code(self, code: str) -> str:
        self.submitted = code
        return self._credential


def _app(
    conn: Any,
    db_path: Path,
    *,
    credential: str | None = None,
    auth: bool = False,
    claude_credentials_path: Path | None = None,
) -> Any:
    return create_app(
        _Handler(),
        conn,
        ui_enabled=True,
        ui_db_path=db_path,
        oauth_cipher=CredentialCipher(_KEY),
        claude_login_factory=lambda: _FakeLogin(credential or _credential()),
        auth0_settings=_settings() if auth else None,
        claude_credentials_path=claude_credentials_path,
    )


async def _open(tmp_path: Path) -> tuple[Any, Path]:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    return conn, db_path


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    )


@pytest.mark.asyncio
async def test_start_returns_url_and_session(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        async with _client(_app(conn, db_path)) as client:
            resp = await client.get("/api/oauth/claude/start")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["authorize_url"].startswith("https://claude.ai/oauth/authorize")
        assert body["login_session"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_submit_code_stores_encrypted_and_connects(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            session = (await client.get("/api/oauth/claude/start")).json()["login_session"]
            resp = await client.post(
                "/api/oauth/claude/submit-code",
                json={"login_session": session, "code": "the-pasted-code"},
            )
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "connected"
        # The credential blob is never in the response body.
        assert "sk-ant-oat-secret" not in resp.text

        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "connected"
        assert status.expires_at == "2100-01-01T00:00:00Z"
        # Stored encrypted, not plaintext.
        cur = await conn.execute("SELECT credential FROM oauth_connections WHERE provider='claude'")
        row = await cur.fetchone()
        assert b"sk-ant-oat-secret" not in bytes(row["credential"])
        stored = await db.oauth_connections.get_credential(conn, "claude", CredentialCipher(_KEY))
        assert stored is not None and "sk-ant-oat-secret" in stored
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_submit_code_rejects_unknown_session(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        async with _client(_app(conn, db_path)) as client:
            resp = await client.post(
                "/api/oauth/claude/submit-code",
                json={"login_session": "never-issued", "code": "x"},
            )
        assert resp.status_code == 404
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_login_session_is_single_use(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            session = (await client.get("/api/oauth/claude/start")).json()["login_session"]
            first = await client.post(
                "/api/oauth/claude/submit-code", json={"login_session": session, "code": "c"}
            )
            assert first.status_code == 200
            second = await client.post(
                "/api/oauth/claude/submit-code", json={"login_session": session, "code": "c"}
            )
            assert second.status_code == 404
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_test_reports_live_then_expired(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        # Live token.
        app = _app(conn, db_path, credential=_credential(_FUTURE_MS))
        async with _client(app) as client:
            session = (await client.get("/api/oauth/claude/start")).json()["login_session"]
            await client.post(
                "/api/oauth/claude/submit-code", json={"login_session": session, "code": "c"}
            )
            live = await client.post("/api/oauth/claude/test")
        assert live.status_code == 200
        assert live.json()["status"] == "live"

        # Expired token → test flips the card to expired.
        app2 = _app(conn, db_path, credential=_credential(_PAST_MS))
        async with _client(app2) as client:
            session = (await client.get("/api/oauth/claude/start")).json()["login_session"]
            await client.post(
                "/api/oauth/claude/submit-code", json={"login_session": session, "code": "c"}
            )
            expired = await client.post("/api/oauth/claude/test")
        assert expired.json()["status"] == "expired"
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_disconnect_clears_row(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            session = (await client.get("/api/oauth/claude/start")).json()["login_session"]
            await client.post(
                "/api/oauth/claude/submit-code", json={"login_session": session, "code": "c"}
            )
            resp = await client.post("/api/oauth/claude/disconnect")
        assert resp.status_code == 200
        assert await db.oauth_connections.get_status(conn, "claude") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_disconnect_removes_local_credentials_file(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        creds_path = tmp_path / ".credentials.json"
        creds_path.write_text("{}", encoding="utf-8")
        app = _app(conn, db_path, claude_credentials_path=creds_path)
        async with _client(app) as client:
            session = (await client.get("/api/oauth/claude/start")).json()["login_session"]
            await client.post(
                "/api/oauth/claude/submit-code", json={"login_session": session, "code": "c"}
            )
            resp = await client.post("/api/oauth/claude/disconnect")
        assert resp.status_code == 200
        assert not creds_path.exists()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_disconnect_without_local_file_still_succeeds(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, claude_credentials_path=tmp_path / "nope" / ".credentials.json")
        async with _client(app) as client:
            resp = await client.post("/api/oauth/claude/disconnect")
        assert resp.status_code == 200
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_start_is_auth_gated(tmp_path: Path) -> None:
    import respx

    with respx.mock:
        respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
        conn, db_path = await _open(tmp_path)
        try:
            app = _app(conn, db_path, auth=True)
            async with _client(app) as client:
                assert (await client.get("/api/oauth/claude/start")).status_code == 401
                authed = await client.get(
                    "/api/oauth/claude/start", headers={"Authorization": f"Bearer {_token()}"}
                )
                assert authed.status_code == 200
        finally:
            await conn.close()
