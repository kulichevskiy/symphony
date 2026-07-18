"""Codex device-auth login over the real app harness (OAuth in UI 6/7).

`start` spawns the (faked) `codex login --device-auth` and returns its
verification URL + user code + a login-session id; `poll` reports pending until
the (faked) subprocess exits, then stores the produced credentials *encrypted*
and reaches `connected`; `test` reports live/expired off the stored token
expiry; `disconnect` clears the row. The credential blob never appears in a
response.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from symphony import db
from symphony.app import create_app
from symphony.codex_login import CodexDeviceAuth, CodexPollResult
from symphony.crypto import CredentialCipher

from .test_auth import JWKS_URI, _jwks, _settings, _token
from .test_webhook import _Handler

_KEY = "deployment-secret"
_FUTURE = 4102444800  # 2100-01-01 (epoch seconds)
_PAST = 1000000000  # 2001-09-09


def _jwt(exp: int) -> str:
    def seg(obj: dict[str, object]) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{seg({'alg': 'RS256'})}.{seg({'exp': exp})}.sig"


def _credential(exp: int = _FUTURE) -> str:
    return json.dumps({"tokens": {"access_token": _jwt(exp), "refresh_token": "sk-codex-refresh"}})


class _FakeLogin:
    """Fakes a device-auth subprocess: `poll` returns `pending` for the first
    `pending_polls` calls, then `success` with the credential."""

    def __init__(self, credential: str, *, pending_polls: int = 1) -> None:
        self._credential = credential
        self._pending_polls = pending_polls
        self.polls = 0
        self.closed = False

    async def start(self) -> CodexDeviceAuth:
        return CodexDeviceAuth(
            verification_uri="https://auth.openai.com/device", user_code="WDJB-MJHT"
        )

    async def poll(self) -> CodexPollResult:
        self.polls += 1
        if self.polls <= self._pending_polls:
            return CodexPollResult(status="pending")
        return CodexPollResult(status="success", credential=self._credential)

    async def close(self) -> None:
        self.closed = True


def _app(
    conn: Any,
    db_path: Path,
    *,
    login: _FakeLogin | None = None,
    auth: bool = False,
    codex_credentials_path: Path | None = None,
) -> Any:
    fake = login or _FakeLogin(_credential())
    return create_app(
        _Handler(),
        conn,
        ui_enabled=True,
        ui_db_path=db_path,
        oauth_cipher=CredentialCipher(_KEY),
        codex_login_factory=lambda: fake,
        auth0_settings=_settings() if auth else None,
        codex_credentials_path=codex_credentials_path,
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
async def test_start_returns_url_code_and_session(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        async with _client(_app(conn, db_path)) as client:
            resp = await client.get("/api/oauth/codex/start")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["verification_uri"] == "https://auth.openai.com/device"
        assert body["user_code"] == "WDJB-MJHT"
        assert body["login_session"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_poll_pending_then_connects_and_stores_encrypted(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, login=_FakeLogin(_credential(), pending_polls=1))
        async with _client(app) as client:
            session = (await client.get("/api/oauth/codex/start")).json()["login_session"]
            pending = await client.post("/api/oauth/codex/poll", json={"login_session": session})
            assert pending.status_code == 200, pending.text
            assert pending.json()["status"] == "pending"

            done = await client.post("/api/oauth/codex/poll", json={"login_session": session})
        assert done.status_code == 200, done.text
        assert done.json()["status"] == "connected"
        # The credential blob is never in a response body.
        assert "sk-codex-refresh" not in pending.text
        assert "sk-codex-refresh" not in done.text

        status = await db.oauth_connections.get_status(conn, "codex")
        assert status is not None and status.status == "connected"
        assert status.expires_at == "2100-01-01T00:00:00Z"
        cur = await conn.execute("SELECT credential FROM oauth_connections WHERE provider='codex'")
        row = await cur.fetchone()
        assert b"sk-codex-refresh" not in bytes(row["credential"])
        stored = await db.oauth_connections.get_credential(conn, "codex", CredentialCipher(_KEY))
        assert stored is not None and "sk-codex-refresh" in stored
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_poll_rejects_unknown_session(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        async with _client(_app(conn, db_path)) as client:
            resp = await client.post("/api/oauth/codex/poll", json={"login_session": "nope"})
        assert resp.status_code == 404
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_poll_session_consumed_after_success(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, login=_FakeLogin(_credential(), pending_polls=0))
        async with _client(app) as client:
            session = (await client.get("/api/oauth/codex/start")).json()["login_session"]
            first = await client.post("/api/oauth/codex/poll", json={"login_session": session})
            assert first.json()["status"] == "connected"
            # The session is popped on success — polling it again 404s.
            again = await client.post("/api/oauth/codex/poll", json={"login_session": session})
            assert again.status_code == 404
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_poll_failure_reports_failed_and_drops_session(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        login = _FakeLogin(_credential())

        async def _fail() -> CodexPollResult:
            return CodexPollResult(status="failed")

        login.poll = _fail  # type: ignore[method-assign]
        app = _app(conn, db_path, login=login)
        async with _client(app) as client:
            session = (await client.get("/api/oauth/codex/start")).json()["login_session"]
            resp = await client.post("/api/oauth/codex/poll", json={"login_session": session})
            assert resp.status_code == 200
            assert resp.json()["status"] == "failed"
            again = await client.post("/api/oauth/codex/poll", json={"login_session": session})
            assert again.status_code == 404
        assert login.closed is True
        assert await db.oauth_connections.get_status(conn, "codex") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_test_reports_live_then_expired(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, login=_FakeLogin(_credential(_FUTURE), pending_polls=0))
        async with _client(app) as client:
            session = (await client.get("/api/oauth/codex/start")).json()["login_session"]
            await client.post("/api/oauth/codex/poll", json={"login_session": session})
            live = await client.post("/api/oauth/codex/test")
        assert live.status_code == 200 and live.json()["status"] == "live"

        # A lapsed access-token JWT with a stored refresh token is still a
        # working connection — the CLI refreshes in its per-run dir.
        app2 = _app(conn, db_path, login=_FakeLogin(_credential(_PAST), pending_polls=0))
        async with _client(app2) as client:
            session = (await client.get("/api/oauth/codex/start")).json()["login_session"]
            await client.post("/api/oauth/codex/poll", json={"login_session": session})
            refreshable = await client.post("/api/oauth/codex/test")
        assert refreshable.json()["status"] == "live"

        # Only a lapsed JWT with NO refresh token is dead.
        refreshless = json.dumps({"tokens": {"access_token": _jwt(_PAST)}})
        app3 = _app(conn, db_path, login=_FakeLogin(refreshless, pending_polls=0))
        async with _client(app3) as client:
            session = (await client.get("/api/oauth/codex/start")).json()["login_session"]
            await client.post("/api/oauth/codex/poll", json={"login_session": session})
            expired = await client.post("/api/oauth/codex/test")
        assert expired.json()["status"] == "expired"
        status = await db.oauth_connections.get_status(conn, "codex")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_disconnect_clears_row_and_local_file(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        creds_path = tmp_path / "auth.json"
        creds_path.write_text("{}", encoding="utf-8")
        app = _app(
            conn,
            db_path,
            login=_FakeLogin(_credential(), pending_polls=0),
            codex_credentials_path=creds_path,
        )
        async with _client(app) as client:
            session = (await client.get("/api/oauth/codex/start")).json()["login_session"]
            await client.post("/api/oauth/codex/poll", json={"login_session": session})
            resp = await client.post("/api/oauth/codex/disconnect")
        assert resp.status_code == 200
        assert await db.oauth_connections.get_status(conn, "codex") is None
        assert not creds_path.exists()
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
                assert (await client.get("/api/oauth/codex/start")).status_code == 401
                authed = await client.get(
                    "/api/oauth/codex/start", headers={"Authorization": f"Bearer {_token()}"}
                )
                assert authed.status_code == 200
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_test_does_not_resurrect_auth_failure_expiry(tmp_path: Path) -> None:
    """A row expired by a real 401 (updated_by=auth-failure) must stay expired
    through offline Test — only a reconnect clears it, or the dispatch gate is
    defeated (Config v2 5/9 + 6/9 review fix)."""
    conn, db_path = await _open(tmp_path)
    try:
        # A live-looking credential, but the row was expired by an auth failure.
        await db.oauth_connections.set_connection(
            conn,
            provider="codex",
            credential=_credential(_FUTURE),
            cipher=CredentialCipher(_KEY),
            status="connected",
            updated_by="oauth",
        )
        await db.oauth_connections.update_status(
            conn, provider="codex", status="expired", updated_by="auth-failure"
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/codex/test")
        assert resp.json()["status"] == "expired"
        status = await db.oauth_connections.get_status(conn, "codex")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()
