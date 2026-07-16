"""GitHub redirect-OAuth over the real app harness (OAuth in UI 2/7).

Asserts the HTTP seam: `start` is Auth0-gated and mints a single-use state +
PKCE, the ungated `callback` rejects a missing/unknown/replayed state and on a
(mocked) code exchange stores the token *encrypted* and flips the card to
`connected` without the token ever appearing in a response/log, and the
`disconnect`/`test` buttons clear the row / ping `GET /user`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx

from symphony import db
from symphony.app import create_app
from symphony.config import Config
from symphony.crypto import CredentialCipher

from .test_auth import JWKS_URI, _jwks, _settings, _token
from .test_webhook import _Handler

_KEY = "deployment-secret"


def _cfg() -> Config:
    return Config(
        linear_api_key="test-linear-key",
        github_oauth_client_id="gh-client-id",
        github_oauth_client_secret="gh-client-secret",
        linear_oauth_client_id="lin-client-id",
        linear_oauth_client_secret="lin-client-secret",
    )


def _app(conn: Any, db_path: Path, *, auth: bool = False, cfg: Config | None = None) -> Any:
    return create_app(
        _Handler(),
        conn,
        ui_enabled=True,
        ui_db_path=db_path,
        ui_external_config=cfg if cfg is not None else _cfg(),
        oauth_cipher=CredentialCipher(_KEY),
        auth0_settings=_settings() if auth else None,
    )


async def _open(tmp_path: Path) -> tuple[Any, Path]:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    return conn, db_path


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=False,
    )


@pytest.mark.asyncio
async def test_start_returns_github_authorize_url(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/oauth/github/start")
        assert resp.status_code == 200, resp.text
        url = resp.json()["authorize_url"]
        parsed = urlparse(url)
        assert parsed.netloc == "github.com"
        q = parse_qs(parsed.query)
        assert q["client_id"] == ["gh-client-id"]
        assert q["scope"] == ["repo workflow"]
        assert q["code_challenge_method"] == ["S256"]
        assert q["redirect_uri"] == ["http://test/api/oauth/github/callback"]
        assert q["state"]  # a state was minted
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_start_uses_configured_public_origin(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        cfg = _cfg().model_copy(
            update={"symphony_oauth_public_origin": "https://symphony.example.com"}
        )
        app = _app(conn, db_path, cfg=cfg)
        async with _client(app) as client:
            resp = await client.get("/api/oauth/github/start")
        assert resp.status_code == 200, resp.text
        q = parse_qs(urlparse(resp.json()["authorize_url"]).query)
        assert q["redirect_uri"] == ["https://symphony.example.com/api/oauth/github/callback"]
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_callback_redirect_uses_configured_public_origin(tmp_path: Path) -> None:
    respx.post("https://github.com/login/oauth/access_token").mock(
        return_value=httpx.Response(200, json={"access_token": "gho_secret_live"})
    )
    conn, db_path = await _open(tmp_path)
    try:
        cfg = _cfg().model_copy(
            update={"symphony_oauth_public_origin": "https://symphony.example.com"}
        )
        app = _app(conn, db_path, cfg=cfg)
        async with _client(app) as client:
            state = await _mint_state(client)
            resp = await client.get(f"/api/oauth/github/callback?code=the-code&state={state}")
        assert resp.status_code == 302
        assert resp.headers["location"] == "https://symphony.example.com/ui/config?connected=github"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_start_503_when_not_configured(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, cfg=Config(linear_api_key="k"))
        async with _client(app) as client:
            resp = await client.get("/api/oauth/github/start")
        assert resp.status_code == 503
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_start_is_auth_gated(tmp_path: Path) -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, auth=True)
        async with _client(app) as client:
            unauth = await client.get("/api/oauth/github/start")
            assert unauth.status_code == 401
            authed = await client.get(
                "/api/oauth/github/start", headers={"Authorization": f"Bearer {_token()}"}
            )
            assert authed.status_code == 200
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_callback_is_public_but_rejects_missing_state(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/oauth/github/callback?code=abc")
        # Ungated (not 401), but a callback with no state is rejected.
        assert resp.status_code == 400
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_callback_rejects_unknown_state(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/oauth/github/callback?code=abc&state=never-issued")
        assert resp.status_code == 400
    finally:
        await conn.close()


async def _mint_state(client: httpx.AsyncClient) -> str:
    resp = await client.get("/api/oauth/github/start")
    url = resp.json()["authorize_url"]
    return parse_qs(urlparse(url).query)["state"][0]


@pytest.mark.asyncio
@respx.mock
async def test_callback_happy_path_stores_encrypted_token(tmp_path: Path) -> None:
    respx.post("https://github.com/login/oauth/access_token").mock(
        return_value=httpx.Response(200, json={"access_token": "gho_secret_live"})
    )
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            state = await _mint_state(client)
            resp = await client.get(f"/api/oauth/github/callback?code=the-code&state={state}")
        # Redirects the browser back into the SPA.
        assert resp.status_code == 302
        assert "/ui/config" in resp.headers["location"]
        # Token never appears in the redirect response.
        assert "gho_secret_live" not in resp.text
        assert "gho_secret_live" not in str(resp.headers)
        # Stored, and encrypted at rest.
        status = await db.oauth_connections.get_status(conn, "github")
        assert status is not None and status.status == "connected"
        assert (
            await db.oauth_connections.get_credential(conn, "github", CredentialCipher(_KEY))
            == "gho_secret_live"
        )
        cur = await conn.execute(
            "SELECT credential FROM oauth_connections WHERE provider = 'github'"
        )
        raw = (await cur.fetchone())["credential"]
        assert b"gho_secret_live" not in raw
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_state_cannot_be_replayed(tmp_path: Path) -> None:
    respx.post("https://github.com/login/oauth/access_token").mock(
        return_value=httpx.Response(200, json={"access_token": "gho_live"})
    )
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            state = await _mint_state(client)
            first = await client.get(f"/api/oauth/github/callback?code=c1&state={state}")
            assert first.status_code == 302
            replay = await client.get(f"/api/oauth/github/callback?code=c2&state={state}")
        assert replay.status_code == 400
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_disconnect_clears_the_connection(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn, provider="github", credential="gho_x", cipher=CredentialCipher(_KEY)
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/github/disconnect")
        assert resp.status_code in (200, 204)
        assert await db.oauth_connections.get_status(conn, "github") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_test_reports_live(tmp_path: Path) -> None:
    respx.get("https://api.github.com/user").mock(
        return_value=httpx.Response(200, json={"login": "octocat"})
    )
    conn, db_path = await _open(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn, provider="github", credential="gho_live", cipher=CredentialCipher(_KEY)
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/github/test")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "live"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_test_marks_expired_when_undecryptable(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        # Stored with a key the running app no longer has (rotated/corrupt).
        await db.oauth_connections.set_connection(
            conn, provider="github", credential="gho_x", cipher=CredentialCipher("old-secret")
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/github/test")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "expired"
        status = await db.oauth_connections.get_status(conn, "github")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


async def _mint_linear_state(client: httpx.AsyncClient) -> str:
    resp = await client.get("/api/oauth/linear/start")
    url = resp.json()["authorize_url"]
    return parse_qs(urlparse(url).query)["state"][0]


@pytest.mark.asyncio
async def test_linear_start_returns_authorize_url(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/oauth/linear/start")
        assert resp.status_code == 200, resp.text
        url = resp.json()["authorize_url"]
        parsed = urlparse(url)
        assert parsed.netloc == "linear.app"
        q = parse_qs(parsed.query)
        assert q["client_id"] == ["lin-client-id"]
        assert q["scope"] == ["read,write"]
        assert q["code_challenge_method"] == ["S256"]
        assert q["redirect_uri"] == ["http://test/api/oauth/linear/callback"]
        assert q["state"]  # a state was minted
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_linear_start_503_when_not_configured(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        # GitHub configured, Linear not — the Linear card must still 503 legibly.
        cfg = Config(
            linear_api_key="k",
            github_oauth_client_id="gh",
            github_oauth_client_secret="gh",
        )
        app = _app(conn, db_path, cfg=cfg)
        async with _client(app) as client:
            resp = await client.get("/api/oauth/linear/start")
        assert resp.status_code == 503
        assert "LINEAR_OAUTH_CLIENT_ID" in resp.text
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_linear_callback_stores_encrypted_token(tmp_path: Path) -> None:
    respx.post("https://api.linear.app/oauth/token").mock(
        return_value=httpx.Response(200, json={"access_token": "lin_oauth_live"})
    )
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            state = await _mint_linear_state(client)
            resp = await client.get(f"/api/oauth/linear/callback?code=the-code&state={state}")
        assert resp.status_code == 302
        assert "/ui/config" in resp.headers["location"]
        assert "lin_oauth_live" not in resp.text
        assert "lin_oauth_live" not in str(resp.headers)
        status = await db.oauth_connections.get_status(conn, "linear")
        assert status is not None and status.status == "connected"
        assert (
            await db.oauth_connections.get_credential(conn, "linear", CredentialCipher(_KEY))
            == "lin_oauth_live"
        )
        cur = await conn.execute(
            "SELECT credential FROM oauth_connections WHERE provider = 'linear'"
        )
        raw = (await cur.fetchone())["credential"]
        assert b"lin_oauth_live" not in raw
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_linear_test_reports_live_via_viewer(tmp_path: Path) -> None:
    route = respx.post("https://api.linear.app/graphql").mock(
        return_value=httpx.Response(200, json={"data": {"viewer": {"id": "u1"}}})
    )
    conn, db_path = await _open(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn, provider="linear", credential="lin_live", cipher=CredentialCipher(_KEY)
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/linear/test")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "live"
        sent = route.calls.last.request
        assert b"viewer" in sent.content
        assert sent.headers["authorization"] == "Bearer lin_live"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_linear_test_reports_expired(tmp_path: Path) -> None:
    respx.post("https://api.linear.app/graphql").mock(return_value=httpx.Response(401))
    conn, db_path = await _open(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn, provider="linear", credential="lin_dead", cipher=CredentialCipher(_KEY)
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/linear/test")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "expired"
        status = await db.oauth_connections.get_status(conn, "linear")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_linear_test_reports_expired_on_200_with_graphql_errors(tmp_path: Path) -> None:
    respx.post("https://api.linear.app/graphql").mock(
        return_value=httpx.Response(
            200, json={"errors": [{"message": "Authentication required, not authenticated"}]}
        )
    )
    conn, db_path = await _open(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn, provider="linear", credential="lin_dead", cipher=CredentialCipher(_KEY)
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/linear/test")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "expired"
        status = await db.oauth_connections.get_status(conn, "linear")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_linear_test_reports_expired_on_200_with_null_data(tmp_path: Path) -> None:
    respx.post("https://api.linear.app/graphql").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": None,
                "errors": [{"message": "Authentication required, not authenticated"}],
            },
        )
    )
    conn, db_path = await _open(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn, provider="linear", credential="lin_dead", cipher=CredentialCipher(_KEY)
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/linear/test")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "expired"
        status = await db.oauth_connections.get_status(conn, "linear")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_linear_test_reports_expired_on_200_with_non_dict_body(tmp_path: Path) -> None:
    respx.post("https://api.linear.app/graphql").mock(return_value=httpx.Response(200, json=[]))
    conn, db_path = await _open(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn, provider="linear", credential="lin_dead", cipher=CredentialCipher(_KEY)
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/linear/test")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "expired"
        status = await db.oauth_connections.get_status(conn, "linear")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_test_reports_expired(tmp_path: Path) -> None:
    respx.get("https://api.github.com/user").mock(return_value=httpx.Response(401))
    conn, db_path = await _open(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn, provider="github", credential="gho_dead", cipher=CredentialCipher(_KEY)
        )
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/oauth/github/test")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "expired"
        # The stored row reflects the dead token so the card reads expired.
        status = await db.oauth_connections.get_status(conn, "github")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()
