from __future__ import annotations

import json
from typing import Any

import httpx
import jwt
import pytest
import respx
from cryptography.hazmat.primitives.asymmetric import rsa

from symphony import db
from symphony.app import create_app
from symphony.auth import Auth0Settings

from .test_webhook import NOW, SECRET, _body, _Handler, _headers, _payload

DOMAIN = "test-tenant.us.auth0.com"
CLIENT_ID = "spa-client-id"
ALLOWED_EMAIL = "alice@example.com"
ISSUER = f"https://{DOMAIN}/"
JWKS_URI = f"https://{DOMAIN}/.well-known/jwks.json"
KID = "test-key-1"

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _jwks() -> dict[str, Any]:
    pub_jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(_KEY.public_key()))
    pub_jwk.update({"kid": KID, "use": "sig", "alg": "RS256"})
    return {"keys": [pub_jwk]}


def _token(
    *,
    email: str = ALLOWED_EMAIL,
    email_verified: bool = True,
    aud: str = CLIENT_ID,
    iss: str = ISSUER,
    key: rsa.RSAPrivateKey = _KEY,
    kid: str = KID,
) -> str:
    payload = {
        "iss": iss,
        "aud": aud,
        "email": email,
        "email_verified": email_verified,
        "sub": "auth0|1",
        "exp": 9999999999,
    }
    return jwt.encode(payload, key, algorithm="RS256", headers={"kid": kid})


def _settings() -> Auth0Settings:
    return Auth0Settings.from_env(
        domain=DOMAIN,
        client_id=CLIENT_ID,
        allowed_emails=f" {ALLOWED_EMAIL} , Bob@Example.com ",
    )


def _app() -> Any:
    return create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=True,
        auth0_settings=_settings(),
    )


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
@respx.mock
async def test_api_without_token_is_401() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    async with _client(_app()) as client:
        resp = await client.get("/api/meta")
    assert resp.status_code == 401


@pytest.mark.asyncio
@respx.mock
async def test_api_valid_token_not_allowlisted_is_403() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    token = _token(email="mallory@example.com")
    async with _client(_app()) as client:
        resp = await client.get("/api/meta", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 403


@pytest.mark.asyncio
@respx.mock
async def test_api_valid_allowlisted_token_is_200() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    token = _token()
    async with _client(_app()) as client:
        resp = await client.get("/api/meta", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200


@pytest.mark.asyncio
@respx.mock
async def test_allowlist_is_case_insensitive() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    token = _token(email="bob@example.com")
    async with _client(_app()) as client:
        resp = await client.get("/api/meta", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200


@pytest.mark.asyncio
@respx.mock
async def test_unverified_email_is_403() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    token = _token(email_verified=False)
    async with _client(_app()) as client:
        resp = await client.get("/api/meta", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 403


@pytest.mark.asyncio
@respx.mock
async def test_jwks_refetched_on_unknown_kid() -> None:
    """Simulates Auth0 rotating signing keys: the first JWKS fetch doesn't
    have the key the token was signed with, so the verifier must refetch
    before rejecting it as unknown."""
    route = respx.get(JWKS_URI).mock(
        side_effect=[
            httpx.Response(200, json={"keys": []}),
            httpx.Response(200, json=_jwks()),
        ]
    )
    token = _token()
    async with _client(_app()) as client:
        resp = await client.get("/api/meta", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_wrong_audience_is_401() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    token = _token(aud="some-other-app")
    async with _client(_app()) as client:
        resp = await client.get("/api/meta", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


@pytest.mark.asyncio
@respx.mock
async def test_wrong_issuer_is_401() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    token = _token(iss="https://evil.example.com/")
    async with _client(_app()) as client:
        resp = await client.get("/api/meta", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


@pytest.mark.asyncio
@respx.mock
async def test_bad_signature_is_401() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    # Signed with a key whose public half is NOT in the served JWKS.
    token = _token(key=_OTHER_KEY)
    async with _client(_app()) as client:
        resp = await client.get("/api/meta", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


@pytest.mark.asyncio
@respx.mock
async def test_webhook_stays_public_when_auth_enabled(tmp_path: Any) -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    from symphony.webhook import WebhookSettings

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        app = create_app(
            _Handler(),
            conn,
            WebhookSettings(secret=SECRET),
            ui_enabled=True,
            auth0_settings=_settings(),
            clock=lambda: NOW,
        )
        body = _body(_payload())
        # No Auth0 bearer token — the webhook's own HMAC is the only gate.
        async with _client(app) as client:
            resp = await client.post("/linear/webhook", content=body, headers=_headers(body))
        assert resp.status_code == 200
    finally:
        await conn.close()


def test_secrets_read_auth0_from_env(monkeypatch: Any) -> None:
    from symphony.config import Secrets

    monkeypatch.setenv("AUTH0_DOMAIN", DOMAIN)
    monkeypatch.setenv("AUTH0_CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("AUTH0_ALLOWED_EMAILS", ALLOWED_EMAIL)
    secrets = Secrets()
    assert secrets.auth0_domain == DOMAIN
    assert secrets.auth0_client_id == CLIENT_ID
    assert secrets.auth0_allowed_emails == ALLOWED_EMAIL


def test_from_env_parses_and_normalizes_allowlist() -> None:
    settings = Auth0Settings.from_env(
        domain=DOMAIN,
        client_id=CLIENT_ID,
        allowed_emails="A@x.com, b@Y.com ,,",
    )
    assert settings.allowed_emails == frozenset({"a@x.com", "b@y.com"})
    assert settings.issuer == ISSUER
    assert settings.jwks_uri == JWKS_URI


@pytest.mark.asyncio
async def test_auth_config_endpoint_reports_disabled_when_unset() -> None:
    app = create_app(_Handler(), object(), ui_enabled=True, auth0_settings=None)  # type: ignore[arg-type]
    async with _client(app) as client:
        resp = await client.get("/api/auth-config")
    assert resp.status_code == 200
    assert resp.json() == {"enabled": False}


@pytest.mark.asyncio
async def test_auth_config_endpoint_is_public_and_reports_enabled() -> None:
    async with _client(_app()) as client:
        resp = await client.get("/api/auth-config")
    assert resp.status_code == 200
    assert resp.json() == {"enabled": True, "domain": DOMAIN, "client_id": CLIENT_ID}


def test_cli_auth0_settings_disabled_when_all_unset() -> None:
    from symphony.cli import _auth0_settings
    from symphony.config import Config

    assert _auth0_settings(Config()) is None


def test_cli_auth0_settings_enabled_when_all_set() -> None:
    from symphony.cli import _auth0_settings
    from symphony.config import Config

    cfg = Config(
        auth0_domain=DOMAIN,
        auth0_client_id=CLIENT_ID,
        auth0_allowed_emails=ALLOWED_EMAIL,
    )
    settings = _auth0_settings(cfg)
    assert settings is not None
    assert settings.domain == DOMAIN


def test_cli_auth0_settings_fails_closed_on_partial_config() -> None:
    import click

    from symphony.cli import _auth0_settings
    from symphony.config import Config

    cfg = Config(auth0_domain=DOMAIN, auth0_client_id=CLIENT_ID)
    with pytest.raises(click.ClickException):
        _auth0_settings(cfg)
