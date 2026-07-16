"""Provider-generic redirect-OAuth engine (OAuth in UI 2/7).

The single-use `state` store + PKCE are the only thing guarding the ungated
callback (a GitHub browser redirect carries no bearer), so these assert: a
minted state is consumable exactly once, an unknown/expired state resolves to
nothing, PKCE challenges are S256 of the verifier, the authorize URL carries
the minimal scopes, and the token exchange returns the access token (mocked).
"""

from __future__ import annotations

import base64
import hashlib
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx

from symphony.oauth import (
    OAuthError,
    OAuthProvider,
    OAuthStateStore,
    build_authorize_url,
    exchange_code,
    generate_pkce,
)

_GITHUB = OAuthProvider(
    provider="github",
    authorize_url="https://github.com/login/oauth/authorize",
    token_url="https://github.com/login/oauth/access_token",
    test_url="https://api.github.com/user",
    client_id="cid",
    client_secret="csecret",
    scopes=("repo", "workflow"),
)


def test_state_is_single_use() -> None:
    store = OAuthStateStore()
    state = store.issue(provider="github", code_verifier="v", redirect_uri="https://x/cb")
    entry = store.consume(state)
    assert entry is not None
    assert entry.provider == "github"
    assert entry.code_verifier == "v"
    assert entry.redirect_uri == "https://x/cb"
    # A replay finds nothing — the store popped it.
    assert store.consume(state) is None


def test_unknown_state_is_none() -> None:
    store = OAuthStateStore()
    assert store.consume("never-issued") is None


def test_expired_state_is_none() -> None:
    clock = [1000.0]
    store = OAuthStateStore(ttl_secs=60.0, now=lambda: clock[0])
    state = store.issue(provider="github", code_verifier="v", redirect_uri="cb")
    clock[0] += 61.0
    assert store.consume(state) is None


def test_issued_states_are_distinct_and_unguessable() -> None:
    store = OAuthStateStore()
    states = {
        store.issue(provider="github", code_verifier="v", redirect_uri="cb") for _ in range(50)
    }
    assert len(states) == 50
    assert all(len(s) >= 32 for s in states)


def test_pkce_challenge_is_s256_of_verifier() -> None:
    verifier, challenge = generate_pkce()
    expected = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    )
    assert challenge == expected


def test_authorize_url_carries_minimal_scopes_and_pkce() -> None:
    url = build_authorize_url(
        _GITHUB,
        state="st",
        code_challenge="ch",
        redirect_uri="https://app/api/oauth/github/callback",
    )
    parsed = urlparse(url)
    assert parsed.netloc == "github.com"
    q = parse_qs(parsed.query)
    assert q["client_id"] == ["cid"]
    assert q["state"] == ["st"]
    assert q["scope"] == ["repo workflow"]
    assert q["code_challenge"] == ["ch"]
    assert q["code_challenge_method"] == ["S256"]
    assert q["response_type"] == ["code"]
    assert q["redirect_uri"] == ["https://app/api/oauth/github/callback"]


@pytest.mark.asyncio
@respx.mock
async def test_exchange_code_returns_access_token() -> None:
    route = respx.post("https://github.com/login/oauth/access_token").mock(
        return_value=httpx.Response(200, json={"access_token": "gho_live", "token_type": "bearer"})
    )
    token = await exchange_code(
        _GITHUB, code="the-code", code_verifier="v", redirect_uri="https://app/cb"
    )
    assert token == "gho_live"
    sent = route.calls.last.request
    body = parse_qs(sent.content.decode())
    assert body["code"] == ["the-code"]
    assert body["code_verifier"] == ["v"]
    assert body["client_secret"] == ["csecret"]
    assert body["grant_type"] == ["authorization_code"]


@pytest.mark.asyncio
@respx.mock
async def test_exchange_code_raises_on_error_response() -> None:
    respx.post("https://github.com/login/oauth/access_token").mock(
        return_value=httpx.Response(200, json={"error": "bad_verification_code"})
    )
    with pytest.raises(OAuthError):
        await exchange_code(_GITHUB, code="x", code_verifier="v", redirect_uri="cb")
