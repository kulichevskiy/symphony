"""Provider-generic redirect-OAuth engine (OAuth in UI 2/7).

The reference authorization-code + PKCE flow, proven end-to-end with GitHub and
reused for Linear in 3/7. An operator's browser hits a gated `start` route that
mints a single-use `state` + PKCE verifier (kept here, server-side) and 302s to
the provider; the provider redirects back to an *ungated* `callback` (a browser
redirect carries no bearer), which validates the `state`, exchanges the `code`
for a user token, and hands it to the caller to encrypt + store.

Security rests entirely on the unguessable single-use `state` + PKCE: the store
below pops a `state` on first use, so an unknown or replayed one resolves to
nothing and the callback rejects it. Nothing provider-specific lives here —
`OAuthProvider` carries the endpoints/scopes so the same engine serves every
provider.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx

# States are short-lived: a consent round-trip is seconds, not minutes. A stale
# entry (operator abandoned the flow) is pruned on the next issue/consume.
_STATE_TTL_SECS = 600.0
# Bytes of entropy for `state`; token_urlsafe renders ~1.3 chars/byte.
_STATE_ENTROPY_BYTES = 32
# PKCE verifier length (RFC 7636 allows 43–128 chars).
_VERIFIER_ENTROPY_BYTES = 64


class OAuthError(Exception):
    """A token exchange failed (provider returned an error or no token). The
    callback surfaces this as a failed connect, never a raw traceback."""


@dataclass(frozen=True)
class OAuthProvider:
    """Everything the engine needs for one provider. `test_url` is the endpoint
    the Connections page's *Test* button pings with the stored token.

    `test_body` carries the request body for providers whose liveness probe is a
    POST (e.g. Linear's GraphQL `viewer` query) — `None` means the probe is a
    bare GET (GitHub's `/user`)."""

    provider: str
    authorize_url: str
    token_url: str
    test_url: str
    client_id: str
    client_secret: str
    scopes: tuple[str, ...]
    test_body: dict[str, Any] | None = None

    @property
    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret)


@dataclass(frozen=True)
class StateEntry:
    """Server-side state bound to one in-flight authorization. `redirect_uri` is
    stored so the token exchange sends byte-identical to what `authorize`
    carried (providers reject a mismatch)."""

    provider: str
    code_verifier: str
    redirect_uri: str
    created_at: float


class OAuthStateStore:
    """In-memory, single-use `state` store with a TTL. Single-process by design
    (Symphony is a single daemon); a `consume` pops the entry, so a replayed or
    unknown `state` returns `None` and the callback rejects it."""

    def __init__(
        self, *, ttl_secs: float = _STATE_TTL_SECS, now: Callable[[], float] = time.monotonic
    ) -> None:
        self._entries: dict[str, StateEntry] = {}
        self._ttl = ttl_secs
        self._now = now

    def issue(self, *, provider: str, code_verifier: str, redirect_uri: str) -> str:
        self._prune()
        state = secrets.token_urlsafe(_STATE_ENTROPY_BYTES)
        self._entries[state] = StateEntry(
            provider=provider,
            code_verifier=code_verifier,
            redirect_uri=redirect_uri,
            created_at=self._now(),
        )
        return state

    def consume(self, state: str) -> StateEntry | None:
        self._prune()
        entry = self._entries.pop(state, None)
        if entry is None:
            return None
        if self._now() - entry.created_at > self._ttl:
            return None
        return entry

    def _prune(self) -> None:
        cutoff = self._now() - self._ttl
        stale = [key for key, entry in self._entries.items() if entry.created_at < cutoff]
        for key in stale:
            del self._entries[key]


def generate_pkce() -> tuple[str, str]:
    """Return `(code_verifier, code_challenge)` — the challenge is the base64url
    (unpadded) SHA-256 of the verifier (PKCE S256)."""
    verifier = secrets.token_urlsafe(_VERIFIER_ENTROPY_BYTES)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


def build_authorize_url(
    provider: OAuthProvider, *, state: str, code_challenge: str, redirect_uri: str
) -> str:
    """The provider consent URL the browser is 302'd to — minimal scopes, PKCE
    challenge, and the single-use `state`."""
    params = {
        "client_id": provider.client_id,
        "redirect_uri": redirect_uri,
        "scope": " ".join(provider.scopes),
        "state": state,
        "response_type": "code",
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{provider.authorize_url}?{urlencode(params)}"


async def exchange_code(
    provider: OAuthProvider,
    *,
    code: str,
    code_verifier: str,
    redirect_uri: str,
    client: httpx.AsyncClient | None = None,
) -> str:
    """Exchange an authorization `code` for the user token, returning the access
    token string. Raises `OAuthError` if the provider returns an error payload
    or no token."""
    data = {
        "client_id": provider.client_id,
        "client_secret": provider.client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
        "grant_type": "authorization_code",
    }
    headers = {"Accept": "application/json"}
    if client is not None:
        resp = await client.post(provider.token_url, data=data, headers=headers)
    else:
        async with httpx.AsyncClient() as owned:
            resp = await owned.post(provider.token_url, data=data, headers=headers)
    try:
        resp.raise_for_status()
        payload = resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise OAuthError(f"token exchange with {provider.provider} failed") from exc
    if not isinstance(payload, dict) or payload.get("error"):
        raise OAuthError(f"token exchange with {provider.provider} was rejected")
    token = payload.get("access_token")
    if not isinstance(token, str) or not token:
        raise OAuthError(f"token exchange with {provider.provider} returned no access token")
    return token
