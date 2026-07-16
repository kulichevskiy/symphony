"""Connections OAuth routes (OAuth in UI 2/7).

Wires the provider-generic redirect engine (`symphony.oauth`) onto two routers:

  * a *gated* router (`start` / `disconnect` / `test`) mounted behind the same
    Auth0 dependency as the rest of `/api/*` — these are operator actions from
    the SPA, which carries a bearer;
  * a *public* router (`callback`) mounted outside the gate — the GitHub
    redirect back to us is a plain browser navigation with no bearer, so the
    unguessable single-use `state` + PKCE (see `symphony.oauth`) are the only
    thing guarding it.

`start` mints the `state`/PKCE and returns the provider consent URL for the SPA
to navigate to (a cross-origin 302 can't be read back out of a `fetch`, so the
URL is handed over as JSON rather than as a redirect the browser follows
blindly). `callback` validates the `state`, exchanges the `code`, encrypts +
stores the token via the shared `oauth_connections` DAO, then 302s back into the
SPA which re-reads connection status.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Request
from starlette.responses import RedirectResponse

from .. import db
from ..crypto import CredentialCipher, CredentialDecryptError, CredentialKeyMissingError
from ..oauth import (
    OAuthError,
    OAuthProvider,
    OAuthStateStore,
    build_authorize_url,
    exchange_code,
    generate_pkce,
)

_log = logging.getLogger(__name__)

# Where the callback lands the browser back in the SPA (basename `/ui`), with a
# marker the page reads to refetch connection status / show the outcome.
_SPA_CONNECTIONS_PATH = "/ui/config"

# GitHub OAuth app endpoints + minimal scopes: `repo` for the bound repos and
# `workflow` so Symphony can push branches/PRs that touch workflow files.
_GITHUB_AUTHORIZE_URL = "https://github.com/login/oauth/authorize"
_GITHUB_TOKEN_URL = "https://github.com/login/oauth/access_token"
_GITHUB_TEST_URL = "https://api.github.com/user"
_GITHUB_SCOPES = ("repo", "workflow")


def github_provider(client_id: str, client_secret: str) -> OAuthProvider:
    """The GitHub `OAuthProvider` (registered even when unconfigured — `start`
    reports a clear 503 so the card renders and Connect fails legibly)."""
    return OAuthProvider(
        provider="github",
        authorize_url=_GITHUB_AUTHORIZE_URL,
        token_url=_GITHUB_TOKEN_URL,
        test_url=_GITHUB_TEST_URL,
        client_id=client_id,
        client_secret=client_secret,
        scopes=_GITHUB_SCOPES,
    )


def _now_iso(clock: Callable[[], datetime] | None) -> str:
    now = clock() if clock is not None else datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


def _spa_redirect(request: Request, params: str) -> RedirectResponse:
    """302 back into the SPA on the same origin as this request."""
    base = str(request.base_url).rstrip("/")
    return RedirectResponse(f"{base}{_SPA_CONNECTIONS_PATH}?{params}", status_code=302)


def create_oauth_routers(
    conn_provider: Callable[[], Awaitable[aiosqlite.Connection]],
    *,
    providers: dict[str, OAuthProvider],
    cipher: CredentialCipher,
    state_store: OAuthStateStore,
    clock: Callable[[], datetime] | None = None,
) -> tuple[APIRouter, APIRouter]:
    """Build `(gated_router, public_router)`. The caller mounts the first behind
    the Auth0 dependency and the second outside it."""
    gated = APIRouter(prefix="/api/oauth")
    public = APIRouter(prefix="/api/oauth")

    def _provider(name: str) -> OAuthProvider:
        provider = providers.get(name)
        if provider is None:
            raise HTTPException(status_code=404, detail=f"unknown OAuth provider {name!r}")
        return provider

    def _callback_uri(request: Request, name: str) -> str:
        return str(request.url_for("oauth_callback", provider=name))

    @gated.get("/{provider}/start")
    async def oauth_start(provider: str, request: Request) -> dict[str, str]:
        cfg = _provider(provider)
        if not cfg.configured:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"{provider} OAuth is not configured; set "
                    "GITHUB_OAUTH_CLIENT_ID/GITHUB_OAUTH_CLIENT_SECRET in the deployment env"
                ),
            )
        if not cipher.available:
            raise HTTPException(
                status_code=503,
                detail="SYMPHONY_ENCRYPTION_KEY is not configured; cannot store credentials",
            )
        verifier, challenge = generate_pkce()
        redirect_uri = _callback_uri(request, provider)
        state = state_store.issue(
            provider=provider, code_verifier=verifier, redirect_uri=redirect_uri
        )
        return {
            "authorize_url": build_authorize_url(
                cfg, state=state, code_challenge=challenge, redirect_uri=redirect_uri
            )
        }

    @public.get("/{provider}/callback", name="oauth_callback")
    async def oauth_callback(
        provider: str,
        request: Request,
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
    ) -> RedirectResponse:
        cfg = _provider(provider)
        if error:
            return _spa_redirect(request, f"error={provider}")
        # Reject a missing/unknown/replayed state before touching the provider:
        # the single-use store popped a valid one on `start`, so a replay or a
        # forged callback finds nothing.
        if not state or not code:
            raise HTTPException(status_code=400, detail="missing code or state")
        entry = state_store.consume(state)
        if entry is None or entry.provider != provider:
            raise HTTPException(status_code=400, detail="unknown or expired state")
        try:
            token = await exchange_code(
                cfg,
                code=code,
                code_verifier=entry.code_verifier,
                redirect_uri=entry.redirect_uri,
            )
        except OAuthError:
            _log.warning("oauth callback: %s token exchange failed", provider)
            return _spa_redirect(request, f"error={provider}")
        conn = await conn_provider()
        await db.oauth_connections.set_connection(
            conn,
            provider=provider,
            credential=token,
            cipher=cipher,
            status="connected",
            updated_at=_now_iso(clock),
            updated_by="oauth",
        )
        _log.info("oauth connection established for %s", provider)
        return _spa_redirect(request, f"connected={provider}")

    @gated.post("/{provider}/disconnect")
    async def oauth_disconnect(provider: str) -> dict[str, str]:
        _provider(provider)
        conn = await conn_provider()
        await db.oauth_connections.delete(conn, provider)
        return {"status": "not_connected"}

    @gated.post("/{provider}/test")
    async def oauth_test(provider: str) -> dict[str, str]:
        cfg = _provider(provider)
        conn = await conn_provider()
        try:
            token = await db.oauth_connections.get_credential(conn, provider, cipher)
        except (CredentialDecryptError, CredentialKeyMissingError):
            token = None
        if not token:
            raise HTTPException(status_code=404, detail=f"{provider} is not connected")
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                cfg.test_url, headers={"Authorization": f"Bearer {token}"}
            )
        live = resp.status_code == 200
        await db.oauth_connections.update_status(
            conn,
            provider=provider,
            status="connected" if live else "expired",
            updated_at=_now_iso(clock),
            updated_by="oauth",
        )
        return {"status": "live" if live else "expired"}

    return gated, public
