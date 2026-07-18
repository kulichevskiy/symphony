"""Codex device-auth login routes (OAuth in UI 6/7).

Codex has no browser-reachable redirect callback, so it can't ride the generic
redirect engine (`symphony.oauth`) GitHub/Linear use. And unlike Claude's
code-paste flow (5/7) there is no code to paste back — the `codex login
--device-auth` subprocess polls the provider itself and exits when the operator
completes it. So the daemon drives the CLI (`symphony.codex_login`) and reports
progress over a poll endpoint:

  * `GET  /api/oauth/codex/start`      — spawn the login, return the verification
                                         URL + user code + a login-session id;
  * `POST /api/oauth/codex/poll`       — report pending → connected/failed as the
                                         subprocess exits, storing the produced
                                         credentials *encrypted* on success;
  * `POST /api/oauth/codex/test`       — report live/expired off the stored token
                                         expiry (no network probe);
  * `POST /api/oauth/codex/disconnect` — clear the row + local `auth.json`.

All four are gated like the rest of `/api/*`; there is no ungated callback to
mount. The live subprocess handle is held across the repeated `poll` requests by
a `PendingLoginRegistry` (reused from 5/7).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import db
from ..claude_login import PendingLoginRegistry
from ..codex_login import (
    STATUS_FAILED,
    STATUS_SUCCESS,
    CodexLoginError,
    CodexLoginProcess,
    codex_credential_expired,
    codex_expires_at,
    default_codex_credentials_path,
)
from ..crypto import (
    CredentialCipher,
    CredentialDecryptError,
    CredentialKeyMissingError,
)

_log = logging.getLogger(__name__)

_PROVIDER = "codex"


class PollRequest(BaseModel):
    login_session: str


def _has_refresh_token(raw: str) -> bool:
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return False
    tokens = payload.get("tokens") if isinstance(payload, dict) else None
    if not isinstance(tokens, dict):
        return False
    refresh = tokens.get("refresh_token")
    return isinstance(refresh, str) and bool(refresh)


def _now_iso(clock: Callable[[], datetime] | None) -> str:
    now = clock() if clock is not None else datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


def create_codex_oauth_router(
    conn_provider: Callable[[], Awaitable[aiosqlite.Connection]],
    *,
    cipher: CredentialCipher,
    registry: PendingLoginRegistry[CodexLoginProcess],
    login_factory: Callable[[], CodexLoginProcess],
    clock: Callable[[], datetime] | None = None,
    credentials_path: Path | None = None,
) -> APIRouter:
    """Build the gated Codex login router. `login_factory` returns a fresh login
    subprocess per `start`; faked in tests."""
    router = APIRouter(prefix="/api/oauth/codex")
    creds_path = credentials_path or default_codex_credentials_path()
    # Serializes the terminal poll's persist against disconnect so a success
    # landing mid-disconnect can't re-create the row the operator just cleared.
    op_lock = asyncio.Lock()

    @router.get("/start")
    async def codex_start() -> dict[str, str]:
        if not cipher.available:
            raise HTTPException(
                status_code=503,
                detail="SYMPHONY_ENCRYPTION_KEY is not configured; cannot store credentials",
            )
        process = login_factory()
        try:
            device_auth = await process.start()
        except CodexLoginError as exc:
            _log.warning("codex login failed to start: %s", exc)
            await process.close()
            raise HTTPException(status_code=502, detail="could not start the Codex login") from exc
        login_session = registry.add(process)
        return {
            "verification_uri": device_auth.verification_uri,
            "user_code": device_auth.user_code,
            "login_session": login_session,
        }

    @router.post("/poll")
    async def codex_poll(body: PollRequest) -> dict[str, str | None]:
        process = registry.peek(body.login_session)
        if process is None:
            raise HTTPException(
                status_code=404, detail="unknown or expired login session; restart the login"
            )
        try:
            result = await process.poll()
        except CodexLoginError as exc:
            _log.warning("codex login poll failed: %s", exc)
            await registry.discard(body.login_session)
            raise HTTPException(status_code=502, detail="Codex login did not complete") from exc
        if result.status == STATUS_SUCCESS and result.credential is not None:
            # Terminal: consume the session, then persist encrypted — under the
            # op lock so a concurrent disconnect fully wins or fully loses.
            async with op_lock:
                if registry.pop(body.login_session) is None:
                    # A concurrent disconnect already tore this session down;
                    # don't resurrect the row it cleared.
                    return {"status": "not_connected", "expires_at": None}
                expires_at = codex_expires_at(result.credential)
                conn = await conn_provider()
                await db.oauth_connections.set_connection(
                    conn,
                    provider=_PROVIDER,
                    credential=result.credential,
                    cipher=cipher,
                    status="connected",
                    expires_at=expires_at,
                    updated_at=_now_iso(clock),
                    updated_by="oauth",
                )
            _log.info("oauth connection established for %s", _PROVIDER)
            return {"status": "connected", "expires_at": expires_at}
        if result.status == STATUS_FAILED:
            await registry.discard(body.login_session)
            return {"status": "failed", "expires_at": None}
        return {"status": "pending", "expires_at": None}

    @router.post("/disconnect")
    async def codex_disconnect() -> dict[str, str]:
        # Under the op lock, and discard the in-flight login first: a late poll
        # can neither slip its persist in before the delete nor pop a session
        # this already tore down (it returns not_connected instead).
        async with op_lock:
            await registry.discard_all()
            conn = await conn_provider()
            await db.oauth_connections.delete(conn, _PROVIDER)
        # The login subprocess already wrote its own credentials file; clear it
        # too so a disconnected card doesn't leave a still-usable local
        # credential a run could pick up outside the DB (mirrors Claude 5/7).
        # Best-effort — a bare DB disconnect still succeeds either way.
        try:
            creds_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            _log.warning("failed to remove local codex credentials file", exc_info=True)
        return {"status": "not_connected"}

    @router.post("/test")
    async def codex_test() -> dict[str, str]:
        conn = await conn_provider()
        try:
            credential = await db.oauth_connections.get_credential(conn, _PROVIDER, cipher)
        except (CredentialDecryptError, CredentialKeyMissingError):
            await db.oauth_connections.update_status(
                conn,
                provider=_PROVIDER,
                status="expired",
                updated_at=_now_iso(clock),
                updated_by="oauth",
            )
            return {"status": "expired"}
        if not credential:
            raise HTTPException(status_code=404, detail=f"{_PROVIDER} is not connected")
        # A run that hit a real 401 flipped the row to expired via auth-failure;
        # the offline Test (JWT/refresh inspection) must not overturn that — only
        # a reconnect clears it, or the expired dispatch gate is defeated.
        status = await db.oauth_connections.get_status(conn, _PROVIDER)
        if (
            status is not None
            and status.status == "expired"
            and status.updated_by == "auth-failure"
        ):
            return {"status": "expired"}
        # Otherwise: a lapsed access-token JWT with a stored refresh token is
        # still a working connection (the CLI refreshes in its per-run dir and
        # the write-back persists it). Only refreshless expiry is dead.
        live = not codex_credential_expired(credential) or _has_refresh_token(credential)
        await db.oauth_connections.update_status(
            conn,
            provider=_PROVIDER,
            status="connected" if live else "expired",
            updated_at=_now_iso(clock),
            updated_by="oauth",
        )
        return {"status": "live" if live else "expired"}

    return router
