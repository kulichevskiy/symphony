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
            # Terminal: consume the session, then persist encrypted.
            registry.pop(body.login_session)
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
        live = not codex_credential_expired(credential)
        await db.oauth_connections.update_status(
            conn,
            provider=_PROVIDER,
            status="connected" if live else "expired",
            updated_at=_now_iso(clock),
            updated_by="oauth",
        )
        return {"status": "live" if live else "expired"}

    return router
