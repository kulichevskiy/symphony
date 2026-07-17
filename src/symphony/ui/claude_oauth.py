"""Claude code-paste login routes (OAuth in UI 5/7).

Claude has no browser-reachable redirect callback, so it can't ride the generic
redirect engine (`symphony.oauth`) GitHub/Linear use. Instead the daemon drives
the `claude` CLI login as a subprocess (`symphony.claude_login`):

  * `GET  /api/oauth/claude/start`       — spawn the login, return its OAuth URL
                                           + a login-session id;
  * `POST /api/oauth/claude/submit-code` — feed the pasted code, store the
                                           produced credentials encrypted;
  * `POST /api/oauth/claude/test`        — report live/expired off the stored
                                           `expiresAt` (no network probe — the
                                           credential is the CLI's own, not a
                                           bearer we can ping);
  * `POST /api/oauth/claude/disconnect`  — clear the row.

All four are gated like the rest of `/api/*`; there is no ungated callback to
mount (the whole point of the code-paste flow). The live subprocess handle is
held between `start` and `submit-code` by a `PendingLoginRegistry`.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import aiosqlite
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import db
from ..claude_login import (
    ClaudeLoginError,
    ClaudeLoginProcess,
    PendingLoginRegistry,
    claude_credential_expired,
    claude_expires_at,
)
from ..crypto import (
    CredentialCipher,
    CredentialDecryptError,
    CredentialKeyMissingError,
)

_log = logging.getLogger(__name__)

_PROVIDER = "claude"


class SubmitCodeRequest(BaseModel):
    login_session: str
    code: str


def _now_iso(clock: Callable[[], datetime] | None) -> str:
    now = clock() if clock is not None else datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


def create_claude_oauth_router(
    conn_provider: Callable[[], Awaitable[aiosqlite.Connection]],
    *,
    cipher: CredentialCipher,
    registry: PendingLoginRegistry,
    login_factory: Callable[[], ClaudeLoginProcess],
    clock: Callable[[], datetime] | None = None,
) -> APIRouter:
    """Build the gated Claude login router. `login_factory` returns a fresh
    login subprocess per `start`; faked in tests."""
    router = APIRouter(prefix="/api/oauth/claude")

    @router.get("/start")
    async def claude_start() -> dict[str, str]:
        if not cipher.available:
            raise HTTPException(
                status_code=503,
                detail="SYMPHONY_ENCRYPTION_KEY is not configured; cannot store credentials",
            )
        process = login_factory()
        try:
            authorize_url = await process.start()
        except ClaudeLoginError as exc:
            _log.warning("claude login failed to start: %s", exc)
            await process.close()
            raise HTTPException(status_code=502, detail="could not start the Claude login") from exc
        login_session = registry.add(process)
        return {"authorize_url": authorize_url, "login_session": login_session}

    @router.post("/submit-code")
    async def claude_submit_code(body: SubmitCodeRequest) -> dict[str, str | None]:
        process = registry.pop(body.login_session)
        if process is None:
            raise HTTPException(
                status_code=404, detail="unknown or expired login session; restart the login"
            )
        try:
            credential = await process.submit_code(body.code)
        except ClaudeLoginError as exc:
            _log.warning("claude login failed on submitted code: %s", exc)
            await process.close()
            raise HTTPException(status_code=502, detail="Claude login did not complete") from exc
        expires_at = claude_expires_at(credential)
        conn = await conn_provider()
        await db.oauth_connections.set_connection(
            conn,
            provider=_PROVIDER,
            credential=credential,
            cipher=cipher,
            status="connected",
            expires_at=expires_at,
            updated_at=_now_iso(clock),
            updated_by="oauth",
        )
        _log.info("oauth connection established for %s", _PROVIDER)
        return {"status": "connected", "expires_at": expires_at}

    @router.post("/disconnect")
    async def claude_disconnect() -> dict[str, str]:
        conn = await conn_provider()
        await db.oauth_connections.delete(conn, _PROVIDER)
        return {"status": "not_connected"}

    @router.post("/test")
    async def claude_test() -> dict[str, str]:
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
        live = not claude_credential_expired(credential)
        await db.oauth_connections.update_status(
            conn,
            provider=_PROVIDER,
            status="connected" if live else "expired",
            updated_at=_now_iso(clock),
            updated_by="oauth",
        )
        return {"status": "live" if live else "expired"}

    return router
