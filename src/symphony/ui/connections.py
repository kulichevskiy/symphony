"""Read-only Connections API (OAuth in UI 1/7).

Surfaces the four onboarding providers (GitHub, Linear, Claude, Codex) with a
`status` + `expires_at` each, built from the `oauth_connections` store. The
credential payload is never read here (it lives encrypted in a column this
router doesn't SELECT) and the response model is a fixed allowlist of
non-secret fields — mirroring the write-only/redaction contract of the
repo-secret store — so credential material can't leak. A provider with no row
reports `not_connected`.

Every actual authorization flow (Connect/Disconnect/Test) arrives in later
slices; this slice is read-only end-to-end.
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from .. import db
from ..db.oauth_connections import PROVIDERS, STATUS_NOT_CONNECTED
from .db import ReadOnlyDbPool

# Provider id → human label shown on the card.
_LABELS: dict[str, str] = {
    "github": "GitHub",
    "linear": "Linear",
    "claude": "Claude",
    "codex": "Codex",
}


class ConnectionView(BaseModel):
    """One provider card. No credential field — never serialized."""

    provider: str
    label: str
    status: str
    expires_at: str | None = None


class ConnectionsKeyView(BaseModel):
    """The effective encryption key's non-reversible fingerprint (Config v2
    2/9) — lets an operator verify which key an instance runs. Never the key."""

    fingerprint: str


def create_connections_router(pool: ReadOnlyDbPool, *, key_fingerprint: str = "") -> APIRouter:
    """Router exposing `GET /api/connections` — the four providers' statuses —
    and `GET /api/connections/key`, the encryption-key fingerprint."""
    router = APIRouter(prefix="/api")

    @router.get("/connections/key", response_model=ConnectionsKeyView)
    async def connections_key() -> ConnectionsKeyView:
        return ConnectionsKeyView(fingerprint=key_fingerprint)

    @router.get("/connections", response_model=list[ConnectionView])
    async def list_connections() -> list[ConnectionView]:
        conn = await pool.connection()
        by_provider = {row.provider: row for row in await db.oauth_connections.list_statuses(conn)}
        views: list[ConnectionView] = []
        for provider in PROVIDERS:
            row = by_provider.get(provider)
            views.append(
                ConnectionView(
                    provider=provider,
                    label=_LABELS[provider],
                    status=row.status if row is not None else STATUS_NOT_CONNECTED,
                    expires_at=row.expires_at if row is not None else None,
                )
            )
        return views

    return router
