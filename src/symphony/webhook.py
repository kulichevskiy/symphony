"""FastAPI receiver for Linear webhooks."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import aiosqlite
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from uvicorn import Config as UvicornConfig

from . import db

LINEAR_DELIVERY_HEADER = "linear-delivery"
LINEAR_SIGNATURE_HEADER = "linear-signature"
LOOPBACK_HOST = "127.0.0.1"


class WebhookHandler(Protocol):
    def handle_linear_webhook(self, payload: dict[str, Any]) -> Awaitable[Any]:
        """Handle a verified, fresh Linear webhook payload."""


@dataclass(frozen=True)
class WebhookSettings:
    secret: str
    dedupe_ttl_secs: int = 600
    timestamp_tolerance_secs: int = 60


Clock = Callable[[], datetime]


def verify_linear_signature(secret: str, body: bytes, signature: str | None) -> bool:
    if not secret or not signature:
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip().casefold())


def _webhook_timestamp_is_fresh(
    payload: dict[str, Any], *, now: datetime, tolerance_secs: int
) -> bool:
    raw_timestamp = payload.get("webhookTimestamp")
    if not isinstance(raw_timestamp, int | float):
        return False
    sent_at = datetime.fromtimestamp(raw_timestamp / 1000, UTC)
    return abs((now - sent_at).total_seconds()) <= tolerance_secs


def create_app(
    handler: WebhookHandler,
    conn: aiosqlite.Connection,
    settings: WebhookSettings,
    *,
    clock: Clock | None = None,
) -> FastAPI:
    app = FastAPI()
    now_fn = clock if clock is not None else lambda: datetime.now(UTC)

    @app.post("/linear/webhook")
    async def linear_webhook(request: Request) -> JSONResponse:
        body = await request.body()
        signature = request.headers.get(LINEAR_SIGNATURE_HEADER)
        if not verify_linear_signature(settings.secret, body, signature):
            raise HTTPException(status_code=401, detail="invalid signature")

        try:
            payload: dict[str, Any] = json.loads(body)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail="invalid json") from e

        now = now_fn()
        if not _webhook_timestamp_is_fresh(
            payload,
            now=now,
            tolerance_secs=settings.timestamp_tolerance_secs,
        ):
            raise HTTPException(status_code=401, detail="stale webhook timestamp")

        delivery_id = request.headers.get(LINEAR_DELIVERY_HEADER)
        if not delivery_id:
            raise HTTPException(status_code=400, detail="missing Linear-Delivery")

        claimed = await db.webhook_deliveries.claim(
            conn,
            delivery_id,
            received_at=now,
            ttl_secs=settings.dedupe_ttl_secs,
        )
        if not claimed:
            return JSONResponse({"status": "duplicate", "handled": False})

        try:
            result = await handler.handle_linear_webhook(payload)
        except Exception:
            await db.webhook_deliveries.forget(conn, delivery_id)
            raise

        return JSONResponse(
            {
                "status": "ok",
                "handled": bool(getattr(result, "handled", True)),
                "kind": str(getattr(result, "kind", payload.get("type", ""))),
            }
        )

    return app


def build_server_config(
    app: FastAPI,
    *,
    host: str = LOOPBACK_HOST,
    port: int = 8787,
) -> UvicornConfig:
    if host != LOOPBACK_HOST:
        raise ValueError("Linear webhook server must bind 127.0.0.1 only")
    return UvicornConfig(app=app, host=host, port=port, log_level="info")
