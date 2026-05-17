"""FastAPI application factory for the daemon HTTP surface."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response
from starlette.types import Scope
from uvicorn import Config as UvicornConfig

from .ui.api import create_api_router
from .ui.db import ReadOnlyDbPool
from .ui.issues import create_issue_detail_router
from .webhook import (
    LOOPBACK_HOST,
    Clock,
    WebhookHandler,
    WebhookSettings,
    create_linear_webhook_router,
)

_DEFAULT_UI_DIST = Path(__file__).resolve().parents[2] / "frontend" / "dist"


class SPAStaticFiles(StaticFiles):
    """StaticFiles variant that falls back to index.html for client routes."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code != 404:
                raise
        return await super().get_response("index.html", scope)


def create_app(
    handler: WebhookHandler,
    conn: aiosqlite.Connection,
    webhook_settings: WebhookSettings | None = None,
    *,
    ui_enabled: bool = True,
    ui_db_path: Path | None = None,
    ui_dist_dir: Path | None = None,
    clock: Clock | None = None,
) -> FastAPI:
    ui_pool = (
        ReadOnlyDbPool(ui_db_path)
        if ui_enabled and ui_db_path is not None
        else None
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if ui_pool is not None:
                await ui_pool.close()

    app = FastAPI(lifespan=lifespan if ui_pool is not None else None)

    if webhook_settings is not None:
        app.include_router(
            create_linear_webhook_router(
                handler,
                conn,
                webhook_settings,
                clock=clock,
            )
        )

    if ui_enabled:
        if ui_pool is not None:
            app.include_router(create_issue_detail_router(ui_pool))

        app.include_router(create_api_router(ui_pool))
        dist_dir = ui_dist_dir or _DEFAULT_UI_DIST
        if dist_dir.exists():
            app.mount(
                "/ui",
                SPAStaticFiles(directory=dist_dir, html=True),
                name="ui",
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
