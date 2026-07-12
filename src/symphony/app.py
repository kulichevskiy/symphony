"""FastAPI application factory for the daemon HTTP surface."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

import aiosqlite
from fastapi import Depends, FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response
from starlette.types import Scope
from uvicorn import Config as UvicornConfig

from .auth import Auth0Settings, create_auth_config_router, create_auth_dependency
from .config import Config
from .github.client import GitHub
from .github.webhook import (
    GitHubWebhookHandler,
    GitHubWebhookSettings,
    create_github_webhook_router,
)
from .linear.client import Linear
from .ui.api import CommandSink, PauseController, create_api_router
from .ui.config_view import create_config_router
from .ui.db import ReadOnlyDbPool
from .ui.external import ExternalSnapshotService, GitHubExternalClient
from .ui.issues import create_issue_detail_router
from .ui.live import create_live_stream_router
from .ui.status import CanonicalState
from .webhook import (
    LOOPBACK_HOST,
    Clock,
    WebhookHandler,
    WebhookSettings,
    create_linear_webhook_router,
)

_DEFAULT_UI_DIST = Path(__file__).resolve().parents[2] / "frontend" / "dist"


class SPAStaticFiles(StaticFiles):
    """StaticFiles variant that falls back to index.html for client routes.

    Vite content-hashes filenames under ``assets/``, so those are immutable and
    cached for a year; ``index.html`` is served ``no-cache`` so deploys are
    picked up immediately via ETag revalidation of just the one small document.
    """

    async def get_response(self, path: str, scope: Scope) -> Response:
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code != 404:
                raise
        return await super().get_response("index.html", scope)

    def file_response(
        self,
        full_path: os.PathLike[str] | str,
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        response = super().file_response(full_path, stat_result, scope, status_code)
        path = Path(full_path)
        if path.parent.name == "assets":
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        elif path.name == "index.html":
            response.headers["Cache-Control"] = "no-cache"
        return response


def create_app(
    handler: WebhookHandler,
    conn: aiosqlite.Connection,
    webhook_settings: WebhookSettings | None = None,
    github_webhook_settings: GitHubWebhookSettings | None = None,
    *,
    github_handler: GitHubWebhookHandler | None = None,
    ui_enabled: bool = True,
    ui_db_path: Path | None = None,
    ui_log_root: Path | None = None,
    ui_dist_dir: Path | None = None,
    ui_status_thresholds: Mapping[CanonicalState, timedelta] | None = None,
    ui_external_config: Config | None = None,
    ui_external_linear: Linear | None = None,
    ui_external_github: GitHubExternalClient | None = None,
    ui_external_service: ExternalSnapshotService | None = None,
    ui_pr_no_progress_threshold: timedelta | None = None,
    ui_command_sink: CommandSink | None = None,
    ui_pause_controller: PauseController | None = None,
    ui_webhook_public_url: str | None = None,
    auth0_settings: Auth0Settings | None = None,
    clock: Clock | None = None,
) -> FastAPI:
    # Publicly-exposed deployments (docker-compose.coolify.yml) set
    # SYMPHONY_REQUIRE_AUTH0=1: without Auth0 settings the /api/* routers mount
    # ungated, so a blank AUTH0_* .env would put mutating endpoints (e.g.
    # POST /api/issues/{id}/command) on the open internet. Fail closed at boot
    # instead of serving unauthenticated.
    if (
        ui_enabled
        and auth0_settings is None
        and os.environ.get("SYMPHONY_REQUIRE_AUTH0", "").strip() not in ("", "0", "false")
    ):
        raise RuntimeError(
            "SYMPHONY_REQUIRE_AUTH0 is set but AUTH0_DOMAIN/AUTH0_CLIENT_ID/"
            "AUTH0_ALLOWED_EMAILS are not configured — refusing to serve the "
            "UI/API unauthenticated on a public deployment. Set all three in "
            ".env, or unset SYMPHONY_REQUIRE_AUTH0 for a local-only stack."
        )
    ui_pool = ReadOnlyDbPool(ui_db_path) if ui_enabled and ui_db_path is not None else None
    external_service = ui_external_service
    if (
        external_service is None
        and ui_external_config is not None
        and ui_external_linear is not None
    ):
        external_service = ExternalSnapshotService(
            ui_external_config,
            ui_external_linear,
            ui_external_github or GitHub(),
            clock=clock,
        )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if external_service is not None:
                external_service.cache.clear()
            if ui_pool is not None:
                await ui_pool.close()

    app = FastAPI(
        lifespan=lifespan if ui_pool is not None or external_service is not None else None
    )
    if external_service is not None:
        app.state.external_snapshot_cache = external_service.cache

    if webhook_settings is not None:
        app.include_router(
            create_linear_webhook_router(
                handler,
                conn,
                webhook_settings,
                clock=clock,
            )
        )

    if github_webhook_settings is not None:
        resolved_github_handler = github_handler
        if resolved_github_handler is None:
            if not isinstance(handler, GitHubWebhookHandler):
                raise TypeError("github webhook settings require a GitHub webhook handler")
            resolved_github_handler = handler
        app.include_router(
            create_github_webhook_router(
                resolved_github_handler,
                conn,
                github_webhook_settings,
                clock=clock,
            )
        )

    if ui_enabled:
        # Unauthenticated: the SPA reads this at startup to know whether it
        # must run the Auth0 login flow before calling the gated routes below.
        app.include_router(create_auth_config_router(auth0_settings))

        # Single gate shared by the two /api/* routers. Webhook routes are
        # mounted above, outside this gate (they verify their own HMAC).
        api_dependencies = (
            [Depends(create_auth_dependency(auth0_settings))] if auth0_settings is not None else []
        )
        if ui_pool is not None:
            app.include_router(
                create_issue_detail_router(
                    ui_pool,
                    external_service=external_service,
                    clock=clock,
                    status_thresholds=ui_status_thresholds,
                    no_progress_threshold=ui_pr_no_progress_threshold,
                    log_root=ui_log_root,
                ),
                dependencies=api_dependencies,
            )
            if ui_log_root is not None:
                app.include_router(
                    create_live_stream_router(ui_pool, log_root=ui_log_root),
                    dependencies=api_dependencies,
                )

        # Read-only view of the loaded config (redacted). Gated like the other
        # /api routers; included before create_api_router's catch-all.
        app.include_router(
            create_config_router(ui_external_config),
            dependencies=api_dependencies,
        )

        ui_teams = (
            sorted({b.linear_team_key for b in ui_external_config.repos})
            if ui_external_config is not None
            else None
        )
        app.include_router(
            create_api_router(
                ui_pool,
                clock=clock,
                status_thresholds=ui_status_thresholds,
                no_progress_threshold=ui_pr_no_progress_threshold,
                command_sink=ui_command_sink,
                pause_controller=ui_pause_controller,
                teams=ui_teams,
                webhook_public_url=ui_webhook_public_url,
            ),
            dependencies=api_dependencies,
        )
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
