"""FastAPI application factory for the daemon HTTP surface."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import cast

import aiosqlite
from fastapi import Depends, FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response
from starlette.types import Scope
from uvicorn import Config as UvicornConfig

from .auth import Auth0Settings, create_auth_config_router, create_auth_dependency
from .claude_login import ClaudeLoginProcess, PendingLoginRegistry, SubprocessClaudeLogin
from .config import Config
from .crypto import CredentialCipher
from .db.config_repo_secrets import RepoSecretView
from .github.client import GitHub
from .github.webhook import (
    GitHubWebhookHandler,
    GitHubWebhookSettings,
    create_github_webhook_router,
)
from .linear.client import Linear
from .oauth import OAuthStateStore
from .ui.api import CommandSink, PauseController, create_api_router
from .ui.claude_oauth import create_claude_oauth_router
from .ui.config_crud import create_config_crud_router
from .ui.config_view import create_config_router
from .ui.connections import create_connections_router
from .ui.db import ReadOnlyDbPool, WriteDbPool
from .ui.external import ExternalSnapshotService, GitHubExternalClient
from .ui.issues import create_issue_detail_router
from .ui.live import create_live_stream_router
from .ui.oauth import create_oauth_routers, github_provider, linear_provider
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
    github_webhook_settings: (
        GitHubWebhookSettings | Callable[[], GitHubWebhookSettings | None] | None
    ) = None,
    *,
    github_handler: GitHubWebhookHandler | None = None,
    ui_enabled: bool = True,
    ui_db_path: Path | None = None,
    ui_log_root: Path | None = None,
    ui_dist_dir: Path | None = None,
    ui_status_thresholds: Mapping[CanonicalState, timedelta] | None = None,
    ui_external_config: Config | Callable[[], Config | None] | None = None,
    ui_external_linear: Linear | Callable[[], Linear | None] | None = None,
    ui_external_github: GitHubExternalClient | None = None,
    ui_external_service: ExternalSnapshotService | None = None,
    ui_pr_no_progress_threshold: timedelta | None = None,
    ui_command_sink: CommandSink | None = None,
    ui_pause_controller: PauseController | None = None,
    ui_config_write_lock: object | None = None,
    ui_repo_secret_view: object | None = None,
    ui_db_owns_topology: bool = True,
    ui_webhook_public_url: str | None = None,
    auth0_settings: Auth0Settings | None = None,
    oauth_cipher: CredentialCipher | None = None,
    claude_login_factory: Callable[[], ClaudeLoginProcess] | None = None,
    claude_credentials_path: Path | None = None,
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
    # A dedicated write connection for the config-CRUD router — never the
    # `conn` shared with the orchestrator (SYM-190; see `WriteDbPool`).
    config_write_pool = (
        WriteDbPool(ui_db_path)
        if ui_enabled and ui_db_owns_topology and ui_db_path is not None
        else None
    )
    # Dedicated write connection for the OAuth callback/disconnect/test routes
    # (the read-only `ui_pool` can't store a freshly-minted token). Independent
    # of `ui_db_owns_topology` — connecting a provider doesn't touch bindings.
    oauth_write_pool = WriteDbPool(ui_db_path) if ui_enabled and ui_db_path is not None else None
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
            if config_write_pool is not None:
                await config_write_pool.close()
            if oauth_write_pool is not None:
                await oauth_write_pool.close()

    app = FastAPI(
        lifespan=lifespan
        if ui_pool is not None
        or external_service is not None
        or config_write_pool is not None
        or oauth_write_pool is not None
        else None
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

        # Single gate shared by the /api/* routers. Webhook routes are mounted
        # above, outside this gate (they verify their own HMAC). Built once so
        # the config-CRUD router can reuse the same dependency instance to read
        # the caller's email for `updated_by` — FastAPI dedupes by identity, so
        # verification still runs only once per request.
        auth_dep = create_auth_dependency(auth0_settings) if auth0_settings is not None else None
        api_dependencies = [Depends(auth_dep)] if auth_dep is not None else []
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
            # Read-only Connections page: per-provider status from the encrypted
            # `oauth_connections` store (credential material never served).
            app.include_router(
                create_connections_router(ui_pool),
                dependencies=api_dependencies,
            )

        # Redirect-OAuth for GitHub (2/7) and Linear (3/7) on one engine.
        # `start`/`disconnect`/`test` are gated like the rest of `/api/*`;
        # `callback` is mounted OUTSIDE the gate — a browser redirect from the
        # provider carries no bearer, so security rests on the single-use
        # `state` + PKCE the engine enforces.
        if oauth_write_pool is not None:
            resolved = ui_external_config() if callable(ui_external_config) else ui_external_config
            base_config = resolved if resolved is not None else Config()
            # Not `CredentialCipher.from_env()`: Coolify mounts `.env` as a file
            # and deliberately never injects it into os.environ (see
            # docker-compose.coolify.yml), so reading process env directly would
            # never see the key there. `base_config` is sourced from `Secrets`,
            # which reads `.env` the same way `github_oauth_client_id` above does.
            cipher = (
                oauth_cipher
                if oauth_cipher is not None
                else CredentialCipher(base_config.symphony_encryption_key)
            )
            # Claude code-paste login (5/7): no browser-reachable callback, so
            # it's a separate router mounted BEFORE the generic engine below —
            # `/api/oauth/claude/*` must win over `/api/oauth/{provider}/*`
            # (which doesn't know `claude` and would 404). Gated like the rest.
            app.include_router(
                create_claude_oauth_router(
                    oauth_write_pool.connection,
                    cipher=cipher,
                    registry=PendingLoginRegistry(),
                    login_factory=claude_login_factory or (lambda: SubprocessClaudeLogin()),
                    clock=clock,
                    credentials_path=claude_credentials_path,
                ),
                dependencies=api_dependencies,
            )
            oauth_gated, oauth_public = create_oauth_routers(
                oauth_write_pool.connection,
                providers={
                    "github": github_provider(
                        base_config.github_oauth_client_id,
                        base_config.github_oauth_client_secret,
                    ),
                    "linear": linear_provider(
                        base_config.linear_oauth_client_id,
                        base_config.linear_oauth_client_secret,
                    ),
                },
                cipher=cipher,
                state_store=OAuthStateStore(),
                clock=clock,
                public_origin=base_config.symphony_oauth_public_origin or None,
            )
            app.include_router(oauth_gated, dependencies=api_dependencies)
            app.include_router(oauth_public)

        # Read-only view of the loaded config (redacted). Gated like the other
        # /api routers; included before create_api_router's catch-all.
        app.include_router(
            create_config_router(ui_external_config),
            dependencies=api_dependencies,
        )

        # Binding CRUD (create/edit/delete + options) — writes go through
        # `config_write_pool`'s own connection (never the daemon's shared
        # `conn`; see `WriteDbPool`), serialized against the daemon's
        # tick-boundary binding reload by the config write lock (SYM-189) so
        # a write's multi-row transaction never interleaves with a reload.
        # Gated on `ui_db_owns_topology` (default True so callers that don't
        # pass it — tests, any future non-daemon entrypoint — keep today's
        # behavior): a legacy YAML topology not yet imported keeps the daemon
        # reading `repos:` from YAML (`reload_bindings_from_db=False`), so
        # writes here would round-trip through the API looking successful
        # while the daemon silently never applies them. `cli._run` is the only
        # caller that computes and passes the real value. Also requires
        # `ui_db_path` (always passed alongside it by `cli._run`) to open the
        # dedicated connection; without one the router simply doesn't mount.
        if ui_db_owns_topology and config_write_pool is not None:

            def _scheduled_slots(key: tuple[str, str, str, str, str]) -> int:
                # The daemon reserves in-memory dispatch/fix-run slots before a
                # run row exists; the drain guard must see them (SYM-193). The
                # orchestrator is `handler` here; a non-orchestrator handler
                # (some tests) simply reports zero reservations.
                fn = getattr(handler, "scheduled_slot_count_for_binding_key", None)
                return int(fn(key)) if fn is not None else 0

            app.include_router(
                create_config_crud_router(
                    config_write_pool.connection,
                    config_provider=ui_external_config,
                    write_lock=ui_config_write_lock,
                    auth_dependency=auth_dep,
                    clock=clock,
                    scheduled_slots=_scheduled_slots,
                    repo_secret_view=cast(RepoSecretView | None, ui_repo_secret_view),
                ),
                dependencies=api_dependencies,
            )

        def _ui_teams() -> list[str] | None:
            current = ui_external_config() if callable(ui_external_config) else ui_external_config
            if current is None:
                return None
            return sorted({b.linear_team_key for b in current.repos})

        app.include_router(
            create_api_router(
                ui_pool,
                clock=clock,
                status_thresholds=ui_status_thresholds,
                no_progress_threshold=ui_pr_no_progress_threshold,
                command_sink=ui_command_sink,
                pause_controller=ui_pause_controller,
                teams=_ui_teams,
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
