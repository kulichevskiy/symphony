"""FastAPI application factory for the daemon HTTP surface."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import aiosqlite
from fastapi import Depends, FastAPI
from fastapi.params import Depends as DependsParam
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response
from starlette.types import Scope
from uvicorn import Config as UvicornConfig

from .agent.codex_catalog import CodexCatalog, codex_catalog_client
from .auth import Auth0Settings, create_auth_config_router, create_auth_dependency
from .claude_login import ClaudeLoginProcess, PendingLoginRegistry, SubprocessClaudeLogin
from .codex_login import CodexLoginProcess, SubprocessCodexLogin, codex_expires_at
from .config import Config
from .credentials import CredentialWriteBack
from .crypto import (
    CredentialCipher,
    CredentialDecryptError,
    CredentialKeyMissingError,
    key_fingerprint,
    resolve_encryption_key,
)
from .db import oauth_connections
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
from .ui.codex_oauth import create_codex_oauth_router
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
    codex_login_factory: Callable[[], CodexLoginProcess] | None = None,
    codex_credentials_path: Path | None = None,
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

    def _create_ui_pools() -> tuple[
        ReadOnlyDbPool | None,
        WriteDbPool | None,
        WriteDbPool | None,
        WriteDbPool | None,
    ]:
        ui_pool = ReadOnlyDbPool(ui_db_path) if ui_enabled and ui_db_path is not None else None
        config_pool = (
            WriteDbPool(ui_db_path)
            if ui_enabled and ui_db_owns_topology and ui_db_path is not None
            else None
        )
        catalog_pool = (
            WriteDbPool(ui_db_path) if config_pool is not None and ui_db_path is not None else None
        )
        oauth_pool = WriteDbPool(ui_db_path) if ui_enabled and ui_db_path is not None else None
        return ui_pool, config_pool, catalog_pool, oauth_pool

    ui_pool, config_write_pool, catalog_write_pool, oauth_write_pool = _create_ui_pools()
    credential_cipher: CredentialCipher | None = None
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

    def _mount_oauth_routers(
        app: FastAPI,
        api_dependencies: list[DependsParam],
    ) -> CredentialCipher | None:
        if oauth_write_pool is None:
            return None
        resolved = ui_external_config() if callable(ui_external_config) else ui_external_config
        base_config = resolved if resolved is not None else Config()
        credential_cipher = oauth_cipher or CredentialCipher(
            resolve_encryption_key(
                base_config.symphony_encryption_key,
                oauth_write_pool.path.parent,
            )
        )
        app.include_router(
            create_claude_oauth_router(
                oauth_write_pool.connection,
                cipher=credential_cipher,
                registry=PendingLoginRegistry[ClaudeLoginProcess](),
                login_factory=(
                    claude_login_factory
                    or (lambda: SubprocessClaudeLogin(credentials_path=claude_credentials_path))
                ),
                clock=clock,
                credentials_path=claude_credentials_path,
            ),
            dependencies=api_dependencies,
        )
        app.include_router(
            create_codex_oauth_router(
                oauth_write_pool.connection,
                cipher=credential_cipher,
                registry=PendingLoginRegistry[CodexLoginProcess](),
                login_factory=(
                    codex_login_factory
                    or (lambda: SubprocessCodexLogin(credentials_path=codex_credentials_path))
                ),
                clock=clock,
                credentials_path=codex_credentials_path,
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
            cipher=credential_cipher,
            state_store=OAuthStateStore(),
            clock=clock,
            public_origin=base_config.symphony_oauth_public_origin or None,
        )
        app.include_router(oauth_gated, dependencies=api_dependencies)
        app.include_router(oauth_public)
        return credential_cipher

    def _mount_config_crud_router(
        app: FastAPI,
        api_dependencies: list[DependsParam],
        auth_dependency: Callable[..., Awaitable[dict[str, Any]]] | None,
        credential_cipher: CredentialCipher | None,
    ) -> None:
        if not (
            ui_db_owns_topology and config_write_pool is not None and catalog_write_pool is not None
        ):
            return

        def _scheduled_slots(key: tuple[str, str, str, str, str]) -> int:
            fn = getattr(handler, "scheduled_slot_count_for_binding_key", None)
            return int(fn(key)) if fn is not None else 0

        async def _codex_catalog() -> CodexCatalog:
            if credential_cipher is None:
                return await codex_catalog_client.get(credential=None, generation=None)
            catalog_conn = await catalog_write_pool.connection()
            try:
                snapshot = await oauth_connections.get_connection_snapshot(
                    catalog_conn,
                    "codex",
                    credential_cipher,
                )
            except (CredentialDecryptError, CredentialKeyMissingError):
                snapshot = None
            if snapshot is None or snapshot.status != "connected":
                return await codex_catalog_client.get(credential=None, generation=None)

            async def _write_back(credential: str) -> int | None:
                wrote = await CredentialWriteBack(catalog_conn, credential_cipher).write_back(
                    "codex",
                    credential,
                    expires_at=codex_expires_at(credential),
                    expected_prior=snapshot.credential,
                    expect_connected_generation=snapshot.generation,
                )
                if not wrote:
                    return None
                status = await oauth_connections.get_status(catalog_conn, "codex")
                return status.generation if status is not None else None

            return await codex_catalog_client.get(
                credential=snapshot.credential,
                generation=snapshot.generation,
                write_back=_write_back,
            )

        app.include_router(
            create_config_crud_router(
                config_write_pool.connection,
                config_provider=ui_external_config,
                write_lock=ui_config_write_lock,
                auth_dependency=auth_dependency,
                clock=clock,
                scheduled_slots=_scheduled_slots,
                repo_secret_view=cast(RepoSecretView | None, ui_repo_secret_view),
                codex_catalog_provider=_codex_catalog,
            ),
            dependencies=api_dependencies,
        )

    async def _close_ui_resources() -> None:
        if external_service is not None:
            external_service.cache.clear()
        for pool in (ui_pool, config_write_pool, catalog_write_pool, oauth_write_pool):
            if pool is not None:
                await pool.close()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await _close_ui_resources()

    app = FastAPI(
        lifespan=lifespan
        if ui_pool is not None
        or external_service is not None
        or config_write_pool is not None
        or catalog_write_pool is not None
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
            # The key *fingerprint* (non-reversible) is exposed so an operator
            # can tell which encryption key the instance runs (Config v2 2/9).
            _fp_config = (
                ui_external_config() if callable(ui_external_config) else ui_external_config
            )
            _fp_base = _fp_config if _fp_config is not None else Config()
            app.include_router(
                create_connections_router(
                    ui_pool,
                    key_fingerprint=key_fingerprint(
                        resolve_encryption_key(
                            _fp_base.symphony_encryption_key, ui_pool.path.parent
                        )
                    ),
                ),
                dependencies=api_dependencies,
            )

        credential_cipher = _mount_oauth_routers(app, api_dependencies)

        # Read-only view of the loaded config (redacted). Gated like the other
        # /api routers; included before create_api_router's catch-all.
        app.include_router(
            create_config_router(ui_external_config),
            dependencies=api_dependencies,
        )

        _mount_config_crud_router(app, api_dependencies, auth_dep, credential_cipher)

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
