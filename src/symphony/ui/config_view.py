"""Read-only projection of the loaded daemon config for the web UI.

Surfaces bindings, resolved per-role agent/model/effort, and concurrency caps
as loaded at startup. Secrets (API keys, webhook secrets, Auth0/Telegram
credentials, resolved per-binding `env` values) are omitted: the projection is
built from an allowlist of non-sensitive fields, never a raw `model_dump` of
`Config`, so no secret can leak by accident.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..config import Config, RoleName

_ROLE_NAMES: tuple[RoleName, ...] = (
    "implement",
    "review_find",
    "review_verify",
    "fix",
    "accept",
)


class RoleView(BaseModel):
    """One resolved pipeline role, as the daemon would dispatch it."""

    agent: str
    model: str | None = None
    effort: str | None = None


class BindingView(BaseModel):
    """One tracker-project ↔ GitHub-repo binding, non-sensitive fields only."""

    provider: str
    project_key: str
    github_repo: str
    max_concurrent: int
    roles: dict[str, RoleView]


class ConfigView(BaseModel):
    """The effective loaded config, redacted for read-only display."""

    read_only: bool = True
    global_max_concurrent: int
    poll_interval_secs: int
    bindings: list[BindingView]


def build_config_view(config: Config) -> ConfigView:
    """Build the redacted view from the loaded `Config`."""
    bindings = [
        BindingView(
            provider=binding.provider,
            project_key=binding.project_key,
            github_repo=binding.github_repo,
            max_concurrent=binding.max_concurrent,
            roles={
                name: RoleView(**binding.resolved_role(name, config.roles).model_dump())
                for name in _ROLE_NAMES
            },
        )
        for binding in config.repos
        if binding.enabled
    ]
    return ConfigView(
        global_max_concurrent=config.global_max_concurrent,
        poll_interval_secs=config.poll_interval_secs,
        bindings=bindings,
    )


def create_config_router(config: Config | Callable[[], Config | None] | None) -> APIRouter:
    """Router exposing `GET /api/config` — the redacted loaded config.

    `config` may be a callable so a DB-owned topology's `GET /api/config`
    reflects the daemon's live, hot-reloaded bindings instead of a snapshot
    frozen at app-creation time (SYM-189) — the view is rebuilt on every
    request rather than once here.
    """
    router = APIRouter(prefix="/api")

    def _current() -> Config | None:
        return config() if callable(config) else config

    @router.get("/config", response_model=ConfigView)
    async def get_config() -> ConfigView:
        current = _current()
        if current is None:
            raise HTTPException(status_code=503, detail="config view is not available")
        return build_config_view(current)

    return router
