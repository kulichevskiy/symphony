"""Configuration loader.

Two layers:
  1. Secrets via env vars (LINEAR_API_KEY, etc.) — `pydantic-settings`.
  2. Topology via YAML file (repo bindings, caps, paths) — `yaml.safe_load`
     into the same model.

Splitting these keeps secrets out of the repo without forcing every config
knob into the env. The YAML is what an operator edits day-to-day; env vars
are what `systemd EnvironmentFile=` (or the managed-platform equivalent)
provides at boot.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .agent.codex_models import DEFAULT_CODEX_MODEL, SUPPORTED_CODEX_MODELS
from .github.client import MergeStrategy


def _expand(path: str | Path) -> Path:
    """Expand ~ and env vars; return absolute Path."""
    import os

    return Path(os.path.expandvars(os.path.expanduser(str(path)))).resolve()


class LinearStates(BaseModel):
    """Workflow state names per role.

    Names must match the Linear team's workflow exactly. Symphony does not
    create or rename states. `ready` has no default — every binding must
    declare which state the orchestrator picks issues up from, since teams
    rename or replace it (Backlog, Todo, Up Next, …).
    """

    ready: str = Field(min_length=1)
    in_progress: str = "In Progress"
    needs_approval: str = "Needs Approval"
    blocked: str = "Blocked"
    done: str = "Done"


class RepoBinding(BaseModel):
    """One (Linear team, GitHub repo) pairing.

    Symphony scans `linear_team_key`'s issues for ones in the configured
    "ready" state with `issue_label` (if set), and dispatches them to
    `github_repo`. Per-repo `max_concurrent` is enforced in addition to the
    global cap so a single noisy team can't starve others.
    """

    linear_team_key: str
    github_repo: str
    agent: Literal["claude", "codex"] = "claude"
    codex_model: str = DEFAULT_CODEX_MODEL
    issue_label: str | None = None
    branch_prefix: str = "symphony"
    base_branch: str | None = None
    merge_strategy: MergeStrategy = "squash"
    max_concurrent: int = 2
    runner: Literal["local", "e2b", "daytona"] = "local"
    # Per-binding cost knobs. `None` falls back to the global default; an
    # explicit `0` disables the cap (useful when one team is exempt).
    cost_cap_usd: float | None = None
    cost_warning_pct: int | None = None
    activity_comments_enabled: bool | None = None
    activity_comment_interval_secs: int | None = Field(default=None, ge=1)
    activity_comment_min_interval_secs: int | None = Field(default=None, ge=1)
    activity_comment_event_threshold: int | None = Field(default=None, ge=1)
    activity_comment_long_running_secs: int | None = Field(default=None, ge=1)
    activity_comment_long_running_repeat_secs: int | None = Field(default=None, ge=1)
    activity_comment_include_failed_output_lines: int | None = Field(default=None, ge=0)
    linear_states: LinearStates

    @field_validator("codex_model")
    @classmethod
    def _known_codex_model(cls, value: str) -> str:
        if value not in SUPPORTED_CODEX_MODELS:
            supported = ", ".join(sorted(SUPPORTED_CODEX_MODELS))
            raise ValueError(f"unknown Codex model {value!r}; supported: {supported}")
        return value


class Secrets(BaseSettings):
    """Secrets sourced from the environment.

    `gh` auth is *not* here — it lives in `~/.config/gh/hosts.yml` on the
    host (a fine-grained PAT pinned to the relevant repos, per docs §10.3).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    linear_api_key: str = Field(default="", validation_alias="LINEAR_API_KEY")
    linear_webhook_secret: str = Field(
        default="", validation_alias="LINEAR_WEBHOOK_SECRET"
    )


class Config(BaseModel):
    """Top-level config. Loaded from YAML, secrets layered on at the end."""

    poll_interval_secs: int = 60
    global_max_concurrent: int = 4
    workspace_root: Path = Path("~/symphony/workspaces")
    log_root: Path = Path("~/symphony/logs")
    db_path: Path = Path("~/symphony/state.sqlite")
    webhook_host: Literal["127.0.0.1"] = "127.0.0.1"
    webhook_port: int = Field(default=8787, ge=1, le=65535)
    webhook_dedupe_ttl_secs: int = Field(default=600, ge=1)
    webhook_timestamp_tolerance_secs: int = Field(default=60, ge=1)

    repos: list[RepoBinding] = Field(default_factory=list)

    review_iteration_cap: int = 12
    cost_cap_per_issue_usd: float = 15.0
    cost_warning_pct: int = 75
    stall_timeout_secs: int = 300
    activity_comments_enabled: bool = True
    activity_comment_interval_secs: int = Field(default=300, ge=1)
    activity_comment_min_interval_secs: int = Field(default=120, ge=1)
    activity_comment_event_threshold: int = Field(default=20, ge=1)
    activity_comment_long_running_secs: int = Field(default=300, ge=1)
    activity_comment_long_running_repeat_secs: int = Field(default=600, ge=1)
    activity_comment_include_failed_output_lines: int = Field(default=2, ge=0)

    # Filled in from Secrets.
    linear_api_key: str = ""
    linear_webhook_secret: str = ""

    @classmethod
    def load(cls, path: Path) -> Config:
        raw = yaml.safe_load(path.read_text())
        cfg = cls.model_validate(raw)
        secrets = Secrets()
        cfg = cfg.model_copy(
            update={
                "linear_api_key": secrets.linear_api_key,
                "linear_webhook_secret": secrets.linear_webhook_secret,
            }
        )
        # Expand ~ now so downstream code can assume absolute paths.
        cfg = cfg.model_copy(
            update={
                "workspace_root": _expand(cfg.workspace_root),
                "log_root": _expand(cfg.log_root),
                "db_path": _expand(cfg.db_path),
            }
        )
        return cfg
