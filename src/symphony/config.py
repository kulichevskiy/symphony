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
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

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
    issue_label: str | None = None
    branch_prefix: str = "symphony"
    base_branch: str | None = None
    merge_strategy: MergeStrategy = "squash"
    max_concurrent: int = 2
    runner: Literal["local", "e2b", "daytona"] = "local"
    linear_states: LinearStates


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


class Config(BaseModel):
    """Top-level config. Loaded from YAML, secrets layered on at the end."""

    poll_interval_secs: int = 60
    global_max_concurrent: int = 4
    workspace_root: Path = Path("~/symphony/workspaces")
    log_root: Path = Path("~/symphony/logs")
    db_path: Path = Path("~/symphony/state.sqlite")

    repos: list[RepoBinding] = Field(default_factory=list)

    review_iteration_cap: int = 6
    cost_cap_per_issue_usd: float = 5.0
    stall_timeout_secs: int = 300

    # Filled in from Secrets.
    linear_api_key: str = ""

    @classmethod
    def load(cls, path: Path) -> Config:
        raw = yaml.safe_load(path.read_text())
        cfg = cls.model_validate(raw)
        secrets = Secrets()
        cfg = cfg.model_copy(update={"linear_api_key": secrets.linear_api_key})
        # Expand ~ now so downstream code can assume absolute paths.
        cfg = cfg.model_copy(
            update={
                "workspace_root": _expand(cfg.workspace_root),
                "log_root": _expand(cfg.log_root),
                "db_path": _expand(cfg.db_path),
            }
        )
        return cfg
