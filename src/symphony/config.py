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

import warnings
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar, Literal, Self

import yaml
from pydantic import AliasChoices, BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .agent.codex_models import DEFAULT_CODEX_MODEL, SUPPORTED_CODEX_MODELS
from .github.client import MergeStrategy
from .ui.status import CanonicalState


def _expand(path: str | Path) -> Path:
    """Expand ~ and env vars; return absolute Path."""
    import os

    return Path(os.path.expandvars(os.path.expanduser(str(path)))).resolve()


class AcceptanceConfig(BaseModel):
    """Per-binding Acceptance-stage knobs.

    `off` preserves the current Review → Merge behavior. The other modes all
    enter the Acceptance stage; this slice runs `code_only` with a real
    Claude diff-vs-description verdict and keeps future modes pass-through.
    """

    mode: Literal["off", "code_only", "dev", "preview"] = "off"
    preview_url_pattern: str | None = None
    preview_wait_timeout_secs: float = Field(default=300.0, ge=0)
    dev_command: str | None = None
    dev_port: int | None = Field(default=None, ge=1, le=65535)
    taste_guide: str | None = None
    time_cap_minutes: float = Field(default=15.0, gt=0)


class TrackerStates(BaseModel):
    """Issue-tracker workflow state names per role.

    Names must match the tracker project's workflow exactly. Symphony does not
    create or rename states. `ready` has no default — every binding must
    declare which state the orchestrator picks issues up from, since teams
    rename or replace it (Backlog, Todo, Up Next, …).

    `local_code_review` is the in-workspace pre-PR review lane; `code_review`
    is the automated PR-review lane; `needs_approval` is the human-input lane
    for failed merges and stage-failure parking. Legacy configs without
    `code_review` inherit their old `needs_approval` lane, including that
    field's legacy default, so existing deployments keep loading during the
    schema split. `blocked` is the agent-error parking lane for
    failed merges and rejected work. `waiting` is a separate optional
    dependency-waiting lane used only when pickup should bounce tickets blocked
    by other Linear issues.
    """

    ready: str = Field(min_length=1)
    local_code_review: str = "Local Code Review"
    code_review: str = ""
    in_progress: str = "In Progress"
    needs_approval: str = "Needs Approval"
    in_acceptance: str = "In Acceptance"
    blocked: str = "Blocked"
    waiting: str | None = None
    done: str = "Done"

    @model_validator(mode="before")
    @classmethod
    def _derive_legacy_code_review(cls, data: Any) -> Any:
        if isinstance(data, dict) and "code_review" not in data:
            needs_approval = data.get(
                "needs_approval", cls.model_fields["needs_approval"].default
            )
            return {**data, "code_review": needs_approval}
        return data


LinearStates = TrackerStates


class RepoBinding(BaseModel):
    """One issue-tracker project to GitHub repo pairing.

    Symphony scans `project_key`'s issues for ones in the configured
    "ready" state with `issue_label` (if set), and dispatches them to
    `github_repo`. Per-repo `max_concurrent` is enforced in addition to the
    global cap so a single noisy team can't starve others.
    """

    provider: Literal["linear", "jira"] = "linear"
    project_key: str = Field(
        validation_alias=AliasChoices("project_key", "linear_team_key")
    )
    github_repo: str
    tracker_provider: str = ""
    tracker_site: str = ""
    base_url: str | None = None
    agent: Literal["claude", "codex"] = "claude"
    codex_model: str = DEFAULT_CODEX_MODEL
    issue_label: str | None = None
    branch_prefix: str = "symphony"
    base_branch: str | None = None
    merge_strategy: MergeStrategy = "squash"
    # `allow_auto_merge` controls GitHub's `gh pr merge --auto` flag.
    # `auto_merge` controls whether Symphony invokes `gh pr merge` at all.
    allow_auto_merge: bool = True
    auto_merge: bool = True
    max_concurrent: int = 2
    runner: Literal["local", "e2b", "daytona"] = "local"
    # Where review happens — two orthogonal switches. `local_review` runs the
    # in-workspace reviewer loop before opening the PR; `remote_review` runs
    # the `@codex` GitHub-bot review on the PR. The default false/true is
    # remote-only (today's behavior). Both true is sequential (local → PR →
    # remote); both false skips review entirely (implement → PR → CI → merge).
    # The legacy `review_strategy` enum maps onto these via a root validator.
    # See `docs/local-review-flow.md`.
    local_review: bool = False
    remote_review: bool = True
    # Reviewer agent for local/hybrid strategies. `None` picks the opposite
    # family of `agent` (claude ↔ codex) so the reviewer has independent
    # blind spots from the implementer.
    reviewer_agent: Literal["claude", "codex"] | None = None
    # Codex model for the reviewer when `reviewer_agent` resolves to
    # `codex`. Defaults to the binding's `codex_model` if unset.
    reviewer_codex_model: str | None = None
    # Claude model for local-review claude passes (finder, single-pass
    # reviewer, pass-2 verifier, and the inline fixer). `None` keeps the
    # operator's CLI default — no `--model` is passed. Independent of the
    # main `implement` stage model.
    local_review_claude_model: str | None = None
    # Per-binding override for the local-review iteration cap. The
    # right number is usually smaller than the remote `review_iteration_cap`
    # because the local loop converges fast or it isn't going to — many
    # rounds usually means the reviewer is stuck on a pathological case.
    # `None` falls back to `Config.local_review_iteration_cap`.
    local_review_iteration_cap: int | None = Field(default=None, ge=1)
    # Per-binding override for the GitHub PR summary comment posted on
    # local-review APPROVED. `None` falls back to
    # `Config.post_local_review_pr_summary`. Some repos prefer quieter
    # PR threads (already wired into a GitHub-side dashboard); others
    # want every verdict surfaced for human reviewers.
    post_local_review_pr_summary: bool | None = None
    # Optional build/test gate (e.g. "pnpm build && pnpm test") run in the
    # workspace after the last code-mutating stage (post local-review fixes)
    # and before push, so what's verified is what gets pushed. Red gets one
    # implementer fix turn, then a re-run; still red fails closed (no push,
    # no PR). `None` skips the gate entirely (today's behavior).
    verify_cmd: str | None = None
    # Wall-clock cap for one `verify_cmd` invocation. `None` falls back to
    # `Config.command_timeout_secs`.
    verify_timeout_secs: int | None = Field(default=None, ge=1)
    # Extra env injected into this binding's agent subprocesses. YAML values
    # name keys in symphony's `.env` (or the process env) — the secrets
    # themselves never live in the YAML. `Config.load` replaces each value
    # with the resolved secret via `resolve_env`; an unresolvable key fails
    # the load so the daemon dies loudly at boot, not mid-run.
    env: dict[str, str] = Field(default_factory=dict)
    # MCP servers this binding's agents may see. Entries are claude
    # `--mcp-config` server definitions, passed through verbatim. Spawns use
    # `--strict-mcp-config`, so anything not listed here is invisible to the
    # agent. Default: none.
    mcp_servers: dict[str, dict[str, Any]] = Field(default_factory=dict)
    # Escape hatch for repos that deliberately run neither CI nor a
    # `verify_cmd`: when true, a clean no_signal + mergeable PR auto-merges
    # even with zero CI checks and no green verify (SYM-108). Default false
    # keeps such unverified PRs waiting for an operator.
    allow_unverified_merge: bool = False
    acceptance: AcceptanceConfig = Field(default_factory=AcceptanceConfig)
    activity_comments_enabled: bool | None = None
    activity_comment_interval_secs: int | None = Field(default=None, ge=1)
    activity_comment_min_interval_secs: int | None = Field(default=None, ge=1)
    activity_comment_event_threshold: int | None = Field(default=None, ge=1)
    activity_comment_long_running_secs: int | None = Field(default=None, ge=1)
    activity_comment_long_running_repeat_secs: int | None = Field(default=None, ge=1)
    activity_comment_include_failed_output_lines: int | None = Field(default=None, ge=0)
    webhook_enabled: bool = True
    webhook_secret: str | None = None
    reconcile_enabled: bool = True
    states: TrackerStates = Field(validation_alias=AliasChoices("states", "linear_states"))

    def _apply_tracker_context_defaults(
        self, *, jira_base_url: str | None = None
    ) -> None:
        if not self.tracker_provider:
            self.tracker_provider = self.provider
        default_site = "default"
        if self.provider == "jira":
            default_site = (self.base_url or jira_base_url or "default").rstrip("/")
        if not self.tracker_site or (
            self.provider == "jira"
            and self.tracker_site == "default"
            and default_site != "default"
        ):
            self.tracker_site = default_site

    def apply_tracker_secret_defaults(self, *, jira_base_url: str | None = None) -> None:
        self._apply_tracker_context_defaults(jira_base_url=jira_base_url)

    def resolve_env(self, source: Mapping[str, str]) -> None:
        """Replace `env:` key names with their values from `source`."""
        missing = sorted(key for key in self.env.values() if key not in source)
        if missing:
            raise ValueError(
                f"binding {self.project_key}/{self.github_repo}: env keys not "
                f"found in .env or process env: {', '.join(missing)}"
            )
        self.env = {var: source[key] for var, key in self.env.items()}

    # Legacy `review_strategy` → boolean mapping. The old enum dropped the
    # remote fallback that `local` once carried; the booleans are now the
    # source of truth.
    _LEGACY_REVIEW_STRATEGY: ClassVar[dict[str, tuple[bool, bool]]] = {
        "remote": (False, True),
        "hybrid": (True, True),
        "local": (True, False),
    }

    @model_validator(mode="before")
    @classmethod
    def _map_legacy_review_strategy(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "review_strategy" not in data:
            return data
        data = dict(data)
        strategy = data.pop("review_strategy")
        if "local_review" in data or "remote_review" in data:
            warnings.warn(
                "Both `review_strategy` and `local_review`/`remote_review` are "
                "set; `review_strategy` is ignored and the booleans win.",
                DeprecationWarning,
                stacklevel=2,
            )
            return data
        if strategy not in cls._LEGACY_REVIEW_STRATEGY:
            supported = ", ".join(sorted(cls._LEGACY_REVIEW_STRATEGY))
            raise ValueError(
                f"unknown review_strategy {strategy!r}; supported: {supported}"
            )
        warnings.warn(
            "`review_strategy` is deprecated; use the `local_review` and "
            "`remote_review` booleans instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        local, remote = cls._LEGACY_REVIEW_STRATEGY[strategy]
        data["local_review"] = local
        data["remote_review"] = remote
        return data

    @model_validator(mode="after")
    def _derive_tracker_context(self) -> Self:
        self._apply_tracker_context_defaults()
        return self

    @property
    def review_strategy(self) -> Literal["remote", "local", "hybrid"]:
        """Deprecated legacy view of the review booleans.

        Bridges call-sites not yet migrated to `resolved_local_review` /
        `resolved_remote_review`. The new "no review" combination
        (`local_review=False, remote_review=False`) has no legacy equivalent
        and reports as `remote`.
        """
        if self.local_review and self.remote_review:
            return "hybrid"
        if self.local_review:
            return "local"
        return "remote"

    @property
    def linear_team_key(self) -> str:
        return self.project_key

    @linear_team_key.setter
    def linear_team_key(self, value: str) -> None:
        self.project_key = value

    @property
    def linear_states(self) -> TrackerStates:
        return self.states

    @linear_states.setter
    def linear_states(self, value: TrackerStates) -> None:
        self.states = value

    def model_copy(
        self, *, update: Mapping[str, Any] | None = None, deep: bool = False
    ) -> Self:
        if update is not None:
            normalized = dict(update)
            if "linear_team_key" in normalized and "project_key" not in normalized:
                normalized["project_key"] = normalized.pop("linear_team_key")
            if "linear_states" in normalized and "states" not in normalized:
                normalized["states"] = normalized.pop("linear_states")
            update = normalized
        copied = super().model_copy(update=update, deep=deep)
        copied._apply_tracker_context_defaults()
        return copied

    @field_validator("codex_model")
    @classmethod
    def _known_codex_model(cls, value: str) -> str:
        if value not in SUPPORTED_CODEX_MODELS:
            supported = ", ".join(sorted(SUPPORTED_CODEX_MODELS))
            raise ValueError(f"unknown Codex model {value!r}; supported: {supported}")
        return value

    @field_validator("reviewer_codex_model")
    @classmethod
    def _known_reviewer_codex_model(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value not in SUPPORTED_CODEX_MODELS:
            supported = ", ".join(sorted(SUPPORTED_CODEX_MODELS))
            raise ValueError(
                f"unknown reviewer Codex model {value!r}; supported: {supported}"
            )
        return value

    def resolved_local_review(self) -> bool:
        """Whether the in-workspace reviewer loop runs before the PR opens."""
        return self.local_review

    def resolved_remote_review(self) -> bool:
        """Whether the `@codex` GitHub-bot review runs on the PR."""
        return self.remote_review

    def resolved_reviewer_agent(self) -> Literal["claude", "codex"]:
        """Reviewer agent after applying the implementer-opposite default."""
        if self.reviewer_agent is not None:
            return self.reviewer_agent
        return "codex" if self.agent == "claude" else "claude"

    def resolved_reviewer_codex_model(self) -> str:
        """Codex model for the reviewer when it's the codex agent."""
        return self.reviewer_codex_model or self.codex_model

    def resolved_local_review_iteration_cap(self, global_cap: int) -> int:
        """Per-binding override wins; falls back to the global default."""
        return (
            self.local_review_iteration_cap
            if self.local_review_iteration_cap is not None
            else global_cap
        )

    def resolved_post_local_review_pr_summary(self, global_value: bool) -> bool:
        """Per-binding override wins; falls back to the global default."""
        if self.post_local_review_pr_summary is None:
            return global_value
        return self.post_local_review_pr_summary

    def resolved_verify_timeout_secs(self, global_default: int) -> int:
        """Per-binding override wins; falls back to `command_timeout_secs`."""
        return (
            self.verify_timeout_secs
            if self.verify_timeout_secs is not None
            else global_default
        )


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
    github_webhook_secret: str = Field(
        default="", validation_alias="GITHUB_WEBHOOK_SECRET"
    )
    jira_base_url: str = Field(default="", validation_alias="JIRA_BASE_URL")
    jira_email: str = Field(default="", validation_alias="JIRA_EMAIL")
    jira_api_token: str = Field(default="", validation_alias="JIRA_API_TOKEN")
    jira_webhook_secret: str = Field(default="", validation_alias="JIRA_WEBHOOK_SECRET")


class UIStatusThresholds(BaseModel):
    """Per-state stuck thresholds for canonical UI status."""

    paused_secs: int = Field(
        default=15 * 60,
        ge=0,
        validation_alias=AliasChoices("paused_secs", "awaiting_operator_secs"),
    )
    awaiting_merge_secs: int = Field(default=4 * 60 * 60, ge=0)
    running_secs: int = Field(default=30 * 60, ge=0)
    awaiting_review_trigger_secs: int = Field(default=10 * 60, ge=0)
    pr_open_secs: int = Field(default=24 * 60 * 60, ge=0)
    pr_no_progress_threshold_secs: int = Field(default=2 * 60 * 60, ge=0)

    def to_timedeltas(self) -> dict[CanonicalState, timedelta]:
        return {
            CanonicalState.PAUSED: timedelta(seconds=self.paused_secs),
            CanonicalState.AWAITING_MERGE: timedelta(
                seconds=self.awaiting_merge_secs
            ),
            CanonicalState.RUNNING: timedelta(seconds=self.running_secs),
            CanonicalState.AWAITING_REVIEW_TRIGGER: timedelta(
                seconds=self.awaiting_review_trigger_secs
            ),
            CanonicalState.PR_OPEN: timedelta(seconds=self.pr_open_secs),
        }

    def pr_no_progress_threshold(self) -> timedelta:
        return timedelta(seconds=self.pr_no_progress_threshold_secs)


class UIConfig(BaseModel):
    """Web UI exposure knobs."""

    enabled: bool = True
    status_stuck_thresholds: UIStatusThresholds = Field(
        default_factory=UIStatusThresholds
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
    reconcile_interval_secs: int = Field(default=300, ge=1)
    reconcile_max_per_tick: int = Field(default=50, ge=1)
    reconcile_max_actions_per_tick: int = Field(default=10, ge=1)
    reconcile_backoff_secs: int = Field(default=600, ge=1)
    ui: UIConfig = Field(default_factory=UIConfig)

    repos: list[RepoBinding] = Field(default_factory=list)

    review_iteration_cap: int = 12
    # Local-review converges fast or it doesn't. The right cap is well
    # below `review_iteration_cap`: more rounds means the in-workspace
    # reviewer is stuck. Per-binding overrides live on `RepoBinding`.
    local_review_iteration_cap: int = Field(default=6, ge=1)
    # When the local-review APPROVES, post a summary PR comment with
    # the iteration count + cost so human reviewers visiting GitHub
    # see the verdict trail (not just the Linear thread). Set to false
    # on bindings where the PR thread should stay quiet.
    post_local_review_pr_summary: bool = True
    stall_timeout_secs: int = 300
    # Outer cap for a single agent tool call (command_execution). While the
    # agent has a command in flight, the stall watchdog measures against this
    # instead of `stall_timeout_secs`, so a long-but-healthy subprocess (broad
    # rg, pnpm install, pytest) isn't killed as a false-positive stall.
    command_timeout_secs: int = 1800
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
    github_webhook_secret: str = ""
    jira_base_url: str = ""
    jira_email: str = ""
    jira_api_token: str = ""
    jira_webhook_secret: str = ""

    @classmethod
    def load(cls, path: Path) -> Config:
        raw = yaml.safe_load(path.read_text())
        cfg = cls.model_validate(raw)
        secrets = Secrets()
        cfg = cfg.model_copy(
            update={
                "linear_api_key": secrets.linear_api_key,
                "linear_webhook_secret": secrets.linear_webhook_secret,
                "github_webhook_secret": secrets.github_webhook_secret,
                "jira_base_url": secrets.jira_base_url,
                "jira_email": secrets.jira_email,
                "jira_api_token": secrets.jira_api_token,
                "jira_webhook_secret": secrets.jira_webhook_secret,
            }
        )
        # Per-binding agent env: same `.env` file pydantic-settings reads,
        # with the real process env winning on conflicts (mirrors
        # pydantic-settings precedence). Resolution happens here so a typo'd
        # key kills the daemon at boot instead of stranding a run.
        import os

        from dotenv import dotenv_values

        env_source: dict[str, str] = {
            key: value for key, value in dotenv_values(".env").items() if value is not None
        }
        env_source.update(os.environ)
        for binding in cfg.repos:
            binding.apply_tracker_secret_defaults(jira_base_url=cfg.jira_base_url)
            binding.resolve_env(env_source)
        # Expand ~ now so downstream code can assume absolute paths.
        cfg = cfg.model_copy(
            update={
                "workspace_root": _expand(cfg.workspace_root),
                "log_root": _expand(cfg.log_root),
                "db_path": _expand(cfg.db_path),
            }
        )
        return cfg
