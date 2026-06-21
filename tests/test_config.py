"""Sanity tests for config loading. Strict-mypy + ruff-clean."""

from __future__ import annotations

import warnings
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from symphony.config import (
    AcceptanceConfig,
    Config,
    LinearStates,
    RepoBinding,
    Secrets,
    TrackerStates,
    UIStatusThresholds,
)
from symphony.ui.status import CanonicalState

_BINDING_STATES = """
    linear_states:
      ready: Todo
      in_progress: In Progress
      code_review: In Review
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
"""


def test_loads_example_config(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test")
    raw = f"""
poll_interval_secs: 30
global_max_concurrent: 2
workspace_root: /tmp/symphony/workspaces
log_root: /tmp/symphony/logs
db_path: /tmp/symphony/state.sqlite
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    issue_label: symphony
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.poll_interval_secs == 30
    assert cfg.global_max_concurrent == 2
    assert len(cfg.repos) == 1
    assert cfg.repos[0].linear_team_key == "ENG"
    assert cfg.repos[0].github_repo == "org/api-svc"
    assert cfg.repos[0].agent == "claude"  # default
    assert cfg.repos[0].merge_strategy == "squash"  # default
    assert cfg.repos[0].auto_merge is True  # default
    assert cfg.repos[0].issue_label == "symphony"
    assert cfg.linear_api_key == "lin_api_test"
    assert cfg.repos[0].linear_states.ready == "Todo"
    assert cfg.repos[0].linear_states.code_review == "In Review"
    assert cfg.repos[0].linear_states.waiting is None
    assert cfg.ui.enabled is True


def test_repo_binding_accepts_legacy_linear_aliases(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test")
    raw = """
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    linear_states:
      ready: Todo
      code_review: In Review
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)

    cfg = Config.load(p)
    binding = cfg.repos[0]

    assert binding.provider == "linear"
    assert binding.project_key == "ENG"
    assert binding.linear_team_key == "ENG"
    assert binding.states.ready == "Todo"
    assert binding.linear_states.code_review == "In Review"


def test_repo_binding_model_copy_accepts_legacy_linear_aliases() -> None:
    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/api-svc",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )

    copied = binding.model_copy(
        update={
            "linear_team_key": "WEB",
            "linear_states": LinearStates(ready="Backlog", code_review="In Review"),
        }
    )

    assert copied.project_key == "WEB"
    assert copied.linear_team_key == "WEB"
    assert copied.states.ready == "Backlog"
    assert copied.linear_states.code_review == "In Review"


def test_jira_binding_and_secrets_validate(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test")
    monkeypatch.setenv("JIRA_BASE_URL", "https://jira.example.test")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.test")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    monkeypatch.setenv("JIRA_WEBHOOK_SECRET", "jira-webhook-secret")
    raw = """
repos:
  - provider: jira
    project_key: SYM
    base_url: https://jira.example.test
    github_repo: org/api-svc
    states:
      ready: To Do
      code_review: In Review
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)

    cfg = Config.load(p)
    binding = cfg.repos[0]
    secrets = Secrets()

    assert binding.provider == "jira"
    assert binding.project_key == "SYM"
    assert binding.base_url == "https://jira.example.test"
    assert isinstance(binding.states, TrackerStates)
    assert binding.states.ready == "To Do"
    assert secrets.jira_base_url == "https://jira.example.test"
    assert secrets.jira_email == "bot@example.test"
    assert secrets.jira_api_token == "jira-token"
    assert secrets.jira_webhook_secret == "jira-webhook-secret"


def test_jira_binding_can_use_env_base_url(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("JIRA_BASE_URL", "https://jira.example.test")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.test")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    raw = """
repos:
  - provider: jira
    project_key: SYM
    github_repo: org/api-svc
    states:
      ready: To Do
      code_review: In Review
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)

    cfg = Config.load(p)
    binding = cfg.repos[0]

    assert binding.base_url is None
    assert binding.tracker_provider == "jira"
    assert binding.tracker_site == "https://jira.example.test"


def test_ui_can_be_disabled(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
ui:
  enabled: false
repos: []
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.ui.enabled is False


def test_ui_status_threshold_defaults_and_overrides(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    defaults = UIStatusThresholds().to_timedeltas()
    assert defaults[CanonicalState.PAUSED] == timedelta(minutes=15)
    assert defaults[CanonicalState.AWAITING_MERGE] == timedelta(hours=4)
    assert defaults[CanonicalState.RUNNING] == timedelta(minutes=30)
    assert defaults[CanonicalState.AWAITING_REVIEW_TRIGGER] == timedelta(minutes=10)
    assert defaults[CanonicalState.PR_OPEN] == timedelta(hours=24)
    assert UIStatusThresholds().pr_no_progress_threshold() == timedelta(hours=2)

    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
ui:
  status_stuck_thresholds:
    paused_secs: 120
    awaiting_merge_secs: 7200
    running_secs: 1800
    awaiting_review_trigger_secs: 60
    pr_open_secs: 3600
    pr_no_progress_threshold_secs: 1800
repos: []
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    thresholds = cfg.ui.status_stuck_thresholds.to_timedeltas()
    assert thresholds[CanonicalState.PAUSED] == timedelta(seconds=120)
    assert thresholds[CanonicalState.AWAITING_MERGE] == timedelta(seconds=7200)
    assert thresholds[CanonicalState.RUNNING] == timedelta(seconds=1800)
    assert thresholds[CanonicalState.AWAITING_REVIEW_TRIGGER] == timedelta(seconds=60)
    assert thresholds[CanonicalState.PR_OPEN] == timedelta(seconds=3600)
    assert cfg.ui.status_stuck_thresholds.pr_no_progress_threshold() == timedelta(
        seconds=1800
    )


def test_ui_status_thresholds_accept_legacy_awaiting_operator_key(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
ui:
  status_stuck_thresholds:
    awaiting_operator_secs: 300
repos: []
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    thresholds = cfg.ui.status_stuck_thresholds.to_timedeltas()
    assert thresholds[CanonicalState.PAUSED] == timedelta(seconds=300)


def test_repo_runner_defaults_to_local(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    p = tmp_path / "cfg.yaml"
    p.write_text(
        f"repos:\n  - linear_team_key: ENG\n    github_repo: org/repo\n{_BINDING_STATES}"
    )
    cfg = Config.load(p)
    assert cfg.repos[0].runner == "local"
    assert cfg.repos[0].codex_model == "gpt-5.1-codex"


def test_acceptance_config_defaults() -> None:
    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )

    assert binding.acceptance == AcceptanceConfig()
    assert binding.acceptance.mode == "off"
    assert binding.acceptance.preview_url_pattern is None
    assert binding.acceptance.preview_wait_timeout_secs == 300
    assert binding.acceptance.dev_command is None
    assert binding.acceptance.dev_port is None
    assert binding.acceptance.taste_guide is None
    assert binding.acceptance.time_cap_minutes == 15
    assert binding.linear_states.in_acceptance == "In Acceptance"


def test_acceptance_config_can_be_configured(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    acceptance:
      mode: code_only
      preview_url_pattern: https://preview.example/{issue}
      preview_wait_timeout_secs: 12.5
      dev_command: npm run dev
      dev_port: 3000
      taste_guide: docs/taste.md
      time_cap_minutes: 0.1
    linear_states:
      ready: Todo
      code_review: In Review
      in_acceptance: QA Acceptance
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)

    assert cfg.repos[0].acceptance.mode == "code_only"
    assert cfg.repos[0].acceptance.preview_url_pattern == "https://preview.example/{issue}"
    assert cfg.repos[0].acceptance.preview_wait_timeout_secs == pytest.approx(12.5)
    assert cfg.repos[0].acceptance.dev_command == "npm run dev"
    assert cfg.repos[0].acceptance.dev_port == 3000
    assert cfg.repos[0].acceptance.taste_guide == "docs/taste.md"
    assert cfg.repos[0].acceptance.time_cap_minutes == pytest.approx(0.1)
    assert cfg.repos[0].linear_states.in_acceptance == "QA Acceptance"


def test_codex_model_can_be_configured(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: codex
    codex_model: gpt-5.1-codex-max
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].codex_model == "gpt-5.1-codex-max"


def test_activity_comment_config_defaults_and_overrides(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
activity_comments_enabled: true
activity_comment_interval_secs: 300
activity_comment_min_interval_secs: 120
activity_comment_event_threshold: 20
activity_comment_long_running_secs: 300
activity_comment_long_running_repeat_secs: 600
activity_comment_include_failed_output_lines: 2
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: codex
    activity_comments_enabled: false
    activity_comment_interval_secs: 60
    activity_comment_event_threshold: 5
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.activity_comment_interval_secs == 300
    assert cfg.activity_comment_long_running_repeat_secs == 600
    assert cfg.repos[0].activity_comments_enabled is False
    assert cfg.repos[0].activity_comment_interval_secs == 60
    assert cfg.repos[0].activity_comment_event_threshold == 5


def test_github_webhook_config_defaults_and_overrides(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "global-secret")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    webhook_secret: repo-secret
{_BINDING_STATES}
  - linear_team_key: WEB
    github_repo: org/web
    webhook_enabled: false
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.github_webhook_secret == "global-secret"
    assert cfg.repos[0].webhook_enabled is True
    assert cfg.repos[0].webhook_secret == "repo-secret"
    assert cfg.repos[1].webhook_enabled is False
    assert cfg.repos[1].webhook_secret is None


def test_reconcile_config_defaults_and_overrides(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
reconcile_interval_secs: 120
reconcile_max_per_tick: 7
reconcile_max_actions_per_tick: 3
reconcile_backoff_secs: 900
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    reconcile_enabled: false
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.reconcile_interval_secs == 120
    assert cfg.reconcile_max_per_tick == 7
    assert cfg.reconcile_max_actions_per_tick == 3
    assert cfg.reconcile_backoff_secs == 900
    assert cfg.repos[0].reconcile_enabled is False

    default_cfg = Config()
    assert default_cfg.reconcile_interval_secs == 300
    assert default_cfg.reconcile_max_per_tick == 50
    assert default_cfg.reconcile_max_actions_per_tick == 10
    assert default_cfg.reconcile_backoff_secs == 600
    assert (
        RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
        ).reconcile_enabled
        is True
    )


def test_unknown_codex_model_fails(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: codex
    codex_model: future-codex
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError, match="unknown Codex model"):
        Config.load(p)


def test_linear_states_ready_has_no_default() -> None:
    """`ready` must be supplied explicitly — there is no safe default."""
    with pytest.raises(ValidationError):
        LinearStates()  # type: ignore[call-arg]


def test_linear_states_review_lane_defaults() -> None:
    """Review lanes keep legacy defaults while adding a local-review lane."""
    states = LinearStates(ready="Todo")

    assert states.local_code_review == "Local Code Review"
    assert states.code_review == "Needs Approval"


def test_per_binding_linear_states(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Each binding declares its own LinearStates block."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    linear_states:
      ready: Backlog
      in_progress: Doing
      code_review: Review
      needs_approval: Review
      blocked: Blocked
      waiting: Waiting
      done: Done
  - linear_team_key: WEB
    github_repo: org/web
    linear_states:
      ready: Todo
      in_progress: In Progress
      code_review: In Review
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].linear_states.ready == "Backlog"
    assert cfg.repos[0].linear_states.in_progress == "Doing"
    assert cfg.repos[0].linear_states.code_review == "Review"
    assert cfg.repos[0].linear_states.waiting == "Waiting"
    assert cfg.repos[1].linear_states.ready == "Todo"


def test_review_strategy_defaults_to_remote(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Default behavior must keep today's @codex-bot loop until operators opt in."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    p = tmp_path / "cfg.yaml"
    p.write_text(
        f"repos:\n  - linear_team_key: ENG\n    github_repo: org/repo\n{_BINDING_STATES}"
    )
    cfg = Config.load(p)
    binding = cfg.repos[0]
    assert binding.review_strategy == "remote"
    assert binding.reviewer_agent is None
    assert binding.reviewer_codex_model is None
    assert binding.local_review_claude_model is None


def test_local_review_claude_model_can_be_set(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Per-binding claude model for local review; None → CLI default."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: codex
    local_review_claude_model: claude-sonnet-4-6
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].local_review_claude_model == "claude-sonnet-4-6"
    # Verifier model is independent; unset → None (CLI default / Opus).
    assert cfg.repos[0].local_review_verifier_claude_model is None


def test_local_review_verifier_claude_model_can_be_set(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Verifier model is selectable independently of the finder model."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    local_review_claude_model: claude-sonnet-4-6
    local_review_verifier_claude_model: claude-opus-4-8
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].local_review_claude_model == "claude-sonnet-4-6"
    assert cfg.repos[0].local_review_verifier_claude_model == "claude-opus-4-8"


def test_review_strategy_can_be_overridden(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    review_strategy: hybrid
    reviewer_agent: codex
    reviewer_codex_model: gpt-5.1-codex-max
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    binding = cfg.repos[0]
    assert binding.review_strategy == "hybrid"
    assert binding.reviewer_agent == "codex"
    assert binding.reviewer_codex_model == "gpt-5.1-codex-max"


def test_resolved_reviewer_agent_defaults_to_opposite_family(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
{_BINDING_STATES}
  - linear_team_key: WEB
    github_repo: org/web
    agent: codex
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].resolved_reviewer_agent() == "codex"
    assert cfg.repos[1].resolved_reviewer_agent() == "claude"


def test_resolved_reviewer_agent_honors_explicit_override(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """An operator who wants same-family review (e.g. for cost) can pin it."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    reviewer_agent: claude
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].resolved_reviewer_agent() == "claude"


def test_resolved_reviewer_codex_model_inherits_implementer_default(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: codex
    codex_model: gpt-5.1-codex-max
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].resolved_reviewer_codex_model() == "gpt-5.1-codex-max"


def test_unknown_reviewer_codex_model_fails(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    reviewer_agent: codex
    reviewer_codex_model: future-codex
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError, match="unknown reviewer Codex model"):
        Config.load(p)


def test_invalid_review_strategy_fails(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    review_strategy: rubber_stamp
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError):
        Config.load(p)


def _review_binding(**overrides) -> RepoBinding:  # type: ignore[no-untyped-def]
    kwargs = dict(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo", code_review="In Review"),
    )
    kwargs.update(overrides)
    return RepoBinding(**kwargs)  # type: ignore[arg-type]


def test_review_booleans_default_to_remote_only() -> None:
    """No fields set → remote-only (false/true), today's default."""
    binding = _review_binding()
    assert binding.local_review is False
    assert binding.remote_review is True
    assert binding.resolved_local_review() is False
    assert binding.resolved_remote_review() is True


@pytest.mark.parametrize(
    "local, remote",
    [(False, True), (True, False), (True, True), (False, False)],
)
def test_review_booleans_truth_table(local: bool, remote: bool) -> None:
    binding = _review_binding(local_review=local, remote_review=remote)
    assert binding.local_review is local
    assert binding.remote_review is remote
    assert binding.resolved_local_review() is local
    assert binding.resolved_remote_review() is remote


@pytest.mark.parametrize(
    "strategy, exp_local, exp_remote",
    [
        ("remote", False, True),
        ("hybrid", True, True),
        ("local", True, False),
    ],
)
def test_legacy_review_strategy_maps_to_booleans(
    strategy: str, exp_local: bool, exp_remote: bool
) -> None:
    with pytest.warns(DeprecationWarning, match="review_strategy"):
        binding = _review_binding(review_strategy=strategy)
    assert binding.local_review is exp_local
    assert binding.remote_review is exp_remote
    assert binding.resolved_local_review() is exp_local
    assert binding.resolved_remote_review() is exp_remote


def test_legacy_review_strategy_booleans_win_on_conflict() -> None:
    """Conflicting legacy + boolean config: booleans win, with a warning."""
    with pytest.warns(DeprecationWarning, match="ignored"):
        binding = _review_binding(
            review_strategy="remote", local_review=True, remote_review=False
        )
    assert binding.resolved_local_review() is True
    assert binding.resolved_remote_review() is False


def test_legacy_review_strategy_partial_boolean_wins_on_conflict() -> None:
    """A single boolean alongside legacy still suppresses the mapping."""
    with pytest.warns(DeprecationWarning, match="ignored"):
        binding = _review_binding(review_strategy="hybrid", remote_review=False)
    # local_review is left at its default (not derived from `hybrid`).
    assert binding.local_review is False
    assert binding.remote_review is False


@pytest.mark.parametrize(
    "local, remote, expected",
    [
        (False, True, "remote"),
        (True, False, "local"),
        (True, True, "hybrid"),
    ],
)
def test_review_strategy_property_bridges_booleans(
    local: bool, remote: bool, expected: str
) -> None:
    """The deprecated `review_strategy` view round-trips the legacy values."""
    binding = _review_binding(local_review=local, remote_review=remote)
    assert binding.review_strategy == expected


def test_local_review_iteration_cap_default_global_is_3(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Global default cap is 3 — well below remote's 12 because the
    local loop converges in 1–3 rounds empirically (138 sessions); the
    expensive 5–6 round tail escalates to a human instead."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    p = tmp_path / "cfg.yaml"
    p.write_text(
        f"repos:\n  - linear_team_key: ENG\n    github_repo: org/repo\n{_BINDING_STATES}"
    )
    cfg = Config.load(p)
    assert cfg.local_review_iteration_cap == 3
    # Remote cap unchanged.
    assert cfg.review_iteration_cap == 12
    binding = cfg.repos[0]
    assert binding.local_review_iteration_cap is None
    # Resolved cap falls back to global default.
    assert (
        binding.resolved_local_review_iteration_cap(
            cfg.local_review_iteration_cap
        )
        == 3
    )


def test_local_review_iteration_cap_per_binding_override(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
local_review_iteration_cap: 8
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    local_review_iteration_cap: 3
{_BINDING_STATES}
  - linear_team_key: WEB
    github_repo: org/web
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.local_review_iteration_cap == 8
    # ENG overrides; WEB inherits.
    assert cfg.repos[0].local_review_iteration_cap == 3
    assert (
        cfg.repos[0].resolved_local_review_iteration_cap(
            cfg.local_review_iteration_cap
        )
        == 3
    )
    assert cfg.repos[1].local_review_iteration_cap is None
    assert (
        cfg.repos[1].resolved_local_review_iteration_cap(
            cfg.local_review_iteration_cap
        )
        == 8
    )


def test_local_review_iteration_cap_must_be_positive(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """ge=1: a zero/negative cap would never enter the loop and is
    almost certainly a typo. Reject at load time."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    local_review_iteration_cap: 0
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError):
        Config.load(p)


def test_post_local_review_pr_summary_default_global_true(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    p = tmp_path / "cfg.yaml"
    p.write_text(
        f"repos:\n  - linear_team_key: ENG\n    github_repo: org/repo\n{_BINDING_STATES}"
    )
    cfg = Config.load(p)
    assert cfg.post_local_review_pr_summary is True
    assert cfg.repos[0].post_local_review_pr_summary is None
    assert (
        cfg.repos[0].resolved_post_local_review_pr_summary(
            cfg.post_local_review_pr_summary
        )
        is True
    )


def test_post_local_review_pr_summary_per_binding_override_off(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Global ON, but this binding's PR thread should stay quiet."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
post_local_review_pr_summary: true
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    post_local_review_pr_summary: false
{_BINDING_STATES}
  - linear_team_key: WEB
    github_repo: org/web
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.post_local_review_pr_summary is True
    # ENG overrides off; WEB inherits global True.
    assert cfg.repos[0].post_local_review_pr_summary is False
    assert (
        cfg.repos[0].resolved_post_local_review_pr_summary(
            cfg.post_local_review_pr_summary
        )
        is False
    )
    assert cfg.repos[1].post_local_review_pr_summary is None
    assert (
        cfg.repos[1].resolved_post_local_review_pr_summary(
            cfg.post_local_review_pr_summary
        )
        is True
    )


def test_post_local_review_pr_summary_per_binding_override_on_when_global_off(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Global OFF, but this binding's reviewers want the summary."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
post_local_review_pr_summary: false
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    post_local_review_pr_summary: true
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.post_local_review_pr_summary is False
    assert (
        cfg.repos[0].resolved_post_local_review_pr_summary(
            cfg.post_local_review_pr_summary
        )
        is True
    )


def test_yaml_missing_ready_fails(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A binding without `ready` must be rejected at load time."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    linear_states:
      in_progress: In Progress
      code_review: In Review
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError):
        Config.load(p)


def test_yaml_missing_code_review_uses_legacy_needs_approval(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Legacy bindings keep loading with review pointed at their old lane."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    linear_states:
      ready: Todo
      in_progress: In Progress
      needs_approval: In Review
      blocked: Blocked
      done: Done
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].linear_states.code_review == "In Review"
    assert cfg.repos[0].linear_states.needs_approval == "In Review"


def test_yaml_missing_code_review_uses_legacy_default_needs_approval(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Legacy bindings that relied on the old default keep loading."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    linear_states:
      ready: Todo
      in_progress: In Progress
      blocked: Blocked
      done: Done
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].linear_states.code_review == "Needs Approval"
    assert cfg.repos[0].linear_states.needs_approval == "Needs Approval"


def test_repo_binding_auto_merge_can_be_disabled(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    auto_merge: false
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].auto_merge is False


# --- roles matrix ---------------------------------------------------------


def test_roles_old_style_claude_config_resolves_identically() -> None:
    """A binding with no `roles:` block resolves builder→claude/None,
    review→opposite-family (codex) carrying the legacy codex_model."""
    binding = _review_binding(agent="claude", codex_model="gpt-5.1-codex-max")
    impl = binding.resolved_role("implement")
    assert impl.agent == "claude"
    assert impl.model is None  # claude builder → CLI default, no --model
    for name in ("fix", "accept"):
        assert binding.resolved_role(name).agent == "claude"
        assert binding.resolved_role(name).model is None
    rf = binding.resolved_role("review_find")
    rv = binding.resolved_role("review_verify")
    assert rf.agent == "codex" and rf.model == "gpt-5.1-codex-max"
    assert rv.agent == "codex" and rv.model == "gpt-5.1-codex-max"


def test_roles_old_style_codex_config_resolves_identically() -> None:
    """codex builder carries codex_model; review defaults to claude with the
    legacy local-review claude models (finder vs verifier kept distinct)."""
    binding = _review_binding(
        agent="codex",
        codex_model="gpt-5.1-codex",
        local_review_claude_model="sonnet",
        local_review_verifier_claude_model="opus",
    )
    impl = binding.resolved_role("implement")
    assert impl.agent == "codex" and impl.model == "gpt-5.1-codex"
    rf = binding.resolved_role("review_find")
    rv = binding.resolved_role("review_verify")
    assert rf.agent == "claude" and rf.model == "sonnet"
    assert rv.agent == "claude" and rv.model == "opus"


def test_roles_per_binding_deep_merges_per_field_over_global(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Global `roles:` default + per-binding `roles:` override deep-merge per
    field: `role = merge(global[role], binding[role])`."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
roles:
  implement:
    agent: claude
    model: sonnet
  review_find:
    agent: codex
    model: gpt-5.1-codex
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    roles:
      implement:
        model: opus
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    binding = cfg.repos[0]
    impl = binding.resolved_role("implement", cfg.roles)
    # agent inherited from global, model overridden per-field by binding.
    assert impl.agent == "claude"
    assert impl.model == "opus"
    # review_find untouched by the binding → global value stands.
    rf = binding.resolved_role("review_find", cfg.roles)
    assert rf.agent == "codex" and rf.model == "gpt-5.1-codex"


def test_roles_unknown_claude_model_fails(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    roles:
      implement:
        agent: claude
        model: gpt-5.1-codex
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError, match="unknown Claude model"):
        Config.load(p)


def test_roles_unknown_codex_model_fails(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    roles:
      implement:
        agent: codex
        model: future-codex
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError, match="unknown Codex model"):
        Config.load(p)


def test_roles_same_family_review_warns(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    roles:
      implement:
        agent: claude
      review_find:
        agent: claude
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.warns(UserWarning, match="cross-family review diversity"):
        Config.load(p)


def test_roles_config_builds_implement_command_with_model(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """A `roles`-based config drives the built `implement` claude command:
    resolved model → `--model`; no `roles:` → no flag (today's behavior)."""
    from symphony.orchestrator.poll import build_runner_command

    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    roles:
      implement:
        model: sonnet
{_BINDING_STATES}
  - linear_team_key: WEB
    github_repo: org/web
    agent: claude
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)

    def implement_command(binding: RepoBinding) -> list[str]:
        role = binding.resolved_role("implement", cfg.roles)
        is_codex = role.agent == "codex"
        return build_runner_command(
            role.agent,
            "do it",
            codex_model=(
                role.model if (is_codex and role.model) else binding.codex_model
            ),
            claude_model=None if is_codex else role.model,
        )

    with_role = implement_command(cfg.repos[0])
    assert with_role[with_role.index("--model") + 1] == "sonnet"
    # Binding without a `roles:` block → claude CLI default, no `--model`.
    assert "--model" not in implement_command(cfg.repos[1])


def test_roles_effort_resolves_and_builds_codex_command(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """A codex role with `effort` resolves onto the role and drives the codex
    command's `model_reasoning_effort` flag."""
    from symphony.orchestrator.poll import build_runner_command

    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: codex
    roles:
      implement:
        model: gpt-5.1-codex
        effort: high
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    role = cfg.repos[0].resolved_role("implement", cfg.roles)
    assert role.effort == "high"
    command = build_runner_command(
        role.agent,
        "do it",
        codex_model=role.model or "gpt-5.1-codex",
        effort=role.effort,
        workspace_path=tmp_path,
    )
    assert 'model_reasoning_effort="high"' in command


def test_roles_effort_resolves_and_builds_claude_command(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """A claude role with `effort` resolves onto the role and drives the claude
    command's `--effort` flag."""
    from symphony.orchestrator.poll import build_runner_command

    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    roles:
      implement:
        model: opus
        effort: high
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    role = cfg.repos[0].resolved_role("implement", cfg.roles)
    assert role.effort == "high"
    command = build_runner_command(
        role.agent,
        "do it",
        claude_model=role.model,
        effort=role.effort,
        workspace_path=tmp_path,
    )
    assert command[command.index("--effort") + 1] == "high"


def test_roles_unknown_claude_effort_fails(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    roles:
      implement:
        model: opus
        effort: turbo
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError, match="unknown Claude effort"):
        Config.load(p)


def test_roles_unknown_codex_effort_fails(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: codex
    roles:
      implement:
        model: gpt-5.1-codex
        effort: turbo
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError, match="unknown Codex effort"):
        Config.load(p)


def test_roles_effort_without_model_fails(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: codex
    roles:
      implement:
        effort: high
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError, match="effort.*requires.*model"):
        Config.load(p)


def test_roles_config_builds_fix_command_with_model(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """A `roles`-based config drives the built `fix` claude command through
    both builder-fix command builders (poll's `build_fix_runner_command` and
    the local-review/verify `_build_fix_command`): resolved model → `--model`;
    no `roles:` → no flag (today's behavior)."""
    from symphony.orchestrator.poll import build_fix_runner_command
    from symphony.pipeline.local_review_session import _build_fix_command

    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    roles:
      fix:
        model: sonnet
{_BINDING_STATES}
  - linear_team_key: WEB
    github_repo: org/web
    agent: claude
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)

    def fix_claude_model(binding: RepoBinding) -> str | None:
        role = binding.resolved_role("fix", cfg.roles)
        return None if role.agent == "codex" else role.model

    # Poll-side builder (remote @codex review fix + merge-gate fix).
    with_role = build_fix_runner_command(
        "claude", "fix it", claude_model=fix_claude_model(cfg.repos[0])
    )
    assert with_role[with_role.index("--model") + 1] == "sonnet"
    no_role = build_fix_runner_command(
        "claude", "fix it", claude_model=fix_claude_model(cfg.repos[1])
    )
    assert "--model" not in no_role

    # Pipeline-side builder (local-review fix loop + verify-gate fix turn).
    with_role = _build_fix_command(
        agent="claude",
        codex_model="gpt-5.1-codex",
        prompt="fix it",
        claude_model=fix_claude_model(cfg.repos[0]),
    )
    assert with_role[with_role.index("--model") + 1] == "sonnet"
    no_role = _build_fix_command(
        agent="claude",
        codex_model="gpt-5.1-codex",
        prompt="fix it",
        claude_model=fix_claude_model(cfg.repos[1]),
    )
    assert "--model" not in no_role


# --- roles matrix: deprecation + conflict (SYM-127) -----------------------


@pytest.mark.parametrize(
    "field, value",
    [
        ("agent", "codex"),
        ("reviewer_agent", "codex"),
        ("codex_model", "gpt-5.1-codex"),
        ("reviewer_codex_model", "gpt-5.1-codex"),
        ("local_review_claude_model", "sonnet"),
        ("local_review_verifier_claude_model", "opus"),
    ],
)
def test_legacy_role_field_emits_deprecation_warning(field: str, value: str) -> None:
    """Any legacy top-level role field warns and points at the matrix."""
    with pytest.warns(DeprecationWarning, match=field):
        _review_binding(**{field: value})


def test_no_legacy_role_field_no_deprecation_warning() -> None:
    """A binding that touches no legacy role field is silent."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _review_binding(local_review=True, roles={"implement": {"agent": "claude"}})
    assert not [w for w in caught if issubclass(w.category, DeprecationWarning)]


def test_legacy_field_and_per_binding_matrix_conflict_fails(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Legacy `agent` + a per-binding `roles[*].agent` for the same cell errors."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    roles:
      implement:
        agent: codex
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError, match="conflicts"):
        Config.load(p)


def test_legacy_field_and_global_matrix_conflict_fails(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Legacy `local_review_claude_model` + global `roles.review_find.model` errors."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
roles:
  review_find:
    model: sonnet
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    local_review_claude_model: haiku
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError, match="conflicts"):
        Config.load(p)


def test_legacy_agent_with_matrix_model_does_not_conflict(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Legacy `agent` and `roles.implement.model` target different cells → no error."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    agent: claude
    roles:
      implement:
        model: sonnet
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].resolved_role("implement", cfg.roles).model == "sonnet"


# --- roles matrix: effort knob (SYM-127) ----------------------------------


def test_roles_effort_unset_defaults_to_none() -> None:
    """All-unset effort = today's CLI-default (no flag) for every role."""
    binding = _review_binding()
    for name in ("implement", "review_find", "review_verify", "fix", "accept"):
        assert binding.resolved_role(name).effort is None


def test_roles_effort_resolves_per_field_over_global(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """Global `roles` effort default + per-binding override deep-merge per field.

    Effort requires an explicit model (validated as a pair), so each role pins
    a codex model; the assertion is about which `effort` wins per field.
    """
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = f"""
roles:
  implement:
    agent: codex
    model: gpt-5.1-codex
    effort: medium
  review_find:
    agent: codex
    model: gpt-5.1-codex
    effort: high
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    roles:
      implement:
        effort: low
{_BINDING_STATES}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    binding = cfg.repos[0]
    assert binding.resolved_role("implement", cfg.roles).effort == "low"
    assert binding.resolved_role("review_find", cfg.roles).effort == "high"
