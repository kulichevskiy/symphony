"""Sanity tests for config loading. Strict-mypy + ruff-clean."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from symphony.config import Config, LinearStates, RepoBinding, UIStatusThresholds
from symphony.ui.status import CanonicalState

_BINDING_STATES = """
    linear_states:
      ready: Todo
      in_progress: In Progress
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
    assert cfg.repos[0].issue_label == "symphony"
    assert cfg.linear_api_key == "lin_api_test"
    assert cfg.repos[0].linear_states.ready == "Todo"
    assert cfg.repos[0].linear_states.waiting is None
    assert cfg.ui.enabled is True


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
            linear_states=LinearStates(ready="Todo"),
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
      needs_approval: Review
      blocked: Blocked
      waiting: Waiting
      done: Done
  - linear_team_key: WEB
    github_repo: org/web
    linear_states:
      ready: Todo
      in_progress: In Progress
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].linear_states.ready == "Backlog"
    assert cfg.repos[0].linear_states.in_progress == "Doing"
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


def test_local_review_iteration_cap_default_global_is_6(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Global default cap is 6 — well below remote's 12 because the
    local loop should converge fast or not at all."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    p = tmp_path / "cfg.yaml"
    p.write_text(
        f"repos:\n  - linear_team_key: ENG\n    github_repo: org/repo\n{_BINDING_STATES}"
    )
    cfg = Config.load(p)
    assert cfg.local_review_iteration_cap == 6
    # Remote cap unchanged.
    assert cfg.review_iteration_cap == 12
    binding = cfg.repos[0]
    assert binding.local_review_iteration_cap is None
    # Resolved cap falls back to global default.
    assert (
        binding.resolved_local_review_iteration_cap(
            cfg.local_review_iteration_cap
        )
        == 6
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
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    with pytest.raises(ValidationError):
        Config.load(p)
