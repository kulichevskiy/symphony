"""Sanity tests for config loading. Strict-mypy + ruff-clean."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from symphony.config import Config, LinearStates

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
    assert cfg.repos[0].issue_label == "symphony"
    assert cfg.linear_api_key == "lin_api_test"
    assert cfg.repos[0].linear_states.ready == "Todo"


def test_repo_runner_defaults_to_local(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    p = tmp_path / "cfg.yaml"
    p.write_text(
        f"repos:\n  - linear_team_key: ENG\n    github_repo: org/repo\n{_BINDING_STATES}"
    )
    cfg = Config.load(p)
    assert cfg.repos[0].runner == "local"


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
    assert cfg.repos[1].linear_states.ready == "Todo"


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
