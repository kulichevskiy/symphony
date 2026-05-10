"""Sanity tests for config loading. Strict-mypy + ruff-clean."""

from __future__ import annotations

from pathlib import Path

from symphony.config import Config


def test_loads_example_config(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test")
    raw = """
poll_interval_secs: 30
global_max_concurrent: 2
workspace_root: /tmp/symphony/workspaces
log_root: /tmp/symphony/logs
db_path: /tmp/symphony/state.sqlite
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    issue_label: symphony
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
    assert cfg.linear_states.ready == "Todo"  # default


def test_repo_runner_defaults_to_local(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    p = tmp_path / "cfg.yaml"
    p.write_text("repos:\n  - linear_team_key: ENG\n    github_repo: org/repo\n")
    cfg = Config.load(p)
    assert cfg.repos[0].runner == "local"
