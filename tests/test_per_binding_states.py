"""Per-binding workflow state overrides (PRD user story #2).

A team using ``Ready for AI`` must coexist with one using the default
``Todo`` without forcing a global rename. Each ``RepoBinding`` may declare
its own ``linear_states`` block; bindings that omit it inherit the global
``linear_states``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony.config import Config


def test_binding_overrides_global_states(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
linear_states:
  ready: Todo
  in_progress: In Progress
  needs_approval: Needs Approval
  blocked: Blocked
  done: Done
repos:
  - linear_team_key: ENG
    github_repo: org/api
  - linear_team_key: WEB
    github_repo: org/web
    linear_states:
      ready: Ready for AI
      in_progress: Implementing
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)

    eng, web = cfg.repos
    assert eng.effective_states(cfg.linear_states).ready == "Todo"
    assert eng.effective_states(cfg.linear_states).in_progress == "In Progress"

    assert web.effective_states(cfg.linear_states).ready == "Ready for AI"
    assert web.effective_states(cfg.linear_states).in_progress == "Implementing"


def test_binding_without_override_returns_global(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    p = tmp_path / "cfg.yaml"
    p.write_text("repos:\n  - linear_team_key: ENG\n    github_repo: org/api\n")
    cfg = Config.load(p)
    eff = cfg.repos[0].effective_states(cfg.linear_states)
    # Identity matters: the global object should be returned unchanged so a
    # caller comparing state references stays cheap.
    assert eff is cfg.linear_states
