from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from symphony import preflight as preflight_mod
from symphony.preflight import run_preflight


def _cfg(tmp_path: Path):
    worktree_root = tmp_path / "worktrees"
    worktree_root.mkdir()
    return SimpleNamespace(
        repo=SimpleNamespace(path=tmp_path, default_branch="main"),
        paths=SimpleNamespace(worktree_root=worktree_root),
    )


def test_preflight_collects_successes(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(preflight_mod, "name_with_owner", lambda p: ("owner", "repo"))

    commands: list[list[str]] = []

    def command_runner(args, cwd):
        commands.append(args)
        return True, "ok"

    def gh_runner(args, cwd):
        if "protection" in args[1]:
            return json.dumps(
                {
                    "required_status_checks": {"checks": [{"context": "ci"}]},
                    "required_pull_request_reviews": {
                        "required_approving_review_count": 1
                    },
                }
            )
        if args[1] == "/repos/owner/repo/installation":
            return "{}"
        if "labels" in args[1]:
            return json.dumps(
                [[{"name": "auto"}, {"name": "auto-stuck"}], [{"name": "auto-cycle"}, {"name": "auto-canceled"}]]
            )
        raise AssertionError(args)

    results = run_preflight(cfg, command_runner=command_runner, gh_runner=gh_runner)

    assert all(result.ok for result in results)
    assert ["gh", "auth", "status"] in commands
    assert ["claude", "--version"] in commands
    assert ["claude", "-p", "ok", "--max-turns", "1"] in commands


def test_preflight_reports_actionable_failures(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(preflight_mod, "name_with_owner", lambda p: ("owner", "repo"))

    def command_runner(args, cwd):
        if args[:2] == ["gh", "auth"]:
            return False, "not logged in"
        return True, "ok"

    def gh_runner(args, cwd):
        if "protection" in args[1]:
            return json.dumps(
                {
                    "required_status_checks": {"checks": []},
                    "required_pull_request_reviews": {
                        "required_approving_review_count": 0
                    },
                }
            )
        if args[1] == "/repos/owner/repo/installation":
            return "{}"
        if "labels" in args[1]:
            return json.dumps([[{"name": "auto"}]])
        raise AssertionError(args)

    results = run_preflight(cfg, command_runner=command_runner, gh_runner=gh_runner)
    by_name = {result.name: result for result in results}

    assert not by_name["gh auth"].ok
    assert "not logged in" in by_name["gh auth"].message
    assert not by_name["branch protection"].ok
    assert "required CI/status check" in by_name["branch protection"].message
    assert "required approving review" in by_name["branch protection"].message
    assert not by_name["labels"].ok
    assert "auto-stuck" in by_name["labels"].message
