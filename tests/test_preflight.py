from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from symphony import preflight as preflight_mod
from symphony.github import GithubError
from symphony.preflight import format_preflight_results, preflight_ok, run_preflight


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
                    "required_pull_request_reviews": None,
                }
            )
        if args[1] == "/repos/owner/repo/installation":
            return json.dumps({"app_slug": "chatgpt-codex-connector"})
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


def test_codex_app_installation_check_warns_when_unverifiable(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(preflight_mod, "name_with_owner", lambda p: ("owner", "repo"))

    def command_runner(args, cwd):
        return True, "ok"

    def gh_runner(args, cwd):
        if "protection" in args[1]:
            return json.dumps(
                {"required_status_checks": {"checks": [{"context": "ci"}]}}
            )
        if args[1] == "/repos/owner/repo/installation":
            raise GithubError("HTTP 401: A JSON web token could not be decoded")
        if "labels" in args[1]:
            return json.dumps(
                [
                    [
                        {"name": "auto"},
                        {"name": "auto-stuck"},
                        {"name": "auto-cycle"},
                        {"name": "auto-canceled"},
                    ]
                ]
            )
        raise AssertionError(args)

    results = run_preflight(cfg, command_runner=command_runner, gh_runner=gh_runner)
    by_name = {result.name: result for result in results}

    assert not by_name["Codex GitHub App"].ok
    assert not by_name["Codex GitHub App"].fatal
    assert preflight_ok(results)
    assert "WARN Codex GitHub App" in format_preflight_results(results)


def test_branch_protection_unreachable_is_warning(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(preflight_mod, "name_with_owner", lambda p: ("owner", "repo"))

    def command_runner(args, cwd):
        return True, "ok"

    def gh_runner(args, cwd):
        if "protection" in args[1]:
            raise GithubError("HTTP 404: Branch not protected")
        if args[1] == "/repos/owner/repo/installation":
            return json.dumps({"app_slug": "chatgpt-codex-connector"})
        if "labels" in args[1]:
            return json.dumps(
                [
                    [
                        {"name": "auto"},
                        {"name": "auto-stuck"},
                        {"name": "auto-cycle"},
                        {"name": "auto-canceled"},
                    ]
                ]
            )
        raise AssertionError(args)

    results = run_preflight(cfg, command_runner=command_runner, gh_runner=gh_runner)
    by_name = {result.name: result for result in results}

    assert not by_name["branch protection"].ok
    assert not by_name["branch protection"].fatal
    assert preflight_ok(results)
    assert "WARN branch protection" in format_preflight_results(results)


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
                    "required_pull_request_reviews": None,
                }
            )
        if args[1] == "/repos/owner/repo/installation":
            return json.dumps({"app_slug": "chatgpt-codex-connector"})
        if "labels" in args[1]:
            return json.dumps([[{"name": "auto"}]])
        raise AssertionError(args)

    results = run_preflight(cfg, command_runner=command_runner, gh_runner=gh_runner)
    by_name = {result.name: result for result in results}

    assert not by_name["gh auth"].ok
    assert "not logged in" in by_name["gh auth"].message
    assert not by_name["branch protection"].ok
    assert not by_name["branch protection"].fatal
    assert "required CI/status check" in by_name["branch protection"].message
    assert not by_name["labels"].ok
    assert "auto-stuck" in by_name["labels"].message
