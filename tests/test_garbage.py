from __future__ import annotations

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from symphony import garbage as garbage_mod
from symphony.garbage import (
    REASON_AUTO_STUCK,
    REASON_CLOSED_NO_PR,
    REASON_CLOSED_PR_CLOSED,
    REASON_CLOSED_PR_MERGED,
    GcCandidate,
    find_gc_candidates,
    remove_worktree,
    run_startup_gc,
)
from symphony.github import GithubError, PR


def _cfg(tmp_path: Path):
    return SimpleNamespace(
        repo=SimpleNamespace(path=tmp_path / "repo", default_branch="main"),
        paths=SimpleNamespace(worktree_root=tmp_path / "wts"),
    )


def _make_wt(cfg, issue_number: int, *, age_days: float = 0, now: float = 1_000_000.0) -> Path:
    cfg.paths.worktree_root.mkdir(parents=True, exist_ok=True)
    wt = cfg.paths.worktree_root / f"symphony-{issue_number}"
    nested = wt / "src"
    nested.mkdir(parents=True)
    f = nested / "f.txt"
    f.write_text("x\n")
    ts = now - (age_days * 24 * 60 * 60)
    for path in (f, nested, wt):
        os.utime(path, (ts, ts))
    return wt


def _patch_repo_resolution(monkeypatch):
    monkeypatch.setattr(garbage_mod, "name_with_owner", lambda p: ("owner", "symphony"))


# ---- find_gc_candidates: original auto-stuck path ----


def test_find_gc_candidates_uses_nested_activity_mtime(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    cfg.paths.worktree_root.mkdir()
    wt = cfg.paths.worktree_root / "symphony-42"
    nested = wt / "src"
    nested.mkdir(parents=True)
    active_file = nested / "active.txt"
    active_file.write_text("recent\n")
    now = 1_000_000.0
    old = now - (20 * 24 * 60 * 60)
    os.utime(wt, (old, old))
    os.utime(nested, (old, old))
    os.utime(active_file, (now, now))

    _patch_repo_resolution(monkeypatch)

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: {42},
        issue_state_fn=lambda n: "OPEN",
        pr_for_branch_fn=lambda b: None,
    )

    assert candidates == []


def test_find_gc_candidates_lists_old_inactive_worktree(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 42, age_days=20, now=now)

    _patch_repo_resolution(monkeypatch)

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: {42},
        issue_state_fn=lambda n: "OPEN",
        pr_for_branch_fn=lambda b: None,
    )

    assert [c.issue_number for c in candidates] == [42]
    assert candidates[0].age_days == 20
    assert candidates[0].reason == REASON_AUTO_STUCK


# ---- find_gc_candidates: closed-orphan paths ----


def test_find_gc_candidates_picks_up_closed_no_pr(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 42, age_days=2, now=now)  # young — but closed-orphan ignores age
    _patch_repo_resolution(monkeypatch)

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: set(),
        issue_state_fn=lambda n: "CLOSED",
        pr_for_branch_fn=lambda branch: None,
    )

    assert [(c.issue_number, c.reason) for c in candidates] == [
        (42, REASON_CLOSED_NO_PR)
    ]


def test_find_gc_candidates_picks_up_closed_merged_pr(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 64, age_days=1, now=now)
    _patch_repo_resolution(monkeypatch)

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: set(),
        issue_state_fn=lambda n: "CLOSED",
        pr_for_branch_fn=lambda b: (PR(number=99, url="u"), "MERGED"),
    )
    assert [(c.issue_number, c.reason) for c in candidates] == [
        (64, REASON_CLOSED_PR_MERGED)
    ]


def test_find_gc_candidates_picks_up_closed_pr_closed(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 7, age_days=0, now=now)
    _patch_repo_resolution(monkeypatch)

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: set(),
        issue_state_fn=lambda n: "CLOSED",
        pr_for_branch_fn=lambda b: (PR(number=99, url="u"), "CLOSED"),
    )
    assert [(c.issue_number, c.reason) for c in candidates] == [
        (7, REASON_CLOSED_PR_CLOSED)
    ]


def test_find_gc_candidates_skips_closed_with_open_pr(monkeypatch, tmp_path):
    """A closed issue whose PR is still OPEN must not be GC'd — concurrent
    PR review may still be running."""
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 5, age_days=1, now=now)
    _patch_repo_resolution(monkeypatch)

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: set(),
        issue_state_fn=lambda n: "CLOSED",
        pr_for_branch_fn=lambda b: (PR(number=99, url="u"), "OPEN"),
    )
    assert candidates == []


def test_find_gc_candidates_skips_open_non_stuck(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 5, age_days=1, now=now)
    _patch_repo_resolution(monkeypatch)

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: set(),
        issue_state_fn=lambda n: "OPEN",
        pr_for_branch_fn=lambda b: None,
    )
    assert candidates == []


def test_find_gc_candidates_skips_foreign_dirs(monkeypatch, tmp_path):
    """Directories not matching ``<repo>-<n>`` are ignored — other tools
    share the worktree root in practice."""
    cfg = _cfg(tmp_path)
    cfg.paths.worktree_root.mkdir(parents=True)
    foreign = cfg.paths.worktree_root / "some-other-tool"
    foreign.mkdir()
    not_numeric = cfg.paths.worktree_root / "symphony-foo"
    not_numeric.mkdir()
    _patch_repo_resolution(monkeypatch)

    candidates = find_gc_candidates(
        cfg,
        days=14,
        stuck_issues_fn=lambda: set(),
        issue_state_fn=lambda n: pytest.fail("should not be called"),
        pr_for_branch_fn=lambda b: pytest.fail("should not be called"),
    )
    assert candidates == []


def test_find_gc_candidates_swallows_github_error(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 9, age_days=1, now=now)
    _patch_repo_resolution(monkeypatch)

    def _boom(_n: int) -> str:
        raise GithubError("network down")

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: set(),
        issue_state_fn=_boom,
        pr_for_branch_fn=lambda b: None,
    )
    assert candidates == []


# ---- run_startup_gc ----


def test_run_startup_gc_removes_three_orphan_shapes(monkeypatch, tmp_path):
    """The exact scenario described in #20: closed issue + no PR, closed +
    merged PR, closed + no PR (empty diff) all get cleaned up at boot."""
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    paths = {
        42: _make_wt(cfg, 42, age_days=1, now=now),
        63: _make_wt(cfg, 63, age_days=1, now=now),
        64: _make_wt(cfg, 64, age_days=1, now=now),
    }
    _patch_repo_resolution(monkeypatch)

    pr_states = {
        "auto/42": None,                                # closed, no PR
        "auto/63": None,                                # closed, empty-diff (no PR)
        "auto/64": (PR(number=100, url="u"), "MERGED"),  # closed, merged
    }

    removed_paths: list[Path] = []
    removed_branches: list[str] = []

    def _fake_remove(repo_path, *, branch, path):
        removed_paths.append(path)
        removed_branches.append(branch)

    monkeypatch.setattr(garbage_mod, "remove_worktree", _fake_remove)

    events: list[tuple[str, int, dict]] = []

    class _EvLog:
        def emit(self, kind, *, issue_number, payload):
            events.append((kind, issue_number, payload))

    removed = run_startup_gc(
        cfg,
        issue_state_fn=lambda n: "CLOSED",
        pr_for_branch_fn=lambda b: pr_states[b],
        event_log=_EvLog(),
    )

    assert sorted(c.issue_number for c in removed) == [42, 63, 64]
    assert sorted(removed_branches) == ["auto/42", "auto/63", "auto/64"]
    assert sorted(removed_paths) == sorted(paths.values())
    assert {e[0] for e in events} == {"startup-gc"}
    reasons = {e[1]: e[2]["reason"] for e in events}
    assert reasons == {
        42: REASON_CLOSED_NO_PR,
        63: REASON_CLOSED_NO_PR,
        64: REASON_CLOSED_PR_MERGED,
    }


def test_run_startup_gc_skips_open_issues(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 5, age_days=0, now=now)
    _patch_repo_resolution(monkeypatch)

    monkeypatch.setattr(
        garbage_mod,
        "remove_worktree",
        lambda *a, **k: pytest.fail("must not remove open-issue worktree"),
    )

    removed = run_startup_gc(
        cfg,
        issue_state_fn=lambda n: "OPEN",
        pr_for_branch_fn=lambda b: None,
    )
    assert removed == []


def test_run_startup_gc_continues_past_github_error(monkeypatch, tmp_path):
    """If a single issue's state fetch fails, others must still be processed."""
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 1, age_days=0, now=now)
    _make_wt(cfg, 2, age_days=0, now=now)
    _patch_repo_resolution(monkeypatch)

    def _state(n: int) -> str:
        if n == 1:
            raise GithubError("flaky")
        return "CLOSED"

    removed_branches: list[str] = []

    def _fake_remove(repo_path, *, branch, path):
        removed_branches.append(branch)

    monkeypatch.setattr(garbage_mod, "remove_worktree", _fake_remove)

    removed = run_startup_gc(
        cfg,
        issue_state_fn=_state,
        pr_for_branch_fn=lambda b: None,
    )
    assert [c.issue_number for c in removed] == [2]
    assert removed_branches == ["auto/2"]


def test_run_startup_gc_continues_past_remove_failure(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    _make_wt(cfg, 1, age_days=0, now=now)
    _make_wt(cfg, 2, age_days=0, now=now)
    _patch_repo_resolution(monkeypatch)

    def _fake_remove(repo_path, *, branch, path):
        if branch == "auto/1":
            raise subprocess.CalledProcessError(
                returncode=128, cmd=["git", "worktree", "remove"], stderr="locked"
            )

    monkeypatch.setattr(garbage_mod, "remove_worktree", _fake_remove)

    removed = run_startup_gc(
        cfg,
        issue_state_fn=lambda n: "CLOSED",
        pr_for_branch_fn=lambda b: None,
    )
    assert [c.issue_number for c in removed] == [2]


def test_run_startup_gc_returns_empty_when_no_root(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)  # worktree_root deliberately not created
    _patch_repo_resolution(monkeypatch)
    assert run_startup_gc(cfg) == []


def test_run_startup_gc_skips_active_paths(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    now = 1_000_000.0
    active_wt = _make_wt(cfg, 1, age_days=0, now=now)
    _make_wt(cfg, 2, age_days=0, now=now)
    _patch_repo_resolution(monkeypatch)

    monkeypatch.setattr(
        garbage_mod, "remove_worktree", lambda repo_path, *, branch, path: None
    )

    removed = run_startup_gc(
        cfg,
        issue_state_fn=lambda n: "CLOSED",
        pr_for_branch_fn=lambda b: None,
        active_paths={active_wt},
    )
    assert [c.issue_number for c in removed] == [2]


# ---- remove_worktree primitive ----


def test_remove_worktree_calls_git_in_order(monkeypatch, tmp_path):
    calls: list[list[str]] = []

    def _fake_run(args, **kwargs):
        calls.append(list(args))
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(garbage_mod.subprocess, "run", _fake_run)
    remove_worktree(tmp_path, branch="auto/42", path=tmp_path / "wt")
    assert calls[0][:3] == ["git", "worktree", "remove"]
    assert calls[1][:3] == ["git", "branch", "-D"]
    assert calls[1][-1] == "auto/42"


def test_remove_worktree_tolerates_missing_branch(monkeypatch, tmp_path):
    def _fake_run(args, **kwargs):
        if args[1] == "branch":
            return subprocess.CompletedProcess(
                args=args, returncode=1, stdout="", stderr="error: branch not found."
            )
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(garbage_mod.subprocess, "run", _fake_run)
    # Should not raise
    remove_worktree(tmp_path, branch="auto/42", path=tmp_path / "wt")


def test_remove_worktree_propagates_real_branch_error(monkeypatch, tmp_path):
    def _fake_run(args, **kwargs):
        if args[1] == "branch":
            return subprocess.CompletedProcess(
                args=args, returncode=128, stdout="", stderr="something else"
            )
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(garbage_mod.subprocess, "run", _fake_run)
    with pytest.raises(subprocess.CalledProcessError):
        remove_worktree(tmp_path, branch="auto/42", path=tmp_path / "wt")
