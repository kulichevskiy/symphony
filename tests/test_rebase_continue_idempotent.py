"""Regression tests for SYM-154 / SYM-148.

``_git_add_and_continue_rebase`` must treat an already-resolved rebase (no
rebase in progress + clean tree) as a benign success instead of hard-failing,
while a genuine unresolved-conflict state still reports failure.
"""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path

from symphony.orchestrator.poll._git import (
    _git_add_and_continue_rebase,
    _git_tree_is_clean,
)


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _init_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-q", "-b", "main")
    _git(path, "config", "user.email", "test@example.com")
    _git(path, "config", "user.name", "Test")


def test_already_resolved_returns_success(tmp_path: Path) -> None:
    """No rebase in progress + clean tree → benign success (no-op).

    This covers the concurrent-completion case (SYM-148): a previous run may
    have completed the rebase via ``--continue``, leaving ORIG_HEAD set but no
    rebase state. We must return True regardless of ORIG_HEAD.
    """
    repo = tmp_path / "repo"
    _init_repo(repo)
    (repo / "f.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")

    # No rebase in progress; tree is clean. `git rebase --continue` will exit
    # non-zero ("No rebase in progress?"), but the desired end state is reached.
    result = asyncio.run(_git_add_and_continue_rebase(repo, []))
    assert result is True


def test_already_resolved_with_orig_head_returns_success(tmp_path: Path) -> None:
    """Completed rebase (ORIG_HEAD set) + clean tree → benign success.

    When a concurrent ``git rebase --continue`` already finished, ORIG_HEAD
    still resolves but no rebase is in progress. We must not reject this state.
    """
    repo = tmp_path / "repo"
    _init_repo(repo)
    (repo / "f.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")

    _git(repo, "checkout", "-q", "-b", "feature")
    (repo / "feature.txt").write_text("feature\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "feature change")

    _git(repo, "checkout", "-q", "main")
    (repo / "main.txt").write_text("main\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "main change")

    _git(repo, "checkout", "-q", "feature")
    # Rebase succeeds cleanly (no conflict), setting ORIG_HEAD but leaving no rebase state.
    _git(repo, "rebase", "main")

    # ORIG_HEAD is now set; call again as a no-op (simulating a concurrent run).
    result = asyncio.run(_git_add_and_continue_rebase(repo, []))
    assert result is True


def test_real_unresolved_conflict_returns_failure(tmp_path: Path) -> None:
    """Genuine unresolved-conflict mid-rebase → failure."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    (repo / "f.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")

    _git(repo, "checkout", "-q", "-b", "feature")
    (repo / "f.txt").write_text("feature\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "feature change")

    _git(repo, "checkout", "-q", "main")
    (repo / "f.txt").write_text("main\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "main change")

    _git(repo, "checkout", "-q", "feature")
    # Conflicting rebase; leave the conflict unresolved.
    subprocess.run(
        ["git", "rebase", "main"],
        cwd=str(repo),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Continue without resolving the conflict → still failing.
    result = asyncio.run(_git_add_and_continue_rebase(repo, []))
    assert result is False


def test_tree_is_clean_false_when_untracked_files_present(tmp_path: Path) -> None:
    """Untracked files make the working tree not clean."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    (repo / "f.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")

    (repo / "untracked.txt").write_text("not tracked\n")
    result = asyncio.run(_git_tree_is_clean(repo))
    assert result is False


def test_tree_is_clean_true_when_no_untracked_files(tmp_path: Path) -> None:
    """Clean committed repo with no untracked files is clean."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    (repo / "f.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")

    result = asyncio.run(_git_tree_is_clean(repo))
    assert result is True


def test_already_resolved_with_stale_files_returns_success(tmp_path: Path) -> None:
    """No rebase in progress + clean tree → success even when files list is non-empty.

    Covers the pre-staging short-circuit: if a concurrent run already finished
    the rebase, we must return True *before* calling ``git add`` on stale paths.
    """
    repo = tmp_path / "repo"
    _init_repo(repo)
    (repo / "f.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")

    # Tree is clean, no rebase in progress. Pass a file that exists but needs
    # no action — simulates the caller passing stale conflict paths after a
    # concurrent run already resolved them.
    result = asyncio.run(_git_add_and_continue_rebase(repo, ["f.txt"]))
    assert result is True


