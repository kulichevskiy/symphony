"""Regression tests for SYM-154 / SYM-148.

``_git_add_and_continue_rebase`` must treat an already-resolved rebase (no
rebase in progress + clean tree) as a benign success instead of hard-failing,
while a genuine unresolved-conflict state still reports failure.
"""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path

from symphony.orchestrator.poll._git import _git_add_and_continue_rebase


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
    """No rebase in progress + clean tree → benign success (no-op)."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    (repo / "f.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")

    # No rebase in progress; tree is clean. `git rebase --continue` will exit
    # non-zero ("No rebase in progress?"), but the desired end state is reached.
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


def test_skip_completed_rebase_returns_failure(tmp_path: Path) -> None:
    """--skip completing a rebase must be rejected, not treated as benign."""
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
    subprocess.run(
        ["git", "rebase", "main"],
        cwd=str(repo),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Skip drops the conflicting commit and completes the rebase.
    subprocess.run(
        ["git", "rebase", "--skip"],
        cwd=str(repo),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # No rebase in progress, no conflicted files, but a commit was silently dropped.
    # ORIG_HEAD exists (set when rebase started), so we must not treat this as benign.
    result = asyncio.run(_git_add_and_continue_rebase(repo, []))
    assert result is False
