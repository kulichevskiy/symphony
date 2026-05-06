"""Tests for symphony.workspace.

Most tests use a real git repository in tmp_path because the surface we're
exercising is the `git worktree` / `git config` interaction itself — mocking it
would re-implement git.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from symphony.workspace import (
    WorkspaceError,
    ensure_worktree,
    sanitize_repo_name,
    worktree_path,
)


def _run(args: list[str], *, cwd: Path) -> None:
    subprocess.run(args, cwd=cwd, check=True, capture_output=True)


def _init_repo(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    _run(["git", "init", "-b", "main"], cwd=path)
    _run(["git", "config", "user.email", "test@example.com"], cwd=path)
    _run(["git", "config", "user.name", "Test"], cwd=path)
    (path / "README.md").write_text("hello\n")
    _run(["git", "add", "."], cwd=path)
    _run(["git", "commit", "-m", "init"], cwd=path)
    return path


def _init_origin_repo(tmp_path: Path) -> tuple[Path, Path]:
    """Create a bare 'origin' repo and a clone with origin/main set up."""
    bare = tmp_path / "origin.git"
    _run(["git", "init", "--bare", "-b", "main", str(bare)], cwd=tmp_path)
    repo = _init_repo(tmp_path / "repo")
    _run(["git", "remote", "add", "origin", str(bare)], cwd=repo)
    _run(["git", "push", "-u", "origin", "main"], cwd=repo)
    return repo, bare


# ---- sanitize_repo_name ----


def test_sanitize_repo_name_alphanumeric_unchanged():
    assert sanitize_repo_name("symphony") == "symphony"
    assert sanitize_repo_name("Repo-1.0_x") == "Repo-1.0_x"


def test_sanitize_repo_name_replaces_invalid_chars():
    assert sanitize_repo_name("my repo") == "my_repo"
    assert sanitize_repo_name("foo/bar") == "foo_bar"
    assert sanitize_repo_name("a@b#c") == "a_b_c"


def test_sanitize_repo_name_unicode_replaced():
    assert sanitize_repo_name("café") == "caf_"


def test_sanitize_repo_name_empty_raises():
    with pytest.raises(WorkspaceError):
        sanitize_repo_name("")


# ---- worktree_path ----


def test_worktree_path_joins_sanitized_name(tmp_path):
    p = worktree_path(tmp_path, "my repo", 7)
    assert p == tmp_path / "my_repo-7"


def test_worktree_path_stays_inside_root_for_dot_dot_repo(tmp_path):
    # Sanitization keeps `..` intact (it matches [A-Za-z0-9._-]) but appending
    # `-<n>` ensures the final path component cannot be `..` or `.` literally.
    p = worktree_path(tmp_path, "..", 1)
    assert p == tmp_path / "..-1"
    assert p.resolve().parent == tmp_path.resolve()


# ---- ensure_worktree ----


def test_ensure_worktree_creates_branch_and_worktree(tmp_path):
    repo, _bare = _init_origin_repo(tmp_path)
    wt_root = tmp_path / "wts"
    wt = ensure_worktree(
        repo_path=repo,
        worktree_root=wt_root,
        repo_name="symphony",
        issue_number=42,
        base_branch="main",
        author_name="Symphony",
        author_email="sym@example.com",
    )
    assert wt == wt_root / "symphony-42"
    assert wt.is_dir()

    # Branch is checked out
    head = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=wt,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert head == "auto/42"

    # Bot identity is set per-worktree (local config), not inherited from global
    name = subprocess.run(
        ["git", "config", "--local", "user.name"],
        cwd=wt,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    email = subprocess.run(
        ["git", "config", "--local", "user.email"],
        cwd=wt,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert name == "Symphony"
    assert email == "sym@example.com"


def test_ensure_worktree_reuses_existing_with_history(tmp_path):
    repo, _bare = _init_origin_repo(tmp_path)
    wt_root = tmp_path / "wts"
    wt = ensure_worktree(
        repo_path=repo,
        worktree_root=wt_root,
        repo_name="symphony",
        issue_number=42,
        base_branch="main",
        author_name="Symphony",
        author_email="sym@example.com",
    )
    # Add a commit in the worktree
    (wt / "new.txt").write_text("hi\n")
    _run(["git", "add", "new.txt"], cwd=wt)
    _run(["git", "commit", "-m", "extra"], cwd=wt)
    sha_before = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=wt, check=True, capture_output=True, text=True
    ).stdout.strip()

    # Re-run: should reuse, history intact
    wt2 = ensure_worktree(
        repo_path=repo,
        worktree_root=wt_root,
        repo_name="symphony",
        issue_number=42,
        base_branch="main",
        author_name="Symphony",
        author_email="sym@example.com",
    )
    assert wt2 == wt
    sha_after = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=wt2,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert sha_after == sha_before
    assert (wt2 / "new.txt").exists()


def test_ensure_worktree_sanitizes_repo_name(tmp_path):
    repo, _bare = _init_origin_repo(tmp_path)
    wt_root = tmp_path / "wts"
    wt = ensure_worktree(
        repo_path=repo,
        worktree_root=wt_root,
        repo_name="my repo/with-slash",
        issue_number=1,
        base_branch="main",
        author_name="X",
        author_email="x@y",
    )
    assert wt.name == "my_repo_with-slash-1"


def test_ensure_worktree_empty_repo_name_rejected(tmp_path):
    repo, _bare = _init_origin_repo(tmp_path)
    wt_root = tmp_path / "wts"
    with pytest.raises(WorkspaceError):
        ensure_worktree(
            repo_path=repo,
            worktree_root=wt_root,
            repo_name="",
            issue_number=1,
            base_branch="main",
            author_name="X",
            author_email="x@y",
        )
