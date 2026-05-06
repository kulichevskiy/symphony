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


def test_ensure_worktree_fetches_remote_branch_before_check(tmp_path):
    """Regression: a long-lived clone where the local remote-tracking ref
    for `auto/<n>` is stale (or absent) must still discover the remote
    branch — `ensure_worktree` fetches it before the existence check.

    Without the fetch, the next `git worktree add` would create the branch
    from `origin/<base>` and the subsequent push would be rejected as
    non-fast-forward.
    """
    repo, _bare = _init_origin_repo(tmp_path)
    wt_root = tmp_path / "wts"

    # Set up a remote `auto/77` with one commit and then erase ALL local
    # traces in `repo` — including the remote-tracking ref — to simulate a
    # clone that hasn't fetched since the remote branch was created.
    _run(["git", "checkout", "-b", "auto/77"], cwd=repo)
    (repo / "remote-only.txt").write_text("hi\n")
    _run(["git", "add", "remote-only.txt"], cwd=repo)
    _run(["git", "commit", "-m", "remote-only"], cwd=repo)
    _run(["git", "push", "-u", "origin", "auto/77"], cwd=repo)
    _run(["git", "checkout", "main"], cwd=repo)
    _run(["git", "branch", "-D", "auto/77"], cwd=repo)
    # Drop the remote-tracking ref too — this is the case Codex flagged.
    _run(["git", "update-ref", "-d", "refs/remotes/origin/auto/77"], cwd=repo)
    # Sanity: the local ref is gone, so without the fetch we'd miss it.
    rc = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", "refs/remotes/origin/auto/77"],
        cwd=repo,
        capture_output=True,
    ).returncode
    assert rc != 0

    wt = ensure_worktree(
        repo_path=repo,
        worktree_root=wt_root,
        repo_name="symphony",
        issue_number=77,
        base_branch="main",
        author_name="Symphony",
        author_email="sym@example.com",
    )
    # The fix fetched origin/auto/77 first, so the worktree HEAD is at the
    # remote tip — the prior commit is preserved.
    assert (wt / "remote-only.txt").exists()


def test_ensure_worktree_tracks_remote_branch_when_local_missing(tmp_path):
    """Regression: if `auto/<n>` is missing locally but exists on origin (e.g.
    after a reclone), `ensure_worktree` must create the local branch tracking
    the remote tip — not from `origin/<base>` — so the next push fast-forwards.
    """
    repo, bare = _init_origin_repo(tmp_path)
    wt_root = tmp_path / "wts"

    # Simulate a prior run by pushing an `auto/42` commit to origin without
    # leaving the local branch around (mimics a fresh clone).
    _run(["git", "checkout", "-b", "auto/42"], cwd=repo)
    (repo / "from-prior-run.txt").write_text("hi\n")
    _run(["git", "add", "from-prior-run.txt"], cwd=repo)
    _run(["git", "commit", "-m", "prior"], cwd=repo)
    _run(["git", "push", "-u", "origin", "auto/42"], cwd=repo)
    _run(["git", "checkout", "main"], cwd=repo)
    _run(["git", "branch", "-D", "auto/42"], cwd=repo)

    wt = ensure_worktree(
        repo_path=repo,
        worktree_root=wt_root,
        repo_name="symphony",
        issue_number=42,
        base_branch="main",
        author_name="Symphony",
        author_email="sym@example.com",
    )
    # Worktree HEAD must be at the remote `auto/42` tip — not at `origin/main` —
    # so the prior commit is preserved and the next push fast-forwards.
    assert (wt / "from-prior-run.txt").exists()
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=wt, check=True, capture_output=True, text=True
    ).stdout.strip()
    remote_head = subprocess.run(
        ["git", "rev-parse", "origin/auto/42"],
        cwd=wt,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert head == remote_head


def test_ensure_worktree_returns_to_branch_after_drift(tmp_path):
    """Regression: a reused worktree that drifted to a different branch
    (e.g. a prior run aborted mid-checkout, or a human peeked) must be
    switched back to `auto/<n>` before being returned. Otherwise the
    agent dispatches on the wrong branch and the subsequent push of
    `auto/<n>` silently drops the new commits from the PR.
    """
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
    # Drift: a human runs `git switch -c side` inside the worktree.
    _run(["git", "switch", "-c", "side"], cwd=wt)
    head_after_drift = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=wt,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert head_after_drift == "side"

    # Re-run: ensure_worktree must put HEAD back on auto/42.
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
    head_after_recover = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=wt2,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert head_after_recover == "auto/42"


def test_remote_branch_exists_returns_false_when_origin_deleted_branch(tmp_path):
    """Regression: a stale local remote-tracking ref must not be trusted when
    the remote branch has been deleted on origin (by another clone)."""
    from symphony.workspace import _remote_branch_exists

    repo, bare = _init_origin_repo(tmp_path)

    # `repo` creates and pushes auto/77, then drops the local branch but
    # keeps its remote-tracking ref. A *separate* clone deletes the branch
    # on origin so `repo`'s ref becomes stale (and pruning only happens if
    # `repo` does an explicit prune fetch).
    _run(["git", "checkout", "-b", "auto/77"], cwd=repo)
    (repo / "remote.txt").write_text("hi\n")
    _run(["git", "add", "remote.txt"], cwd=repo)
    _run(["git", "commit", "-m", "remote"], cwd=repo)
    _run(["git", "push", "-u", "origin", "auto/77"], cwd=repo)
    _run(["git", "checkout", "main"], cwd=repo)
    _run(["git", "branch", "-D", "auto/77"], cwd=repo)

    other = tmp_path / "other-clone"
    _run(["git", "clone", str(bare), str(other)], cwd=tmp_path)
    _run(["git", "push", "origin", "--delete", "auto/77"], cwd=other)

    # The local remote-tracking ref in `repo` is still there — `repo` has
    # not fetched since the deletion. The previous implementation would
    # rev-parse that stale ref and return True, resurrecting deleted commits.
    assert _branch_check_remote_tracking(repo, "auto/77")  # stale ref present

    assert _remote_branch_exists(repo, "auto/77") is False
    # Stale ref pruned as a side-effect.
    assert not _branch_check_remote_tracking(repo, "auto/77")


def test_remote_branch_exists_refreshes_force_pushed_branch(tmp_path):
    """Regression: force-pushed origin branches must refresh local tracking refs."""
    from symphony.workspace import _remote_branch_exists

    repo, bare = _init_origin_repo(tmp_path)

    _run(["git", "checkout", "-b", "auto/88"], cwd=repo)
    (repo / "old.txt").write_text("old\n")
    _run(["git", "add", "old.txt"], cwd=repo)
    _run(["git", "commit", "-m", "old"], cwd=repo)
    _run(["git", "push", "-u", "origin", "auto/88"], cwd=repo)
    old_sha = subprocess.run(
        ["git", "rev-parse", "origin/auto/88"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    _run(["git", "checkout", "main"], cwd=repo)
    _run(["git", "branch", "-D", "auto/88"], cwd=repo)

    other = tmp_path / "force-pusher"
    _run(["git", "clone", str(bare), str(other)], cwd=tmp_path)
    _run(["git", "config", "user.email", "test@example.com"], cwd=other)
    _run(["git", "config", "user.name", "Test"], cwd=other)
    _run(["git", "checkout", "-b", "auto/88", "origin/main"], cwd=other)
    (other / "new.txt").write_text("new\n")
    _run(["git", "add", "new.txt"], cwd=other)
    _run(["git", "commit", "-m", "new"], cwd=other)
    _run(["git", "push", "--force", "origin", "auto/88"], cwd=other)
    new_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=other,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert new_sha != old_sha

    assert _remote_branch_exists(repo, "auto/88") is True
    refreshed_sha = subprocess.run(
        ["git", "rev-parse", "origin/auto/88"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert refreshed_sha == new_sha


def test_remote_branch_exists_ignores_tail_matched_branch(tmp_path):
    """Regression: `auto/<n>` must not match `foo/auto/<n>` on origin."""
    from symphony.workspace import _remote_branch_exists

    repo, _bare = _init_origin_repo(tmp_path)
    _run(["git", "push", "origin", "main:refs/heads/foo/auto/99"], cwd=repo)

    assert _remote_branch_exists(repo, "auto/99") is False
    assert not _branch_check_remote_tracking(repo, "auto/99")


def _branch_check_remote_tracking(repo: Path, branch: str) -> bool:
    return subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"refs/remotes/origin/{branch}"],
        cwd=repo,
        capture_output=True,
    ).returncode == 0


def test_ensure_worktree_does_not_rewrite_checked_out_branch(tmp_path):
    """Regression: update-ref must not move a branch checked out in repo_path."""
    repo, bare = _init_origin_repo(tmp_path)
    wt_root = tmp_path / "wts"

    _run(["git", "checkout", "-b", "auto/55"], cwd=repo)
    (repo / "first.txt").write_text("first\n")
    _run(["git", "add", "first.txt"], cwd=repo)
    _run(["git", "commit", "-m", "first"], cwd=repo)
    _run(["git", "push", "-u", "origin", "auto/55"], cwd=repo)
    local_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    other = tmp_path / "other"
    _run(["git", "clone", str(bare), str(other)], cwd=tmp_path)
    _run(["git", "config", "user.email", "test@example.com"], cwd=other)
    _run(["git", "config", "user.name", "Test"], cwd=other)
    _run(["git", "checkout", "auto/55"], cwd=other)
    (other / "second.txt").write_text("second\n")
    _run(["git", "add", "second.txt"], cwd=other)
    _run(["git", "commit", "-m", "second"], cwd=other)
    _run(["git", "push", "origin", "auto/55"], cwd=other)

    with pytest.raises(WorkspaceError, match="git worktree add failed"):
        ensure_worktree(
            repo_path=repo,
            worktree_root=wt_root,
            repo_name="symphony",
            issue_number=55,
            base_branch="main",
            author_name="Symphony",
            author_email="sym@example.com",
        )

    head_after = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert head_after == local_sha
    status = subprocess.run(
        ["git", "status", "--short"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert status == ""


def test_ensure_worktree_fast_forwards_local_to_remote_tip(tmp_path):
    """Regression: when local `auto/<n>` is behind `origin/auto/<n>` (another
    clone advanced the remote since we last fetched), `ensure_worktree` must
    fast-forward the local branch first. Otherwise the agent runs on stale
    history and the next push is rejected as non-fast-forward.
    """
    repo, bare = _init_origin_repo(tmp_path)
    wt_root = tmp_path / "wts"

    # Create local + remote auto/55 at SHA1.
    _run(["git", "checkout", "-b", "auto/55"], cwd=repo)
    (repo / "first.txt").write_text("first\n")
    _run(["git", "add", "first.txt"], cwd=repo)
    _run(["git", "commit", "-m", "first"], cwd=repo)
    _run(["git", "push", "-u", "origin", "auto/55"], cwd=repo)
    sha1 = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()
    _run(["git", "checkout", "main"], cwd=repo)

    # Simulate another clone advancing origin/auto/55 (SHA2). We push from a
    # second clone of the same bare repo so origin's tip moves without the
    # original `repo`'s local `auto/55` ref moving.
    other = tmp_path / "other"
    other.mkdir()
    _run(["git", "clone", str(bare), str(other)], cwd=tmp_path)
    _run(["git", "config", "user.email", "test@example.com"], cwd=other)
    _run(["git", "config", "user.name", "Test"], cwd=other)
    _run(["git", "checkout", "auto/55"], cwd=other)
    (other / "second.txt").write_text("second\n")
    _run(["git", "add", "second.txt"], cwd=other)
    _run(["git", "commit", "-m", "second"], cwd=other)
    _run(["git", "push", "origin", "auto/55"], cwd=other)
    sha2 = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=other, check=True, capture_output=True, text=True
    ).stdout.strip()
    assert sha2 != sha1

    # Local `repo`'s `auto/55` is still at SHA1; origin is now SHA2. ensure_worktree
    # must fast-forward `auto/55` to SHA2 before adding the worktree.
    wt = ensure_worktree(
        repo_path=repo,
        worktree_root=wt_root,
        repo_name="symphony",
        issue_number=55,
        base_branch="main",
        author_name="Symphony",
        author_email="sym@example.com",
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=wt,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert head == sha2
    assert (wt / "second.txt").exists()


def test_ensure_worktree_recovers_from_pruned_directory(tmp_path):
    """Regression: a worktree dir that was rm'd but is still registered in
    git metadata must be auto-pruned so the next `worktree add` succeeds."""
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
    assert wt.is_dir()

    # Simulate someone (or a crash) removing the worktree directory without
    # calling `git worktree remove`. The metadata under .git/worktrees/ stays.
    import shutil

    shutil.rmtree(wt)
    assert not wt.exists()

    # Re-running ensure_worktree must auto-prune and recreate at the same path.
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
    assert wt2.is_dir()
    head = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=wt2,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert head == "auto/42"


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
