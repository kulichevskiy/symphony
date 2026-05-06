"""Git worktree creation and reuse for per-issue agent runs.

The worktree key is the *sanitized* repo name plus the issue number:
``<worktree_root>/<sanitized-repo>-<n>``. The branch is always ``auto/<n>``
and is created from ``origin/<default_branch>`` when missing. Repeated calls
with the same ``(repo, issue_number)`` reuse the existing branch and worktree
verbatim — no history is dropped.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_VALID_CHAR = re.compile(r"[A-Za-z0-9._\-]")


class WorkspaceError(Exception):
    """Raised when a worktree cannot be created or violates a safety invariant."""


def sanitize_repo_name(name: str) -> str:
    """Map a repo name to a filesystem-safe key.

    Allowed characters are ``[A-Za-z0-9._-]`` per SYMPHONY.md; anything else
    becomes ``_``. Empty input is rejected.
    """
    if not name:
        raise WorkspaceError("Repo name must not be empty")
    return "".join(c if _VALID_CHAR.match(c) else "_" for c in name)


def worktree_path(worktree_root: Path, repo_name: str, issue_number: int) -> Path:
    """Compute the worktree path and assert it stays inside ``worktree_root``."""
    sanitized = sanitize_repo_name(repo_name)
    target = worktree_root / f"{sanitized}-{issue_number}"
    root_resolved = worktree_root.resolve()
    target_resolved = target.resolve()
    try:
        target_resolved.relative_to(root_resolved)
    except ValueError as e:
        raise WorkspaceError(
            f"Worktree path {target_resolved} escapes worktree_root {root_resolved}"
        ) from e
    return target


def _run_git(args: list[str], *, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    )


def _branch_exists(repo_path: Path, branch: str) -> bool:
    res = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    return res.returncode == 0


def _remote_branch_exists(repo_path: Path, branch: str) -> bool:
    """Authoritatively check whether ``origin`` has ``branch`` and refresh the
    local remote-tracking ref so callers see the latest tip.

    The previous implementation swallowed errors from ``git fetch`` and then
    trusted any pre-existing local remote-tracking ref. In a long-lived
    clone whose remote branch has since been deleted, that stale local ref
    would make the caller resurrect old commits. We now ask origin itself
    (``git ls-remote``) and act on its answer:

    - branch present → fetch (so ``origin/<branch>`` is up-to-date) and return True
    - branch absent  → prune any stale local ref and return False
    - any other error (network, auth) → raise :class:`WorkspaceError` loudly
      rather than fall through to a wrong "false" answer.
    """
    ls = subprocess.run(
        ["git", "ls-remote", "--exit-code", "--heads", "origin", branch],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    if ls.returncode == 0:
        fetch = subprocess.run(
            ["git", "fetch", "origin", f"+{branch}:refs/remotes/origin/{branch}"],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        if fetch.returncode != 0:
            raise WorkspaceError(
                f"git fetch origin {branch} failed: {fetch.stderr.strip() or fetch.stdout.strip()}"
            )
        return True
    if ls.returncode == 2:
        # ls-remote signals "ref not found" with exit code 2. Drop any stale
        # remote-tracking ref so a later rev-parse can't return success on
        # commits that no longer exist on origin.
        subprocess.run(
            ["git", "update-ref", "-d", f"refs/remotes/origin/{branch}"],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        return False
    raise WorkspaceError(
        f"git ls-remote origin {branch} failed (exit {ls.returncode}): "
        f"{ls.stderr.strip() or ls.stdout.strip()}"
    )


def _is_ancestor(repo_path: Path, ancestor: str, descendant: str) -> bool:
    """Return True iff ``ancestor`` is reachable from ``descendant``.

    A commit is its own ancestor, so this also handles the equal-SHAs case
    (no fast-forward needed, but the caller's update-ref is a harmless
    no-op anyway).
    """
    res = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    return res.returncode == 0


def _worktree_exists(repo_path: Path, target: Path) -> bool:
    res = _run_git(["worktree", "list", "--porcelain"], cwd=repo_path)
    target_resolved = str(target.resolve())
    for line in res.stdout.splitlines():
        if line.startswith("worktree "):
            wt = line[len("worktree ") :]
            if str(Path(wt).resolve()) == target_resolved:
                return True
    return False


def _branch_checked_out(repo_path: Path, branch: str) -> bool:
    res = _run_git(["worktree", "list", "--porcelain"], cwd=repo_path)
    return f"branch refs/heads/{branch}" in res.stdout.splitlines()


def ensure_worktree(
    *,
    repo_path: Path,
    worktree_root: Path,
    repo_name: str,
    issue_number: int,
    base_branch: str,
    author_name: str,
    author_email: str,
) -> Path:
    """Create-or-reuse the per-issue worktree and pin the bot git identity.

    Returns the absolute path to the worktree. Idempotent: calling twice with
    the same arguments returns the same path and never drops commits made in
    the worktree by previous runs.
    """
    target = worktree_path(worktree_root, repo_name, issue_number)
    worktree_root.mkdir(parents=True, exist_ok=True)

    # Drop stale worktree metadata for paths whose directories no longer
    # exist. Without this, a manually-removed worktree directory leaves a
    # registration behind and the next `git worktree add` fails with
    # "already exists at <path>". Idempotent and harmless when nothing is
    # stale.
    _run_git(["worktree", "prune"], cwd=repo_path)

    branch = f"auto/{issue_number}"
    has_branch = _branch_exists(repo_path, branch)
    has_remote_branch = _remote_branch_exists(repo_path, branch)
    has_worktree = target.is_dir() and _worktree_exists(repo_path, target)

    # When both local and remote `auto/<n>` exist, fast-forward the local
    # branch to the remote tip if it's strictly behind. Without this, a
    # stale local branch (another clone or runner pushed to origin since)
    # would have the agent run on out-of-date history and the subsequent
    # `git push` would be rejected as non-fast-forward. Skipped when the
    # branch is currently checked out in a worktree (you can't update a
    # checked-out branch via `update-ref` without leaving that checkout's
    # index/worktree inconsistent); the worktree-reuse path below handles
    # the target-worktree case via `git switch` + the agent's later commits.
    if (
        has_branch
        and has_remote_branch
        and not has_worktree
        and not _branch_checked_out(repo_path, branch)
        and _is_ancestor(repo_path, branch, f"origin/{branch}")
    ):
        try:
            _run_git(
                ["update-ref", f"refs/heads/{branch}", f"origin/{branch}"],
                cwd=repo_path,
            )
        except subprocess.CalledProcessError as e:
            raise WorkspaceError(
                f"could not fast-forward {branch} to origin/{branch}: "
                f"{e.stderr.strip() if e.stderr else e}"
            ) from e

    if has_worktree:
        # The worktree could have drifted off `auto/<n>` between runs (a prior
        # run aborted mid-checkout, or a human peeked at it). Force HEAD back
        # to the right branch so the agent dispatches on the branch we'll
        # later push, and not on a stale branch whose commits would be
        # silently dropped from the PR.
        try:
            _run_git(["switch", branch], cwd=target)
        except subprocess.CalledProcessError as e:
            raise WorkspaceError(
                f"could not switch worktree {target} to {branch}: "
                f"{e.stderr.strip() if e.stderr else e}"
            ) from e
    else:
        try:
            if has_branch:
                _run_git(["worktree", "add", str(target), branch], cwd=repo_path)
            elif has_remote_branch:
                # Local branch missing but remote `auto/<n>` exists (e.g. after a
                # reclone or local prune). Track the remote so the new local
                # branch starts at its tip — otherwise the next push would be
                # rejected as non-fast-forward.
                _run_git(
                    [
                        "worktree",
                        "add",
                        "-b",
                        branch,
                        "--track",
                        str(target),
                        f"origin/{branch}",
                    ],
                    cwd=repo_path,
                )
            else:
                base_ref = f"origin/{base_branch}"
                _run_git(
                    ["worktree", "add", "-b", branch, str(target), base_ref],
                    cwd=repo_path,
                )
        except subprocess.CalledProcessError as e:
            raise WorkspaceError(
                f"git worktree add failed: {e.stderr.strip() if e.stderr else e}"
            ) from e

    # Pin identity on every call so a config change in symphony.toml takes effect
    # next dispatch even on a reused worktree. Local config wins over global.
    _run_git(["config", "--local", "user.name", author_name], cwd=target)
    _run_git(["config", "--local", "user.email", author_email], cwd=target)

    return target
