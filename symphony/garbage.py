"""Garbage-collection helpers for stale and orphaned per-issue worktrees.

Two shapes qualify:

- ``auto-stuck``: issue is open, labeled ``auto-stuck``, worktree has been
  idle longer than ``--days``. The classic "review loop gave up; come look"
  case.
- ``closed-orphan``: issue is closed and either has no PR or the PR is
  merged/closed. These accumulate when the per-issue worktree outlives the
  run that owned it — agent failure, ``merge_failed``, manual ``gh pr
  merge``, issue closed without a PR, empty-diff runs.

Both kinds flow through the same :func:`remove_worktree` primitive so any
future post-merge cleanup uses the same ordering (worktree first, then
branch) and the bug surface stays in one place.
"""

from __future__ import annotations

import logging
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable, Iterable
from typing import Any

from .config import Config
from .github import (
    GithubError,
    find_pr_for_branch,
    get_issue_state,
    list_open_issues_with_label,
    name_with_owner,
)
from .workspace import sanitize_repo_name

log = logging.getLogger(__name__)


# Reasons surfaced to the user / event log so each removal can be explained.
REASON_AUTO_STUCK = "auto-stuck"
REASON_CLOSED_NO_PR = "closed-no-pr"
REASON_CLOSED_PR_MERGED = "closed-pr-merged"
REASON_CLOSED_PR_CLOSED = "closed-pr-closed"


@dataclass(frozen=True)
class GcCandidate:
    issue_number: int
    path: Path
    branch: str
    age_days: int
    reason: str = REASON_AUTO_STUCK


def _iter_issue_worktrees(cfg: Config, *, repo_name: str) -> Iterable[tuple[int, Path]]:
    """Yield ``(issue_number, path)`` for each ``<repo>-<n>`` worktree dir.

    Foreign directories (other tools share the worktree root in practice,
    per the issue's "out of scope") are ignored.
    """
    root = cfg.paths.worktree_root
    if not root.is_dir():
        return
    prefix = sanitize_repo_name(repo_name)
    pattern = re.compile(rf"^{re.escape(prefix)}-(\d+)$")
    for path in root.iterdir():
        if not path.is_dir():
            continue
        match = pattern.match(path.name)
        if match is None:
            continue
        yield int(match.group(1)), path


def _classify_closed_orphan(
    issue_number: int,
    branch: str,
    *,
    issue_state_fn: Callable[[int], str],
    pr_for_branch_fn: Callable[[str], tuple[Any, str] | None],
) -> str | None:
    """Return a closed-orphan reason for the issue, or ``None``.

    ``None`` covers both "issue still open" and "issue closed but PR open"
    (concurrent PR review still active). ``GithubError`` propagates so the
    caller can log + skip — never block the boot path.
    """
    state = issue_state_fn(issue_number)
    if state != "CLOSED":
        return None
    pr = pr_for_branch_fn(branch)
    if pr is None:
        return REASON_CLOSED_NO_PR
    _, pr_state = pr
    if pr_state == "MERGED":
        return REASON_CLOSED_PR_MERGED
    if pr_state == "CLOSED":
        return REASON_CLOSED_PR_CLOSED
    # PR still OPEN despite closed issue — leave it alone.
    return None


def find_gc_candidates(
    cfg: Config,
    *,
    days: int = 14,
    now: float | None = None,
    stuck_issues_fn: Callable[[], set[int]] | None = None,
    issue_state_fn: Callable[[int], str] | None = None,
    pr_for_branch_fn: Callable[[str], tuple[object, str] | None] | None = None,
) -> list[GcCandidate]:
    """Return worktrees eligible for garbage collection.

    Two shapes are considered (see module docstring):

    - open ``auto-stuck`` issues older than ``days``
    - closed issues with no PR or with merged/closed PR (any age)
    """
    if days < 0:
        raise ValueError("days must be >= 0")
    root = cfg.paths.worktree_root
    if not root.is_dir():
        return []

    if stuck_issues_fn is None:
        stuck_issues_fn = lambda: {  # noqa: E731
            issue.number
            for issue in list_open_issues_with_label(
                "auto-stuck", repo_path=cfg.repo.path
            )
        }
    stuck_issues = stuck_issues_fn()
    owner, repo_name = name_with_owner(cfg.repo.path)

    if issue_state_fn is None:
        issue_state_fn = lambda n: get_issue_state(n, repo_path=cfg.repo.path)  # noqa: E731
    if pr_for_branch_fn is None:
        pr_for_branch_fn = lambda branch: find_pr_for_branch(  # noqa: E731
            branch,
            repo_path=cfg.repo.path,
            base_branch=cfg.repo.default_branch,
            expected_owner=owner,
        )

    now_ts = time.time() if now is None else now
    min_age_s = days * 24 * 60 * 60
    candidates: list[GcCandidate] = []
    for issue_number, path in _iter_issue_worktrees(cfg, repo_name=repo_name):
        branch = f"auto/{issue_number}"
        age_s = max(0.0, now_ts - _latest_activity_mtime(path))
        age_days = int(age_s // (24 * 60 * 60))

        if issue_number in stuck_issues:
            if age_s >= min_age_s:
                candidates.append(
                    GcCandidate(
                        issue_number=issue_number,
                        path=path,
                        branch=branch,
                        age_days=age_days,
                        reason=REASON_AUTO_STUCK,
                    )
                )
            continue

        # Not in the auto-stuck open set — see if the issue is closed and the
        # worktree is genuinely orphaned.
        try:
            reason = _classify_closed_orphan(
                issue_number,
                branch,
                issue_state_fn=issue_state_fn,
                pr_for_branch_fn=pr_for_branch_fn,
            )
        except GithubError as e:
            log.warning(
                "could not classify worktree for issue #%d: %s; skipping",
                issue_number,
                e,
            )
            continue
        if reason is None:
            continue
        candidates.append(
            GcCandidate(
                issue_number=issue_number,
                path=path,
                branch=branch,
                age_days=age_days,
                reason=reason,
            )
        )
    return sorted(candidates, key=lambda c: c.issue_number)


def _latest_activity_mtime(path: Path) -> float:
    latest = path.stat().st_mtime
    for child in path.rglob("*"):
        try:
            latest = max(latest, child.stat().st_mtime)
        except OSError:
            continue
    return latest


def remove_worktree(repo_path: Path, *, branch: str, path: Path) -> None:
    """Remove a per-issue worktree and its local branch.

    Single primitive used by the post-merge happy path, ``symphony gc``, and
    startup GC so ordering bugs (worktree-then-branch vs branch-first) get
    fixed in one place. ``git worktree remove`` first because deleting a
    branch that's checked out anywhere is rejected.
    """
    subprocess.run(
        ["git", "worktree", "remove", "--force", str(path)],
        cwd=repo_path,
        check=True,
        capture_output=True,
        text=True,
    )
    res = subprocess.run(
        ["git", "branch", "-D", branch],
        cwd=repo_path,
        check=False,
        capture_output=True,
        text=True,
    )
    if res.returncode != 0 and "not found" not in (res.stderr + res.stdout).lower():
        raise subprocess.CalledProcessError(
            res.returncode,
            res.args,
            output=res.stdout,
            stderr=res.stderr,
        )


def remove_gc_candidate(cfg: Config, candidate: GcCandidate) -> None:
    remove_worktree(cfg.repo.path, branch=candidate.branch, path=candidate.path)


def run_startup_gc(
    cfg: Config,
    *,
    issue_state_fn: Callable[[int], str] | None = None,
    pr_for_branch_fn: Callable[[str], tuple[object, str] | None] | None = None,
    active_paths: set[Path] | None = None,
    event_log: object | None = None,
) -> list[GcCandidate]:
    """Remove worktrees whose issue is closed (no PR / merged / closed PR).

    Called by :func:`symphony.orchestrator.run_forever` before its first
    poll tick. Failures to query GitHub are logged and skipped — startup
    must never block on GC.

    ``active_paths`` lets a caller exclude worktrees currently in use by an
    in-flight dispatch. The orchestrator's startup path has none, but the
    parameter keeps the function safe to call from elsewhere.
    """
    root = cfg.paths.worktree_root
    if not root.is_dir():
        return []

    try:
        owner, repo_name = name_with_owner(cfg.repo.path)
    except GithubError as e:
        log.warning("startup-gc: could not resolve repo owner: %s; skipping", e)
        return []

    if issue_state_fn is None:
        issue_state_fn = lambda n: get_issue_state(n, repo_path=cfg.repo.path)  # noqa: E731
    if pr_for_branch_fn is None:
        pr_for_branch_fn = lambda branch: find_pr_for_branch(  # noqa: E731
            branch,
            repo_path=cfg.repo.path,
            base_branch=cfg.repo.default_branch,
            expected_owner=owner,
        )

    removed: list[GcCandidate] = []
    for issue_number, path in _iter_issue_worktrees(cfg, repo_name=repo_name):
        if active_paths and path.resolve() in {p.resolve() for p in active_paths}:
            continue
        branch = f"auto/{issue_number}"
        try:
            reason = _classify_closed_orphan(
                issue_number,
                branch,
                issue_state_fn=issue_state_fn,
                pr_for_branch_fn=pr_for_branch_fn,
            )
        except GithubError as e:
            log.warning(
                "startup-gc: could not classify issue #%d: %s; skipping",
                issue_number,
                e,
            )
            continue
        if reason is None:
            continue
        candidate = GcCandidate(
            issue_number=issue_number,
            path=path,
            branch=branch,
            age_days=0,
            reason=reason,
        )
        try:
            remove_worktree(cfg.repo.path, branch=branch, path=path)
        except subprocess.CalledProcessError as e:
            detail = (e.stderr or e.stdout or str(e)).strip()
            log.warning(
                "startup-gc: failed to remove %s for issue #%d: %s",
                path,
                issue_number,
                detail,
            )
            continue
        log.info(
            "startup-gc: removed worktree for issue #%d (%s)",
            issue_number,
            reason,
        )
        if event_log is not None:
            try:
                event_log.emit(
                    "startup-gc",
                    issue_number=issue_number,
                    payload={
                        "path": str(path),
                        "branch": branch,
                        "reason": reason,
                    },
                )
            except Exception:  # pragma: no cover — event-log failure is non-fatal
                log.exception("startup-gc: event log emit failed for #%d", issue_number)
        removed.append(candidate)
    return removed
