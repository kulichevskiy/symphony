"""Garbage-collection helpers for stale auto-stuck worktrees."""

from __future__ import annotations

import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .config import Config
from .github import list_open_issues_with_label, name_with_owner
from .workspace import sanitize_repo_name


@dataclass(frozen=True)
class GcCandidate:
    issue_number: int
    path: Path
    branch: str
    age_days: int


def find_gc_candidates(
    cfg: Config,
    *,
    days: int = 14,
    now: float | None = None,
    stuck_issues_fn: Callable[[], set[int]] | None = None,
) -> list[GcCandidate]:
    """Find local worktrees for open ``auto-stuck`` issues older than ``days``."""
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
    if not stuck_issues:
        return []

    _, repo_name = name_with_owner(cfg.repo.path)
    prefix = sanitize_repo_name(repo_name)
    pattern = re.compile(rf"^{re.escape(prefix)}-(\d+)$")
    now_ts = time.time() if now is None else now
    min_age_s = days * 24 * 60 * 60
    candidates: list[GcCandidate] = []
    for path in root.iterdir():
        if not path.is_dir():
            continue
        match = pattern.match(path.name)
        if match is None:
            continue
        issue_number = int(match.group(1))
        if issue_number not in stuck_issues:
            continue
        age_s = max(0.0, now_ts - path.stat().st_mtime)
        if age_s < min_age_s:
            continue
        candidates.append(
            GcCandidate(
                issue_number=issue_number,
                path=path,
                branch=f"auto/{issue_number}",
                age_days=int(age_s // (24 * 60 * 60)),
            )
        )
    return sorted(candidates, key=lambda c: c.issue_number)


def remove_gc_candidate(cfg: Config, candidate: GcCandidate) -> None:
    subprocess.run(
        ["git", "worktree", "remove", "--force", str(candidate.path)],
        cwd=cfg.repo.path,
        check=True,
        capture_output=True,
        text=True,
    )
    res = subprocess.run(
        ["git", "branch", "-D", candidate.branch],
        cwd=cfg.repo.path,
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
