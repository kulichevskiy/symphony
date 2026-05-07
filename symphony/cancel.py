"""Manual cancellation markers for cooperative Symphony shutdown of an issue."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import Config
from .events import EventLog
from .github import label_issue


def cancel_marker_path(repo_path: Path, issue_number: int) -> Path:
    return repo_path / ".symphony" / "canceled" / str(issue_number)


def mark_issue_canceled(repo_path: Path, issue_number: int) -> Path:
    marker = cancel_marker_path(repo_path, issue_number)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("canceled\n")
    return marker


def is_issue_canceled(repo_path: Path, issue_number: int) -> bool:
    return cancel_marker_path(repo_path, issue_number).exists()


def request_cancel(
    cfg: Config,
    issue_number: int,
    *,
    label_fn: Any = label_issue,
    event_log: EventLog | None = None,
) -> Path:
    """Record a local cancel request and label the GitHub issue."""
    marker = mark_issue_canceled(cfg.repo.path, issue_number)
    label_fn(issue_number, "auto-canceled", repo_path=cfg.repo.path)
    (event_log or EventLog.for_repo(cfg.repo.path)).emit(
        "auto-canceled",
        issue_number=issue_number,
        payload={"reason": "manual"},
    )
    return marker
