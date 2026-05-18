"""Soft warning derivation for UI issue payloads."""

from __future__ import annotations

from datetime import timedelta

from .status import CanonicalState, CanonicalStatus

NO_PROGRESS_WARNING = "no_progress"
DEFAULT_PR_NO_PROGRESS_THRESHOLD = timedelta(hours=2)


def issue_warnings(
    status: CanonicalStatus,
    *,
    latest_activity_age_secs: int | None,
    pr_no_progress_threshold: timedelta = DEFAULT_PR_NO_PROGRESS_THRESHOLD,
) -> list[str]:
    if (
        status.state == CanonicalState.PR_OPEN
        and latest_activity_age_secs is not None
        and latest_activity_age_secs > int(pr_no_progress_threshold.total_seconds())
    ):
        return [NO_PROGRESS_WARNING]
    return []


__all__ = [
    "DEFAULT_PR_NO_PROGRESS_THRESHOLD",
    "NO_PROGRESS_WARNING",
    "issue_warnings",
]
