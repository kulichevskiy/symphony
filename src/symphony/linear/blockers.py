"""Dependency predicates for Linear issue relations."""

from __future__ import annotations

from ..tracker import Issue

OPEN_BLOCKER_TYPES = frozenset({"backlog", "unstarted", "started", "triage"})


def open_blocker_ids(issue: Issue) -> list[str]:
    """Return open blocker identifiers in Linear's relation order."""
    return [
        blocker.identifier
        for blocker in issue.blocked_by
        if not blocker.archived and blocker.state_type in OPEN_BLOCKER_TYPES
    ]


def is_blocked(issue: Issue) -> bool:
    return bool(open_blocker_ids(issue))
