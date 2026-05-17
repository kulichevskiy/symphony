"""Canonical issue status derivation for the daemon UI."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import aiosqlite


class CanonicalState(StrEnum):
    AWAITING_OPERATOR = "awaiting_operator"
    RUNNING = "running"
    FAILED = "failed"
    AWAITING_REVIEW_TRIGGER = "awaiting_review_trigger"
    PR_OPEN = "pr_open"
    DONE = "done"
    IDLE = "idle"


@dataclass(frozen=True)
class CanonicalStatus:
    state: CanonicalState
    since: str | None
    subtitle: str | None
    stuck_for: int | None

    def to_dict(self) -> dict[str, str | int | None]:
        return {
            "state": self.state.value,
            "since": self.since,
            "subtitle": self.subtitle,
            "stuck_for": self.stuck_for,
        }


DEFAULT_STUCK_THRESHOLDS: Mapping[CanonicalState, timedelta] = {
    CanonicalState.AWAITING_OPERATOR: timedelta(minutes=15),
    CanonicalState.RUNNING: timedelta(minutes=30),
    CanonicalState.AWAITING_REVIEW_TRIGGER: timedelta(minutes=10),
    CanonicalState.PR_OPEN: timedelta(hours=24),
}

STATE_PRIORITY: Mapping[CanonicalState, int] = {
    CanonicalState.AWAITING_OPERATOR: 0,
    CanonicalState.RUNNING: 1,
    CanonicalState.FAILED: 2,
    CanonicalState.AWAITING_REVIEW_TRIGGER: 3,
    CanonicalState.PR_OPEN: 4,
    CanonicalState.DONE: 5,
    CanonicalState.IDLE: 6,
}


async def _fetch_one(
    conn: aiosqlite.Connection,
    query: str,
    params: tuple[object, ...],
) -> dict[str, Any] | None:
    cur = await conn.execute(query, params)
    row = await cur.fetchone()
    return dict(row) if row is not None else None


def _as_str(value: object) -> str | None:
    return None if value is None else str(value)


def _parse_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None

    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_now(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=UTC)
    return now.astimezone(UTC)


def _stuck_for_seconds(
    *,
    state: CanonicalState,
    since: str | None,
    now: datetime,
    thresholds: Mapping[CanonicalState, timedelta],
) -> int | None:
    since_dt = _parse_timestamp(since)
    if since_dt is None:
        return None

    age = now - since_dt
    if age.total_seconds() < 0:
        return None

    if state is CanonicalState.FAILED:
        return int(age.total_seconds())

    threshold = thresholds.get(state)
    if threshold is None or age < threshold:
        return None
    return int(age.total_seconds())


def _status(
    state: CanonicalState,
    *,
    since: str | None,
    subtitle: str | None,
    now: datetime,
    thresholds: Mapping[CanonicalState, timedelta],
) -> CanonicalStatus:
    return CanonicalStatus(
        state=state,
        since=since,
        subtitle=subtitle,
        stuck_for=_stuck_for_seconds(
            state=state,
            since=since,
            now=now,
            thresholds=thresholds,
        ),
    )


async def compute_canonical_status(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    now: datetime | None = None,
    thresholds: Mapping[CanonicalState, timedelta] = DEFAULT_STUCK_THRESHOLDS,
) -> CanonicalStatus:
    """Return the first matching canonical UI status for an issue."""

    effective_now = _normalize_now(now or datetime.now(UTC))

    operator_wait = await _fetch_one(
        conn,
        """
        SELECT kind, created_at
        FROM operator_waits
        WHERE issue_id = ?
        ORDER BY created_at DESC, run_id DESC
        LIMIT 1
        """,
        (issue_id,),
    )
    if operator_wait is not None:
        return _status(
            CanonicalState.AWAITING_OPERATOR,
            since=_as_str(operator_wait["created_at"]),
            subtitle=_as_str(operator_wait["kind"]),
            now=effective_now,
            thresholds=thresholds,
        )

    running_run = await _fetch_one(
        conn,
        """
        SELECT stage, started_at
        FROM runs
        WHERE issue_id = ? AND status = 'running'
        ORDER BY started_at DESC, id DESC
        LIMIT 1
        """,
        (issue_id,),
    )
    if running_run is not None:
        return _status(
            CanonicalState.RUNNING,
            since=_as_str(running_run["started_at"]),
            subtitle=_as_str(running_run["stage"]),
            now=effective_now,
            thresholds=thresholds,
        )

    latest_run = await _fetch_one(
        conn,
        """
        SELECT stage, status, started_at, ended_at
        FROM runs
        WHERE issue_id = ?
        ORDER BY started_at DESC, COALESCE(ended_at, '') DESC, id DESC
        LIMIT 1
        """,
        (issue_id,),
    )
    if latest_run is not None and latest_run["status"] == "failed":
        return _status(
            CanonicalState.FAILED,
            since=_as_str(latest_run["ended_at"]) or _as_str(latest_run["started_at"]),
            subtitle=_as_str(latest_run["stage"]),
            now=effective_now,
            thresholds=thresholds,
        )

    open_pr = await _fetch_one(
        conn,
        """
        SELECT pr_number, created_at
        FROM issue_prs
        WHERE issue_id = ? AND merged_at IS NULL
        ORDER BY created_at DESC, github_repo ASC
        LIMIT 1
        """,
        (issue_id,),
    )
    if open_pr is not None:
        return _status(
            CanonicalState.PR_OPEN,
            since=_as_str(open_pr["created_at"]),
            subtitle=f"#{int(open_pr['pr_number'])}",
            now=effective_now,
            thresholds=thresholds,
        )

    latest_pr = await _fetch_one(
        conn,
        """
        SELECT pr_number, merged_at
        FROM issue_prs
        WHERE issue_id = ?
        ORDER BY COALESCE(merged_at, '') DESC, created_at DESC, github_repo ASC
        LIMIT 1
        """,
        (issue_id,),
    )
    if latest_pr is not None:
        return _status(
            CanonicalState.DONE,
            since=_as_str(latest_pr["merged_at"]),
            subtitle=None,
            now=effective_now,
            thresholds=thresholds,
        )

    review_state = await _fetch_one(
        conn,
        """
        SELECT iteration
        FROM review_state
        WHERE issue_id = ? AND iteration > 0
        """,
        (issue_id,),
    )
    if review_state is not None:
        latest_comment = await _fetch_one(
            conn,
            """
            SELECT seen_at
            FROM comment_events
            WHERE issue_id = ?
            ORDER BY seen_at DESC, comment_id DESC
            LIMIT 1
            """,
            (issue_id,),
        )
        since = None
        if latest_comment is not None:
            since = _as_str(latest_comment["seen_at"])
        if since is None and latest_run is not None:
            since = _as_str(latest_run["ended_at"]) or _as_str(latest_run["started_at"])
        return _status(
            CanonicalState.AWAITING_REVIEW_TRIGGER,
            since=since,
            subtitle=f"iteration={int(review_state['iteration'])}",
            now=effective_now,
            thresholds=thresholds,
        )

    return _status(
        CanonicalState.IDLE,
        since=None,
        subtitle=None,
        now=effective_now,
        thresholds=thresholds,
    )


def canonical_status_sort_key(status: CanonicalStatus) -> tuple[int, int, datetime]:
    since = _parse_timestamp(status.since) or datetime.max.replace(tzinfo=UTC)
    return (
        0 if status.stuck_for is not None else 1,
        STATE_PRIORITY[status.state],
        since,
    )
