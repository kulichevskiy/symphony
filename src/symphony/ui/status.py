"""Canonical issue status derivation for the daemon UI."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import aiosqlite

from ..db import operator_waits


class CanonicalState(StrEnum):
    DRIFT_DETECTED = "drift_detected"
    HALTED = "halted"
    PAUSED = "paused"
    AWAITING_MERGE = "awaiting_merge"
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


@dataclass(frozen=True)
class ExternalDriftFlag:
    field: str
    source_name: str
    flagged_at: str
    drift_kind: str


@dataclass(frozen=True)
class ExternalStatusSnapshot:
    drift_flags: Sequence[ExternalDriftFlag]


DEFAULT_STUCK_THRESHOLDS: Mapping[CanonicalState, timedelta] = {
    CanonicalState.PAUSED: timedelta(minutes=15),
    CanonicalState.AWAITING_MERGE: timedelta(hours=4),
    CanonicalState.RUNNING: timedelta(minutes=30),
    CanonicalState.AWAITING_REVIEW_TRIGGER: timedelta(minutes=10),
    CanonicalState.PR_OPEN: timedelta(hours=24),
}

STATE_PRIORITY: Mapping[CanonicalState, int] = {
    CanonicalState.DRIFT_DETECTED: 0,
    CanonicalState.HALTED: 1,
    CanonicalState.FAILED: 2,
    CanonicalState.PAUSED: 3,
    CanonicalState.AWAITING_MERGE: 4,
    CanonicalState.RUNNING: 5,
    CanonicalState.AWAITING_REVIEW_TRIGGER: 6,
    CanonicalState.PR_OPEN: 7,
    CanonicalState.DONE: 8,
    CanonicalState.IDLE: 9,
}

ALWAYS_STUCK_STATES = frozenset(
    {
        CanonicalState.DRIFT_DETECTED,
        CanonicalState.HALTED,
        CanonicalState.FAILED,
    }
)

# A run row in one of these states means there is no live worker behind it.
# `interrupted` is set by reconcile.py when the host PID has died (e.g. after
# a symphony restart); resurrection paths must treat it the same as `failed`.
DEAD_RUN_STATUSES: frozenset[str] = frozenset({"failed", "interrupted"})

OPERATOR_WAIT_STATES: Mapping[str, CanonicalState] = {
    operator_waits.KIND_ACCEPTANCE_REJECTED: CanonicalState.PAUSED,
    operator_waits.KIND_IMPLEMENT_FAILED: CanonicalState.HALTED,
    operator_waits.KIND_REVIEW_FAILED: CanonicalState.HALTED,
    operator_waits.KIND_REVIEW_STOPPED: CanonicalState.PAUSED,
    operator_waits.KIND_COST_CAP: CanonicalState.PAUSED,
    operator_waits.KIND_MERGE: CanonicalState.AWAITING_MERGE,
}

OPERATOR_WAIT_SUPERSEDED_BY_STAGES: Mapping[str, tuple[str, ...]] = {
    operator_waits.KIND_ACCEPTANCE_REJECTED: ("acceptance_fix",),
    operator_waits.KIND_IMPLEMENT_FAILED: ("implement",),
    operator_waits.KIND_REVIEW_FAILED: ("review_fix",),
}


async def _fetch_one(
    conn: aiosqlite.Connection,
    query: str,
    params: tuple[object, ...],
) -> dict[str, Any] | None:
    cur = await conn.execute(query, params)
    row = await cur.fetchone()
    return dict(row) if row is not None else None


async def _fetch_all(
    conn: aiosqlite.Connection,
    query: str,
    params: tuple[object, ...],
) -> list[dict[str, Any]]:
    cur = await conn.execute(query, params)
    rows = await cur.fetchall()
    return [dict(row) for row in rows]


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


def _run_supersedes_operator_wait(
    run: Mapping[str, Any],
    wait_created_at: str | None,
) -> bool:
    status = _as_str(run.get("status"))
    if status == "running":
        return True
    if status != "completed":
        return False

    wait_created = _parse_timestamp(wait_created_at)
    run_progressed = _parse_timestamp(_as_str(run.get("ended_at"))) or _parse_timestamp(
        _as_str(run.get("started_at"))
    )
    return (
        wait_created is not None
        and run_progressed is not None
        and run_progressed > wait_created
    )


async def _operator_wait_superseding_run(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    wait_kind: str | None,
    wait_created_at: str | None,
) -> dict[str, Any] | None:
    stages = OPERATOR_WAIT_SUPERSEDED_BY_STAGES.get(wait_kind or "")
    if not stages:
        return None

    placeholders = ",".join("?" * len(stages))
    rows = await _fetch_all(
        conn,
        f"""
        SELECT stage, status, started_at, ended_at
        FROM runs
        WHERE issue_id = ?
          AND stage IN ({placeholders})
          AND status IN ('running', 'completed')
        ORDER BY COALESCE(ended_at, started_at) DESC, started_at DESC, id DESC
        LIMIT 10
        """,
        (issue_id, *stages),
    )
    for run in rows:
        if _run_supersedes_operator_wait(run, wait_created_at):
            return run
    return None


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

    if state in ALWAYS_STUCK_STATES:
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


def _earliest_flagged_at(flags: Sequence[ExternalDriftFlag]) -> str | None:
    earliest: tuple[datetime, str] | None = None
    for flag in flags:
        flagged_at = _parse_timestamp(flag.flagged_at)
        if flagged_at is None:
            continue
        if earliest is None or flagged_at < earliest[0]:
            earliest = (flagged_at, flag.flagged_at)
    return earliest[1] if earliest is not None else None


def _drift_status(
    snapshot: ExternalStatusSnapshot,
    *,
    now: datetime,
    thresholds: Mapping[CanonicalState, timedelta],
) -> CanonicalStatus | None:
    flags = tuple(snapshot.drift_flags)
    if not flags:
        return None
    return _status(
        CanonicalState.DRIFT_DETECTED,
        since=_earliest_flagged_at(flags),
        subtitle=f"{len(flags)} field(s) disagree",
        now=now,
        thresholds=thresholds,
    )


async def compute_canonical_status(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    now: datetime | None = None,
    thresholds: Mapping[CanonicalState, timedelta] = DEFAULT_STUCK_THRESHOLDS,
    external_snapshot: ExternalStatusSnapshot | None = None,
) -> CanonicalStatus:
    """Return the first matching canonical UI status for an issue."""

    effective_now = _normalize_now(now or datetime.now(UTC))
    if external_snapshot is not None:
        status = _drift_status(
            external_snapshot,
            now=effective_now,
            thresholds=thresholds,
        )
        if status is not None:
            return status

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
        wait_kind = _as_str(operator_wait["kind"])
        superseding_run = await _operator_wait_superseding_run(
            conn,
            issue_id=issue_id,
            wait_kind=wait_kind,
            wait_created_at=_as_str(operator_wait["created_at"]),
        )
        if superseding_run is not None and superseding_run["status"] == "running":
            return _status(
                CanonicalState.RUNNING,
                since=_as_str(superseding_run["started_at"]),
                subtitle=_as_str(superseding_run["stage"]),
                now=effective_now,
                thresholds=thresholds,
            )
        if superseding_run is None:
            return _status(
                OPERATOR_WAIT_STATES.get(wait_kind or "", CanonicalState.PAUSED),
                since=_as_str(operator_wait["created_at"]),
                subtitle=wait_kind,
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
    if latest_run is not None and latest_run["status"] in DEAD_RUN_STATUSES:
        stage = _as_str(latest_run["stage"])
        run_status = _as_str(latest_run["status"])
        subtitle: str | None
        if run_status and run_status != "failed":
            subtitle = f"{stage} ({run_status})" if stage else run_status
        else:
            subtitle = stage
        return _status(
            CanonicalState.FAILED,
            since=_as_str(latest_run["ended_at"]) or _as_str(latest_run["started_at"]),
            subtitle=subtitle,
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
