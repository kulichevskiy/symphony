"""HTTP API routes for the web UI."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated

import aiosqlite
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .db import ReadOnlyDbPool
from .status import (
    DEFAULT_STUCK_THRESHOLDS,
    CanonicalState,
    canonical_status_sort_key,
    compute_canonical_status,
)
from .warnings import DEFAULT_PR_NO_PROGRESS_THRESHOLD, issue_warnings


class CanonicalStatusPayload(BaseModel):
    state: str
    since: str | None
    subtitle: str | None
    stuck_for: int | None


class IssueSummary(BaseModel):
    id: str
    identifier: str
    title: str
    team_key: str
    latest_activity_ts: str | None
    latest_activity_age_secs: int | None
    canonical_status: CanonicalStatusPayload
    warnings: list[str] = Field(default_factory=list)


class IssueScope(StrEnum):
    ACTIVE = "active"
    RECENT = "recent"
    ALL = "all"


_ISSUE_SCOPE_CTES = """
WITH active_issue_ids(issue_id) AS (
    SELECT issue_id FROM runs WHERE status = 'running'
    UNION
    SELECT issue_id FROM operator_waits
    UNION
    SELECT issue_id FROM issue_prs WHERE merged_at IS NULL
    UNION
    SELECT rs.issue_id
    FROM review_state rs
    WHERE rs.iteration > 0
      AND NOT EXISTS (
          SELECT 1 FROM issue_prs ip
          WHERE ip.issue_id = rs.issue_id
      )
),
latest_events(issue_id, ts) AS (
    SELECT issue_id, started_at FROM runs
    UNION ALL
    SELECT issue_id, ended_at FROM runs WHERE ended_at IS NOT NULL
    UNION ALL
    SELECT issue_id, created_at FROM issue_prs
    UNION ALL
    SELECT issue_id, merged_at FROM issue_prs WHERE merged_at IS NOT NULL
    UNION ALL
    SELECT issue_id, seen_at FROM comment_events
    UNION ALL
    SELECT r.issue_id, m.last_event_at
    FROM activity_comment_marks m
    JOIN runs r ON r.id = m.run_id
    WHERE m.last_event_at IS NOT NULL
    UNION ALL
    SELECT r.issue_id, m.last_posted_at
    FROM activity_comment_marks m
    JOIN runs r ON r.id = m.run_id
    WHERE m.last_posted_at IS NOT NULL
    UNION ALL
    SELECT issue_id, warning_posted_at
    FROM issue_cost_marks
    WHERE warning_posted_at IS NOT NULL
    UNION ALL
    SELECT issue_id, created_at FROM operator_waits
    UNION ALL
    SELECT issue_id, ts FROM state_transitions
),
latest_activity_sources(issue_id, ts) AS (
    SELECT issue_id, COALESCE(ended_at, started_at) FROM runs
    UNION ALL
    SELECT issue_id, ts FROM state_transitions
    UNION ALL
    SELECT issue_id, seen_at FROM comment_events
    UNION ALL
    SELECT r.issue_id, m.last_event_at
    FROM activity_comment_marks m
    JOIN runs r ON r.id = m.run_id
    WHERE m.last_event_at IS NOT NULL
    UNION ALL
    SELECT issue_id, COALESCE(merged_at, created_at) FROM issue_prs
    UNION ALL
    SELECT issue_id, created_at FROM operator_waits
),
latest_activity(issue_id, latest_activity_ts) AS (
    SELECT issue_id, MAX(ts)
    FROM latest_activity_sources
    WHERE ts IS NOT NULL
    GROUP BY issue_id
),
recent_issue_ids(issue_id) AS (
    SELECT issue_id
    FROM latest_events
    GROUP BY issue_id
    ORDER BY MAX(ts) DESC, issue_id ASC
    LIMIT 50
)
"""


def _identifier_sort_key(identifier: str) -> tuple[str, int, str]:
    team, separator, suffix = identifier.partition("-")
    if separator and suffix.isdigit():
        return (team, int(suffix), identifier)
    return (identifier, 2**31 - 1, identifier)


def _utc_iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    return int(str(value))


def _list_issues_query(
    scope: IssueScope,
    q: str | None,
    *,
    now: datetime,
) -> tuple[str, tuple[str, ...]]:
    where: list[str] = []
    params: list[str] = [_utc_iso(now)]

    if scope is IssueScope.ACTIVE:
        where.append("i.id IN (SELECT issue_id FROM active_issue_ids)")
    elif scope is IssueScope.RECENT:
        where.append(
            "("
            "i.id IN (SELECT issue_id FROM active_issue_ids) "
            "OR i.id IN (SELECT issue_id FROM recent_issue_ids)"
            ")"
        )

    normalized_q = q.strip().lower() if q is not None else ""
    if normalized_q:
        where.append(
            "(instr(lower(i.identifier), ?) > 0 OR instr(lower(i.title), ?) > 0)"
        )
        params.extend([normalized_q, normalized_q])

    where_sql = "" if not where else f"WHERE {' AND '.join(where)}"
    return (
        f"""
        {_ISSUE_SCOPE_CTES}
        SELECT
            i.id,
            i.identifier,
            i.title,
            i.team_key,
            la.latest_activity_ts,
            CASE
                WHEN la.latest_activity_ts IS NULL THEN NULL
                ELSE CAST(
                    MAX(0, strftime('%s', ?) - strftime('%s', la.latest_activity_ts))
                    AS INTEGER
                )
            END AS latest_activity_age_secs
        FROM issues i
        LEFT JOIN latest_activity la ON la.issue_id = i.id
        {where_sql}
        """,
        tuple(params),
    )


def create_api_router(
    ui_db_pool: ReadOnlyDbPool | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
    status_thresholds: Mapping[CanonicalState, timedelta] | None = None,
    no_progress_threshold: timedelta | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/api")
    thresholds = status_thresholds or DEFAULT_STUCK_THRESHOLDS
    pr_no_progress_threshold = (
        DEFAULT_PR_NO_PROGRESS_THRESHOLD
        if no_progress_threshold is None
        else no_progress_threshold
    )

    def now() -> datetime:
        return clock() if clock is not None else datetime.now(UTC)

    @router.get(
        "/issues",
        response_model=list[IssueSummary],
        response_model_exclude_defaults=True,
    )
    async def list_issues(
        q: Annotated[str | None, Query()] = None,
        scope: Annotated[IssueScope, Query()] = IssueScope.ACTIVE,
    ) -> list[IssueSummary]:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")

        try:
            conn = await ui_db_pool.connection()
            request_now = now()
            query, params = _list_issues_query(scope, q, now=request_now)
            cur = await conn.execute(query, params)
            rows = await cur.fetchall()
            issues = [dict(row) for row in rows]
            statuses = [
                (
                    issue,
                    await compute_canonical_status(
                        conn,
                        str(issue["id"]),
                        now=request_now,
                        thresholds=thresholds,
                    ),
                )
                for issue in issues
            ]
        except aiosqlite.Error as exc:
            raise HTTPException(
                status_code=503,
                detail="UI database is not available",
            ) from exc

        statuses.sort(
            key=lambda item: (
                *canonical_status_sort_key(item[1]),
                _identifier_sort_key(str(item[0]["identifier"])),
            )
        )
        payloads: list[IssueSummary] = []
        for issue, status in statuses:
            warnings = issue_warnings(
                status,
                latest_activity_age_secs=_optional_int(
                    issue["latest_activity_age_secs"]
                ),
                pr_no_progress_threshold=pr_no_progress_threshold,
            )
            payload: dict[str, object] = {
                **issue,
                "canonical_status": status.to_dict(),
            }
            if warnings:
                payload["warnings"] = warnings
            payloads.append(IssueSummary.model_validate(payload))
        return payloads

    @router.api_route(
        "/{path:path}",
        methods=["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
    )
    async def api_placeholder(path: str) -> JSONResponse:
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    return router
