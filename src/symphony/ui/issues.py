"""Issue detail API for the read-only daemon UI."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import aiosqlite
from fastapi import APIRouter, HTTPException, Query

from .. import db
from .db import ReadOnlyDbPool
from .external import ExternalSnapshotService
from .status import (
    DEFAULT_STUCK_THRESHOLDS,
    CanonicalState,
    ExternalDriftFlag,
    ExternalStatusSnapshot,
    compute_canonical_status,
)
from .warnings import DEFAULT_PR_NO_PROGRESS_THRESHOLD, issue_warnings


def _dict(row: aiosqlite.Row) -> dict[str, Any]:
    return dict(row)


async def _fetch_all(
    conn: aiosqlite.Connection,
    query: str,
    params: tuple[object, ...],
) -> list[dict[str, Any]]:
    cur = await conn.execute(query, params)
    rows = await cur.fetchall()
    return [_dict(row) for row in rows]


async def _fetch_one(
    conn: aiosqlite.Connection,
    query: str,
    params: tuple[object, ...],
) -> dict[str, Any] | None:
    cur = await conn.execute(query, params)
    row = await cur.fetchone()
    return _dict(row) if row is not None else None


def _external_fields_changed(drift_kind: str | None) -> list[str]:
    if drift_kind == "merge_zombie":
        return ["operator_waits", "issue_prs.merged_at"]
    if drift_kind == "pr_locally_merged":
        return ["issue_prs.merged_at"]
    if drift_kind == "pr_closed_no_merge":
        return ["operator_waits", "issue_prs"]
    return []


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


def _age_seconds(ts: str | None, request_now: datetime) -> int | None:
    parsed = _parse_timestamp(ts)
    if parsed is None:
        return None
    return max(0, int((_normalize_now(request_now) - parsed).total_seconds()))


async def _latest_activity(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    request_now: datetime,
) -> tuple[str | None, int | None]:
    row = await _fetch_one(
        conn,
        """
        WITH latest_activity_sources(ts) AS (
            SELECT COALESCE(ended_at, started_at)
            FROM runs
            WHERE issue_id = ?
            UNION ALL
            SELECT ts
            FROM state_transitions
            WHERE issue_id = ?
            UNION ALL
            SELECT seen_at
            FROM comment_events
            WHERE issue_id = ?
            UNION ALL
            SELECT m.last_event_at
            FROM activity_comment_marks m
            JOIN runs r ON r.id = m.run_id
            WHERE r.issue_id = ? AND m.last_event_at IS NOT NULL
            UNION ALL
            SELECT COALESCE(merged_at, created_at)
            FROM issue_prs
            WHERE issue_id = ?
            UNION ALL
            SELECT created_at
            FROM operator_waits
            WHERE issue_id = ?
        )
        SELECT MAX(ts) AS latest_activity_ts
        FROM latest_activity_sources
        WHERE ts IS NOT NULL
        """,
        (issue_id, issue_id, issue_id, issue_id, issue_id, issue_id),
    )
    if row is None:
        return None, None
    latest_activity_ts = (
        str(row["latest_activity_ts"])
        if row["latest_activity_ts"] is not None
        else None
    )
    return latest_activity_ts, _age_seconds(latest_activity_ts, request_now)


def _external_status_snapshot(payload: dict[str, Any]) -> ExternalStatusSnapshot:
    fetched_at = str(payload.get("fetched_at") or "")
    flags: list[ExternalDriftFlag] = []
    raw_flags = payload.get("drift_flags")
    if not isinstance(raw_flags, list):
        return ExternalStatusSnapshot(drift_flags=())
    for raw_flag in raw_flags:
        if not isinstance(raw_flag, dict):
            continue
        if raw_flag.get("severity") == "warning":
            continue
        field = raw_flag.get("field")
        if field is None:
            continue
        flagged_at = raw_flag.get("flagged_at") or fetched_at
        flags.append(
            ExternalDriftFlag(
                field=str(field),
                source_name=str(raw_flag.get("source_name") or ""),
                flagged_at=str(flagged_at),
                drift_kind=str(raw_flag.get("field") or field),
            )
        )
    return ExternalStatusSnapshot(drift_flags=tuple(flags))


def _timeline_event(row: dict[str, Any]) -> dict[str, Any]:
    kind = row["kind"]
    payload: dict[str, Any]
    if kind == "run_started":
        payload = {
            "run_id": row["run_id"],
            "stage": row["stage"],
            "pid": row["pid"],
        }
    elif kind == "run_ended":
        payload = {
            "run_id": row["run_id"],
            "stage": row["stage"],
            "status": row["status"],
            "cost_usd": row["cost_usd"],
        }
    elif kind == "pr_opened":
        payload = {
            "github_repo": row["github_repo"],
            "pr_number": row["pr_number"],
            "pr_url": row["pr_url"],
        }
    elif kind == "pr_merged":
        payload = {
            "github_repo": row["github_repo"],
            "pr_number": row["pr_number"],
        }
    elif kind == "comment_seen":
        payload = {"comment_id": row["comment_id"]}
    elif kind == "activity_comment_posted":
        payload = {
            "run_id": row["run_id"],
            "fingerprint": row["fingerprint"],
        }
    elif kind == "cost_warning_posted":
        payload = {}
    elif kind == "review_state_changed":
        payload = {
            "field": row["field"],
            "old": row["old_value"],
            "new": row["new_value"],
        }
    elif kind in {"operator_wait_started", "operator_wait_ended"}:
        payload = {"kind": row["wait_kind"]}
    elif kind == "external_observed":
        payload = {
            "source": row["external_source"],
            "drift_kind": row["drift_kind"],
        }
    elif kind == "external_cleared":
        payload = {
            "source": row["external_source"],
            "drift_kind": row["drift_kind"],
            "fields_changed": _external_fields_changed(row["drift_kind"]),
        }
    elif kind == "external_state_change":
        payload = {
            "source": row["external_source"] or "linear",
            "field": row["field"],
            "new_value": row["new_value"],
        }
    else:
        raise ValueError(f"unknown timeline kind: {kind}")

    return {"ts": row["ts"], "kind": kind, "payload": payload}


def create_issue_detail_router(
    pool: ReadOnlyDbPool,
    *,
    external_service: ExternalSnapshotService | None = None,
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

    @router.get("/issues/{issue_id}")
    async def issue_detail(
        issue_id: str,
        include_external: Annotated[bool, Query()] = False,
    ) -> dict[str, Any]:
        conn = await pool.connection()
        request_now = now()
        issue = await _fetch_one(
            conn,
            """
            SELECT id, identifier, title, team_key
            FROM issues
            WHERE id = ?
            """,
            (issue_id,),
        )
        if issue is None:
            raise HTTPException(status_code=404, detail="Issue not found")
        external_snapshot: ExternalStatusSnapshot | None = None
        external_payload: dict[str, Any] | None = None
        if include_external and external_service is not None:
            external_payload = await external_service.get_issue_external(
                conn,
                issue_id,
            )
            if external_payload is not None:
                external_snapshot = _external_status_snapshot(external_payload)
        canonical_status = await compute_canonical_status(
            conn,
            issue_id,
            now=request_now,
            thresholds=thresholds,
            external_snapshot=external_snapshot,
        )
        latest_activity_ts, latest_activity_age_secs = await _latest_activity(
            conn,
            issue_id,
            request_now=request_now,
        )

        runs = await _fetch_all(
            conn,
            """
            SELECT id, stage, status, pid, started_at, ended_at, cost_usd,
                   input_tokens, output_tokens, cache_write_tokens,
                   cache_read_tokens,
                   termination_kind, termination_detail, exit_returncode
            FROM runs
            WHERE issue_id = ?
            ORDER BY started_at DESC, id DESC
            """,
            (issue_id,),
        )
        issue_prs = await _fetch_all(
            conn,
            """
            SELECT github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            FROM issue_prs
            WHERE issue_id = ?
            ORDER BY created_at DESC, github_repo ASC
            """,
            (issue_id,),
        )
        operator_waits = await _fetch_all(
            conn,
            """
            SELECT run_id, kind, linear_team_key, github_repo, issue_label, created_at
            FROM operator_waits
            WHERE issue_id = ?
            ORDER BY created_at DESC, run_id DESC
            """,
            (issue_id,),
        )
        review_state = await _fetch_one(
            conn,
            """
            SELECT iteration, last_trigger_signature, ci_fetch_failures,
                   pr_number, pr_url, github_repo, issue_label, codex_lgtm_comment_id
            FROM review_state
            WHERE issue_id = ?
            """,
            (issue_id,),
        )
        comment_events = await _fetch_all(
            conn,
            """
            SELECT comment_id, seen_at
            FROM comment_events
            WHERE issue_id = ?
            ORDER BY seen_at DESC, comment_id DESC
            LIMIT 50
            """,
            (issue_id,),
        )
        activity_comment_marks = await _fetch_all(
            conn,
            """
            SELECT m.run_id, m.first_unpublished_at, m.last_event_at,
                   m.event_count_since_post, m.last_posted_at, m.last_fingerprint
            FROM activity_comment_marks m
            JOIN runs r ON r.id = m.run_id
            WHERE r.issue_id = ?
            ORDER BY COALESCE(m.last_event_at, m.last_posted_at, '') DESC,
                     m.run_id DESC
            """,
            (issue_id,),
        )
        issue_cost_marks = await _fetch_one(
            conn,
            """
            SELECT warning_posted_at
            FROM issue_cost_marks
            WHERE issue_id = ?
            """,
            (issue_id,),
        )

        warnings = issue_warnings(
            canonical_status,
            latest_activity_age_secs=latest_activity_age_secs,
            pr_no_progress_threshold=pr_no_progress_threshold,
        )
        payload: dict[str, Any] = {
            "issue": issue,
            "canonical_status": canonical_status.to_dict(),
            "runs": runs,
            "issue_prs": issue_prs,
            "operator_waits": operator_waits,
            "review_state": review_state,
            "comment_events": comment_events,
            "activity_comment_marks": activity_comment_marks,
            "issue_cost_marks": issue_cost_marks,
        }
        if warnings:
            payload["warnings"] = warnings
            payload["latest_activity_ts"] = latest_activity_ts
            payload["latest_activity_age_secs"] = latest_activity_age_secs
        if external_payload is not None:
            payload["external_snapshot"] = external_payload
        return payload

    @router.get("/issues/{issue_id}/external")
    async def issue_external(
        issue_id: str,
        refresh: Annotated[int, Query(ge=0, le=1)] = 0,
    ) -> dict[str, Any]:
        if external_service is None:
            raise HTTPException(
                status_code=503,
                detail="External issue pull is not configured",
            )
        conn = await pool.connection()
        payload = await external_service.get_issue_external(
            conn,
            issue_id,
            refresh=bool(refresh),
        )
        if payload is None:
            raise HTTPException(status_code=404, detail="Issue not found")
        return payload

    @router.get("/issues/{issue_id}/observations")
    async def issue_observations(issue_id: str) -> list[dict[str, Any]]:
        conn = await pool.connection()
        issue = await _fetch_one(
            conn,
            "SELECT id FROM issues WHERE id = ?",
            (issue_id,),
        )
        if issue is None:
            raise HTTPException(status_code=404, detail="Issue not found")
        observations = await db.external_observations.list_recent_for_issue(
            conn,
            issue_id,
            limit=20,
        )
        return [
            {
                "id": observation.id,
                "issue_id": observation.issue_id,
                "source": observation.source,
                "observed_at": observation.observed_at,
                "payload_json": observation.payload_json,
                "drift_kind": observation.drift_kind,
                "action_taken": observation.action_taken,
            }
            for observation in observations
        ]

    @router.get("/issues/{issue_id}/timeline")
    async def issue_timeline(issue_id: str) -> list[dict[str, Any]]:
        conn = await pool.connection()
        issue = await _fetch_one(
            conn,
            "SELECT id FROM issues WHERE id = ?",
            (issue_id,),
        )
        if issue is None:
            raise HTTPException(status_code=404, detail="Issue not found")

        rows = await _fetch_all(
            conn,
            """
            SELECT *
            FROM (
                SELECT started_at AS ts, 'run_started' AS kind,
                       id AS run_id, stage, pid, NULL AS status, NULL AS cost_usd,
                       NULL AS github_repo, NULL AS pr_number, NULL AS pr_url,
                       NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind, NULL AS external_source,
                       NULL AS drift_kind, NULL AS action_taken
                FROM runs
                WHERE issue_id = ?

                UNION ALL

                SELECT ended_at AS ts, 'run_ended' AS kind,
                       id AS run_id, stage, NULL AS pid, status, cost_usd,
                       NULL AS github_repo, NULL AS pr_number, NULL AS pr_url,
                       NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind, NULL AS external_source,
                       NULL AS drift_kind, NULL AS action_taken
                FROM runs
                WHERE issue_id = ? AND ended_at IS NOT NULL

                UNION ALL

                SELECT created_at AS ts, 'pr_opened' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, github_repo, pr_number, pr_url,
                       NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind, NULL AS external_source,
                       NULL AS drift_kind, NULL AS action_taken
                FROM issue_prs
                WHERE issue_id = ?

                UNION ALL

                SELECT merged_at AS ts, 'pr_merged' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, github_repo, pr_number, NULL AS pr_url,
                       NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind, NULL AS external_source,
                       NULL AS drift_kind, NULL AS action_taken
                FROM issue_prs
                WHERE issue_id = ? AND merged_at IS NOT NULL

                UNION ALL

                SELECT seen_at AS ts, 'comment_seen' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind, NULL AS external_source,
                       NULL AS drift_kind, NULL AS action_taken
                FROM comment_events
                WHERE issue_id = ?

                UNION ALL

                SELECT m.last_posted_at AS ts, 'activity_comment_posted' AS kind,
                       m.run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, m.last_fingerprint AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind, NULL AS external_source,
                       NULL AS drift_kind, NULL AS action_taken
                FROM activity_comment_marks m
                JOIN runs r ON r.id = m.run_id
                WHERE r.issue_id = ? AND m.last_posted_at IS NOT NULL

                UNION ALL

                SELECT warning_posted_at AS ts, 'cost_warning_posted' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind, NULL AS external_source,
                       NULL AS drift_kind, NULL AS action_taken
                FROM issue_cost_marks
                WHERE issue_id = ? AND warning_posted_at IS NOT NULL

                UNION ALL

                SELECT ts, 'review_state_changed' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, NULL AS fingerprint,
                       field, old_value, new_value, NULL AS wait_kind,
                       NULL AS external_source, NULL AS drift_kind,
                       NULL AS action_taken
                FROM state_transitions
                WHERE issue_id = ? AND table_name = 'review_state'

                UNION ALL

                SELECT ts, 'operator_wait_ended' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       old_value AS wait_kind, NULL AS external_source,
                       NULL AS drift_kind, NULL AS action_taken
                FROM state_transitions
                WHERE issue_id = ?
                  AND table_name = 'operator_waits'
                  AND field = 'kind'
                  AND old_value IS NOT NULL

                UNION ALL

                SELECT ts, 'operator_wait_started' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       new_value AS wait_kind, NULL AS external_source,
                       NULL AS drift_kind, NULL AS action_taken
                FROM state_transitions
                WHERE issue_id = ?
                  AND table_name = 'operator_waits'
                  AND field = 'kind'
                  AND new_value IS NOT NULL

                UNION ALL

                SELECT observed_at AS ts,
                       CASE
                           WHEN action_taken = 'cleared' THEN 'external_cleared'
                           ELSE 'external_observed'
                       END AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind, source AS external_source,
                       drift_kind, action_taken
                FROM external_observations
                WHERE issue_id = ? AND action_taken != 'noted'

                UNION ALL

                SELECT ts, 'external_state_change' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, NULL AS fingerprint,
                       field, old_value, new_value, NULL AS wait_kind,
                       old_value AS external_source, NULL AS drift_kind,
                       NULL AS action_taken
                FROM state_transitions
                WHERE issue_id = ?
                  AND table_name = 'external_observations'
                  AND field = 'external_state_change'
            )
            ORDER BY ts ASC, kind ASC
            """,
            (
                issue_id,
                issue_id,
                issue_id,
                issue_id,
                issue_id,
                issue_id,
                issue_id,
                issue_id,
                issue_id,
                issue_id,
                issue_id,
                issue_id,
            ),
        )
        return [_timeline_event(row) for row in rows]

    return router


__all__ = ["create_issue_detail_router"]
