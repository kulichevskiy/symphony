"""Issue detail API for the read-only daemon UI."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import aiosqlite
from fastapi import APIRouter, HTTPException, Query

from .db import ReadOnlyDbPool
from .external import ExternalSnapshotService
from .status import DEFAULT_STUCK_THRESHOLDS, CanonicalState, compute_canonical_status


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
    else:
        raise ValueError(f"unknown timeline kind: {kind}")

    return {"ts": row["ts"], "kind": kind, "payload": payload}


def create_issue_detail_router(
    pool: ReadOnlyDbPool,
    *,
    external_service: ExternalSnapshotService | None = None,
    clock: Callable[[], datetime] | None = None,
    status_thresholds: Mapping[CanonicalState, timedelta] | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/api")
    thresholds = status_thresholds or DEFAULT_STUCK_THRESHOLDS

    def now() -> datetime:
        return clock() if clock is not None else datetime.now(UTC)

    @router.get("/issues/{issue_id}")
    async def issue_detail(issue_id: str) -> dict[str, Any]:
        conn = await pool.connection()
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
        canonical_status = await compute_canonical_status(
            conn,
            issue_id,
            now=now(),
            thresholds=thresholds,
        )

        runs = await _fetch_all(
            conn,
            """
            SELECT id, stage, status, pid, started_at, ended_at, cost_usd
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

        return {
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
                       NULL AS wait_kind
                FROM runs
                WHERE issue_id = ?

                UNION ALL

                SELECT ended_at AS ts, 'run_ended' AS kind,
                       id AS run_id, stage, NULL AS pid, status, cost_usd,
                       NULL AS github_repo, NULL AS pr_number, NULL AS pr_url,
                       NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind
                FROM runs
                WHERE issue_id = ? AND ended_at IS NOT NULL

                UNION ALL

                SELECT created_at AS ts, 'pr_opened' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, github_repo, pr_number, pr_url,
                       NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind
                FROM issue_prs
                WHERE issue_id = ?

                UNION ALL

                SELECT merged_at AS ts, 'pr_merged' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, github_repo, pr_number, NULL AS pr_url,
                       NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind
                FROM issue_prs
                WHERE issue_id = ? AND merged_at IS NOT NULL

                UNION ALL

                SELECT seen_at AS ts, 'comment_seen' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind
                FROM comment_events
                WHERE issue_id = ?

                UNION ALL

                SELECT m.last_posted_at AS ts, 'activity_comment_posted' AS kind,
                       m.run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, m.last_fingerprint AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind
                FROM activity_comment_marks m
                JOIN runs r ON r.id = m.run_id
                WHERE r.issue_id = ? AND m.last_posted_at IS NOT NULL

                UNION ALL

                SELECT warning_posted_at AS ts, 'cost_warning_posted' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       NULL AS wait_kind
                FROM issue_cost_marks
                WHERE issue_id = ? AND warning_posted_at IS NOT NULL

                UNION ALL

                SELECT ts, 'review_state_changed' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, NULL AS fingerprint,
                       field, old_value, new_value, NULL AS wait_kind
                FROM state_transitions
                WHERE issue_id = ? AND table_name = 'review_state'

                UNION ALL

                SELECT ts, 'operator_wait_ended' AS kind,
                       NULL AS run_id, NULL AS stage, NULL AS pid, NULL AS status,
                       NULL AS cost_usd, NULL AS github_repo, NULL AS pr_number,
                       NULL AS pr_url, NULL AS comment_id, NULL AS fingerprint,
                       NULL AS field, NULL AS old_value, NULL AS new_value,
                       old_value AS wait_kind
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
                       new_value AS wait_kind
                FROM state_transitions
                WHERE issue_id = ?
                  AND table_name = 'operator_waits'
                  AND field = 'kind'
                  AND new_value IS NOT NULL
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
            ),
        )
        return [_timeline_event(row) for row in rows]

    return router


__all__ = ["create_issue_detail_router"]
