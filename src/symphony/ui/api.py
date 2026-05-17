"""HTTP API routes for the web UI."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta

import aiosqlite
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .db import ReadOnlyDbPool
from .status import (
    DEFAULT_STUCK_THRESHOLDS,
    CanonicalState,
    canonical_status_sort_key,
    compute_canonical_status,
)


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
    canonical_status: CanonicalStatusPayload


def _identifier_sort_key(identifier: str) -> tuple[str, int, str]:
    team, separator, suffix = identifier.partition("-")
    if separator and suffix.isdigit():
        return (team, int(suffix), identifier)
    return (identifier, 2**31 - 1, identifier)


def create_api_router(
    ui_db_pool: ReadOnlyDbPool | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
    status_thresholds: Mapping[CanonicalState, timedelta] | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/api")
    thresholds = status_thresholds or DEFAULT_STUCK_THRESHOLDS

    def now() -> datetime:
        return clock() if clock is not None else datetime.now(UTC)

    @router.get("/issues", response_model=list[IssueSummary])
    async def list_issues() -> list[IssueSummary]:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")

        try:
            conn = await ui_db_pool.connection()
            cur = await conn.execute(
                """
                SELECT id, identifier, title, team_key
                FROM issues
                """
            )
            rows = await cur.fetchall()
        except aiosqlite.Error as exc:
            raise HTTPException(
                status_code=503,
                detail="UI database is not available",
            ) from exc

        issues = [dict(row) for row in rows]
        statuses = [
            (
                issue,
                await compute_canonical_status(
                    conn,
                    str(issue["id"]),
                    now=now(),
                    thresholds=thresholds,
                ),
            )
            for issue in issues
        ]
        statuses.sort(
            key=lambda item: (
                *canonical_status_sort_key(item[1]),
                _identifier_sort_key(str(item[0]["identifier"])),
            )
        )
        return [
            IssueSummary.model_validate(
                {
                    **issue,
                    "canonical_status": status.to_dict(),
                }
            )
            for issue, status in statuses
        ]

    @router.api_route(
        "/{path:path}",
        methods=["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
    )
    async def api_placeholder(path: str) -> JSONResponse:
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    return router
