"""HTTP API routes for the web UI."""

from __future__ import annotations

import aiosqlite
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .db import ReadOnlyDbPool


class IssueSummary(BaseModel):
    id: str
    identifier: str
    title: str
    team_key: str


def create_api_router(ui_db_pool: ReadOnlyDbPool | None = None) -> APIRouter:
    router = APIRouter(prefix="/api")

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
                ORDER BY
                    team_key ASC,
                    CASE
                        WHEN instr(identifier, '-') > 0
                             AND substr(identifier, instr(identifier, '-') + 1) GLOB '[0-9]*'
                        THEN CAST(substr(identifier, instr(identifier, '-') + 1) AS INTEGER)
                        ELSE NULL
                    END ASC,
                    identifier ASC
                """
            )
            rows = await cur.fetchall()
        except aiosqlite.Error as exc:
            raise HTTPException(
                status_code=503,
                detail="UI database is not available",
            ) from exc

        return [IssueSummary.model_validate(dict(row)) for row in rows]

    @router.api_route(
        "/{path:path}",
        methods=["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
    )
    async def api_placeholder(path: str) -> JSONResponse:
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    return router
