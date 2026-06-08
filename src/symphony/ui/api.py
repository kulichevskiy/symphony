"""HTTP API routes for the web UI."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Protocol

import aiosqlite
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ..linear.slash import SlashKind
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
    input_tokens: int
    output_tokens: int
    cache_write_tokens: int
    cache_read_tokens: int
    latest_activity_ts: str | None
    latest_activity_age_secs: int | None
    canonical_status: CanonicalStatusPayload
    warnings: list[str] = Field(default_factory=list)
    # Set only on the `done` scope — the time the issue completed (latest PR
    # merge, else latest activity). Excluded from active/recent/all responses.
    completed_at: str | None = None


class IssueScope(StrEnum):
    ACTIVE = "active"
    DONE = "done"


class TeamSpend(BaseModel):
    key: str
    input_tokens: int
    output_tokens: int
    cache_write_tokens: int
    cache_read_tokens: int
    issues: int


class SpendTotals(BaseModel):
    input_tokens: int
    output_tokens: int
    cache_write_tokens: int
    cache_read_tokens: int
    issues: int


class ModelSpend(BaseModel):
    model: str
    input_tokens: int
    output_tokens: int
    cache_write_tokens: int
    cache_read_tokens: int
    issues: int


class ProviderSpend(BaseModel):
    provider: str
    input_tokens: int
    output_tokens: int
    cache_write_tokens: int
    cache_read_tokens: int
    issues: int
    per_model: list[ModelSpend]


class SpendSummary(BaseModel):
    totals: SpendTotals
    per_team: list[TeamSpend]
    per_provider: list[ProviderSpend]


class HeatmapDay(BaseModel):
    date: str
    input_tokens: int
    output_tokens: int
    cache_write_tokens: int
    cache_read_tokens: int
    issues: int


class SpendHeatmap(BaseModel):
    days: list[HeatmapDay]
    start: str
    end: str


class CommandRequest(BaseModel):
    command: str


class CommandAccepted(BaseModel):
    status: str
    command_id: str
    command: str


class CommandSink(Protocol):
    """The orchestrator surface the web UI uses to submit operator commands."""

    def enqueue_web_command(self, issue_id: str, kind: SlashKind) -> str: ...


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


def _parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


_SPEND_SUMMARY_QUERY = """
SELECT
    i.team_key AS team_key,
    COALESCE(SUM(r.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(r.output_tokens), 0) AS output_tokens,
    COALESCE(SUM(r.cache_write_tokens), 0) AS cache_write_tokens,
    COALESCE(SUM(r.cache_read_tokens), 0) AS cache_read_tokens,
    COUNT(DISTINCT r.issue_id) AS issues
FROM runs r
JOIN issues i ON i.id = r.issue_id
GROUP BY i.team_key
"""


# Token attribution by (provider, model), aggregated from the run_model_usage
# child table. Issue counts are DISTINCT per group so a model used across many
# issues counts each once.
_SPEND_PER_MODEL_QUERY = """
SELECT
    u.provider AS provider,
    u.model AS model,
    COALESCE(SUM(u.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(u.output_tokens), 0) AS output_tokens,
    COALESCE(SUM(u.cache_write_tokens), 0) AS cache_write_tokens,
    COALESCE(SUM(u.cache_read_tokens), 0) AS cache_read_tokens,
    COUNT(DISTINCT r.issue_id) AS issues
FROM run_model_usage u
JOIN runs r ON r.id = u.run_id
GROUP BY u.provider, u.model
"""


# Provider-level distinct issue counts, computed separately because summing
# the per-model issue counts would double-count issues spanning two models.
_SPEND_PER_PROVIDER_ISSUES_QUERY = """
SELECT u.provider AS provider, COUNT(DISTINCT r.issue_id) AS issues
FROM run_model_usage u
JOIN runs r ON r.id = u.run_id
GROUP BY u.provider
"""


# Bucket spend by UTC day. Timestamps are stored as UTC ISO strings, so the
# first 10 chars are the calendar day and lexicographic compare is date-correct.
_SPEND_HEATMAP_QUERY = """
SELECT
    substr(r.started_at, 1, 10) AS day,
    COALESCE(SUM(r.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(r.output_tokens), 0) AS output_tokens,
    COALESCE(SUM(r.cache_write_tokens), 0) AS cache_write_tokens,
    COALESCE(SUM(r.cache_read_tokens), 0) AS cache_read_tokens,
    COUNT(DISTINCT r.issue_id) AS issues
FROM runs r
WHERE substr(r.started_at, 1, 10) >= ?
GROUP BY day
ORDER BY day
"""


# Same daily buckets, but scoped to a single provider via the run_model_usage
# child table. Tokens come from the per-(provider, model) rows so a run that
# spans providers contributes only its share of the selected provider.
_SPEND_HEATMAP_BY_PROVIDER_QUERY = """
SELECT
    substr(r.started_at, 1, 10) AS day,
    COALESCE(SUM(u.input_tokens), 0) AS input_tokens,
    COALESCE(SUM(u.output_tokens), 0) AS output_tokens,
    COALESCE(SUM(u.cache_write_tokens), 0) AS cache_write_tokens,
    COALESCE(SUM(u.cache_read_tokens), 0) AS cache_read_tokens,
    COUNT(DISTINCT r.issue_id) AS issues
FROM run_model_usage u
JOIN runs r ON r.id = u.run_id
WHERE substr(r.started_at, 1, 10) >= ? AND u.provider = ?
GROUP BY day
ORDER BY day
"""


def _build_per_provider(
    model_rows: list[dict[str, object]],
    provider_issue_rows: list[dict[str, object]],
) -> list[ProviderSpend]:
    provider_issues = {
        str(row["provider"]): int(row["issues"] or 0) for row in provider_issue_rows
    }
    models_by_provider: dict[str, list[ModelSpend]] = {}
    acc_by_provider: dict[str, dict[str, int]] = {}
    for row in model_rows:
        provider = str(row["provider"])
        inp = int(row["input_tokens"] or 0)
        out = int(row["output_tokens"] or 0)
        cw = int(row["cache_write_tokens"] or 0)
        cr = int(row["cache_read_tokens"] or 0)
        models_by_provider.setdefault(provider, []).append(
            ModelSpend(
                model=str(row["model"]),
                input_tokens=inp,
                output_tokens=out,
                cache_write_tokens=cw,
                cache_read_tokens=cr,
                issues=int(row["issues"] or 0),
            )
        )
        acc = acc_by_provider.setdefault(
            provider,
            {
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_write_tokens": 0,
                "cache_read_tokens": 0,
            },
        )
        acc["input_tokens"] += inp
        acc["output_tokens"] += out
        acc["cache_write_tokens"] += cw
        acc["cache_read_tokens"] += cr

    per_provider: list[ProviderSpend] = []
    for provider, acc in acc_by_provider.items():
        models = sorted(
            models_by_provider[provider],
            key=lambda m: m.output_tokens,
            reverse=True,
        )
        per_provider.append(
            ProviderSpend(
                provider=provider,
                input_tokens=acc["input_tokens"],
                output_tokens=acc["output_tokens"],
                cache_write_tokens=acc["cache_write_tokens"],
                cache_read_tokens=acc["cache_read_tokens"],
                issues=provider_issues.get(provider, 0),
                per_model=models,
            )
        )
    per_provider.sort(key=lambda p: p.output_tokens, reverse=True)
    return per_provider


def _build_spend_summary(
    rows: list[dict[str, object]],
    model_rows: list[dict[str, object]] | None = None,
    provider_issue_rows: list[dict[str, object]] | None = None,
) -> SpendSummary:
    per_team: list[TeamSpend] = []
    acc = {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_write_tokens": 0,
        "cache_read_tokens": 0,
        "issues": 0,
    }
    for row in rows:
        inp = int(row["input_tokens"] or 0)
        out = int(row["output_tokens"] or 0)
        cw = int(row["cache_write_tokens"] or 0)
        cr = int(row["cache_read_tokens"] or 0)
        issues = int(row["issues"] or 0)
        per_team.append(
            TeamSpend(
                key=str(row["team_key"]),
                input_tokens=inp,
                output_tokens=out,
                cache_write_tokens=cw,
                cache_read_tokens=cr,
                issues=issues,
            )
        )
        acc["input_tokens"] += inp
        acc["output_tokens"] += out
        acc["cache_write_tokens"] += cw
        acc["cache_read_tokens"] += cr
        acc["issues"] += issues
    per_team.sort(key=lambda t: t.output_tokens, reverse=True)
    totals = SpendTotals(
        input_tokens=acc["input_tokens"],
        output_tokens=acc["output_tokens"],
        cache_write_tokens=acc["cache_write_tokens"],
        cache_read_tokens=acc["cache_read_tokens"],
        issues=acc["issues"],
    )
    per_provider = _build_per_provider(
        model_rows or [], provider_issue_rows or []
    )
    return SpendSummary(
        totals=totals, per_team=per_team, per_provider=per_provider
    )


def _list_issues_query(
    scope: IssueScope,
    q: str | None,
    *,
    now: datetime,
    cutoff: datetime | None = None,
    provider: str | None = None,
) -> tuple[str, tuple[str, ...]]:
    where: list[str] = []
    where_params: list[str] = []

    if scope is IssueScope.ACTIVE:
        where.append("i.id IN (SELECT issue_id FROM active_issue_ids)")
    elif scope is IssueScope.DONE:
        # Candidate prefilter: completion plausibly within the window — either a
        # PR merged since the cutoff, or activity since the cutoff. The precise
        # "is this canonically done?" check runs in Python on this small set.
        cutoff_iso = _utc_iso(cutoff if cutoff is not None else now)
        where.append(
            "((pr.max_merged_at IS NOT NULL AND pr.max_merged_at >= ?) "
            "OR (la.latest_activity_ts IS NOT NULL AND la.latest_activity_ts >= ?))"
        )
        where_params.extend([cutoff_iso, cutoff_iso])

    normalized_q = q.strip().lower() if q is not None else ""
    if normalized_q:
        where.append(
            "(instr(lower(i.identifier), ?) > 0 OR instr(lower(i.title), ?) > 0)"
        )
        where_params.extend([normalized_q, normalized_q])

    # Token columns: when a provider is selected, scope the per-issue sums to that
    # provider's rows in the run_model_usage child table (joined to runs) and drop
    # issues with no usage for it. Otherwise sum every run, all providers.
    token_params: list[str] = []
    if provider is not None:
        token_join = """
        LEFT JOIN (
            SELECT
                r.issue_id AS issue_id,
                SUM(u.input_tokens) AS input_tokens,
                SUM(u.output_tokens) AS output_tokens,
                SUM(u.cache_write_tokens) AS cache_write_tokens,
                SUM(u.cache_read_tokens) AS cache_read_tokens
            FROM run_model_usage u
            JOIN runs r ON r.id = u.run_id
            WHERE u.provider = ?
            GROUP BY r.issue_id
        ) ru ON ru.issue_id = i.id
        """
        token_params.append(provider)
        where.append("ru.issue_id IS NOT NULL")
    else:
        token_join = """
        LEFT JOIN (
            SELECT
                issue_id,
                SUM(input_tokens) AS input_tokens,
                SUM(output_tokens) AS output_tokens,
                SUM(cache_write_tokens) AS cache_write_tokens,
                SUM(cache_read_tokens) AS cache_read_tokens
            FROM runs
            GROUP BY issue_id
        ) ru ON ru.issue_id = i.id
        """

    params = [_utc_iso(now), *token_params, *where_params]
    where_sql = "" if not where else f"WHERE {' AND '.join(where)}"
    return (
        f"""
        {_ISSUE_SCOPE_CTES}
        SELECT
            i.id,
            i.identifier,
            i.title,
            i.team_key,
            COALESCE(ru.input_tokens, 0) AS input_tokens,
            COALESCE(ru.output_tokens, 0) AS output_tokens,
            COALESCE(ru.cache_write_tokens, 0) AS cache_write_tokens,
            COALESCE(ru.cache_read_tokens, 0) AS cache_read_tokens,
            la.latest_activity_ts,
            pr.max_merged_at AS max_merged_at,
            CASE
                WHEN la.latest_activity_ts IS NULL THEN NULL
                ELSE CAST(
                    MAX(0, strftime('%s', ?) - strftime('%s', la.latest_activity_ts))
                    AS INTEGER
                )
            END AS latest_activity_age_secs
        FROM issues i
        LEFT JOIN latest_activity la ON la.issue_id = i.id
        LEFT JOIN (
            SELECT issue_id, MAX(merged_at) AS max_merged_at
            FROM issue_prs
            GROUP BY issue_id
        ) pr ON pr.issue_id = i.id
        {token_join}
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
    command_sink: CommandSink | None = None,
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
        within_secs: Annotated[int, Query(ge=1)] = 7 * 86_400,
        provider: Annotated[str | None, Query()] = None,
    ) -> list[IssueSummary]:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")

        # "all" (and the omitted param) mean no provider scoping.
        provider_filter = None if provider in (None, "all") else provider
        is_done = scope is IssueScope.DONE
        try:
            conn = await ui_db_pool.connection()
            request_now = now()
            cutoff = (
                request_now - timedelta(seconds=within_secs) if is_done else None
            )
            query, params = _list_issues_query(
                scope, q, now=request_now, cutoff=cutoff, provider=provider_filter
            )
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

        if is_done:
            # Keep only canonically-done issues whose completion lands inside the
            # window, newest first.
            kept: list[tuple[dict[str, object], object, str]] = []
            for issue, status in statuses:
                if status.state != "done":
                    continue
                completed_at = issue.get("max_merged_at") or issue.get(
                    "latest_activity_ts"
                )
                completed_dt = _parse_iso(completed_at)
                if (
                    completed_dt is None
                    or cutoff is None
                    or completed_dt < cutoff
                ):
                    continue
                kept.append((issue, status, str(completed_at)))
            kept.sort(
                key=lambda item: (item[2], _identifier_sort_key(str(item[0]["identifier"]))),
                reverse=True,
            )
            triples = kept
        else:
            statuses.sort(
                key=lambda item: (
                    *canonical_status_sort_key(item[1]),
                    _identifier_sort_key(str(item[0]["identifier"])),
                )
            )
            triples = [(issue, status, None) for issue, status in statuses]

        payloads: list[IssueSummary] = []
        for issue, status, completed_at in triples:
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
            payload.pop("max_merged_at", None)
            if completed_at is not None:
                payload["completed_at"] = completed_at
            if warnings:
                payload["warnings"] = warnings
            payloads.append(IssueSummary.model_validate(payload))
        return payloads

    @router.get("/spend/summary", response_model=SpendSummary)
    async def spend_summary() -> SpendSummary:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")
        try:
            conn = await ui_db_pool.connection()
            cur = await conn.execute(_SPEND_SUMMARY_QUERY)
            rows = [dict(row) for row in await cur.fetchall()]
            cur = await conn.execute(_SPEND_PER_MODEL_QUERY)
            model_rows = [dict(row) for row in await cur.fetchall()]
            cur = await conn.execute(_SPEND_PER_PROVIDER_ISSUES_QUERY)
            provider_issue_rows = [dict(row) for row in await cur.fetchall()]
        except aiosqlite.Error as exc:
            raise HTTPException(
                status_code=503, detail="UI database is not available"
            ) from exc
        return _build_spend_summary(rows, model_rows, provider_issue_rows)

    @router.get("/spend/heatmap", response_model=SpendHeatmap)
    async def spend_heatmap(
        days: Annotated[int, Query(ge=1, le=400)] = 371,
        provider: Annotated[str | None, Query()] = None,
    ) -> SpendHeatmap:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")
        request_now = now()
        start = (request_now - timedelta(days=days - 1)).date()
        if provider is None:
            query, params = _SPEND_HEATMAP_QUERY, (start.isoformat(),)
        else:
            query = _SPEND_HEATMAP_BY_PROVIDER_QUERY
            params = (start.isoformat(), provider)
        try:
            conn = await ui_db_pool.connection()
            cur = await conn.execute(query, params)
            rows = [dict(row) for row in await cur.fetchall()]
        except aiosqlite.Error as exc:
            raise HTTPException(
                status_code=503, detail="UI database is not available"
            ) from exc
        return SpendHeatmap(
            days=[
                HeatmapDay(
                    date=str(r["day"]),
                    input_tokens=int(r["input_tokens"] or 0),
                    output_tokens=int(r["output_tokens"] or 0),
                    cache_write_tokens=int(r["cache_write_tokens"] or 0),
                    cache_read_tokens=int(r["cache_read_tokens"] or 0),
                    issues=int(r["issues"] or 0),
                )
                for r in rows
            ],
            start=start.isoformat(),
            end=request_now.date().isoformat(),
        )

    @router.post("/issues/{issue_id}/command", response_model=CommandAccepted)
    async def issue_command(issue_id: str, body: CommandRequest) -> CommandAccepted:
        if command_sink is None:
            raise HTTPException(
                status_code=503, detail="commands are not available"
            )
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")
        try:
            kind = SlashKind(body.command)
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=f"unknown command: {body.command}"
            ) from exc
        try:
            conn = await ui_db_pool.connection()
            cur = await conn.execute(
                "SELECT 1 FROM issues WHERE id = ?", (issue_id,)
            )
            exists = await cur.fetchone() is not None
        except aiosqlite.Error as exc:
            raise HTTPException(
                status_code=503, detail="UI database is not available"
            ) from exc
        if not exists:
            raise HTTPException(status_code=404, detail="issue not found")
        command_id = command_sink.enqueue_web_command(issue_id, kind)
        return CommandAccepted(
            status="accepted", command_id=command_id, command=f"${kind.value}"
        )

    @router.api_route(
        "/{path:path}",
        methods=["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
    )
    async def api_placeholder(path: str) -> JSONResponse:
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    return router
