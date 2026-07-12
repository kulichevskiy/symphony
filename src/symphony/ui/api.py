"""HTTP API routes for the web UI."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, Protocol

import aiosqlite
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ..linear.slash import SlashKind
from ..tracker import DEFAULT_PROVIDER, DEFAULT_SITE
from .db import ReadOnlyDbPool
from .status import (
    DEFAULT_STUCK_THRESHOLDS,
    CanonicalState,
    canonical_status_sort_key,
    compute_canonical_statuses,
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
    # False for issues known only from the tracker's dispatch queue (Todo /
    # Waiting in Linear) — the daemon has no runs/PRs for them, so there is no
    # issue page to link to. Excluded from responses when True (the default).
    tracked: bool = True
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


class StageSpend(BaseModel):
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


class ModelRef(BaseModel):
    provider: str
    model: str


class SpendSummary(BaseModel):
    totals: SpendTotals
    per_team: list[TeamSpend]
    per_provider: list[ProviderSpend]
    # One row per distinct runs.stage in the filtered window, under the same
    # filters as per_team/per_provider so its grand total reconciles with them.
    per_stage: list[StageSpend]
    # Always-unscoped list of team keys from config, populating the Teams
    # filter popover. Never narrowed by an active filter.
    teams: list[str] = Field(default_factory=list)
    # Always-unscoped list of (provider, model) pairs distinct in
    # run_model_usage, populating the Models filter popover. Never narrowed.
    models: list[ModelRef] = Field(default_factory=list)


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


class StageSeriesBucket(BaseModel):
    # Bucket start day (`YYYY-MM-DD`): the calendar day for daily buckets, the
    # week's Monday for weekly ones. `output_tokens` maps stage -> summed output
    # for the bucket, carrying only non-zero stages (the client zero-fills).
    start: str
    output_tokens: dict[str, int]


class StageSeries(BaseModel):
    buckets: list[StageSeriesBucket]
    # "day" for short windows (<= ~6 weeks), "week" beyond.
    bucket: str
    # Distinct runs.stage values present in the window (incl. zero-output ones),
    # like per_stage. Pipeline ordering is applied client-side.
    stages: list[str]
    # Inclusive bucketing window; null only when unfiltered and no runs exist.
    start: str | None
    end: str | None


class CommandRequest(BaseModel):
    command: str


class CommandAccepted(BaseModel):
    status: str
    command_id: str
    command: str


class CommandSink(Protocol):
    """The orchestrator surface the web UI uses to submit operator commands."""

    def enqueue_web_command(self, issue_id: str, kind: SlashKind) -> str: ...


class PauseState(BaseModel):
    paused: bool


class PauseRequest(BaseModel):
    paused: bool


class PauseController(Protocol):
    """Daemon-level dispatch kill-switch surface for the web UI."""

    def is_dispatch_paused(self) -> bool: ...

    async def set_dispatch_paused(self, paused: bool) -> None: ...


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


# Day-string param shape (`YYYY-MM-DD`) for the `from`/`to` date-window filters.
DAY_PATTERN = r"^\d{4}-\d{2}-\d{2}$"


def _started_at_window(date_from: str | None, date_to: str | None) -> tuple[list[str], list[str]]:
    """SQL conditions + params windowing `runs.started_at` to a UTC-day range.
    Timestamps are stored as UTC ISO strings, so the first 10 chars are the
    calendar day and a lexicographic compare is date-correct."""
    conds: list[str] = []
    params: list[str] = []
    if date_from is not None:
        conds.append("substr(r.started_at, 1, 10) >= ?")
        params.append(date_from)
    if date_to is not None:
        conds.append("substr(r.started_at, 1, 10) <= ?")
        params.append(date_to)
    return conds, params


def _where(conds: list[str]) -> str:
    return f"WHERE {' AND '.join(conds)}" if conds else ""


def _parse_teams(value: str | None) -> list[str]:
    """Split a comma-joined `teams` param into a deduped, order-preserving list.

    Pass-through: unknown keys are kept verbatim so they reach the `IN (...)`
    clause and self-correct to an empty view rather than erroring."""
    if not value:
        return []
    seen: list[str] = []
    for raw in value.split(","):
        key = raw.strip()
        if key and key not in seen:
            seen.append(key)
    return seen


def _team_in_clause(teams: list[str]) -> str:
    return f"i.team_key IN ({','.join('?' for _ in teams)})"


def _parse_models(value: str | None) -> list[tuple[str, str]]:
    """Split a comma-joined `models` param into deduped, order-preserving
    (provider, model) pairs. Each token is provider-qualified `provider:model`.

    Pass-through: well-formed but unknown pairs are kept verbatim so they reach
    the `IN (...)` clause and self-correct to an empty view; tokens missing the
    `provider:model` shape are dropped."""
    if not value:
        return []
    seen: list[tuple[str, str]] = []
    for raw in value.split(","):
        provider, sep, model = raw.partition(":")
        provider = provider.strip()
        model = model.strip()
        if not sep or not provider or not model:
            continue
        pair = (provider, model)
        if pair not in seen:
            seen.append(pair)
    return seen


def _model_in_clause(models: list[tuple[str, str]]) -> str:
    """Row-value `IN (VALUES (?,?), …)` over (provider, model) pairs."""
    return f"(u.provider, u.model) IN (VALUES {','.join('(?,?)' for _ in models)})"


def _model_params(models: list[tuple[str, str]]) -> list[str]:
    return [value for pair in models for value in pair]


# Per-team spend, sourced from the run_model_usage child table so it carries a
# provider dimension. With no provider filter every provider's rows are summed,
# so rail = sum of teams = codex + claude reconciles exactly. A provider and/or
# `teams` filter AND together, plus an optional UTC-day window on run start day.
# Issue counts are DISTINCT issues touched by the (optionally scoped) usage.
def _spend_summary_query(
    provider: str | None,
    teams: list[str],
    models: list[tuple[str, str]],
    date_from: str | None,
    date_to: str | None,
) -> tuple[str, tuple[str, ...]]:
    conds: list[str] = []
    params: list[str] = []
    if provider is not None:
        conds.append("u.provider = ?")
        params.append(provider)
    if teams:
        conds.append(_team_in_clause(teams))
        params.extend(teams)
    if models:
        conds.append(_model_in_clause(models))
        params.extend(_model_params(models))
    wconds, wparams = _started_at_window(date_from, date_to)
    conds += wconds
    params += wparams
    return (
        f"""
        SELECT
            i.team_key AS team_key,
            COALESCE(SUM(u.input_tokens), 0) AS input_tokens,
            COALESCE(SUM(u.output_tokens), 0) AS output_tokens,
            COALESCE(SUM(u.cache_write_tokens), 0) AS cache_write_tokens,
            COALESCE(SUM(u.cache_read_tokens), 0) AS cache_read_tokens,
            COUNT(DISTINCT r.issue_id) AS issues
        FROM run_model_usage u
        JOIN runs r ON r.id = u.run_id
        JOIN issues i ON i.id = r.issue_id
        {_where(conds)}
        GROUP BY i.team_key
        """,
        tuple(params),
    )


# Token attribution by (provider, model), aggregated from the run_model_usage
# child table. Issue counts are DISTINCT per group so a model used across many
# issues counts each once. Joins `issues` only when scoping to teams; windowed
# by run start day like the per-team query.
def _spend_per_model_query(
    teams: list[str],
    models: list[tuple[str, str]],
    date_from: str | None,
    date_to: str | None,
) -> tuple[str, tuple[str, ...]]:
    join = "JOIN issues i ON i.id = r.issue_id" if teams else ""
    conds, params = _started_at_window(date_from, date_to)
    if teams:
        conds.append(_team_in_clause(teams))
        params.extend(teams)
    if models:
        conds.append(_model_in_clause(models))
        params.extend(_model_params(models))
    return (
        f"""
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
        {join}
        {_where(conds)}
        GROUP BY u.provider, u.model
        """,
        tuple(params),
    )


# Provider-level distinct issue counts, computed separately because summing
# the per-model issue counts would double-count issues spanning two models.
def _spend_per_provider_issues_query(
    teams: list[str],
    models: list[tuple[str, str]],
    date_from: str | None,
    date_to: str | None,
) -> tuple[str, tuple[str, ...]]:
    join = "JOIN issues i ON i.id = r.issue_id" if teams else ""
    conds, params = _started_at_window(date_from, date_to)
    if teams:
        conds.append(_team_in_clause(teams))
        params.extend(teams)
    if models:
        conds.append(_model_in_clause(models))
        params.extend(_model_params(models))
    return (
        f"""
        SELECT u.provider AS provider, COUNT(DISTINCT r.issue_id) AS issues
        FROM run_model_usage u
        JOIN runs r ON r.id = u.run_id
        {join}
        {_where(conds)}
        GROUP BY u.provider
        """,
        tuple(params),
    )


# Per-stage spend, grouped by runs.stage from the run_model_usage child table
# under the SAME provider/teams/models/date filters as _spend_summary_query, so
# per_stage reconciles to the same grand total as per_team / per_provider. The
# set of stages is whatever runs.stage values are present — no whitelist; a
# stage whose runs spent zero output still appears (e.g. review). Joins `issues`
# only when scoping to teams, like the per-model query.
def _spend_per_stage_query(
    provider: str | None,
    teams: list[str],
    models: list[tuple[str, str]],
    date_from: str | None,
    date_to: str | None,
) -> tuple[str, tuple[str, ...]]:
    join = "JOIN issues i ON i.id = r.issue_id" if teams else ""
    conds: list[str] = []
    params: list[str] = []
    if provider is not None:
        conds.append("u.provider = ?")
        params.append(provider)
    if teams:
        conds.append(_team_in_clause(teams))
        params.extend(teams)
    if models:
        conds.append(_model_in_clause(models))
        params.extend(_model_params(models))
    wconds, wparams = _started_at_window(date_from, date_to)
    conds += wconds
    params += wparams
    return (
        f"""
        SELECT
            r.stage AS stage,
            COALESCE(SUM(u.input_tokens), 0) AS input_tokens,
            COALESCE(SUM(u.output_tokens), 0) AS output_tokens,
            COALESCE(SUM(u.cache_write_tokens), 0) AS cache_write_tokens,
            COALESCE(SUM(u.cache_read_tokens), 0) AS cache_read_tokens,
            COUNT(DISTINCT r.issue_id) AS issues
        FROM run_model_usage u
        JOIN runs r ON r.id = u.run_id
        {join}
        {_where(conds)}
        GROUP BY r.stage
        """,
        tuple(params),
    )


# Always-unscoped distinct (provider, model) pairs from run_model_usage, used to
# populate the Models filter popover. Never narrowed by an active filter.
def _spend_models_query() -> str:
    return "SELECT DISTINCT provider, model FROM run_model_usage ORDER BY provider, model"


# Bucket spend by UTC day. Timestamps are stored as UTC ISO strings, so the
# first 10 chars are the calendar day and lexicographic compare is date-correct.
# With no provider the run-level token columns are summed; a provider filter
# switches the source to the run_model_usage child table so a run that spans
# providers contributes only its share. A `teams` filter ANDs onto either.
def _spend_heatmap_query(
    start: str, provider: str | None, teams: list[str], models: list[tuple[str, str]]
) -> tuple[str, tuple[str, ...]]:
    # A provider and/or models filter pulls tokens from the run_model_usage
    # child table so a run that spans providers/models contributes only its
    # share; otherwise the run-level columns are summed.
    use_usage = provider is not None or bool(models)
    token_alias = "u" if use_usage else "r"
    from_sql = (
        "FROM run_model_usage u\n        JOIN runs r ON r.id = u.run_id"
        if use_usage
        else "FROM runs r"
    )
    if teams:
        from_sql += "\n        JOIN issues i ON i.id = r.issue_id"
    cond = ["substr(r.started_at, 1, 10) >= ?"]
    params: list[str] = [start]
    if provider is not None:
        cond.append("u.provider = ?")
        params.append(provider)
    if teams:
        cond.append(_team_in_clause(teams))
        params.extend(teams)
    if models:
        cond.append(_model_in_clause(models))
        params.extend(_model_params(models))
    return (
        f"""
        SELECT
            substr(r.started_at, 1, 10) AS day,
            COALESCE(SUM({token_alias}.input_tokens), 0) AS input_tokens,
            COALESCE(SUM({token_alias}.output_tokens), 0) AS output_tokens,
            COALESCE(SUM({token_alias}.cache_write_tokens), 0) AS cache_write_tokens,
            COALESCE(SUM({token_alias}.cache_read_tokens), 0) AS cache_read_tokens,
            COUNT(DISTINCT r.issue_id) AS issues
        {from_sql}
        WHERE {" AND ".join(cond)}
        GROUP BY day
        ORDER BY day
        """,
        tuple(params),
    )


# The breakdown dimension a series is grouped by. The key column is selected
# AS `series_key` so the bucketing machinery stays dimension-agnostic: "stage"
# is runs.stage, "team" is the issue's team key, "model" is provider/model
# (matching the per-model rowKey the client builds for the totals table).
SERIES_DIMENSIONS: dict[str, str] = {
    "stage": "r.stage",
    "team": "i.team_key",
    "model": "u.provider || '/' || u.model",
}


# Per-dimension output tokens bucketed by UTC start day, sourced from the
# run_model_usage child table under the SAME provider/teams/models/date filters
# as the matching totals query so the trend reconciles with the totals view.
# Returns one row per (day, series_key); weekly rollup happens in Python.
def _spend_stage_series_query(
    provider: str | None,
    teams: list[str],
    models: list[tuple[str, str]],
    date_from: str | None,
    date_to: str | None,
    by: str = "stage",
) -> tuple[str, tuple[str, ...]]:
    key_expr = SERIES_DIMENSIONS[by]
    # The team key only exists on issues; join when grouping by team too.
    join = "JOIN issues i ON i.id = r.issue_id" if teams or by == "team" else ""
    conds: list[str] = []
    params: list[str] = []
    if provider is not None:
        conds.append("u.provider = ?")
        params.append(provider)
    if teams:
        conds.append(_team_in_clause(teams))
        params.extend(teams)
    if models:
        conds.append(_model_in_clause(models))
        params.extend(_model_params(models))
    wconds, wparams = _started_at_window(date_from, date_to)
    conds += wconds
    params += wparams
    return (
        f"""
        SELECT
            substr(r.started_at, 1, 10) AS day,
            {key_expr} AS stage,
            COALESCE(SUM(u.output_tokens), 0) AS output_tokens
        FROM run_model_usage u
        JOIN runs r ON r.id = u.run_id
        {join}
        {_where(conds)}
        GROUP BY day, stage
        ORDER BY day
        """,
        tuple(params),
    )


# Daily buckets while the window spans <= ~6 weeks (42-day span), weekly beyond.
_STAGE_SERIES_DAILY_MAX_SPAN_DAYS = 42


def _series_granularity(start: str, end: str) -> str:
    span = (date.fromisoformat(end) - date.fromisoformat(start)).days
    return "day" if span <= _STAGE_SERIES_DAILY_MAX_SPAN_DAYS else "week"


def _week_start(day: date) -> date:
    """The Monday of the ISO week containing `day` (matches the heatmap grid)."""
    return day - timedelta(days=day.weekday())


def _build_stage_series(
    rows: list[dict[str, Any]],
    date_from: str | None,
    date_to: str | None,
) -> StageSeries:
    """Roll per-(day, stage) output rows into a dense day/week-bucketed series.

    The window follows the active date filter; an open bound falls back to the
    earliest/latest observed run day so an unfiltered series spans all history.
    """
    observed = sorted({str(r["day"]) for r in rows})
    start = date_from or (observed[0] if observed else None)
    end = date_to or (observed[-1] if observed else None)
    if start is None or end is None or start > end:
        return StageSeries(buckets=[], bucket="day", stages=[], start=start, end=end)

    granularity = _series_granularity(start, end)

    def bucket_of(day_str: str) -> str:
        if granularity == "week":
            return _week_start(date.fromisoformat(day_str)).isoformat()
        return day_str

    stages: list[str] = []
    agg: dict[str, dict[str, int]] = {}
    for r in rows:
        stage = str(r["stage"])
        if stage not in stages:
            stages.append(stage)
        slot = agg.setdefault(bucket_of(str(r["day"])), {})
        slot[stage] = slot.get(stage, 0) + int(r["output_tokens"] or 0)
    stages.sort()

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    starts: list[str] = []
    if granularity == "week":
        cur, last = _week_start(start_d), _week_start(end_d)
        while cur <= last:
            starts.append(cur.isoformat())
            cur += timedelta(days=7)
    else:
        cur = start_d
        while cur <= end_d:
            starts.append(cur.isoformat())
            cur += timedelta(days=1)

    buckets = [
        StageSeriesBucket(
            start=s,
            output_tokens={k: v for k, v in agg.get(s, {}).items() if v},
        )
        for s in starts
    ]
    return StageSeries(buckets=buckets, bucket=granularity, stages=stages, start=start, end=end)


def _build_per_provider(
    model_rows: list[dict[str, Any]],
    provider_issue_rows: list[dict[str, Any]],
) -> list[ProviderSpend]:
    provider_issues = {str(row["provider"]): int(row["issues"] or 0) for row in provider_issue_rows}
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
    rows: list[dict[str, Any]],
    model_rows: list[dict[str, Any]] | None = None,
    provider_issue_rows: list[dict[str, Any]] | None = None,
    teams: list[str] | None = None,
    models: list[dict[str, Any]] | None = None,
    stage_rows: list[dict[str, Any]] | None = None,
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
    per_provider = _build_per_provider(model_rows or [], provider_issue_rows or [])
    per_stage = [
        StageSpend(
            key=str(r["stage"]),
            input_tokens=int(r["input_tokens"] or 0),
            output_tokens=int(r["output_tokens"] or 0),
            cache_write_tokens=int(r["cache_write_tokens"] or 0),
            cache_read_tokens=int(r["cache_read_tokens"] or 0),
            issues=int(r["issues"] or 0),
        )
        for r in (stage_rows or [])
    ]
    return SpendSummary(
        totals=totals,
        per_team=per_team,
        per_provider=per_provider,
        per_stage=per_stage,
        teams=teams or [],
        models=[
            ModelRef(provider=str(r["provider"]), model=str(r["model"])) for r in (models or [])
        ],
    )


def _queue_status_dict(row: Mapping[str, Any]) -> dict[str, str | int | None]:
    """Canonical-status payload for an issue sitting in the dispatch queue."""
    waiting = str(row["queue"]) == "waiting"
    blocked_by = str(row["blocked_by"] or "")
    subtitle = f"blocked by {blocked_by}" if waiting and blocked_by else str(row["state_name"])
    return {
        "state": "waiting" if waiting else "todo",
        "since": str(row["seen_at"]),
        "subtitle": subtitle,
        "stuck_for": None,
    }


def _queue_row_matches(
    row: Mapping[str, Any],
    *,
    q: str | None,
    teams: list[str] | None,
    date_from: str | None,
    date_to: str | None,
) -> bool:
    if teams and str(row["team_key"]) not in teams:
        return False
    # Mirror the tracked issues' latest-activity window: `seen_at` is when the
    # issue entered its current queue, so out-of-window rows stay out of
    # historical views.
    day = str(row["seen_at"])[:10]
    if date_from is not None and day < date_from:
        return False
    if date_to is not None and day > date_to:
        return False
    needle = (q or "").strip().lower()
    if (
        needle
        and needle not in str(row["identifier"]).lower()
        and needle not in str(row["title"]).lower()
    ):
        return False
    return True


def _queue_tracker_context(row: Mapping[str, Any]) -> tuple[str, str]:
    """(provider, site) from a queue row's binding scope (`repo#label#provider#site`)."""
    parts = str(row.get("scope") or "").rsplit("#", 2)
    if len(parts) == 3:
        return (parts[1], parts[2])
    return (DEFAULT_PROVIDER, DEFAULT_SITE)


def _queue_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    """Full tracker identity of a queue row: (provider, site, tracker issue id)."""
    provider, site = _queue_tracker_context(row)
    return (provider, site, str(row["issue_id"]))


def _dedupe_queue_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse multi-binding snapshots to one row per tracker issue.

    Two bindings on one team can both see the same queued issue; the board
    must show one card. Prefer the ready row (the daemon will dispatch it),
    then the earliest sighting. The key carries the tracker identity, so the
    same raw id from two different trackers never collapses.
    """
    best: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["team_key"]), *_queue_key(row))
        cur = best.get(key)
        if (
            cur is None
            or (str(cur["queue"]) == "waiting" and str(row["queue"]) == "ready")
            or (
                str(cur["queue"]) == str(row["queue"]) and str(row["seen_at"]) < str(cur["seen_at"])
            )
        ):
            best[key] = row
    return list(best.values())


async def _resolve_queue_storage_ids(
    conn: aiosqlite.Connection,
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str, str], str]:
    """Map queue rows' tracker identities to `issues` storage ids (if any)."""
    if not rows:
        return {}
    keys = {_queue_key(row) for row in rows}
    conds = " OR ".join("(provider = ? AND site = ? AND tracker_issue_id = ?)" for _ in keys)
    params = tuple(value for key in keys for value in key)
    cur = await conn.execute(
        f"SELECT id, provider, site, tracker_issue_id FROM issues WHERE {conds}",
        params,
    )
    return {
        (str(row["provider"]), str(row["site"]), str(row["tracker_issue_id"])): str(row["id"])
        for row in await cur.fetchall()
    }


async def _queue_ids_matching_usage(
    conn: aiosqlite.Connection,
    storage_ids: set[str],
    *,
    provider: str | None,
    models: list[tuple[str, str]],
) -> set[str]:
    """Storage ids (of queued issues) whose historical usage matches the
    active provider/model filters — mirrors `_list_issues_query` semantics."""
    if not storage_ids:
        return set()
    placeholders = ",".join("?" * len(storage_ids))
    conds = [f"r.issue_id IN ({placeholders})"]
    params: list[str] = list(storage_ids)
    if provider is not None:
        conds.append("u.provider = ?")
        params.append(provider)
    if models:
        conds.append(_model_in_clause(models))
        params.extend(_model_params(models))
    cur = await conn.execute(
        f"""
        SELECT DISTINCT r.issue_id
        FROM run_model_usage u
        JOIN runs r ON r.id = u.run_id
        WHERE {" AND ".join(conds)}
        """,
        tuple(params),
    )
    return {str(row["issue_id"]) for row in await cur.fetchall()}


def _list_issues_query(
    scope: IssueScope,
    q: str | None,
    *,
    now: datetime,
    date_from: str | None = None,
    date_to: str | None = None,
    provider: str | None = None,
    teams: list[str] | None = None,
    models: list[tuple[str, str]] | None = None,
) -> tuple[str, tuple[str, ...]]:
    models = models or []
    where: list[str] = []
    where_params: list[str] = []

    if scope is IssueScope.ACTIVE:
        where.append("i.id IN (SELECT issue_id FROM active_issue_ids)")
        # Date applies to active issues by their last activity day.
        if date_from is not None:
            where.append("substr(la.latest_activity_ts, 1, 10) >= ?")
            where_params.append(date_from)
        if date_to is not None:
            where.append("substr(la.latest_activity_ts, 1, 10) <= ?")
            where_params.append(date_to)
    elif scope is IssueScope.DONE:
        # Candidate prefilter: completion (latest PR merge, else last activity)
        # plausibly within the window. The precise "is this canonically done?"
        # check runs in Python on this small set.
        completion = "COALESCE(pr.max_merged_at, la.latest_activity_ts)"
        if date_from is not None:
            where.append(f"substr({completion}, 1, 10) >= ?")
            where_params.append(date_from)
        if date_to is not None:
            where.append(f"substr({completion}, 1, 10) <= ?")
            where_params.append(date_to)

    normalized_q = q.strip().lower() if q is not None else ""
    if normalized_q:
        where.append("(instr(lower(i.identifier), ?) > 0 OR instr(lower(i.title), ?) > 0)")
        where_params.extend([normalized_q, normalized_q])

    if teams:
        where.append(_team_in_clause(teams))
        where_params.extend(teams)

    # Token columns: when a provider and/or models filter is active, scope the
    # per-issue sums to the matching run_model_usage rows (provider AND the OR-ed
    # model pairs) and drop issues with no such usage. Otherwise sum every run.
    token_params: list[str] = []
    if provider is not None or models:
        usage_conds: list[str] = []
        if provider is not None:
            usage_conds.append("u.provider = ?")
            token_params.append(provider)
        if models:
            usage_conds.append(_model_in_clause(models))
            token_params.extend(_model_params(models))
        usage_where = " AND ".join(usage_conds)
        token_join = f"""
        LEFT JOIN (
            SELECT
                r.issue_id AS issue_id,
                SUM(u.input_tokens) AS input_tokens,
                SUM(u.output_tokens) AS output_tokens,
                SUM(u.cache_write_tokens) AS cache_write_tokens,
                SUM(u.cache_read_tokens) AS cache_read_tokens
            FROM run_model_usage u
            JOIN runs r ON r.id = u.run_id
            WHERE {usage_where}
            GROUP BY r.issue_id
        ) ru ON ru.issue_id = i.id
        """
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


class Meta(BaseModel):
    """Deployment-level info for the UI shell (not issue data)."""

    tunnel_url: str | None = None
    linear_webhook_url: str | None = None


def create_api_router(
    ui_db_pool: ReadOnlyDbPool | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
    status_thresholds: Mapping[CanonicalState, timedelta] | None = None,
    no_progress_threshold: timedelta | None = None,
    command_sink: CommandSink | None = None,
    pause_controller: PauseController | None = None,
    teams: list[str] | None = None,
    webhook_public_url: str | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/api")
    # Public origin the webhook receiver is reachable at (e.g. the dev
    # cloudflared quick-tunnel), surfaced in /meta so the UI can show the
    # paste-ready Linear webhook URL. None in normal/prod runs.
    webhook_base = (webhook_public_url or "").strip().rstrip("/") or None
    # Always-unscoped team keys from config, surfaced in /spend/summary to
    # populate the Teams filter popover. Sorted for a stable popover order.
    config_teams = sorted(teams or [])
    thresholds = status_thresholds or DEFAULT_STUCK_THRESHOLDS
    pr_no_progress_threshold = (
        DEFAULT_PR_NO_PROGRESS_THRESHOLD if no_progress_threshold is None else no_progress_threshold
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
        date_from: Annotated[str | None, Query(alias="from", pattern=DAY_PATTERN)] = None,
        date_to: Annotated[str | None, Query(alias="to", pattern=DAY_PATTERN)] = None,
        provider: Annotated[str | None, Query()] = None,
        teams: Annotated[str | None, Query()] = None,
        models: Annotated[str | None, Query()] = None,
    ) -> list[IssueSummary]:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")

        # "all" (and the omitted param) mean no provider scoping.
        provider_filter = None if provider in (None, "all") else provider
        team_filter = _parse_teams(teams)
        model_filter = _parse_models(models)
        is_done = scope is IssueScope.DONE
        try:
            conn = await ui_db_pool.connection()
            request_now = now()
            query, params = _list_issues_query(
                scope,
                q,
                now=request_now,
                date_from=date_from,
                date_to=date_to,
                provider=provider_filter,
                teams=team_filter,
                models=model_filter,
            )
            cur = await conn.execute(query, params)
            rows = await cur.fetchall()
            issues = [dict(row) for row in rows]
            qcur = await conn.execute(
                """
                SELECT team_key, scope, issue_id, identifier, title, queue,
                       state_name, blocked_by, seen_at
                FROM tracker_queue
                """
            )
            raw_queue = [dict(row) for row in await qcur.fetchall()]
            queue_rows: list[dict[str, Any]] = []
            if not is_done:
                queue_rows = _dedupe_queue_rows(
                    [
                        row
                        for row in raw_queue
                        if _queue_row_matches(
                            row, q=q, teams=team_filter, date_from=date_from, date_to=date_to
                        )
                    ]
                )
            # Queue issues the daemon has seen before (a requeued issue, or
            # dispatch paused) have an `issues` row and thus an issue page —
            # resolve their storage ids so they stay internal links. Keyed by
            # the full tracker identity (provider, site, tracker issue id)
            # carried in the snapshot's scope — display identifiers are not
            # unique across tracker providers/sites. The done scope resolves
            # every queued row so it can suppress issues that were requeued.
            resolve_rows = raw_queue if is_done else queue_rows
            queue_storage_ids = await _resolve_queue_storage_ids(conn, resolve_rows)
            queued_storage_id_set = set(queue_storage_ids.values())
            if not is_done and (provider_filter is not None or model_filter):
                # Usage filters: queue-only rows have no runs and drop out,
                # but a requeued issue with matching historical usage stays.
                matching = await _queue_ids_matching_usage(
                    conn,
                    queued_storage_id_set,
                    provider=provider_filter,
                    models=model_filter,
                )
                queue_rows = [
                    row for row in queue_rows if queue_storage_ids.get(_queue_key(row)) in matching
                ]
            queue_totals: dict[str, dict[str, Any]] = {}
            if queue_storage_ids and not is_done:
                # A requeued issue keeps its historical spend and activity —
                # don't report zeros just because it is back in the queue.
                placeholders = ",".join("?" * len(queue_storage_ids))
                tcur = await conn.execute(
                    f"""
                    SELECT issue_id,
                           SUM(input_tokens) AS input_tokens,
                           SUM(output_tokens) AS output_tokens,
                           SUM(cache_write_tokens) AS cache_write_tokens,
                           SUM(cache_read_tokens) AS cache_read_tokens,
                           MAX(COALESCE(ended_at, started_at)) AS latest_activity_ts
                    FROM runs
                    WHERE issue_id IN ({placeholders})
                    GROUP BY issue_id
                    """,
                    tuple(queue_storage_ids.values()),
                )
                queue_totals = {str(row["issue_id"]): dict(row) for row in await tcur.fetchall()}
            status_by_id = await compute_canonical_statuses(
                conn,
                [str(issue["id"]) for issue in issues],
                now=request_now,
                thresholds=thresholds,
            )
            statuses = [(issue, status_by_id[str(issue["id"])]) for issue in issues]
        except aiosqlite.Error as exc:
            raise HTTPException(
                status_code=503,
                detail="UI database is not available",
            ) from exc

        if is_done:
            # Keep only canonically-done issues whose completion lands inside the
            # window, newest first. The window is open-ended when a bound is
            # omitted (all-time done by default).
            kept: list[tuple[dict[str, Any], Any, str | None]] = []
            for issue, status in statuses:
                if status.state != "done":
                    continue
                # A completed issue that was requeued (back in Todo/Waiting)
                # shows in the active scope's queue lanes — not in Done too.
                if str(issue["id"]) in queued_storage_id_set:
                    continue
                completed_at = issue.get("max_merged_at") or issue.get("latest_activity_ts")
                if completed_at is None:
                    continue
                completed_day = str(completed_at)[:10]
                if date_from is not None and completed_day < date_from:
                    continue
                if date_to is not None and completed_day > date_to:
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

        queue_by_storage_id = {
            queue_storage_ids[_queue_key(row)]: row
            for row in queue_rows
            if _queue_key(row) in queue_storage_ids
        }
        payloads: list[IssueSummary] = []
        for issue, status, completed_at in triples:
            warnings = issue_warnings(
                status,
                latest_activity_age_secs=_optional_int(issue["latest_activity_age_secs"]),
                pr_no_progress_threshold=pr_no_progress_threshold,
            )
            payload: dict[str, object] = {
                **issue,
                "canonical_status": status.to_dict(),
            }
            # A tracked issue with no daemon state that sits in the dispatch
            # queue is really Todo/Waiting, not idle — reflect the queue.
            queue_row = queue_by_storage_id.get(str(issue["id"]))
            if queue_row is not None and status.state == CanonicalState.IDLE:
                payload["canonical_status"] = _queue_status_dict(queue_row)
            payload.pop("max_merged_at", None)
            if completed_at is not None:
                payload["completed_at"] = completed_at
            if warnings:
                payload["warnings"] = warnings
            payloads.append(IssueSummary.model_validate(payload))

        # Queue issues absent from the active scope: no live daemon state, so
        # they surface with their queue status to keep the Todo/Waiting lanes
        # honest. Ones with an `issues` row keep their storage id, issue page,
        # and historical totals; the rest are untracked and link out to the
        # tracker.
        active_ids = {str(issue["id"]) for issue, _ in statuses}
        extras = [
            row for row in queue_rows if queue_storage_ids.get(_queue_key(row)) not in active_ids
        ]
        extras.sort(key=lambda row: _identifier_sort_key(str(row["identifier"])))
        for row in extras:
            storage_id = queue_storage_ids.get(_queue_key(row))
            totals = queue_totals.get(storage_id or "", {})
            payloads.append(
                IssueSummary.model_validate(
                    {
                        "id": storage_id or str(row["issue_id"]),
                        "identifier": str(row["identifier"]),
                        "title": str(row["title"]),
                        "team_key": str(row["team_key"]),
                        "input_tokens": int(totals.get("input_tokens") or 0),
                        "output_tokens": int(totals.get("output_tokens") or 0),
                        "cache_write_tokens": int(totals.get("cache_write_tokens") or 0),
                        "cache_read_tokens": int(totals.get("cache_read_tokens") or 0),
                        "latest_activity_ts": totals.get("latest_activity_ts"),
                        "latest_activity_age_secs": None,
                        "canonical_status": _queue_status_dict(row),
                        "tracked": storage_id is not None,
                    }
                )
            )
        return payloads

    @router.get("/spend/summary", response_model=SpendSummary)
    async def spend_summary(
        provider: Annotated[str | None, Query()] = None,
        teams: Annotated[str | None, Query()] = None,
        models: Annotated[str | None, Query()] = None,
        date_from: Annotated[str | None, Query(alias="from", pattern=DAY_PATTERN)] = None,
        date_to: Annotated[str | None, Query(alias="to", pattern=DAY_PATTERN)] = None,
    ) -> SpendSummary:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")
        # "all" (and the omitted param) mean no provider scoping.
        provider_filter = None if provider in (None, "all") else provider
        team_filter = _parse_teams(teams)
        model_filter = _parse_models(models)
        team_query, team_params = _spend_summary_query(
            provider_filter, team_filter, model_filter, date_from, date_to
        )
        model_query, model_params = _spend_per_model_query(
            team_filter, model_filter, date_from, date_to
        )
        provider_issues_query, provider_issues_params = _spend_per_provider_issues_query(
            team_filter, model_filter, date_from, date_to
        )
        stage_query, stage_params = _spend_per_stage_query(
            provider_filter, team_filter, model_filter, date_from, date_to
        )
        models_query = _spend_models_query()
        try:
            conn = await ui_db_pool.connection()
            cur = await conn.execute(team_query, team_params)
            rows = [dict(row) for row in await cur.fetchall()]
            cur = await conn.execute(model_query, model_params)
            model_rows = [dict(row) for row in await cur.fetchall()]
            cur = await conn.execute(provider_issues_query, provider_issues_params)
            provider_issue_rows = [dict(row) for row in await cur.fetchall()]
            cur = await conn.execute(stage_query, stage_params)
            stage_rows = [dict(row) for row in await cur.fetchall()]
            cur = await conn.execute(models_query)
            available_models = [dict(row) for row in await cur.fetchall()]
        except aiosqlite.Error as exc:
            raise HTTPException(status_code=503, detail="UI database is not available") from exc
        return _build_spend_summary(
            rows,
            model_rows,
            provider_issue_rows,
            teams=config_teams,
            models=available_models,
            stage_rows=stage_rows,
        )

    @router.get("/spend/heatmap", response_model=SpendHeatmap)
    async def spend_heatmap(
        days: Annotated[int, Query(ge=1, le=400)] = 371,
        provider: Annotated[str | None, Query()] = None,
        teams: Annotated[str | None, Query()] = None,
        models: Annotated[str | None, Query()] = None,
    ) -> SpendHeatmap:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")
        request_now = now()
        start = (request_now - timedelta(days=days - 1)).date()
        provider_filter = None if provider in (None, "all") else provider
        team_filter = _parse_teams(teams)
        model_filter = _parse_models(models)
        query, params = _spend_heatmap_query(
            start.isoformat(), provider_filter, team_filter, model_filter
        )
        try:
            conn = await ui_db_pool.connection()
            cur = await conn.execute(query, params)
            rows = [dict(row) for row in await cur.fetchall()]
        except aiosqlite.Error as exc:
            raise HTTPException(status_code=503, detail="UI database is not available") from exc
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

    @router.get("/spend/stage-series", response_model=StageSeries)
    async def spend_stage_series(
        provider: Annotated[str | None, Query()] = None,
        teams: Annotated[str | None, Query()] = None,
        models: Annotated[str | None, Query()] = None,
        by: Annotated[str, Query(pattern="^(stage|team|model)$")] = "stage",
        date_from: Annotated[str | None, Query(alias="from", pattern=DAY_PATTERN)] = None,
        date_to: Annotated[str | None, Query(alias="to", pattern=DAY_PATTERN)] = None,
    ) -> StageSeries:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")
        # "all" (and the omitted param) mean no provider scoping.
        provider_filter = None if provider in (None, "all") else provider
        team_filter = _parse_teams(teams)
        model_filter = _parse_models(models)
        query, params = _spend_stage_series_query(
            provider_filter, team_filter, model_filter, date_from, date_to, by
        )
        try:
            conn = await ui_db_pool.connection()
            cur = await conn.execute(query, params)
            rows = [dict(row) for row in await cur.fetchall()]
        except aiosqlite.Error as exc:
            raise HTTPException(status_code=503, detail="UI database is not available") from exc
        return _build_stage_series(rows, date_from, date_to)

    @router.post("/issues/{issue_id}/command", response_model=CommandAccepted)
    async def issue_command(issue_id: str, body: CommandRequest) -> CommandAccepted:
        if command_sink is None:
            raise HTTPException(status_code=503, detail="commands are not available")
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")
        try:
            kind = SlashKind(body.command)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"unknown command: {body.command}") from exc
        try:
            conn = await ui_db_pool.connection()
            cur = await conn.execute("SELECT 1 FROM issues WHERE id = ?", (issue_id,))
            exists = await cur.fetchone() is not None
        except aiosqlite.Error as exc:
            raise HTTPException(status_code=503, detail="UI database is not available") from exc
        if not exists:
            raise HTTPException(status_code=404, detail="issue not found")
        command_id = command_sink.enqueue_web_command(issue_id, kind)
        return CommandAccepted(status="accepted", command_id=command_id, command=f"${kind.value}")

    @router.get("/pause", response_model=PauseState)
    async def get_pause() -> PauseState:
        if pause_controller is None:
            raise HTTPException(status_code=503, detail="pause control is not available")
        return PauseState(paused=pause_controller.is_dispatch_paused())

    @router.post("/pause", response_model=PauseState)
    async def set_pause(body: PauseRequest) -> PauseState:
        if pause_controller is None:
            raise HTTPException(status_code=503, detail="pause control is not available")
        await pause_controller.set_dispatch_paused(body.paused)
        return PauseState(paused=pause_controller.is_dispatch_paused())

    @router.get("/meta", response_model=Meta, response_model_exclude_defaults=True)
    async def meta() -> Meta:
        return Meta(
            tunnel_url=webhook_base,
            linear_webhook_url=(f"{webhook_base}/linear/webhook" if webhook_base else None),
        )

    @router.api_route(
        "/{path:path}",
        methods=["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
    )
    async def api_placeholder(path: str) -> JSONResponse:
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    return router
