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

from ..db.issues import contextual_id
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


_ACTIVE_ISSUE_IDS_CTE = """active_issue_ids(issue_id) AS (
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
)"""


# latest_activity, with every UNION ALL source narrowed to `activity_candidates`
# (the issues that can appear in the response) so MAX(ts) never scans history
# belonging to issues outside that set. Mirrors issues.py::_latest_activity,
# whose per-issue variant already restricts each branch to a single issue.
_LATEST_ACTIVITY_CTE = """latest_activity_sources(issue_id, ts) AS (
    SELECT issue_id, COALESCE(ended_at, started_at) FROM runs
    WHERE issue_id IN (SELECT issue_id FROM activity_candidates)
    UNION ALL
    SELECT issue_id, ts FROM state_transitions
    WHERE issue_id IN (SELECT issue_id FROM activity_candidates)
    UNION ALL
    SELECT issue_id, seen_at FROM comment_events
    WHERE issue_id IN (SELECT issue_id FROM activity_candidates)
    UNION ALL
    SELECT r.issue_id, m.last_event_at
    FROM activity_comment_marks m
    JOIN runs r ON r.id = m.run_id
    WHERE m.last_event_at IS NOT NULL
      AND r.issue_id IN (SELECT issue_id FROM activity_candidates)
    UNION ALL
    SELECT issue_id, COALESCE(merged_at, created_at) FROM issue_prs
    WHERE issue_id IN (SELECT issue_id FROM activity_candidates)
    UNION ALL
    SELECT issue_id, created_at FROM operator_waits
    WHERE issue_id IN (SELECT issue_id FROM activity_candidates)
),
latest_activity(issue_id, latest_activity_ts) AS (
    SELECT issue_id, MAX(ts)
    FROM latest_activity_sources
    WHERE ts IS NOT NULL
    GROUP BY issue_id
)"""


def _issue_scope_ctes(candidate_sql: str) -> str:
    """WITH block for the issue list. `candidate_sql` defines the
    `activity_candidates(issue_id)` set that the latest_activity sources are
    restricted to: the active issue ids for the active scope, or the
    activity-independent prefilter (team/q) of issues for the done scope."""
    return f"""
WITH {_ACTIVE_ISSUE_IDS_CTE},
activity_candidates(issue_id) AS (
    {candidate_sql}
),
{_LATEST_ACTIVITY_CTE}
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

# Safety ceiling on the done scope of /api/issues when no `limit` is given.
# The primary bound is now the 24h time window (DONE_SCOPE_DEFAULT_WINDOW); this
# count only guards against an exceptionally busy window returning an unbounded
# list. The newest N is enough for the dashboard's Done lane + table.
DONE_SCOPE_DEFAULT_LIMIT = 50

# Default rolling window for the done scope when the request gives no explicit
# lower bound (`from`): only issues completed within the last 24h are listed, so
# the dashboard's Done section stays a snapshot of "what we just finished".
# Explicit `from`/`to` override this (the seam the future archive page uses).
DONE_SCOPE_DEFAULT_WINDOW = timedelta(hours=24)


def _started_at_window(date_from: str | None, date_to: str | None) -> tuple[list[str], list[str]]:
    """SQL conditions + params windowing `runs.started_at` to a UTC-day range.
    Timestamps are stored as UTC ISO strings, so a plain lexicographic range on
    the raw column is date-correct AND sargable (an index can serve it): the
    lower bound compares against the day, the upper bound against the day after
    so any intra-day time on `date_to` is still included."""
    conds: list[str] = []
    params: list[str] = []
    if date_from is not None:
        conds.append("r.started_at >= ?")
        params.append(date_from)
    if date_to is not None:
        conds.append("r.started_at < date(?, '+1 day')")
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
    cond = ["r.started_at >= ?"]
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


def _queue_fallback_id(row: Mapping[str, Any]) -> str:
    """Response id for a queue row with no `issues` storage row.

    Default-tracker rows keep the raw tracker id (what `issues.upsert` would
    assign); non-default trackers get the same contextual id the storage
    layer would use, so two trackers sharing a raw id never emit duplicate
    React keys."""
    provider, site = _queue_tracker_context(row)
    if (provider, site) == (DEFAULT_PROVIDER, DEFAULT_SITE):
        return str(row["issue_id"])
    return contextual_id(id=str(row["issue_id"]), provider=provider, site=site)


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


# Chunk size for queue-derived id lists. Keeps every generated statement well
# under SQLite's bound-parameter and expression-depth limits, so a huge Todo
# lane can never 503 the issues endpoint.
_QUEUE_SQL_CHUNK = 300


async def _resolve_queue_storage_ids(
    conn: aiosqlite.Connection,
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str, str], str]:
    """Map queue rows' tracker identities to `issues` storage ids (if any)."""
    keys = sorted({_queue_key(row) for row in rows})
    result: dict[tuple[str, str, str], str] = {}
    for start in range(0, len(keys), _QUEUE_SQL_CHUNK):
        chunk = keys[start : start + _QUEUE_SQL_CHUNK]
        values = ",".join("(?, ?, ?)" for _ in chunk)
        cur = await conn.execute(
            "SELECT id, provider, site, tracker_issue_id FROM issues "
            f"WHERE (provider, site, tracker_issue_id) IN (VALUES {values})",
            tuple(value for key in chunk for value in key),
        )
        for row in await cur.fetchall():
            key = (str(row["provider"]), str(row["site"]), str(row["tracker_issue_id"]))
            result[key] = str(row["id"])
    return result


async def _queue_ids_matching_usage(
    conn: aiosqlite.Connection,
    storage_ids: set[str],
    *,
    provider: str | None,
    models: list[tuple[str, str]],
) -> set[str]:
    """Storage ids (of queued issues) whose historical usage matches the
    active provider/model filters — mirrors `_list_issues_query` semantics."""
    ordered = sorted(storage_ids)
    matching: set[str] = set()
    for start in range(0, len(ordered), _QUEUE_SQL_CHUNK):
        chunk = ordered[start : start + _QUEUE_SQL_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        conds = [f"r.issue_id IN ({placeholders})"]
        params: list[str] = list(chunk)
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
        matching.update(str(row["issue_id"]) for row in await cur.fetchall())
    return matching


async def _queue_issue_totals(
    conn: aiosqlite.Connection,
    storage_ids: set[str],
    *,
    provider: str | None,
    models: list[tuple[str, str]],
) -> dict[str, dict[str, Any]]:
    """Historical token totals + latest activity for requeued issues.

    Unfiltered requests sum whole runs; with a provider/model filter active
    the totals come from the matching `run_model_usage` slice instead, so a
    Todo card reconciles with the filtered tracked rows and spend views.
    """
    ordered = sorted(storage_ids)
    filtered = provider is not None or bool(models)
    totals: dict[str, dict[str, Any]] = {}
    for start in range(0, len(ordered), _QUEUE_SQL_CHUNK):
        chunk = ordered[start : start + _QUEUE_SQL_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        if not filtered:
            query = f"""
                SELECT issue_id,
                       SUM(input_tokens) AS input_tokens,
                       SUM(output_tokens) AS output_tokens,
                       SUM(cache_write_tokens) AS cache_write_tokens,
                       SUM(cache_read_tokens) AS cache_read_tokens,
                       MAX(COALESCE(ended_at, started_at)) AS latest_activity_ts
                FROM runs
                WHERE issue_id IN ({placeholders})
                GROUP BY issue_id
            """
            params: list[str] = list(chunk)
        else:
            conds = [f"r.issue_id IN ({placeholders})"]
            params = list(chunk)
            if provider is not None:
                conds.append("u.provider = ?")
                params.append(provider)
            if models:
                conds.append(_model_in_clause(models))
                params.extend(_model_params(models))
            query = f"""
                SELECT r.issue_id AS issue_id,
                       SUM(u.input_tokens) AS input_tokens,
                       SUM(u.output_tokens) AS output_tokens,
                       SUM(u.cache_write_tokens) AS cache_write_tokens,
                       SUM(u.cache_read_tokens) AS cache_read_tokens,
                       MAX(COALESCE(r.ended_at, r.started_at)) AS latest_activity_ts
                FROM run_model_usage u
                JOIN runs r ON r.id = u.run_id
                WHERE {" AND ".join(conds)}
                GROUP BY r.issue_id
            """
        cur = await conn.execute(query, tuple(params))
        for row in await cur.fetchall():
            totals[str(row["issue_id"])] = dict(row)
    return totals


def _list_issues_query(
    scope: IssueScope,
    q: str | None,
    *,
    now: datetime,
    date_from: str | None = None,
    date_to: str | None = None,
    done_since: str | None = None,
    provider: str | None = None,
    teams: list[str] | None = None,
    models: list[tuple[str, str]] | None = None,
) -> tuple[str, tuple[str, ...]]:
    models = models or []
    where: list[str] = []
    where_params: list[str] = []

    # Activity-independent filters (team, q) reused both in the WHERE clause and,
    # for the done scope, in the activity_candidates prefilter.
    normalized_q = q.strip().lower() if q is not None else ""
    q_cond: str | None = None
    q_params: list[str] = []
    if normalized_q:
        q_cond = "(instr(lower(i.identifier), ?) > 0 OR instr(lower(i.title), ?) > 0)"
        q_params = [normalized_q, normalized_q]
    team_cond: str | None = None
    team_params: list[str] = []
    if teams:
        team_cond = _team_in_clause(teams)
        team_params = list(teams)

    if scope is IssueScope.ACTIVE:
        where.append("i.id IN (SELECT issue_id FROM active_issue_ids)")
        # Date applies to active issues by their last activity day. Plain
        # lexicographic range on the ISO-UTC timestamp (sargable, day-correct).
        if date_from is not None:
            where.append("la.latest_activity_ts >= ?")
            where_params.append(date_from)
        if date_to is not None:
            where.append("la.latest_activity_ts < date(?, '+1 day')")
            where_params.append(date_to)
    elif scope is IssueScope.DONE:
        # Candidate prefilter: completion (latest PR merge, else last activity)
        # plausibly within the window. The precise "is this canonically done?"
        # check runs in Python on this small set.
        completion = "COALESCE(pr.max_merged_at, la.latest_activity_ts)"
        if date_from is not None:
            # Explicit lower bound (day string) overrides the 24h default.
            where.append(f"{completion} >= ?")
            where_params.append(date_from)
        elif done_since is not None:
            # Rolling now-24h default: an ISO-timestamp lower bound (sub-day),
            # so the Done section reflects only what was just finished.
            where.append(f"{completion} >= ?")
            where_params.append(done_since)
        if date_to is not None:
            where.append(f"{completion} < date(?, '+1 day')")
            where_params.append(date_to)

    if q_cond is not None:
        where.append(q_cond)
        where_params.extend(q_params)
    if team_cond is not None:
        where.append(team_cond)
        where_params.extend(team_params)

    # Scope the activity aggregate to only the issues that can appear in the
    # response. Active: the active issue ids. Done: the completion window still
    # needs latest_activity (chicken-and-egg), so narrow by the
    # activity-independent filters — a superset of the response's done set.
    if scope is IssueScope.ACTIVE:
        candidate_sql = "SELECT issue_id FROM active_issue_ids"
        candidate_params: list[str] = []
    else:
        cand_conds = [c for c in (q_cond, team_cond) if c is not None]
        cand_where = f"WHERE {' AND '.join(cand_conds)}" if cand_conds else ""
        candidate_sql = f"SELECT i.id AS issue_id FROM issues i {cand_where}"
        candidate_params = [*q_params, *team_params]

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

    params = [*candidate_params, _utc_iso(now), *token_params, *where_params]
    where_sql = "" if not where else f"WHERE {' AND '.join(where)}"
    return (
        f"""
        {_issue_scope_ctes(candidate_sql)}
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
    teams: list[str] | Callable[[], list[str] | None] | None = None,
    webhook_public_url: str | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/api")
    # Public origin the webhook receiver is reachable at (e.g. the dev
    # cloudflared quick-tunnel), surfaced in /meta so the UI can show the
    # paste-ready Linear webhook URL. None in normal/prod runs.
    webhook_base = (webhook_public_url or "").strip().rstrip("/") or None
    thresholds = status_thresholds or DEFAULT_STUCK_THRESHOLDS

    def _config_teams() -> list[str]:
        # Always-unscoped team keys from config, surfaced in /spend/summary to
        # populate the Teams filter popover. `teams` may be a callable so a
        # DB-owned topology's popover reflects the daemon's live,
        # hot-reloaded bindings instead of a snapshot frozen at
        # app-creation time (SYM-189). Sorted for a stable popover order.
        resolved = teams() if callable(teams) else teams
        return sorted(resolved or [])

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
        limit: Annotated[int | None, Query()] = None,
    ) -> list[IssueSummary]:
        if ui_db_pool is None:
            raise HTTPException(status_code=503, detail="UI database is not configured")

        # "all" (and the omitted param) mean no provider scoping.
        provider_filter = None if provider in (None, "all") else provider
        team_filter = _parse_teams(teams)
        model_filter = _parse_models(models)
        is_done = scope is IssueScope.DONE
        # Active scope ignores `limit` entirely, so an out-of-range value there
        # is harmless and shouldn't 422 (only the done scope's bounds matter).
        if is_done and limit is not None and not (1 <= limit <= 500):
            raise HTTPException(status_code=422, detail="limit must be between 1 and 500")

        triples: list[tuple[dict[str, Any], Any, str | None]]
        try:
            conn = await ui_db_pool.connection()
            request_now = now()
            # Done scope defaults to a rolling now-24h window unless the request
            # gives an explicit date bound. Any explicit `from`/`to` is an
            # override (the archive-page seam) — including a `to`-only historical
            # query, which must not inherit the now-24h lower bound and come back
            # empty.
            done_since = (
                _utc_iso(request_now - DONE_SCOPE_DEFAULT_WINDOW)
                if is_done and date_from is None and date_to is None
                else None
            )
            query, params = _list_issues_query(
                scope,
                q,
                now=request_now,
                date_from=date_from,
                date_to=date_to,
                done_since=done_since,
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
                queue_totals = await _queue_issue_totals(
                    conn,
                    queued_storage_id_set,
                    provider=provider_filter,
                    models=model_filter,
                )

            statuses: list[tuple[dict[str, Any], Any]] = []
            if is_done:
                # Newest N done issues (default 50). `len == limit` implies more
                # done history exists beyond what's returned.
                effective_limit = DONE_SCOPE_DEFAULT_LIMIT if limit is None else limit
                # Sort by the SQL-computed completion timestamp — no canonical
                # status needed for that — so the status fan-out below can stop
                # as soon as `effective_limit` done issues are found instead of
                # computing status for the whole (possibly all-time) window.
                issues.sort(
                    key=lambda issue: (
                        str(issue.get("max_merged_at") or issue.get("latest_activity_ts") or ""),
                        _identifier_sort_key(str(issue["identifier"])),
                    ),
                    reverse=True,
                )
                triples = []
                chunk_size = max(effective_limit, DONE_SCOPE_DEFAULT_LIMIT)
                for start in range(0, len(issues), chunk_size):
                    chunk = issues[start : start + chunk_size]
                    status_by_id = await compute_canonical_statuses(
                        conn,
                        [str(issue["id"]) for issue in chunk],
                        now=request_now,
                        thresholds=thresholds,
                    )
                    for issue in chunk:
                        status = status_by_id[str(issue["id"])]
                        if status.state != "done":
                            continue
                        # A completed issue that was requeued (back in
                        # Todo/Waiting) shows in the active scope's queue
                        # lanes — not in Done too.
                        if str(issue["id"]) in queued_storage_id_set:
                            continue
                        completed_at = issue.get("max_merged_at") or issue.get("latest_activity_ts")
                        if completed_at is None:
                            continue
                        if done_since is not None and str(completed_at) < done_since:
                            continue
                        completed_day = str(completed_at)[:10]
                        if date_from is not None and completed_day < date_from:
                            continue
                        if date_to is not None and completed_day > date_to:
                            continue
                        triples.append((issue, status, str(completed_at)))
                        if len(triples) >= effective_limit:
                            break
                    if len(triples) >= effective_limit:
                        break
            else:
                status_by_id = await compute_canonical_statuses(
                    conn,
                    [str(issue["id"]) for issue in issues],
                    now=request_now,
                    thresholds=thresholds,
                )
                statuses = [(issue, status_by_id[str(issue["id"])]) for issue in issues]
                statuses.sort(
                    key=lambda item: (
                        *canonical_status_sort_key(item[1]),
                        _identifier_sort_key(str(item[0]["identifier"])),
                    )
                )
                triples = [(issue, status, None) for issue, status in statuses]
        except aiosqlite.Error as exc:
            raise HTTPException(
                status_code=503,
                detail="UI database is not available",
            ) from exc

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
                        "id": storage_id or _queue_fallback_id(row),
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
            teams=_config_teams(),
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
