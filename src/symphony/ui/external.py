"""External Linear/GitHub truth for the issue detail UI."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

import aiosqlite

from ..config import Config, RepoBinding

JsonDict = dict[str, Any]

SOURCE_LINEAR = "linear"
SOURCE_GITHUB = "github"
DEFAULT_EXTERNAL_TTL = timedelta(seconds=60)
DEFAULT_SOURCE_ERROR_BACKOFF = timedelta(seconds=30)
COMMENT_BODY_LIMIT = 500


class LinearExternalClient(Protocol):
    async def issue_external_snapshot(self, issue_id: str) -> JsonDict:
        """Return the Linear issue state/comments payload for the UI."""


class GitHubExternalClient(Protocol):
    async def pr_external_snapshot(self, pr: int | str, *, repo: str) -> JsonDict:
        """Return the GitHub PR state/comments payload for the UI."""


@dataclass(frozen=True)
class DriftFlag:
    field: str
    sqlite_value: str | None
    source_value: str | None
    source_name: str
    severity: str = "drift"

    def to_dict(self) -> dict[str, str | None]:
        return {
            "field": self.field,
            "sqlite_value": self.sqlite_value,
            "source_value": self.source_value,
            "source_name": self.source_name,
            "severity": self.severity,
        }


@dataclass
class _LastKnownGood:
    fetched_at: str
    payload: JsonDict


@dataclass
class _CachedPayload:
    fetched_at: datetime
    payload: JsonDict


@dataclass
class _SourceError:
    failed_at: datetime
    error: str


@dataclass
class ExternalSnapshotCache:
    ttl: timedelta = DEFAULT_EXTERNAL_TTL
    source_error_backoff: timedelta = DEFAULT_SOURCE_ERROR_BACKOFF
    payloads: dict[str, _CachedPayload] = field(default_factory=dict)
    last_known_good: dict[tuple[str, str], _LastKnownGood] = field(default_factory=dict)
    source_errors: dict[tuple[str, str], _SourceError] = field(default_factory=dict)

    def get(self, issue_id: str, *, now: datetime) -> JsonDict | None:
        cached = self.payloads.get(issue_id)
        if cached is None:
            return None
        if now - cached.fetched_at >= self.ttl:
            return None
        return cached.payload

    def remember_payload(self, issue_id: str, *, fetched_at: datetime, payload: JsonDict) -> None:
        self.payloads[issue_id] = _CachedPayload(fetched_at=fetched_at, payload=payload)

    def remember_source(
        self,
        issue_id: str,
        source: str,
        *,
        fetched_at: str,
        payload: JsonDict,
    ) -> None:
        self.last_known_good[(issue_id, source)] = _LastKnownGood(
            fetched_at=fetched_at,
            payload=dict(payload),
        )
        self.source_errors.pop((issue_id, source), None)

    def remember_source_error(
        self,
        issue_id: str,
        source: str,
        *,
        failed_at: datetime,
        error: str,
    ) -> None:
        self.source_errors[(issue_id, source)] = _SourceError(
            failed_at=failed_at,
            error=error,
        )

    def source_backoff_error(self, issue_id: str, source: str, *, now: datetime) -> str | None:
        previous = self.source_errors.get((issue_id, source))
        if previous is None:
            return None
        if now - previous.failed_at < self.source_error_backoff:
            return previous.error
        return None

    def source_error(self, issue_id: str, source: str, error: str) -> JsonDict:
        last_good = self.last_known_good.get((issue_id, source))
        if last_good is None:
            return {"error": error}
        return {
            **last_good.payload,
            "error": error,
            "stale": True,
            "stale_fetched_at": last_good.fetched_at,
        }

    def clear(self) -> None:
        self.payloads.clear()
        self.last_known_good.clear()
        self.source_errors.clear()


def _normalize_now(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=UTC)
    return now.astimezone(UTC)


def _iso(now: datetime) -> str:
    return _normalize_now(now).isoformat().replace("+00:00", "Z")


def _as_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _truncate_comment(comment: JsonDict) -> JsonDict:
    body = str(comment.get("body") or "")
    truncated = len(body) > COMMENT_BODY_LIMIT
    return {
        **comment,
        "body": body[:COMMENT_BODY_LIMIT] if truncated else body,
        "truncated": truncated,
    }


def _source_has_payload(payload: JsonDict) -> bool:
    return any(key != "error" for key in payload)


async def _fetch_one(
    conn: aiosqlite.Connection,
    query: str,
    params: tuple[object, ...],
) -> JsonDict | None:
    cur = await conn.execute(query, params)
    row = await cur.fetchone()
    return dict(row) if row is not None else None


async def _fetch_all(
    conn: aiosqlite.Connection,
    query: str,
    params: tuple[object, ...],
) -> list[JsonDict]:
    cur = await conn.execute(query, params)
    rows = await cur.fetchall()
    return [dict(row) for row in rows]


async def sqlite_external_view(conn: aiosqlite.Connection, issue_id: str) -> JsonDict | None:
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
        return None

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
        SELECT run_id, kind, issue_label, created_at
        FROM operator_waits
        WHERE issue_id = ?
        ORDER BY created_at DESC, run_id DESC
        """,
        (issue_id,),
    )
    running_runs = await _fetch_all(
        conn,
        """
        SELECT id, stage, status, started_at
        FROM runs
        WHERE issue_id = ? AND status = 'running'
        ORDER BY started_at DESC, id DESC
        """,
        (issue_id,),
    )
    review_state = await _fetch_one(
        conn,
        """
        SELECT pr_number, pr_url, github_repo, issue_label
        FROM review_state
        WHERE issue_id = ?
        """,
        (issue_id,),
    )
    return {
        "issue": issue,
        "issue_prs": issue_prs,
        "operator_waits": operator_waits,
        "running_runs": running_runs,
        "review_state": review_state,
    }


def _sqlite_issue_label(sqlite_view: JsonDict) -> str | None:
    review_state = sqlite_view.get("review_state")
    if isinstance(review_state, dict) and review_state.get("issue_label"):
        return str(review_state["issue_label"])

    operator_waits = sqlite_view.get("operator_waits")
    if isinstance(operator_waits, list):
        for wait in operator_waits:
            if isinstance(wait, dict) and wait.get("issue_label"):
                return str(wait["issue_label"])
    return None


def _resolve_binding(config: Config, sqlite_view: JsonDict) -> RepoBinding | None:
    issue = sqlite_view["issue"]
    team_key = str(issue["team_key"])
    issue_prs = sqlite_view["issue_prs"]
    github_repo = None
    if issue_prs:
        github_repo = str(issue_prs[0]["github_repo"])
    else:
        review_state = sqlite_view.get("review_state")
        if isinstance(review_state, dict) and review_state.get("github_repo"):
            github_repo = str(review_state["github_repo"])

    candidates: list[RepoBinding] = []
    for binding in config.repos:
        if binding.linear_team_key != team_key:
            continue
        if github_repo is not None and binding.github_repo != github_repo:
            continue
        candidates.append(binding)

    if not candidates:
        return None

    issue_label = _sqlite_issue_label(sqlite_view)
    if issue_label:
        for binding in candidates:
            if binding.issue_label == issue_label:
                return binding
        for binding in candidates:
            if binding.issue_label is None:
                return binding
        return None

    return candidates[0]


def _github_pr_target(sqlite_view: JsonDict) -> tuple[str, int] | None:
    for row in sqlite_view["issue_prs"]:
        return str(row["github_repo"]), int(row["pr_number"])

    review_state = sqlite_view.get("review_state")
    if not isinstance(review_state, dict):
        return None
    if not review_state.get("pr_number") or not review_state.get("github_repo"):
        return None
    return str(review_state["github_repo"]), int(review_state["pr_number"])


def _matching_pr_row(sqlite_view: JsonDict, github: JsonDict) -> JsonDict | None:
    pr_number = github.get("pr_number")
    github_repo = github.get("github_repo")
    issue_prs = sqlite_view.get("issue_prs")
    if not isinstance(issue_prs, list):
        return None
    for row in issue_prs:
        if not isinstance(row, dict):
            continue
        if pr_number is not None and int(row["pr_number"]) != int(pr_number):
            continue
        if github_repo is not None and str(row["github_repo"]) != str(github_repo):
            continue
        return cast(JsonDict, row)
    if issue_prs and isinstance(issue_prs[0], dict):
        return cast(JsonDict, issue_prs[0])
    return None


def compute_drift(
    sqlite_view: JsonDict,
    snapshot: JsonDict,
    *,
    linear_done_state: str = "Done",
) -> list[DriftFlag]:
    flags: list[DriftFlag] = []
    linear = snapshot.get(SOURCE_LINEAR)
    github = snapshot.get(SOURCE_GITHUB)

    if isinstance(linear, dict) and not linear.get("error"):
        linear_state = _as_str(linear.get("state"))
        operator_waits = sqlite_view["operator_waits"]
        if linear_state == linear_done_state and operator_waits:
            flags.append(
                DriftFlag(
                    field="linear.state",
                    sqlite_value=str(operator_waits[0]["kind"]),
                    source_value=linear_state,
                    source_name="Linear",
                )
            )

    if isinstance(github, dict) and not github.get("error"):
        pr_row = _matching_pr_row(sqlite_view, github)
        state = _as_str(github.get("state"))
        merged_at = _as_str(github.get("merged_at"))
        sqlite_merged_at = _as_str(pr_row.get("merged_at")) if pr_row is not None else None

        if state in {"MERGED", "CLOSED"} and sqlite_merged_at is None:
            flags.append(
                DriftFlag(
                    field="github.state",
                    sqlite_value="merged_at=null",
                    source_value=state,
                    source_name="GitHub",
                )
            )
        if merged_at is not None and sqlite_merged_at is None:
            flags.append(
                DriftFlag(
                    field="github.merged_at",
                    sqlite_value=None,
                    source_value=merged_at,
                    source_name="GitHub",
                )
            )

        checks = github.get("check_summary")
        failing = 0
        if isinstance(checks, dict):
            failing = int(checks.get("failing") or 0)
        if failing > 0 and sqlite_view["running_runs"]:
            flags.append(
                DriftFlag(
                    field="github.checks",
                    sqlite_value="running",
                    source_value=f"{failing} failing",
                    source_name="GitHub",
                    severity="warning",
                )
            )

    return flags


class ExternalSnapshotService:
    def __init__(
        self,
        config: Config,
        linear: LinearExternalClient,
        github: GitHubExternalClient,
        *,
        cache: ExternalSnapshotCache | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._linear = linear
        self._github = github
        self.cache = cache or ExternalSnapshotCache()
        self._clock = clock

    def _now(self) -> datetime:
        if self._clock is not None:
            return _normalize_now(self._clock())
        return datetime.now(UTC)

    async def get_issue_external(
        self,
        conn: aiosqlite.Connection,
        issue_id: str,
        *,
        refresh: bool = False,
    ) -> JsonDict | None:
        now = self._now()
        if not refresh:
            cached = self.cache.get(issue_id, now=now)
            if cached is not None:
                return cached

        sqlite_view = await sqlite_external_view(conn, issue_id)
        if sqlite_view is None:
            return None

        binding = _resolve_binding(self._config, sqlite_view)
        fetched_at = _iso(now)
        linear = await self._pull_linear(issue_id, fetched_at=fetched_at, now=now)
        github = await self._pull_github(sqlite_view, fetched_at=fetched_at, now=now)
        payload: JsonDict = {
            "fetched_at": fetched_at,
            SOURCE_LINEAR: linear,
            SOURCE_GITHUB: github,
        }
        payload["drift_flags"] = [
            flag.to_dict()
            for flag in compute_drift(
                sqlite_view,
                payload,
                linear_done_state=(
                    binding.linear_states.done if binding is not None else "Done"
                ),
            )
        ]
        self.cache.remember_payload(issue_id, fetched_at=now, payload=payload)
        return payload

    async def _pull_linear(self, issue_id: str, *, fetched_at: str, now: datetime) -> JsonDict:
        backoff_error = self.cache.source_backoff_error(
            issue_id,
            SOURCE_LINEAR,
            now=now,
        )
        if backoff_error is not None:
            return self.cache.source_error(issue_id, SOURCE_LINEAR, backoff_error)

        try:
            payload = await self._linear.issue_external_snapshot(issue_id)
            payload["comments"] = [
                _truncate_comment(comment)
                for comment in payload.get("comments", [])
                if isinstance(comment, dict)
            ]
        except Exception as exc:  # noqa: BLE001 - source errors must not block the page.
            error = str(exc)
            self.cache.remember_source_error(
                issue_id,
                SOURCE_LINEAR,
                failed_at=now,
                error=error,
            )
            return self.cache.source_error(issue_id, SOURCE_LINEAR, error)

        if _source_has_payload(payload):
            self.cache.remember_source(
                issue_id,
                SOURCE_LINEAR,
                fetched_at=fetched_at,
                payload=payload,
            )
        return payload

    async def _pull_github(
        self,
        sqlite_view: JsonDict,
        *,
        fetched_at: str,
        now: datetime,
    ) -> JsonDict:
        issue_id = str(sqlite_view["issue"]["id"])
        binding = _resolve_binding(self._config, sqlite_view)
        target = _github_pr_target(sqlite_view)
        if target is None:
            payload: JsonDict = {"error": "No GitHub PR is recorded for this issue"}
            if binding is not None:
                payload["github_repo"] = binding.github_repo
            return payload

        repo, pr_number = target
        backoff_error = self.cache.source_backoff_error(
            issue_id,
            SOURCE_GITHUB,
            now=now,
        )
        if backoff_error is not None:
            return self.cache.source_error(issue_id, SOURCE_GITHUB, backoff_error)

        try:
            payload = await self._github.pr_external_snapshot(pr_number, repo=repo)
            payload["github_repo"] = repo
            payload["comments"] = [
                _truncate_comment(comment)
                for comment in payload.get("comments", [])
                if isinstance(comment, dict)
            ]
        except Exception as exc:  # noqa: BLE001 - source errors must not block the page.
            error = str(exc)
            self.cache.remember_source_error(
                issue_id,
                SOURCE_GITHUB,
                failed_at=now,
                error=error,
            )
            return self.cache.source_error(issue_id, SOURCE_GITHUB, error)

        if _source_has_payload(payload):
            self.cache.remember_source(
                issue_id,
                SOURCE_GITHUB,
                fetched_at=fetched_at,
                payload=payload,
            )
        return payload


__all__ = [
    "ExternalSnapshotCache",
    "ExternalSnapshotService",
    "DriftFlag",
    "compute_drift",
    "sqlite_external_view",
]
