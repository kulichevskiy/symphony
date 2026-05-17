"""Background external-truth observation reconciler.

This slice is deliberately audit-only: it records Linear/GitHub snapshots and
classified drift into `external_observations`, but it never clears waits,
marks PRs merged, moves Linear issues, or changes run state.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import aiosqlite

from .. import db
from ..config import Config, RepoBinding
from ..github.client import GitHub, GitHubError
from ..github.webhook import GitHubWebhookEvent
from ..linear.client import Linear, LinearError, LinearIssue

log = logging.getLogger(__name__)

SOURCE_LINEAR = "linear"
SOURCE_GITHUB = "github"

DRIFT_MERGE_ZOMBIE = "merge_zombie"
DRIFT_PR_CLOSED_NO_MERGE = "pr_closed_no_merge"
DRIFT_LINEAR_STATE_DONE = "linear_state_done"
DRIFT_PR_LOCALLY_MERGED = "pr_locally_merged"

ACTION_OBSERVED = "observed"
ACTION_WOULD_CLEAR = "would_clear"


@dataclass(frozen=True)
class ReconcileCandidate:
    issue_id: str
    identifier: str
    team_key: str
    first_candidate_at: str
    last_observed_at: str | None


@dataclass(frozen=True)
class LocalIssuePr:
    issue_id: str
    github_repo: str
    binding_key: str
    pr_number: int
    pr_url: str
    created_at: str


@dataclass(frozen=True)
class GithubPrObservation:
    github_repo: str
    pr_number: int
    state: str
    mergeable: str | None
    merged: bool
    merged_at: str | None
    url: str
    error: str | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "github_repo": self.github_repo,
            "pr_number": self.pr_number,
            "state": self.state,
            "mergeable": self.mergeable,
            "merged": self.merged,
            "merged_at": self.merged_at,
            "url": self.url,
        }
        if self.error is not None:
            payload["error"] = self.error
        return payload


class _BackoffRequested(RuntimeError):
    def __init__(self, *, source: str, error: str) -> None:
        super().__init__(error)
        self.source = source
        self.error = error


def reconcile_dry_run_enabled(env: Mapping[str, str] | None = None) -> bool:
    value = (env or os.environ).get("SYMPHONY_RECONCILE_DRYRUN", "")
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def classify_linear_drift(
    *,
    has_operator_wait: bool,
    state_name: str | None,
    done_state_names: set[str],
) -> str | None:
    if has_operator_wait and state_name is not None and state_name in done_state_names:
        return DRIFT_LINEAR_STATE_DONE
    return None


def classify_github_drift(
    *,
    has_merge_wait: bool,
    prs: list[GithubPrObservation],
) -> str | None:
    valid_prs = [pr for pr in prs if pr.error is None]
    if has_merge_wait and any(pr.merged for pr in valid_prs):
        return DRIFT_MERGE_ZOMBIE
    if has_merge_wait and any(
        pr.state.upper() == "CLOSED" and not pr.merged for pr in valid_prs
    ):
        return DRIFT_PR_CLOSED_NO_MERGE
    if any(pr.merged for pr in valid_prs):
        return DRIFT_PR_LOCALLY_MERGED
    return None


class Reconciler:
    def __init__(
        self,
        config: Config,
        conn: aiosqlite.Connection,
        linear: Linear,
        gh: GitHub,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self._conn = conn
        self._linear = linear
        self._gh = gh
        self._clock = clock
        self._backoff_until: datetime | None = None

    def _now(self) -> datetime:
        if self._clock is not None:
            now = self._clock()
        else:
            now = datetime.now(UTC)
        if now.tzinfo is None:
            return now.replace(tzinfo=UTC)
        return now.astimezone(UTC)

    async def run(self, shutdown: asyncio.Event) -> None:
        log.info(
            "external reconciler entering loop (interval=%ds max_per_tick=%d)",
            self.config.reconcile_interval_secs,
            self.config.reconcile_max_per_tick,
        )
        while not shutdown.is_set():
            try:
                await self.tick()
            except Exception:  # noqa: BLE001
                log.exception("external reconcile tick failed")
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=self.config.reconcile_interval_secs
                )
            except TimeoutError:
                pass

    async def tick(self) -> int:
        if self._backoff_active():
            return 0

        observed = 0
        for candidate in await self._list_candidates():
            if observed // 2 >= self.config.reconcile_max_per_tick:
                break
            if not await self._candidate_enabled(candidate.issue_id, candidate.team_key):
                continue
            try:
                observed += await self.reconcile_issue(
                    candidate.issue_id,
                    reason="periodic",
                )
            except _BackoffRequested as exc:
                self._enter_backoff(source=exc.source, error=exc.error)
                break
        return observed

    async def reconcile_issue(self, issue_id: str, *, reason: str) -> int:
        if self._backoff_active():
            return 0

        issue_row = await self._issue_row(issue_id)
        if issue_row is None:
            return 0
        wait = await db.operator_waits.get(self._conn, issue_id)
        prs = await self._open_prs(issue_id)
        if wait is None and not prs:
            return 0
        if not await self._candidate_enabled(issue_id, str(issue_row["team_key"])):
            return 0

        observed_at = self._now().isoformat()
        done_state_names = self._done_state_names(str(issue_row["team_key"]))
        try:
            linear_issue, linear_payload = await self._linear_payload(issue_id)
            github_prs, github_payload = await self._github_payload(prs)
        except _BackoffRequested:
            raise

        linear_drift = classify_linear_drift(
            has_operator_wait=wait is not None,
            state_name=linear_issue.state_name if linear_issue is not None else None,
            done_state_names=done_state_names,
        )
        github_drift = classify_github_drift(
            has_merge_wait=wait is not None and wait.kind == db.operator_waits.KIND_MERGE,
            prs=github_prs,
        )
        linear_action = _action_for(linear_drift)
        github_action = _action_for(github_drift)

        await db.external_observations.insert(
            self._conn,
            issue_id=issue_id,
            source=SOURCE_LINEAR,
            observed_at=observed_at,
            payload_json=_json_payload(
                {
                    **linear_payload,
                    "reason": reason,
                }
            ),
            drift_kind=linear_drift,
            action_taken=linear_action,
        )
        await db.external_observations.insert(
            self._conn,
            issue_id=issue_id,
            source=SOURCE_GITHUB,
            observed_at=observed_at,
            payload_json=_json_payload(
                {
                    **github_payload,
                    "reason": reason,
                }
            ),
            drift_kind=github_drift,
            action_taken=github_action,
        )
        return 2

    async def reconcile_github_event(self, event: GitHubWebhookEvent) -> int:
        if event.pr_number is None:
            return 0
        cur = await self._conn.execute(
            """
            SELECT issue_id
            FROM issue_prs
            WHERE lower(github_repo) = lower(?) AND pr_number = ?
            ORDER BY merged_at IS NOT NULL, created_at DESC
            LIMIT 1
            """,
            (event.repo, event.pr_number),
        )
        row = await cur.fetchone()
        if row is None:
            return 0
        try:
            return await self.reconcile_issue(
                str(row["issue_id"]),
                reason=f"github_webhook:{event.event_type}.{event.action}",
            )
        except _BackoffRequested as exc:
            self._enter_backoff(source=exc.source, error=exc.error)
            return 0

    async def reconcile_linear_issue_event(
        self,
        *,
        issue_id: str,
        action: str,
    ) -> int:
        try:
            return await self.reconcile_issue(
                issue_id,
                reason=f"linear_webhook:issue.{action}",
            )
        except _BackoffRequested as exc:
            self._enter_backoff(source=exc.source, error=exc.error)
            return 0

    def _backoff_active(self) -> bool:
        if self._backoff_until is None:
            return False
        if self._now() >= self._backoff_until:
            self._backoff_until = None
            return False
        return True

    def _enter_backoff(self, *, source: str, error: str) -> None:
        until = self._now() + timedelta(seconds=self.config.reconcile_backoff_secs)
        self._backoff_until = until
        log.warning(
            "external reconciler backoff source=%s until=%s error=%s",
            source,
            until.isoformat(),
            error,
        )

    async def _list_candidates(self) -> list[ReconcileCandidate]:
        cur = await self._conn.execute(
            """
            WITH candidate_events(issue_id, source_ts) AS (
                SELECT issue_id, created_at FROM operator_waits
                UNION ALL
                SELECT issue_id, created_at FROM issue_prs WHERE merged_at IS NULL
            )
            SELECT
                i.id AS issue_id,
                i.identifier,
                i.team_key,
                MIN(c.source_ts) AS first_candidate_at,
                MAX(o.observed_at) AS last_observed_at
            FROM candidate_events c
            JOIN issues i ON i.id = c.issue_id
            LEFT JOIN external_observations o ON o.issue_id = c.issue_id
            GROUP BY i.id, i.identifier, i.team_key
            ORDER BY
                CASE WHEN MAX(o.observed_at) IS NULL THEN 0 ELSE 1 END ASC,
                MAX(o.observed_at) ASC,
                MIN(c.source_ts) ASC,
                i.id ASC
            """
        )
        rows = await cur.fetchall()
        return [
            ReconcileCandidate(
                issue_id=str(row["issue_id"]),
                identifier=str(row["identifier"]),
                team_key=str(row["team_key"]),
                first_candidate_at=str(row["first_candidate_at"]),
                last_observed_at=(
                    str(row["last_observed_at"])
                    if row["last_observed_at"] is not None
                    else None
                ),
            )
            for row in rows
        ]

    async def _issue_row(self, issue_id: str) -> aiosqlite.Row | None:
        cur = await self._conn.execute(
            "SELECT id, identifier, title, team_key FROM issues WHERE id = ?",
            (issue_id,),
        )
        return await cur.fetchone()

    async def _open_prs(self, issue_id: str) -> list[LocalIssuePr]:
        cur = await self._conn.execute(
            """
            SELECT issue_id, github_repo, binding_key, pr_number, pr_url, created_at
            FROM issue_prs
            WHERE issue_id = ? AND merged_at IS NULL
            ORDER BY created_at ASC, github_repo ASC
            """,
            (issue_id,),
        )
        rows = await cur.fetchall()
        return [
            LocalIssuePr(
                issue_id=str(row["issue_id"]),
                github_repo=str(row["github_repo"]),
                binding_key=str(row["binding_key"] or ""),
                pr_number=int(row["pr_number"]),
                pr_url=str(row["pr_url"]),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]

    async def _linear_payload(
        self, issue_id: str
    ) -> tuple[LinearIssue | None, dict[str, object]]:
        try:
            issue = await self._linear.lookup_issue(issue_id)
        except LinearError as exc:
            if _should_backoff(str(exc)):
                raise _BackoffRequested(source=SOURCE_LINEAR, error=str(exc)) from exc
            return None, {"error": str(exc)}
        return issue, {
            "id": issue.id,
            "identifier": issue.identifier,
            "state": issue.state_name,
            "state_type": issue.state_type,
            "updated_at": issue.updated_at,
            "team_key": issue.team_key,
            "labels": issue.labels,
        }

    async def _github_payload(
        self,
        prs: list[LocalIssuePr],
    ) -> tuple[list[GithubPrObservation], dict[str, object]]:
        observations: list[GithubPrObservation] = []
        for pr in prs:
            try:
                view = await self._gh.pr_view(pr.pr_number, repo=pr.github_repo)
            except GitHubError as exc:
                if _should_backoff(str(exc)):
                    raise _BackoffRequested(source=SOURCE_GITHUB, error=str(exc)) from exc
                observations.append(
                    GithubPrObservation(
                        github_repo=pr.github_repo,
                        pr_number=pr.pr_number,
                        state="ERROR",
                        mergeable=None,
                        merged=False,
                        merged_at=None,
                        url=pr.pr_url,
                        error=str(exc),
                    )
                )
                continue

            state = str(view.get("state") or "")
            merged_at = _optional_str(view.get("mergedAt"))
            merged = bool(view.get("merged")) or bool(merged_at) or state.upper() == "MERGED"
            observations.append(
                GithubPrObservation(
                    github_repo=pr.github_repo,
                    pr_number=pr.pr_number,
                    state=state,
                    mergeable=_optional_str(view.get("mergeable")),
                    merged=merged,
                    merged_at=merged_at,
                    url=_optional_str(view.get("url")) or pr.pr_url,
                )
            )
        payload: dict[str, object] = {"prs": [pr.to_payload() for pr in observations]}
        if not prs:
            payload["error"] = "no linked unmerged PR"
        return observations, payload

    async def _candidate_enabled(self, issue_id: str, team_key: str) -> bool:
        wait = await db.operator_waits.get(self._conn, issue_id)
        prs = await self._open_prs(issue_id)
        matched: list[RepoBinding] = []
        if wait is not None:
            matched.extend(
                self._matching_bindings(
                    team_key=wait.linear_team_key,
                    github_repo=wait.github_repo,
                    issue_label=wait.issue_label,
                )
            )
        for pr in prs:
            matched.extend(
                self._matching_bindings(
                    team_key=team_key,
                    github_repo=pr.github_repo,
                    issue_label=_label_from_binding_key(pr.binding_key),
                )
            )
        if not matched:
            return True
        return any(binding.reconcile_enabled for binding in matched)

    def _matching_bindings(
        self,
        *,
        team_key: str,
        github_repo: str,
        issue_label: str | None,
    ) -> list[RepoBinding]:
        bindings: list[RepoBinding] = []
        for binding in self.config.repos:
            if binding.linear_team_key != team_key:
                continue
            if binding.github_repo != github_repo:
                continue
            if issue_label is not None and (binding.issue_label or "") != issue_label:
                continue
            bindings.append(binding)
        return bindings

    def _done_state_names(self, team_key: str) -> set[str]:
        names = {
            binding.linear_states.done
            for binding in self.config.repos
            if binding.linear_team_key == team_key and binding.reconcile_enabled
        }
        return names or {"Done"}


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _label_from_binding_key(binding_key: str) -> str | None:
    if not binding_key:
        return None
    try:
        raw: Any = json.loads(binding_key)
    except ValueError:
        return None
    if not isinstance(raw, list) or len(raw) < 3:
        return None
    label = raw[2]
    if label is None:
        return ""
    return str(label)


def _action_for(drift_kind: str | None) -> str:
    if drift_kind is not None and reconcile_dry_run_enabled():
        return ACTION_WOULD_CLEAR
    return ACTION_OBSERVED


def _json_payload(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _should_backoff(message: str) -> bool:
    text = message.casefold()
    return any(
        marker in text
        for marker in (
            "429",
            "500",
            "502",
            "503",
            "504",
            "rate limit",
            "secondary rate",
        )
    )


__all__ = [
    "ACTION_OBSERVED",
    "ACTION_WOULD_CLEAR",
    "DRIFT_LINEAR_STATE_DONE",
    "DRIFT_MERGE_ZOMBIE",
    "DRIFT_PR_CLOSED_NO_MERGE",
    "DRIFT_PR_LOCALLY_MERGED",
    "GithubPrObservation",
    "ReconcileCandidate",
    "Reconciler",
    "classify_github_drift",
    "classify_linear_drift",
    "reconcile_dry_run_enabled",
]
