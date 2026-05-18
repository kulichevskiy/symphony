"""Background external-truth observation reconciler.

The reconciler stays observe-only by default. When active auto-clear is
explicitly enabled, it only applies monotonic local corrections: removing
obsolete operator waits, marking locally unmerged PR rows merged, and noting
external Linear completion in the timeline.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
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
ACTION_CLEARED = "cleared"
ACTION_NOTED = "noted"

_TRANSIENT_STATUS_RE = re.compile(
    r"\b(?:http(?:\s+status)?|status(?:\s+code)?|response(?:\s+status)?|"
    r"server\s+error|api\s+error|error)\D{0,24}(?:429|5\d\d)\b"
    r"|\b(?:429|5\d\d)\b\D{0,24}(?:too many requests|server\s+error|"
    r"bad\s+gateway|service\s+unavailable|gateway\s+timeout)\b",
    re.IGNORECASE,
)


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


@dataclass(frozen=True)
class _ReconcileIssueResult:
    observations: int
    actions_taken: int
    actions_deferred: int


class _BackoffRequested(RuntimeError):
    def __init__(self, *, source: str, error: str) -> None:
        super().__init__(error)
        self.source = source
        self.error = error


def reconcile_dry_run_enabled(env: Mapping[str, str] | None = None) -> bool:
    value = (env or os.environ).get("SYMPHONY_RECONCILE_DRYRUN", "")
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def reconcile_autoclear_disabled(env: Mapping[str, str] | None = None) -> bool:
    value = (env or os.environ).get("SYMPHONY_RECONCILE_AUTOCLEAR_DISABLED", "")
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def reconcile_auto_clear_enabled(env: Mapping[str, str] | None = None) -> bool:
    values = env or os.environ
    if reconcile_autoclear_disabled(values):
        return False
    value = values.get("SYMPHONY_RECONCILE_DRYRUN")
    if value is None:
        return False
    return value.strip().casefold() in {"0", "false", "no", "off"}


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
        actions_taken = 0
        actions_deferred = 0
        for candidate in await self._list_candidates():
            if observed // 2 >= self.config.reconcile_max_per_tick:
                break
            if not await self._candidate_enabled(candidate.issue_id, candidate.team_key):
                continue
            try:
                action_budget_remaining = None
                if reconcile_auto_clear_enabled():
                    action_budget_remaining = max(
                        self.config.reconcile_max_actions_per_tick - actions_taken,
                        0,
                    )
                result = await self._reconcile_issue(
                    candidate.issue_id,
                    reason="periodic",
                    action_budget_remaining=action_budget_remaining,
                )
                observed += result.observations
                actions_taken += result.actions_taken
                actions_deferred += result.actions_deferred
            except _BackoffRequested as exc:
                self._enter_backoff(source=exc.source, error=exc.error)
                break
        if actions_deferred:
            log.warning(
                "external reconciler action cap reached max_actions=%d "
                "actions_taken=%d deferred_actions=%d",
                self.config.reconcile_max_actions_per_tick,
                actions_taken,
                actions_deferred,
            )
        return observed

    async def reconcile_issue(self, issue_id: str, *, reason: str) -> int:
        result = await self._reconcile_issue(
            issue_id,
            reason=reason,
            action_budget_remaining=None,
        )
        return result.observations

    async def _reconcile_issue(
        self,
        issue_id: str,
        *,
        reason: str,
        action_budget_remaining: int | None,
    ) -> _ReconcileIssueResult:
        if self._backoff_active():
            return _ReconcileIssueResult(
                observations=0,
                actions_taken=0,
                actions_deferred=0,
            )

        issue_row = await self._issue_row(issue_id)
        if issue_row is None:
            return _ReconcileIssueResult(
                observations=0,
                actions_taken=0,
                actions_deferred=0,
            )
        wait = await db.operator_waits.get(self._conn, issue_id)
        prs = await self._open_prs(issue_id)
        if wait is None and not prs:
            return _ReconcileIssueResult(
                observations=0,
                actions_taken=0,
                actions_deferred=0,
            )
        matched_bindings = self._matched_bindings(
            team_key=str(issue_row["team_key"]),
            wait=wait,
            prs=prs,
        )
        if not any(binding.reconcile_enabled for binding in matched_bindings):
            return _ReconcileIssueResult(
                observations=0,
                actions_taken=0,
                actions_deferred=0,
            )

        observed_at = self._now().isoformat()
        done_state_names = self._done_state_names(matched_bindings)
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
            prs=_github_prs_for_drift(wait=wait, github_prs=github_prs),
        )
        active = reconcile_auto_clear_enabled()
        remaining = action_budget_remaining
        actions_taken = 0
        actions_deferred = 0

        linear_action = _passive_action_for(linear_drift)
        if active and linear_drift == DRIFT_LINEAR_STATE_DONE:
            if remaining is None or remaining > 0:
                linear_action = ACTION_NOTED
                actions_taken += 1
                if remaining is not None:
                    remaining -= 1
            else:
                actions_deferred += 1

        github_action = _passive_action_for(github_drift)
        github_clearable = _github_clearable(
            github_drift=github_drift,
            wait=wait,
            github_prs=_github_prs_for_drift(wait=wait, github_prs=github_prs),
        )
        if active and github_clearable:
            if remaining is None or remaining > 0:
                github_action = ACTION_CLEARED
                actions_taken += 1
                if remaining is not None:
                    remaining -= 1
            else:
                actions_deferred += 1

        try:
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
                commit=False,
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
                commit=False,
            )
            if linear_action == ACTION_NOTED and linear_issue is not None:
                await self._note_external_state_change(
                    issue_id=issue_id,
                    source=SOURCE_LINEAR,
                    state_name=linear_issue.state_name,
                    ts=observed_at,
                )
            if github_action == ACTION_CLEARED:
                await self._apply_github_clear(
                    issue_id=issue_id,
                    wait=wait,
                    drift_kind=github_drift,
                    github_prs=_github_prs_for_drift(wait=wait, github_prs=github_prs),
                )
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

        return _ReconcileIssueResult(
            observations=2,
            actions_taken=actions_taken,
            actions_deferred=actions_deferred,
        )

    async def reconcile_github_event(self, event: GitHubWebhookEvent) -> int:
        if event.event_type != "pull_request":
            return 0
        if event.action not in {"closed", "merged", "reopened"}:
            return 0
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
            ),
            candidate_summary AS (
                SELECT issue_id, MIN(source_ts) AS first_candidate_at
                FROM candidate_events
                GROUP BY issue_id
            ),
            observation_summary AS (
                SELECT issue_id, MAX(observed_at) AS last_observed_at
                FROM external_observations
                GROUP BY issue_id
            )
            SELECT
                i.id AS issue_id,
                i.identifier,
                i.team_key,
                c.first_candidate_at,
                o.last_observed_at
            FROM candidate_summary c
            JOIN issues i ON i.id = c.issue_id
            LEFT JOIN observation_summary o ON o.issue_id = c.issue_id
            ORDER BY
                CASE WHEN o.last_observed_at IS NULL THEN 0 ELSE 1 END ASC,
                o.last_observed_at ASC,
                c.first_candidate_at ASC,
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
        matched = self._matched_bindings(team_key=team_key, wait=wait, prs=prs)
        return any(binding.reconcile_enabled for binding in matched)

    def _matched_bindings(
        self,
        *,
        team_key: str,
        wait: db.operator_waits.OperatorWait | None,
        prs: list[LocalIssuePr],
    ) -> list[RepoBinding]:
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
            matched.extend(self._bindings_for_pr(team_key=team_key, pr=pr))
        return matched

    def _bindings_for_pr(
        self,
        *,
        team_key: str,
        pr: LocalIssuePr,
    ) -> list[RepoBinding]:
        if pr.binding_key:
            for binding in self.config.repos:
                if _binding_storage_key(binding) == pr.binding_key:
                    return [binding]

        matches = self._matching_bindings(
            team_key=team_key,
            github_repo=pr.github_repo,
            issue_label=None,
        )
        stored_label = _label_from_binding_key(pr.binding_key)
        if stored_label is not None:
            return [
                binding
                for binding in matches
                if (binding.issue_label or "") == stored_label
            ]
        if len(matches) == 1:
            return matches
        return []

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

    def _done_state_names(self, bindings: list[RepoBinding]) -> set[str]:
        names = {
            binding.linear_states.done
            for binding in bindings
            if binding.reconcile_enabled
        }
        return names or {"Done"}

    async def _note_external_state_change(
        self,
        *,
        issue_id: str,
        source: str,
        state_name: str,
        ts: str,
    ) -> None:
        await db.state_transitions.record_transition(
            self._conn,
            issue_id,
            "external_observations",
            "external_state_change",
            source,
            f"{source}:{state_name}",
            ts=ts,
        )

    async def _apply_github_clear(
        self,
        *,
        issue_id: str,
        wait: db.operator_waits.OperatorWait | None,
        drift_kind: str | None,
        github_prs: list[GithubPrObservation],
    ) -> None:
        if drift_kind == DRIFT_PR_CLOSED_NO_MERGE:
            if wait is None:
                raise RuntimeError("cannot clear closed PR drift without an operator wait")
            await db.operator_waits.delete(
                self._conn,
                issue_id,
                wait.run_id,
                commit=False,
            )
            return

        merged_prs = _merged_prs_with_timestamps(github_prs)
        if not merged_prs:
            raise RuntimeError("cannot clear merged PR drift without github.mergedAt")

        if drift_kind == DRIFT_MERGE_ZOMBIE:
            if wait is None:
                raise RuntimeError("cannot clear merge zombie without an operator wait")
            await db.operator_waits.delete(
                self._conn,
                issue_id,
                wait.run_id,
                commit=False,
            )
        elif drift_kind != DRIFT_PR_LOCALLY_MERGED:
            raise RuntimeError(f"unsupported github drift clear: {drift_kind}")

        for pr in merged_prs:
            if pr.merged_at is None:
                continue
            updated = await db.issue_prs.update_merged(
                self._conn,
                issue_id=issue_id,
                github_repo=pr.github_repo,
                pr_number=pr.pr_number,
                merged_at=pr.merged_at,
                commit=False,
            )
            if not updated:
                raise RuntimeError(
                    "could not update merged_at for "
                    f"{pr.github_repo}#{pr.pr_number}"
                )


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


def _binding_storage_key(binding: RepoBinding) -> str:
    return json.dumps(
        (binding.linear_team_key, binding.github_repo, binding.issue_label or ""),
        separators=(",", ":"),
    )


def _passive_action_for(drift_kind: str | None) -> str:
    if (
        drift_kind is not None
        and not reconcile_autoclear_disabled()
        and reconcile_dry_run_enabled()
    ):
        return ACTION_WOULD_CLEAR
    return ACTION_OBSERVED


def _github_clearable(
    *,
    github_drift: str | None,
    wait: db.operator_waits.OperatorWait | None,
    github_prs: list[GithubPrObservation],
) -> bool:
    if github_drift == DRIFT_PR_CLOSED_NO_MERGE:
        return wait is not None and wait.kind == db.operator_waits.KIND_MERGE
    if github_drift == DRIFT_MERGE_ZOMBIE:
        return (
            wait is not None
            and wait.kind == db.operator_waits.KIND_MERGE
            and bool(_merged_prs_with_timestamps(github_prs))
        )
    if github_drift == DRIFT_PR_LOCALLY_MERGED:
        return bool(_merged_prs_with_timestamps(github_prs))
    return False


def _github_prs_for_drift(
    *,
    wait: db.operator_waits.OperatorWait | None,
    github_prs: list[GithubPrObservation],
) -> list[GithubPrObservation]:
    if wait is None or wait.kind != db.operator_waits.KIND_MERGE:
        return github_prs
    return [
        pr
        for pr in github_prs
        if pr.github_repo.casefold() == wait.github_repo.casefold()
    ]


def _merged_prs_with_timestamps(
    github_prs: list[GithubPrObservation],
) -> list[GithubPrObservation]:
    return [
        pr
        for pr in github_prs
        if pr.error is None and pr.merged and pr.merged_at is not None
    ]


def _json_payload(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _should_backoff(message: str) -> bool:
    text = message.casefold()
    if any(
        marker in text
        for marker in ("rate limit", "secondary rate", "too many requests")
    ):
        return True
    return _TRANSIENT_STATUS_RE.search(message) is not None


__all__ = [
    "ACTION_CLEARED",
    "ACTION_NOTED",
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
    "reconcile_auto_clear_enabled",
    "reconcile_autoclear_disabled",
    "reconcile_dry_run_enabled",
]
