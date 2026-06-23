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
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import aiosqlite

from .. import db
from ..config import Config, RepoBinding
from ..github.client import GitHub, GitHubError
from ..github.webhook import GitHubWebhookEvent
from ..linear.client import LinearError
from ..tracker import (
    DEFAULT_PROVIDER,
    DEFAULT_SITE,
    IssueTracker,
    TrackerContext,
    TrackerRegistry,
    context_for_binding,
)
from ..tracker import (
    Issue as LinearIssue,
)

log = logging.getLogger(__name__)

SOURCE_LINEAR = "linear"
SOURCE_GITHUB = "github"

DRIFT_MERGE_ZOMBIE = "merge_zombie"
DRIFT_PR_CLOSED_NO_MERGE = "pr_closed_no_merge"
DRIFT_LINEAR_STATE_DONE = "linear_state_done"
DRIFT_PR_LOCALLY_MERGED = "pr_locally_merged"
DRIFT_ORPHAN_PR_OPEN = "orphan_pr_open"

ACTION_OBSERVED = "observed"
ACTION_WOULD_CLEAR = "would_clear"
ACTION_CLEARED = "cleared"
ACTION_NOTED = "noted"
ACTION_ADOPTED = "adopted"

# Parked operator-wait kinds whose head branch we probe for an orphan open PR.
# These are terminal handoffs where a PR may have been opened but never
# recorded in `issue_prs`.
_PARKED_WAIT_KINDS = frozenset(
    {
        db.operator_waits.KIND_IMPLEMENT_FAILED,
        db.operator_waits.KIND_DELIVER_FAILED,
    }
)

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
class _AdoptableOrphanPr:
    binding: RepoBinding
    observation: GithubPrObservation


@dataclass(frozen=True)
class _PostCommitReviewRequest:
    github_repo: str
    pr_number: int


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


def _register_configured_trackers(
    registry: TrackerRegistry,
    config: Config,
    tracker: IssueTracker,
) -> None:
    registry.register(DEFAULT_PROVIDER, DEFAULT_SITE, tracker)
    for binding in config.repos:
        ctx = context_for_binding(binding)
        registry.register(
            ctx.provider,
            ctx.site,
            tracker,
            project_key=ctx.project_key,
        )


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


def _parse_rfc3339(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


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
        tracker_or_registry: IssueTracker | TrackerRegistry,
        gh: GitHub,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self._conn = conn
        if isinstance(tracker_or_registry, TrackerRegistry):
            self._trackers = tracker_or_registry
        else:
            self._trackers = TrackerRegistry()
            _register_configured_trackers(self._trackers, config, tracker_or_registry)
        self._gh = gh
        self._clock = clock
        self._backoff_until: datetime | None = None

    def tracker(self, ctx: TrackerContext | None = None) -> IssueTracker:
        return self._trackers.resolve(ctx)

    def _now(self) -> datetime:
        if self._clock is not None:
            now = self._clock()
        else:
            now = datetime.now(UTC)  # noqa: clock — sanctioned wall-clock entry point
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
            except Exception:  # noqa: BLE001
                log.exception(
                    "external reconcile failed for issue=%s; skipping candidate",
                    candidate.issue_id,
                )
                continue
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
        tracker_ctx = self._tracker_context_from_issue_row(issue_row, matched_bindings)
        post_commit_review_request: _PostCommitReviewRequest | None = None
        try:
            tracker_issue_id = str(issue_row["tracker_issue_id"] or issue_id)
            linear_issue, linear_payload = await self._tracker_payload(
                tracker_issue_id,
                tracker_ctx,
            )
            github_prs, github_payload = await self._github_payload(prs)
            orphans = await self._orphan_open_prs(
                issue_row=issue_row,
                wait=wait,
                matched_bindings=matched_bindings,
                recorded_observations=github_prs,
            )
        except _BackoffRequested:
            raise

        if orphans:
            combined = github_prs + [orphan.observation for orphan in orphans]
            github_payload = {"prs": [pr.to_payload() for pr in combined]}

        drift_prs = _github_prs_for_drift(wait=wait, github_prs=github_prs)
        linear_drift = classify_linear_drift(
            has_operator_wait=wait is not None,
            state_name=linear_issue.state_name if linear_issue is not None else None,
            done_state_names=done_state_names,
        )
        github_drift = classify_github_drift(
            has_merge_wait=wait is not None and wait.kind == db.operator_waits.KIND_MERGE,
            prs=drift_prs,
        )
        if (
            github_drift is None
            and linear_drift != DRIFT_LINEAR_STATE_DONE
            and orphans
        ):
            github_drift = DRIFT_ORPHAN_PR_OPEN
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
            github_prs=drift_prs,
        )
        if active and github_drift == DRIFT_ORPHAN_PR_OPEN and orphans:
            if remaining is None or remaining > 0:
                github_action = ACTION_ADOPTED
                actions_taken += 1
                if remaining is not None:
                    remaining -= 1
            else:
                actions_deferred += 1
        elif active and github_clearable:
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
                    github_prs=drift_prs,
                )
            elif github_action == ACTION_ADOPTED:
                post_commit_review_request = await self._adopt_orphan_prs(
                    issue_id=issue_id,
                    tracker_issue_id=tracker_issue_id,
                    tracker_ctx=tracker_ctx,
                    team_key=str(issue_row["team_key"]),
                    wait=wait,
                    orphans=orphans,
                    observed_at=observed_at,
                )
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

        if post_commit_review_request is not None:
            try:
                await self._gh.pr_comment(
                    post_commit_review_request.pr_number,
                    "@codex review",
                    repo=post_commit_review_request.github_repo,
                )
            except GitHubError as e:
                log.warning(
                    "could not post @codex review on %s#%d: %s",
                    post_commit_review_request.github_repo,
                    post_commit_review_request.pr_number,
                    e,
                )

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
            """
            SELECT id, tracker_issue_id, provider, site, identifier, title, team_key
            FROM issues
            WHERE id = ?
            """,
            (issue_id,),
        )
        return await cur.fetchone()

    def _tracker_context_from_issue_row(
        self,
        row: aiosqlite.Row,
        bindings: list[RepoBinding],
    ) -> TrackerContext:
        provider = str(row["provider"] or "")
        site = str(row["site"] or "")
        if provider and site:
            project_key = str(row["team_key"] or "") if provider == "jira" else ""
            return TrackerContext(provider=provider, site=site, project_key=project_key)
        for binding in bindings:
            return context_for_binding(binding)
        return TrackerContext()

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

    async def _tracker_payload(
        self,
        issue_id: str,
        ctx: TrackerContext,
    ) -> tuple[LinearIssue | None, dict[str, object]]:
        try:
            issue = await self.tracker(ctx).lookup_issue(issue_id)
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

    async def _orphan_open_prs(
        self,
        *,
        issue_row: aiosqlite.Row,
        wait: db.operator_waits.OperatorWait | None,
        matched_bindings: list[RepoBinding],
        recorded_observations: list[GithubPrObservation],
    ) -> list[_AdoptableOrphanPr]:
        """Probe each parked binding's head branch for an unrecorded open PR.

        Only fires for a parked implement/deliver-failed wait. Lists by head branch
        (``gh pr list --head <branch_prefix>/<identifier>``) so a PR that was
        opened for the branch but never landed in ``issue_prs`` is still found.
        """
        if wait is None or wait.kind not in _PARKED_WAIT_KINDS:
            return []
        identifier = str(issue_row["identifier"] or "").strip().lower()
        if not identifier:
            return []
        active_recorded_repos = {
            pr.github_repo.casefold()
            for pr in recorded_observations
            if pr.error is None and pr.state.upper() == "OPEN"
        }
        seen_repos: set[str] = set()
        orphans: list[_AdoptableOrphanPr] = []
        for binding in matched_bindings:
            if not binding.reconcile_enabled:
                continue
            repo_key = binding.github_repo.casefold()
            if repo_key in active_recorded_repos or repo_key in seen_repos:
                continue
            seen_repos.add(repo_key)
            head = f"{binding.branch_prefix}/{identifier}"
            try:
                found = await self._gh.open_pr_for_head(
                    head=head, repo=binding.github_repo
                )
            except GitHubError as exc:
                if _should_backoff(str(exc)):
                    raise _BackoffRequested(
                        source=SOURCE_GITHUB, error=str(exc)
                    ) from exc
                continue
            if found is None:
                continue
            orphans.append(
                _AdoptableOrphanPr(
                    binding=binding,
                    observation=GithubPrObservation(
                        github_repo=binding.github_repo,
                        pr_number=int(found["number"]),
                        state="OPEN",
                        mergeable=None,
                        merged=False,
                        merged_at=None,
                        url=str(found["url"]),
                    ),
                )
            )
        return orphans

    async def _adopt_orphan_prs(
        self,
        *,
        issue_id: str,
        tracker_issue_id: str,
        tracker_ctx: TrackerContext | None,
        team_key: str,
        wait: db.operator_waits.OperatorWait | None,
        orphans: list[_AdoptableOrphanPr],
        observed_at: str,
    ) -> _PostCommitReviewRequest | None:
        """Adopt a discovered orphan PR: record it and route to review/merge.

        Mirrors the durable writes of the normal implement-success path
        (`issue_prs` row + `review_state`, plus a ``review`` run when the
        binding configures review) and moves the parked Linear issue out of
        ``blocked`` into the active lane the relevant poller expects, then
        clears the operator wait. Without that move the review/merge pollers
        reject the issue (both treat ``Blocked`` as inactive) and the adopted
        run is closed instead of advancing.

        Local-only review requires durable evidence that a completed
        ``local_review`` run covers this PR cycle. If that evidence is absent,
        adopt the PR record but park the review row and issue in the manual
        approval lane instead of creating a merge candidate.

        Only one orphan is adopted per tick: ``review_state`` is keyed per
        issue (``ON CONFLICT(issue_id)``) so multiple orphans would clobber the
        single row, and the action budget counts one adoption per tick.
        """
        orphan = orphans[0]
        binding = orphan.binding
        obs = orphan.observation
        local_review_configured = binding.resolved_local_review()
        remote_review_configured = binding.resolved_remote_review()
        review_configured = (
            local_review_configured or remote_review_configured
        )
        local_only_review_ready = True
        if local_review_configured and not remote_review_configured:
            local_only_review_ready = await self._local_review_completed_for_adoption(
                issue_id=issue_id,
                pr_created_at=observed_at,
            )
        await db.issue_prs.upsert(
            self._conn,
            issue_id=issue_id,
            github_repo=obs.github_repo,
            binding_key=_binding_storage_key(binding),
            pr_number=obs.pr_number,
            pr_url=obs.url,
            created_at=observed_at,
            review_bypassed=not review_configured,
            commit=False,
        )
        await db.review_state.begin_review(
            self._conn,
            issue_id,
            pr_number=obs.pr_number,
            pr_url=obs.url,
            github_repo=obs.github_repo,
            issue_label=binding.issue_label,
            commit=False,
        )
        if review_configured:
            review_run_status = "running"
            if local_review_configured and not remote_review_configured:
                if not local_only_review_ready:
                    review_run_status = db.runs.NEEDS_APPROVAL_STATUS
            review_run_id = str(uuid.uuid4())
            await db.runs.create(
                self._conn,
                id=review_run_id,
                issue_id=issue_id,
                stage="review",
                status=review_run_status,
                pid=None,
                started_at=observed_at,
                commit=False,
            )
            if remote_review_configured:
                target_state = binding.linear_states.code_review
            elif local_only_review_ready:
                target_state = binding.linear_states.local_code_review
            else:
                target_state = binding.linear_states.needs_approval
                await db.operator_waits.upsert(
                    self._conn,
                    issue_id=issue_id,
                    run_id=review_run_id,
                    kind=db.operator_waits.KIND_REVIEW_FAILED,
                    linear_team_key=binding.linear_team_key,
                    github_repo=binding.github_repo,
                    issue_label=binding.issue_label or "",
                    created_at=observed_at,
                    provider=binding.provider,
                    tracker_provider=binding.tracker_provider,
                    tracker_site=binding.tracker_site,
                    commit=False,
                )
        else:
            # No review configured: the success path routes straight to merge
            # with review_bypassed=True and starts no review stage. Land the
            # issue in the merge-active lane (in_progress) so the merge poller
            # picks it up.
            target_state = binding.linear_states.in_progress
        await self._move_issue_to_state(
            tracker_issue_id=tracker_issue_id,
            tracker_ctx=tracker_ctx,
            team_key=team_key,
            state_name=target_state,
        )
        if wait is not None and not (
            local_review_configured
            and not remote_review_configured
            and not local_only_review_ready
        ):
            await db.operator_waits.delete(
                self._conn,
                issue_id,
                wait.run_id,
                commit=False,
            )
        if remote_review_configured:
            return _PostCommitReviewRequest(
                github_repo=obs.github_repo,
                pr_number=obs.pr_number,
            )
        return None

    async def _local_review_completed_for_adoption(
        self,
        *,
        issue_id: str,
        pr_created_at: str,
    ) -> bool:
        latest_implement = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=issue_id,
            stage="implement",
        )
        if latest_implement is None or _parse_rfc3339(
            latest_implement.started_at
        ) > _parse_rfc3339(pr_created_at):
            return False
        latest_local_review = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=issue_id,
            stage="local_review",
            started_at_gte=latest_implement.started_at,
        )
        if (
            latest_local_review is None
            or latest_local_review.status != "completed"
        ):
            return False
        latest_fix = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=issue_id,
            stage="review_fix",
        )
        if latest_fix is not None and _parse_rfc3339(
            latest_fix.started_at
        ) > _parse_rfc3339(latest_local_review.started_at):
            return False
        return True

    async def _move_issue_to_state(
        self,
        *,
        tracker_issue_id: str,
        tracker_ctx: TrackerContext | None,
        team_key: str,
        state_name: str,
    ) -> None:
        """Move the tracked issue to ``state_name``, aborting adoption on failure.

        A failed move must NOT commit. Adoption deletes the operator wait and
        writes ``issue_prs``/``review_state``, but the review/merge pollers
        reject the still-``Blocked`` issue and close the adopted run — and with
        the wait gone the orphan probe never re-fires (``wait is None``), so the
        issue is stuck for good with no auto-recovery. Raising here propagates
        to the reconcile transaction's rollback (reconciler ``except`` →
        ``rollback``), so the next tick re-probes the intact wait and retries.
        Transient Linear errors route through ``_BackoffRequested`` like the
        other tracker calls.
        """
        if not state_name:
            raise LinearError(
                f"missing Linear state {state_name!r} for {tracker_issue_id} "
                "during adoption"
            )
        tracker = self.tracker(tracker_ctx)
        try:
            states = await tracker.team_states(team_key)
        except LinearError as e:
            if _should_backoff(str(e)):
                raise _BackoffRequested(source=SOURCE_LINEAR, error=str(e)) from e
            raise
        state_id = states.get(state_name)
        if state_id is None:
            raise LinearError(
                f"missing Linear state {state_name!r} for {tracker_issue_id} "
                "during adoption"
            )
        try:
            await tracker.move_issue(tracker_issue_id, state_id)
        except LinearError as e:
            if _should_backoff(str(e)):
                raise _BackoffRequested(source=SOURCE_LINEAR, error=str(e)) from e
            raise

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
            closed_prs = _closed_unmerged_prs(github_prs)
            if not closed_prs:
                raise RuntimeError("cannot clear closed PR drift without a closed PR")
            await db.operator_waits.delete(
                self._conn,
                issue_id,
                wait.run_id,
                commit=False,
            )
            for pr in closed_prs:
                deleted = await db.issue_prs.delete(
                    self._conn,
                    issue_id=issue_id,
                    github_repo=pr.github_repo,
                    pr_number=pr.pr_number,
                    commit=False,
                )
                if not deleted:
                    raise RuntimeError(
                        "could not delete closed PR row for "
                        f"{pr.github_repo}#{pr.pr_number}"
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
        (
            binding.linear_team_key,
            binding.github_repo,
            binding.issue_label or "",
            binding.tracker_provider,
            binding.tracker_site,
        ),
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
        return (
            wait is not None
            and wait.kind == db.operator_waits.KIND_MERGE
            and bool(_closed_unmerged_prs(github_prs))
        )
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


def _closed_unmerged_prs(
    github_prs: list[GithubPrObservation],
) -> list[GithubPrObservation]:
    return [
        pr
        for pr in github_prs
        if pr.error is None and pr.state.upper() == "CLOSED" and not pr.merged
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
    "ACTION_ADOPTED",
    "ACTION_CLEARED",
    "ACTION_NOTED",
    "ACTION_OBSERVED",
    "ACTION_WOULD_CLEAR",
    "DRIFT_LINEAR_STATE_DONE",
    "DRIFT_MERGE_ZOMBIE",
    "DRIFT_ORPHAN_PR_OPEN",
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
