"""`_OrchestratorBase` — the state + foundation layer of the poll loop (SYM-144).

The base owns the `__init__`, every in-memory state attribute (the many
`dict`/`set` keyed by `run_id`/`issue_id`, the semaphores, task sets, the
`_reconciler`, …) **and** the class-level annotation of each attribute, so the
domain layer that inherits it sees those types for free.

It also owns the foundation methods every domain calls — tracker/binding/
state-resolve — plus the small binding/tracker free helpers they lean on.
`Orchestrator` (in `__init__.py`) inherits this class.

Pure structural extraction: method bodies are byte-for-byte unchanged from the
pre-split `Orchestrator`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import aiosqlite

from ... import db
from ...agent.runner import Runner
from ...agent.runners.local import LocalRunner
from ...config import Config, RepoBinding
from ...github.client import GitHub, GitHubClient
from ...linear.client import LinearError
from ...linear.slash import SlashKind
from ...pipeline.cost_guard import UsageDelta
from ...pipeline.local_review_loop import LoopResult
from ...tracker import (
    DEFAULT_PROVIDER,
    DEFAULT_SITE,
    IssueTracker,
    StateCacheKey,
    TrackerContext,
    TrackerRegistry,
    context_for_binding,
)
from ...tracker import (
    Issue as LinearIssue,
)
from ...workspace import Workspace
from ..reconciler import Reconciler
from ._git import (
    _default_force_push,
    _default_push,
)

PushFn = Callable[[Path, str], Awaitable[None]]
BindingKey = tuple[str, str, str, str, str]


@dataclass(frozen=True)
class _ImplementHandoff:
    """Context carried from a blocked-run `$retry` to the fresh implement run."""

    blocked_reason: str
    operator_comment: str


@dataclass(frozen=True)
class _PendingDelivery:
    """Inputs needed to (re)run the post-completion delivery path.

    The completion gate has already passed, so the agent's work is final. A
    delivery-step failure (push / `ensure_pr` / review handoff) parks a
    ``deliver_failed`` operator wait keyed by ``run_id`` and stashes this
    context so a `$retry` can resume delivery on the existing branch without
    re-dispatching the agent or re-running the completion gate.
    """

    binding: RepoBinding
    issue: LinearIssue
    storage_issue_id: str
    run_id: str
    workspace_path: Path
    branch: str
    cumulative_usage: UsageDelta
    local_review_result: LoopResult | None
    # True when a deliver_failed `$retry` reacquired this workspace. Retry
    # contexts must prove the branch still carries work before pushing because
    # the original workspace was already released and may have been swept.
    retry_workspace_acquired: bool = False
    # True when rebuilt by `_resolve_pending_delivery` after a daemon restart
    # lost the in-memory stash. The workspace was re-acquired (possibly
    # re-cloned) and the live local-review verdict is gone, so the resume
    # path treats the gate as already-passed and skips degenerate audit
    # artifacts (e.g. an "iterations: 0" PR summary).
    reconstructed: bool = False


def _tracker_context_for_binding(binding: RepoBinding) -> TrackerContext:
    return context_for_binding(binding)


def _state_cache_key(binding: RepoBinding) -> StateCacheKey:
    return (binding.tracker_provider, binding.tracker_site, binding.linear_team_key)


def _register_configured_trackers(
    registry: TrackerRegistry,
    config: Config,
    tracker: IssueTracker,
) -> None:
    registry.register(DEFAULT_PROVIDER, DEFAULT_SITE, tracker)
    for binding in config.repos:
        ctx = _tracker_context_for_binding(binding)
        registry.register(
            ctx.provider,
            ctx.site,
            tracker,
            project_key=ctx.project_key,
        )


class _OrchestratorBase:
    """Owns the poll loop's state + foundation methods; `Orchestrator` extends it."""

    # --- attribute annotations (single source of truth for the whole class) ---
    config: Config
    _trackers: TrackerRegistry
    _conn: aiosqlite.Connection
    _shutdown: asyncio.Event
    _wake: asyncio.Event
    _web_commands: asyncio.Queue[tuple[str, SlashKind, str]]
    _gh: GitHubClient
    _runner: Runner
    _workspace: Workspace
    _push_fn: PushFn
    _force_push_fn: PushFn
    _clock: Callable[[], datetime] | None
    _states: dict[StateCacheKey, dict[str, str]]
    _dispatch_tasks: set[asyncio.Task[None]]
    _scheduled_issue_ids: set[str]
    _known_waiting_issue_ids: set[str]
    _scheduled_issue_refcounts: dict[str, int]
    _scheduled_binding_counts: dict[BindingKey, int]
    _schedule_lock: asyncio.Lock
    _comment_event_lock: asyncio.Lock
    _active_run_ids: set[str]
    _dispatch_run_ids: dict[str, str]
    _operator_wait_run_ids: set[str]
    _implement_failed_run_bindings: dict[str, RepoBinding]
    _implement_blocked_run_bindings: dict[str, RepoBinding]
    _deliver_failed_run_bindings: dict[str, RepoBinding]
    _pending_deliveries: dict[str, _PendingDelivery]
    _implement_handoffs: dict[str, _ImplementHandoff]
    _review_failed_run_bindings: dict[str, RepoBinding]
    _merge_needs_approval_bindings: dict[str, RepoBinding]
    _acceptance_rejected_run_bindings: dict[str, RepoBinding]
    _budget_exceeded_run_bindings: dict[str, RepoBinding]
    _runs_moved_to_in_progress: set[str]
    _review_poll_tasks: set[asyncio.Task[None]]
    _review_poll_run_ids: set[str]
    _review_poll_issue_ids: dict[str, str]
    _review_poll_run_tasks: dict[str, asyncio.Task[None]]
    _merge_wait_reconcile_issue_ids: set[str]
    _review_rearm_retry_run_ids: set[str]
    _review_no_signal_rearm_heads: set[tuple[str, str]]
    _parked_manual_merge_revival_issue_ids: set[str]
    _merged_linear_state_reconcile_ticks: int
    _merged_linear_state_drift_comment_keys: set[tuple[str, str]]
    _parked_closed_unmerged_comment_keys: set[tuple[str, str, int]]
    _parked_closed_unmerged_lock: asyncio.Lock
    _global_dispatch_sem: asyncio.Semaphore
    _binding_dispatch_sems: dict[BindingKey, asyncio.Semaphore]
    _review_fix_sem: asyncio.Semaphore
    _review_fix_binding_sems: dict[BindingKey, asyncio.Semaphore]
    _reconciler: Reconciler
    _reconcile_task: asyncio.Task[None] | None
    _merge_wait_reconcile_task: asyncio.Task[None] | None
    _reconcile_event_tasks: set[asyncio.Task[None]]

    def __init__(
        self,
        config: Config,
        tracker_or_registry: IssueTracker | TrackerRegistry,
        conn: aiosqlite.Connection,
        *,
        runner: Runner | None = None,
        gh: GitHubClient | None = None,
        workspace: Workspace | None = None,
        push_fn: PushFn | None = None,
        force_push_fn: PushFn | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        if isinstance(tracker_or_registry, TrackerRegistry):
            self._trackers = tracker_or_registry
        else:
            self._trackers = TrackerRegistry()
            _register_configured_trackers(self._trackers, config, tracker_or_registry)
        self._conn = conn
        self._shutdown = asyncio.Event()
        # Operator commands submitted from the web UI. Enqueued by the HTTP
        # handler, drained by the poll loop so they apply on the loop's turn
        # (never concurrently with `_tick` on the shared connection). `_wake`
        # interrupts the inter-tick sleep so a command applies near-instantly.
        self._wake = asyncio.Event()
        self._web_commands: asyncio.Queue[tuple[str, SlashKind, str]] = (
            asyncio.Queue()
        )
        self._gh: GitHubClient = gh if gh is not None else GitHub()
        self._runner: Runner = runner if runner is not None else LocalRunner()
        self._workspace: Workspace = (
            workspace
            if workspace is not None
            else Workspace(root=config.workspace_root, clone_fn=self._gh.repo_clone)
        )
        self._push_fn: PushFn = push_fn if push_fn is not None else _default_push
        self._force_push_fn: PushFn = (
            force_push_fn if force_push_fn is not None else _default_force_push
        )
        self._clock = clock
        # Cache of ((provider, site, team_key) -> {state_name: state_uuid}).
        # Re-fetched on startup; never mutated at runtime.
        self._states: dict[StateCacheKey, dict[str, str]] = {}
        self._dispatch_tasks: set[asyncio.Task[None]] = set()
        self._scheduled_issue_ids: set[str] = set()
        self._known_waiting_issue_ids: set[str] = set()
        self._scheduled_issue_refcounts: dict[str, int] = {}
        self._scheduled_binding_counts: dict[BindingKey, int] = {}
        self._schedule_lock = asyncio.Lock()
        self._comment_event_lock = asyncio.Lock()
        self._active_run_ids: set[str] = set()
        self._dispatch_run_ids: dict[str, str] = {}
        self._operator_wait_run_ids: set[str] = set()
        self._implement_failed_run_bindings: dict[str, RepoBinding] = {}
        self._implement_blocked_run_bindings: dict[str, RepoBinding] = {}
        self._deliver_failed_run_bindings: dict[str, RepoBinding] = {}
        # Pending delivery contexts, keyed by run_id. Set when a post-completion
        # delivery step fails and a `deliver_failed` wait is parked; consumed by
        # a `$retry` to resume push + `ensure_pr` + handoff on the existing
        # branch. In-memory only: if the daemon restarts, the `$retry` falls
        # back to reconstructing the context from the wait + workspace.
        self._pending_deliveries: dict[str, _PendingDelivery] = {}
        # Pending blocked-resume handoffs, keyed by storage issue_id. Set when an
        # operator `$retry`s an IMPLEMENT_BLOCKED wait; consumed by the next
        # implement dispatch to seed the fresh run's prompt. In-memory only: if
        # the daemon restarts between the `$retry` and the dispatch, the issue
        # simply re-runs without the handoff block.
        self._implement_handoffs: dict[str, _ImplementHandoff] = {}
        self._review_failed_run_bindings: dict[str, RepoBinding] = {}
        self._merge_needs_approval_bindings: dict[str, RepoBinding] = {}
        self._acceptance_rejected_run_bindings: dict[str, RepoBinding] = {}
        self._budget_exceeded_run_bindings: dict[str, RepoBinding] = {}
        self._runs_moved_to_in_progress: set[str] = set()
        self._review_poll_tasks: set[asyncio.Task[None]] = set()
        self._review_poll_run_ids: set[str] = set()
        # Maps issue_id → review poll run_id for issues in active review monitoring.
        # Populated alongside _review_poll_run_ids so skip-review slash commands
        # can be received even when no fix-run is active.
        self._review_poll_issue_ids: dict[str, str] = {}
        # Maps review monitor run_id → its asyncio Task so _handle_skip_review_intent
        # can cancel the task immediately, preventing mid-iteration fix-run dispatch.
        self._review_poll_run_tasks: dict[str, asyncio.Task[None]] = {}
        self._merge_wait_reconcile_issue_ids: set[str] = set()
        # Resurrected review monitors whose no-signal @codex re-arm hit a
        # transient GitHub read/write failure and should be retried while live.
        self._review_rearm_retry_run_ids: set[str] = set()
        # Live review monitors that already attempted a no-signal @codex
        # re-arm for the current PR head. Keyed by (run_id, head_sha) so a new
        # commit naturally allows one fresh ping.
        self._review_no_signal_rearm_heads: set[tuple[str, str]] = set()
        self._parked_manual_merge_revival_issue_ids: set[str] = set()
        self._merged_linear_state_reconcile_ticks = 0
        self._merged_linear_state_drift_comment_keys: set[tuple[str, str]] = set()
        self._parked_closed_unmerged_comment_keys: set[tuple[str, str, int]] = set()
        self._parked_closed_unmerged_lock = asyncio.Lock()
        self._global_dispatch_sem = asyncio.Semaphore(
            max(config.global_max_concurrent, 1)
        )
        self._binding_dispatch_sems: dict[BindingKey, asyncio.Semaphore] = {}
        # Review fix-runs also reserve normal dispatch capacity so they outrank
        # new implementation work, while this separate pool still lets us cap
        # review-fix concurrency independently.
        self._review_fix_sem = asyncio.Semaphore(
            max(config.global_max_concurrent, 1)
        )
        self._review_fix_binding_sems: dict[BindingKey, asyncio.Semaphore] = {}
        self._reconciler = Reconciler(
            config,
            conn,
            self._trackers,
            self._gh,
            clock=clock,
        )
        self._reconcile_task: asyncio.Task[None] | None = None
        self._merge_wait_reconcile_task: asyncio.Task[None] | None = None
        self._reconcile_event_tasks: set[asyncio.Task[None]] = set()

    def _now(self) -> datetime:
        if self._clock is not None:
            return self._clock()
        return datetime.now(UTC)  # noqa: clock — sanctioned wall-clock entry point

    def tracker(self, ctx: TrackerContext | RepoBinding | None = None) -> IssueTracker:
        if isinstance(ctx, RepoBinding):
            ctx = _tracker_context_for_binding(ctx)
        return self._trackers.resolve(ctx)

    @property
    def linear(self) -> IssueTracker:
        return self.tracker(TrackerContext())

    async def _stored_tracker_identity_for_issue(
        self, issue_id: str
    ) -> tuple[str, TrackerContext] | None:
        cur = await self._conn.execute(
            "SELECT tracker_issue_id, provider, site, team_key FROM issues WHERE id = ?",
            (issue_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        provider = str(row["provider"] or "")
        site = str(row["site"] or "")
        if not provider or not site:
            return None
        tracker_issue_id = str(row["tracker_issue_id"] or issue_id)
        project_key = str(row["team_key"] or "") if provider == "jira" else ""
        return tracker_issue_id, TrackerContext(
            provider=provider,
            site=site,
            project_key=project_key,
        )

    async def _storage_issue_ids_for_tracker_issue(
        self, issue_id: str, *, provider: str | None = None
    ) -> list[str]:
        query = """
            SELECT id
              FROM issues
             WHERE (id = ? OR tracker_issue_id = ?)
        """
        params: list[str] = [issue_id, issue_id]
        if provider is not None:
            query += " AND provider = ?"
            params.append(provider)
        query += """
             ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END, id
        """
        params.append(issue_id)
        cur = await self._conn.execute(query, params)
        rows = await cur.fetchall()
        return [str(row["id"]) for row in rows]

    async def _storage_issue_id_for_tracker_issue(
        self, issue_id: str, tracker_ctx: TrackerContext
    ) -> str:
        cur = await self._conn.execute(
            """
            SELECT id
              FROM issues
             WHERE provider = ?
               AND site = ?
               AND (id = ? OR tracker_issue_id = ?)
             ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END
             LIMIT 1
            """,
            (
                tracker_ctx.provider,
                tracker_ctx.site,
                issue_id,
                issue_id,
                issue_id,
            ),
        )
        row = await cur.fetchone()
        if row is None:
            return issue_id
        return str(row["id"])

    async def _stored_tracker_context_for_issue(
        self, issue_id: str, *, provider: str | None = None
    ) -> TrackerContext | None:
        if provider is None:
            identity = await self._stored_tracker_identity_for_issue(issue_id)
            if identity is None:
                return None
            return identity[1]

        cur = await self._conn.execute(
            """
            SELECT provider, site, team_key
              FROM issues
             WHERE provider = ?
               AND (id = ? OR tracker_issue_id = ?)
             ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END
             LIMIT 1
            """,
            (provider, issue_id, issue_id, issue_id),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        row_provider = str(row["provider"] or "")
        site = str(row["site"] or "")
        if not row_provider or not site:
            return None
        project_key = str(row["team_key"] or "") if row_provider == "jira" else ""
        return TrackerContext(provider=row_provider, site=site, project_key=project_key)

    async def _tracker_context_for_issue(self, issue_id: str) -> TrackerContext:
        return await self._stored_tracker_context_for_issue(issue_id) or TrackerContext()

    async def _tracker_identity_for_issue(
        self, issue_id: str
    ) -> tuple[str, TrackerContext]:
        return await self._stored_tracker_identity_for_issue(issue_id) or (
            issue_id,
            TrackerContext(),
        )

    async def _tracker_for_issue_id(self, issue_id: str) -> IssueTracker:
        return self.tracker(await self._tracker_context_for_issue(issue_id))

    def _configured_tracker_contexts(
        self, *, provider: str | None = None
    ) -> list[TrackerContext]:
        contexts: list[TrackerContext] = []
        seen: set[TrackerContext] = set()
        for binding in self.config.repos:
            if provider is not None and binding.tracker_provider != provider:
                continue
            ctx = _tracker_context_for_binding(binding)
            if ctx in seen:
                continue
            seen.add(ctx)
            contexts.append(ctx)
        if not contexts:
            contexts.append(
                TrackerContext(provider=provider or DEFAULT_PROVIDER, site=DEFAULT_SITE)
            )
        return contexts

    async def _lookup_webhook_issue(
        self, issue_id: str, *, provider: str | None = None
    ) -> tuple[LinearIssue, TrackerContext]:
        stored_ctx = await self._stored_tracker_context_for_issue(
            issue_id, provider=provider
        )
        if stored_ctx is not None:
            return await self.tracker(stored_ctx).lookup_issue(issue_id), stored_ctx

        not_found: LinearError | None = None
        for ctx in self._configured_tracker_contexts(provider=provider):
            try:
                return await self.tracker(ctx).lookup_issue(issue_id), ctx
            except LinearError as exc:
                if not str(exc).startswith(f"issue not found: {issue_id}"):
                    raise
                not_found = exc
        if not_found is not None:
            raise not_found
        ctx = TrackerContext(provider=provider or DEFAULT_PROVIDER, site=DEFAULT_SITE)
        return await self.tracker(ctx).lookup_issue(issue_id), ctx

    async def _states_for_binding(self, binding: RepoBinding) -> dict[str, str]:
        state_key = _state_cache_key(binding)
        states = self._states.get(state_key)
        if states is None:
            # Older tests and long-lived in-process callers may have seeded
            # the pre-refactor team-key cache directly. Normalize it on read.
            legacy_states = cast(dict[object, dict[str, str]], self._states).get(
                binding.linear_team_key
            )
            if isinstance(legacy_states, dict):
                states = legacy_states
                self._states[state_key] = states
        if states is None:
            states = await self.tracker(binding).team_states(binding.linear_team_key)
            self._states[state_key] = states
        return states

    def _binding_for_issue(
        self, issue: LinearIssue, tracker_ctx: TrackerContext | None = None
    ) -> RepoBinding | None:
        for binding in self.config.repos:
            if (
                tracker_ctx is not None
                and _tracker_context_for_binding(binding) != tracker_ctx
            ):
                continue
            if binding.linear_team_key != issue.team_key:
                continue
            if binding.issue_label and binding.issue_label not in issue.labels:
                continue
            return binding
        return None

    def _binding_for_review(
        self,
        issue: LinearIssue,
        state: db.review_state.ReviewState,
        tracker_ctx: TrackerContext | None = None,
    ) -> RepoBinding | None:
        if state.github_repo:
            for binding in self.config.repos:
                if (
                    tracker_ctx is not None
                    and _tracker_context_for_binding(binding) != tracker_ctx
                ):
                    continue
                if binding.linear_team_key != issue.team_key:
                    continue
                if binding.github_repo != state.github_repo:
                    continue
                if (binding.issue_label or "") != state.issue_label:
                    continue
                return binding
            return None
        return self._binding_for_issue(issue, tracker_ctx=tracker_ctx)
