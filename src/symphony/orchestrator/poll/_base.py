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

# --- imports merged from the pre-split poll/__init__.py (SYM-151) ---
import json
import logging
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import aiosqlite

from ... import db
from ...agent.activity import (
    ActivityPublishReason,
    ActivitySession,
    ActivitySettings,
    digest_fingerprint,
    format_activity_digest,
)
from ...agent.codex_models import DEFAULT_CODEX_MODEL
from ...agent.model_usage import ModelUsage, parse_model_usage
from ...agent.process import parse_event_line
from ...agent.prompt import implement_prompt
from ...agent.runner import Runner, RunnerSpec
from ...agent.runners.local import LocalRunner
from ...config import Config, RepoBinding, binding_natural_key
from ...effective_config import ConfigBootError, assemble_effective_config
from ...github.client import GitHub, GitHubClient, GitHubError
from ...github.webhook import GitHubWebhookEvent
from ...linear.client import LinearError, comment_from_webhook_payload
from ...linear.slash import SlashIntent, SlashKind
from ...linear.templates import (
    CommentVars,
    awaiting_approval,
    budget_exceeded,
    failed,
    implement_already_satisfied,
    implement_blocked,
    truncate_body,
)
from ...notify import EVENT_OPERATOR_WAIT, EVENT_RUN_FAILED, TelegramNotifier, build_message
from ...pipeline.cost_guard import (
    UsageCostEstimator,
    UsageDelta,
)
from ...pipeline.local_review import StreamApiError, extract_last_agent_message
from ...pipeline.local_review_loop import LoopOutcome, LoopResult
from ...pipeline.state_machine import on_runner_event
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
    Comment as LinearComment,
)
from ...tracker import (
    Issue as LinearIssue,
)
from ...workspace import Workspace
from ..reconciler import Reconciler
from ._git import (
    _branch_ahead_of_base,
    _default_force_push,
    _default_push,
    _git_status_short,
    _workspace_dirty_files,
    _workspace_ref_landed_in_base,
)
from ._helpers import (
    _add_run_usage,
    _local_review_termination_reason,
    _parse_optional_datetime,
    _parse_rfc3339,
    _sum_usage,
    _termination_kwargs,
    _TerminationKwargs,
    build_fix_runner_command,
    build_runner_command,
    pr_number_from_url,
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


def _binding_key(binding: RepoBinding) -> BindingKey:
    # Single source of truth for the tuple layout — the persisted
    # `config_bindings` natural key must stay byte-compatible with this.
    return binding_natural_key(binding)


def _queue_scope(binding: RepoBinding) -> str:
    """`tracker_queue.scope` for a binding: `_binding_key` minus the team
    (already its own column), so two bindings on one team never clobber each
    other's snapshot rows."""
    return "#".join(_binding_key(binding)[1:])


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


class SlashHandlerFailure(RuntimeError):
    """Raised from a `_handle_*_slash_intent` when a critical Linear/GitHub
    call fails mid-handler (e.g. `move_issue` cannot reach the target state).

    The outer `_handle_unseen_slash_comment` catches this, posts a
    `command_rejected` Linear comment with `reason`, and intentionally does
    NOT mark the comment as seen so the next poll tick can retry. This
    prevents the silent-drop family (SYM-32, #59, #104) where a slash command
    is read off Linear, advances the cursor, but never triggers the
    underlying state transition.
    """

    def __init__(self, slash_text: str, reason: str) -> None:
        super().__init__(reason)
        self.slash_text = slash_text
        self.reason = reason


class _OrchestratorBase:
    """Owns the poll loop's state + foundation methods; `Orchestrator` extends it."""

    if TYPE_CHECKING:
        # Sibling-domain methods provided by the concrete `Orchestrator`
        # (defined on the mixins it is assembled from). Declared here so
        # `mypy --strict` resolves the cross-domain calls the poll loop makes.
        def _cancel_deliver_failed_review_poll_tasks(self, issue_id: str) -> None: ...

        async def _clear_review_rearm_retry(self, run_id: str) -> None: ...

        async def _drain_web_commands(self) -> None: ...

        async def _handle_unseen_slash_comment(
            self, issue_id: str, run_id: str, comment: LinearComment
        ) -> bool: ...

        async def _parked_manual_merge_run_id_for_issue(self, issue_id: str) -> str | None: ...

        async def _poll_merge_candidates(self) -> list[asyncio.Task[None]]: ...

        async def _poll_review_runs(self) -> list[asyncio.Task[None]]: ...

        async def _poll_slash_commands(self) -> None: ...

        async def _post_command_rejected(
            self, issue_id: str, slash_text: str, reason: str
        ) -> None: ...

        def _ready_binding_for_issue(
            self, issue: LinearIssue, tracker_ctx: TrackerContext | None = None
        ) -> RepoBinding | None: ...

        async def _reconcile_auto_recoverable_merge_waits(
            self, *, reason: str = "manual"
        ) -> int: ...

        async def _reconcile_merged_issues_linear_state(self) -> int: ...

        async def _reconcile_orphaned_merge_runs(self, *, reason: str = "manual") -> int: ...

        async def _reconcile_parked_closed_unmerged_pr_event(
            self, event: GitHubWebhookEvent
        ) -> int: ...

        async def _resurrect_review_runs(self) -> list[asyncio.Task[None]]: ...

        @asynccontextmanager
        async def _review_fix_dispatch_slot(
            self,
            binding: RepoBinding,
            issue: LinearIssue,
            *,
            dispatch_capacity_held: bool = False,
        ) -> AsyncIterator[None]:
            yield

        async def _run_auto_recoverable_merge_wait_reconciler(
            self, shutdown: asyncio.Event
        ) -> None: ...

        async def _scan_binding(self, binding: RepoBinding) -> list[asyncio.Task[None]]: ...

        async def _schedule_parked_manual_merge_revival_for_issue_event(
            self,
            *,
            issue: LinearIssue,
            old_state_id: str | None,
            old_state_name: str | None,
            new_state_id: str | None,
            new_state_name: str | None,
        ) -> asyncio.Task[None] | None: ...

        async def _schedule_ready_issue(
            self, binding: RepoBinding, issue: LinearIssue
        ) -> asyncio.Task[None] | None: ...

        def _slash_command_run_eligible(self, run_id: str) -> bool: ...

        @staticmethod
        def _slash_text(intent: SlashIntent) -> str: ...

        async def _track_review_failed_wait(
            self, issue_id: str, run_id: str, binding: RepoBinding
        ) -> None: ...

    # --- attribute annotations (single source of truth for the whole class) ---
    config: Config
    _trackers: TrackerRegistry
    _tracker_factory: Callable[[RepoBinding], IssueTracker] | None
    _reload_bindings_from_db: bool
    _config_write_lock: asyncio.Lock
    _binding_keys: frozenset[BindingKey] | None
    _stale_tracker_contexts: set[TrackerContext]
    _hot_added_trackers: dict[TrackerContext, IssueTracker]
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
    _notifier: TelegramNotifier
    _states: dict[StateCacheKey, dict[str, str]]
    _validated_waiting_state_bindings: set[BindingKey]
    _dispatch_tasks: set[asyncio.Task[None]]
    _scheduled_issue_ids: set[str]
    _known_waiting_issue_ids: set[str]
    _scheduled_issue_refcounts: dict[str, int]
    _scheduled_binding_counts: dict[BindingKey, int]
    _schedule_lock: asyncio.Lock
    _dispatch_pause_lock: asyncio.Lock
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
        reload_bindings_from_db: bool = False,
        tracker_factory: Callable[[RepoBinding], IssueTracker] | None = None,
    ) -> None:
        self.config = config
        if isinstance(tracker_or_registry, TrackerRegistry):
            self._trackers = tracker_or_registry
            # A production registry that can grow: `tracker_factory` builds a
            # real client from `Secrets` when a reload introduces a context the
            # registry never saw at boot.
            self._tracker_factory = tracker_factory
        else:
            self._trackers = TrackerRegistry()
            _register_configured_trackers(self._trackers, config, tracker_or_registry)
            # Single-tracker deployments (and the test harness) hot-add by
            # reusing the one tracker for any new context.
            self._tracker_factory = tracker_factory or (lambda _binding: tracker_or_registry)
        self._conn = conn
        self._shutdown = asyncio.Event()
        # Operator commands submitted from the web UI. Enqueued by the HTTP
        # handler, drained by the poll loop so they apply on the loop's turn
        # (never concurrently with `_tick` on the shared connection). `_wake`
        # interrupts the inter-tick sleep so a command applies near-instantly.
        self._wake = asyncio.Event()
        self._web_commands: asyncio.Queue[tuple[str, SlashKind, str]] = asyncio.Queue()
        # Daemon-level dispatch kill-switch (SYM-170). When True, the poll loop
        # and webhook path start no new runs for Ready issues; in-flight runs
        # (and their review/merge/acceptance follow-ups) are unaffected. In
        # memory only: a daemon restart clears it back to running.
        self._dispatch_paused = False
        # Hot-apply at the tick boundary (SYM-189): when True, `_tick` re-reads
        # all bindings from the config DB via the shared effective-config
        # assembly at the start of every poll. Off for YAML/single-tracker
        # deployments and the test harness, whose config is fixed at boot.
        self._reload_bindings_from_db = reload_bindings_from_db
        # Serializes a config write's multi-row transaction against the tick's
        # binding reload on the shared connection, so a reload never observes an
        # uncommitted save (the write path itself lands in a later slice). The
        # reload takes it; a config write will take it too.
        self._config_write_lock = asyncio.Lock()
        # The binding natural keys applied on the last reload. `None` until the
        # first tick, so the first pass always reacts (prunes stale
        # `tracker_queue` scopes + registers trackers), and every later pass
        # reacts only when the set actually changed — the one-shot boot prune
        # becomes a reaction to the binding set changing.
        self._binding_keys: frozenset[BindingKey] | None = None
        # Tracker contexts whose constructor payload (e.g. a Jira binding's
        # declared `states`) changed on a reload that left the natural key —
        # and so `_binding_keys` — unchanged. `_react_to_binding_set` must not
        # skip its reaction just because the key set matches (SYM-189).
        self._stale_tracker_contexts: set[TrackerContext] = set()
        # Clients built by `_hot_add_trackers` for a provider/site/project
        # never seen at boot (or rebuilt for a stale context), keyed by that
        # context. Unlike boot trackers — entered through
        # `_configured_tracker_registry`'s `AsyncExitStack` — these have no
        # other owner: `_close_removed_hot_added_trackers` closes one as soon
        # as its binding disappears, and any still here at process exit close
        # in `aclose_hot_added_trackers` (SYM-189).
        self._hot_added_trackers: dict[TrackerContext, IssueTracker] = {}
        # Serializes toggling `_dispatch_paused` against the final
        # check-then-insert in `_dispatch_one`, so a pause request can never
        # land in the middle of that critical section.
        self._dispatch_pause_lock = asyncio.Lock()
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
        # Telegram push for attention-needed events (SYM-171). A no-op unless
        # both token + chat id are configured in the environment.
        self._notifier = TelegramNotifier(config.telegram_bot_token, config.telegram_chat_id)
        # Cache of ((provider, site, team_key) -> {state_name: state_uuid}).
        # Re-fetched on startup; never mutated at runtime.
        self._states: dict[StateCacheKey, dict[str, str]] = {}
        # Bindings whose `linear_states.waiting` has been checked against the
        # tracker's actual workflow — tracked separately from `_states` since
        # a state_key cache hit can come from a sibling binding, not this
        # one's own load (SYM-189).
        self._validated_waiting_state_bindings: set[BindingKey] = set()
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
        self._global_dispatch_sem = asyncio.Semaphore(max(config.global_max_concurrent, 1))
        self._binding_dispatch_sems: dict[BindingKey, asyncio.Semaphore] = {}
        # Review fix-runs also reserve normal dispatch capacity so they outrank
        # new implementation work, while this separate pool still lets us cap
        # review-fix concurrency independently.
        self._review_fix_sem = asyncio.Semaphore(max(config.global_max_concurrent, 1))
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

    async def _tracker_identity_for_issue(self, issue_id: str) -> tuple[str, TrackerContext]:
        return await self._stored_tracker_identity_for_issue(issue_id) or (
            issue_id,
            TrackerContext(),
        )

    async def _tracker_for_issue_id(self, issue_id: str) -> IssueTracker:
        return self.tracker(await self._tracker_context_for_issue(issue_id))

    def _configured_tracker_contexts(self, *, provider: str | None = None) -> list[TrackerContext]:
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
        stored_ctx = await self._stored_tracker_context_for_issue(issue_id, provider=provider)
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
        # Tracked per binding, not per state_key: a state_key cache hit can
        # come from a *sibling* binding on the same team (e.g. a second
        # hot-added github_repo) that warmed the cache first. Validating only
        # on a state_key miss would let this binding's own `waiting` state go
        # unchecked forever — mirrors the check `warmup` runs for boot
        # bindings, so a hot-added/mid-run binding with a `waiting` state
        # absent from its Linear workflow fails loud instead of producing
        # wrong auto-unblock behavior (SYM-189).
        binding_key = _binding_key(binding)
        if binding_key not in self._validated_waiting_state_bindings:
            self._validate_waiting_state(binding, states)
            self._validated_waiting_state_bindings.add(binding_key)
        return states

    def _binding_for_issue(
        self, issue: LinearIssue, tracker_ctx: TrackerContext | None = None
    ) -> RepoBinding | None:
        for binding in self.config.repos:
            if tracker_ctx is not None and _tracker_context_for_binding(binding) != tracker_ctx:
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
                if tracker_ctx is not None and _tracker_context_for_binding(binding) != tracker_ctx:
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

    async def warmup(self) -> None:
        """One-time startup work: cache team workflow states, validate auth."""
        viewer_keys_by_ctx: dict[TrackerContext, list[str]] = {}
        for binding in self.config.repos:
            ctx = _tracker_context_for_binding(binding)
            viewer_keys = viewer_keys_by_ctx.get(ctx)
            if viewer_keys is None:
                viewer_keys = await self.tracker(ctx).viewer_team_keys()
                viewer_keys_by_ctx[ctx] = viewer_keys
                log.info("linear viewer sees teams: %s", viewer_keys)
            if binding.linear_team_key not in viewer_keys:
                log.warning(
                    "team %s configured but not visible to API key — "
                    "the binding will produce no work",
                    binding.linear_team_key,
                )
                continue
            state_key = _state_cache_key(binding)
            self._states[state_key] = await self.tracker(binding).team_states(
                binding.linear_team_key
            )
            self._validate_waiting_state(binding, self._states[state_key])
            self._validated_waiting_state_bindings.add(_binding_key(binding))

    def _validate_waiting_state(self, binding: RepoBinding, states: dict[str, str]) -> None:
        waiting = binding.linear_states.waiting
        if waiting is None:
            return
        if waiting not in states:
            available = sorted(states.keys())
            raise LinearError(
                f"{binding.linear_team_key} declares waiting state {waiting!r} "
                f"for {binding.github_repo}, but it is not in the Linear workflow; "
                f"available states: {available}"
            )

    async def shutdown(self) -> None:
        self._shutdown.set()
        self._wake.set()

    async def aclose_hot_added_trackers(self) -> None:
        """Close every client `_hot_add_trackers` built at runtime. Boot
        clients are entered through `_configured_tracker_registry`'s
        `AsyncExitStack` and close there; a hot-added one has no other owner,
        so it would otherwise leak its `httpx.AsyncClient` through process
        exit (SYM-189). Call once, after `run()` returns."""
        for tracker in self._hot_added_trackers.values():
            try:
                await tracker.aclose()
            except Exception:  # noqa: BLE001 — must not block shutdown
                log.exception("failed to close hot-added tracker")
        self._hot_added_trackers.clear()

    async def run(self) -> None:
        """The single long-lived task. Cancellation-safe."""
        await self.warmup()
        await self._restore_operator_waits()
        await self._reconcile_orphaned_merge_runs(reason="startup")
        await self._reconcile_auto_recoverable_merge_waits(reason="startup")
        self._merge_wait_reconcile_task = asyncio.create_task(
            self._run_auto_recoverable_merge_wait_reconciler(self._shutdown)
        )
        self._reconcile_task = asyncio.create_task(self._reconciler.run(self._shutdown))
        log.info("orchestrator entering poll loop (interval=%ds)", self.config.poll_interval_secs)
        try:
            while not self._shutdown.is_set():
                # Clear before draining so a command enqueued during this
                # iteration re-sets `_wake` and is picked up immediately rather
                # than waiting out the full poll interval.
                self._wake.clear()
                await self._drain_web_commands()
                try:
                    await self._tick()
                except Exception:  # noqa: BLE001 — must not kill the loop
                    log.exception("poll cycle failed")
                if self._shutdown.is_set():
                    break
                try:
                    await asyncio.wait_for(
                        self._wake.wait(), timeout=self.config.poll_interval_secs
                    )
                except TimeoutError:
                    pass
        finally:
            if self._merge_wait_reconcile_task is not None:
                self._merge_wait_reconcile_task.cancel()
                try:
                    await self._merge_wait_reconcile_task
                except asyncio.CancelledError:
                    pass
            if self._reconcile_task is not None:
                self._reconcile_task.cancel()
                try:
                    await self._reconcile_task
                except asyncio.CancelledError:
                    pass
            await self.drain_reconcile_event_tasks(cancel=True)
            await self.drain_dispatch_tasks(cancel=True)

    def _schedule_reconcile_task(
        self, awaitable: Awaitable[int], *, source: str
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(self._run_reconcile_task(awaitable, source=source))
        self._reconcile_event_tasks.add(task)
        task.add_done_callback(self._reconcile_event_task_done)
        return task

    async def _run_reconcile_task(self, awaitable: Awaitable[int], *, source: str) -> None:
        try:
            observed = await awaitable
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("external reconcile task failed source=%s", source)
            return
        log.info(
            "external reconcile task complete source=%s observations=%d",
            source,
            observed,
        )

    def _reconcile_event_task_done(self, task: asyncio.Task[None]) -> None:
        self._reconcile_event_tasks.discard(task)

    async def drain_reconcile_event_tasks(self, *, cancel: bool = False) -> None:
        if cancel:
            for task in tuple(self._reconcile_event_tasks):
                task.cancel()
        while self._reconcile_event_tasks:
            await asyncio.gather(
                *tuple(self._reconcile_event_tasks),
                return_exceptions=True,
            )

    async def _tick(self) -> list[asyncio.Task[None]]:
        scheduled: list[asyncio.Task[None]] = []
        if self._reload_bindings_from_db:
            try:
                await self._reload_bindings()
            except Exception:  # noqa: BLE001 — must not kill the loop
                log.exception("binding reload failed")
        # React to the current binding set — the first tick always fires (prunes
        # stale `tracker_queue` scopes, registers boot trackers), later ticks
        # only when a reload changed the set. Runs before the scan loop so a
        # hot-added binding's tracker is registered before `_scan_binding`
        # resolves it.
        await self._react_to_binding_set()
        await self._restore_operator_waits()
        self._merged_linear_state_reconcile_ticks += 1
        if (
            self._merged_linear_state_reconcile_ticks % MERGED_LINEAR_STATE_RECONCILE_TICK_INTERVAL
            == 0
        ):
            try:
                corrected = await self._reconcile_merged_issues_linear_state()
                if corrected:
                    log.info(
                        "reconciled %d merged Linear issue(s) back to Done",
                        corrected,
                    )
            except Exception:  # noqa: BLE001 — must not kill the loop
                log.exception("merged issue Linear state reconcile failed")
        try:
            await self._retry_pending_notifications()
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("telegram notification retry failed")
        try:
            scheduled.extend(await self._poll_merge_candidates())
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("merge candidate poll failed")
        try:
            scheduled.extend(await self._poll_review_runs())
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("review poll failed")
        try:
            scheduled.extend(await self._resurrect_review_runs())
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("review resurrection failed")
        for binding in self.config.repos:
            ctx = _tracker_context_for_binding(binding)
            # A stale context's registered tracker is the pre-edit client
            # pending a rebuild (`_hot_add_trackers`) — scanning with it would
            # serve a constructor payload (e.g. Jira states) the binding no
            # longer declares (SYM-189 review fix).
            if not self._tracker_context_registered(ctx) or ctx in self._stale_tracker_contexts:
                log.warning(
                    "skipping scan for %s: tracker context %s not registered "
                    "or pending rebuild (hot-add likely failed)",
                    binding.linear_team_key,
                    ctx,
                )
                continue
            try:
                scheduled.extend(await self._scan_binding(binding))
            except Exception:  # noqa: BLE001 — one dead lane must not starve the rest
                log.exception("scan failed for binding %s", binding.linear_team_key)
        try:
            await self._poll_slash_commands()
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("slash command poll failed")
        return scheduled

    @property
    def config_write_lock(self) -> asyncio.Lock:
        """Guards a config write's multi-row transaction against the tick's
        binding reload on the shared connection (SYM-189). A config write must
        run its whole transaction while holding this lock so the reload — which
        also takes it — only ever observes committed state."""
        return self._config_write_lock

    async def _reload_bindings(self) -> None:
        """Re-read all bindings (enabled + disabled) from the config DB and
        hot-apply them to `self.config` (SYM-189).

        Taken under `_config_write_lock` so the reload never reads a config
        write's uncommitted multi-row transaction off the shared connection.
        Uses `boot_gates=False`: a mid-run assembly must never crash the loop —
        a bad edit or a transient DB error keeps the last good config.

        The per-stage contract lives here: the assembly rebuilds fresh
        `RepoBinding` objects from the raw DB payloads (env key names resolved
        on these copies, never written back), so a stage that starts after this
        reload reads the current row, while an already-spawned agent keeps the
        argv/env captured at its dispatch. `_react_to_binding_set` (called next
        in `_tick`) reconciles the tracker registry and queue scopes.
        """
        previous_states_by_state_key: dict[StateCacheKey, dict[BindingKey, object]] = {}
        previous_max_concurrent_by_key: dict[BindingKey, int] = {}
        # A Jira binding's `tracker_site` is its stable natural-key component —
        # editing `base_url` alone leaves both it and `state_key` unchanged, so
        # neither the hot-add "context already registered" check nor the
        # states comparison below would ever rebuild the tracker. Track it
        # per context (last-write-wins, mirroring `_hot_add_trackers`'
        # `representative` selection) and mark the context stale below when it
        # moves (SYM-189).
        previous_jira_base_url_by_ctx: dict[TrackerContext, str | None] = {}
        for binding in self.config.repos:
            previous_states_by_state_key.setdefault(_state_cache_key(binding), {})[
                _binding_key(binding)
            ] = binding.linear_states
            previous_max_concurrent_by_key[_binding_key(binding)] = binding.max_concurrent
            if binding.provider == "jira":
                previous_jira_base_url_by_ctx[_tracker_context_for_binding(binding)] = (
                    binding.base_url
                )
        async with self._config_write_lock:
            try:
                effective = await assemble_effective_config(
                    self._conn, self.config, boot_gates=False, is_reload=True
                )
            except ConfigBootError:
                log.exception("binding reload rejected invalid config; keeping current")
                return
        for binding in effective.repos:
            if binding.provider != "jira":
                continue
            ctx = _tracker_context_for_binding(binding)
            if (
                ctx in previous_jira_base_url_by_ctx
                and previous_jira_base_url_by_ctx[ctx] != binding.base_url
            ):
                self._stale_tracker_contexts.add(ctx)
        # `_states_for_binding` caches team workflow states by `_state_cache_key`
        # (provider, site, team) — coarser than a binding's natural key, so two
        # bindings can share one entry — but validates each binding's own
        # `waiting` state separately (`_validated_waiting_state_bindings`).
        # Compare both at the state_key granularity below (an edit that
        # changes `linear_states` together with a natural-key component absent
        # from the state-cache-key, e.g. `github_repo`, must still evict) and
        # per binding_key, so a sibling binding's warm cache can't hide this
        # binding's own edited/never-checked `waiting` state (SYM-189).
        new_states_by_state_key: dict[StateCacheKey, dict[BindingKey, object]] = {}
        for binding in effective.repos:
            new_states_by_state_key.setdefault(_state_cache_key(binding), {})[
                _binding_key(binding)
            ] = binding.linear_states
        for state_key, new_per_binding in new_states_by_state_key.items():
            previous_per_binding = previous_states_by_state_key.get(state_key, {})
            if new_per_binding != previous_per_binding:
                self._states.pop(state_key, None)
                # `for_binding` bakes a Jira binding's declared states into its
                # `JiraTracker` at construction (`_states`, returned as-is by
                # `team_states()`); a natural-key-preserving edit to those
                # states never triggers `_hot_add_trackers`'s "context already
                # registered" skip otherwise, so the tracker would keep
                # serving the pre-edit workflow forever. `project_key` aliases
                # `linear_team_key` for Jira, so `state_key` already is the
                # tracker context (SYM-189).
                provider, site, team_key = state_key
                if provider == "jira":
                    self._stale_tracker_contexts.add(
                        TrackerContext(provider=provider, site=site, project_key=team_key)
                    )
            # `_states_for_binding` only validates `linear_states.waiting` the
            # first time it loads a *state_key*, so a binding sharing an
            # already-warm state_key with a sibling binding (e.g. a second
            # hot-added github_repo on the same team) never gets its own
            # waiting state checked. Evict this binding's validated mark too,
            # on top of the state_key cache above, whenever its own
            # `linear_states` changed (or it's new) so it's re-checked on its
            # own next lookup regardless of the sibling's cache state
            # (SYM-189).
            for binding_key, new_linear_states in new_per_binding.items():
                if previous_per_binding.get(binding_key) != new_linear_states:
                    self._validated_waiting_state_bindings.discard(binding_key)
        # A state-cache-key with no surviving binding is stale too — the loop
        # above only ever touches keys still present in the new config, so a
        # removed binding's entry would otherwise linger forever (SYM-189).
        for state_key in list(self._states):
            if state_key not in new_states_by_state_key:
                self._states.pop(state_key, None)
        # `_binding_dispatch_sems`/`_review_fix_binding_sems` are sized once
        # from `binding.max_concurrent` via `setdefault` and never resized in
        # place — dropping the stale entry here lets the next dispatch recreate
        # it at the new capacity (SYM-189). In-flight work already holding the
        # old semaphore object is unaffected and drains under the old cap.
        for binding in effective.repos:
            key = _binding_key(binding)
            previous_max_concurrent = previous_max_concurrent_by_key.get(key)
            if previous_max_concurrent is None:
                continue
            if previous_max_concurrent != binding.max_concurrent:
                self._binding_dispatch_sems.pop(key, None)
                self._review_fix_binding_sems.pop(key, None)
        # A binding that's gone entirely leaves its semaphores behind forever
        # otherwise — a long-running daemon with churny bindings would
        # accumulate one stale entry per removed binding (SYM-189).
        current_binding_keys = {_binding_key(binding) for binding in effective.repos}
        for key in list(self._binding_dispatch_sems):
            if key not in current_binding_keys:
                self._binding_dispatch_sems.pop(key, None)
        for key in list(self._review_fix_binding_sems):
            if key not in current_binding_keys:
                self._review_fix_binding_sems.pop(key, None)
        # Same accumulation risk as the semaphores above (SYM-189).
        for key in list(self._validated_waiting_state_bindings):
            if key not in current_binding_keys:
                self._validated_waiting_state_bindings.discard(key)
        self.config = effective
        # The reconciler resolves bindings by iterating its own `config.repos`;
        # keep it pointed at the same effective config the poll loop uses.
        self._reconciler.config = effective

    async def _react_to_binding_set(self) -> None:
        """Reconcile in-memory state that must follow the binding set: register
        trackers for newly-seen contexts, rebuild any whose constructor
        payload changed under an unchanged natural key, close a hot-added
        tracker whose binding is gone, prune `tracker_queue` scopes for
        bindings that are gone, and publish only bindings whose tracker
        actually registered. A no-op when the set is unchanged and no
        tracker context is pending a rebuild."""
        keys = frozenset(_binding_key(binding) for binding in self.config.repos)
        if keys == self._binding_keys and not self._stale_tracker_contexts:
            return
        hot_add_ok = await self._hot_add_trackers()
        # Runs after the hot-add above so a still-live default-Linear alias
        # (SYM-189 review fix) is repointed away from a departing tracker
        # before that tracker closes.
        await self._close_removed_hot_added_trackers()
        # Only commit the new key set once the hot-add and the prune both
        # succeed, so a transient failure in either is retried on the next
        # tick instead of stranding an unscanned binding or stale scopes
        # until the set changes again.
        prune_ok = await self._prune_tracker_queue_scopes()
        self._publish_registered_bindings()
        if hot_add_ok and prune_ok:
            self._binding_keys = keys

    def _publish_registered_bindings(self) -> None:
        """Drop a binding whose tracker context never got registered (e.g. a
        missing/invalid secret failed `_tracker_factory`) from `self.config`/
        the reconciler's config, so every other consumer — not just the
        tick's own scan-loop guard — can't act on a binding backed by no
        tracker. `_reload_bindings` re-derives `self.config` fresh from the DB
        every tick, so this never permanently drops a binding: the next
        tick's hot-add attempt retries it (SYM-189 review fix)."""
        registered = [
            binding
            for binding in self.config.repos
            if self._tracker_context_registered(_tracker_context_for_binding(binding))
        ]
        if len(registered) == len(self.config.repos):
            return
        self.config = self.config.model_copy(update={"repos": registered})
        self._reconciler.config = self.config

    async def _hot_add_trackers(self) -> bool:
        """Register a tracker client for any binding whose provider/site (for
        Jira, /project) context the registry never saw at boot, and rebuild +
        replace one whose constructor payload changed (`_stale_tracker_contexts`)
        even though its context was already registered. Returns whether every
        binding's context is now registered."""
        all_registered = True
        # One representative binding per context, mirroring the last-write-wins
        # registration order `_configured_tracker_registry` used at boot — a
        # rebuild must construct from the same binding boot would have.
        representative: dict[TrackerContext, RepoBinding] = {}
        for binding in self.config.repos:
            representative[_tracker_context_for_binding(binding)] = binding
        handled_stale: set[TrackerContext] = set()
        for ctx, binding in representative.items():
            stale = ctx in self._stale_tracker_contexts
            if self._tracker_context_registered(ctx) and not stale:
                continue
            if self._tracker_factory is None:
                log.warning("cannot hot-add tracker for %s: no tracker factory", ctx)
                all_registered = False
                continue
            try:
                tracker = self._tracker_factory(binding)
            except Exception:  # noqa: BLE001 — must not kill the loop
                log.exception("failed to build tracker for hot-add: %s", ctx)
                all_registered = False
                continue
            old = self._trackers.get(ctx) if stale else None
            self._trackers.register(ctx.provider, ctx.site, tracker, project_key=ctx.project_key)
            self._hot_added_trackers[ctx] = tracker
            if stale:
                handled_stale.add(ctx)
                log.info("rebuilt tracker for %s: constructor payload changed", ctx)
            else:
                log.info("hot-added tracker for %s", ctx)
            # Single-tracker deployments reuse one client for every context
            # (see `Orchestrator.__init__`'s fallback factory) — never close
            # the client we just re-registered.
            if old is not None and old is not tracker:
                try:
                    await old.aclose()
                except Exception:  # noqa: BLE001 — must not kill the loop
                    log.exception("failed to close stale tracker for %s", ctx)
        # Drop a stale mark once rebuilt, or once its context no longer
        # belongs to any current binding — there's nothing left to rebuild.
        self._stale_tracker_contexts = {
            ctx
            for ctx in self._stale_tracker_contexts
            if ctx in representative and ctx not in handled_stale
        }
        # Boot registration aliases the first Linear binding it iterates to
        # `(DEFAULT_PROVIDER, DEFAULT_SITE)`, for callers like
        # `_external_linear_tracker` that resolve the "default" Linear client
        # without a full `TrackerContext` (e.g. a fresh install boots with no
        # Linear binding, so no alias is set, and hot-adds the first one
        # later). Keep the alias pointed at whichever Linear context is first
        # in the current binding order, mirroring boot's own selection
        # (SYM-189 review fix).
        first_linear_ctx = next(
            (ctx for ctx in representative if ctx.provider == DEFAULT_PROVIDER), None
        )
        if first_linear_ctx is not None:
            first_linear_tracker = self._trackers.get(first_linear_ctx)
            default_ctx = TrackerContext()
            if first_linear_tracker is not None and first_linear_tracker is not self._trackers.get(
                default_ctx
            ):
                self._trackers.register(DEFAULT_PROVIDER, DEFAULT_SITE, first_linear_tracker)
        return all_registered

    async def _close_removed_hot_added_trackers(self) -> None:
        """Close a hot-added client whose binding is gone — otherwise it only
        closes at process shutdown (`aclose_hot_added_trackers`), leaking its
        connection for the rest of the run if a DB-owned daemon hot-adds a
        binding for a new provider/site/project and that binding is later
        deleted (SYM-189 review fix). A client still serving another live
        context — single-tracker fallback deployments reuse one client for
        every context — is left open, just dropped from this bookkeeping."""
        live_contexts = {_tracker_context_for_binding(binding) for binding in self.config.repos}
        removed = [ctx for ctx in self._hot_added_trackers if ctx not in live_contexts]
        for ctx in removed:
            tracker = self._hot_added_trackers.pop(ctx)
            self._trackers.discard(ctx.provider, ctx.site, ctx.project_key)
            default_ctx = TrackerContext()
            if self._trackers.get(default_ctx) is tracker:
                self._trackers.discard(
                    default_ctx.provider, default_ctx.site, default_ctx.project_key
                )
            if tracker in self._hot_added_trackers.values():
                continue
            try:
                await tracker.aclose()
            except Exception:  # noqa: BLE001 — must not kill the loop
                log.exception("failed to close removed-binding tracker for %s", ctx)

    def _tracker_context_registered(self, ctx: TrackerContext) -> bool:
        try:
            self._trackers.resolve(ctx)
        except KeyError:
            return False
        return True

    async def _prune_tracker_queue_scopes(self) -> bool:
        """Drop `tracker_queue` rows from scopes no longer configured, so a
        removed/renamed binding's lanes can't linger in the UI. Returns whether
        the prune committed (False on a transient failure, so the caller retries
        next tick)."""
        try:
            await db.tracker_queue.prune_scopes(
                self._conn,
                keep=[
                    (binding.linear_team_key, _queue_scope(binding))
                    for binding in self.config.repos
                ],
            )
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("tracker queue scope prune failed")
            return False
        return True

    async def handle_linear_webhook(
        self, payload: dict[str, Any], *, provider: str | None = None
    ) -> WebhookDispatchResult:
        """Handle a verified Linear webhook payload.

        Webhooks are just another low-latency source for the same work the
        poll loop already performs: issue state changes enter the normal
        dispatch scheduler, and comment events enter the slash-command
        handler shared with `_poll_slash_commands`.
        """
        event_type = str(payload.get("type") or "").casefold()
        if event_type == "comment":
            return await self._handle_webhook_comment(payload, provider=provider)
        if event_type == "issue":
            return await self._handle_webhook_issue(payload, provider=provider)
        return WebhookDispatchResult(
            kind=event_type or "unknown",
            handled=False,
            detail="ignored event type",
        )

    async def handle_github_webhook(self, event: GitHubWebhookEvent) -> WebhookDispatchResult:
        """Accept a verified GitHub webhook event and audit external truth."""
        log.info(
            "github webhook event received: repo=%s type=%s action=%s pr=%s delivery=%s",
            event.repo,
            event.event_type,
            event.action,
            event.pr_number,
            event.delivery_id,
        )
        self._schedule_reconcile_task(
            self._reconciler.reconcile_github_event(event),
            source=f"github.{event.event_type}.{event.action or 'unknown'}",
        )
        if (
            event.event_type == "pull_request"
            and event.action == "closed"
            and event.pr_number is not None
            and not event.merged
        ):
            self._schedule_reconcile_task(
                self._reconcile_parked_closed_unmerged_pr_event(event),
                source="parked_closed.github.pull_request.closed",
            )
        if event.event_type == "pull_request" and event.pr_number is not None:
            self._schedule_reconcile_task(
                self._reconcile_auto_recoverable_merge_waits(
                    reason=f"github_webhook:{event.event_type}.{event.action or 'unknown'}"
                ),
                source=(f"merge_wait.github.{event.event_type}.{event.action or 'unknown'}"),
            )
        return WebhookDispatchResult(
            kind=f"github.{event.event_type}",
            handled=True,
            detail="reconcile scheduled",
        )

    async def _handle_webhook_comment(
        self, payload: Mapping[str, Any], *, provider: str | None = None
    ) -> WebhookDispatchResult:
        comment = _comment_from_webhook_payload(payload)
        if comment is None:
            return WebhookDispatchResult(
                kind="comment", handled=False, detail="missing comment fields"
            )
        issue_id = _comment_issue_id_from_webhook_payload(payload)
        if issue_id is None:
            return WebhookDispatchResult(kind="comment", handled=False, detail="missing issue id")
        candidate_issue_ids = await self._storage_issue_ids_for_tracker_issue(
            issue_id, provider=provider
        )
        if issue_id not in candidate_issue_ids:
            candidate_issue_ids.append(issue_id)
        await self._restore_operator_waits()
        storage_issue_id = issue_id
        run_id: str | None = None
        for candidate_issue_id in candidate_issue_ids:
            candidate_run_id = self._dispatch_run_ids.get(
                candidate_issue_id
            ) or self._review_poll_issue_ids.get(candidate_issue_id)
            if candidate_run_id is None or not self._slash_command_run_eligible(candidate_run_id):
                continue
            storage_issue_id = candidate_issue_id
            run_id = candidate_run_id
            break
        if run_id is None:
            for candidate_issue_id in candidate_issue_ids:
                candidate_run_id = await self._parked_manual_merge_run_id_for_issue(
                    candidate_issue_id
                )
                if candidate_run_id is None or not self._slash_command_run_eligible(
                    candidate_run_id
                ):
                    continue
                storage_issue_id = candidate_issue_id
                run_id = candidate_run_id
                break
        if run_id is None:
            return WebhookDispatchResult(kind="comment", handled=False, detail="no active run")
        try:
            handled = await self._handle_unseen_slash_comment(storage_issue_id, run_id, comment)
        except SlashHandlerFailure as exc:
            # Rejection has already been posted inside the lock, and the
            # comment was deliberately NOT marked seen so the next poll tick
            # can retry. Returning a successful dispatch result keeps the
            # webhook delivery dedupe claim in place — re-raising would let
            # `src/symphony/webhook.py` treat this as a failed delivery,
            # forget the claim, and the provider would retry quickly,
            # generating one extra rejection comment per webhook retry.
            return WebhookDispatchResult(
                kind="comment",
                handled=True,
                detail=f"slash handler failed: {exc.reason}",
            )
        if not handled:
            return WebhookDispatchResult(
                kind="comment", handled=False, detail="comment already handled"
            )
        return WebhookDispatchResult(kind="comment", handled=True)

    async def _handle_webhook_issue(
        self, payload: Mapping[str, Any], *, provider: str | None = None
    ) -> WebhookDispatchResult:
        action = str(payload.get("action") or "").casefold()
        if action and action not in {"create", "update"}:
            return WebhookDispatchResult(kind="issue", handled=False, detail="ignored action")
        data = payload.get("data")
        if not isinstance(data, Mapping):
            return WebhookDispatchResult(kind="issue", handled=False, detail="missing issue data")
        issue_id = data.get("id")
        if not isinstance(issue_id, str) or not issue_id:
            return WebhookDispatchResult(kind="issue", handled=False, detail="missing issue id")
        state_changed = _linear_issue_state_changed(payload)
        issue, tracker_ctx = await self._lookup_webhook_issue(issue_id, provider=provider)
        storage_issue_id = await self._storage_issue_id_for_tracker_issue(issue.id, tracker_ctx)
        if state_changed:
            self._schedule_reconcile_task(
                self._reconciler.reconcile_linear_issue_event(
                    issue_id=storage_issue_id,
                    action=action or "update",
                ),
                source=f"linear.issue.{action or 'update'}",
            )
        old_state_id, old_state_name, new_state_id, new_state_name = _linear_issue_state_transition(
            payload
        )
        revived = False
        if state_changed:
            revived = (
                await self._schedule_parked_manual_merge_revival_for_issue_event(
                    issue=issue,
                    old_state_id=old_state_id,
                    old_state_name=old_state_name,
                    new_state_id=new_state_id,
                    new_state_name=new_state_name,
                )
                is not None
            )
        binding = self._ready_binding_for_issue(issue, tracker_ctx)
        if binding is None:
            return WebhookDispatchResult(
                kind="issue",
                handled=revived,
                detail=("parked manual merge revived" if revived else "issue is not dispatchable"),
            )
        task = await self._schedule_ready_issue(binding, issue)
        return WebhookDispatchResult(
            kind="issue",
            handled=task is not None or revived,
            detail="" if task is not None else "issue is already scheduled or active",
        )

    async def _track_implement_failed_wait(
        self, issue_id: str, run_id: str, binding: RepoBinding
    ) -> None:
        self._dispatch_run_ids[issue_id] = run_id
        self._operator_wait_run_ids.add(run_id)
        self._implement_failed_run_bindings[run_id] = binding
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue_id,
            run_id=run_id,
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )

    async def _track_implement_blocked_wait(
        self, issue_id: str, run_id: str, binding: RepoBinding
    ) -> None:
        self._dispatch_run_ids[issue_id] = run_id
        self._operator_wait_run_ids.add(run_id)
        self._implement_blocked_run_bindings[run_id] = binding
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue_id,
            run_id=run_id,
            kind=db.operator_waits.KIND_IMPLEMENT_BLOCKED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )

    async def _blocked_reason_for_run(self, run_id: str) -> str:
        run = await db.runs.get_with_issue(self._conn, run_id)
        if run is None:
            return ""
        return run.run.termination_detail or ""

    async def _restore_operator_waits(self) -> None:
        waits = await db.operator_waits.list_all(self._conn)
        matched_run_ids: set[str] = set()
        for wait in waits:
            if wait.kind not in (
                db.operator_waits.KIND_IMPLEMENT_FAILED,
                db.operator_waits.KIND_IMPLEMENT_BLOCKED,
                db.operator_waits.KIND_DELIVER_FAILED,
                db.operator_waits.KIND_REVIEW_FAILED,
                db.operator_waits.KIND_REVIEW_STOPPED,
                db.operator_waits.KIND_MERGE,
                db.operator_waits.KIND_ACCEPTANCE_BLOCKED,
                db.operator_waits.KIND_ACCEPTANCE_REJECTED,
                db.operator_waits.KIND_BUDGET_EXCEEDED,
            ):
                log.warning(
                    "ignoring unsupported operator wait kind %r for issue %s",
                    wait.kind,
                    wait.issue_id,
                )
                continue
            binding = self._binding_for_operator_wait(wait)
            if binding is None:
                log.warning(
                    "cannot restore operator wait for issue %s: "
                    "no binding for %s/%s/%s/%s label=%r",
                    wait.issue_id,
                    wait.tracker_provider,
                    wait.tracker_site,
                    wait.linear_team_key,
                    wait.github_repo,
                    wait.issue_label,
                )
                continue
            matched_run_ids.add(wait.run_id)
            self._register_operator_wait_binding(wait, binding)
        self._evict_unmatched_operator_waits(matched_run_ids)

    def _evict_unmatched_operator_waits(self, matched_run_ids: set[str]) -> None:
        """Drop a previously-restored wait whose binding a reload removed (or
        whose row is gone) from the in-memory tracking this restore pass
        populates — otherwise it lingers forever, since this pass only ever
        adds entries that still match `self.config` (SYM-189 review fix)."""
        stale_run_ids = self._operator_wait_run_ids - matched_run_ids
        if not stale_run_ids:
            return
        for run_id in stale_run_ids:
            self._operator_wait_run_ids.discard(run_id)
            self._implement_failed_run_bindings.pop(run_id, None)
            self._implement_blocked_run_bindings.pop(run_id, None)
            self._deliver_failed_run_bindings.pop(run_id, None)
            self._review_failed_run_bindings.pop(run_id, None)
            self._merge_needs_approval_bindings.pop(run_id, None)
            self._acceptance_rejected_run_bindings.pop(run_id, None)
            self._budget_exceeded_run_bindings.pop(run_id, None)
        for issue_id, run_id in list(self._dispatch_run_ids.items()):
            if run_id in stale_run_ids:
                self._dispatch_run_ids.pop(issue_id, None)

    def _register_operator_wait_binding(
        self, wait: db.operator_waits.OperatorWait, binding: RepoBinding
    ) -> None:
        self._dispatch_run_ids[wait.issue_id] = wait.run_id
        self._operator_wait_run_ids.add(wait.run_id)
        if wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED:
            self._implement_failed_run_bindings[wait.run_id] = binding
        elif wait.kind == db.operator_waits.KIND_IMPLEMENT_BLOCKED:
            self._implement_blocked_run_bindings[wait.run_id] = binding
        elif wait.kind == db.operator_waits.KIND_DELIVER_FAILED:
            self._deliver_failed_run_bindings[wait.run_id] = binding
        elif wait.kind in (
            db.operator_waits.KIND_REVIEW_FAILED,
            db.operator_waits.KIND_REVIEW_STOPPED,
        ):
            self._review_failed_run_bindings[wait.run_id] = binding
        elif wait.kind == db.operator_waits.KIND_MERGE:
            self._merge_needs_approval_bindings[wait.run_id] = binding
        elif wait.kind == db.operator_waits.KIND_ACCEPTANCE_REJECTED:
            self._acceptance_rejected_run_bindings[wait.run_id] = binding
        elif wait.kind == db.operator_waits.KIND_BUDGET_EXCEEDED:
            self._budget_exceeded_run_bindings[wait.run_id] = binding

    async def _restore_operator_wait_binding(
        self,
        issue_id: str,
        run_id: str,
        intent: SlashIntent,
        *,
        expected_kinds: tuple[str, ...],
    ) -> RepoBinding | None:
        wait = await db.operator_waits.get_by_run_id(self._conn, run_id)
        if wait is None or wait.issue_id != issue_id:
            log.warning(
                "operator wait binding missing for slash %s run %s issue %s",
                intent.kind,
                run_id,
                issue_id,
            )
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                "operator wait is no longer active",
            )
            return None
        if wait.kind not in expected_kinds:
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                f"operator wait is {wait.kind}, not one of {', '.join(expected_kinds)}",
            )
            return None
        binding = self._binding_for_operator_wait(wait)
        if binding is None:
            log.warning(
                "operator wait binding cannot be restored for issue %s run %s",
                issue_id,
                run_id,
            )
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                "no repository binding found for operator wait",
            )
            return None
        self._register_operator_wait_binding(wait, binding)
        return binding

    def _binding_for_operator_wait(
        self, wait: db.operator_waits.OperatorWait
    ) -> RepoBinding | None:
        for binding in self.config.repos:
            if (
                binding.linear_team_key == wait.linear_team_key
                and binding.github_repo == wait.github_repo
                and (binding.issue_label or "") == wait.issue_label
                and binding.tracker_provider == wait.tracker_provider
                and binding.tracker_site == wait.tracker_site
            ):
                return binding
        return None

    async def _clear_operator_wait(self, issue_id: str, run_id: str) -> None:
        if self._dispatch_run_ids.get(issue_id) == run_id:
            self._dispatch_run_ids.pop(issue_id, None)
        self._operator_wait_run_ids.discard(run_id)
        self._implement_failed_run_bindings.pop(run_id, None)
        self._implement_blocked_run_bindings.pop(run_id, None)
        self._deliver_failed_run_bindings.pop(run_id, None)
        self._review_failed_run_bindings.pop(run_id, None)
        self._merge_needs_approval_bindings.pop(run_id, None)
        self._acceptance_rejected_run_bindings.pop(run_id, None)
        self._budget_exceeded_run_bindings.pop(run_id, None)
        await db.operator_waits.delete(self._conn, issue_id, run_id)

    async def _token_budget_ceiling(self, issue_id: str, binding: RepoBinding) -> float | None:
        """Soft ceiling = `per_issue_token_budget + granted_token_budget`.

        Returns `None` when the gate is off for this binding (no global
        default and no per-binding override).
        """
        budget = binding.resolved_per_issue_token_budget(self.config.per_issue_token_budget)
        if budget is None:
            return None
        granted = await db.issues.get_granted_token_budget(self._conn, issue_id)
        return float(budget + granted)

    async def _would_exceed_token_budget(self, issue_id: str, binding: RepoBinding) -> bool:
        """True when cumulative effective tokens have reached the ceiling.

        Soft gate: uses whatever token data is recorded; the ~40% of runs
        without token data err toward *not* parking, which is acceptable.
        """
        ceiling = await self._token_budget_ceiling(issue_id, binding)
        if ceiling is None:
            return False
        tokens = await db.runs.tokens_for_issue(self._conn, issue_id)
        return tokens.effective_tokens >= ceiling

    async def _maybe_park_for_token_budget(
        self, issue_id: str, run_id: str, binding: RepoBinding
    ) -> bool:
        """Park the issue instead of dispatching its next run if over budget.

        Evaluated at agent-dispatch boundaries (never mid-run — runaway within
        one process is covered by `stall_timeout`). Returns True when parked,
        so the caller skips the dispatch it was about to make.
        """
        if not await self._would_exceed_token_budget(issue_id, binding):
            return False
        await self._park_for_token_budget(issue_id, run_id, binding)
        return True

    async def _park_for_token_budget(
        self, issue_id: str, run_id: str, binding: RepoBinding
    ) -> None:
        ceiling = await self._token_budget_ceiling(issue_id, binding)
        tokens = await db.runs.tokens_for_issue(self._conn, issue_id)
        breakdown = await db.runs.effective_tokens_by_stage_for_issue(self._conn, issue_id)
        tracker_issue_id, _ = await self._tracker_identity_for_issue(issue_id)
        tracker = self.tracker(binding)
        pr = await db.issue_prs.get_for_issue(self._conn, issue_id=issue_id)
        # Surface the real boundary stage + human identifier in the comment.
        # The run row is absent only when a merge-gate park synthesizes a fix
        # run id (no live merge run); fall back to the PR-implied stage there.
        run_row = await db.runs.get_with_issue(self._conn, run_id)
        stage = (
            run_row.run.stage
            if run_row is not None
            else ("review" if pr is not None else "implement")
        )
        linear_identifier = run_row.identifier if run_row is not None else ""
        body = budget_exceeded(
            CommentVars(
                stage=stage,
                repo=binding.github_repo,
                issue=pr.pr_number if pr is not None else 0,
                run_id=run_id,
                pr_url=pr.pr_url if pr is not None else "(no PR yet)",
                linear_identifier=linear_identifier,
            ),
            used_effective=tokens.effective_tokens,
            ceiling=ceiling or 0.0,
            breakdown=list(breakdown.items()),
        )
        try:
            await tracker.post_comment(tracker_issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("budget-exceeded comment failed for %s: %s", issue_id, e)
        try:
            states = await self._states_for_binding(binding)
            needs_approval_id = states.get(binding.linear_states.needs_approval)
            if needs_approval_id is not None:
                await tracker.move_issue(tracker_issue_id, needs_approval_id)
        except LinearError as e:
            log.warning("could not park %s for token budget: %s", issue_id, e)
        # Complete the boundary run row (no subprocess is live at a dispatch
        # boundary) so the issue has no active run blocking re-dispatch after
        # `$approve`. The live agent is never killed.
        await db.runs.update_status(
            self._conn,
            run_id,
            "completed",
            ended_at=self._now().isoformat(),
        )
        self._dispatch_run_ids[issue_id] = run_id
        self._operator_wait_run_ids.add(run_id)
        self._budget_exceeded_run_bindings[run_id] = binding
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue_id,
            run_id=run_id,
            kind=db.operator_waits.KIND_BUDGET_EXCEEDED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )
        if self._notifier.enabled:
            try:
                tracked_issue = await tracker.lookup_issue(tracker_issue_id)
                issue_identifier = tracked_issue.identifier
                issue_url = tracked_issue.url
            except LinearError as e:
                log.warning(
                    "could not look up %s for budget-exceeded notification: %s", issue_id, e
                )
                issue_identifier = linear_identifier or tracker_issue_id
                issue_url = ""
            await self._notify_attention(
                event=EVENT_OPERATOR_WAIT,
                issue_identifier=issue_identifier,
                issue_url=issue_url,
                dedupe_key=f"operator_wait:{run_id}",
            )

    async def drain_dispatch_tasks(self, *, cancel: bool = False) -> None:
        if cancel:
            await asyncio.gather(
                *(self._kill_active_runner(run_id) for run_id in tuple(self._active_run_ids)),
                return_exceptions=True,
            )
            for task in tuple(self._dispatch_tasks):
                task.cancel()
            for task in tuple(self._review_poll_tasks):
                task.cancel()
        while self._dispatch_tasks:
            await asyncio.gather(
                *tuple(self._dispatch_tasks),
                return_exceptions=True,
            )
        while self._review_poll_tasks:
            await asyncio.gather(
                *tuple(self._review_poll_tasks),
                return_exceptions=True,
            )

    async def _kill_active_runner(self, run_id: str) -> None:
        try:
            await self._runner.kill(run_id)
        except Exception:
            log.exception("failed to kill runner for run_id=%s", run_id)

    def _binding_for_pr(self, candidate: db.issue_prs.IssuePR) -> RepoBinding | None:
        stored_label = _binding_label_from_storage_key(candidate.binding_key)
        if candidate.binding_key:
            for binding in self.config.repos:
                if _binding_storage_key(binding) == candidate.binding_key:
                    return binding

        matches = [
            binding
            for binding in self.config.repos
            if (
                binding.linear_team_key == candidate.team_key
                and binding.github_repo == candidate.github_repo
            )
        ]
        if stored_label is not None:
            labeled_matches = [
                binding for binding in matches if (binding.issue_label or "") == stored_label
            ]
            if len(labeled_matches) == 1:
                return labeled_matches[0]
            return None
        if len(matches) == 1:
            return matches[0]
        return None

    async def _agent_infra_retry_count(self, issue_id: str) -> int:
        """Consecutive most-recent *terminated* runs requeued on a transient API
        error, across all stages. Derived from the durable runs history (survives
        a restart): count back from the latest terminated run until one without a
        retry marker. Still-`running` runs (the in-flight attempt whose failure
        is being classified) are skipped so the count reflects only prior
        attempts. This is the retry-attempt counter the backoff window and the
        escalation cutoff both read.

        Counting across implement and review_fix stages (the only stages that
        carry retry markers) lets a shared budget cap repeated failures
        regardless of which stage retried, and correctly tracks review-fix
        transient retries (REVIEW_FIX_TRANSIENT_RETRY_KIND) alongside implement
        and local-review retries. Sub-runs (local_review, local_review_fix, …)
        are skipped — they never carry retry markers but interleave in history
        and would otherwise break the consecutive count."""
        history = await db.runs.history_for_issue(self._conn, issue_id)
        count = 0
        for run in reversed(history):
            if run.status in db.runs.LIVE_STATUSES:
                continue
            if run.status == db.runs.SUPERSEDED_STATUS:
                continue  # bookkeeping close; not a real attempt
            if run.stage not in ("implement", "review_fix"):
                continue  # sub-runs never carry retry markers; skip, don't break
            if run.termination_kind in _AGENT_INFRA_RETRY_KINDS:
                count += 1
            else:
                break
        return count

    async def _agent_infra_retry_backoff_active(self, issue_id: str) -> bool:
        """True while a transiently-failed run is still inside its capped
        backoff window — the poll loop must not re-dispatch yet. Mirrors
        `_acceptance_infra_retry_backoff_active` but reads the marker + ended_at
        off the latest run with a retry marker (across all stages) rather than
        acceptance_state. Checks all stages so review-fix retries
        (REVIEW_FIX_TRANSIENT_RETRY_KIND) are covered alongside implement and
        local-review retries. Sub-runs (local_review, local_review_fix, …) are
        skipped — they never carry retry markers."""
        history = await db.runs.history_for_issue(self._conn, issue_id)
        latest_retry: db.runs.Run | None = None
        for run in reversed(history):
            if run.status in db.runs.LIVE_STATUSES:
                continue
            if run.status == db.runs.SUPERSEDED_STATUS:
                continue  # bookkeeping close; not a real attempt
            if run.stage not in ("implement", "review_fix"):
                continue  # sub-runs never carry retry markers
            if run.termination_kind in _AGENT_INFRA_RETRY_KINDS and run.ended_at is not None:
                latest_retry = run
                break
        if latest_retry is None:
            return False
        count = await self._agent_infra_retry_count(issue_id)
        if count <= 0:
            return False
        try:
            ended_at = _parse_rfc3339(latest_retry.ended_at)  # type: ignore[arg-type]
        except ValueError:
            return False
        backoff_secs = _infra_retry_backoff_secs(min(count, AGENT_INFRA_RETRY_LIMIT))
        return self._now() < ended_at + timedelta(seconds=backoff_secs)

    async def _maybe_requeue_transient_agent_failure(
        self,
        *,
        run_id: str,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str,
        api_error: StreamApiError | None,
        reason: str,
        returncode: int | None = None,
        termination_kind: str = db.runs.TRANSIENT_API_RETRY_KIND,
        workspace_path: Path | None = None,
    ) -> bool:
        """Requeue an implement run that died on a *transient* provider API
        error instead of escalating, until the retry budget is spent.

        A clean 5xx/429 means the agent did no work, so re-invoking is safe. The
        run is marked failed with a durable marker + `ended_at` and the issue is
        moved back to its Ready lane; the poll loop re-dispatches once the
        per-attempt backoff window elapses (`_agent_infra_retry_backoff_active`)
        — no workspace slot is held during the wait. Returns True when it handled
        the failure this way (the caller must skip escalation). Returns False —
        the caller escalates exactly as today — for a non-transient error, no API
        error, or once `AGENT_INFRA_RETRY_LIMIT` consecutive retries have already
        happened.

        `termination_kind` selects the durable marker stamped on the run:
        `TRANSIENT_API_RETRY_KIND` for implement-phase failures (the default) and
        `LOCAL_REVIEW_TRANSIENT_RETRY_KIND` for local-review-phase failures. The
        re-dispatch resume logic reads this to decide whether to re-run the
        implementer or short-circuit to the pre-push gates.

        For `REVIEW_FIX_TRANSIENT_RETRY_KIND` the issue is NOT moved to Ready —
        the existing PR is intact and the retry is driven by the merge/acceptance
        loop once the backoff elapses."""
        if api_error is None or not api_error.transient:
            return False
        # The current run is still `running` (its marker is stamped below), so
        # `_agent_infra_retry_count` skips it and counts only the *prior*
        # requeued attempts.
        prior = await self._agent_infra_retry_count(storage_issue_id)
        if prior >= AGENT_INFRA_RETRY_LIMIT:
            return False
        # An agent can edit files before the provider 5xx fires. A dirty tree
        # means work happened, so re-invoking is not safe — escalate instead.
        if workspace_path is not None:
            status = await _git_status_short(workspace_path)
            if status:
                log.warning(
                    "workspace dirty after transient API error for %s — escalating: %s",
                    issue.identifier,
                    status[:200],
                )
                return False
        if termination_kind == db.runs.REVIEW_FIX_TRANSIENT_RETRY_KIND:
            # Review-fix retry: the PR and commits are intact; leave the issue
            # in its current review state so the merge/acceptance loop drives
            # the re-dispatch once the backoff window elapses.
            pass
        else:
            try:
                states = await self._states_for_binding(binding)
            except LinearError as e:
                log.warning(
                    "could not load states to requeue %s after transient API error: %s",
                    issue.identifier,
                    e,
                )
                return False
            ready_id = states.get(binding.linear_states.ready)
            if ready_id is None:
                return False
            try:
                await self.tracker(binding).move_issue(issue.id, ready_id)
            except LinearError as e:
                log.warning(
                    "could not requeue %s to Ready after transient API error: %s",
                    issue.identifier,
                    e,
                )
                return False
        await self._fail_run(
            run_id,
            reason,
            returncode=returncode,
            termination_kind=termination_kind,
            termination_detail=reason,
        )
        attempt = prior + 1
        log.info(
            "requeueing %s after transient API error (attempt %d/%d, backoff %ds): %s",
            issue.identifier,
            attempt,
            AGENT_INFRA_RETRY_LIMIT,
            _infra_retry_backoff_secs(min(attempt, AGENT_INFRA_RETRY_LIMIT)),
            reason,
        )
        return True

    async def _park_local_only_review_needs_approval(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
        result: LoopResult | None,
        operator_wait: bool = False,
    ) -> None:
        reason = _local_review_termination_reason(result)
        findings = ""
        if result is not None and result.last_verdict is not None:
            findings = result.last_verdict.findings.strip()
        detail_parts = [reason]
        if findings:
            detail_parts.append("Last unresolved findings:\n\n" + findings)
        detail = "\n\n".join(detail_parts)

        tracker = self.tracker(binding)
        try:
            states = await self._states_for_binding(binding)
            needs_approval_id = states.get(binding.linear_states.needs_approval)
        except LinearError as e:
            log.warning(
                "could not load states while parking %s after local review: %s",
                issue.identifier,
                e,
            )
            needs_approval_id = None

        if needs_approval_id is not None:
            try:
                await tracker.move_issue(issue.id, needs_approval_id)
            except LinearError as e:
                log.warning(
                    "could not move %s to needs_approval after local review: %s",
                    issue.identifier,
                    e,
                )
        else:
            log.warning(
                "missing Linear needs_approval state %r for %s after local review",
                binding.linear_states.needs_approval,
                issue.identifier,
            )

        tokens = await db.runs.tokens_for_issue(self._conn, run.issue_id)
        body = awaiting_approval(
            CommentVars(
                stage="local review",
                next_stage="done",
                repo=binding.github_repo,
                issue=pr_number_from_url(pr_url) or 0,
                pr_url=pr_url,
                run_id=run.id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                error=reason,
            )
        )
        if findings:
            body += "\nLast unresolved findings:\n\n" + findings + "\n"
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "local-review needs_approval comment failed on %s: %s",
                issue.identifier,
                e,
            )

        await db.runs.update_status(
            self._conn,
            run.id,
            "needs_approval",
            ended_at=self._now().isoformat(),
            **_termination_kwargs(status="needs_approval", reason=detail),
        )
        await self._clear_review_rearm_retry(run.id)
        if operator_wait:
            await self._track_review_failed_wait(issue.id, run.id, binding)
        await self._notify_attention(
            event=EVENT_OPERATOR_WAIT,
            issue_identifier=issue.identifier,
            issue_url=issue.url,
            dedupe_key=f"operator_wait:{run.id}",
            detail=reason,
        )

    async def _start_review_stage(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str | None = None,
        pr_url: str,
        post_codex_review: bool = True,
    ) -> db.runs.Run:
        """Persist the review state row, optionally ping `@codex review`.

        `post_codex_review=False` is the local-only / no-review / local-terminal
        entry point: state tracking and PR persistence still happen, but the
        remote bot ping and remote review lane move are suppressed.

        Idempotent in spirit: failure to post the bot ping does not block
        the run row from being created, but is logged loudly so an
        operator can re-ping with a slash command if needed.
        """
        storage_issue_id = storage_issue_id or issue.id
        pr_number = pr_number_from_url(pr_url)
        if pr_number is None:
            log.warning(
                "could not parse PR number from %r for %s — skipping @codex review",
                pr_url,
                issue.identifier,
            )

        review_run_id = str(uuid.uuid4())
        started_at = self._now().isoformat()

        existing = await db.runs.latest_live_for_issue_stage(
            self._conn, issue_id=storage_issue_id, stage="review"
        )
        if existing is not None:
            previous_state = await db.review_state.get(self._conn, storage_issue_id)
            if pr_number is not None:
                await db.review_state.refresh_pr_metadata(
                    self._conn,
                    storage_issue_id,
                    pr_number=pr_number,
                    pr_url=pr_url,
                    github_repo=binding.github_repo,
                    issue_label=binding.issue_label,
                )
                existing_issue_pr = await db.issue_prs.get(
                    self._conn,
                    issue_id=storage_issue_id,
                    github_repo=binding.github_repo,
                )
                await db.issue_prs.upsert(
                    self._conn,
                    issue_id=storage_issue_id,
                    github_repo=binding.github_repo,
                    binding_key=_binding_storage_key(binding),
                    pr_number=pr_number,
                    pr_url=pr_url,
                    created_at=(
                        existing_issue_pr.created_at
                        if existing_issue_pr is not None
                        else existing.started_at
                    ),
                )
                if previous_state.pr_number not in (None, pr_number):
                    await db.review_state.set_codex_review_requested_at(
                        self._conn,
                        storage_issue_id,
                        "",
                    )
            if post_codex_review and binding.resolved_remote_review():
                state = await db.review_state.get(self._conn, storage_issue_id)
                if pr_number is not None and not state.codex_review_requested_at:
                    await self._post_codex_review_request(
                        binding=binding,
                        storage_issue_id=storage_issue_id,
                        pr_number=pr_number,
                    )
                await self._move_issue_to_review_state(binding=binding, issue=issue)
            return existing

        # Write PR metadata before exposing the live review run. A poll tick
        # can see the run immediately after insertion, so it must not observe
        # a default review_state row with no PR number.
        await db.review_state.begin_review(
            self._conn,
            storage_issue_id,
            pr_number=pr_number,
            pr_url=pr_url,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label,
        )
        if pr_number is not None:
            await db.issue_prs.upsert(
                self._conn,
                issue_id=storage_issue_id,
                github_repo=binding.github_repo,
                binding_key=_binding_storage_key(binding),
                pr_number=pr_number,
                pr_url=pr_url,
                created_at=started_at,
            )

        # Idempotent under a `$retry` resume: a handoff that faulted after this
        # point already left a live review run, so guard creation (mirrors
        # `_merge_approved_pr`) and adopt the existing one instead of inserting
        # a second running review-run row.
        inserted = await db.runs.create_if_no_active(
            self._conn,
            id=review_run_id,
            issue_id=storage_issue_id,
            stage="review",
            status="running",
            pid=None,
            started_at=started_at,
        )
        created_review_run = inserted
        if not inserted:
            existing = await db.runs.latest_live_for_issue_stage(
                self._conn, issue_id=storage_issue_id, stage="review"
            )
            if existing is not None:
                if post_codex_review and binding.resolved_remote_review():
                    state = await db.review_state.get(self._conn, storage_issue_id)
                    if pr_number is not None and not state.codex_review_requested_at:
                        await self._post_codex_review_request(
                            binding=binding,
                            storage_issue_id=storage_issue_id,
                            pr_number=pr_number,
                        )
                    await self._move_issue_to_review_state(binding=binding, issue=issue)
                return existing
            # Guard tripped on a live run in another stage, but no live Review
            # row exists to adopt: force-create so the returned Run is persisted.
            await db.runs.create(
                self._conn,
                id=review_run_id,
                issue_id=storage_issue_id,
                stage="review",
                status="running",
                pid=None,
                started_at=started_at,
            )
            created_review_run = True
        if created_review_run and pr_number is not None and post_codex_review:
            await self._post_codex_review_request(
                binding=binding,
                storage_issue_id=storage_issue_id,
                pr_number=pr_number,
            )

        if created_review_run and post_codex_review and binding.resolved_remote_review():
            await self._move_issue_to_review_state(binding=binding, issue=issue)
        return db.runs.Run(
            id=review_run_id,
            issue_id=storage_issue_id,
            stage="review",
            status="running",
            pid=None,
            started_at=started_at,
            ended_at=None,
            cost_usd=0.0,
        )

    async def _post_codex_review_request(
        self,
        *,
        binding: RepoBinding,
        storage_issue_id: str,
        pr_number: int,
    ) -> None:
        try:
            await self._gh.pr_comment(
                pr_number,
                "@codex review",
                repo=binding.github_repo,
            )
        except GitHubError as e:
            log.warning(
                "could not post @codex review on %s#%d: %s",
                binding.github_repo,
                pr_number,
                e,
            )
            return
        await db.review_state.set_codex_review_requested_at(
            self._conn,
            storage_issue_id,
            self._now().isoformat(),
        )

    async def _move_issue_to_local_code_review_state(
        self, *, binding: RepoBinding, issue: LinearIssue
    ) -> None:
        try:
            states = await self._states_for_binding(binding)
            review_state_id = states.get(binding.linear_states.local_code_review)
        except LinearError as e:
            log.warning(
                "could not load states while moving %s to local review: %s",
                issue.identifier,
                e,
            )
            return
        if review_state_id is None:
            log.warning(
                "missing Linear local review state %r for %s",
                binding.linear_states.local_code_review,
                issue.identifier,
            )
            return
        try:
            await self.tracker(binding).move_issue(issue.id, review_state_id)
        except LinearError as e:
            log.warning(
                "could not move %s to local review state %r: %s",
                issue.identifier,
                binding.linear_states.local_code_review,
                e,
            )

    async def _move_issue_to_review_state(
        self, *, binding: RepoBinding, issue: LinearIssue
    ) -> None:
        try:
            states = await self._states_for_binding(binding)
            review_state_id = states.get(binding.linear_states.code_review)
        except LinearError as e:
            log.warning(
                "could not load states while moving %s to review: %s",
                issue.identifier,
                e,
            )
            return
        if review_state_id is None:
            log.warning(
                "missing Linear review state %r for %s",
                binding.linear_states.code_review,
                issue.identifier,
            )
            return
        try:
            await self.tracker(binding).move_issue(issue.id, review_state_id)
        except LinearError as e:
            log.warning(
                "could not move %s to review state %r: %s",
                issue.identifier,
                binding.linear_states.code_review,
                e,
            )

    def _activity_session(
        self,
        *,
        binding: RepoBinding,
        run_id: str,
        stage: str,
        workspace_path: Path,
    ) -> ActivitySession | None:
        if binding.agent != "codex" or stage not in {"implement", "review_fix"}:
            return None
        settings = _activity_settings_for(self.config, binding)
        if not settings.enabled:
            return None
        return ActivitySession(
            settings=settings,
            run_id=run_id,
            stage=stage,
            workspace_path=workspace_path,
        )

    async def _record_activity_stdout(
        self,
        *,
        session: ActivitySession | None,
        binding: RepoBinding,
        issue: LinearIssue,
        line: str,
        cumulative_usage: UsageDelta,
    ) -> None:
        if session is None:
            return
        now = self._now()
        if not session.record_line(line, now):
            return
        await db.activity_comments.record_event(
            self._conn,
            run_id=session.run_id,
            occurred_at=now.isoformat(),
        )
        mark = await db.activity_comments.get(self._conn, session.run_id)
        last_posted_at = _parse_optional_datetime(mark.last_posted_at) if mark is not None else None
        reason = session.due_reason(now, last_posted_at=last_posted_at)
        if reason is not None:
            await self._publish_activity_digest(
                session=session,
                binding=binding,
                issue=issue,
                reason=reason,
                now=now,
                cumulative_usage=cumulative_usage,
            )

    async def _record_activity_tick(
        self,
        *,
        session: ActivitySession | None,
        binding: RepoBinding,
        issue: LinearIssue,
        cumulative_usage: UsageDelta,
    ) -> None:
        if session is None:
            return
        now = self._now()
        if not session.has_heartbeat_candidate(now):
            return
        if session.needs_heartbeat_mark_lookup(now):
            raw_marks = await db.activity_comments.heartbeat_marks(
                self._conn,
                run_id=session.run_id,
            )
            session.cache_heartbeat_marks(
                {
                    item_id: parsed
                    for item_id, raw in raw_marks.items()
                    if (parsed := _parse_optional_datetime(raw)) is not None
                }
            )
        due_item_ids = session.heartbeat_due_item_ids(now)
        if not due_item_ids:
            return
        await self._publish_activity_digest(
            session=session,
            binding=binding,
            issue=issue,
            reason="heartbeat",
            now=now,
            cumulative_usage=cumulative_usage,
            heartbeat_item_ids=due_item_ids,
        )

    async def _flush_activity(
        self,
        *,
        session: ActivitySession | None,
        binding: RepoBinding,
        issue: LinearIssue,
        cumulative_usage: UsageDelta,
    ) -> None:
        if session is None or not session.has_unpublished_events():
            return
        await self._publish_activity_digest(
            session=session,
            binding=binding,
            issue=issue,
            reason="final",
            now=self._now(),
            cumulative_usage=cumulative_usage,
        )

    async def _publish_activity_digest(
        self,
        *,
        session: ActivitySession,
        binding: RepoBinding,
        issue: LinearIssue,
        reason: ActivityPublishReason,
        now: datetime,
        cumulative_usage: UsageDelta,
        heartbeat_item_ids: tuple[str, ...] = (),
    ) -> bool:
        digest = session.build_digest(
            reason=reason,
            now=now,
            input_tokens=cumulative_usage.input_tokens,
            output_tokens=cumulative_usage.output_tokens,
            cache_write_tokens=cumulative_usage.cache_write_tokens,
            cache_read_tokens=cumulative_usage.cache_read_tokens,
        )
        body = truncate_body(format_activity_digest(digest))
        fingerprint = digest_fingerprint(body)
        mark = await db.activity_comments.get(self._conn, session.run_id)
        if (
            mark is not None
            and mark.last_fingerprint == fingerprint
            and not session.has_unpublished_events()
        ):
            return False
        tracker = self.tracker(binding)
        try:
            await tracker.post_comment(issue.id, body)
        except LinearError as e:
            log.warning(
                "activity comment failed on %s run %s: %s",
                issue.identifier,
                session.run_id,
                e,
            )
            return False
        await db.activity_comments.mark_published(
            self._conn,
            run_id=session.run_id,
            posted_at=now.isoformat(),
            fingerprint=fingerprint,
        )
        for item_id in heartbeat_item_ids:
            await db.activity_comments.mark_heartbeat(
                self._conn,
                run_id=session.run_id,
                item_id=item_id,
                posted_at=now.isoformat(),
            )
        session.mark_heartbeat_posted(heartbeat_item_ids, now)
        session.mark_published()
        return True

    async def _run_agent(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str | None = None,
        run_id: str,
        workspace_path: Path,
        prior_total: float,
    ) -> tuple[UsageDelta, str, int | None]:
        """Spawn the runner and consume events. Returns
        (cumulative_usage, final_event_kind, final_returncode).

        `prior_total` is the issue's cost so far. It is passed through to
        `_run_stage_command` but is no longer consumed anywhere: activity
        digests now report per-run token counts rather than a cumulative
        dollar total, so it is currently unused.
        """
        storage_issue_id = storage_issue_id or issue.id
        # Consume a pending blocked-resume handoff (set when the operator
        # `$retry`d an IMPLEMENT_BLOCKED wait), so the fresh run's prompt carries
        # the original block reason + the operator's resume instructions.
        handoff = self._implement_handoffs.pop(storage_issue_id, None)
        prompt = implement_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
            blocked_reason=handoff.blocked_reason if handoff else "",
            operator_comment=handoff.operator_comment if handoff else "",
        )
        role = binding.resolved_role("implement", self.config.roles)
        is_codex = role.agent == "codex"
        command = build_runner_command(
            role.agent,
            prompt,
            codex_model=role.model if (is_codex and role.model) else binding.codex_model,
            claude_model=None if is_codex else role.model,
            effort=role.effort,
            workspace_path=workspace_path,
            mcp_servers=binding.mcp_servers,
        )
        return await self._run_stage_command(
            binding=binding,
            issue=issue,
            storage_issue_id=storage_issue_id,
            command=command,
            run_id=run_id,
            workspace_path=workspace_path,
            stage="implement",
            prior_total=prior_total,
        )

    async def _run_stage_command(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str | None = None,
        command: list[str],
        run_id: str,
        workspace_path: Path,
        stage: str,
        prior_total: float,
    ) -> tuple[UsageDelta, str, int | None]:
        storage_issue_id = storage_issue_id or issue.id
        spec = RunnerSpec(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            env=dict(binding.env),
            stall_secs=self.config.stall_timeout_secs,
            command_secs=self.config.command_timeout_secs,
            wall_clock_secs=self.config.wall_clock_timeout_secs,
            stage=stage,
        )

        log_path = self.config.log_root / f"{run_id}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        cumulative_usage = UsageDelta()
        final_kind = "exit"
        final_returncode: int | None = None
        cost_estimator = _UsageCostEstimator(
            agent=binding.agent,
            codex_model=binding.codex_model,
        )
        activity = self._activity_session(
            binding=binding,
            run_id=run_id,
            stage=stage,
            workspace_path=workspace_path,
        )
        self._active_run_ids.add(run_id)
        try:
            with log_path.open("a", encoding="utf-8") as logf:
                async for ev in self._runner.run(spec):
                    if ev.kind == "started" and ev.pid is not None:
                        await db.runs.update_pid(self._conn, run_id, ev.pid)
                    elif ev.kind == "stdout" and ev.line is not None:
                        logf.write(ev.line + "\n")
                        logf.flush()
                        usage = parse_event_line(ev.line)
                        if usage is not None:
                            cumulative_usage = _sum_usage(
                                cumulative_usage, cost_estimator.delta(usage)
                            )
                        await self._record_activity_stdout(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            line=ev.line,
                            cumulative_usage=cumulative_usage,
                        )
                    elif ev.kind == "stderr" and ev.line is not None:
                        logf.write(f"[stderr] {ev.line}\n")
                        logf.flush()
                    elif ev.kind == "tick":
                        await self._record_activity_tick(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_usage=cumulative_usage,
                        )
                    elif ev.kind in (
                        "exit",
                        "stall_timeout",
                        "wall_clock_timeout",
                        "spawn_failed",
                    ):
                        await self._flush_activity(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_usage=cumulative_usage,
                        )
                        final_kind = ev.kind
                        final_returncode = ev.returncode
                        break
        finally:
            self._active_run_ids.discard(run_id)
        await _record_run_model_usage(self._conn, run_id, log_path, codex_model=binding.codex_model)
        return cumulative_usage, final_kind, final_returncode

    def _fix_claude_model(self, binding: RepoBinding) -> str | None:
        """The `fix` role's resolved Claude `--model`.

        Resolves through the same matrix + per-binding override path as the
        `implement` role (SYM-124): set → `--model <alias>`, unset → CLI
        default. `None` for a codex-resolved `fix` role (the `--model` flag
        is claude-only; codex carries its model via `codex_model`).
        """
        role = binding.resolved_role("fix", self.config.roles)
        return None if role.agent == "codex" else role.model

    async def _run_fix_agent(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        run_id: str,
        workspace_path: Path,
        prompt: str,
        prior_total: float,
    ) -> tuple[UsageDelta, str, int | None]:
        command = build_fix_runner_command(
            binding.agent,
            prompt,
            codex_model=binding.codex_model,
            claude_model=self._fix_claude_model(binding),
            workspace_path=workspace_path,
            mcp_servers=binding.mcp_servers,
        )
        return await self._run_runner(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stage="review",
            agent=binding.agent,
            codex_model=binding.codex_model,
            binding=binding,
            issue=issue,
            activity_stage="review_fix",
            prior_total=prior_total,
        )

    async def _run_fix_dispatch(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        ignored_stages: tuple[str, ...],
        on_acquire_failure: Callable[[Exception], Awaitable[None]],
        body: Callable[[Path, str, Callable[[], None]], Awaitable[bool | None]],
        setup: Callable[[Path], Awaitable[bool]] | None = None,
        after_dedup: Callable[[str], Awaitable[None]] | None = None,
        on_dedup_loss: Callable[[], Awaitable[bool | None]] | None = None,
        dispatch_capacity_held: bool = False,
    ) -> bool | None:
        """Shared scaffolding for the `_dispatch_*_fix_run` family (SYM-157).

        Owns the path every fix-run dispatch repeats: reserve the review-fix
        slot, acquire the workspace, run optional pre-dedup ``setup`` (git
        fetch/sync), atomically claim the ``review_fix`` run row (SYM-152
        dedup), register the dispatch run id, then guarantee cleanup (drop the
        dispatch id + release the workspace) once ``body`` returns.

        Domain specifics stay in the thin wrappers via callbacks:

        * ``on_acquire_failure`` — escalate when the workspace can't be
          acquired (the workspace was never held, so no release is owed).
        * ``setup`` — pre-dedup git work; return ``False`` after escalating to
          abort (the helper releases the workspace and returns ``False``).
        * ``after_dedup`` — fire-and-forget work once the run row is claimed
          (e.g. an ``on_started`` callback or a "starting" comment).
        * ``body(workspace_path, fix_run_id, drop_dispatch_id)`` — the actual
          agent run plus post-run validation/push. ``drop_dispatch_id`` must be
          called as soon as the runner subprocess exits (before any post-run
          work) so the dispatch slot is freed at the right time. The helper's
          ``finally`` block calls it again as a safety net (idempotent).
        * ``on_dedup_loss`` — value to return when another live ``review_fix``
          already exists (defaults to ``False``).
        """
        async with self._review_fix_dispatch_slot(
            binding,
            issue,
            dispatch_capacity_held=dispatch_capacity_held,
        ):
            try:
                workspace_path = await self._workspace.acquire(binding, issue)
            except Exception as e:  # noqa: BLE001
                log.exception("workspace acquire failed for fix-run %s", issue.identifier)
                await on_acquire_failure(e)
                return False

            try:
                if setup is not None and not await setup(workspace_path):
                    return False

                fix_run_id = str(uuid.uuid4())
                inserted = await db.runs.create_if_no_active(
                    self._conn,
                    id=fix_run_id,
                    issue_id=issue.id,
                    stage="review_fix",
                    status="running",
                    pid=None,
                    started_at=self._now().isoformat(),
                    ignored_stages=ignored_stages,
                )
                if not inserted:
                    # Lost the race: another live review_fix already exists (SYM-152).
                    if on_dedup_loss is not None:
                        return await on_dedup_loss()
                    return False
                self._dispatch_run_ids[issue.id] = fix_run_id

                if after_dedup is not None:
                    await after_dedup(fix_run_id)

                _id_dropped = False

                def _drop_dispatch_id() -> None:
                    nonlocal _id_dropped
                    if _id_dropped:
                        return
                    _id_dropped = True
                    if self._dispatch_run_ids.get(issue.id) == fix_run_id:
                        self._dispatch_run_ids.pop(issue.id, None)

                try:
                    return await body(workspace_path, fix_run_id, _drop_dispatch_id)
                finally:
                    _drop_dispatch_id()  # no-op if body already called it
            finally:
                self._workspace.release(binding, issue)

    async def _run_dirty_tree_fix_turn(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str,
        workspace_path: Path,
        parent_run_id: str,
        dirty_files: list[str],
    ) -> None:
        """One agent turn to commit (or clean up) a dirty working tree.

        Reuses the review fix-run machinery: same command builder, same
        runner plumbing, its own `runs` row so the spend is attributed.
        Best-effort — any failure here just leaves the tree dirty and the
        caller's re-check fails closed.
        """
        fix_run_id = f"{parent_run_id}-dirty-fix"
        await db.runs.create(
            self._conn,
            id=fix_run_id,
            issue_id=storage_issue_id,
            stage="implement_fix",
            status="running",
            pid=None,
            started_at=self._now().isoformat(),
        )
        command = build_fix_runner_command(
            binding.agent,
            dirty_tree_fix_prompt(dirty_files),
            codex_model=binding.codex_model,
            workspace_path=workspace_path,
        )
        status = "failed"
        try:
            usage, final_kind, returncode = await self._run_runner(
                run_id=fix_run_id,
                workspace_path=workspace_path,
                command=command,
                stage="implement_fix",
                agent=binding.agent,
                codex_model=binding.codex_model,
                binding=binding,
                issue=issue,
            )
            await _add_run_usage(self._conn, fix_run_id, usage)
            transition = on_runner_event(
                stage="implement",
                event_kind=final_kind,
                returncode=returncode,
            )
            status = transition.next_run_status
        except Exception as e:  # noqa: BLE001
            log.warning("dirty-tree fix turn failed for %s: %s", issue.identifier, e)
        await db.runs.update_status(
            self._conn,
            fix_run_id,
            status,
            ended_at=self._now().isoformat(),
        )

    async def _run_runner(
        self,
        *,
        run_id: str,
        workspace_path: Path,
        command: list[str],
        stage: str,
        agent: str,
        binding: RepoBinding,
        issue: LinearIssue,
        codex_model: str = DEFAULT_CODEX_MODEL,
        activity_stage: str | None = None,
        prior_total: float = 0.0,
        clear_pid_on_finish: bool = False,
    ) -> tuple[UsageDelta, str, int | None]:
        spec = RunnerSpec(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            env=dict(binding.env),
            stall_secs=self.config.stall_timeout_secs,
            command_secs=self.config.command_timeout_secs,
            wall_clock_secs=self.config.wall_clock_timeout_secs,
            stage=stage,
        )

        log_path = self.config.log_root / f"{run_id}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        cumulative_usage = UsageDelta()
        final_kind = "exit"
        final_returncode: int | None = None
        cost_estimator = _UsageCostEstimator(agent=agent, codex_model=codex_model)
        activity = (
            self._activity_session(
                binding=binding,
                run_id=run_id,
                stage=activity_stage,
                workspace_path=workspace_path,
            )
            if activity_stage is not None
            else None
        )
        self._active_run_ids.add(run_id)
        try:
            with log_path.open("a", encoding="utf-8") as logf:
                async for ev in self._runner.run(spec):
                    if ev.kind == "started" and ev.pid is not None:
                        await db.runs.update_pid(self._conn, run_id, ev.pid)
                    elif ev.kind == "stdout" and ev.line is not None:
                        logf.write(ev.line + "\n")
                        logf.flush()
                        usage = parse_event_line(ev.line)
                        if usage is not None:
                            cumulative_usage = _sum_usage(
                                cumulative_usage, cost_estimator.delta(usage)
                            )
                        await self._record_activity_stdout(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            line=ev.line,
                            cumulative_usage=cumulative_usage,
                        )
                    elif ev.kind == "stderr" and ev.line is not None:
                        logf.write(f"[stderr] {ev.line}\n")
                        logf.flush()
                    elif ev.kind == "tick":
                        await self._record_activity_tick(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_usage=cumulative_usage,
                        )
                    elif ev.kind in (
                        "exit",
                        "stall_timeout",
                        "wall_clock_timeout",
                        "spawn_failed",
                    ):
                        await self._flush_activity(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_usage=cumulative_usage,
                        )
                        final_kind = ev.kind
                        final_returncode = ev.returncode
                        break
        finally:
            self._active_run_ids.discard(run_id)
            if clear_pid_on_finish:
                await db.runs.update_pid(self._conn, run_id, None)
        await _record_run_model_usage(self._conn, run_id, log_path, codex_model=codex_model)
        return cumulative_usage, final_kind, final_returncode

    async def _fail_run(
        self,
        run_id: str,
        reason: str,
        *,
        final_kind: str | None = None,
        returncode: int | None = None,
        exc: BaseException | str | None = None,
        termination_kind: str | None = None,
        termination_detail: str | None = None,
    ) -> None:
        if termination_kind is not None:
            # Explicit classification (e.g. a "blocked" completion-gate verdict);
            # bypass the heuristic `classify_termination` so the kind/detail are
            # recorded verbatim.
            kwargs: _TerminationKwargs = {
                "kind": termination_kind,
                "detail": termination_detail if termination_detail is not None else reason,
                "returncode": returncode,
            }
        else:
            kwargs = _termination_kwargs(
                status="failed",
                final_kind=final_kind,
                returncode=returncode,
                exc=exc,
                reason=reason,
            )
        await db.runs.update_status(
            self._conn,
            run_id,
            "failed",
            ended_at=self._now().isoformat(),
            **kwargs,
        )

    async def _notify_attention(
        self,
        *,
        event: str,
        issue_identifier: str,
        issue_url: str,
        dedupe_key: str,
        detail: str = "",
    ) -> None:
        """Push a Telegram message for an attention-needed event.

        A no-op when the notifier is unconfigured; `dedupe_key` guards against
        re-firing on repeated polls. The claim commits immediately (rather
        than staying open across the outbound HTTP call), so it never rides
        on the same transaction as unrelated writes on the shared connection.
        A send that fails after the claim is queued in `pending_notifications`
        for `_retry_pending_notifications` to flush on a later tick, since
        most call sites fire once on a state transition and won't re-derive
        the same event. Never raises into the poll loop.
        """
        if not self._notifier.enabled:
            return
        try:
            claimed = await db.notifications.claim(self._conn, dedupe_key, self._now().isoformat())
        except Exception as e:  # noqa: BLE001
            log.warning("telegram notification claim failed for %s: %s", issue_identifier, e)
            return
        if not claimed:
            return
        text = build_message(
            event=event,
            issue_identifier=issue_identifier,
            issue_url=issue_url,
            detail=detail,
        )
        try:
            await self._notifier.send(text)
        except Exception as e:  # noqa: BLE001
            log.warning("telegram notification failed for %s: %s", issue_identifier, e)
            try:
                await db.notifications.queue_retry(self._conn, dedupe_key, text)
            except Exception as queue_exc:  # noqa: BLE001
                log.warning(
                    "telegram notification retry-queue failed for %s: %s",
                    issue_identifier,
                    queue_exc,
                )

    async def _retry_pending_notifications(self) -> None:
        """Flush `pending_notifications` — sends that were claimed but failed."""
        if not self._notifier.enabled:
            return
        try:
            pending = await db.notifications.list_pending(self._conn)
        except Exception as e:  # noqa: BLE001
            log.warning("telegram pending-notification lookup failed: %s", e)
            return
        for event_key, text in pending:
            try:
                await self._notifier.send(text)
            except Exception as e:  # noqa: BLE001
                log.warning("telegram notification retry failed for %s: %s", event_key, e)
                continue
            try:
                await db.notifications.clear_pending(self._conn, event_key)
            except Exception as e:  # noqa: BLE001
                log.warning("telegram pending-notification cleanup failed for %s: %s", event_key, e)

    async def _fail_run_and_reset_issue(
        self,
        run_id: str,
        reason: str,
        *,
        issue: LinearIssue,
        storage_issue_id: str | None = None,
        rollback_state_id: str,
        binding: RepoBinding | None = None,
        final_kind: str | None = None,
        returncode: int | None = None,
        exc: BaseException | str | None = None,
        termination_kind: str | None = None,
        termination_detail: str | None = None,
    ) -> None:
        storage_issue_id = storage_issue_id or issue.id
        await self._fail_run(
            run_id,
            reason,
            final_kind=final_kind,
            returncode=returncode,
            exc=exc,
            termination_kind=termination_kind,
            termination_detail=termination_detail,
        )
        target_state_id = rollback_state_id
        if binding is not None:
            try:
                states = await self._states_for_binding(binding)
            except LinearError as e:
                log.warning(
                    "could not load states while parking failed implement %s: %s",
                    issue.identifier,
                    e,
                )
            else:
                ready_id = states.get(binding.linear_states.ready)
                if issue.state_id == ready_id:
                    target_state_id = (
                        states.get(binding.linear_states.needs_approval)
                        or states.get(binding.linear_states.blocked)
                        or rollback_state_id
                    )
        try:
            tracker = (
                self.tracker(binding)
                if binding is not None
                else await self._tracker_for_issue_id(issue.id)
            )
            await tracker.move_issue(issue.id, target_state_id)
        except LinearError as e:
            log.warning(
                "could not park %s after failed dispatch: %s",
                issue.identifier,
                e,
            )
        if binding is None:
            return
        await self._track_implement_failed_wait(storage_issue_id, run_id, binding)
        await self._notify_attention(
            event=EVENT_RUN_FAILED,
            issue_identifier=issue.identifier,
            issue_url=issue.url,
            dedupe_key=f"run_failed:{run_id}",
            detail=reason,
        )
        tokens = await db.runs.tokens_for_issue(self._conn, storage_issue_id)
        body = failed(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                error=reason,
            )
        )
        body += (
            "\nReply with `$retry` or `$approve` to requeue this issue. "
            "Reply with `$reject` or `$stop` to leave it halted.\n"
        )
        try:
            tracker = self.tracker(binding)
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("implement failed comment post failed on %s: %s", issue.identifier, e)

    async def _block_implement_run(
        self,
        run_id: str,
        reason: str,
        *,
        issue: LinearIssue,
        storage_issue_id: str | None = None,
        rollback_state_id: str,
        binding: RepoBinding,
        returncode: int | None = None,
    ) -> None:
        """Park an Implement run that ended blocked on a human action.

        Mirrors `_fail_run_and_reset_issue` but opens an IMPLEMENT_BLOCKED
        operator wait and posts the verbatim human-action handoff comment so
        the operator can act and `$retry`. The run record keeps
        termination_kind="blocked" with the reason verbatim.
        """
        storage_issue_id = storage_issue_id or issue.id
        await self._fail_run(
            run_id,
            reason,
            returncode=returncode,
            termination_kind="blocked",
            termination_detail=reason,
        )
        target_state_id = rollback_state_id
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states while parking blocked implement %s: %s",
                issue.identifier,
                e,
            )
        else:
            ready_id = states.get(binding.linear_states.ready)
            if issue.state_id == ready_id:
                target_state_id = (
                    states.get(binding.linear_states.needs_approval)
                    or states.get(binding.linear_states.blocked)
                    or rollback_state_id
                )
        try:
            tracker = self.tracker(binding)
            await tracker.move_issue(issue.id, target_state_id)
        except LinearError as e:
            log.warning(
                "could not park %s after blocked dispatch: %s",
                issue.identifier,
                e,
            )
        await self._track_implement_blocked_wait(storage_issue_id, run_id, binding)
        await self._notify_attention(
            event=EVENT_OPERATOR_WAIT,
            issue_identifier=issue.identifier,
            issue_url=issue.url,
            dedupe_key=f"operator_wait:{run_id}",
            detail=reason,
        )
        tokens = await db.runs.tokens_for_issue(self._conn, storage_issue_id)
        body = implement_blocked(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                error=reason,
            )
        )
        try:
            tracker = self.tracker(binding)
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("implement blocked comment post failed on %s: %s", issue.identifier, e)

    async def _complete_already_satisfied_run(
        self,
        run_id: str,
        delivered_ref: str,
        *,
        issue: LinearIssue,
        storage_issue_id: str | None = None,
        rollback_state_id: str,
        binding: RepoBinding,
        workspace_path: Path,
        base_branch: str | None,
        returncode: int | None = None,
    ) -> bool:
        """Close a no-op Implement run whose scope was already delivered.

        The agent emitted ``SYMPHONY_ALREADY_DONE: <ref>`` and made no commit.
        Three things are verified before auto-closing, and any failing parks
        the run on the failed path instead: (1) the working tree is clean — a
        dirty tree means the agent edited files but did not commit, which
        contradicts "nothing to commit because it was pre-delivered" and is the
        genuine no-op failure the guard must catch; (2) the named commit is real
        and reachable from the delivery *base* branch — not merely an ancestor
        of HEAD, since unpushed commits left on the issue branch by an earlier
        failed implement are ancestors of HEAD too and must not pass as a
        landed-elsewhere delivery; (3) HEAD is not ahead of the base branch — a
        retry can start from a workspace whose branch already carries committed
        work from an earlier failed implement, and even when the named delivering
        commit legitimately lives in base, those extra commits are real unpushed
        work that closing as already-satisfied would silently discard (no push,
        no PR, no `$retry`), so an ahead branch is sent down the deliver path
        instead. The issue is moved to the terminal Done lane
        *before* the run is marked completed: a no-op run has nothing to push,
        so completing it while the issue is still in In Progress would strand
        the issue with no PR, no `$retry` path, and no reconciler. So if Done is
        unmapped/unloadable or the move raises, this returns False *without*
        marking the run completed, leaving the caller to park it on the
        failed/operator-wait path. On success the run is marked completed, an
        auto-comment references the delivering commit, and push / local review /
        PR are skipped entirely. Returns True when the issue actually reached
        Done; False when the claim is unverifiable or the close could not be
        completed (a plain done-without-commits still parks on an operator).
        """
        storage_issue_id = storage_issue_id or issue.id
        dirty = await _workspace_dirty_files(workspace_path)
        if dirty:
            log.warning(
                "implement run %s claimed already-done but left %d uncommitted "
                "change(s) in the workspace; a dirty tree contradicts the no-op "
                "claim, treating as failed",
                run_id,
                len(dirty),
            )
            return False
        candidate = _extract_delivering_commit(delivered_ref)
        if candidate is None or not await _workspace_ref_landed_in_base(
            workspace_path, candidate, base_branch
        ):
            log.warning(
                "implement run %s claimed already-done (ref=%r) but no "
                "delivering commit could be verified as landed in the base "
                "branch (%s); treating as failed",
                run_id,
                delivered_ref,
                base_branch,
            )
            return False
        if await _branch_ahead_of_base(workspace_path, base_branch):
            log.warning(
                "implement run %s claimed already-done but HEAD is ahead of the "
                "base branch (%s): the workspace carries committed, unpushed "
                "work — likely from an earlier failed implement — that closing "
                "as already-satisfied would silently discard (no push, no PR). "
                "Treating as failed so the work reaches the normal deliver path",
                run_id,
                base_branch,
            )
            return False

        # Work IS delivered, so Done reads more accurately than Cancelled. Move
        # the issue to Done before marking the run completed: only treat the
        # close as successful once the issue has actually reached Done, so a
        # failed transition cannot leave the issue stranded on a completed run.
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states while closing already-satisfied %s; "
                "leaving for the failed path: %s",
                issue.identifier,
                e,
            )
            return False
        done_id = states.get(binding.linear_states.done)
        if done_id is None:
            log.warning(
                "Done lane unmapped for %s; cannot close already-satisfied "
                "run %s, leaving for the failed path",
                issue.identifier,
                run_id,
            )
            return False
        if done_id != issue.state_id:
            try:
                await self.tracker(binding).move_issue(issue.id, done_id)
            except LinearError as e:
                log.warning(
                    "could not move %s to Done while closing already-satisfied "
                    "run %s; leaving for the failed path: %s",
                    issue.identifier,
                    run_id,
                    e,
                )
                return False

        await db.runs.update_status(
            self._conn,
            run_id,
            "completed",
            ended_at=self._now().isoformat(),
        )
        tokens = await db.runs.tokens_for_issue(self._conn, storage_issue_id)
        body = implement_already_satisfied(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
            ),
            delivered_ref=delivered_ref.strip() or candidate,
        )
        try:
            await self.tracker(binding).post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "already-satisfied close comment post failed on %s: %s",
                issue.identifier,
                e,
            )
        log.info(
            "implement run %s closed as already-satisfied (delivered by %s)",
            run_id,
            candidate,
        )
        return True

    async def _park_deliver_failed(
        self,
        reason: str,
        *,
        ctx: _PendingDelivery,
        exc: BaseException | str | None = None,
    ) -> None:
        """Park a post-completion delivery failure as a `deliver_failed` wait.

        The completion gate already passed, so — unlike `_fail_run_and_reset_issue`
        — this never rewinds the issue to the ready lane (which would re-dispatch
        the agent and re-park on "HEAD did not advance"). The branch and commits
        stay intact; the delivery context is stashed so a `$retry` resumes
        delivery via `_deliver_implement_run`.
        """
        binding = ctx.binding
        issue = ctx.issue
        storage_issue_id = ctx.storage_issue_id
        run_id = ctx.run_id
        await self._fail_run(
            run_id,
            reason,
            exc=exc,
            termination_kind="deliver_failed",
            termination_detail=reason,
        )
        # Park in a non-dispatch, operator-visible lane — never `ready`, which
        # the implement scan would pick up and re-run the agent on.
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states while parking deliver_failed %s: %s",
                issue.identifier,
                e,
            )
        else:
            target_state_id = states.get(binding.linear_states.needs_approval) or states.get(
                binding.linear_states.blocked
            )
            if target_state_id is not None and target_state_id != issue.state_id:
                try:
                    await self.tracker(binding).move_issue(issue.id, target_state_id)
                except LinearError as e:
                    log.warning(
                        "could not park %s after delivery failure: %s",
                        issue.identifier,
                        e,
                    )
        if ctx.reconstructed:
            self._pending_deliveries.pop(run_id, None)
        else:
            self._pending_deliveries[run_id] = ctx
        # Persist the real local-review verdict so a `$retry` after a daemon
        # restart (which drops the in-memory stash) rebuilds the human-approval
        # gate faithfully instead of assuming APPROVED. A needs-approval verdict
        # (EXHAUSTED / STUCK_LOOP) must stay parked, not silently ping @codex.
        local_review_outcome = (
            ctx.local_review_result.outcome.value if ctx.local_review_result is not None else None
        )
        await self._track_deliver_failed_wait(
            storage_issue_id,
            run_id,
            binding,
            local_review_outcome=local_review_outcome,
        )
        await self._notify_attention(
            event=EVENT_RUN_FAILED,
            issue_identifier=issue.identifier,
            issue_url=issue.url,
            dedupe_key=f"run_failed:{run_id}",
            detail=reason,
        )
        self._cancel_deliver_failed_review_poll_tasks(storage_issue_id)
        tokens = await db.runs.tokens_for_issue(self._conn, storage_issue_id)
        body = failed(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                error=reason,
            )
        )
        body += (
            "\nThe change is committed; only delivery failed. Reply with "
            "`$retry` to resume delivery (push + PR + handoff) on the existing "
            "branch without re-running the agent. Reply with `$reject` or "
            "`$stop` to leave it halted.\n"
        )
        try:
            await self.tracker(binding).post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("deliver_failed comment post failed on %s: %s", issue.identifier, e)

    async def _track_delivery_handoff_recovery_wait(self, ctx: _PendingDelivery) -> None:
        """Persist a temporary retry target before first review handoff."""
        local_review_outcome = (
            ctx.local_review_result.outcome.value if ctx.local_review_result is not None else None
        )
        await self._track_deliver_failed_wait(
            ctx.storage_issue_id,
            ctx.run_id,
            ctx.binding,
            local_review_outcome=local_review_outcome,
        )

    async def _track_deliver_failed_wait(
        self,
        issue_id: str,
        run_id: str,
        binding: RepoBinding,
        *,
        local_review_outcome: str | None = None,
    ) -> None:
        self._dispatch_run_ids[issue_id] = run_id
        self._operator_wait_run_ids.add(run_id)
        self._deliver_failed_run_bindings[run_id] = binding
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue_id,
            run_id=run_id,
            kind=db.operator_waits.KIND_DELIVER_FAILED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
            local_review_outcome=local_review_outcome,
        )

    async def _resolve_pending_delivery(
        self,
        issue_id: str,
        run_id: str,
        binding: RepoBinding,
        intent: SlashIntent,
    ) -> _PendingDelivery | None:
        """Return the delivery context to resume, reconstructing it if the
        daemon restarted and the in-memory stash was lost.

        The completion gate already passed before parking, so a reconstructed
        context reads the issue + workspace fresh (the branch and commits are
        intact on disk) and restores the local-review verdict persisted on the
        wait — preserving the human-approval gate for a `needs_approval`
        verdict rather than assuming APPROVED.
        """
        ctx = self._pending_deliveries.get(run_id)
        if ctx is not None:
            try:
                workspace_path = await self._workspace.acquire(binding, ctx.issue)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "could not re-acquire workspace to resume delivery for %s: %s",
                    ctx.issue.identifier,
                    e,
                )
                await self._post_command_rejected(
                    issue_id,
                    self._slash_text(intent),
                    f"could not re-acquire workspace to resume delivery: {e}",
                )
                return None
            return replace(
                ctx,
                workspace_path=workspace_path,
                retry_workspace_acquired=True,
            )
        tracker_issue_id, _ = await self._tracker_identity_for_issue(issue_id)
        try:
            issue = await self.tracker(binding).lookup_issue(tracker_issue_id)
        except LinearError as e:
            log.warning("could not look up %s to resume delivery: %s", issue_id, e)
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                f"could not look up issue to resume delivery: {e}",
            )
            return None
        try:
            workspace_path = await self._workspace.acquire(binding, issue)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "could not re-acquire workspace to resume delivery for %s: %s",
                issue.identifier,
                e,
            )
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                f"could not re-acquire workspace to resume delivery: {e}",
            )
            return None
        tokens = await db.runs.tokens_for_issue(self._conn, issue_id)
        return _PendingDelivery(
            binding=binding,
            issue=issue,
            storage_issue_id=issue_id,
            run_id=run_id,
            workspace_path=workspace_path,
            branch=f"{binding.branch_prefix}/{issue.identifier.lower()}",
            cumulative_usage=UsageDelta(
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
            ),
            local_review_result=await self._reconstructed_local_review_result(run_id),
            reconstructed=True,
            retry_workspace_acquired=True,
        )

    async def _reconstructed_local_review_result(self, run_id: str) -> LoopResult | None:
        """Rebuild the local-review verdict for a restart-reconstructed resume.

        Reads the outcome persisted on the `deliver_failed` wait. A
        needs-approval verdict (EXHAUSTED / STUCK_LOOP) is preserved so the
        delivery handoff re-parks for human approval instead of pinging
        `@codex` / merging. Falls back to a synthetic APPROVED when nothing was
        persisted (legacy rows, or a binding without local review): `None`
        would read as "not approved" and dead-end a `local_review` binding in
        `_fail_review_run`, whereas APPROVED lets the gate treat it as passed.
        """
        wait = await db.operator_waits.get_by_run_id(self._conn, run_id)
        outcome = LoopOutcome.APPROVED
        if wait is not None and wait.local_review_outcome:
            try:
                outcome = LoopOutcome(wait.local_review_outcome)
            except ValueError:
                log.warning(
                    "unknown persisted local_review_outcome %r for run %s; treating as APPROVED",
                    wait.local_review_outcome,
                    run_id,
                )
        return LoopResult(outcome=outcome, iterations=0, verdicts=())


log = logging.getLogger(__name__)


MERGE_WAIT_RECONCILE_INTERVAL_SECS = 600


# Grace before an orphaned merge `needs_approval` run (operator wait gone) is
# retired — long enough to never race a freshly-created wait.
ORPHANED_MERGE_RUN_GRACE_SECS = 120


MERGED_LINEAR_STATE_RECONCILE_TICK_INTERVAL = 5


MERGED_LINEAR_STATE_RECONCILE_LOOKBACK_HOURS = 24


PARKED_CLOSED_UNMERGED_COMMENT = "🛑 PR closed without merge — marking done"


# Shared, capped exponential-backoff knobs for every requeue-based infra-error
# retry path (acceptance + the agent stages). `AGENT_INFRA_RETRY_LIMIT` is the
# transient-API-error retry budget for reviewer/implement/fix runs before they
# fall through to the existing infra-failure escalation.
ACCEPTANCE_INFRA_RETRY_BASE_BACKOFF_SECS = 30


ACCEPTANCE_INFRA_RETRY_MAX_BACKOFF_SECS = 120


AGENT_INFRA_RETRY_LIMIT = 5


# All transient-retry kinds share the same retry budget and backoff logic.
# TRANSIENT_API_RETRY_KIND: implement-phase failure (no work done, HEAD unchanged).
# LOCAL_REVIEW_TRANSIENT_RETRY_KIND: local-review-phase failure (implement succeeded,
#   commits intact, but reviewer got a transient 500 before verdicting).
# REVIEW_FIX_TRANSIENT_RETRY_KIND: review-stage fix agent failure (PR exists, commits
#   intact, but fix agent got a transient 500 and made no HEAD advance).
_AGENT_INFRA_RETRY_KINDS: frozenset[str] = frozenset(
    {
        db.runs.TRANSIENT_API_RETRY_KIND,
        db.runs.LOCAL_REVIEW_TRANSIENT_RETRY_KIND,
        db.runs.REVIEW_FIX_TRANSIENT_RETRY_KIND,
    }
)


_UsageCostEstimator = UsageCostEstimator  # back-compat alias for internal callers


async def _record_run_model_usage(
    conn: aiosqlite.Connection,
    run_id: str,
    log_path: Path,
    *,
    codex_model: str | None,
) -> None:
    """Attribute a finished run's tokens to (provider, model) from its log.

    Re-parses the full run log with the reusable `parse_model_usage` parser
    and rewrites `run_model_usage` wholesale, so calling it repeatedly as a
    multi-subprocess run's log grows stays idempotent. Best-effort: a read
    or DB error must never fail the run itself.
    """
    try:
        text = await asyncio.to_thread(log_path.read_text, encoding="utf-8", errors="replace")
    except OSError:
        return
    usages = parse_model_usage(text.splitlines(), codex_model=codex_model)
    if not usages:
        return
    try:
        await db.run_model_usage.replace_for_run(conn, run_id, usages)
    except aiosqlite.Error:
        log.warning("could not persist per-model usage for run %s", run_id)


def _parse_local_review_model_usage(
    log_dir: Path,
    *,
    implementer_codex_model: str | None,
    reviewer_codex_model: str | None,
) -> list[ModelUsage]:
    """Parse local-review role transcripts into per-(provider, model) usage.

    `fix-*.out.log` are implementer turns, `review-*.out.log` reviewer
    turns; each file is one process, so it is parsed independently and the
    rows are merged downstream. Sync (file IO) — call via `to_thread`.
    """
    usages: list[ModelUsage] = []
    try:
        paths = sorted(log_dir.glob("*.out.log"))
    except OSError:
        return usages
    for path in paths:
        if path.name.startswith("fix-"):
            codex_model = implementer_codex_model
        elif path.name.startswith("review-"):
            codex_model = reviewer_codex_model
        else:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        usages.extend(parse_model_usage(text.splitlines(), codex_model=codex_model))
    return usages


def _activity_settings_for(config: Config, binding: RepoBinding) -> ActivitySettings:
    return ActivitySettings(
        enabled=(
            config.activity_comments_enabled
            if binding.activity_comments_enabled is None
            else binding.activity_comments_enabled
        ),
        interval_secs=(
            config.activity_comment_interval_secs
            if binding.activity_comment_interval_secs is None
            else binding.activity_comment_interval_secs
        ),
        min_interval_secs=(
            config.activity_comment_min_interval_secs
            if binding.activity_comment_min_interval_secs is None
            else binding.activity_comment_min_interval_secs
        ),
        event_threshold=(
            config.activity_comment_event_threshold
            if binding.activity_comment_event_threshold is None
            else binding.activity_comment_event_threshold
        ),
        long_running_secs=(
            config.activity_comment_long_running_secs
            if binding.activity_comment_long_running_secs is None
            else binding.activity_comment_long_running_secs
        ),
        long_running_repeat_secs=(
            config.activity_comment_long_running_repeat_secs
            if binding.activity_comment_long_running_repeat_secs is None
            else binding.activity_comment_long_running_repeat_secs
        ),
        include_failed_output_lines=(
            config.activity_comment_include_failed_output_lines
            if binding.activity_comment_include_failed_output_lines is None
            else binding.activity_comment_include_failed_output_lines
        ),
    )


@dataclass(frozen=True)
class WebhookDispatchResult:
    kind: str
    handled: bool
    detail: str = ""


def _local_review_status_from_result(result: LoopResult | None) -> str:
    """Map a `LoopResult` to a `runs.status` literal.

    Symmetric with how Implement uses `completed` / `failed`.
    """
    if result is None:
        return "failed"
    if result.outcome == LoopOutcome.APPROVED:
        return "completed"
    return "failed"


def _binding_storage_key(binding: RepoBinding) -> str:
    return json.dumps(_binding_key(binding), separators=(",", ":"))


def _binding_label_from_storage_key(binding_key: str) -> str | None:
    if not binding_key:
        return None
    try:
        raw = json.loads(binding_key)
    except ValueError:
        return None
    if not isinstance(raw, list) or len(raw) < 3:
        return None
    label = raw[2]
    if label is None:
        return ""
    return str(label)


def _infra_retry_backoff_secs(attempt: int) -> int:
    """Capped exponential backoff for the `attempt`-th infra retry (>= 1).

    Shared by the acceptance and agent-run transient-error requeue paths so all
    stages back off identically off the same knobs."""
    return int(
        min(
            ACCEPTANCE_INFRA_RETRY_BASE_BACKOFF_SECS * (2 ** (attempt - 1)),
            ACCEPTANCE_INFRA_RETRY_MAX_BACKOFF_SECS,
        )
    )


def _read_run_final_message(log_path: Path, *, agent: str) -> str:
    """Extract the agent's final message from a stage run log, or "" on error.

    The run log is the runner's stdout (`claude` stream-json or `codex`
    JSONL). The completion gate reads the final message to look for the
    SYMPHONY_DONE / SYMPHONY_BLOCKED marker.
    """
    if agent not in ("claude", "codex"):
        return ""
    try:
        stdout = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    reviewer_agent: Literal["claude", "codex"] = "claude" if agent == "claude" else "codex"
    return extract_last_agent_message(agent=reviewer_agent, stdout=stdout)


_COMMIT_SHA_RE = re.compile(r"\b[0-9a-fA-F]{7,40}\b")


def _extract_delivering_commit(ref: str) -> str | None:
    """Pull the first commit-SHA-looking token out of a `SYMPHONY_ALREADY_DONE`
    ref (e.g. ``"f483299 (adjust/adjust_os#291)"`` -> ``"f483299"``).

    Returns None when the ref names no verifiable commit SHA — a bare PR number
    or prose can't be checked for ancestry, so the caller treats it as a failed
    no-op rather than auto-closing on an unverifiable claim.
    """
    if not ref:
        return None
    match = _COMMIT_SHA_RE.search(ref)
    return match.group(0) if match else None


def dirty_tree_fix_prompt(dirty_files: list[str]) -> str:
    """Prompt for the single pre-push fix turn on a dirty working tree.

    Deliberately does NOT auto-commit for the agent: shipping `.env`
    files or debug artifacts blindly is worse than failing the run.
    """
    listing = "\n".join(f"- {line}" for line in dirty_files)
    return (
        "These files are uncommitted:\n"
        f"{listing}\n\n"
        "Either commit them or explain why they are scratch/junk "
        "(then clean them up). The working tree must be clean "
        "(`git status --porcelain` empty) when you finish."
    )


def _linear_issue_state_changed(payload: Mapping[str, Any]) -> bool:
    action = str(payload.get("action") or "").casefold()
    if action and action not in {"update", "updated", "issue_updated"}:
        return False
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return False
    updated_from = payload.get("updatedFrom") or data.get("updatedFrom")
    if isinstance(updated_from, Mapping) and any(
        key in updated_from for key in ("state", "stateId", "state_id", "stateName", "state_name")
    ):
        return True
    return False


def _linear_issue_state_transition(
    payload: Mapping[str, Any],
) -> tuple[str | None, str | None, str | None, str | None]:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None, None, None, None
    updated_from = payload.get("updatedFrom") or data.get("updatedFrom")
    old_state_id: str | None = None
    old_state_name: str | None = None
    if isinstance(updated_from, Mapping):
        old_state_id, old_state_name = _linear_state_fields(updated_from)
    new_state_id, new_state_name = _linear_state_fields(data)
    return old_state_id, old_state_name, new_state_id, new_state_name


def _linear_state_fields(source: Mapping[str, Any]) -> tuple[str | None, str | None]:
    state_id: str | None = None
    state_name: str | None = None
    state = source.get("state")
    if isinstance(state, Mapping):
        state_id = _first_str(state, "id", "stateId", "state_id")
        state_name = _first_str(state, "name", "stateName", "state_name")
    elif isinstance(state, str):
        state_name = state
    state_id = state_id or _first_str(source, "stateId", "state_id")
    state_name = state_name or _first_str(source, "stateName", "state_name")
    return state_id, state_name


def _first_str(source: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = source.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _comment_issue_id_from_webhook_payload(payload: Mapping[str, Any]) -> str | None:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    issue_id = data.get("issueId")
    if isinstance(issue_id, str) and issue_id:
        return issue_id
    issue = data.get("issue")
    if isinstance(issue, Mapping):
        nested = issue.get("id")
        if isinstance(nested, str) and nested:
            return nested
    return None


def _comment_from_webhook_payload(payload: Mapping[str, Any]) -> LinearComment | None:
    return comment_from_webhook_payload(payload)
