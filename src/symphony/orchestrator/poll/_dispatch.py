"""`_DispatchMixin` — the dispatch domain of the poll loop (SYM-149).

Owns scheduling, capacity/slot/sem accounting, the ready-issue scan, and the
park guards (`_park_already_has_pr`, `_park_blocked_by_deps`). It extends
`_OrchestratorBase` so it sees the shared state + foundation methods; the
concrete `Orchestrator` (in `__init__.py`) inherits this mixin.

The cross-domain methods this layer calls (`_dispatch_one`, `_fail_run`, …)
still live on `Orchestrator`; they are declared under `TYPE_CHECKING` below so
mypy resolves them without a runtime stub.

Pure structural extraction: method bodies are byte-for-byte unchanged from the
pre-split `Orchestrator`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
from collections.abc import Set as AbstractSet
from contextlib import asynccontextmanager
from dataclasses import replace
from functools import partial
from typing import TYPE_CHECKING, cast

from ... import db
from ...config import RepoBinding
from ...github.client import GitHubError
from ...linear.blockers import is_blocked, open_blocker_ids
from ...linear.client import LinearError
from ...linear.templates import CommentVars, moved_to_waiting, truncate_body
from ...tracker import Issue as LinearIssue
from ...tracker import TrackerContext
from ._base import (
    BindingKey,
    _binding_key,
    _OrchestratorBase,
    _queue_scope,
    _tracker_context_for_binding,
)
from ._helpers import _pr_view_is_closed, _pr_view_is_merged

log = logging.getLogger(__name__)


class _DispatchMixin(_OrchestratorBase):
    """Dispatch domain of the poll loop; `Orchestrator` extends it."""

    if TYPE_CHECKING:
        # Sibling-domain methods provided by the concrete `Orchestrator`.
        async def _agent_infra_retry_backoff_active(self, issue_id: str) -> bool: ...

        async def _mark_parked_closed_unmerged_pr_done(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            pr: db.issue_prs.IssuePR,
        ) -> bool: ...

        async def _dispatch_one(self, binding: RepoBinding, issue: LinearIssue) -> str | None: ...

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
        ) -> None: ...

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
        ) -> None: ...

    async def _scan_binding(self, binding: RepoBinding) -> list[asyncio.Task[None]]:
        scheduled: list[asyncio.Task[None]] = []
        # Disabled binding: start no new issues and drop its tracker-queue lanes
        # so the UI board reflects the pause. The binding stays loaded (and so
        # visible to review monitors, merge polling, and operator-wait
        # resolution, which all iterate `config.repos`), letting in-flight work
        # drain to completion (SYM-193).
        if not binding.enabled:
            await self._clear_disabled_binding_lanes(binding)
            return scheduled
        ready_state = binding.linear_states.ready
        waiting_state = binding.linear_states.waiting
        waiting_issues: list[LinearIssue] = []
        tracker = self.tracker(binding)
        try:
            if waiting_state is None:
                issues = await tracker.issues_in_state(
                    binding.linear_team_key, ready_state, binding.issue_label
                )
            else:
                ready_result, waiting_result = await asyncio.gather(
                    tracker.issues_in_state(
                        binding.linear_team_key, ready_state, binding.issue_label
                    ),
                    tracker.issues_in_state(
                        binding.linear_team_key, waiting_state, binding.issue_label
                    ),
                    return_exceptions=True,
                )
                scan_failed = False
                for result in (ready_result, waiting_result):
                    if isinstance(result, LinearError):
                        log.warning("scan failed for %s: %s", binding.linear_team_key, result)
                        scan_failed = True
                    elif isinstance(result, BaseException):
                        raise result
                if scan_failed:
                    return scheduled
                issues = cast(list[LinearIssue], ready_result)
                waiting_issues = cast(list[LinearIssue], waiting_result)
                self._known_waiting_issue_ids = {issue.id for issue in waiting_issues}
        except LinearError as e:
            log.warning("scan failed for %s: %s", binding.linear_team_key, e)
            return scheduled
        log.info(
            "scan %s: %d issue(s) in %s%s",
            binding.linear_team_key,
            len(issues),
            ready_state,
            f" with label '{binding.issue_label}'" if binding.issue_label else "",
        )
        # Unblock first so the snapshot reflects the moves — an issue promoted
        # to Ready must not linger in the UI's Waiting lane until the next poll.
        unblocked_ids = await self._auto_unblock_waiting(binding, waiting_issues)
        await self._persist_queue_snapshot(
            binding, issues, waiting_issues, unblocked_ids=unblocked_ids
        )
        if self.config.global_max_concurrent <= 0 or binding.max_concurrent <= 0:
            log.info(
                "scan %s: dispatch capacity is zero (global=%d, binding=%d)",
                binding.linear_team_key,
                self.config.global_max_concurrent,
                binding.max_concurrent,
            )
            return scheduled
        capacity = self._dispatch_capacity(binding)
        if capacity <= 0:
            log.info("scan %s: dispatch capacity is full", binding.linear_team_key)
            return scheduled
        for issue in issues:
            task = await self._schedule_ready_issue(binding, issue)
            if task is None:
                continue
            scheduled.append(task)
            if len(scheduled) >= capacity:
                break
        return scheduled

    async def _clear_disabled_binding_lanes(self, binding: RepoBinding) -> None:
        """Drop a disabled binding's `tracker_queue` snapshot so its Ready/
        Waiting lanes vanish from the UI board while it's paused (SYM-193).
        `_prune_tracker_queue_scopes` can't do this — the scope is still
        configured, just disabled — so replace the snapshot with no rows."""
        await db.tracker_queue.replace_scan(
            self._conn,
            team_key=binding.linear_team_key,
            scope=_queue_scope(binding),
            rows=[],
            seen_at=self._now().isoformat(),
        )

    async def _persist_queue_snapshot(
        self,
        binding: RepoBinding,
        ready_issues: Sequence[LinearIssue],
        waiting_issues: Sequence[LinearIssue],
        *,
        unblocked_ids: AbstractSet[str] = frozenset(),
    ) -> None:
        """Mirror the scan result into `tracker_queue` for the UI board.

        The waiting fetch is not label-filtered (auto-unblock covers the whole
        lane), so apply the binding's label here to keep the snapshot scoped to
        issues Symphony would actually dispatch. Waiting issues that
        `_auto_unblock_waiting` just moved to Ready are recorded as ready.
        """
        rows = [
            db.tracker_queue.QueueRow(
                issue_id=issue.id,
                identifier=issue.identifier,
                title=issue.title,
                queue="ready",
                state_name=issue.state_name,
            )
            for issue in ready_issues
        ]
        # The Ready and Waiting fetches run concurrently, so an issue moving
        # between them can appear in both — the ready row wins (a duplicate
        # insert would violate the snapshot's primary key).
        seen_ids = {issue.id for issue in ready_issues}
        for issue in waiting_issues:
            if issue.id in seen_ids:
                continue
            seen_ids.add(issue.id)
            if binding.issue_label and binding.issue_label not in issue.labels:
                continue
            if issue.id in unblocked_ids:
                rows.append(
                    db.tracker_queue.QueueRow(
                        issue_id=issue.id,
                        identifier=issue.identifier,
                        title=issue.title,
                        queue="ready",
                        state_name=binding.linear_states.ready,
                    )
                )
                continue
            rows.append(
                db.tracker_queue.QueueRow(
                    issue_id=issue.id,
                    identifier=issue.identifier,
                    title=issue.title,
                    queue="waiting",
                    state_name=issue.state_name,
                    blocked_by=", ".join(open_blocker_ids(issue)),
                )
            )
        await db.tracker_queue.replace_scan(
            self._conn,
            team_key=binding.linear_team_key,
            scope=_queue_scope(binding),
            rows=rows,
            seen_at=self._now().isoformat(),
        )

    async def _auto_unblock_waiting(
        self, binding: RepoBinding, waiting_issues: list[LinearIssue]
    ) -> set[str]:
        """Move no-longer-blocked Waiting issues to Ready; return moved ids."""
        unblocked_issues = [issue for issue in waiting_issues if not is_blocked(issue)]
        if not unblocked_issues:
            return set()
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states before auto-unblocking waiting issues for %s: %s",
                binding.linear_team_key,
                e,
            )
            return set()
        ready_id = states.get(binding.linear_states.ready)
        if ready_id is None:
            log.warning(
                "could not auto-unblock waiting issues for %s: missing Linear state %r",
                binding.linear_team_key,
                binding.linear_states.ready,
            )
            return set()

        tracker = self.tracker(binding)
        moved: set[str] = set()
        for issue in unblocked_issues:
            try:
                await tracker.move_issue(issue.id, ready_id)
            except LinearError as e:
                log.warning("could not auto-unblock %s to Ready: %s", issue.identifier, e)
                continue
            log.info("auto-unblocked %s -> Ready", issue.identifier)
            moved.add(issue.id)
        return moved

    def _dispatch_capacity(self, binding: RepoBinding) -> int:
        if self.config.global_max_concurrent <= 0 or binding.max_concurrent <= 0:
            return 0
        binding_key = _binding_key(binding)
        return max(
            0,
            min(
                self.config.global_max_concurrent - self._scheduled_slot_count(),
                binding.max_concurrent - self._scheduled_binding_counts.get(binding_key, 0),
            ),
        )

    def _scheduled_slot_count(self) -> int:
        return sum(self._scheduled_issue_refcounts.values())

    def _reserve_scheduled_slot(self, *, issue_id: str, binding_key: BindingKey) -> None:
        self._scheduled_issue_refcounts[issue_id] = (
            self._scheduled_issue_refcounts.get(issue_id, 0) + 1
        )
        self._scheduled_issue_ids.add(issue_id)
        self._scheduled_binding_counts[binding_key] = (
            self._scheduled_binding_counts.get(binding_key, 0) + 1
        )

    def _release_scheduled_slot(self, *, issue_id: str, binding_key: BindingKey) -> None:
        issue_count = self._scheduled_issue_refcounts.get(issue_id, 0)
        if issue_count <= 1:
            self._scheduled_issue_refcounts.pop(issue_id, None)
            self._scheduled_issue_ids.discard(issue_id)
        else:
            self._scheduled_issue_refcounts[issue_id] = issue_count - 1
        count = self._scheduled_binding_counts.get(binding_key, 0)
        if count <= 1:
            self._scheduled_binding_counts.pop(binding_key, None)
        else:
            self._scheduled_binding_counts[binding_key] = count - 1

    @asynccontextmanager
    async def _review_fix_dispatch_slot(
        self,
        binding: RepoBinding,
        issue: LinearIssue,
        *,
        dispatch_capacity_held: bool = False,
    ) -> AsyncIterator[None]:
        """Reserve priority capacity for a review-fix job.

        Review monitors may run without consuming dispatch slots, but once a
        monitor finds work that changes code, that work should sit ahead of new
        implementation jobs in both the global and per-binding queues.
        """
        binding_key = _binding_key(binding)
        binding_sem = self._binding_dispatch_sems.setdefault(
            binding_key,
            asyncio.Semaphore(max(binding.max_concurrent, 1)),
        )
        review_binding_sem = self._review_fix_binding_sems.setdefault(
            binding_key,
            asyncio.Semaphore(max(binding.max_concurrent, 1)),
        )
        if dispatch_capacity_held:
            # The caller already holds normal dispatch capacity. Acquiring
            # review-fix semaphores here would invert the order used below
            # and can deadlock against an active review-fix waiting for the
            # dispatch semaphores.
            yield
            return

        self._reserve_scheduled_slot(issue_id=issue.id, binding_key=binding_key)
        try:
            async with self._review_fix_sem, review_binding_sem:
                async with self._global_dispatch_sem, binding_sem:
                    yield
        finally:
            self._release_scheduled_slot(issue_id=issue.id, binding_key=binding_key)

    def is_dispatch_paused(self) -> bool:
        """Whether the daemon-level dispatch kill-switch is engaged."""
        return self._dispatch_paused

    async def set_dispatch_paused(self, paused: bool) -> None:
        """Engage/release the dispatch kill-switch.

        Acquires `_dispatch_pause_lock` so the toggle can't land in the
        middle of `_dispatch_one`'s final check-then-insert critical section.
        Resuming wakes the poll loop so pending Ready issues dispatch
        promptly instead of waiting for the next poll interval.
        """
        async with self._dispatch_pause_lock:
            self._dispatch_paused = paused
        if not paused:
            self._wake.set()

    async def _schedule_ready_issue(
        self, binding: RepoBinding, issue: LinearIssue
    ) -> asyncio.Task[None] | None:
        async with self._schedule_lock:
            # Kill-switch: start no new runs while paused (in-flight runs and
            # their follow-up stages are unaffected — they don't route here).
            if self._dispatch_paused:
                return None
            if self._dispatch_capacity(binding) <= 0:
                return None
            if issue.id in self._scheduled_issue_ids:
                return None
            if issue.id in self._dispatch_run_ids:
                return None
            if await db.runs.has_running_or_completed(self._conn, issue.id):
                return None
            # Resolve the storage issue id (db.issues.upsert may assign a
            # different id when two tracker sites share the same raw issue id).
            # Runs are stored under the storage id, so the backoff check must
            # use it — otherwise the backoff window is invisible to the gate.
            _storage_id = await self._storage_issue_id_for_tracker_issue(
                issue.id, _tracker_context_for_binding(binding)
            )
            if await self._agent_infra_retry_backoff_active(_storage_id):
                # A prior run hit a transient API error and is inside
                # its capped backoff window — re-dispatch once it elapses.
                return None
            pr = await db.issue_prs.get(
                self._conn,
                issue_id=issue.id,
                github_repo=binding.github_repo,
            )
            if pr is not None:
                blocking_pr, handled = await self._blocking_existing_pr(binding, issue, pr)
                if handled:
                    return None
                if blocking_pr is not None:
                    await self._park_already_has_pr(binding, issue, blocking_pr)
                    return None
            if binding.linear_states.waiting is not None and is_blocked(issue):
                await self._park_blocked_by_deps(binding, issue)
                return None
            return self._schedule_dispatch(binding, issue)

    async def _blocking_existing_pr(
        self,
        binding: RepoBinding,
        issue: LinearIssue,
        pr: db.issue_prs.IssuePR,
    ) -> tuple[db.issue_prs.IssuePR | None, bool]:
        if pr.merged_at is not None:
            return pr, False

        try:
            view = await self._gh.pr_view(pr.pr_number, repo=binding.github_repo)
        except GitHubError as e:
            log.warning(
                "could not verify existing PR before ready dispatch for %s#%d: %s",
                binding.github_repo,
                pr.pr_number,
                e,
            )
            return pr, False

        if _pr_view_is_merged(view):
            merged_at = str(view.get("mergedAt") or self._now().isoformat())
            updated = await db.issue_prs.update_merged(
                self._conn,
                issue_id=issue.id,
                github_repo=binding.github_repo,
                pr_number=pr.pr_number,
                merged_at=merged_at,
            )
            if not updated:
                log.warning(
                    "could not mark existing PR row merged before parking %s for %s#%d",
                    issue.identifier,
                    binding.github_repo,
                    pr.pr_number,
                )
            return replace(pr, merged_at=merged_at), False

        if _pr_view_is_closed(view):
            if pr.parked_at is not None and not binding.auto_merge:
                await self._mark_parked_closed_unmerged_pr_done(
                    binding=binding,
                    issue=issue,
                    pr=pr,
                )
                return None, True
            deleted = await db.issue_prs.delete(
                self._conn,
                issue_id=issue.id,
                github_repo=binding.github_repo,
                pr_number=pr.pr_number,
            )
            if not deleted:
                log.warning(
                    "could not delete closed unmerged PR row before ready dispatch for %s#%d",
                    binding.github_repo,
                    pr.pr_number,
                )
            return None, False

        return pr, False

    async def _park_already_has_pr(
        self,
        binding: RepoBinding,
        issue: LinearIssue,
        pr: db.issue_prs.IssuePR,
    ) -> None:
        if pr.merged_at is not None:
            target_state = binding.linear_states.done
            body = (
                f"🛑 Cannot re-implement: PR #{pr.pr_number} was already merged at "
                f"{pr.merged_at}. Moving issue back to {target_state}. To genuinely "
                "redo this work, revert the merge and remove the `issue_prs` row. "
                f"{pr.pr_url}"
            )
        else:
            target_state = binding.linear_states.in_progress
            body = (
                f"🛑 Cannot re-implement: PR #{pr.pr_number} is still open. Moving "
                f"issue back to {target_state}. Close the PR via `gh pr close` if "
                f"you want to abandon it. {pr.pr_url}"
            )

        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states before parking %s with existing PR #%d: %s",
                issue.identifier,
                pr.pr_number,
                e,
            )
            return
        else:
            target_id = states.get(target_state)
            if target_id is None:
                log.warning(
                    "could not move %s after existing PR guard: missing Linear "
                    "state %r for team %s",
                    issue.identifier,
                    target_state,
                    binding.linear_team_key,
                )
                return
            else:
                tracker = self.tracker(binding)
                try:
                    await tracker.move_issue(issue.id, target_id)
                except LinearError as e:
                    log.warning(
                        "could not move %s after existing PR guard for PR #%d: %s",
                        issue.identifier,
                        pr.pr_number,
                        e,
                    )
                    return

        # The guard moved the issue out of the queue lanes — drop its
        # just-written snapshot row so the board reflects the park now.
        await db.tracker_queue.remove(
            self._conn,
            team_key=binding.linear_team_key,
            scope=_queue_scope(binding),
            issue_id=issue.id,
        )

        try:
            await self.tracker(binding).post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "could not comment after existing PR guard for %s PR #%d: %s",
                issue.identifier,
                pr.pr_number,
                e,
            )

    async def _park_blocked_by_deps(self, binding: RepoBinding, issue: LinearIssue) -> None:
        blockers = open_blocker_ids(issue)
        if not blockers:
            return
        waiting = binding.linear_states.waiting
        if waiting is None:
            return
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states before moving %s to waiting for blockers %s: %s",
                issue.identifier,
                blockers,
                e,
            )
            return

        waiting_id = states.get(waiting)
        if waiting_id is None:
            log.warning(
                "could not move %s to waiting: missing Linear state %r for team %s",
                issue.identifier,
                waiting,
                binding.linear_team_key,
            )
            return
        ready_id = states.get(binding.linear_states.ready)

        tracker = self.tracker(binding)
        try:
            await tracker.move_issue(issue.id, waiting_id)
        except LinearError as e:
            log.warning(
                "could not move %s to waiting for dependency blockers %s: %s",
                issue.identifier,
                blockers,
                e,
            )
            return

        body = moved_to_waiting(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,
                next_stage=waiting,
                linear_identifier=issue.identifier,
            ),
            blockers,
        )
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "could not comment after moving %s to waiting for blockers %s: %s",
                issue.identifier,
                blockers,
                e,
            )
            if ready_id is not None:
                try:
                    await tracker.move_issue(issue.id, ready_id)
                except LinearError as rollback_error:
                    log.warning(
                        "could not move %s back to ready after waiting comment failed: %s",
                        issue.identifier,
                        rollback_error,
                    )
            return

        log.info(
            "moved %s to %s because it is blocked by %s",
            issue.identifier,
            waiting,
            ", ".join(blockers),
        )
        # Same-tick park: flip the just-written queue row so the UI doesn't
        # show a Todo card for an issue this very poll moved to Waiting.
        await db.tracker_queue.mark_waiting(
            self._conn,
            team_key=binding.linear_team_key,
            scope=_queue_scope(binding),
            issue_id=issue.id,
            state_name=waiting,
            blocked_by=", ".join(blockers),
            seen_at=self._now().isoformat(),
        )

    def _ready_binding_for_issue(
        self, issue: LinearIssue, tracker_ctx: TrackerContext | None = None
    ) -> RepoBinding | None:
        issue_labels = set(issue.labels)
        for binding in self.config.repos:
            # A disabled binding starts no new work — the same pause
            # `_scan_binding` applies to the poll scan, mirrored here so a
            # Linear issue webhook can't route a first dispatch around it
            # (SYM-193 review).
            if not binding.enabled:
                continue
            if tracker_ctx is not None and _tracker_context_for_binding(binding) != tracker_ctx:
                continue
            if binding.linear_team_key != issue.team_key:
                continue
            if issue.state_name != binding.linear_states.ready:
                continue
            if binding.issue_label and binding.issue_label not in issue_labels:
                continue
            return binding
        return None

    def _schedule_dispatch(self, binding: RepoBinding, issue: LinearIssue) -> asyncio.Task[None]:
        binding_key = _binding_key(binding)
        self._reserve_scheduled_slot(issue_id=issue.id, binding_key=binding_key)
        task = asyncio.create_task(self._dispatch_with_limits(binding, issue))
        self._dispatch_tasks.add(task)
        task.add_done_callback(
            partial(
                self._dispatch_task_done,
                issue_id=issue.id,
                binding_key=binding_key,
            )
        )
        return task

    async def _dispatch_with_limits(self, binding: RepoBinding, issue: LinearIssue) -> None:
        key = _binding_key(binding)
        binding_sem = self._binding_dispatch_sems.setdefault(
            key,
            asyncio.Semaphore(max(binding.max_concurrent, 1)),
        )
        try:
            async with self._global_dispatch_sem:
                async with binding_sem:
                    # Re-check the kill-switch: it may have been toggled on
                    # while this task was waiting on the semaphores above.
                    if self._dispatch_paused:
                        log.info(
                            "skipping %s: dispatch paused while waiting for a slot",
                            issue.identifier,
                        )
                        return
                    current = await self._refresh_dispatch_candidate(binding, issue)
                    if current is None:
                        return
                    if self._dispatch_paused:
                        log.info(
                            "skipping %s: dispatch paused while refreshing candidate",
                            issue.identifier,
                        )
                        return
                    await self._dispatch_one(binding, current)
        except asyncio.CancelledError:
            await self._mark_cancelled_dispatch(issue, binding)
            raise
        finally:
            run_id = self._dispatch_run_ids.get(issue.id)
            if run_id is not None:
                if run_id not in self._operator_wait_run_ids:
                    self._dispatch_run_ids.pop(issue.id, None)
                self._runs_moved_to_in_progress.discard(run_id)

    async def _mark_cancelled_dispatch(
        self, issue: LinearIssue, binding: RepoBinding | None = None
    ) -> None:
        run_id = self._dispatch_run_ids.get(issue.id)
        if run_id is None:
            return
        log.info("dispatch cancelled for %s [run_id=%s]", issue.identifier, run_id)
        if run_id in self._runs_moved_to_in_progress:
            await self._fail_run_and_reset_issue(
                run_id,
                "dispatch cancelled",
                issue=issue,
                rollback_state_id=issue.state_id,
                binding=binding,
            )
        else:
            await self._fail_run(run_id, "dispatch cancelled")

    async def _refresh_dispatch_candidate(
        self, binding: RepoBinding, issue: LinearIssue
    ) -> LinearIssue | None:
        tracker = self.tracker(binding)
        try:
            current = await tracker.lookup_issue(issue.id)
        except LinearError as e:
            log.warning("could not revalidate %s before dispatch: %s", issue.identifier, e)
            return None
        if current.team_key != binding.linear_team_key:
            log.info(
                "skipping %s: team changed from %s to %s before dispatch",
                issue.identifier,
                binding.linear_team_key,
                current.team_key,
            )
            return None
        if current.state_name != binding.linear_states.ready:
            log.info(
                "skipping %s: state changed from %s to %s before dispatch",
                issue.identifier,
                binding.linear_states.ready,
                current.state_name,
            )
            return None
        if binding.issue_label and binding.issue_label not in current.labels:
            log.info(
                "skipping %s: label %r removed before dispatch",
                issue.identifier,
                binding.issue_label,
            )
            return None
        return current

    def _dispatch_task_done(
        self, task: asyncio.Task[None], issue_id: str, binding_key: BindingKey
    ) -> None:
        self._dispatch_tasks.discard(task)
        self._release_scheduled_slot(issue_id=issue_id, binding_key=binding_key)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("dispatch task crashed for issue_id=%s", issue_id)
