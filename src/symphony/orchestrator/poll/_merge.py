"""`_MergeMixin` — the merge domain of the poll loop (SYM-147).

Owns merge-candidate polling, merge execution + its fix-runs (conflict rebase,
required-check), manual-merge park/revival, the auto-recoverable merge-wait
reconciler, and the merged/closed-unmerged Linear-state reconcilers. The
merge-exclusive free functions are co-located here too. `Orchestrator`
(in `__init__.py`) inherits this class.

Pure structural extraction: method bodies are byte-for-byte unchanged from the
pre-split `Orchestrator`.

The shared module-level constants/helpers/logger that merge leans on stay in
`__init__.py` and are imported here via `from . import ...`; `__init__` imports
`_MergeMixin` only after defining them, so the (partial) package import
resolves. Cross-mixin `self.<method>` calls (into domains that still live on
`Orchestrator`) cannot be seen by a standalone `_MergeMixin`, so they resolve to
`Any`. `attr-defined` (the call itself) and `no-any-return` (a method that
returns such a call's result) are disabled file-wide for that reason; every
other strict check stays on.
"""

# mypy: disable-error-code="attr-defined, no-any-return"

from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from collections.abc import (
    Awaitable,
    Callable,
)
from dataclasses import replace
from datetime import (
    timedelta,
)
from functools import partial
from pathlib import Path
from typing import Any

from ... import db
from ...agent.prompt import (
    merge_conflict_fix_prompt,
    merge_conflict_rebase_fix_prompt,
    merge_prompt,
    merge_required_check_fix_prompt,
)
from ...config import RepoBinding
from ...github.branch_protection import get_required_contexts
from ...github.client import (
    CheckRun as GitHubCheckRun,
)
from ...github.client import (
    GitHubError,
    _is_merge_conflict_error,
)
from ...github.webhook import GitHubWebhookEvent
from ...linear.client import LinearError
from ...linear.slash import (
    SlashIntent,
    SlashKind,
)
from ...linear.templates import (
    CommentVars,
    awaiting_approval,
    codex_lgtm,
    fix_pushed,
    fixing_merge_conflict,
    resumed,
    stage_done,
    truncate_body,
)
from ...pipeline.cost_guard import UsageDelta
from ...pipeline.local_review_loop import (
    LoopOutcome,
    LoopResult,
)
from ...pipeline.review_classifier import (
    CheckRun as ReviewCheckRun,
)
from ...pipeline.review_classifier import (
    ReviewSnapshot,
    Verdict,
    VerdictKind,
    has_hit_iteration_cap,
    is_codex_author,
    review_classifier,
    should_dispatch_fix_run,
)
from ...pipeline.state_machine import on_runner_event
from ...tracker import Issue as LinearIssue
from ._base import (
    MERGE_WAIT_RECONCILE_INTERVAL_SECS,
    MERGED_LINEAR_STATE_RECONCILE_LOOKBACK_HOURS,
    ORPHANED_MERGE_RUN_GRACE_SECS,
    PARKED_CLOSED_UNMERGED_COMMENT,
    SlashHandlerFailure,
    _binding_key,
    _OrchestratorBase,
)
from ._git import (
    _git_abort_rebase,
    _git_add_and_continue_rebase,
    _git_conflicted_files,
    _git_fetch,
    _git_fetch_branch,
    _git_rebase,
    _git_status_short,
    _sync_workspace_to_remote,
    _workspace_head_sha,
    _workspace_ref_sha,
)
from ._helpers import (
    NEEDS_HUMAN_APPROVAL_LABEL,
    _add_run_usage,
    _github_commit_url,
    _local_review_termination_reason,
    _needs_human_approval_label_present,
    _no_signal_head_check_state,
    _parse_rfc3339,
    _pr_base_ref_from_view,
    _pr_url_for_state,
    _pr_view_has_merge_conflict,
    _pr_view_is_clean_mergeable,
    _pr_view_is_closed,
    _pr_view_is_merged,
    _pr_view_skips_required_check_fix,
    _required_check_detail,
    _required_check_trigger_signature,
    _status_check_failed,
    _status_check_identity,
    _status_check_names,
    _status_check_sha,
    _status_rollup_nodes,
    _sum_usage,
    _termination_kwargs,
    build_fix_runner_command,
    build_merge_runner_command,
)
from ._review import (
    CODEX_NO_ISSUES_MARKER,
    _codex_lgtm_reactions_from_issue_comments,
    _local_review_infra_failed,
    _local_review_needs_approval,
    _reactions_from_github,
    _read_run_stream_api_error_obj,
    _review_comments_from_github,
    _reviews_from_github,
)

log = logging.getLogger(__name__)



async def _abort_rebase_safely(
    workspace_path: Path, *, issue_identifier: str, reason: str
) -> None:
    try:
        await _git_abort_rebase(workspace_path)
    except Exception as e:  # noqa: BLE001
        log.warning(
            "could not abort rebase after %s for %s: %s",
            reason,
            issue_identifier,
            e,
        )



def _merge_issue_matches_binding(issue: LinearIssue, binding: RepoBinding) -> bool:
    active_states = {
        binding.linear_states.in_progress,
        binding.linear_states.needs_approval,
    }
    if binding.resolved_local_review() and binding.linear_states.local_code_review:
        active_states.add(binding.linear_states.local_code_review)
    if binding.resolved_remote_review() and binding.linear_states.code_review:
        active_states.add(binding.linear_states.code_review)
    if binding.acceptance.mode != "off":
        active_states.add(binding.linear_states.in_acceptance)
    return (
        issue.team_key == binding.linear_team_key
        and issue.state_name in active_states
        and (binding.issue_label is None or binding.issue_label in issue.labels)
    )



def _review_check_from_github(run: GitHubCheckRun) -> ReviewCheckRun:
    if run.bucket in ("pass", "skipping"):
        return ReviewCheckRun(
            name=run.name,
            status="completed",
            conclusion="success",
            required=True,
        )
    if run.bucket == "cancel":
        return ReviewCheckRun(
            name=run.name,
            status="completed",
            conclusion="cancelled",
            required=True,
        )
    if run.bucket == "fail":
        return ReviewCheckRun(
            name=run.name,
            status="completed",
            conclusion="failure",
            required=True,
        )
    return ReviewCheckRun(
        name=run.name,
        status="in_progress",
        conclusion=None,
        required=True,
    )


class _MergeMixin(_OrchestratorBase):
    """Owns the poll loop's merge domain; `Orchestrator` extends it."""

    _CODEX_NO_ISSUES_MARKER = CODEX_NO_ISSUES_MARKER


    async def _run_auto_recoverable_merge_wait_reconciler(
        self, shutdown: asyncio.Event
    ) -> None:
        log.info(
            "auto-recoverable merge wait reconciler entering loop (interval=%ds)",
            MERGE_WAIT_RECONCILE_INTERVAL_SECS,
        )
        while not shutdown.is_set():
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=MERGE_WAIT_RECONCILE_INTERVAL_SECS
                )
                break
            except TimeoutError:
                pass
            try:
                await self._reconcile_orphaned_merge_runs(reason="periodic")
            except Exception:  # noqa: BLE001
                log.exception("orphaned merge run reconcile failed")
            try:
                recovered = await self._reconcile_auto_recoverable_merge_waits(
                    reason="periodic"
                )
            except Exception:  # noqa: BLE001
                log.exception("auto-recoverable merge wait reconcile failed")
                continue
            if recovered:
                log.info(
                    "auto-recoverable merge wait reconcile dispatched %d recovery run(s)",
                    recovered,
                )


    async def _reconcile_orphaned_merge_runs(self, *, reason: str = "manual") -> int:
        """Retire zombie merge `needs_approval` runs whose operator wait is gone.

        When a merge wait is cleared without superseding its `needs_approval`
        run (e.g. a revival retry that a host restart then orphaned), the run
        lingers and `list_merge_candidates` keeps the still-open PR out of merge
        polling forever — and the dead run shows the issue as Halted. Retiring
        the run re-opens candidacy so the normal merge poll re-engages.
        """
        before = (
            self._now() - timedelta(seconds=ORPHANED_MERGE_RUN_GRACE_SECS)
        ).isoformat()
        issue_ids = await db.runs.supersede_orphaned_merge_needs_approval(
            self._conn, before=before
        )
        if issue_ids:
            log.info(
                "reconcile(%s): retired %d orphaned merge needs_approval run(s), "
                "re-opening merge candidacy for %s",
                reason,
                len(issue_ids),
                ", ".join(sorted(set(issue_ids))),
            )
            self._wake.set()
        return len(issue_ids)


    async def _reconcile_auto_recoverable_merge_waits(
        self, *, reason: str = "manual"
    ) -> int:
        """Re-drive stale merge waits whose current PR state is now auto-recoverable."""
        dispatched = 0
        repo_view_cache: dict[str, dict[str, object] | None] = {}
        waits = await db.operator_waits.list_all(self._conn)
        for wait in waits:
            if wait.kind != db.operator_waits.KIND_MERGE:
                continue
            try:
                if await self._reconcile_auto_recoverable_merge_wait(
                    wait,
                    reason=reason,
                    repo_view_cache=repo_view_cache,
                ):
                    dispatched += 1
            except Exception:  # noqa: BLE001
                log.exception(
                    "auto-recoverable merge wait reconcile failed for issue %s",
                    wait.issue_id,
                )
        return dispatched


    async def _reconcile_auto_recoverable_merge_wait(
        self,
        wait: db.operator_waits.OperatorWait,
        *,
        reason: str,
        repo_view_cache: dict[str, dict[str, object] | None],
    ) -> bool:
        binding = self._binding_for_operator_wait(wait)
        if binding is None:
            log.warning(
                "cannot reconcile merge wait for issue %s: no binding for %s/%s label=%r",
                wait.issue_id,
                wait.linear_team_key,
                wait.github_repo,
                wait.issue_label,
            )
            return False

        pr = await db.issue_prs.get(
            self._conn,
            issue_id=wait.issue_id,
            github_repo=binding.github_repo,
        )
        if pr is None or pr.merged_at is not None:
            return False

        tracker_issue_id, _ = await self._tracker_identity_for_issue(wait.issue_id)
        tracker = self.tracker(binding)
        try:
            issue = await tracker.lookup_issue(tracker_issue_id)
        except LinearError as e:
            log.warning(
                "could not look up %s before merge wait reconcile: %s",
                wait.issue_id,
                e,
            )
            return False
        if not _merge_issue_matches_binding(issue, binding):
            log.info(
                "skipping merge wait reconcile for %s: issue is no longer active "
                "for binding %s/%s",
                issue.identifier,
                binding.github_repo,
                binding.issue_label or "",
            )
            return False

        try:
            view = await self._gh.pr_view(pr.pr_number, repo=binding.github_repo)
        except GitHubError as e:
            log.warning(
                "could not view PR for merge wait reconcile %s#%d: %s",
                binding.github_repo,
                pr.pr_number,
                e,
            )
            return False

        await self._repo_view_for_merge_wait_reconcile(
            binding.github_repo,
            repo_view_cache,
        )

        classifier: str | None = None
        if _pr_view_has_merge_conflict(view):
            classifier = "merge-conflict rebase fix-run"
        elif _pr_view_is_clean_mergeable(view):
            classifier = "clean merge retry"
        if classifier is None:
            return False

        approved_head_sha = str(view.get("headRefOid") or "")
        if classifier == "clean merge retry":
            try:
                verdict = await self._review_verdict_for_pr(
                    binding=binding,
                    pr_number=pr.pr_number,
                    view=view,
                )
            except GitHubError as e:
                log.warning(
                    "could not classify review before clean merge wait reconcile "
                    "%s#%d: %s",
                    binding.github_repo,
                    pr.pr_number,
                    e,
                )
                return False
            if verdict.kind is not VerdictKind.APPROVED:
                log.info(
                    "skipping clean merge wait reconcile for %s#%d: current HEAD "
                    "%s is not approved (%s)",
                    binding.github_repo,
                    pr.pr_number,
                    approved_head_sha[:12] or "(unknown)",
                    verdict.rule or verdict.kind.value,
                )
                return False

        async with self._schedule_lock:
            current_wait = await db.operator_waits.get(self._conn, wait.issue_id)
            if current_wait != wait:
                return False
            if wait.issue_id in self._scheduled_issue_ids:
                return False
            if wait.issue_id in self._merge_wait_reconcile_issue_ids:
                return False
            if await db.runs.has_active(
                self._conn,
                wait.issue_id,
                ignored_stage="review",
            ):
                return False
            await self._complete_review_monitors_for_merge(issue)
            if await db.runs.has_running_or_completed(self._conn, wait.issue_id):
                return False

            body = (
                "♻️ Reconciling stuck merge wait: applying "
                f"{classifier} auto-recovery (no `$approve` needed)."
            )
            try:
                await tracker.post_comment(tracker_issue_id, truncate_body(body))
            except LinearError as e:
                log.warning(
                    "could not post merge wait reconcile comment for %s: %s",
                    issue.identifier,
                    e,
                )

            if classifier == "merge-conflict rebase fix-run":
                self._schedule_reconciled_merge_conflict_rebase_fix(
                    binding=binding,
                    issue=issue,
                    pr_number=pr.pr_number,
                    pr_url=pr.pr_url,
                    view=view,
                    wait_run_id=wait.run_id,
                )
            else:
                async def clear_reconciled_merge_wait(_new_run_id: str) -> None:
                    await self._clear_operator_wait(wait.issue_id, wait.run_id)

                self._schedule_merge(
                    binding=binding,
                    issue=issue,
                    pr_number=pr.pr_number,
                    pr_url=pr.pr_url,
                    approved_head_sha=approved_head_sha,
                    on_started=clear_reconciled_merge_wait,
                )
            log.info(
                "reconciled merge wait for %s via %s (reason=%s)",
                issue.identifier,
                classifier,
                reason,
            )
            return True


    async def _repo_view_for_merge_wait_reconcile(
        self,
        repo: str,
        cache: dict[str, dict[str, object] | None],
    ) -> dict[str, object] | None:
        if repo in cache:
            return cache[repo]
        repo_view = getattr(self._gh, "repo_view", None)
        if repo_view is None:
            cache[repo] = None
            return None
        try:
            result = repo_view(repo)
            if inspect.isawaitable(result):
                result = await result
        except Exception as e:  # noqa: BLE001
            log.debug("repo view failed during merge wait reconcile for %s: %s", repo, e)
            cache[repo] = None
            return None
        if isinstance(result, dict):
            cache[repo] = result
            return result
        cache[repo] = None
        return None


    def _schedule_reconciled_merge_conflict_rebase_fix(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        view: dict[str, object],
        wait_run_id: str,
    ) -> asyncio.Task[None]:
        self._merge_wait_reconcile_issue_ids.add(issue.id)

        async def dispatch_conflict_fix() -> None:
            recovered = await self._dispatch_merge_conflict_rebase_fix_run(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                view=view,
                merge_run_id=wait_run_id,
            )
            if recovered:
                await self._clear_operator_wait(issue.id, wait_run_id)

        task = asyncio.create_task(dispatch_conflict_fix())
        self._dispatch_tasks.add(task)
        task.add_done_callback(
            partial(
                self._merge_wait_reconcile_task_done,
                issue_id=issue.id,
            )
        )
        return task


    def _merge_wait_reconcile_task_done(
        self, task: asyncio.Task[None], *, issue_id: str
    ) -> None:
        self._dispatch_tasks.discard(task)
        self._merge_wait_reconcile_issue_ids.discard(issue_id)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("merge wait recovery task crashed for issue_id=%s", issue_id)


    async def _complete_review_monitors_for_merge(self, issue: LinearIssue) -> None:
        """Retire review polling once a merge run owns the issue."""
        live_review_runs = [
            run
            for run in await db.runs.list_live_by_stage(self._conn, stage="review")
            if run.issue_id == issue.id
        ]
        if not live_review_runs:
            return

        now = self._now().isoformat()
        closed_run_ids: set[str] = set()
        for run in live_review_runs:
            await db.runs.update_status(
                self._conn,
                run.id,
                "completed",
                ended_at=now,
            )
            closed_run_ids.add(run.id)
            self._clear_review_no_signal_rearm_heads(run.id)
            task = self._review_poll_run_tasks.pop(run.id, None)
            if task is not None:
                self._review_poll_tasks.discard(task)
                if not task.done():
                    task.cancel()
            self._review_poll_run_ids.discard(run.id)
            await self._clear_review_rearm_retry(run.id)

        for mapped_issue_id, mapped_run_id in list(self._review_poll_issue_ids.items()):
            if mapped_issue_id == issue.id or mapped_run_id in closed_run_ids:
                self._review_poll_issue_ids.pop(mapped_issue_id, None)

        log.info(
            "completed review monitor(s) %s for %s before merge",
            ", ".join(sorted(closed_run_ids)),
            issue.identifier,
        )


    async def _interrupt_stale_merge_needs_approval_for_state(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        state: db.review_state.ReviewState,
    ) -> int:
        if state.pr_number is None:
            return 0
        interrupted = await db.runs.interrupt_stale_merge_needs_approval(
            self._conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            pr_number=state.pr_number,
        )
        if interrupted:
            log.info(
                "interrupted %d stale merge needs_approval runs for %s#%d",
                interrupted,
                binding.github_repo,
                state.pr_number,
            )
        return interrupted


    async def _resolve_pr_base_ref(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        view: dict[str, object] | None,
    ) -> str:
        if view is not None:
            base_ref = _pr_base_ref_from_view(view)
            if base_ref is not None:
                return base_ref
        if binding.base_branch is not None:
            return binding.base_branch
        try:
            result_obj: object = self._gh.repo_default_branch(binding.github_repo)
            if inspect.isawaitable(result_obj):
                result_obj = await result_obj
            if isinstance(result_obj, str) and result_obj.strip():
                return result_obj.strip()
        except (GitHubError, AttributeError, TypeError) as e:
            log.warning(
                "repo_default_branch failed for merge-conflict fix-run %s; "
                "falling back to 'main': %s",
                issue.identifier,
                e,
            )
        return "main"


    async def _required_check_failures_for_view(
        self,
        *,
        binding: RepoBinding,
        pr_number: int,
        view: dict[str, object],
        required_context_cache: dict[tuple[str, str], tuple[str, ...]],
    ) -> list[dict[str, object]]:
        if _pr_view_skips_required_check_fix(view):
            return []
        head_sha = str(view.get("headRefOid") or "")
        failing_rollup_checks: list[dict[str, object]] = []
        for check in _status_rollup_nodes(view.get("statusCheckRollup")):
            check_sha = _status_check_sha(check)
            if check_sha and head_sha and check_sha != head_sha:
                continue
            if _status_check_failed(check):
                failing_rollup_checks.append(_required_check_detail(check))
        if not failing_rollup_checks:
            return []

        try:
            required_contexts = await get_required_contexts(
                binding.github_repo,
                pr_number,
                gh=self._gh,
                cache=required_context_cache,
            )
        except GitHubError as e:
            log.warning(
                "could not fetch required status contexts for %s#%d: %s",
                binding.github_repo,
                pr_number,
                e,
            )
            return []
        required = {context.strip() for context in required_contexts if context.strip()}
        if not required:
            return []
        return [
            check
            for check in failing_rollup_checks
            if _status_check_names(check) & required
        ]


    async def _merge_required_check_fix_should_dispatch(
        self,
        *,
        issue_id: str,
        head_sha: str,
        failing_checks: list[dict[str, object]],
    ) -> bool:
        signature = _required_check_trigger_signature(
            head_sha=head_sha,
            failing_checks=failing_checks,
        )
        state = await db.review_state.get(self._conn, issue_id)
        return should_dispatch_fix_run(
            prev_signature=state.last_trigger_signature,
            new_signature=signature,
        )


    async def _merge_required_check_action_log_tail(
        self,
        *,
        repo: str,
        failing_checks: list[dict[str, object]],
    ) -> str:
        sections: list[str] = []
        for check in failing_checks:
            if str(check.get("__typename") or "") != "CheckRun":
                continue
            run_id = str(check.get("runId") or "").strip()
            name = _status_check_identity(check)
            try:
                if run_id:
                    tail = await self._gh.run_failed_log_tail(run_id, repo=repo)
                else:
                    link = str(check.get("detailsUrl") or check.get("targetUrl") or "")
                    tail = await self._gh.check_log_tail(
                        GitHubCheckRun(
                            name=name,
                            state=str(check.get("state") or check.get("conclusion") or ""),
                            bucket="fail",
                            link=link or None,
                        ),
                        repo=repo,
                    )
            except GitHubError as e:
                log.warning(
                    "could not fetch failed log for required check %s in %s: %s",
                    name,
                    repo,
                    e,
                )
                continue
            if tail.strip():
                sections.append(f"## {name}\n{tail.strip()}")
        return "\n\n".join(sections)


    async def _mark_merge_required_check_fix_needs_approval(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
        reason: str,
        merge_run_id: str | None,
    ) -> None:
        await self._mark_merge_needs_approval(
            binding=binding,
            issue=issue,
            pr_url=pr_url,
            run_id=merge_run_id or str(uuid.uuid4()),
            reason=reason,
            create_run=merge_run_id is None,
        )


    async def _merge_required_check_terminal_run(
        self, *, issue: LinearIssue, merge_run_id: str | None
    ) -> db.runs.Run:
        run_id = merge_run_id or str(uuid.uuid4())
        started_at = self._now().isoformat()
        if merge_run_id is None:
            await db.runs.create(
                self._conn,
                id=run_id,
                issue_id=issue.id,
                stage="merge",
                status="running",
                pid=None,
                started_at=started_at,
            )
        return db.runs.Run(
            id=run_id,
            issue_id=issue.id,
            stage="merge",
            status="running",
            pid=None,
            started_at=started_at,
            ended_at=None,
            cost_usd=0.0,
        )


    async def _dispatch_merge_required_check_fix_if_allowed(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        head_sha: str,
        failing_checks: list[dict[str, object]],
        merge_error: str,
        merge_run_id: str | None = None,
        dispatch_capacity_held: bool = False,
    ) -> bool:
        signature = _required_check_trigger_signature(
            head_sha=head_sha,
            failing_checks=failing_checks,
        )
        state = await db.review_state.get(self._conn, issue.id)
        if not should_dispatch_fix_run(
            prev_signature=state.last_trigger_signature,
            new_signature=signature,
        ):
            return False
        # A prior review-fix run that hit a transient API error stays in the
        # review state (not moved to Ready). Guard here so the merge loop does
        # not immediately re-dispatch before the backoff window elapses.
        if await self._agent_infra_retry_backoff_active(issue.id):
            if merge_run_id is not None:
                await self._fail_run(
                    merge_run_id,
                    "required-check fix dispatch deferred: transient retry backoff active",
                )
            return True
        if has_hit_iteration_cap(
            iteration=state.iteration,
            cap=self.config.review_iteration_cap,
        ):
            await self._mark_merge_required_check_fix_needs_approval(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                reason=f"required-check iteration cap reached: {signature}",
                merge_run_id=merge_run_id,
            )
            return False

        # Soft per-issue token-budget gate at the merge-gate fix-run dispatch
        # boundary: park instead of dispatching the next fix. Mirrors the
        # remote-review gate. No fix run exists yet at this boundary, so fall
        # back to a fresh run id when no live merge run is driving us.
        if await self._maybe_park_for_token_budget(
            issue.id, merge_run_id or str(uuid.uuid4()), binding
        ):
            return False

        iteration = state.iteration + 1
        dispatched = await self._dispatch_merge_required_check_fix_run(
            binding=binding,
            issue=issue,
            pr_number=pr_number,
            pr_url=pr_url,
            head_sha=head_sha,
            failing_checks=failing_checks,
            merge_error=merge_error,
            trigger_signature=signature,
            iteration=iteration,
            merge_run_id=merge_run_id,
            dispatch_capacity_held=dispatch_capacity_held,
        )
        if dispatched is True:
            await db.review_state.bump_iteration(self._conn, issue.id)
            await db.review_state.set_signature(self._conn, issue.id, signature)
        return dispatched is not False


    async def _dispatch_merge_required_check_fix_run(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        head_sha: str,
        failing_checks: list[dict[str, object]],
        merge_error: str,
        trigger_signature: str,
        iteration: int,
        merge_run_id: str | None = None,
        dispatch_capacity_held: bool = False,
    ) -> bool | None:
        action_log_tail = await self._merge_required_check_action_log_tail(
            repo=binding.github_repo,
            failing_checks=failing_checks,
        )
        prompt = merge_required_check_fix_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
            pr_number=pr_number,
            head_sha=head_sha,
            merge_error=merge_error,
            failing_checks=failing_checks,
            action_log_tail=action_log_tail,
            trigger_signature=trigger_signature,
            iteration=f"{iteration}/{self.config.review_iteration_cap}",
        )

        async with self._review_fix_dispatch_slot(
            binding,
            issue,
            dispatch_capacity_held=dispatch_capacity_held,
        ):
            prior_total = await db.runs.cost_for_issue(self._conn, issue.id)

            try:
                workspace_path = await self._workspace.acquire(binding, issue)
            except Exception as e:  # noqa: BLE001
                log.exception(
                    "workspace acquire failed for required-check fix-run %s",
                    issue.identifier,
                )
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=f"required-check fix-run failed: workspace acquire failed: {e}",
                    merge_run_id=merge_run_id,
                )
                return False

            branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
            try:
                await _git_fetch_branch(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                self._workspace.release(binding, issue)
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=(
                        "required-check fix-run failed: could not fetch "
                        f"remote HEAD for {branch}: {e}"
                    ),
                    merge_run_id=merge_run_id,
                )
                return False

            start_sha = await _workspace_ref_sha(workspace_path, f"origin/{branch}")
            if not start_sha:
                self._workspace.release(binding, issue)
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=(
                        "required-check fix-run failed: could not read remote "
                        f"HEAD for {branch}"
                    ),
                    merge_run_id=merge_run_id,
                )
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
                ignored_stages=("review", "merge"),
            )
            if not inserted:
                # Lost the race: an existing review_fix is already running.
                # Interrupt the parent merge so it doesn't stay stuck in "running".
                self._workspace.release(binding, issue)
                if merge_run_id is not None:
                    await db.runs.interrupt_running_merge(self._conn, merge_run_id)
                return None
            self._dispatch_run_ids[issue.id] = fix_run_id

            try:
                (
                    usage_delta,
                    final_kind,
                    final_returncode,
                ) = await self._run_required_check_fix_agent(
                    binding=binding,
                    issue=issue,
                    run_id=fix_run_id,
                    workspace_path=workspace_path,
                    prompt=prompt,
                    prior_total=prior_total,
                )
            except Exception as e:  # noqa: BLE001
                log.exception(
                    "required-check fix-run execution failed for %s",
                    issue.identifier,
                )
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        exc=e,
                        reason=f"required-check fix-run failed: {e}",
                    ),
                )
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=f"required-check fix-run failed: {e}",
                    merge_run_id=merge_run_id,
                )
                return False
            finally:
                if self._dispatch_run_ids.get(issue.id) == fix_run_id:
                    self._dispatch_run_ids.pop(issue.id, None)
                self._workspace.release(binding, issue)

            await _add_run_usage(self._conn, fix_run_id, usage_delta)

            transition = on_runner_event(
                stage="review",
                event_kind=final_kind,
                returncode=final_returncode,
            )
            if transition.next_run_status != "completed":
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    transition.next_run_status,
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status=transition.next_run_status,
                        final_kind=final_kind,
                        returncode=final_returncode,
                        reason=f"required-check fix-run ended with {final_kind}",
                    ),
                )
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=f"required-check fix-run ended with {final_kind}",
                    merge_run_id=merge_run_id,
                )
                return False

            pushed_sha = await _workspace_head_sha(workspace_path)
            if not pushed_sha or pushed_sha == start_sha:
                short_sha = (pushed_sha or start_sha)[:12] or "(unknown)"
                reason = (
                    "required-check fix-run completed without advancing "
                    f"{branch}; HEAD stayed at {short_sha}"
                )
                # Before escalating, check for a transient provider API error
                # (exit 0, no HEAD advance). If transient, requeue with backoff;
                # return None so the caller skips signature recording but still
                # treats this as "handled" (no needs_approval escalation).
                log_path = self.config.log_root / f"{fix_run_id}.log"
                api_error = _read_run_stream_api_error_obj(log_path)
                if await self._maybe_requeue_transient_agent_failure(
                    run_id=fix_run_id,
                    binding=binding,
                    issue=issue,
                    storage_issue_id=issue.id,
                    api_error=api_error,
                    reason=reason,
                    termination_kind=db.runs.REVIEW_FIX_TRANSIENT_RETRY_KIND,
                    workspace_path=workspace_path,
                ):
                    if merge_run_id is not None:
                        running_interrupted = await db.runs.interrupt_running_merge(
                            self._conn,
                            merge_run_id,
                        )
                        if running_interrupted:
                            log.info(
                                "interrupted active merge run %s after required-check "
                                "fix-run transient retry",
                                merge_run_id,
                            )
                    return None
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        reason=reason,
                    ),
                )
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=reason,
                    merge_run_id=merge_run_id,
                )
                return False

            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "completed",
                ended_at=self._now().isoformat(),
            )

            local_review_result: LoopResult | None = None
            pending_local_only_needs_approval: LoopResult | None = None
            if binding.resolved_local_review():
                local_review_result = await self._run_local_review_phase(
                    binding=binding,
                    issue=issue,
                    storage_issue_id=issue.id,
                    workspace_path=workspace_path,
                    parent_run_id=fix_run_id,
                )
                if not binding.resolved_remote_review():
                    if _local_review_infra_failed(local_review_result):
                        run = await self._merge_required_check_terminal_run(
                            issue=issue,
                            merge_run_id=merge_run_id,
                        )
                        await self._block_local_only_review_infra_failure(
                            binding=binding,
                            issue=issue,
                            storage_issue_id=issue.id,
                            run_id=run.id,
                            result=local_review_result,
                        )
                        return False
                    assert local_review_result is not None
                    if _local_review_needs_approval(local_review_result):
                        pending_local_only_needs_approval = local_review_result
                    elif local_review_result.outcome != LoopOutcome.APPROVED:
                        await self._mark_merge_required_check_fix_needs_approval(
                            binding=binding,
                            issue=issue,
                            pr_url=pr_url,
                            reason=(
                                "post-required-check local-only review did not "
                                "approve: "
                                f"{_local_review_termination_reason(local_review_result)}"
                            ),
                            merge_run_id=merge_run_id,
                        )
                        return False

            try:
                await self._push_fn(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "git push failed for required-check fix-run %s: %s",
                    issue.identifier,
                    e,
                )
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=f"required-check fix-run push failed: {e}",
                    merge_run_id=merge_run_id,
                )
                return False

            if pending_local_only_needs_approval is not None:
                run = await self._merge_required_check_terminal_run(
                    issue=issue,
                    merge_run_id=merge_run_id,
                )
                await self._park_local_only_review_needs_approval(
                    run=run,
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    result=pending_local_only_needs_approval,
                )
                return True

            state = await db.review_state.get(self._conn, issue.id)
            if state.pr_number is None:
                state = replace(
                    state,
                    pr_number=pr_number,
                    pr_url=pr_url,
                    github_repo=binding.github_repo,
                    issue_label=binding.issue_label or "",
                )
            await self._retrigger_codex_review_unless_approved(
                binding=binding,
                issue=issue,
                state=state,
            )
            await self._interrupt_stale_merge_needs_approval_for_state(
                binding=binding,
                issue=issue,
                state=state,
            )
            if merge_run_id is not None:
                running_interrupted = await db.runs.interrupt_running_merge(
                    self._conn,
                    merge_run_id,
                )
                if running_interrupted:
                    log.info(
                        "interrupted active merge run %s after required-check fix-run",
                        merge_run_id,
                    )
            return True


    async def _run_required_check_fix_agent(
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
        return await self._run_stage_command(
            binding=binding,
            issue=issue,
            command=command,
            run_id=run_id,
            workspace_path=workspace_path,
            stage="review_fix",
            prior_total=prior_total,
        )


    async def _mark_merge_conflict_fix_needs_approval(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
        reason: str,
        merge_run_id: str | None,
    ) -> None:
        await self._mark_merge_needs_approval(
            binding=binding,
            issue=issue,
            pr_url=pr_url,
            run_id=merge_run_id or str(uuid.uuid4()),
            reason=reason,
            create_run=merge_run_id is None,
        )


    async def _dispatch_merge_conflict_rebase_fix_run(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        view: dict[str, object] | None,
        merge_run_id: str | None = None,
        dispatch_capacity_held: bool = False,
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> bool:
        base_ref = await self._resolve_pr_base_ref(
            binding=binding,
            issue=issue,
            view=view,
        )
        async with self._review_fix_dispatch_slot(
            binding,
            issue,
            dispatch_capacity_held=dispatch_capacity_held,
        ):
            prior_total = await db.runs.cost_for_issue(self._conn, issue.id)

            try:
                workspace_path = await self._workspace.acquire(binding, issue)
            except Exception as e:  # noqa: BLE001
                log.exception(
                    "workspace acquire failed for merge-conflict rebase fix-run %s",
                    issue.identifier,
                )
                await self._mark_merge_conflict_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=f"merge-conflict fix-run failed: workspace acquire failed: {e}",
                    merge_run_id=merge_run_id,
                )
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
                ignored_stages=("review", "merge"),
            )
            if not inserted:
                # Lost the race: an existing review_fix is already running.
                # Interrupt the parent merge so it doesn't stay stuck in "running".
                # Return False so callers don't clear operator waits for a fix that
                # was never started (e.g. the reconcile-merge-wait path).
                self._workspace.release(binding, issue)
                if merge_run_id is not None:
                    await db.runs.interrupt_running_merge(self._conn, merge_run_id)
                return False
            self._dispatch_run_ids[issue.id] = fix_run_id
            if on_started is not None:
                try:
                    await on_started(fix_run_id)
                except Exception:  # noqa: BLE001
                    log.exception(
                        "merge-conflict rebase fix-run start callback failed "
                        "for %s run %s",
                        issue.identifier,
                        fix_run_id,
                    )

            prompt = merge_conflict_rebase_fix_prompt(
                issue_title=issue.title,
                issue_body=issue.description,
                labels=list(issue.labels),
                pr_number=pr_number,
                base_ref=base_ref,
            )
            command = build_fix_runner_command(
                binding.agent,
                prompt,
                codex_model=binding.codex_model,
                claude_model=self._fix_claude_model(binding),
                workspace_path=workspace_path,
                mcp_servers=binding.mcp_servers,
            )
            try:
                usage_delta, final_kind, final_returncode = (
                    await self._run_stage_command(
                        binding=binding,
                        issue=issue,
                        command=command,
                        run_id=fix_run_id,
                        workspace_path=workspace_path,
                        stage="review_fix",
                        prior_total=prior_total,
                    )
                )
            except Exception as e:  # noqa: BLE001
                log.exception(
                    "merge-conflict rebase fix-run execution failed for %s",
                    issue.identifier,
                )
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        exc=e,
                        reason=f"merge-conflict fix-run failed: {e}",
                    ),
                )
                await self._mark_merge_conflict_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=f"merge-conflict fix-run failed: {e}",
                    merge_run_id=merge_run_id,
                )
                return False
            finally:
                if self._dispatch_run_ids.get(issue.id) == fix_run_id:
                    self._dispatch_run_ids.pop(issue.id, None)
                self._workspace.release(binding, issue)

            await _add_run_usage(self._conn, fix_run_id, usage_delta)

            transition = on_runner_event(
                stage="review",
                event_kind=final_kind,
                returncode=final_returncode,
            )
            if transition.next_run_status != "completed":
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    transition.next_run_status,
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status=transition.next_run_status,
                        final_kind=final_kind,
                        returncode=final_returncode,
                        reason=f"merge-conflict fix-run failed: runner ended with {final_kind}",
                    ),
                )
                await self._mark_merge_conflict_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=(
                        "merge-conflict fix-run failed: "
                        f"runner ended with {final_kind}"
                    ),
                    merge_run_id=merge_run_id,
                )
                return False

            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "completed",
                ended_at=self._now().isoformat(),
            )
            fixed_head_sha = ""
            try:
                fixed_view = await self._gh.pr_view(pr_number, repo=binding.github_repo)
                fixed_head_sha = str(fixed_view.get("headRefOid") or "")
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "could not refresh PR head after merge-conflict fix-run "
                    "for %s#%d: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
            marked = await db.issue_prs.mark_merge_conflict_fixed(
                self._conn,
                issue_id=issue.id,
                github_repo=binding.github_repo,
                pr_number=pr_number,
                head_sha=fixed_head_sha,
                marked_at=self._now().isoformat(),
            )
            if not marked:
                log.warning(
                    "could not persist merge-conflict fixed marker for %s#%d",
                    binding.github_repo,
                    pr_number,
                )
            interrupted = await db.runs.interrupt_stale_merge_needs_approval(
                self._conn,
                issue_id=issue.id,
                github_repo=binding.github_repo,
                pr_number=pr_number,
            )
            if interrupted:
                log.info(
                    "interrupted %d stale merge needs_approval runs for %s#%d "
                    "after merge-conflict fix-run",
                    interrupted,
                    binding.github_repo,
                    pr_number,
                )
            if merge_run_id is not None:
                running_interrupted = await db.runs.interrupt_running_merge(
                    self._conn,
                    merge_run_id,
                )
                if running_interrupted:
                    log.info(
                        "interrupted active merge run %s after merge-conflict fix-run",
                        merge_run_id,
                    )
            return True


    async def _schedule_parked_manual_merge_revival_for_issue_event(
        self,
        *,
        issue: LinearIssue,
        old_state_id: str | None,
        old_state_name: str | None,
        new_state_id: str | None,
        new_state_name: str | None,
    ) -> asyncio.Task[None] | None:
        candidate = await db.issue_prs.get_for_issue(self._conn, issue_id=issue.id)
        if candidate is None or candidate.parked_at is None:
            return None
        binding = self._binding_for_pr(candidate)
        if binding is None:
            log.warning(
                "no binding for parked manual-merge revive candidate %s in %s",
                candidate.identifier,
                candidate.github_repo,
            )
            return None
        if candidate.issue_id in self._scheduled_issue_ids:
            return None
        if await db.operator_waits.get(self._conn, candidate.issue_id) is not None:
            return None
        if await db.runs.has_active(
            self._conn,
            candidate.issue_id,
            ignored_stage="review",
        ):
            return None
        return await self._schedule_parked_manual_merge_revival_if_requested(
            binding=binding,
            issue=issue,
            candidate=candidate,
            view=None,
            old_state_id=old_state_id,
            old_state_name=old_state_name,
            new_state_id=new_state_id,
            new_state_name=new_state_name,
        )


    async def _schedule_parked_manual_merge_revival_if_requested(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        candidate: db.issue_prs.IssuePR,
        view: dict[str, object] | None,
        old_state_id: str | None = None,
        old_state_name: str | None = None,
        new_state_id: str | None = None,
        new_state_name: str | None = None,
    ) -> asyncio.Task[None] | None:
        if candidate.parked_at is None:
            return None
        if binding.linear_states.code_review == binding.linear_states.needs_approval:
            return None
        if issue.state_name != binding.linear_states.code_review:
            return None
        if not _merge_issue_matches_binding(issue, binding):
            return None
        if issue.id in self._parked_manual_merge_revival_issue_ids:
            return None
        if not await self._parked_manual_merge_transition_matches(
            binding=binding,
            old_state_id=old_state_id,
            old_state_name=old_state_name,
            new_state_id=new_state_id,
            new_state_name=new_state_name,
        ):
            return None
        if view is None:
            try:
                view = await self._gh.pr_view(
                    candidate.pr_number,
                    repo=binding.github_repo,
                    include_status_checks=True,
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "could not view parked manual-merge PR %s#%d before revive: %s",
                    binding.github_repo,
                    candidate.pr_number,
                    e,
                )
                return None
        if await self._finalize_pr_if_closed(
            binding=binding,
            issue=issue,
            pr_number=candidate.pr_number,
            pr_url=candidate.pr_url,
            run_id=str(uuid.uuid4()),
            create_run=True,
            view=view,
        ):
            return None
        async def clear_parked_marker(_run_id: str) -> None:
            await db.issue_prs.clear_parked_for_manual_merge(
                self._conn,
                issue_id=candidate.issue_id,
                github_repo=binding.github_repo,
                pr_number=candidate.pr_number,
            )

        return self._schedule_parked_manual_merge_revival(
            binding=binding,
            issue=issue,
            pr_number=candidate.pr_number,
            pr_url=candidate.pr_url,
            view=view,
            on_started=clear_parked_marker,
        )


    async def _parked_manual_merge_transition_matches(
        self,
        *,
        binding: RepoBinding,
        old_state_id: str | None,
        old_state_name: str | None,
        new_state_id: str | None,
        new_state_name: str | None,
    ) -> bool:
        if old_state_name is not None and old_state_name != (
            binding.linear_states.needs_approval
        ):
            return False
        if new_state_name is not None and new_state_name != (
            binding.linear_states.code_review
        ):
            return False
        if old_state_id is None and new_state_id is None:
            return True
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states before parked manual-merge revive: %s",
                e,
            )
            return False
        needs_approval_id = states.get(binding.linear_states.needs_approval)
        code_review_id = states.get(binding.linear_states.code_review)
        if old_state_id is not None and old_state_id != needs_approval_id:
            return False
        if new_state_id is not None and new_state_id != code_review_id:
            return False
        return True


    def _schedule_parked_manual_merge_revival(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        view: dict[str, object],
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> asyncio.Task[None]:
        self._parked_manual_merge_revival_issue_ids.add(issue.id)

        async def dispatch_conflict_fix() -> None:
            await self._dispatch_merge_conflict_rebase_fix_run(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                view=view,
                on_started=on_started,
            )

        task = asyncio.create_task(dispatch_conflict_fix())
        self._dispatch_tasks.add(task)
        task.add_done_callback(
            partial(
                self._parked_manual_merge_revival_task_done,
                issue_id=issue.id,
            )
        )
        return task


    def _parked_manual_merge_revival_task_done(
        self, task: asyncio.Task[None], *, issue_id: str
    ) -> None:
        self._dispatch_tasks.discard(task)
        self._parked_manual_merge_revival_issue_ids.discard(issue_id)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception(
                "parked manual-merge revival task crashed for issue_id=%s",
                issue_id,
            )


    async def _dispatch_merge_conflict_fix_run(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        iteration: int,
    ) -> bool:
        if await self._review_poll_deferred_by_deliver_failed_wait(
            run.issue_id, run.id
        ):
            return False
        base_branch = binding.base_branch
        if base_branch is None:
            try:
                base_branch = await self._gh.repo_default_branch(binding.github_repo)
            except GitHubError as e:
                log.warning(
                    "repo_default_branch failed for %s; falling back to 'main': %s",
                    issue.identifier,
                    e,
                )
                base_branch = "main"

        state = await db.review_state.get(self._conn, issue.id)
        pr_url = _pr_url_for_state(
            repo=binding.github_repo,
            pr_number=state.pr_number,
            pr_url=state.pr_url,
        )
        tracker = self.tracker(binding)

        async with self._review_fix_dispatch_slot(binding, issue):
            # Post the "fixing" comment once we have a slot.
            v_start = CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=pr_url,
                review_iter=iteration,
            )
            try:
                await tracker.post_comment(
                    issue.id, truncate_body(fixing_merge_conflict(v_start))
                )
            except LinearError as e:
                log.warning(
                    "could not post fixing_merge_conflict comment for %s: %s",
                    issue.identifier,
                    e,
                )

            try:
                workspace_path = await self._workspace.acquire(binding, issue)
            except Exception as e:  # noqa: BLE001
                log.exception(
                    "workspace acquire failed for merge-conflict fix-run %s",
                    issue.identifier,
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"workspace acquire failed: {e}",
                    last_log=str(e),
                )
                return False

            # Step 1: orchestrator fetches origin.
            branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
            try:
                await _sync_workspace_to_remote(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning("workspace sync failed for %s: %s", issue.identifier, e)
                self._workspace.release(binding, issue)
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"workspace sync failed: {e}",
                    last_log=str(e),
                )
                return False

            start_sha = await _workspace_ref_sha(workspace_path, f"origin/{branch}")
            if not start_sha:
                self._workspace.release(binding, issue)
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"could not read review fix-run remote HEAD for {branch}",
                    last_log="",
                    auto_retry=False,
                    operator_wait=True,
                )
                return False

            try:
                await _git_fetch(workspace_path)
            except Exception as e:  # noqa: BLE001
                log.warning("git fetch failed for %s: %s", issue.identifier, e)
                self._workspace.release(binding, issue)
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"git fetch failed: {e}",
                    last_log=str(e),
                )
                return False

            # Step 2: orchestrator attempts the rebase.
            upstream = f"origin/{base_branch or 'main'}"
            try:
                rebase_clean = await _git_rebase(workspace_path, upstream)
            except Exception as e:  # noqa: BLE001
                log.warning("git rebase failed for %s: %s", issue.identifier, e)
                self._workspace.release(binding, issue)
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"git rebase failed: {e}",
                    last_log=str(e),
                )
                return False

            conflicted_files: list[str] = []
            if not rebase_clean:
                conflicted_files = await _git_conflicted_files(workspace_path)
                if not conflicted_files:
                    # Rebase exited non-zero but Git did not leave unresolved
                    # paths. This commonly means a dirty workspace or another
                    # non-content-conflict failure; surface status so operators
                    # can debug the real state instead of seeing a blank error.
                    status_short = await _git_status_short(workspace_path)
                    log.warning(
                        "rebase non-zero but no unresolved paths for %s; git status:\n%s",
                        issue.identifier,
                        status_short or "<clean>",
                    )
                    await _abort_rebase_safely(
                        workspace_path,
                        issue_identifier=issue.identifier,
                        reason="rebase with no unresolved paths",
                    )
                    self._workspace.release(binding, issue)
                    error = "rebase failed with no unresolved paths"
                    if status_short:
                        error += f"; git status: {status_short}"
                    await self._fail_review_run(
                        run=run,
                        binding=binding,
                        issue=issue,
                        error=error,
                        last_log=status_short,
                    )
                    return False

            # Create a review_fix row for cost tracking and dispatch_run_ids cleanup.
            fix_run_id = str(uuid.uuid4())
            inserted = await db.runs.create_if_no_active(
                self._conn,
                id=fix_run_id,
                issue_id=issue.id,
                stage="review_fix",
                status="running",
                pid=None,
                started_at=self._now().isoformat(),
                ignored_stage="review",
            )
            if not inserted:
                # Lost the race against a concurrent fix-run dispatch (SYM-152).
                # Release the workspace and bail without clobbering dispatch ids.
                self._workspace.release(binding, issue)
                return False
            self._dispatch_run_ids[issue.id] = fix_run_id

            try:
                cumulative_usage = UsageDelta()
                if rebase_clean:
                    # No conflicts: skip the agent entirely.
                    log.info(
                        "rebase was clean for %s; skipping agent", issue.identifier
                    )
                while not rebase_clean:
                    # Step 3: dispatch the agent to resolve conflict markers (no git cmds).
                    prompt = merge_conflict_fix_prompt(
                        issue_title=issue.title,
                        issue_body=issue.description,
                        labels=list(issue.labels),
                        base_branch=base_branch or "main",
                        conflicted_files=conflicted_files,
                    )
                    try:
                        prior_total = (
                            await db.runs.cost_for_issue(self._conn, issue.id)
                        ) + cumulative_usage.cost_usd
                        (
                            run_usage,
                            final_kind,
                            final_returncode,
                        ) = await self._run_fix_agent(
                            binding=binding,
                            issue=issue,
                            run_id=fix_run_id,
                            workspace_path=workspace_path,
                            prompt=prompt,
                            prior_total=prior_total,
                        )
                    except Exception as e:  # noqa: BLE001
                        log.exception(
                            "merge-conflict fix-run execution failed for %s",
                            issue.identifier,
                        )
                        await _abort_rebase_safely(
                            workspace_path,
                            issue_identifier=issue.identifier,
                            reason="merge-conflict fix-run execution failure",
                        )
                        await db.runs.update_status(
                            self._conn,
                            fix_run_id,
                            "failed",
                            ended_at=self._now().isoformat(),
                            **_termination_kwargs(
                                status="failed",
                                exc=e,
                                reason=f"merge-conflict fix-run execution failed: {e}",
                            ),
                        )
                        await self._fail_review_run(
                            run=run,
                            binding=binding,
                            issue=issue,
                            error=f"merge-conflict fix-run execution failed: {e}",
                            last_log=str(e),
                        )
                        return False
                    cumulative_usage = _sum_usage(cumulative_usage, run_usage)

                    transition = on_runner_event(
                        stage="review",
                        event_kind=final_kind,
                        returncode=final_returncode,
                    )
                    if transition.next_run_status != "completed":
                        await _abort_rebase_safely(
                            workspace_path,
                            issue_identifier=issue.identifier,
                            reason=f"merge-conflict fix-run {final_kind}",
                        )
                        await db.runs.update_status(
                            self._conn,
                            fix_run_id,
                            transition.next_run_status,
                            ended_at=self._now().isoformat(),
                            **_termination_kwargs(
                                status=transition.next_run_status,
                                final_kind=final_kind,
                                returncode=final_returncode,
                                reason=f"merge-conflict fix-run ended with {final_kind}",
                            ),
                        )
                        await self._fail_review_run(
                            run=run,
                            binding=binding,
                            issue=issue,
                            error=f"merge-conflict fix-run ended with {final_kind}",
                            last_log="",
                        )
                        return False

                    # Step 4: stage resolved files and continue the rebase.
                    try:
                        rebase_clean = await _git_add_and_continue_rebase(
                            workspace_path, conflicted_files
                        )
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "rebase --continue failed for %s: %s", issue.identifier, e
                        )
                        await _abort_rebase_safely(
                            workspace_path,
                            issue_identifier=issue.identifier,
                            reason="rebase --continue failure",
                        )
                        await db.runs.update_status(
                            self._conn,
                            fix_run_id,
                            "failed",
                            ended_at=self._now().isoformat(),
                            **_termination_kwargs(
                                status="failed",
                                exc=e,
                                reason=f"rebase --continue failed: {e}",
                            ),
                        )
                        await self._fail_review_run(
                            run=run,
                            binding=binding,
                            issue=issue,
                            error=f"rebase --continue failed: {e}",
                            last_log=str(e),
                        )
                        return False
                    if not rebase_clean:
                        conflicted_files = await _git_conflicted_files(workspace_path)
                        if not conflicted_files:
                            status_short = await _git_status_short(workspace_path)
                            log.warning(
                                "rebase --continue non-zero but no unresolved paths "
                                "for %s; git status:\n%s",
                                issue.identifier,
                                status_short or "<clean>",
                            )
                            await _abort_rebase_safely(
                                workspace_path,
                                issue_identifier=issue.identifier,
                                reason="rebase --continue with no unresolved paths",
                            )
                            error = "rebase --continue failed with no unresolved paths"
                            if status_short:
                                error += f"; git status: {status_short}"
                            await db.runs.update_status(
                                self._conn,
                                fix_run_id,
                                "failed",
                                ended_at=self._now().isoformat(),
                                **_termination_kwargs(
                                    status="failed",
                                    reason=error,
                                ),
                            )
                            await self._fail_review_run(
                                run=run,
                                binding=binding,
                                issue=issue,
                                error=error,
                                last_log=status_short,
                            )
                            return False

            finally:
                if self._dispatch_run_ids.get(issue.id) == fix_run_id:
                    self._dispatch_run_ids.pop(issue.id, None)
                self._workspace.release(binding, issue)

            await _add_run_usage(self._conn, fix_run_id, cumulative_usage)

            pushed_sha = await self._validate_review_fix_advanced(
                run=run,
                fix_run_id=fix_run_id,
                binding=binding,
                issue=issue,
                workspace_path=workspace_path,
                branch=branch,
                start_sha=start_sha,
            )
            if not pushed_sha:
                return False

            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "completed",
                ended_at=self._now().isoformat(),
            )

            # Step 5: force-push the rebased branch.
            try:
                await self._force_push_fn(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "force-push failed for merge-conflict fix-run %s: %s",
                    issue.identifier,
                    e,
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"force-push failed: {e}",
                    last_log=str(e),
                )
                return False

            tokens = await db.runs.tokens_for_issue(self._conn, issue.id)
            v_done = CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=pr_url,
                review_iter=iteration,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                commit_url=_github_commit_url(binding.github_repo, pushed_sha),
            )
            try:
                await tracker.post_comment(
                    issue.id, truncate_body(fix_pushed(v_done))
                )
            except LinearError as e:
                log.warning(
                    "could not post fix_pushed comment for %s: %s", issue.identifier, e
                )

            state = await db.review_state.get(self._conn, issue.id)
            await self._retrigger_codex_review_unless_approved(
                binding=binding,
                issue=issue,
                state=state,
            )
            await self._interrupt_stale_merge_needs_approval_for_state(
                binding=binding,
                issue=issue,
                state=state,
            )
            return True


    async def _handle_merge_needs_approval_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        """Handle `$approve`/`$reject`/`$stop` on a merge `needs_approval` run."""
        binding = self._merge_needs_approval_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(db.operator_waits.KIND_MERGE,),
            )
            if binding is None:
                return
        tracker = self.tracker(binding)
        if intent.kind is SlashKind.APPROVE:
            parked_pr = await db.issue_prs.get(
                self._conn,
                issue_id=issue_id,
                github_repo=binding.github_repo,
            )
            if (
                parked_pr is not None
                and parked_pr.merged_at is None
                and parked_pr.parked_at is not None
            ):
                await self._handle_parked_manual_merge_slash_intent(
                    issue_id,
                    intent,
                    binding=binding,
                    pr=parked_pr,
                )
                return
        if intent.kind in (SlashKind.REJECT, SlashKind.STOP):
            states = await self._states_for_binding(binding)
            blocked_id = states.get(binding.linear_states.blocked)
            try:
                issue = await tracker.lookup_issue(issue_id)
            except LinearError as e:
                log.warning("could not look up %s for merge reject: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not look up issue for merge reject: {e}",
                ) from e
            if blocked_id is not None:
                try:
                    await tracker.move_issue(issue_id, blocked_id)
                except LinearError as e:
                    log.warning(
                        "could not move %s to blocked after merge reject: %s",
                        issue.identifier,
                        e,
                    )
                    raise SlashHandlerFailure(
                        slash_text=self._slash_text(intent),
                        reason=f"could not move issue to blocked state: {e}",
                    ) from e
            await self._clear_operator_wait(issue_id, run_id)
            return
        if intent.kind not in (SlashKind.APPROVE, SlashKind.RETRY):
            log.info("slash %s for merge-needs-approval run %s ignored", intent.kind, run_id)
            return

        # $approve or $retry: re-dispatch the merge.
        try:
            issue = await tracker.lookup_issue(issue_id)
        except LinearError as e:
            log.warning("could not look up %s for merge re-dispatch: %s", issue_id, e)
            raise SlashHandlerFailure(
                slash_text=self._slash_text(intent),
                reason=f"could not look up issue for merge re-dispatch: {e}",
            ) from e
        state = await db.review_state.get(self._conn, issue_id)
        if state.pr_number is None:
            log.warning("merge re-dispatch for %s: no PR number in review_state", issue_id)
            return
        pr_number = state.pr_number
        pr_url = state.pr_url or (
            f"https://github.com/{binding.github_repo}/pull/{pr_number}"
        )
        log.info(
            "merge re-dispatch: scheduling merge for %s (PR #%d)",
            issue.identifier,
            pr_number,
        )

        async def on_merge_started(new_run_id: str) -> None:
            await self._clear_operator_wait(issue_id, run_id)
            try:
                await tracker.post_comment(
                    issue_id,
                    truncate_body(
                        resumed(
                            CommentVars(
                                stage="merge",
                                repo=binding.github_repo,
                                issue=pr_number,
                                pr_url=pr_url,
                                run_id=new_run_id,
                                next_stage="merge",
                            )
                        )
                    ),
                )
            except LinearError as e:
                log.warning(
                    "merge re-dispatch comment failed for %s: %s",
                    issue.identifier,
                    e,
                )

        self._schedule_merge(
            binding=binding,
            issue=issue,
            pr_number=pr_number,
            pr_url=pr_url,
            on_started=on_merge_started,
        )


    async def _parked_closed_unmerged_pr_for_event(
        self, event: GitHubWebhookEvent
    ) -> db.issue_prs.IssuePR | None:
        if (
            event.event_type != "pull_request"
            or event.action.casefold() != "closed"
            or event.pr_number is None
            or event.merged
        ):
            return None
        cur = await self._conn.execute(
            """
            SELECT p.issue_id, i.identifier, i.title, i.team_key, p.github_repo,
                   p.binding_key, p.pr_number, p.pr_url, p.created_at,
                   p.merged_at, p.parked_at
            FROM issue_prs p
            JOIN issues i ON i.id = p.issue_id
            WHERE lower(p.github_repo) = lower(?)
              AND p.pr_number = ?
              AND p.merged_at IS NULL
              AND p.parked_at IS NOT NULL
            ORDER BY p.created_at DESC
            LIMIT 1
            """,
            (event.repo, event.pr_number),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return db.issue_prs.IssuePR(
            issue_id=str(row["issue_id"]),
            identifier=str(row["identifier"]),
            title=str(row["title"]),
            team_key=str(row["team_key"]),
            github_repo=str(row["github_repo"]),
            binding_key=str(row["binding_key"] or ""),
            pr_number=int(row["pr_number"]),
            pr_url=str(row["pr_url"]),
            created_at=str(row["created_at"]),
            merged_at=(
                str(row["merged_at"]) if row["merged_at"] is not None else None
            ),
            parked_at=(
                str(row["parked_at"]) if row["parked_at"] is not None else None
            ),
        )


    async def _reconcile_parked_closed_unmerged_pr_event(
        self, event: GitHubWebhookEvent
    ) -> int:
        pr = await self._parked_closed_unmerged_pr_for_event(event)
        if pr is None:
            return 0
        binding = self._binding_for_pr(pr)
        if binding is None or binding.auto_merge:
            return 0
        try:
            view = await self._gh.pr_view(pr.pr_number, repo=binding.github_repo)
        except GitHubError as e:
            log.warning(
                "could not verify parked closed-unmerged PR state for %s#%d: %s",
                binding.github_repo,
                pr.pr_number,
                e,
            )
            return 0
        if _pr_view_is_merged(view) or not _pr_view_is_closed(view):
            return 0
        tracker = self.tracker(binding)
        try:
            issue = await tracker.lookup_issue(pr.issue_id)
        except LinearError as e:
            log.warning(
                "could not refresh parked closed-unmerged issue %s: %s",
                pr.identifier,
                e,
            )
            return 0
        if issue.team_key != binding.linear_team_key:
            return 0
        if binding.issue_label is not None and binding.issue_label not in issue.labels:
            return 0
        if await self._mark_parked_closed_unmerged_pr_done(
            binding=binding,
            issue=issue,
            pr=pr,
        ):
            return 1
        return 0


    async def _mark_parked_closed_unmerged_pr_done(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr: db.issue_prs.IssuePR,
    ) -> bool:
        async with self._parked_closed_unmerged_lock:
            current = await db.issue_prs.get(
                self._conn,
                issue_id=issue.id,
                github_repo=binding.github_repo,
            )
            if (
                current is None
                or current.pr_number != pr.pr_number
                or current.parked_at is None
            ):
                return True
            pr = current

            try:
                states = await self._states_for_binding(binding)
            except LinearError as e:
                log.warning(
                    "could not load states while closing parked PR %s#%d: %s",
                    binding.github_repo,
                    pr.pr_number,
                    e,
                )
                return False
            done_id = states.get(binding.linear_states.done)
            if done_id is None:
                log.warning(
                    "missing Linear done state %r while closing parked PR for %s",
                    binding.linear_states.done,
                    issue.identifier,
                )
                return False

            if issue.state_name != binding.linear_states.done and issue.state_id != done_id:
                tracker = self.tracker(binding)
                try:
                    await tracker.move_issue(issue.id, done_id)
                except LinearError as e:
                    log.warning(
                        "could not move %s to done after parked PR closed: %s",
                        issue.identifier,
                        e,
                    )
                    return False

            comment_key = (issue.id, binding.github_repo, pr.pr_number)
            if comment_key not in self._parked_closed_unmerged_comment_keys:
                tracker = self.tracker(binding)
                try:
                    await tracker.post_comment(
                        issue.id, truncate_body(PARKED_CLOSED_UNMERGED_COMMENT)
                    )
                except LinearError as e:
                    log.warning(
                        "could not post parked closed-unmerged comment for %s: %s",
                        issue.identifier,
                        e,
                    )
                    return False
                self._parked_closed_unmerged_comment_keys.add(comment_key)

            deleted = await db.issue_prs.delete(
                self._conn,
                issue_id=issue.id,
                github_repo=binding.github_repo,
                pr_number=pr.pr_number,
            )
            if not deleted:
                log.warning(
                    "could not delete parked closed-unmerged PR row for %s#%d",
                    binding.github_repo,
                    pr.pr_number,
                )
            return True


    async def _reconcile_merged_issues_linear_state(self) -> int:
        since = self._now() - timedelta(
            hours=MERGED_LINEAR_STATE_RECONCILE_LOOKBACK_HOURS
        )
        recent_merged = await db.issue_prs.list_recent_merged(self._conn, since=since)
        corrected = 0
        for pr in recent_merged:
            binding = self._binding_for_pr(pr)
            if binding is None:
                log.warning(
                    "no binding for merged Linear-state reconcile candidate %s in %s",
                    pr.identifier,
                    pr.github_repo,
                )
                continue
            try:
                states = await self._states_for_binding(binding)
            except LinearError as e:
                log.warning(
                    "could not load states while reconciling merged issue %s: %s",
                    pr.identifier,
                    e,
                )
                continue
            done_id = states.get(binding.linear_states.done)
            if done_id is None:
                log.warning(
                    "missing Linear done state %r while reconciling merged issue %s",
                    binding.linear_states.done,
                    pr.identifier,
                )
                continue
            tracker = self.tracker(binding)
            try:
                issue = await tracker.lookup_issue(pr.issue_id)
            except LinearError as e:
                log.warning(
                    "could not refresh merged issue %s for state reconcile: %s",
                    pr.identifier,
                    e,
                )
                continue
            if issue.state_name == binding.linear_states.done or issue.state_id == done_id:
                continue

            observed_state = issue.state_name or issue.state_id or "unknown"
            try:
                await tracker.move_issue(issue.id, done_id)
            except LinearError as e:
                log.warning(
                    "could not re-move merged issue %s from %s to %s: %s",
                    issue.identifier,
                    observed_state,
                    binding.linear_states.done,
                    e,
                )
                continue
            corrected += 1

            comment_key = (issue.id, observed_state)
            if comment_key in self._merged_linear_state_drift_comment_keys:
                continue
            body = (
                f"♻️ Linear status drifted back to {observed_state} after merge — "
                f"re-moving to {binding.linear_states.done}. PR #{pr.pr_number} "
                f"was merged at {pr.merged_at}."
            )
            try:
                await tracker.post_comment(issue.id, truncate_body(body))
            except LinearError as e:
                log.warning(
                    "could not post merged issue drift correction comment for %s: %s",
                    issue.identifier,
                    e,
                )
                continue
            self._merged_linear_state_drift_comment_keys.add(comment_key)
        return corrected


    async def _refresh_issue_for_acceptance_merge_handoff(
        self, binding: RepoBinding, issue: LinearIssue
    ) -> LinearIssue:
        tracker = self.tracker(binding)
        try:
            return await tracker.lookup_issue(issue.id)
        except LinearError as e:
            log.warning(
                "could not refresh %s labels before acceptance merge handoff: %s",
                issue.identifier,
                e,
            )
            return issue


    async def _open_merge_wait_for_human_approval_label(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
    ) -> None:
        await self._complete_review_monitors_for_merge(issue)
        await self._mark_merge_needs_approval(
            binding=binding,
            issue=issue,
            pr_url=pr_url,
            run_id=str(uuid.uuid4()),
            reason=f"{NEEDS_HUMAN_APPROVAL_LABEL} label present",
            create_run=True,
        )


    async def _park_pr_for_manual_merge(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
    ) -> None:
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states while parking %s for manual merge: %s",
                issue.identifier,
                e,
            )
            return

        needs_approval_id = states.get(binding.linear_states.needs_approval)
        if needs_approval_id is None:
            log.warning(
                "missing Linear needs_approval state %r while parking %s for "
                "manual merge",
                binding.linear_states.needs_approval,
                issue.identifier,
            )
            return

        tracker = self.tracker(binding)
        try:
            await tracker.move_issue(issue.id, needs_approval_id)
        except LinearError as e:
            log.warning(
                "could not move %s to needs approval for manual merge: %s",
                issue.identifier,
                e,
            )
            return

        await self._complete_review_monitors_for_merge(issue)

        parked = await db.issue_prs.mark_parked_for_manual_merge(
            self._conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            pr_number=pr_number,
            parked_at=self._now().isoformat(),
        )
        if not parked:
            return

        body = f"✅ review passed, ready for manual merge: {pr_url}"
        try:
            await tracker.post_comment(issue.id, body)
        except LinearError as e:
            log.warning(
                "could not post manual merge comment for %s: %s",
                issue.identifier,
                e,
            )


    async def _poll_merge_candidates(self) -> list[asyncio.Task[None]]:
        """Advance approved Review PRs into Merge without operator action."""
        scheduled: list[asyncio.Task[None]] = []
        required_context_cache: dict[tuple[str, str], tuple[str, ...]] = {}
        candidates = await db.issue_prs.list_merge_candidates(self._conn)
        for candidate in candidates:
            binding = self._binding_for_pr(candidate)
            if binding is None:
                log.warning(
                    "no binding for merge candidate %s in %s",
                    candidate.identifier,
                    candidate.github_repo,
                )
                continue
            if candidate.issue_id in self._scheduled_issue_ids:
                continue
            if await db.operator_waits.get(self._conn, candidate.issue_id) is not None:
                continue
            if await db.runs.has_active(
                self._conn,
                candidate.issue_id,
                ignored_stage="review",
            ):
                continue
            if await db.operator_waits.get(self._conn, candidate.issue_id) is not None:
                continue
            tracker = self.tracker(binding)
            tracker_issue_id, _ = await self._tracker_identity_for_issue(
                candidate.issue_id
            )
            try:
                issue = await tracker.lookup_issue(tracker_issue_id)
            except LinearError as e:
                log.warning(
                    "could not refresh %s before merge: %s",
                    candidate.identifier,
                    e,
                )
                continue
            parked_done_cleanup = (
                candidate.parked_at is not None
                and not binding.auto_merge
                and issue.team_key == binding.linear_team_key
                and issue.state_name == binding.linear_states.done
                and (
                    binding.issue_label is None or binding.issue_label in issue.labels
                )
            )
            if (
                not _merge_issue_matches_binding(issue, binding)
                and not parked_done_cleanup
            ):
                log.info(
                    "skipping merge candidate %s: issue is no longer active for "
                    "binding %s/%s",
                    issue.identifier,
                    binding.github_repo,
                    binding.issue_label or "",
                )
                continue
            latest_merge = await db.runs.latest_for_issue_stage(
                self._conn,
                issue_id=candidate.issue_id,
                stage="merge",
                started_at_gte=candidate.created_at,
            )
            if latest_merge is not None and latest_merge.status == "completed":
                await self._poll_submitted_merge(
                    binding=binding,
                    issue=issue,
                    pr_number=candidate.pr_number,
                    pr_url=candidate.pr_url,
                    run_id=latest_merge.id,
                )
                continue
            try:
                view = await self._gh.pr_view(
                    candidate.pr_number,
                    repo=binding.github_repo,
                    include_status_checks=True,
                )
                if await self._finalize_pr_if_closed(
                    binding=binding,
                    issue=issue,
                    pr=candidate,
                    pr_number=candidate.pr_number,
                    pr_url=candidate.pr_url,
                    run_id=str(uuid.uuid4()),
                    create_run=True,
                    view=view,
                ):
                    continue
                if candidate.parked_at is not None:
                    revived = (
                        await self._schedule_parked_manual_merge_revival_if_requested(
                            binding=binding,
                            issue=issue,
                            candidate=candidate,
                            view=view,
                        )
                    )
                    if revived is not None:
                        scheduled.append(revived)
                    continue
                if _pr_view_has_merge_conflict(view):
                    await db.issue_prs.clear_merge_conflict_fixed(
                        self._conn,
                        issue_id=candidate.issue_id,
                        github_repo=binding.github_repo,
                        pr_number=candidate.pr_number,
                        pr_created_at=candidate.created_at,
                    )
                    scheduled.append(
                        self._schedule_merge_conflict_rebase_fix(
                            binding=binding,
                            issue=issue,
                            pr_number=candidate.pr_number,
                            pr_url=candidate.pr_url,
                            view=view,
                        )
                    )
                    continue
                required_check_failures = await self._required_check_failures_for_view(
                    binding=binding,
                    pr_number=candidate.pr_number,
                    view=view,
                    required_context_cache=required_context_cache,
                )
                if (
                    required_check_failures
                    and await self._merge_required_check_fix_should_dispatch(
                        issue_id=issue.id,
                        head_sha=str(view.get("headRefOid") or ""),
                        failing_checks=required_check_failures,
                    )
                ):
                    scheduled.append(
                        self._schedule_merge_required_check_fix(
                            binding=binding,
                            issue=issue,
                            pr_number=candidate.pr_number,
                            pr_url=candidate.pr_url,
                            head_sha=str(view.get("headRefOid") or ""),
                            failing_checks=required_check_failures,
                            merge_error="required status check failed before merge",
                        )
                    )
                    continue
            except Exception as e:  # noqa: BLE001 — retry finalization next tick
                log.warning(
                    "could not check finalized PR state for %s#%d: %s",
                    binding.github_repo,
                    candidate.pr_number,
                    e,
                )
                continue
            try:
                verdict = await self._review_verdict_for_pr(
                    binding=binding,
                    pr_number=candidate.pr_number,
                    view=view,
                )
            except GitHubError as e:
                log.warning(
                    "could not classify review for %s#%d: %s",
                    binding.github_repo,
                    candidate.pr_number,
                    e,
                )
                continue

            head_sha = str(view.get("headRefOid") or "")
            no_signal_mergeable = (
                verdict.kind is VerdictKind.PENDING
                and verdict.rule == "no_signal"
                and str(view.get("mergeable") or "").upper() == "MERGEABLE"
            )
            conflict_fix_ready = False
            if no_signal_mergeable:
                conflict_fix_ready = await db.issue_prs.has_merge_conflict_fixed(
                    self._conn,
                    issue_id=candidate.issue_id,
                    github_repo=binding.github_repo,
                    pr_number=candidate.pr_number,
                    pr_created_at=candidate.created_at,
                    head_sha=head_sha,
                )

            # `remote_review: false` is an intentional review bypass: no
            # `@codex` approval will ever land, so once CI and mergeability
            # pass we treat the clean no_signal state as the merge signal.
            # Local-only bindings (local_review: true) additionally require a
            # completed local-review loop; no-review bindings (false/false)
            # gate on CI alone.
            review_bypass_ready = False
            if no_signal_mergeable and not binding.resolved_remote_review():
                review_bypass_ready = (
                    not binding.resolved_local_review()
                    or await self._local_review_completed_for_issue(
                        candidate
                    )
                )

            # SYM-108: a no_signal merge trigger (conflict-fix or review
            # bypass) is honored only when the head's CI is green. PR #24
            # merged on an empty rollup before its build voted. Gate it:
            # green → merge; pending → keep polling; failed → defer to the
            # review/required-check fix path. With zero checks reporting,
            # merge only when `verify_cmd` ran green for this exact head (or
            # the repo opted into `allow_unverified_merge`); otherwise hand to
            # an operator instead of silently merging unverified code.
            if conflict_fix_ready or review_bypass_ready:
                check_state = _no_signal_head_check_state(view)
                if check_state == "none":
                    verified = await db.issue_prs.has_verify_passed(
                        self._conn,
                        issue_id=candidate.issue_id,
                        github_repo=binding.github_repo,
                        head_sha=head_sha,
                    )
                    if not (binding.allow_unverified_merge or verified):
                        await self._mark_merge_needs_approval(
                            binding=binding,
                            issue=issue,
                            pr_url=candidate.pr_url,
                            run_id=str(uuid.uuid4()),
                            reason=(
                                "no CI checks report on the head and no green "
                                "verify_cmd for it — merge needs operator "
                                "approval"
                            ),
                            create_run=True,
                        )
                        continue
                elif check_state == "pending":
                    # Checks still running — keep polling until they settle.
                    # Never merge.
                    continue
                elif check_state == "failed":
                    # A failing *required* check is driven by the required-check
                    # fix path above (it dispatched a rerun this tick, or one is
                    # already mid-flight); keep polling for that. But a failing
                    # *non-required* check (e.g. a Vercel build, PR #24) yields
                    # no `required_check_failures` and thus no fix path — keep
                    # polling forever otherwise. Escalate to an operator instead,
                    # mirroring the "none" branch.
                    if not required_check_failures:
                        await self._mark_merge_needs_approval(
                            binding=binding,
                            issue=issue,
                            pr_url=candidate.pr_url,
                            run_id=str(uuid.uuid4()),
                            reason=(
                                "head CI failed and no failing check is "
                                "branch-protection required (no fix path) — "
                                "merge needs operator approval"
                            ),
                            create_run=True,
                        )
                    continue

            if (
                verdict.kind is VerdictKind.APPROVED
                or conflict_fix_ready
                or review_bypass_ready
            ):
                if (
                    binding.acceptance.mode != "off"
                    and not await self._acceptance_passed_for_candidate(
                        candidate, binding, head_sha
                    )
                ):
                    if self._dispatch_capacity(binding) <= 0:
                        continue
                    if await self._acceptance_infra_retry_backoff_active(
                        candidate.issue_id
                    ):
                        continue
                    scheduled.append(
                        self._schedule_acceptance(
                            binding=binding,
                            issue=issue,
                            pr_number=candidate.pr_number,
                            pr_url=candidate.pr_url,
                            pr_head_sha=head_sha,
                        )
                    )
                    continue
                if not binding.auto_merge:
                    await self._park_pr_for_manual_merge(
                        binding=binding,
                        issue=issue,
                        pr_number=candidate.pr_number,
                        pr_url=candidate.pr_url,
                    )
                    continue
                if self._dispatch_capacity(binding) <= 0:
                    continue
                if _needs_human_approval_label_present(issue):
                    await self._open_merge_wait_for_human_approval_label(
                        binding=binding,
                        issue=issue,
                        pr_url=candidate.pr_url,
                    )
                    continue
                on_started: Callable[[str], Awaitable[None]] | None = None
                if conflict_fix_ready:
                    async def clear_conflict_fix_marker(
                        _run_id: str,
                        *,
                        issue_id: str = candidate.issue_id,
                        github_repo: str = binding.github_repo,
                        pr_number: int = candidate.pr_number,
                        pr_created_at: str = candidate.created_at,
                    ) -> None:
                        await db.issue_prs.clear_merge_conflict_fixed(
                            self._conn,
                            issue_id=issue_id,
                            github_repo=github_repo,
                            pr_number=pr_number,
                            pr_created_at=pr_created_at,
                        )

                    on_started = clear_conflict_fix_marker

                scheduled.append(
                    self._schedule_merge(
                        binding=binding,
                        issue=issue,
                        pr_number=candidate.pr_number,
                        pr_url=candidate.pr_url,
                        approved_head_sha=head_sha,
                        skip_review=verdict.kind is not VerdictKind.APPROVED,
                        on_started=on_started,
                    )
                )
            elif verdict.merge_conflict:
                await db.issue_prs.clear_merge_conflict_fixed(
                    self._conn,
                    issue_id=candidate.issue_id,
                    github_repo=binding.github_repo,
                    pr_number=candidate.pr_number,
                    pr_created_at=candidate.created_at,
                )
                scheduled.append(
                    self._schedule_merge_conflict_rebase_fix(
                        binding=binding,
                        issue=issue,
                        pr_number=candidate.pr_number,
                        pr_url=candidate.pr_url,
                        view=view,
                    )
                )
            elif verdict.kind is VerdictKind.CHANGES_REQUESTED:
                await db.issue_prs.clear_merge_conflict_fixed(
                    self._conn,
                    issue_id=candidate.issue_id,
                    github_repo=binding.github_repo,
                    pr_number=candidate.pr_number,
                    pr_created_at=candidate.created_at,
                )
        return scheduled


    def _schedule_merge_conflict_rebase_fix(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        view: dict[str, object],
    ) -> asyncio.Task[None]:
        async def dispatch_conflict_fix() -> None:
            await self._dispatch_merge_conflict_rebase_fix_run(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                view=view,
            )

        task = asyncio.create_task(dispatch_conflict_fix())
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)
        return task


    def _schedule_merge_required_check_fix(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        head_sha: str,
        failing_checks: list[dict[str, object]],
        merge_error: str,
    ) -> asyncio.Task[None]:
        async def dispatch_required_check_fix() -> None:
            await self._dispatch_merge_required_check_fix_if_allowed(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                head_sha=head_sha,
                failing_checks=failing_checks,
                merge_error=merge_error,
            )

        task = asyncio.create_task(dispatch_required_check_fix())
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)
        return task


    def _schedule_merge(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        approved_head_sha: str = "",
        skip_review: bool = False,
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> asyncio.Task[None]:
        binding_key = _binding_key(binding)
        self._reserve_scheduled_slot(issue_id=issue.id, binding_key=binding_key)
        task = asyncio.create_task(
            self._merge_with_limits(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                approved_head_sha=approved_head_sha,
                skip_review=skip_review,
                on_started=on_started,
            )
        )
        self._dispatch_tasks.add(task)
        task.add_done_callback(
            partial(
                self._dispatch_task_done,
                issue_id=issue.id,
                binding_key=binding_key,
            )
        )
        return task


    async def _merge_with_limits(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        approved_head_sha: str = "",
        skip_review: bool = False,
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        key = _binding_key(binding)
        binding_sem = self._binding_dispatch_sems.setdefault(
            key,
            asyncio.Semaphore(max(binding.max_concurrent, 1)),
        )
        try:
            async with self._global_dispatch_sem:
                async with binding_sem:
                    current = await self._refresh_merge_candidate(binding, issue)
                    if current is None:
                        return
                    await self._merge_approved_pr(
                        binding=binding,
                        issue=current,
                        pr_number=pr_number,
                        pr_url=pr_url,
                        approved_head_sha=approved_head_sha,
                        skip_review=skip_review,
                        on_started=on_started,
                    )
        except asyncio.CancelledError:
            run_id = self._dispatch_run_ids.get(issue.id)
            if run_id is not None:
                await self._fail_run(run_id, "merge cancelled")
            raise


    async def _refresh_merge_candidate(
        self,
        binding: RepoBinding,
        issue: LinearIssue,
    ) -> LinearIssue | None:
        tracker = self.tracker(binding)
        try:
            current = await tracker.lookup_issue(issue.id)
        except LinearError as e:
            log.warning(
                "could not revalidate %s before merge execution: %s",
                issue.identifier,
                e,
            )
            return None
        if not _merge_issue_matches_binding(current, binding):
            log.info(
                "skipping merge for %s: issue is no longer active for binding %s/%s",
                current.identifier,
                binding.github_repo,
                binding.issue_label or "",
            )
            return None
        return current


    async def _maybe_post_codex_lgtm(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        state: db.review_state.ReviewState,
        pr_number: int | None,
        head_committed_at: str = "",
        issue_comments: list[dict[str, object]] | None = None,
    ) -> None:
        """Fetch PR issue comments; if Codex posted a 'no issues' comment that
        hasn't been announced in Linear yet, post the notification once."""
        if pr_number is None:
            return
        if issue_comments is None:
            try:
                raw = await self._gh.pr_issue_comments(
                    pr_number, repo=binding.github_repo
                )
            except GitHubError as e:
                log.warning(
                    "could not fetch issue comments for %s#%d: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
                return
        else:
            raw = issue_comments

        lgtm_comment: dict[str, Any] | None = None
        cycle_started_raw = run.started_at
        issue_pr = await db.issue_prs.get(
            self._conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
        )
        if issue_pr is not None:
            cycle_started_raw = issue_pr.created_at
        try:
            cycle_started_at = _parse_rfc3339(cycle_started_raw)
        except ValueError:
            log.warning(
                "could not parse review cycle start for %s: %s",
                issue.identifier,
                cycle_started_raw,
            )
            return
        min_created_at = cycle_started_at
        if head_committed_at:
            try:
                head_dt = _parse_rfc3339(head_committed_at)
            except ValueError:
                log.warning(
                    "could not parse PR head commit time for %s: %s",
                    issue.identifier,
                    head_committed_at,
                )
            else:
                min_created_at = max(min_created_at, head_dt)
        for entry in raw:
            user: dict[str, Any] = entry.get("user") or {}
            login = str(user.get("login") or "")
            body = str(entry.get("body") or "")
            created_at_raw = str(entry.get("created_at") or entry.get("createdAt") or "")
            if not created_at_raw:
                continue
            try:
                created_at = _parse_rfc3339(created_at_raw)
            except ValueError:
                log.warning(
                    "ignoring Codex LGTM comment with invalid created_at: %s",
                    created_at_raw,
                )
                continue
            if created_at < min_created_at:
                continue
            if is_codex_author(login) and self._CODEX_NO_ISSUES_MARKER in body.lower():
                lgtm_comment = entry

        if lgtm_comment is None:
            return

        comment_id = str(lgtm_comment.get("id") or "")
        if not comment_id or comment_id == state.codex_lgtm_comment_id:
            return

        pr_url = state.pr_url or f"https://github.com/{binding.github_repo}/pull/{pr_number}"
        v = CommentVars(
            stage="review",
            repo=binding.github_repo,
            issue=pr_number,
            pr_url=pr_url,
            run_id=str(run.id),
        )
        tracker = self.tracker(binding)
        try:
            await tracker.post_comment(issue.id, codex_lgtm(v))
        except LinearError as e:
            log.warning(
                "could not post codex_lgtm comment for %s: %s",
                issue.identifier,
                e,
            )
            return
        await db.review_state.set_codex_lgtm_comment_id(self._conn, issue.id, comment_id)


    async def _review_verdict_for_pr(
        self,
        *,
        binding: RepoBinding,
        pr_number: int,
        view: dict[str, object] | None = None,
    ) -> Verdict:
        if view is None:
            view = await self._gh.pr_view(pr_number, repo=binding.github_repo)
        head_sha = str(view.get("headRefOid") or "")
        if not head_sha:
            raise GitHubError(f"pr view missing headRefOid for {binding.github_repo}#{pr_number}")

        checks = await self._gh.pr_checks(pr_number, repo=binding.github_repo)
        ci = [_review_check_from_github(run) for run in checks.runs]
        if not binding.resolved_remote_review():
            human_reviews = tuple(
                r
                for r in _reviews_from_github(
                    await self._gh.pr_reviews(pr_number, repo=binding.github_repo)
                )
                if not is_codex_author(r.user_login)
            )
            return review_classifier(
                comments=[],
                ci=ci,
                snapshot=ReviewSnapshot(
                    head_sha=head_sha,
                    head_committed_at="",
                    reviews=human_reviews,
                    reactions=(),
                    mergeable=str(view.get("mergeable") or ""),
                ),
            )

        comments = await self._gh.pr_review_comments(
            pr_number,
            repo=binding.github_repo,
        )
        reviews = await self._gh.pr_reviews(pr_number, repo=binding.github_repo)
        reactions = await self._gh.pr_reactions(pr_number, repo=binding.github_repo)
        try:
            issue_comments = await self._gh.pr_issue_comments(
                pr_number,
                repo=binding.github_repo,
            )
        except GitHubError as e:
            log.warning(
                "could not fetch PR issue comments for %s#%d: %s",
                binding.github_repo,
                pr_number,
                e,
            )
            issue_comments = []
        committed_at = await self._gh.commit_committed_at(binding.github_repo, head_sha)

        snapshot = ReviewSnapshot(
            head_sha=head_sha,
            head_committed_at=committed_at,
            reactions=(
                *_reactions_from_github(reactions),
                *_codex_lgtm_reactions_from_issue_comments(issue_comments),
            ),
            reviews=_reviews_from_github(reviews),
            mergeable=str(view.get("mergeable") or ""),
        )
        return review_classifier(
            comments=_review_comments_from_github(comments),
            ci=ci,
            snapshot=snapshot,
        )


    async def _poll_submitted_merge(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        run_id: str,
    ) -> None:
        try:
            view = await self._gh.pr_view(pr_number, repo=binding.github_repo)
            if await self._finalize_pr_if_closed(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                run_id=run_id,
                create_run=False,
                view=view,
            ):
                return
        except Exception as e:  # noqa: BLE001 — retry finalization next tick
            log.warning(
                "could not verify submitted merge for %s#%d: %s",
                binding.github_repo,
                pr_number,
                e,
            )
            return

        try:
            verdict = await self._review_verdict_for_pr(
                binding=binding,
                pr_number=pr_number,
                view=view,
            )
        except GitHubError as e:
            log.warning(
                "could not reclassify submitted merge for %s#%d: %s",
                binding.github_repo,
                pr_number,
                e,
            )
            return
        if verdict.kind is VerdictKind.CHANGES_REQUESTED:
            reason = "merge readiness regressed"
            if verdict.failing_checks:
                reason = "required CI failed: " + ", ".join(verdict.failing_checks)
            elif verdict.merge_conflict:
                reason = "merge conflict against base"
            elif verdict.rule:
                reason = f"review readiness regressed: {verdict.rule}"
            await self._mark_merge_needs_approval(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                run_id=run_id,
                reason=reason,
            )


    async def _finalize_pr_if_closed(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr: db.issue_prs.IssuePR | None = None,
        pr_number: int,
        pr_url: str,
        run_id: str,
        create_run: bool,
        view: dict[str, object],
    ) -> bool:
        if _pr_view_is_merged(view):
            if create_run:
                inserted = await db.runs.create_if_no_active(
                    self._conn,
                    id=run_id,
                    issue_id=issue.id,
                    stage="merge",
                    status="running",
                    pid=None,
                    started_at=self._now().isoformat(),
                    ignored_stage="review",
                )
                if not inserted:
                    return True
            try:
                await self._mark_merge_done(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    run_id=run_id,
                )
            except Exception as e:
                if not create_run:
                    raise
                log.warning(
                    "could not finalize externally merged PR %s#%d: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
                await self._mark_merge_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    run_id=run_id,
                    reason=f"merge finalization failed: {e}",
                    create_run=False,
                )
            return True
        if _pr_view_is_closed(view):
            if pr is not None and pr.parked_at is not None and not binding.auto_merge:
                await self._mark_parked_closed_unmerged_pr_done(
                    binding=binding,
                    issue=issue,
                    pr=pr,
                )
                return True
            await self._mark_merge_needs_approval(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                run_id=run_id,
                reason="pull request closed before merge",
                create_run=create_run,
            )
            return True
        return False


    async def _merge_approved_pr(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        approved_head_sha: str = "",
        skip_review: bool = False,
        on_started: Callable[[str], Awaitable[None]] | None = None,
    ) -> str | None:
        run_id = str(uuid.uuid4())
        now = self._now().isoformat()
        inserted = await db.runs.create_if_no_active(
            self._conn,
            id=run_id,
            issue_id=issue.id,
            stage="merge",
            status="running",
            pid=None,
            started_at=now,
            ignored_stage="review",
            ignored_stages=("review_fix",) if skip_review else (),
        )
        if not inserted:
            return None

        await self._complete_review_monitors_for_merge(issue)
        self._dispatch_run_ids[issue.id] = run_id
        if on_started is not None:
            try:
                await on_started(run_id)
            except Exception:  # noqa: BLE001
                log.exception(
                    "merge start callback failed for %s run %s",
                    issue.identifier,
                    run_id,
                )
        workspace_path: Path | None = None
        try:
            try:
                workspace_path = await self._workspace.acquire(binding, issue)
            except Exception as e:  # noqa: BLE001
                log.exception("workspace acquire failed for merge %s", issue.identifier)
                await self._mark_merge_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    run_id=run_id,
                    reason=f"workspace acquire failed: {e}",
                    exc=e,
                )
                return run_id

            # Sync workspace to the remote branch so the agent starts from a
            # clean state and any subsequent push succeeds (fast-forward).
            # Review-fix runs may have left behind local commits that diverge
            # from the remote; resetting here avoids a non-fast-forward failure.
            branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
            try:
                await _sync_workspace_to_remote(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "workspace sync failed for merge %s, proceeding anyway: %s",
                    issue.identifier,
                    e,
                )

            try:
                prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
                (
                    cumulative_usage,
                    final_kind,
                    final_returncode,
                ) = await self._run_merge_agent(
                    binding=binding,
                    issue=issue,
                    run_id=run_id,
                    workspace_path=workspace_path,
                    pr_url=pr_url,
                    prior_total=prior_total,
                )
            except Exception as e:  # noqa: BLE001
                log.exception("merge agent execution failed for %s", issue.identifier)
                await self._mark_merge_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    run_id=run_id,
                    reason=f"merge agent execution failed: {e}",
                    exc=e,
                )
                return run_id

            await _add_run_usage(self._conn, run_id, cumulative_usage)

            transition = on_runner_event(
                stage="merge",
                event_kind=final_kind,
                returncode=final_returncode,
            )
            if transition.next_run_status != "completed":
                await self._mark_merge_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    run_id=run_id,
                    reason=f"merge runner ended with {final_kind}",
                    final_kind=final_kind,
                    returncode=final_returncode,
                )
                return run_id

            try:
                await self._push_fn(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning("merge push failed for %s#%d: %s", binding.github_repo, pr_number, e)
                await self._mark_merge_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    run_id=run_id,
                    reason=str(e),
                    exc=e,
                )
                return run_id

            try:
                premerge_view = await self._gh.pr_view(
                    pr_number,
                    repo=binding.github_repo,
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "could not pre-check mergeability for %s#%d before merge: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
                if approved_head_sha:
                    await self._mark_merge_needs_approval(
                        binding=binding,
                        issue=issue,
                        pr_url=pr_url,
                        run_id=run_id,
                        reason=f"post-push HEAD verification failed: {e}",
                        exc=e,
                    )
                    return run_id
            else:
                premerge_head_sha = str(premerge_view.get("headRefOid") or "")
                if approved_head_sha and premerge_head_sha != approved_head_sha:
                    try:
                        verdict = await self._review_verdict_for_pr(
                            binding=binding,
                            pr_number=pr_number,
                            view=premerge_view,
                        )
                    except GitHubError as e:
                        log.warning(
                            "could not classify review for post-merge-agent HEAD "
                            "%s#%d at %s: %s",
                            binding.github_repo,
                            pr_number,
                            premerge_head_sha[:12] or "(unknown)",
                            e,
                        )
                        verdict = None
                    if verdict is None or verdict.kind is not VerdictKind.APPROVED:
                        reason = (
                            "merge-agent pushed unreviewed HEAD "
                            f"{premerge_head_sha or '(unknown)'}"
                        )
                        await self._mark_merge_needs_approval(
                            binding=binding,
                            issue=issue,
                            pr_url=pr_url,
                            run_id=run_id,
                            reason=reason,
                        )
                        state = await db.review_state.get(self._conn, issue.id)
                        await self._retrigger_codex_review_unless_approved(
                            binding=binding,
                            issue=issue,
                            state=state,
                        )
                        return run_id

                if _pr_view_has_merge_conflict(premerge_view):
                    await self._dispatch_merge_conflict_rebase_fix_run(
                        binding=binding,
                        issue=issue,
                        pr_number=pr_number,
                        pr_url=pr_url,
                        view=premerge_view,
                        merge_run_id=run_id,
                        dispatch_capacity_held=True,
                    )
                    return run_id

            try:
                await self._gh.pr_merge(
                    pr_number,
                    strategy=binding.merge_strategy,
                    auto=binding.allow_auto_merge,
                    repo=binding.github_repo,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("merge failed for %s#%d: %s", binding.github_repo, pr_number, e)
                if _is_merge_conflict_error(e):
                    try:
                        conflict_view = await self._gh.pr_view(
                            pr_number,
                            repo=binding.github_repo,
                        )
                    except Exception as view_error:  # noqa: BLE001
                        log.warning(
                            "could not refresh PR base after merge conflict for "
                            "%s#%d: %s",
                            binding.github_repo,
                            pr_number,
                            view_error,
                        )
                        conflict_view = None
                    await self._dispatch_merge_conflict_rebase_fix_run(
                        binding=binding,
                        issue=issue,
                        pr_number=pr_number,
                        pr_url=pr_url,
                        view=conflict_view,
                        merge_run_id=run_id,
                        dispatch_capacity_held=True,
                    )
                    return run_id
                required_view: dict[str, object] | None = None
                if (
                    "premerge_view" in locals()
                    and isinstance(premerge_view, dict)
                    and "statusCheckRollup" in premerge_view
                ):
                    required_view = premerge_view
                else:
                    try:
                        required_view = await self._gh.pr_view(
                            pr_number,
                            repo=binding.github_repo,
                            include_status_checks=True,
                        )
                    except Exception as view_error:  # noqa: BLE001
                        log.warning(
                            "could not refresh PR checks after merge failure for "
                            "%s#%d: %s",
                            binding.github_repo,
                            pr_number,
                            view_error,
                        )
                if required_view is not None:
                    required_failures = await self._required_check_failures_for_view(
                        binding=binding,
                        pr_number=pr_number,
                        view=required_view,
                        required_context_cache={},
                    )
                    if required_failures:
                        dispatched = await self._dispatch_merge_required_check_fix_if_allowed(
                            binding=binding,
                            issue=issue,
                            pr_number=pr_number,
                            pr_url=pr_url,
                            head_sha=str(required_view.get("headRefOid") or ""),
                            failing_checks=required_failures,
                            merge_error=str(e),
                            merge_run_id=run_id,
                            dispatch_capacity_held=True,
                        )
                        if (
                            dispatched
                            or await db.operator_waits.get(self._conn, issue.id)
                            is not None
                        ):
                            return run_id
                await self._mark_merge_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    run_id=run_id,
                    reason=str(e),
                    exc=e,
                )
                return run_id

            try:
                merged = await self._mark_merge_done_if_merged(
                    binding=binding,
                    issue=issue,
                    pr_number=pr_number,
                    pr_url=pr_url,
                    run_id=run_id,
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "could not verify merge completion for %s#%d: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
                await self._mark_merge_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    run_id=run_id,
                    reason=f"merge finalization failed: {e}",
                    exc=e,
                )
                return run_id
            if not merged:
                await db.runs.update_status(
                    self._conn,
                    run_id,
                    "completed",
                    ended_at=self._now().isoformat(),
                )
            return run_id
        finally:
            if workspace_path is not None:
                self._workspace.release(binding, issue)
            self._dispatch_run_ids.pop(issue.id, None)


    async def _mark_merge_done_if_merged(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        run_id: str,
    ) -> bool:
        view = await self._gh.pr_view(pr_number, repo=binding.github_repo)
        if not _pr_view_is_merged(view):
            return False
        await self._mark_merge_done(
            binding=binding,
            issue=issue,
            pr_url=pr_url,
            run_id=run_id,
        )
        return True


    async def _mark_merge_done(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
        run_id: str,
    ) -> None:
        states = await self._states_for_binding(binding)
        done_id = states.get(binding.linear_states.done)
        if done_id is None:
            await self._mark_merge_needs_approval(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                run_id=run_id,
                reason=f"missing Linear state: {binding.linear_states.done}",
            )
            return

        tokens = await db.runs.tokens_for_issue(self._conn, issue.id)
        tracker = self.tracker(binding)
        await tracker.move_issue(issue.id, done_id)
        final_body = stage_done(
            CommentVars(
                stage="merge",
                next_stage="done",
                repo=binding.github_repo,
                issue=0,
                pr_url=pr_url,
                run_id=run_id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
            )
        )
        try:
            await tracker.post_comment(issue.id, truncate_body(final_body))
        except LinearError as e:
            log.warning(
                "could not post final merge comment for %s: %s",
                issue.identifier,
                e,
            )
        ended_at = self._now().isoformat()
        await db.issue_prs.mark_merged(
            self._conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            merged_at=ended_at,
        )
        await db.runs.update_status(self._conn, run_id, "done", ended_at=ended_at)
        await self._clear_operator_wait(issue.id, run_id)
        try:
            archived = await self._workspace.cleanup(issue)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "workspace cleanup failed after merge finalization for %s: %s",
                issue.identifier,
                e,
            )
        else:
            for archive_path in archived:
                try:
                    await tracker.post_comment(
                        issue.id,
                        "⚠️ Workspace had uncommitted or unpushed work and was "
                        f"archived to `{archive_path}` (kept 14 days).",
                    )
                except LinearError as e:
                    log.warning(
                        "could not post workspace archive comment for %s: %s",
                        issue.identifier,
                        e,
                    )


    async def _mark_merge_needs_approval(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
        run_id: str,
        reason: str,
        create_run: bool = False,
        final_kind: str | None = None,
        returncode: int | None = None,
        exc: BaseException | str | None = None,
    ) -> None:
        if create_run:
            inserted = await db.runs.create_if_no_active(
                self._conn,
                id=run_id,
                issue_id=issue.id,
                stage="merge",
                status="running",
                pid=None,
                started_at=self._now().isoformat(),
                ignored_stage="review",
            )
            if not inserted:
                return

        try:
            needs_approval_id: str | None = None
            tracker = self.tracker(binding)
            try:
                states = await self._states_for_binding(binding)
                needs_approval_id = states.get(binding.linear_states.needs_approval)
            except LinearError as e:
                log.warning(
                    "could not load states while parking %s in needs approval: %s",
                    issue.identifier,
                    e,
                )

            tokens = await db.runs.tokens_for_issue(self._conn, issue.id)
            body = awaiting_approval(
                CommentVars(
                    stage="merge",
                    next_stage="done",
                    repo=binding.github_repo,
                    issue=0,
                    pr_url=pr_url,
                    run_id=run_id,
                    input_tokens=tokens.input_tokens,
                    output_tokens=tokens.output_tokens,
                    cache_write_tokens=tokens.cache_write_tokens,
                    cache_read_tokens=tokens.cache_read_tokens,
                    error=reason,
                )
            )
            if needs_approval_id is not None:
                try:
                    await tracker.move_issue(issue.id, needs_approval_id)
                except LinearError as e:
                    log.warning(
                        "could not move %s to needs approval after merge failure: %s",
                        issue.identifier,
                        e,
                    )
            else:
                log.warning(
                    "missing Linear state %r for %s after merge failure",
                    binding.linear_states.needs_approval,
                    issue.identifier,
                )
            try:
                await tracker.post_comment(issue.id, truncate_body(body))
            except LinearError as e:
                log.warning("needs approval comment failed on %s: %s", issue.identifier, e)
        finally:
            await db.runs.update_status(
                self._conn,
                run_id,
                "needs_approval",
                ended_at=self._now().isoformat(),
                **_termination_kwargs(
                    status="needs_approval",
                    final_kind=final_kind,
                    returncode=returncode,
                    exc=exc,
                    reason=reason,
                ),
            )
            # Register so $approve/$reject can be received after restart.
            # Done inside finally so it runs even when a non-LinearError above escapes.
            self._dispatch_run_ids[issue.id] = run_id
            self._operator_wait_run_ids.add(run_id)
            self._merge_needs_approval_bindings[run_id] = binding
            try:
                await db.operator_waits.upsert(
                    self._conn,
                    issue_id=issue.id,
                    run_id=run_id,
                    kind=db.operator_waits.KIND_MERGE,
                    linear_team_key=binding.linear_team_key,
                    github_repo=binding.github_repo,
                    issue_label=binding.issue_label or "",
                    created_at=self._now().isoformat(),
                    provider=binding.provider,
                    tracker_provider=binding.tracker_provider,
                    tracker_site=binding.tracker_site,
                )
            except Exception:
                log.warning(
                    "could not persist operator_wait for %s run %s",
                    issue.identifier,
                    run_id,
                )


    async def _run_merge_agent(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        run_id: str,
        workspace_path: Path,
        pr_url: str,
        prior_total: float,
    ) -> tuple[UsageDelta, str, int | None]:
        prompt = merge_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
            pr_url=pr_url,
        )
        command = build_merge_runner_command(
            binding.agent,
            prompt,
            codex_model=binding.codex_model,
            workspace_path=workspace_path,
            mcp_servers=binding.mcp_servers,
        )
        return await self._run_stage_command(
            binding=binding,
            issue=issue,
            command=command,
            run_id=run_id,
            workspace_path=workspace_path,
            stage="merge",
            prior_total=prior_total,
        )
