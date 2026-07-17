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
from typing import NamedTuple

from ... import db
from ...agent.prompt import (
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
    resumed,
    stage_done,
    truncate_body,
)
from ...notify import EVENT_OPERATOR_WAIT, EVENT_PR_MERGED
from ...pipeline.cost_guard import UsageDelta
from ...pipeline.local_review_loop import (
    LoopOutcome,
    LoopResult,
)
from ...pipeline.review_classifier import (
    Verdict,
    VerdictKind,
    has_hit_iteration_cap,
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
    _binding_storage_key,
    _OrchestratorBase,
)
from ._git import (
    _git_fetch_branch,
    _sync_workspace_to_remote,
    _workspace_head_sha,
    _workspace_ref_sha,
)
from ._helpers import (
    NEEDS_HUMAN_APPROVAL_LABEL,
    _add_run_usage,
    _local_review_termination_reason,
    _needs_human_approval_label_present,
    _no_signal_head_check_state,
    _pr_base_ref_from_view,
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
    _termination_kwargs,
    build_fix_runner_command,
    build_merge_runner_command,
    role_claude_model,
    role_codex_model,
)
from ._review import (
    CODEX_NO_ISSUES_MARKER,
    _local_review_infra_failed,
    _local_review_needs_approval,
    _read_run_stream_api_error_obj,
)

log = logging.getLogger(__name__)


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


class _MergePreDispatch(NamedTuple):
    """Outcome of the pre-merge finalize/fix stage for a single merge candidate.

    `handled` is non-None (the tasks to schedule) when a finalize/parked/
    conflict/required-check branch owns the candidate and the caller should
    stop. When None, the candidate proceeds to the merge decision using the
    fetched `view` and `required_check_failures`.
    """

    handled: list[asyncio.Task[None]] | None
    view: dict[str, object] | None
    required_check_failures: list[dict[str, object]]


class _NoSignalMergeReadiness(NamedTuple):
    """Whether a clean no_signal head is merge-ready, and via which path."""

    conflict_fix_ready: bool
    review_bypass_ready: bool


class _MergeMixin(_OrchestratorBase):
    """Owns the poll loop's merge domain; `Orchestrator` extends it."""

    _CODEX_NO_ISSUES_MARKER = CODEX_NO_ISSUES_MARKER

    async def _run_auto_recoverable_merge_wait_reconciler(self, shutdown: asyncio.Event) -> None:
        log.info(
            "auto-recoverable merge wait reconciler entering loop (interval=%ds)",
            MERGE_WAIT_RECONCILE_INTERVAL_SECS,
        )
        while not shutdown.is_set():
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=MERGE_WAIT_RECONCILE_INTERVAL_SECS)
                break
            except TimeoutError:
                pass
            try:
                await self._reconcile_orphaned_merge_runs(reason="periodic")
            except Exception:  # noqa: BLE001
                log.exception("orphaned merge run reconcile failed")
            try:
                recovered = await self._reconcile_auto_recoverable_merge_waits(reason="periodic")
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
        before = (self._now() - timedelta(seconds=ORPHANED_MERGE_RUN_GRACE_SECS)).isoformat()
        issue_ids = await db.runs.supersede_orphaned_merge_needs_approval(self._conn, before=before)
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

    async def _reconcile_auto_recoverable_merge_waits(self, *, reason: str = "manual") -> int:
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
                "skipping merge wait reconcile for %s: issue is no longer active for binding %s/%s",
                issue.identifier,
                binding.github_repo,
                binding.issue_label or "",
            )
            return False

        try:
            view = await (await self._gh_client()).pr_view(pr.pr_number, repo=binding.github_repo)
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
                    "could not classify review before clean merge wait reconcile %s#%d: %s",
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

                # Reserved under `config_write_lock` so the drain guard's
                # `scheduled_slots` sample can't miss this reservation
                # (SYM-193 review; see `_review_fix_dispatch_slot` in
                # `_dispatch.py`).
                async with self._config_write_lock:
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
        gh = await self._gh_client()
        repo_view = getattr(gh, "repo_view", None)
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

    def _merge_wait_reconcile_task_done(self, task: asyncio.Task[None], *, issue_id: str) -> None:
        self._dispatch_tasks.discard(task)
        self._merge_wait_reconcile_issue_ids.discard(issue_id)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("merge wait recovery task crashed for issue_id=%s", issue_id)

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
            result_obj: object = (await self._gh_client()).repo_default_branch(binding.github_repo)
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
                gh=await self._gh_client(),
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
        return [check for check in failing_rollup_checks if _status_check_names(check) & required]

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
                    tail = await (await self._gh_client()).run_failed_log_tail(run_id, repo=repo)
                else:
                    link = str(check.get("detailsUrl") or check.get("targetUrl") or "")
                    tail = await (await self._gh_client()).check_log_tail(
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
        self, *, binding: RepoBinding, issue: LinearIssue, merge_run_id: str | None
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
                binding_key=_binding_storage_key(binding),
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
        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
        start_sha = ""

        async def on_acquire_failure(e: Exception) -> None:
            await self._mark_merge_required_check_fix_needs_approval(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                reason=f"required-check fix-run failed: workspace acquire failed: {e}",
                merge_run_id=merge_run_id,
            )

        async def on_dedup_loss() -> bool | None:
            # Lost the race: an existing review_fix is already running.
            # Interrupt the parent merge so it doesn't stay stuck in "running".
            if merge_run_id is not None:
                await db.runs.interrupt_running_merge(self._conn, merge_run_id)
            return None

        async def setup(workspace_path: Path) -> bool:
            nonlocal start_sha
            try:
                github_token = await self._resolve_github_token(repo=binding.github_repo)
                await _git_fetch_branch(workspace_path, branch, github_token=github_token)
            except Exception as e:  # noqa: BLE001
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
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=(
                        f"required-check fix-run failed: could not read remote HEAD for {branch}"
                    ),
                    merge_run_id=merge_run_id,
                )
                return False
            return True

        async def body(
            workspace_path: Path,
            fix_run_id: str,
            drop_dispatch_id: Callable[[], None],
        ) -> bool | None:
            prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
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

            drop_dispatch_id()
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
                            binding=binding,
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
                    binding=binding,
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

        return await self._run_fix_dispatch(
            binding=binding,
            issue=issue,
            ignored_stages=("review", "merge"),
            on_acquire_failure=on_acquire_failure,
            body=body,
            setup=setup,
            on_dedup_loss=on_dedup_loss,
            dispatch_capacity_held=dispatch_capacity_held,
        )

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
        role = binding.resolved_role("fix", self.config.roles)
        command = build_fix_runner_command(
            role.agent,
            prompt,
            codex_model=role_codex_model(role),
            claude_model=role_claude_model(role),
            effort=role.effort,
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
            role=role,
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

        async def on_acquire_failure(e: Exception) -> None:
            await self._mark_merge_conflict_fix_needs_approval(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                reason=f"merge-conflict fix-run failed: workspace acquire failed: {e}",
                merge_run_id=merge_run_id,
            )

        async def on_dedup_loss() -> bool:
            # Lost the race: an existing review_fix is already running.
            # Interrupt the parent merge so it doesn't stay stuck in "running".
            # Return False so callers don't clear operator waits for a fix that
            # was never started (e.g. the reconcile-merge-wait path).
            if merge_run_id is not None:
                await db.runs.interrupt_running_merge(self._conn, merge_run_id)
            return False

        async def after_dedup(fix_run_id: str) -> None:
            if on_started is None:
                return
            try:
                await on_started(fix_run_id)
            except Exception:  # noqa: BLE001
                log.exception(
                    "merge-conflict rebase fix-run start callback failed for %s run %s",
                    issue.identifier,
                    fix_run_id,
                )

        async def body(
            workspace_path: Path,
            fix_run_id: str,
            drop_dispatch_id: Callable[[], None],
        ) -> bool:
            prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
            prompt = merge_conflict_rebase_fix_prompt(
                issue_title=issue.title,
                issue_body=issue.description,
                labels=list(issue.labels),
                pr_number=pr_number,
                base_ref=base_ref,
            )
            role = binding.resolved_role("fix", self.config.roles)
            command = build_fix_runner_command(
                role.agent,
                prompt,
                codex_model=role_codex_model(role),
                claude_model=role_claude_model(role),
                effort=role.effort,
                workspace_path=workspace_path,
                mcp_servers=binding.mcp_servers,
            )
            try:
                usage_delta, final_kind, final_returncode = await self._run_stage_command(
                    binding=binding,
                    issue=issue,
                    command=command,
                    run_id=fix_run_id,
                    workspace_path=workspace_path,
                    stage="review_fix",
                    role=role,
                    prior_total=prior_total,
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

            drop_dispatch_id()
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
                    reason=(f"merge-conflict fix-run failed: runner ended with {final_kind}"),
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
                fixed_view = await (await self._gh_client()).pr_view(
                    pr_number, repo=binding.github_repo
                )
                fixed_head_sha = str(fixed_view.get("headRefOid") or "")
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "could not refresh PR head after merge-conflict fix-run for %s#%d: %s",
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

        result = await self._run_fix_dispatch(
            binding=binding,
            issue=issue,
            ignored_stages=("review", "merge"),
            on_acquire_failure=on_acquire_failure,
            body=body,
            after_dedup=after_dedup,
            on_dedup_loss=on_dedup_loss,
            dispatch_capacity_held=dispatch_capacity_held,
        )
        return bool(result)

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
                view = await (await self._gh_client()).pr_view(
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
        if old_state_name is not None and old_state_name != (binding.linear_states.needs_approval):
            return False
        if new_state_name is not None and new_state_name != (binding.linear_states.code_review):
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
        pr_url = state.pr_url or (f"https://github.com/{binding.github_repo}/pull/{pr_number}")
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

        # Reserved under `config_write_lock` — see `_review_fix_dispatch_slot`
        # in `_dispatch.py` (SYM-193 review).
        async with self._config_write_lock:
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
            merged_at=(str(row["merged_at"]) if row["merged_at"] is not None else None),
            parked_at=(str(row["parked_at"]) if row["parked_at"] is not None else None),
        )

    async def _reconcile_parked_closed_unmerged_pr_event(self, event: GitHubWebhookEvent) -> int:
        pr = await self._parked_closed_unmerged_pr_for_event(event)
        if pr is None:
            return 0
        binding = self._binding_for_pr(pr)
        if binding is None or binding.auto_merge:
            return 0
        try:
            view = await (await self._gh_client()).pr_view(pr.pr_number, repo=binding.github_repo)
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
            if current is None or current.pr_number != pr.pr_number or current.parked_at is None:
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
        since = self._now() - timedelta(hours=MERGED_LINEAR_STATE_RECONCILE_LOOKBACK_HOURS)
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
                "missing Linear needs_approval state %r while parking %s for manual merge",
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
        for candidate in await db.issue_prs.list_merge_candidates(self._conn):
            scheduled.extend(await self._process_merge_candidate(candidate, required_context_cache))
        return scheduled

    async def _process_merge_candidate(
        self,
        candidate: db.issue_prs.IssuePR,
        required_context_cache: dict[tuple[str, str], tuple[str, ...]],
    ) -> list[asyncio.Task[None]]:
        """Decide and schedule the next merge action for one candidate PR."""
        resolved = await self._resolve_merge_candidate(candidate)
        if resolved is None:
            return []
        binding, issue = resolved

        if await self._poll_completed_merge_candidate(candidate, binding, issue):
            return []

        pre = await self._handle_merge_candidate_pre_dispatch(
            candidate=candidate,
            binding=binding,
            issue=issue,
            required_context_cache=required_context_cache,
        )
        if pre.handled is not None:
            return pre.handled
        assert pre.view is not None
        view = pre.view

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
            return []

        head_sha = str(view.get("headRefOid") or "")
        readiness = await self._merge_candidate_no_signal_readiness(
            candidate=candidate,
            binding=binding,
            verdict=verdict,
            view=view,
            head_sha=head_sha,
        )

        gated = await self._gate_no_signal_merge_on_ci(
            candidate=candidate,
            binding=binding,
            issue=issue,
            view=view,
            head_sha=head_sha,
            readiness=readiness,
            required_check_failures=pre.required_check_failures,
        )
        if gated is not None:
            return gated

        return await self._dispatch_merge_candidate_decision(
            candidate=candidate,
            binding=binding,
            issue=issue,
            view=view,
            verdict=verdict,
            head_sha=head_sha,
            readiness=readiness,
        )

    async def _resolve_merge_candidate(
        self, candidate: db.issue_prs.IssuePR
    ) -> tuple[RepoBinding, LinearIssue] | None:
        """Resolve binding + refreshed issue, or None when the candidate is skipped."""
        binding = self._binding_for_pr(candidate)
        if binding is None:
            log.warning(
                "no binding for merge candidate %s in %s",
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
        if await db.operator_waits.get(self._conn, candidate.issue_id) is not None:
            return None
        tracker = self.tracker(binding)
        tracker_issue_id, _ = await self._tracker_identity_for_issue(candidate.issue_id)
        try:
            issue = await tracker.lookup_issue(tracker_issue_id)
        except LinearError as e:
            log.warning(
                "could not refresh %s before merge: %s",
                candidate.identifier,
                e,
            )
            return None
        parked_done_cleanup = (
            candidate.parked_at is not None
            and not binding.auto_merge
            and issue.team_key == binding.linear_team_key
            and issue.state_name == binding.linear_states.done
            and (binding.issue_label is None or binding.issue_label in issue.labels)
        )
        if not _merge_issue_matches_binding(issue, binding) and not parked_done_cleanup:
            log.info(
                "skipping merge candidate %s: issue is no longer active for binding %s/%s",
                issue.identifier,
                binding.github_repo,
                binding.issue_label or "",
            )
            return None
        return binding, issue

    async def _poll_completed_merge_candidate(
        self,
        candidate: db.issue_prs.IssuePR,
        binding: RepoBinding,
        issue: LinearIssue,
    ) -> bool:
        """Poll an already-submitted merge; True when this candidate is handled."""
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
            return True
        return False

    async def _handle_merge_candidate_pre_dispatch(
        self,
        *,
        candidate: db.issue_prs.IssuePR,
        binding: RepoBinding,
        issue: LinearIssue,
        required_context_cache: dict[tuple[str, str], tuple[str, ...]],
    ) -> _MergePreDispatch:
        """Finalize / fix the PR before the merge decision.

        Handles closed PRs, parked manual-merge revival, merge-conflict rebase
        fix-runs, and required-check fix-runs. When one owns the candidate,
        `handled` carries the tasks to schedule and the caller stops; otherwise
        `handled` is None and the fetched `view` + `required_check_failures`
        flow into the merge decision.
        """
        tasks: list[asyncio.Task[None]] = []
        try:
            view = await (await self._gh_client()).pr_view(
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
                return _MergePreDispatch(handled=tasks, view=None, required_check_failures=[])
            if candidate.parked_at is not None:
                revived = await self._schedule_parked_manual_merge_revival_if_requested(
                    binding=binding,
                    issue=issue,
                    candidate=candidate,
                    view=view,
                )
                if revived is not None:
                    tasks.append(revived)
                return _MergePreDispatch(handled=tasks, view=None, required_check_failures=[])
            if _pr_view_has_merge_conflict(view):
                await db.issue_prs.clear_merge_conflict_fixed(
                    self._conn,
                    issue_id=candidate.issue_id,
                    github_repo=binding.github_repo,
                    pr_number=candidate.pr_number,
                    pr_created_at=candidate.created_at,
                )
                tasks.append(
                    self._schedule_merge_conflict_rebase_fix(
                        binding=binding,
                        issue=issue,
                        pr_number=candidate.pr_number,
                        pr_url=candidate.pr_url,
                        view=view,
                    )
                )
                return _MergePreDispatch(handled=tasks, view=None, required_check_failures=[])
            required_check_failures = await self._required_check_failures_for_view(
                binding=binding,
                pr_number=candidate.pr_number,
                view=view,
                required_context_cache=required_context_cache,
            )
            if required_check_failures and await self._merge_required_check_fix_should_dispatch(
                issue_id=issue.id,
                head_sha=str(view.get("headRefOid") or ""),
                failing_checks=required_check_failures,
            ):
                tasks.append(
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
                return _MergePreDispatch(handled=tasks, view=None, required_check_failures=[])
            return _MergePreDispatch(
                handled=None,
                view=view,
                required_check_failures=required_check_failures,
            )
        except Exception as e:  # noqa: BLE001 — retry finalization next tick
            log.warning(
                "could not check finalized PR state for %s#%d: %s",
                binding.github_repo,
                candidate.pr_number,
                e,
            )
            return _MergePreDispatch(handled=tasks, view=None, required_check_failures=[])

    async def _merge_candidate_no_signal_readiness(
        self,
        *,
        candidate: db.issue_prs.IssuePR,
        binding: RepoBinding,
        verdict: Verdict,
        view: dict[str, object],
        head_sha: str,
    ) -> _NoSignalMergeReadiness:
        """Whether a clean no_signal head is merge-ready via conflict-fix or bypass."""
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
                or await self._local_review_completed_for_issue(candidate)
            )
        return _NoSignalMergeReadiness(
            conflict_fix_ready=conflict_fix_ready,
            review_bypass_ready=review_bypass_ready,
        )

    async def _gate_no_signal_merge_on_ci(
        self,
        *,
        candidate: db.issue_prs.IssuePR,
        binding: RepoBinding,
        issue: LinearIssue,
        view: dict[str, object],
        head_sha: str,
        readiness: _NoSignalMergeReadiness,
        required_check_failures: list[dict[str, object]],
    ) -> list[asyncio.Task[None]] | None:
        """Gate a no_signal merge trigger on the head's CI state (SYM-108).

        Returns None to let the merge decision proceed; an empty task list when
        the candidate must keep polling / be escalated and not merge this tick.
        """
        if not (readiness.conflict_fix_ready or readiness.review_bypass_ready):
            return None

        # SYM-108: a no_signal merge trigger (conflict-fix or review
        # bypass) is honored only when the head's CI is green. PR #24
        # merged on an empty rollup before its build voted. Gate it:
        # green → merge; pending → keep polling; failed → defer to the
        # review/required-check fix path. With zero checks reporting,
        # merge only when `verify_cmd` ran green for this exact head (or
        # the repo opted into `allow_unverified_merge`); otherwise hand to
        # an operator instead of silently merging unverified code.
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
                return []
        elif check_state == "pending":
            # Checks still running — keep polling until they settle.
            # Never merge.
            return []
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
            return []
        return None

    async def _dispatch_merge_candidate_decision(
        self,
        *,
        candidate: db.issue_prs.IssuePR,
        binding: RepoBinding,
        issue: LinearIssue,
        view: dict[str, object],
        verdict: Verdict,
        head_sha: str,
        readiness: _NoSignalMergeReadiness,
    ) -> list[asyncio.Task[None]]:
        """Schedule acceptance / park / merge / conflict-fix per the final verdict."""
        if (
            verdict.kind is VerdictKind.APPROVED
            or readiness.conflict_fix_ready
            or readiness.review_bypass_ready
        ):
            if binding.acceptance.mode != "off" and not await self._acceptance_passed_for_candidate(
                candidate, binding, head_sha
            ):
                if self._dispatch_capacity(binding) <= 0:
                    return []
                if await self._acceptance_infra_retry_backoff_active(candidate.issue_id):
                    return []
                # Reserved under `config_write_lock` — see
                # `_review_fix_dispatch_slot` in `_dispatch.py` (SYM-193
                # review).
                async with self._config_write_lock:
                    return [
                        self._schedule_acceptance(
                            binding=binding,
                            issue=issue,
                            pr_number=candidate.pr_number,
                            pr_url=candidate.pr_url,
                            pr_head_sha=head_sha,
                        )
                    ]
            if not binding.auto_merge:
                await self._park_pr_for_manual_merge(
                    binding=binding,
                    issue=issue,
                    pr_number=candidate.pr_number,
                    pr_url=candidate.pr_url,
                )
                return []
            if self._dispatch_capacity(binding) <= 0:
                return []
            if _needs_human_approval_label_present(issue):
                await self._open_merge_wait_for_human_approval_label(
                    binding=binding,
                    issue=issue,
                    pr_url=candidate.pr_url,
                )
                return []
            on_started: Callable[[str], Awaitable[None]] | None = None
            if readiness.conflict_fix_ready:

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

            # Reserved under `config_write_lock` — see
            # `_review_fix_dispatch_slot` in `_dispatch.py` (SYM-193 review).
            async with self._config_write_lock:
                return [
                    self._schedule_merge(
                        binding=binding,
                        issue=issue,
                        pr_number=candidate.pr_number,
                        pr_url=candidate.pr_url,
                        approved_head_sha=head_sha,
                        skip_review=verdict.kind is not VerdictKind.APPROVED,
                        on_started=on_started,
                    )
                ]
        elif verdict.merge_conflict:
            await db.issue_prs.clear_merge_conflict_fixed(
                self._conn,
                issue_id=candidate.issue_id,
                github_repo=binding.github_repo,
                pr_number=candidate.pr_number,
                pr_created_at=candidate.created_at,
            )
            return [
                self._schedule_merge_conflict_rebase_fix(
                    binding=binding,
                    issue=issue,
                    pr_number=candidate.pr_number,
                    pr_url=candidate.pr_url,
                    view=view,
                )
            ]
        elif verdict.kind is VerdictKind.CHANGES_REQUESTED:
            await db.issue_prs.clear_merge_conflict_fixed(
                self._conn,
                issue_id=candidate.issue_id,
                github_repo=binding.github_repo,
                pr_number=candidate.pr_number,
                pr_created_at=candidate.created_at,
            )
        return []

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
            view = await (await self._gh_client()).pr_view(pr_number, repo=binding.github_repo)
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
                    binding_key=_binding_storage_key(binding),
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
        # Launch gate: merge is a follow-up stage (drains in-flight work even
        # on a disabled binding), but a lowered `max_concurrent` must still
        # admit nothing new (SYM-193 review). Held under `_dispatch_pause_lock`
        # (the same lock `_dispatch_one` uses) so the gate's occupancy read
        # and this insert move atomically.
        async with self._dispatch_pause_lock:
            if not await self._launch_gate_admits(binding, first_dispatch=False):
                return None
            inserted = await db.runs.create_if_no_active(
                self._conn,
                id=run_id,
                issue_id=issue.id,
                stage="merge",
                status="running",
                pid=None,
                started_at=now,
                binding_key=_binding_storage_key(binding),
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
            workspace_path = await self._acquire_merge_workspace(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                run_id=run_id,
            )
            if workspace_path is None:
                return run_id

            # Sync workspace to the remote branch so the agent starts from a
            # clean state and any subsequent push succeeds (fast-forward).
            # Review-fix runs may have left behind local commits that diverge
            # from the remote; resetting here avoids a non-fast-forward failure.
            branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
            await self._sync_merge_workspace(
                workspace_path=workspace_path,
                branch=branch,
                issue=issue,
            )

            if await self._run_merge_agent_step(
                binding=binding,
                issue=issue,
                run_id=run_id,
                workspace_path=workspace_path,
                pr_url=pr_url,
            ):
                return run_id

            if await self._push_merge_branch(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                run_id=run_id,
                workspace_path=workspace_path,
                branch=branch,
                pr_number=pr_number,
            ):
                return run_id

            halted, premerge_view = await self._verify_premerge_head(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                run_id=run_id,
                approved_head_sha=approved_head_sha,
            )
            if halted:
                return run_id

            if await self._execute_pr_merge(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                run_id=run_id,
                premerge_view=premerge_view,
            ):
                return run_id

            await self._finalize_merge(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                run_id=run_id,
            )
            return run_id
        finally:
            if workspace_path is not None:
                self._workspace.release(binding, issue)
            self._dispatch_run_ids.pop(issue.id, None)

    async def _acquire_merge_workspace(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
        run_id: str,
    ) -> Path | None:
        """Acquire the per-issue workspace clone; mark needs-approval and return
        None on failure so the caller halts the run."""
        try:
            return await self._workspace.acquire(binding, issue)
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
            return None

    async def _sync_merge_workspace(
        self,
        *,
        workspace_path: Path,
        branch: str,
        issue: LinearIssue,
    ) -> None:
        """Reset the workspace to the remote branch; a failure is logged and
        tolerated (the merge proceeds anyway)."""
        try:
            github_token = await self._resolve_github_token()
            await _sync_workspace_to_remote(workspace_path, branch, github_token=github_token)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "workspace sync failed for merge %s, proceeding anyway: %s",
                issue.identifier,
                e,
            )

    async def _run_merge_agent_step(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        run_id: str,
        workspace_path: Path,
        pr_url: str,
    ) -> bool:
        """Run the merge agent, record its usage, and check the runner verdict.
        Returns True if the run halted (caller returns the run id)."""
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
            return True

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
            return True
        return False

    async def _push_merge_branch(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
        run_id: str,
        workspace_path: Path,
        branch: str,
        pr_number: int,
    ) -> bool:
        """Push the merge branch; mark needs-approval and return True (halt) on
        failure."""
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
            return True
        return False

    async def _verify_premerge_head(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        run_id: str,
        approved_head_sha: str,
    ) -> tuple[bool, dict[str, object] | None]:
        """Re-fetch the PR before merging and verify the pushed HEAD is the
        approved one (re-classifying review on drift) and conflict-free.

        Returns ``(halted, premerge_view)``. ``halted`` True means the caller
        returns the run id; ``premerge_view`` is the fetched view (None when the
        pre-check could not run), forwarded to the merge step for required-check
        recovery.
        """
        try:
            premerge_view = await (await self._gh_client()).pr_view(
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
                return True, None
            return False, None

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
                    "could not classify review for post-merge-agent HEAD %s#%d at %s: %s",
                    binding.github_repo,
                    pr_number,
                    premerge_head_sha[:12] or "(unknown)",
                    e,
                )
                verdict = None
            if verdict is None or verdict.kind is not VerdictKind.APPROVED:
                reason = f"merge-agent pushed unreviewed HEAD {premerge_head_sha or '(unknown)'}"
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
                return True, premerge_view

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
            return True, premerge_view

        return False, premerge_view

    async def _execute_pr_merge(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        run_id: str,
        premerge_view: dict[str, object] | None,
    ) -> bool:
        """Merge the PR, recovering from merge conflicts / required-check
        failures. Returns True if the run halted (caller returns the run id)."""
        try:
            await (await self._gh_client()).pr_merge(
                pr_number,
                strategy=binding.merge_strategy,
                auto=binding.allow_auto_merge,
                repo=binding.github_repo,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("merge failed for %s#%d: %s", binding.github_repo, pr_number, e)
            if _is_merge_conflict_error(e):
                try:
                    conflict_view = await (await self._gh_client()).pr_view(
                        pr_number,
                        repo=binding.github_repo,
                    )
                except Exception as view_error:  # noqa: BLE001
                    log.warning(
                        "could not refresh PR base after merge conflict for %s#%d: %s",
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
                return True
            required_view: dict[str, object] | None = None
            if (
                premerge_view is not None
                and isinstance(premerge_view, dict)
                and "statusCheckRollup" in premerge_view
            ):
                required_view = premerge_view
            else:
                try:
                    required_view = await (await self._gh_client()).pr_view(
                        pr_number,
                        repo=binding.github_repo,
                        include_status_checks=True,
                    )
                except Exception as view_error:  # noqa: BLE001
                    log.warning(
                        "could not refresh PR checks after merge failure for %s#%d: %s",
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
                    if dispatched or await db.operator_waits.get(self._conn, issue.id) is not None:
                        return True
            await self._mark_merge_needs_approval(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                run_id=run_id,
                reason=str(e),
                exc=e,
            )
            return True
        return False

    async def _finalize_merge(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        run_id: str,
    ) -> None:
        """Confirm the merge landed and close out the run; a verification error
        marks needs-approval."""
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
            return
        if not merged:
            await db.runs.update_status(
                self._conn,
                run_id,
                "completed",
                ended_at=self._now().isoformat(),
            )

    async def _mark_merge_done_if_merged(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        run_id: str,
    ) -> bool:
        view = await (await self._gh_client()).pr_view(pr_number, repo=binding.github_repo)
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
        await self._notify_attention(
            event=EVENT_PR_MERGED,
            issue_identifier=issue.identifier,
            issue_url=issue.url,
            dedupe_key=f"pr_merged:{issue.id}:{run_id}",
        )
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
                binding_key=_binding_storage_key(binding),
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
            await self._notify_attention(
                event=EVENT_OPERATOR_WAIT,
                issue_identifier=issue.identifier,
                issue_url=issue.url,
                dedupe_key=f"operator_wait:{run_id}",
                detail=reason,
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
        role = binding.resolved_role("implement", self.config.roles)
        command = build_merge_runner_command(
            role.agent,
            prompt,
            codex_model=role_codex_model(role),
            claude_model=role_claude_model(role),
            effort=role.effort,
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
            role=role,
            prior_total=prior_total,
        )
