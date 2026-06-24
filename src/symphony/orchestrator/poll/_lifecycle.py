"""`_LifecycleMixin` — the run-lifecycle domain of the poll loop (SYM-150).

Owns the path a run travels end-to-end: dispatch → implement → pre-push gates
(local review / verify) → publish → deliver. `Orchestrator` (in `__init__.py`)
inherits this mixin, which in turn extends `_OrchestratorBase` so the lifecycle
methods see all in-memory state + foundation methods for free.

Pure structural extraction: method bodies are byte-for-byte unchanged from the
pre-split `Orchestrator`.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

import aiosqlite

from ... import db
from ...config import RepoBinding
from ...github.client import GitHubError
from ...linear.client import LinearError
from ...linear.templates import (
    CommentVars,
    failed,
    run_started,
    stage_done,
    truncate_body,
)
from ...pipeline.cost_guard import UsageCostEstimator as _UsageCostEstimator
from ...pipeline.cost_guard import UsageDelta
from ...pipeline.local_review import LocalVerdict, StreamApiError
from ...pipeline.local_review_loop import LoopOutcome, LoopResult
from ...pipeline.local_review_session import run_local_review_session
from ...pipeline.state_machine import classify_implement_completion, on_runner_event
from ...pipeline.verify import VerifyResult, run_verify_session
from ...tokens import effective_tokens
from ...tracker import Issue as LinearIssue

# Cross-cutting free helpers that still live in `poll/__init__.py` (SYM-143 left
# them there because every domain — not just lifecycle — calls them). `__init__`
# imports this mixin only after defining them, so the package namespace is fully
# populated by the time this import runs.
from . import (  # noqa: E402  (intentional: depends on `__init__` being populated)
    _add_run_usage,
    _binding_storage_key,
    _local_review_failure_log,
    _local_review_infra_failed,
    _local_review_needs_approval,
    _local_review_permits_remote,
    _local_review_status_from_result,
    _local_review_termination_reason,
    _parse_local_review_model_usage,
    _read_run_final_message,
    _read_run_stream_api_error_obj,
    _record_run_model_usage,
    _termination_kwargs,
)
from ._base import _OrchestratorBase, _PendingDelivery
from ._git import (
    _branch_ahead_of_base,
    _git_status_short,
    _workspace_commits_ahead,
    _workspace_diff_size,
    _workspace_dirty_files,
    _workspace_head_sha,
    _workspace_scrub,
)
from ._helpers import build_pr_title, pr_number_from_url

log = logging.getLogger(__name__)


class _LifecycleMixin(_OrchestratorBase):
    """Run-lifecycle domain methods; `Orchestrator` mixes this in."""

    if TYPE_CHECKING:
        # Sibling-domain methods provided by `Orchestrator` (or other domains
        # not yet extracted). Declared here so the mixin type-checks in
        # isolation under `mypy --strict`; the real implementations live on
        # `Orchestrator` and override these at runtime.
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
        ) -> None: ...

        async def _clear_operator_wait(self, issue_id: str, run_id: str) -> None: ...

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
        ) -> bool: ...

        async def _fail_review_run(
            self,
            *,
            run: db.runs.Run,
            binding: RepoBinding,
            issue: LinearIssue,
            error: str,
            last_log: str,
            auto_retry: bool = True,
            operator_wait: bool = False,
        ) -> None: ...

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

        def _fix_claude_model(self, binding: RepoBinding) -> str | None: ...

        async def _maybe_park_for_token_budget(
            self, issue_id: str, run_id: str, binding: RepoBinding
        ) -> bool: ...

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
        ) -> bool: ...

        async def _move_issue_to_local_code_review_state(
            self, *, binding: RepoBinding, issue: LinearIssue
        ) -> None: ...

        async def _park_deliver_failed(
            self,
            reason: str,
            *,
            ctx: _PendingDelivery,
            exc: BaseException | str | None = None,
        ) -> None: ...

        async def _park_local_only_review_needs_approval(
            self,
            *,
            run: db.runs.Run,
            binding: RepoBinding,
            issue: LinearIssue,
            pr_url: str,
            result: LoopResult | None,
            operator_wait: bool = False,
        ) -> None: ...

        async def _run_agent(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            storage_issue_id: str | None = None,
            run_id: str,
            workspace_path: Path,
            prior_total: float,
        ) -> tuple[UsageDelta, str, int | None]: ...

        async def _run_dirty_tree_fix_turn(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            storage_issue_id: str,
            workspace_path: Path,
            parent_run_id: str,
            dirty_files: list[str],
        ) -> None: ...

        async def _start_review_stage(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            storage_issue_id: str | None = None,
            pr_url: str,
            post_codex_review: bool = True,
        ) -> db.runs.Run: ...

        async def _track_delivery_handoff_recovery_wait(
            self, ctx: _PendingDelivery
        ) -> None: ...

        async def _track_implement_failed_wait(
            self, issue_id: str, run_id: str, binding: RepoBinding
        ) -> None: ...

    async def _dispatch_one(
        self, binding: RepoBinding, issue: LinearIssue
    ) -> str | None:
        """Drive one issue end-to-end through the Implement stage.

        Persists first, announces second: if the host crashed after
        `post_comment` succeeded but before the row was written, the next
        poll would post a duplicate 🚀. Inserting first closes that
        window. The insert is atomic against a racing dispatch.
        """
        run_id = str(uuid.uuid4())
        now = self._now().isoformat()

        issue_id = await db.issues.upsert(
            self._conn,
            id=issue.id,
            provider=binding.tracker_provider,
            site=binding.tracker_site,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        inserted = await db.runs.create_if_not_dispatched(
            self._conn,
            id=run_id,
            issue_id=issue_id,
            stage="implement",
            status="running",
            pid=None,
            started_at=now,
        )
        if not inserted:
            log.info(
                "skipping dispatch for %s: already running or completed",
                issue.identifier,
            )
            return None
        self._dispatch_run_ids[issue_id] = run_id

        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning("could not load states for %s: %s", binding.linear_team_key, e)
            await self._fail_run(run_id, f"team_states failed: {e}")
            return run_id

        ready_id = states.get(binding.linear_states.ready)
        in_progress_id = states.get(binding.linear_states.in_progress)
        missing_state = next(
            (
                state
                for state, state_id in (
                    (binding.linear_states.ready, ready_id),
                    (binding.linear_states.in_progress, in_progress_id),
                )
                if state_id is None
            ),
            None,
        )
        if missing_state is not None:
            log.warning(
                "could not dispatch %s: missing Linear state %r for team %s",
                issue.identifier,
                missing_state,
                binding.linear_team_key,
            )
            await self._fail_run(
                run_id,
                f"missing Linear state: {missing_state}",
            )
            return run_id
        assert ready_id is not None
        assert in_progress_id is not None

        log.info(
            "dispatching %s (%s) -> %s [run_id=%s]",
            issue.identifier,
            issue.title,
            binding.github_repo,
            run_id,
        )

        # 1. 🚀 "starting" Linear comment.
        tracker = self.tracker(binding)
        starting = run_started(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
            )
        )
        try:
            await tracker.post_comment(issue.id, truncate_body(starting))
        except LinearError as e:
            log.warning("could not announce dispatch on %s: %s", issue.identifier, e)
            await db.runs.update_status(
                self._conn,
                run_id,
                "failed",
                ended_at=self._now().isoformat(),
                **_termination_kwargs(
                    status="failed",
                    reason=f"post_comment failed: {e}",
                ),
            )
            return run_id

        # 2. Move the Linear issue to In Progress.
        try:
            await tracker.move_issue(issue.id, in_progress_id)
        except LinearError as e:
            log.warning(
                "could not move %s to %s: %s",
                issue.identifier,
                binding.linear_states.in_progress,
                e,
            )
            await self._fail_run(run_id, f"move_issue failed: {e}")
            return run_id
        self._runs_moved_to_in_progress.add(run_id)

        # 3. Acquire a per-issue workspace clone.
        try:
            workspace_path = await self._workspace.acquire(binding, issue)
        except Exception as e:  # noqa: BLE001 — surface as failed run
            log.exception("workspace acquire failed for %s", issue.identifier)
            await self._fail_run_and_reset_issue(
                run_id,
                f"workspace acquire failed: {e}",
                issue=issue,
                storage_issue_id=issue_id,
                rollback_state_id=issue.state_id,
                binding=binding,
                exc=e,
            )
            return run_id

        workspace_released = False

        def release_workspace() -> None:
            nonlocal workspace_released
            if not workspace_released:
                self._workspace.release(binding, issue)
                workspace_released = True

        # 3.5. Resolve the delivery base once; both the branch-already-ahead
        # short-circuit and ensure_pr use it.
        release_on_setup_failure = False
        try:
            base_branch = await self._resolve_base_branch(binding)

            # Branch-already-ahead short-circuit: when HEAD already contains
            # commits over the delivery base, skip the implementer and its
            # completion gate and go straight to the agent-free publish path.
            # The pre-push gates (local-review / verify / dirty-tree) still
            # run against the reused workspace so the resume records the
            # verify SHA, guards the dirty tree, and feeds publish a real
            # local-review verdict — not the `None` its handoff would mis-read
            # as "did not approve". A pending operator `$retry` handoff
            # (`_implement_handoffs`) likewise forces the agent path so the
            # handoff is consumed (poll.py:_run_agent) instead of silently
            # dropped. Previous non-publish implement failures are unsafe to
            # publish blindly because the agent may have left partial commits.
            pending_handoff = self._implement_handoffs.get(issue_id) is not None
            previous_terminal_kind = await self._previous_implement_terminal_kind(
                issue_id=issue_id,
                current_run_id=run_id,
            )
            # Delivery failures (publish/deliver) passed the completion gate
            # AND local review, so a resume skips the agent and re-runs only
            # the agent-free publish path.
            delivery_resume_kinds = {
                db.runs.PUBLISH_FAILED_KIND,
                db.operator_waits.KIND_DELIVER_FAILED,
            }
            # A local-review *infra* failure (no verdict / the session raised)
            # also passed the completion gate, so the commits are complete —
            # but local review never finished, so the resume re-runs the gates
            # *with fixes enabled* (a re-review may now request changes). Only
            # the explicit kinds qualify: they are stamped solely for
            # reviewer-never-verdicted failures, never for fix-run failures
            # (which may have left partial commits and must re-run the agent).
            # LOCAL_REVIEW_TRANSIENT_RETRY_KIND is the backoff variant: the
            # implement succeeded and the local-review phase got a transient 500,
            # so the re-dispatch must also skip the implementer and re-run only
            # the pre-push gates (with fixes allowed) rather than treating it
            # like a plain implement failure that needs a full re-run.
            resume_after_local_review = previous_terminal_kind in {
                db.runs.LOCAL_REVIEW_INFRA_FAILED_KIND,
                db.runs.LOCAL_REVIEW_TRANSIENT_RETRY_KIND,
            }
            previous_requires_agent = (
                previous_terminal_kind is not None
                and previous_terminal_kind not in delivery_resume_kinds
                and not resume_after_local_review
            )
            branch_ahead = await _branch_ahead_of_base(workspace_path, base_branch)
        except Exception as e:  # noqa: BLE001 — surface as failed run
            release_on_setup_failure = True
            log.exception("implement dispatch setup failed for %s", issue.identifier)
            await self._fail_run_and_reset_issue(
                run_id,
                f"implement dispatch setup failed: {e}",
                issue=issue,
                storage_issue_id=issue_id,
                rollback_state_id=issue.state_id,
                binding=binding,
                exc=e,
            )
            return run_id
        finally:
            if release_on_setup_failure:
                release_workspace()

        cumulative_usage: UsageDelta
        local_review_result: LoopResult | None
        # A prior delivery-failure marker proves only that an earlier checkout
        # reached delivery. The current checkout still needs its own proof of
        # deliverable commits before publish can be resumed; when the base is
        # unresolved, _branch_ahead_of_base deliberately cannot supply that
        # proof, so the run falls back to the agent/completion-gate path.
        short_circuit = (
            branch_ahead
            and not pending_handoff
            and not previous_requires_agent
        )
        if short_circuit:
            log.info(
                "branch for %s already ahead of %s; skipping agent, "
                "proceeding to publish",
                issue.identifier,
                base_branch,
            )
            cumulative_usage = UsageDelta()
            # Release before the gates run (mirroring the implement path,
            # poll.py:_run_implement_phase): the gates operate on the released
            # workspace, and releasing first means a gate raising can't leak
            # the workspace into WorkspaceManager._in_use forever.
            release_workspace()
            try:
                proceed, local_review_result = await self._run_prepush_gates(
                    binding=binding,
                    issue=issue,
                    storage_issue_id=issue_id,
                    run_id=run_id,
                    workspace_path=workspace_path,
                    # Delivery resumes already passed review → fail closed (no
                    # code-mutating fix turn). A local-review-infra resume never
                    # got a verdict, so it must allow the normal fix loop;
                    # otherwise a re-review requesting changes would park as
                    # FIX_RUN_FAILED instead of being fixed.
                    allow_fixes=resume_after_local_review,
                )
            except Exception as e:  # noqa: BLE001 — fail closed before publish
                log.exception(
                    "pre-push gate failed during publish resume for %s",
                    issue.identifier,
                )
                await self._fail_run_and_reset_issue(
                    run_id,
                    f"pre-push gate failed during publish resume: {e}",
                    issue=issue,
                    storage_issue_id=issue_id,
                    rollback_state_id=issue.state_id,
                    binding=binding,
                    exc=e,
                )
                return run_id
            if not proceed:
                # A gate halted the run; state is already recorded.
                return run_id
        else:
            phase = await self._run_implement_phase(
                binding=binding,
                issue=issue,
                storage_issue_id=issue_id,
                run_id=run_id,
                workspace_path=workspace_path,
                base_branch=base_branch,
            )
            if phase is None:
                # The agent step halted (failed / blocked / parked); the run
                # state is already recorded. Never reaches publish.
                return run_id
            cumulative_usage, local_review_result = phase

        return await self._publish_stage(
            binding=binding,
            issue=issue,
            storage_issue_id=issue_id,
            run_id=run_id,
            workspace_path=workspace_path,
            base_branch=base_branch,
            cumulative_usage=cumulative_usage,
            local_review_result=local_review_result,
        )

    async def _previous_implement_terminal_kind(
        self, *, issue_id: str, current_run_id: str
    ) -> str | None:
        """Return the prior failed implement terminal kind, if any."""

        history = await db.runs.history_for_issue(self._conn, issue_id)
        previous = next(
            (
                run
                for run in reversed(history)
                if run.stage == "implement" and run.id != current_run_id
            ),
            None,
        )
        if previous is None:
            return None
        if previous.status not in db.runs.TERMINAL_NON_SUCCESS_STATUSES:
            return None
        return previous.termination_kind

    async def _resolve_base_branch(self, binding: RepoBinding) -> str | None:
        """The PR base: the binding's configured base, else the repo default.

        On a default-branch lookup error, fall back to gh's own default (None)
        rather than failing the run.
        """
        if binding.base_branch is not None:
            return binding.base_branch
        try:
            return await self._gh.repo_default_branch(binding.github_repo)
        except GitHubError as e:
            log.warning(
                "repo_default_branch failed for %s; falling back to gh default: %s",
                binding.github_repo,
                e,
            )
            return None

    async def _run_implement_phase(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str,
        run_id: str,
        workspace_path: Path,
        base_branch: str | None = None,
    ) -> tuple[UsageDelta, LoopResult | None] | None:
        """The agent step: run the implementer, apply the completion gate, then
        the local-review / verify / dirty-tree pre-push gates.

        Returns ``(cumulative_usage, local_review_result)`` when the branch is
        ready to publish, or ``None`` when the run was halted (failed, blocked,
        or parked on an operator wait) and the caller should return. The
        completion gate lives here and only here — it never guards delivery.
        """
        issue_id = storage_issue_id
        prior_total = await db.runs.cost_for_issue(self._conn, issue_id)
        # Branch base before the agent runs, so the completion gate can tell
        # whether the run actually advanced HEAD (≥1 new commit).
        head_before = await _workspace_head_sha(workspace_path)

        try:
            cumulative_usage, final_kind, final_returncode = (
                await self._run_agent(
                    binding=binding,
                    issue=issue,
                    storage_issue_id=issue_id,
                    run_id=run_id,
                    workspace_path=workspace_path,
                    prior_total=prior_total,
                )
            )
        except Exception as e:  # noqa: BLE001 — surface as failed run
            log.exception("agent execution failed for %s", issue.identifier)
            await self._fail_run_and_reset_issue(
                run_id,
                f"agent execution failed: {e}",
                issue=issue,
                storage_issue_id=issue_id,
                rollback_state_id=issue.state_id,
                binding=binding,
                exc=e,
            )
            return None
        finally:
            self._workspace.release(binding, issue)

        # 4. Persist accumulated usage.
        await _add_run_usage(self._conn, run_id, cumulative_usage)

        transition = on_runner_event(
            stage="implement",
            event_kind=final_kind,
            returncode=final_returncode,
        )

        if transition.next_run_status != "completed":
            log.info(
                "implement run %s ended in %s (rc=%s) -> failed",
                run_id,
                final_kind,
                final_returncode,
            )
            await self._fail_run_and_reset_issue(
                run_id,
                f"runner ended with {final_kind}",
                issue=issue,
                storage_issue_id=issue_id,
                rollback_state_id=issue.state_id,
                binding=binding,
                final_kind=final_kind,
                returncode=final_returncode,
            )
            return None

        # 4.25. Completion gate. rc=0 alone is not "done": an agent that ends
        # its turn politely blocked on a human action (MCH-14) also exits 0.
        # Require the SYMPHONY_DONE / SYMPHONY_BLOCKED marker plus a HEAD
        # advance, and fall back to a cheap classifier of the final message.
        head_after = await _workspace_head_sha(workspace_path)
        head_advanced = bool(head_after) and head_after != head_before
        log_path = self.config.log_root / f"{run_id}.log"
        final_message = _read_run_final_message(log_path, agent=binding.agent)
        completion = classify_implement_completion(
            final_message=final_message,
            head_advanced=head_advanced,
        )
        if completion.outcome == "blocked":
            reason = (
                completion.blocked_reason
                or "agent blocked on a human action but gave no reason"
            )
            log.info("implement run %s classified blocked: %s", run_id, reason)
            # Captured verbatim on the run record (termination_kind="blocked",
            # termination_detail=<reason>) and parked on an IMPLEMENT_BLOCKED
            # operator wait for human handoff. The gate returns before any
            # push/local-review, so a blocked run never opens a PR, and the
            # workspace (with any uncommitted work) is left untouched for the
            # `$retry` resume.
            await self._block_implement_run(
                run_id,
                reason,
                issue=issue,
                storage_issue_id=issue_id,
                rollback_state_id=issue.state_id,
                binding=binding,
                returncode=final_returncode,
            )
            return None
        if completion.outcome == "already_satisfied":
            # The agent verified the scope was already delivered elsewhere and
            # made no commit (HEAD did not advance). This is a no-op done, not a
            # failure: close the issue as Done referencing the delivering commit
            # — but only after verifying the tree is clean and that commit is
            # real and landed in the base branch, so neither uncommitted work
            # nor a bogus already-done claim can auto-close an issue. An
            # unverifiable claim falls back to the failed path below.
            closed = await self._complete_already_satisfied_run(
                run_id,
                completion.already_satisfied_ref,
                issue=issue,
                storage_issue_id=issue_id,
                rollback_state_id=issue.state_id,
                binding=binding,
                workspace_path=workspace_path,
                base_branch=base_branch,
                returncode=final_returncode,
            )
            if closed:
                return None
            # Verification failed — fall through to the failed path so the
            # existing no-op guard still parks the run on an operator.
        if completion.outcome != "completed":
            if completion.outcome == "already_satisfied":
                reason = (
                    "implement run claimed SYMPHONY_ALREADY_DONE but could not "
                    "be auto-closed: the working tree was dirty, the delivering "
                    "commit was not verifiable as landed in the base branch, or "
                    "the move to Done failed "
                    f"(ref: {completion.already_satisfied_ref or '(none)'})"
                )
            else:
                reason = (
                    "implement run exited 0 but did not satisfy the completion "
                    "contract: HEAD did not advance and no SYMPHONY_DONE marker "
                    "could be confirmed as done"
                )
                # A run that ended only on a provider API error (e.g. 500)
                # exits 0 with no marker — surface the real "API Error: …"
                # cause instead of the generic completion-contract text, and
                # (when transient) requeue with backoff instead of escalating.
                api_error = _read_run_stream_api_error_obj(log_path)
                if api_error is not None:
                    reason = api_error.message
                    if await self._maybe_requeue_transient_agent_failure(
                        run_id=run_id,
                        binding=binding,
                        issue=issue,
                        storage_issue_id=issue_id,
                        api_error=api_error,
                        reason=reason,
                        returncode=final_returncode,
                        workspace_path=workspace_path,
                    ):
                        return None
            log.info("implement run %s -> failed (completion gate): %s", run_id, reason)
            await self._fail_run_and_reset_issue(
                run_id,
                reason,
                issue=issue,
                storage_issue_id=issue_id,
                rollback_state_id=issue.state_id,
                binding=binding,
                returncode=final_returncode,
            )
            return None

        proceed, local_review_result = await self._run_prepush_gates(
            binding=binding,
            issue=issue,
            storage_issue_id=issue_id,
            run_id=run_id,
            workspace_path=workspace_path,
        )
        if not proceed:
            # A gate halted the run (failed / blocked / parked); state is
            # already recorded. Never reaches publish.
            return None

        return cumulative_usage, local_review_result

    async def _run_prepush_gates(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str,
        run_id: str,
        workspace_path: Path,
        allow_fixes: bool = True,
    ) -> tuple[bool, LoopResult | None]:
        """Pre-push validation gates: local-review, verify, dirty-tree.

        Runs after the agent step — or, on the branch-already-ahead
        short-circuit, in place of it — and before the agent-free publish
        stage, so what gets pushed is what was reviewed and verified. The
        branch-ahead short-circuit reuses an existing workspace and calls with
        ``allow_fixes=False``: validators may run, but any red/dirty result
        fails closed instead of spawning a code-mutating fix turn.

        Returns ``(proceed, local_review_result)``: ``proceed`` is False when
        a gate halted the run (failed / blocked / parked) and the caller
        should stop — the run state is already recorded.
        """
        issue_id = storage_issue_id

        # Soft per-issue token-budget gate at the pre-push fix boundary
        # (next local-review fix / verify-gate fix). The implement agent step
        # has finished; if the issue has already crossed its budget, park
        # rather than spawning further fix turns. Evaluated only when fixes
        # could be dispatched (`allow_fixes`), never mid-run.
        if allow_fixes and await self._maybe_park_for_token_budget(
            issue_id, run_id, binding
        ):
            return False, None

        # 4.5. Local-review pre-flight. When `binding.local_review` is set,
        # run the reviewer in-workspace before pushing. This shortens the
        # iteration loop dramatically: the slow part of the existing flow is
        # round-tripping each fix through GitHub for the remote `@codex` bot.
        # See `docs/local-review-flow.md`.
        local_review_result: LoopResult | None = None
        if binding.resolved_local_review():
            local_review_result = await self._run_local_review_phase(
                binding=binding,
                issue=issue,
                storage_issue_id=issue_id,
                workspace_path=workspace_path,
                parent_run_id=run_id,
                allow_fixes=allow_fixes,
            )
            if _local_review_infra_failed(local_review_result):
                # A transient provider API error in the reviewer/fix turn left
                # the agent's commits intact and did no further work — requeue
                # with backoff instead of escalating, until the budget is spent.
                # LOCAL_REVIEW_TRANSIENT_RETRY_KIND is used (not TRANSIENT_API_RETRY_KIND)
                # so the re-dispatch resume logic recognises that the implement
                # succeeded and short-circuits to the pre-push gates instead of
                # re-running the implementer on an already-complete branch.
                _lr_api_error = (
                    local_review_result.api_error
                    if local_review_result is not None
                    else None
                )
                if await self._maybe_requeue_transient_agent_failure(
                    run_id=run_id,
                    binding=binding,
                    issue=issue,
                    storage_issue_id=issue_id,
                    api_error=_lr_api_error,
                    reason=_local_review_termination_reason(local_review_result),
                    termination_kind=db.runs.LOCAL_REVIEW_TRANSIENT_RETRY_KIND,
                    workspace_path=workspace_path,
                ):
                    return False, local_review_result
                # Budget exhausted or non-transient error. When the api_error was
                # transient and the workspace is still clean, the fixer did no work
                # across all retries — stamp LOCAL_REVIEW_INFRA_FAILED_KIND so a
                # $retry can resume agent-free from the pre-push gates.
                _transient_clean = (
                    _lr_api_error is not None
                    and _lr_api_error.transient
                    and not await _git_status_short(workspace_path)
                )
                await self._block_local_only_review_infra_failure(
                    binding=binding,
                    issue=issue,
                    storage_issue_id=issue_id,
                    run_id=run_id,
                    result=local_review_result,
                    force_local_review_resume=_transient_clean,
                )
                return False, local_review_result

        # 4.7. Verify gate. When `binding.verify_cmd` is set, run it after
        # the last code-mutating stage (post local-review fixes) and before
        # push, so what's verified is what gets pushed. Red gets one
        # implementer fix turn, then a re-run; still red fails closed:
        # no push, no PR, operator wait.
        if binding.verify_cmd:
            verify_result = await self._run_verify_phase(
                binding=binding,
                issue=issue,
                storage_issue_id=issue_id,
                workspace_path=workspace_path,
                parent_run_id=run_id,
                allow_fixes=allow_fixes,
            )
            if not verify_result.ok:
                # Verify-gate and dirty-tree fix failures are intentionally not
                # routed through _maybe_requeue_transient_agent_failure: VerifyResult
                # does not carry an api_error field (the fix turn's transient signal
                # is not surfaced), and dirty-tree fix failures are best-effort
                # (any failure just leaves the tree dirty and the re-check fails
                # closed). Both are treated as non-transient, fail-closed operator
                # waits. Wiring transient retry here would require plumbing api_error
                # through run_verify_session / _run_dirty_tree_fix_turn — out of scope.
                await self._block_verify_failure(
                    binding=binding,
                    issue=issue,
                    storage_issue_id=issue_id,
                    run_id=run_id,
                    result=verify_result,
                )
                return False, local_review_result
            # SYM-108: record the green gate against the exact head it
            # verified so the merge gate can treat a no-CI repo as mergeable
            # only for this SHA. A later HEAD-advancing fix turn (e.g. the
            # dirty-tree gate) won't match, falling back to an operator wait.
            verified_head = await _workspace_head_sha(workspace_path)
            if verified_head:
                await db.issue_prs.mark_verify_passed(
                    self._conn,
                    issue_id=issue_id,
                    github_repo=binding.github_repo,
                    head_sha=verified_head,
                    marked_at=self._now().isoformat(),
                )

        # 4.8. Pre-push dirty-tree gate. Pushing only commits means any
        # uncommitted work silently vanishes on workspace cleanup (MCH-14).
        # Runs after the verify gate so leftovers from the verify fix turn
        # are caught too. Normal implement path: one fix turn, re-check, then
        # fail closed. Publish-resume path: no fix turn; fail closed directly.
        dirty_files = await _workspace_dirty_files(workspace_path)
        if dirty_files and allow_fixes:
            log.warning(
                "dirty working tree before push for %s (%d entries); "
                "dispatching one fix turn",
                issue.identifier,
                len(dirty_files),
            )
            await self._run_dirty_tree_fix_turn(
                binding=binding,
                issue=issue,
                storage_issue_id=issue_id,
                workspace_path=workspace_path,
                parent_run_id=run_id,
                dirty_files=dirty_files,
            )
            dirty_files = await _workspace_dirty_files(workspace_path)
        if dirty_files:
            listing = "\n".join(f"- `{line}`" for line in dirty_files)
            reason = (
                "working tree still dirty after one fix turn; not pushing."
                if allow_fixes
                else "working tree dirty during publish resume; not pushing."
            )
            await self._fail_run_and_reset_issue(
                run_id,
                f"{reason} Uncommitted files:\n{listing}",
                issue=issue,
                storage_issue_id=issue_id,
                rollback_state_id=issue.state_id,
                binding=binding,
            )
            return False, local_review_result

        return True, local_review_result

    async def _publish_stage(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str,
        run_id: str,
        workspace_path: Path,
        base_branch: str | None,
        cumulative_usage: UsageDelta,
        local_review_result: LoopResult | None,
    ) -> str:
        """Delivery: push + ensure_pr + review/merge handoff.

        Agent-free and idempotent — safe to (re)run on a branch that is already
        pushed / already has a PR: the push fast-forwards to a no-op and
        ``ensure_pr`` adopts the existing PR. The completion gate never guards
        this step.
        """
        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
        return await self._deliver_implement_run(
            ctx=_PendingDelivery(
                binding=binding,
                issue=issue,
                storage_issue_id=storage_issue_id,
                run_id=run_id,
                workspace_path=workspace_path,
                branch=branch,
                cumulative_usage=cumulative_usage,
                local_review_result=local_review_result,
            ),
            base_branch=base_branch,
        )

    async def _delivery_handoff_started(
        self, *, ctx: _PendingDelivery, pr_url: str
    ) -> bool:
        """True once delivery reached durable PR/review handoff metadata."""
        if await db.runs.has_live_stage(
            self._conn, ctx.storage_issue_id, stage="review"
        ):
            return True

        pr_number = pr_number_from_url(pr_url)
        state = await db.review_state.get(self._conn, ctx.storage_issue_id)
        if state.github_repo == ctx.binding.github_repo:
            if pr_number is not None and state.pr_number == pr_number:
                return True
            if state.pr_url and state.pr_url == pr_url:
                return True

        issue_pr = await db.issue_prs.get(
            self._conn,
            issue_id=ctx.storage_issue_id,
            github_repo=ctx.binding.github_repo,
        )
        if issue_pr is None:
            return False
        if pr_number is not None and issue_pr.pr_number == pr_number:
            return True
        return issue_pr.pr_url == pr_url

    async def _deliver_implement_run(
        self, *, ctx: _PendingDelivery, base_branch: str | None = None
    ) -> str:
        """Push + open PR + hand off to review/merge for a completed implement.

        Re-entrant: invoked once at the end of the implement dispatch and again
        on a `$retry` of a `deliver_failed` wait. Never re-dispatches the agent
        and never runs the completion gate. Push / `ensure_pr` failures park a
        `deliver_failed` operator wait via `_park_deliver_failed`.
        """
        binding = ctx.binding
        issue = ctx.issue
        run_id = ctx.run_id
        workspace_path = ctx.workspace_path
        branch = ctx.branch
        cumulative_usage = ctx.cumulative_usage
        tracker = self.tracker(binding)

        if base_branch is None:
            base_branch = await self._resolve_base_branch(binding)

        # On a reconstructed resume the workspace was re-acquired after the
        # daemon restart. In-memory `$retry` resumes also re-acquire because
        # the original implement dispatch already released the workspace. On a
        # *push*-failure the commits lived only in that workspace; if it was
        # swept past its TTL, `acquire()` may have re-cloned an empty branch
        # from origin (the publish step is the very one that failed). Refuse to
        # push / open an empty no-op PR with the work silently lost; re-park
        # instead.
        if ctx.reconstructed or ctx.retry_workspace_acquired:
            if base_branch is None:
                msg = (
                    f"could not resolve base branch for $retry of "
                    f"{branch}; refusing to deliver without proving branch work"
                )
                log.warning("%s for %s", msg, issue.identifier)
                await self._park_deliver_failed(msg, ctx=ctx)
                return run_id
            ahead = await _workspace_commits_ahead(workspace_path, base_branch)
            if ahead is None:
                msg = (
                    f"could not compare $retry branch {branch} "
                    f"against {base_branch}; refusing to deliver without "
                    "proving branch work"
                )
                log.warning("%s for %s", msg, issue.identifier)
                await self._park_deliver_failed(msg, ctx=ctx)
                return run_id
            if ahead == 0:
                msg = (
                    f"workspace swept before $retry: branch {branch} carries no "
                    f"commits over {base_branch}; refusing to deliver an empty PR"
                )
                log.warning("%s for %s", msg, issue.identifier)
                await self._park_deliver_failed(msg, ctx=ctx)
                return run_id

        # 5. Push branch, open PR, post stage-transition comment.
        try:
            await self._push_fn(workspace_path, branch)
        except Exception as e:  # noqa: BLE001
            log.warning("git push failed for %s: %s", issue.identifier, e)
            await self._park_deliver_failed(f"push failed: {e}", ctx=ctx, exc=e)
            return run_id

        pr_url: str = ""
        try:
            pr_url = await self._gh.ensure_pr(
                title=build_pr_title(issue),
                body="",
                base=base_branch,
                head=branch,
                repo=binding.github_repo,
                linear_url=issue.url,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("pr_create failed for %s: %s", issue.identifier, e)
            await self._park_deliver_failed(f"pr_create failed: {e}", ctx=ctx, exc=e)
            return run_id

        # First handoff vs. a `$retry` resume. Review-enabled handoff writes a
        # live review run; no-review handoff writes only review_state/issue_prs.
        # The Linear stage_done announcement has its own durable marker because
        # a handoff can fail before either PR/review metadata row exists.
        first_handoff = not await self._delivery_handoff_started(
            ctx=ctx, pr_url=pr_url
        )
        stage_done_announced = await db.runs.has_stage_done_announced(
            self._conn, run_id
        )

        # The stage-transition comment is first-delivery only. The completed
        # write must happen before the first handoff so the review run can pass
        # its active-run guard. Persist a recovery wait before that completed
        # write so a daemon crash cannot leave a terminal implement run with no
        # handoff metadata and no `$retry` target. Successful handoff clears the
        # temporary wait below.
        if first_handoff:
            if not stage_done_announced:
                await db.runs.mark_stage_done_announced(
                    self._conn,
                    run_id,
                    announced_at=self._now().isoformat(),
                )
                try:
                    next_stage = (
                        "merge"
                        if (
                            not binding.resolved_local_review()
                            and not binding.resolved_remote_review()
                        )
                        else "review"
                    )
                    done_body = stage_done(
                        CommentVars(
                            stage="implement",
                            next_stage=next_stage,
                            repo=binding.github_repo,
                            issue=0,
                            pr_url=pr_url or "(no PR)",
                            run_id=run_id,
                            input_tokens=cumulative_usage.input_tokens,
                            output_tokens=cumulative_usage.output_tokens,
                            cache_write_tokens=cumulative_usage.cache_write_tokens,
                            cache_read_tokens=cumulative_usage.cache_read_tokens,
                        )
                    )
                    await tracker.post_comment(issue.id, truncate_body(done_body))
                except LinearError as e:
                    log.warning(
                        "stage_done comment failed on %s: %s", issue.identifier, e
                    )

            await self._track_delivery_handoff_recovery_wait(ctx)
            await db.runs.update_status(
                self._conn,
                run_id,
                "completed",
                ended_at=self._now().isoformat(),
            )

        # Past the PR open the handoff (review-state writes, review-stage
        # start, PR summary) is still post-completion delivery: an unexpected
        # DB / Linear failure here must park `deliver_failed` for a `$retry`
        # rather than propagate and leave a completed run with no review
        # started and no operator wait.
        try:
            delivered_run_id = await self._deliver_review_handoff(
                ctx=ctx, pr_url=pr_url
            )
        except Exception as e:  # noqa: BLE001
            log.warning("delivery handoff failed for %s: %s", issue.identifier, e)
            await self._park_deliver_failed(f"handoff failed: {e}", ctx=ctx, exc=e)
            return run_id
        if first_handoff:
            await self._clear_operator_wait(ctx.storage_issue_id, run_id)
            # The run now continues into the review/merge stage, so it stays the
            # active dispatch run for this issue. `_clear_operator_wait` drops the
            # `_dispatch_run_ids` entry (correct when tearing down a parked wait,
            # not here on a successful first handoff), so restore it.
            self._dispatch_run_ids[ctx.storage_issue_id] = run_id
        if not first_handoff:
            await db.runs.update_status(
                self._conn,
                run_id,
                "completed",
                ended_at=self._now().isoformat(),
            )
        return delivered_run_id

    async def _deliver_review_handoff(
        self, *, ctx: _PendingDelivery, pr_url: str
    ) -> str:
        """Post-PR handoff: register the merge candidate (no-review) or start
        the Review stage. Raises on unexpected DB/Linear faults so the caller
        can park `deliver_failed`."""
        binding = ctx.binding
        issue = ctx.issue
        issue_id = ctx.storage_issue_id
        run_id = ctx.run_id
        local_review_result = ctx.local_review_result

        if (
            not binding.resolved_local_review()
            and not binding.resolved_remote_review()
        ):
            pr_number = pr_number_from_url(pr_url)
            await db.review_state.begin_review(
                self._conn,
                issue_id,
                pr_number=pr_number,
                pr_url=pr_url,
                github_repo=binding.github_repo,
                issue_label=binding.issue_label,
            )
            if pr_number is None:
                log.warning(
                    "could not parse PR number from %r for no-review %s",
                    pr_url,
                    issue.identifier,
                )
            else:
                await db.issue_prs.upsert(
                    self._conn,
                    issue_id=issue_id,
                    github_repo=binding.github_repo,
                    binding_key=_binding_storage_key(binding),
                    pr_number=pr_number,
                    pr_url=pr_url,
                    created_at=self._now().isoformat(),
                    review_bypassed=True,
                )
            return run_id

        # 6. Start the Review stage. A local loop is a hard pre-PR gate:
        #    true/true runs remote review after local APPROVED, or after an
        #    operator explicitly bypasses the local gate with $skip-local-review.
        #    Other local terminals are parked below instead of falling through
        #    to the GitHub bot.
        local_review_blocks_remote = (
            binding.resolved_local_review()
            and not _local_review_permits_remote(local_review_result)
        )
        post_codex_review = (
            binding.resolved_remote_review() and not local_review_blocks_remote
        )
        review_run = await self._start_review_stage(
            binding=binding,
            issue=issue,
            storage_issue_id=issue_id,
            pr_url=pr_url,
            post_codex_review=post_codex_review,
        )
        if (
            binding.resolved_local_review()
            and _local_review_needs_approval(local_review_result)
        ):
            await self._park_local_only_review_needs_approval(
                run=review_run,
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                result=local_review_result,
                operator_wait=binding.resolved_remote_review(),
            )
            return run_id
        if (
            binding.resolved_local_review()
            and not (
                binding.resolved_remote_review()
                and _local_review_permits_remote(local_review_result)
            )
            and (
                local_review_result is None
                or local_review_result.outcome != LoopOutcome.APPROVED
            )
        ):
            await self._fail_review_run(
                run=review_run,
                binding=binding,
                issue=issue,
                error=(
                    "local-only review did not approve: "
                    f"{_local_review_termination_reason(local_review_result)}"
                ),
                last_log=_local_review_failure_log(local_review_result),
                auto_retry=False,
                operator_wait=True,
            )
            return run_id
        # 7. Surface the local-review verdict on the GitHub PR thread
        #    (not just on Linear) so human reviewers see the audit
        #    trail. Only fires when local-review APPROVED; local-only
        #    failures are parked for operator action. The binding-level
        #    override wins over the global config so an
        #    operator can keep one repo's PR thread quiet without
        #    disabling the feature everywhere. Skipped on a reconstructed
        #    resume, whose synthetic APPROVED result would post a degenerate
        #    "iterations: 0" summary.
        if (
            not ctx.reconstructed
            and binding.resolved_post_local_review_pr_summary(
                self.config.post_local_review_pr_summary
            )
            and local_review_result is not None
            and local_review_result.outcome == LoopOutcome.APPROVED
        ):
            await self._post_local_review_pr_summary(
                binding=binding,
                pr_url=pr_url,
                reviewer_agent=binding.resolved_role(
                    "review_find", self.config.roles
                ).agent,
                result=local_review_result,
            )
        return run_id

    async def _run_local_review_phase(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str | None = None,
        workspace_path: Path,
        parent_run_id: str,
        allow_fixes: bool = True,
    ) -> LoopResult | None:
        """Run the local-review session and surface its outcome on Linear.

        Returns the `LoopResult` so future iterations can use the verdict
        to gate downstream behavior. Today the result is logged and
        commented on Linear but does not change the flow — that ramp-up
        is intentional, so we can prove the loop converges on real diffs
        before letting it drive the PR.

        Errors here are caught and logged: a broken local-review path
        returns `None` so the caller can either continue to remote review
        or park a local-only PR for operator action.
        """
        storage_issue_id = storage_issue_id or issue.id
        try:
            base_branch = binding.base_branch
            if base_branch is None:
                try:
                    base_branch = await self._gh.repo_default_branch(
                        binding.github_repo
                    )
                except GitHubError as e:
                    log.warning(
                        "repo_default_branch failed during local review on %s: %s; "
                        "falling back to 'main'",
                        issue.identifier,
                        e,
                    )
                    base_branch = "main"

            # Resolve the reviewer from the roles matrix (review_find), falling
            # back through resolved_role to the legacy reviewer defaults when no
            # `roles:` block is set. The legacy resolved_reviewer_agent() ignores
            # the matrix, so a `roles: review_find/verify` block was silently
            # dead config and the reviewer always defaulted to codex.
            review_find_role = binding.resolved_role("review_find", self.config.roles)
            reviewer_agent = review_find_role.agent
            reviewer_codex_model = (
                review_find_role.model
                if reviewer_agent == "codex" and review_find_role.model
                else binding.resolved_reviewer_codex_model()
            )
            last_message_dir = (
                self.config.log_root / "local_review" / parent_run_id
            )
            # Local-review uses its own cap (default 3) which is
            # typically lower than `review_iteration_cap` (default 12)
            # used by the remote `@codex` loop. The two loops have
            # different convergence shapes; conflating their caps was
            # forcing a compromise.
            cap = binding.resolved_local_review_iteration_cap(
                self.config.local_review_iteration_cap
            )

            await self._move_issue_to_local_code_review_state(
                binding=binding, issue=issue
            )
            await self._post_local_review_starting_comment(
                binding=binding,
                issue=issue,
                reviewer_agent=reviewer_agent,
                strategy=binding.review_strategy,
                cap=cap,
            )

            # Create a `runs` row so the local-review cost participates
            # in `cost_for_issue` going forward (re-dispatches see the
            # full historical cost) and so the runs-history audit trail
            # shows the local-review phase alongside implement/review/
            # merge stages.
            local_review_run_id = str(uuid.uuid4())
            await db.runs.create(
                self._conn,
                id=local_review_run_id,
                issue_id=storage_issue_id,
                stage="local_review",
                status="running",
                pid=None,
                started_at=self._now().isoformat(),
            )

            async def _on_iteration(
                i: int, verdict: LocalVerdict, _cost_so_far: float
            ) -> None:
                await self._post_local_review_iteration_comment(
                    binding=binding,
                    issue=issue,
                    iteration=i,
                    verdict=verdict,
                )

            result: LoopResult | None = None
            try:
                result = await run_local_review_session(
                    runner=self._runner,
                    workspace_path=workspace_path,
                    base_branch=base_branch,
                    parent_run_id=parent_run_id,
                    issue_title=issue.title,
                    issue_body=issue.description,
                    labels=list(issue.labels),
                    implementer_agent=binding.agent,
                    implementer_codex_model=binding.codex_model,
                    reviewer_agent=reviewer_agent,
                    reviewer_codex_model=reviewer_codex_model,
                    local_review_claude_model=binding.local_review_claude_model,
                    local_review_verifier_claude_model=(
                        binding.local_review_verifier_claude_model
                    ),
                    fix_claude_model=self._fix_claude_model(binding),
                    cap=cap,
                    stall_secs=self.config.stall_timeout_secs,
                    command_secs=self.config.command_timeout_secs,
                    binding_env=dict(binding.env),
                    mcp_servers=dict(binding.mcp_servers),
                    last_message_dir=last_message_dir,
                    head_sha_provider=_workspace_head_sha,
                    diff_size_provider=partial(
                        _workspace_diff_size, base_branch=base_branch
                    ),
                    workspace_scrubber=_workspace_scrub,
                    on_iteration=_on_iteration,
                    allow_fixes=allow_fixes,
                )
            finally:
                await self._finalize_local_review_run(
                    run_id=local_review_run_id,
                    result=result,
                    log_dir=last_message_dir,
                    implementer_codex_model=binding.codex_model,
                    reviewer_codex_model=reviewer_codex_model,
                )

            log.info(
                "local-review phase for %s ended in %s (iterations=%d, "
                "strategy=%s, reviewer=%s)",
                issue.identifier,
                result.outcome.value,
                result.iterations,
                binding.review_strategy,
                reviewer_agent,
            )
            await self._post_local_review_comment(
                binding=binding, issue=issue, result=result
            )
            return result
        except Exception as e:  # noqa: BLE001
            # Never break the pipeline because of a local-review fault.
            log.warning(
                "local-review phase raised on %s: %s; continuing with remote review",
                issue.identifier,
                e,
            )
            return None

    async def _finalize_local_review_run(
        self,
        *,
        run_id: str,
        result: LoopResult | None,
        log_dir: Path | None = None,
        implementer_codex_model: str | None = None,
        reviewer_codex_model: str | None = None,
    ) -> None:
        """Close the local-review `runs` row started by the phase.

        Always called from the phase's `finally`, even when the session
        raised. `result=None` means the session never returned a
        `LoopResult` (uncaught exception inside the session); mark the
        row failed with zero cost so the row reflects the abort.
        """
        if result is not None:
            try:
                await _add_run_usage(
                    self._conn,
                    run_id,
                    UsageDelta(
                        cost_usd=result.total_cost_usd,
                        input_tokens=result.input_tokens,
                        output_tokens=result.output_tokens,
                        cache_write_tokens=result.cache_write_tokens,
                        cache_read_tokens=result.cache_read_tokens,
                    ),
                )
            except Exception:  # noqa: BLE001
                log.warning(
                    "could not persist local-review usage for run %s",
                    run_id,
                )
            if log_dir is not None:
                await self._record_local_review_model_usage(
                    run_id=run_id,
                    log_dir=log_dir,
                    implementer_codex_model=implementer_codex_model,
                    reviewer_codex_model=reviewer_codex_model,
                )
        status = _local_review_status_from_result(result)
        try:
            if status == "completed":
                await db.runs.update_status(
                    self._conn,
                    run_id,
                    status,
                    ended_at=self._now().isoformat(),
                )
            else:
                await db.runs.update_status(
                    self._conn,
                    run_id,
                    status,
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status=status,
                        reason=_local_review_termination_reason(result),
                    ),
                )
        except Exception:  # noqa: BLE001
            log.warning(
                "could not finalize local-review run %s (status=%s)",
                run_id,
                status,
            )

    async def _record_local_review_model_usage(
        self,
        *,
        run_id: str,
        log_dir: Path,
        implementer_codex_model: str | None,
        reviewer_codex_model: str | None,
    ) -> None:
        """Attribute a local-review run's tokens to (provider, model).

        The phase writes one role transcript per iteration under
        `log_dir`: `fix-*.out.log` (implementer) and `review-*.out.log`
        (reviewer). Each file is parsed with the codex model of its role
        (Claude ignores it — its `modelUsage` carries the exact model).
        Best-effort; never fails the run.
        """
        usages = await asyncio.to_thread(
            _parse_local_review_model_usage,
            log_dir,
            implementer_codex_model=implementer_codex_model,
            reviewer_codex_model=reviewer_codex_model,
        )
        if not usages:
            return
        try:
            await db.run_model_usage.replace_for_run(self._conn, run_id, usages)
        except aiosqlite.Error:
            log.warning("could not persist per-model usage for run %s", run_id)

    async def _post_local_review_pr_summary(
        self,
        *,
        binding: RepoBinding,
        pr_url: str,
        reviewer_agent: str,
        result: LoopResult,
    ) -> None:
        """Post a short verdict trail to the GitHub PR thread.

        Visible to anyone reviewing the PR on GitHub. Mirrors the
        Linear comment but in the language GitHub reviewers expect:
        which reviewer ran, how many iterations, what it cost. The
        intent is not to *replace* human review — it's to give a
        human reviewer enough context to decide "I trust this and
        will skim" vs. "I'll review carefully."
        """
        pr_number = pr_number_from_url(pr_url)
        if pr_number is None:
            log.warning(
                "could not parse PR number from %r — skipping local-review "
                "PR summary",
                pr_url,
            )
            return
        body = (
            f"**Symphony local reviewer ({reviewer_agent}) approved this PR.**\n\n"
            f"- iterations: {result.iterations}\n"
            f"- cost: ${result.total_cost_usd:.4f}\n"
            f"- strategy: `{binding.review_strategy}`\n"
        )
        try:
            await self._gh.pr_comment(
                pr_number, body, repo=binding.github_repo
            )
        except GitHubError as e:
            log.warning(
                "could not post local-review PR summary on %s#%d: %s",
                binding.github_repo,
                pr_number,
                e,
            )

    async def _post_local_review_starting_comment(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        reviewer_agent: str,
        strategy: str,
        cap: int,
    ) -> None:
        body = (
            "**Local review starting** "
            f"(strategy=`{strategy}`, reviewer=`{reviewer_agent}`, "
            f"cap={cap})."
        )
        tracker = self.tracker(binding)
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "local-review starting comment failed on %s: %s",
                issue.identifier,
                e,
            )

    async def _post_local_review_iteration_comment(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        iteration: int,
        verdict: LocalVerdict,
    ) -> None:
        """Per-iteration heartbeat so a 5-minute review doesn't look dead.

        Posted right after the verdict is parsed (before any fix-run
        dispatch), so operators can see "iteration 1: changes_requested
        — first finding…" while the fix-run is still running. Short
        snippets keep the issue thread readable.

        Per-iteration token deltas aren't plumbed into this callback
        (only a cumulative cost was, which we no longer render), so the
        heartbeat omits a spend figure entirely; the final outcome
        comment carries the token breakdown.
        """
        snippet = ""
        if verdict.findings:
            snippet = verdict.findings.strip().splitlines()[0][:280]
        body_parts = [
            f"**Local review iter {iteration}:** `{verdict.kind.value}`",
        ]
        if snippet:
            body_parts.append(f"> {snippet}")
        body = "\n\n".join(body_parts)
        tracker = self.tracker(binding)
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "local-review iter comment failed on %s: %s",
                issue.identifier,
                e,
            )

    async def _post_local_review_comment(
        self, *, binding: RepoBinding, issue: LinearIssue, result: LoopResult
    ) -> None:
        outcome = result.outcome.value
        last_findings = ""
        if result.last_verdict is not None and result.last_verdict.findings:
            last_findings = result.last_verdict.findings
        eff = effective_tokens(
            result.input_tokens,
            result.output_tokens,
            result.cache_write_tokens,
            result.cache_read_tokens,
        )
        body_parts = [
            f"**Local-review outcome:** `{outcome}` "
            f"(iterations={result.iterations}, "
            f"tokens: in {result.input_tokens} · out {result.output_tokens} · "
            f"cache w {result.cache_write_tokens} / r {result.cache_read_tokens} "
            f"· eff {eff:,.0f})",
        ]
        if result.error:
            body_parts.append(f"_Error:_ {result.error}")
        if last_findings:
            body_parts.append("Last findings:\n\n" + last_findings)
        body = "\n\n".join(body_parts)
        tracker = self.tracker(binding)
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "local-review comment post failed on %s: %s", issue.identifier, e
            )

    async def _block_local_only_review_infra_failure(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str,
        run_id: str,
        result: LoopResult | None,
        force_local_review_resume: bool = False,
    ) -> None:
        reason = _local_review_termination_reason(result)
        # Tag only failures where the reviewer never produced a verdict — a
        # `REVIEWER_FAILED` (no marker / reviewer crashed) or `result is None`
        # (the session raised). Both happen *after* the implement completion
        # gate, with no fix attempted, so the agent's commits are intact and a
        # $retry can safely resume agent-free. `FIX_RUN_FAILED`/`FIX_RUN_BLOCKED`
        # are deliberately left untagged: a fixer can fail/block after leaving
        # partial commits, so those must re-run the implementer.
        # Exception: `force_local_review_resume` is set when the transient retry
        # budget was exhausted with a clean workspace — the fixer did no work, so
        # the resume marker is safe.
        reviewer_never_verdicted = (
            force_local_review_resume
            or result is None
            or result.outcome == LoopOutcome.REVIEWER_FAILED
        )
        await self._fail_run(
            run_id,
            reason,
            termination_kind=(
                db.runs.LOCAL_REVIEW_INFRA_FAILED_KIND
                if reviewer_never_verdicted
                else None
            ),
        )

        tracker = self.tracker(binding)
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states while blocking %s after local review: %s",
                issue.identifier,
                e,
            )
            states = {}

        blocked_id = states.get(binding.linear_states.blocked)
        if blocked_id is not None:
            try:
                await tracker.move_issue(issue.id, blocked_id)
            except LinearError as e:
                log.warning(
                    "could not move %s to blocked after local-review failure: %s",
                    issue.identifier,
                    e,
                )
        else:
            log.warning(
                "missing Linear blocked state %r for %s after local-review failure",
                binding.linear_states.blocked,
                issue.identifier,
            )

        await self._track_implement_failed_wait(storage_issue_id, run_id, binding)
        tokens = await db.runs.tokens_for_issue(self._conn, storage_issue_id)
        body = failed(
            CommentVars(
                stage="local review",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                error=reason,
                last_log=_local_review_failure_log(result),
                auto_retry=False,
            )
        )
        body += (
            "\nReply with `$retry` or `$approve` to requeue this issue. "
            "Reply with `$reject` or `$stop` to leave it halted.\n"
        )
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "local-review blocked comment post failed on %s: %s",
                issue.identifier,
                e,
            )

    async def _run_verify_phase(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str,
        workspace_path: Path,
        parent_run_id: str,
        allow_fixes: bool = True,
    ) -> VerifyResult:
        """Run the binding's `verify_cmd` gate in the workspace.

        Unlike local review (which degrades on infra faults so a broken
        reviewer can't dead-end the pipeline), a fault here returns a
        failed result: the gate exists to stop unbuildable code from
        reaching a PR, so an unverifiable workspace must not push either.

        Mirrors `_run_local_review_phase`: a `stage="verify"` runs row is
        created so the fix turn's spend lands in `cost_for_issue` (feeding
        the re-dispatch cost guard) and in the fail-closed Linear comment's
        token totals — otherwise the implementer the fix turn spawns bills
        nothing.
        """
        verify_cmd = binding.verify_cmd or ""
        # `UsageCostEstimator.delta` is threaded into the fix turn's
        # `collect_runner_output` to bill its tokens; the fix-turn stdout
        # is written to `verify_log_path` for per-model attribution.
        cost_estimator = _UsageCostEstimator(
            agent=binding.agent, codex_model=binding.codex_model
        )
        verify_run_id = str(uuid.uuid4())
        verify_log_path = self.config.log_root / f"{verify_run_id}.log"
        await db.runs.create(
            self._conn,
            id=verify_run_id,
            issue_id=storage_issue_id,
            stage="verify",
            status="running",
            pid=None,
            started_at=self._now().isoformat(),
        )
        result: VerifyResult | None = None
        try:
            result = await run_verify_session(
                runner=self._runner,
                workspace_path=workspace_path,
                verify_cmd=verify_cmd,
                timeout_secs=binding.resolved_verify_timeout_secs(
                    self.config.command_timeout_secs
                ),
                parent_run_id=parent_run_id,
                issue_title=issue.title,
                issue_body=issue.description,
                labels=list(issue.labels),
                implementer_agent=binding.agent,
                implementer_codex_model=binding.codex_model,
                fix_claude_model=self._fix_claude_model(binding),
                stall_secs=self.config.stall_timeout_secs,
                command_secs=self.config.command_timeout_secs,
                usage_handler=cost_estimator.delta,
                fix_log_path=verify_log_path,
                allow_fixes=allow_fixes,
            )
        except Exception as e:  # noqa: BLE001 — fail closed
            log.exception("verify phase raised on %s", issue.identifier)
            result = VerifyResult(ok=False, error=f"verify phase raised: {e}")
        finally:
            await self._finalize_verify_run(
                run_id=verify_run_id,
                ok=result.ok if result is not None else False,
                cost_estimator=cost_estimator,
                log_path=verify_log_path,
                codex_model=binding.codex_model,
            )
        log.info(
            "verify phase for %s: ok=%s fix_attempted=%s",
            issue.identifier,
            result.ok,
            result.fix_attempted,
        )
        return result

    async def _finalize_verify_run(
        self,
        *,
        run_id: str,
        ok: bool,
        cost_estimator: _UsageCostEstimator,
        log_path: Path,
        codex_model: str | None,
    ) -> None:
        """Persist the verify fix turn's spend and close its `runs` row.

        Always called from the phase's `finally`. The delta accumulated by
        `cost_estimator` is the fix turn's whole-run spend; the row is
        marked completed on a green gate, failed otherwise (the fail-closed
        path also fails the parent implement run separately)."""
        try:
            await _add_run_usage(
                self._conn,
                run_id,
                UsageDelta(
                    cost_usd=cost_estimator.total_cost_usd,
                    input_tokens=cost_estimator.total_input_tokens,
                    output_tokens=cost_estimator.total_output_tokens,
                    cache_write_tokens=cost_estimator.total_cache_write_tokens,
                    cache_read_tokens=cost_estimator.total_cache_read_tokens,
                ),
            )
        except Exception:  # noqa: BLE001
            log.warning("could not persist verify usage for run %s", run_id)
        await _record_run_model_usage(
            self._conn, run_id, log_path, codex_model=codex_model
        )
        try:
            if ok:
                await db.runs.update_status(
                    self._conn,
                    run_id,
                    "completed",
                    ended_at=self._now().isoformat(),
                )
            else:
                await db.runs.update_status(
                    self._conn,
                    run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed", reason="verify_cmd failed"
                    ),
                )
        except Exception:  # noqa: BLE001
            log.warning("could not finalize verify run %s", run_id)

    async def _block_verify_failure(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        storage_issue_id: str,
        run_id: str,
        result: VerifyResult,
    ) -> None:
        """Fail-closed on a red verify gate: block the issue like a failed
        implement run, with the failure tail in the Linear comment."""
        reason = result.error or "verify_cmd failed"
        await self._fail_run(run_id, reason)

        tracker = self.tracker(binding)
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states while blocking %s after failed verify: %s",
                issue.identifier,
                e,
            )
            states = {}

        blocked_id = states.get(binding.linear_states.blocked)
        if blocked_id is not None:
            try:
                await tracker.move_issue(issue.id, blocked_id)
            except LinearError as e:
                log.warning(
                    "could not move %s to blocked after verify failure: %s",
                    issue.identifier,
                    e,
                )
        else:
            log.warning(
                "missing Linear blocked state %r for %s after verify failure",
                binding.linear_states.blocked,
                issue.identifier,
            )

        await self._track_implement_failed_wait(storage_issue_id, run_id, binding)
        tokens = await db.runs.tokens_for_issue(self._conn, storage_issue_id)
        body = failed(
            CommentVars(
                stage="verify",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                error=f"`{binding.verify_cmd}` — {reason}",
                last_log=result.tail,
                auto_retry=False,
            )
        )
        body += (
            "\nReply with `$retry` or `$approve` to requeue this issue. "
            "Reply with `$reject` or `$stop` to leave it halted.\n"
        )
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "verify blocked comment post failed on %s: %s",
                issue.identifier,
                e,
            )
