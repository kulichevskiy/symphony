"""`_ReviewMixin` — the review-monitoring domain of the poll loop (SYM-146).

Owns the Review-stage monitor: polling open review runs, the @codex
verdict/retrigger/re-arm machinery, the review fix-dispatch loop
(comment / CI / merge-conflict fixes), review operator waits, and the
resurrect / fail / park paths for review monitors. It extends
`_OrchestratorBase` so it sees the shared state + foundation methods; the
concrete `Orchestrator` (in `__init__.py`) inherits this mixin.

The cross-domain methods this layer calls (`_run_fix_agent`, `_schedule_merge`,
…) still live on `Orchestrator`; they are declared under `TYPE_CHECKING` below
so mypy resolves them without a runtime stub.

Pure structural extraction: method bodies are byte-for-byte unchanged from the
pre-split `Orchestrator`.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import re
import uuid
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
)
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
)

from ... import db
from ...agent.prompt import (
    merge_conflict_fix_prompt,
    review_comment_fix_prompt,
    review_fix_prompt,
)
from ...config import RepoBinding
from ...github.client import (
    CheckRun as GitHubCheckRun,
)
from ...github.client import (
    GitHubClient,
    GitHubError,
    PRChecks,
)
from ...linear.client import LinearError
from ...linear.slash import (
    SlashIntent,
    SlashKind,
)
from ...linear.templates import (
    CommentVars,
    codex_lgtm,
    command_rejected,
    failed,
    fix_pushed,
    fixing_merge_conflict,
    resumed,
    review_retry_requested,
    review_stopped,
    reviewing_feedback,
    skip_review_forced,
    stuck_loop_escape,
    truncate_body,
)
from ...notify import EVENT_RUN_FAILED
from ...pipeline.cost_guard import UsageDelta
from ...pipeline.local_review import (
    StreamApiError,
    classify_stream_api_error,
)
from ...pipeline.local_review_loop import (
    LoopOutcome,
    LoopResult,
)
from ...pipeline.review_classifier import (
    BLOCKING_CHECK_CONCLUSIONS,
    Reaction,
    Review,
    ReviewComment,
    ReviewSnapshot,
    Verdict,
    VerdictKind,
    has_hit_iteration_cap,
    is_codex_author,
    review_classifier,
    should_dispatch_fix_run,
)
from ...pipeline.review_classifier import (
    CheckRun as ReviewCheckRun,
)
from ...pipeline.state_machine import (
    on_runner_event,
)
from ...tracker import (
    Issue as LinearIssue,
)
from ...tracker import (
    TrackerContext,
)
from ._base import SlashHandlerFailure as SlashHandlerFailure
from ._base import (
    _binding_key,
    _binding_storage_key,
    _OrchestratorBase,
    _tracker_context_for_binding,
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
    _add_run_usage,
    _github_commit_url,
    _local_review_termination_reason,
    _parse_optional_datetime,
    _parse_rfc3339,
    _pr_url_for_state,
    _sum_usage,
    _termination_kwargs,
)

log = logging.getLogger(__name__)


CI_FETCH_FAILURE_LIMIT = 5


REVIEW_RESURRECT_COOLDOWN_SECS = 120


CODEX_NO_ISSUES_MARKER = "any major issues"


def _local_review_needs_approval(result: LoopResult | None) -> bool:
    return result is not None and result.outcome in {
        LoopOutcome.EXHAUSTED,
        LoopOutcome.STUCK_LOOP,
    }


def _local_review_infra_failed(result: LoopResult | None) -> bool:
    # FIX_RUN_BLOCKED (SYM-107): a fix-run politely stalled on a human action.
    # Routed through the same pre-push block path as infra failures so no push
    # / PR happens and the issue is parked for the operator with the blocked
    # reason captured verbatim (`result.error`).
    return result is None or result.outcome in {
        LoopOutcome.REVIEWER_FAILED,
        LoopOutcome.FIX_RUN_FAILED,
        LoopOutcome.FIX_RUN_BLOCKED,
    }


def _local_review_permits_remote(result: LoopResult | None) -> bool:
    return result is not None and result.outcome in {
        LoopOutcome.APPROVED,
    }


def _local_review_failure_log(result: LoopResult | None) -> str:
    if result is None:
        return ""
    parts: list[str] = []
    if result.error:
        parts.append(result.error)
    if result.last_verdict is not None and result.last_verdict.findings:
        parts.append(result.last_verdict.findings)
    return "\n\n".join(parts)


def _review_issue_is_active(issue: LinearIssue, binding: RepoBinding) -> bool:
    active_states = {binding.linear_states.in_progress}
    if binding.resolved_local_review() and binding.linear_states.local_code_review:
        active_states.add(binding.linear_states.local_code_review)
    if binding.resolved_remote_review() and binding.linear_states.code_review:
        active_states.add(binding.linear_states.code_review)
    return issue.state_name in active_states


def _user_login(entry: dict[str, object]) -> str:
    user = entry.get("user")
    if isinstance(user, dict):
        login = user.get("login")
        if login is not None:
            return str(login)
    return ""


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


def _review_comments_from_github(
    entries: list[dict[str, object]],
) -> list[ReviewComment]:
    comments: list[ReviewComment] = []
    for entry in entries:
        line_value = entry.get("line")
        line = line_value if isinstance(line_value, int) else None
        comments.append(
            ReviewComment(
                user_login=_user_login(entry),
                body=str(entry.get("body") or ""),
                commit_sha=str(entry.get("commit_id") or entry.get("original_commit_id") or ""),
                created_at=str(entry.get("created_at") or ""),
                path=str(entry.get("path") or ""),
                line=line,
            )
        )
    return comments


def _reviews_from_github(entries: list[dict[str, object]]) -> tuple[Review, ...]:
    reviews: list[Review] = []
    for entry in entries:
        reviews.append(
            Review(
                user_login=_user_login(entry),
                state=str(entry.get("state") or ""),
                commit_sha=str(entry.get("commit_id") or ""),
                submitted_at=str(entry.get("submitted_at") or ""),
                body=str(entry.get("body") or ""),
            )
        )
    return tuple(reviews)


def _reactions_from_github(entries: list[dict[str, object]]) -> tuple[Reaction, ...]:
    reactions: list[Reaction] = []
    for entry in entries:
        reactions.append(
            Reaction(
                user_login=_user_login(entry),
                content=str(entry.get("content") or ""),
                created_at=str(entry.get("created_at") or ""),
            )
        )
    return tuple(reactions)


_CODEX_REVIEWED_COMMIT_RE = re.compile(
    r"reviewed\s+commit:\s*\**\s*`?\s*([0-9a-fA-F]{7,40})",
    re.IGNORECASE,
)


def _codex_lgtm_reactions_from_issue_comments(
    entries: list[dict[str, object]],
) -> tuple[Reaction, ...]:
    """Treat Codex's "no major issues" PR issue comment as an approval signal.

    Codex sometimes reports the 👍 as text inside a top-level PR comment instead
    of as a GitHub reaction. The classifier already knows how to validate +1
    signals against the head commit time, so normalize this shape into the same
    representation. When the comment names the commit it reviewed
    ("Reviewed commit: <sha>"), thread the SHA through so the classifier can
    reject the approval once HEAD moves past that commit (branch update/rebase).
    """
    reactions: list[Reaction] = []
    for entry in entries:
        login = _user_login(entry)
        body = str(entry.get("body") or "")
        created_at = str(entry.get("created_at") or entry.get("createdAt") or "")
        if not created_at:
            continue
        if is_codex_author(login) and CODEX_NO_ISSUES_MARKER in body.casefold():
            match = _CODEX_REVIEWED_COMMIT_RE.search(body)
            reactions.append(
                Reaction(
                    user_login=login,
                    content="+1",
                    created_at=created_at,
                    commit_sha=match.group(1) if match else "",
                )
            )
    return tuple(reactions)


def _has_codex_review_request_after_head(
    entries: list[dict[str, object]],
    *,
    head_committed_at: str,
) -> bool:
    head_dt = _parse_optional_datetime(head_committed_at)
    if head_dt is None:
        return False
    for entry in entries:
        body = str(entry.get("body") or "").strip()
        if body.casefold() != "@codex review":
            continue
        created_at = _parse_optional_datetime(
            str(entry.get("created_at") or entry.get("createdAt") or "")
        )
        if created_at is not None and created_at >= head_dt:
            return True
    return False


async def _commit_committed_at_or_empty(
    gh: GitHubClient,
    *,
    repo: str,
    sha: str,
) -> str:
    if not sha:
        return ""
    try:
        result = gh.commit_committed_at(repo, sha)
        if inspect.isawaitable(result):
            return str(await result)
        if isinstance(result, str):
            return result
    except GitHubError as e:
        log.warning("could not fetch commit time for %s@%s: %s", repo, sha, e)
    except (AttributeError, TypeError) as e:
        log.debug("commit time unavailable for %s@%s: %s", repo, sha, e)
    return ""


async def _abort_rebase_safely(workspace_path: Path, *, issue_identifier: str, reason: str) -> None:
    try:
        await _git_abort_rebase(workspace_path)
    except Exception as e:  # noqa: BLE001
        log.warning(
            "could not abort rebase after %s for %s: %s",
            reason,
            issue_identifier,
            e,
        )


def _read_run_stream_api_error_obj(log_path: Path) -> StreamApiError | None:
    """The typed provider API error recovered from a stage run log, or None.

    An rc=0 implement run can carry only a transient provider API error
    (claude `is_error`/`api_error_status` or codex `turn.failed`/`error`) and no
    completion marker. Returning the typed signal lets the completion gate both
    surface the real message *and* gate the transient-error retry path on
    `.transient`.
    """
    try:
        stdout = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    return classify_stream_api_error(stdout)


def _review_check_from_gh(run: GitHubCheckRun) -> ReviewCheckRun:
    bucket = run.bucket.lower()
    state = run.state.lower()
    status = "completed" if bucket in {"pass", "fail", "cancel", "skipping"} else "in_progress"
    conclusion_by_bucket = {
        "pass": "success",
        "fail": "failure",
        "cancel": "cancelled",
        "skipping": "skipped",
    }
    conclusion = conclusion_by_bucket.get(bucket)
    if conclusion is None and status == "completed":
        conclusion = state or None
    return ReviewCheckRun(
        name=run.name,
        status=status,
        conclusion=conclusion,
        required=True,
    )


def _unknown_head_ci_scope(checks: PRChecks) -> str:
    failed = [run for run in checks.runs if run.bucket.lower() in {"fail", "cancel"}]
    scoped_runs = failed or checks.runs
    parts = sorted(
        "\0".join(
            [
                run.name,
                run.bucket.lower(),
                run.state.lower(),
                run.link or "",
            ]
        )
        for run in scoped_runs
    )
    digest = hashlib.sha256("\n".join(parts).encode()).hexdigest()[:16]
    return f"unknown-head-{digest}"


class _ReviewMixin(_OrchestratorBase):
    """Review-monitoring domain of the poll loop; `Orchestrator` extends it."""

    # Substring that identifies a Codex "no issues found" issue comment.
    # Codex posts: "Didn't find any major issues. Delightful!"
    _CODEX_NO_ISSUES_MARKER = CODEX_NO_ISSUES_MARKER

    if TYPE_CHECKING:
        # Sibling-domain methods provided by the concrete `Orchestrator`.
        async def _agent_infra_retry_backoff_active(self, issue_id: str) -> bool: ...

        def _binding_for_pr(self, candidate: db.issue_prs.IssuePR) -> RepoBinding | None: ...

        async def _block_local_only_review_infra_failure(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            storage_issue_id: str,
            run_id: str,
            result: LoopResult | None,
            force_local_review_resume: bool = False,
        ) -> None: ...

        async def _clear_operator_wait(self, issue_id: str, run_id: str) -> None: ...

        async def _interrupt_stale_merge_needs_approval_for_state(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            state: db.review_state.ReviewState,
        ) -> int: ...

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

        async def _move_issue_to_review_state(
            self, *, binding: RepoBinding, issue: LinearIssue
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

        async def _post_command_rejected(
            self, issue_id: str, slash_text: str, reason: str
        ) -> None: ...

        async def _restore_operator_wait_binding(
            self,
            issue_id: str,
            run_id: str,
            intent: SlashIntent,
            *,
            expected_kinds: tuple[str, ...],
        ) -> RepoBinding | None: ...

        @asynccontextmanager
        async def _review_fix_dispatch_slot(
            self,
            binding: RepoBinding,
            issue: LinearIssue,
            *,
            dispatch_capacity_held: bool = False,
        ) -> AsyncIterator[None]:
            yield

        async def _run_fix_agent(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            run_id: str,
            workspace_path: Path,
            prompt: str,
            prior_total: float,
        ) -> tuple[UsageDelta, str, int | None]: ...

        async def _run_local_review_phase(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            storage_issue_id: str | None = None,
            workspace_path: Path,
            parent_run_id: str,
            allow_fixes: bool = True,
        ) -> LoopResult | None: ...

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
        ) -> asyncio.Task[None]: ...

        @staticmethod
        def _slash_text(intent: SlashIntent) -> str: ...

    async def _handle_active_review_retry_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        state = await db.review_state.get(self._conn, issue_id)
        binding = await self._binding_for_review_issue_id(issue_id, state=state)
        if binding is None:
            await self._post_command_rejected(
                issue_id,
                "$retry",
                "no repository binding found for the active review monitor",
            )
            return
        if state.pr_number is None:
            await self._post_command_rejected(
                issue_id,
                "$retry",
                "no PR found for the active review monitor",
            )
            return

        pr_url = _pr_url_for_state(
            repo=binding.github_repo,
            pr_number=state.pr_number,
            pr_url=state.pr_url,
        )
        # Only ping the remote bot when `remote_review` is enabled. Local-only
        # and no-review bindings must never fire `@codex review` — the manual
        # retry just re-arms the monitor without a remote ping.
        if binding.resolved_remote_review():
            try:
                await (await self._gh_client()).pr_comment(
                    state.pr_number, "@codex review", repo=binding.github_repo
                )
            except GitHubError as e:
                log.warning(
                    "could not re-post @codex review for active monitor %s#%d: %s",
                    binding.github_repo,
                    state.pr_number,
                    e,
                )
                await self._post_command_rejected(
                    issue_id,
                    "$retry",
                    f"could not re-post @codex review: {e}",
                )
                return

        signature = f"manual_retry:{run_id}:{intent.comment_id}"
        await db.review_state.set_signature(self._conn, issue_id, signature)
        log.info(
            "$retry received for active review monitor %s (issue %s); re-triggered @codex review",
            run_id,
            issue_id,
        )
        body = review_retry_requested(
            CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number,
                pr_url=pr_url,
                run_id=run_id,
            )
        )
        tracker = self.tracker(binding)
        try:
            await tracker.post_comment(issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("active review retry comment failed for %s: %s", issue_id, e)

    async def _stop_review_monitor(self, issue_id: str, run_id: str) -> None:
        log.info("$stop received for review monitor %s (issue %s)", run_id, issue_id)
        now = self._now().isoformat()
        fix_run_id = self._dispatch_run_ids.get(issue_id)
        if fix_run_id is not None and fix_run_id != run_id:
            log.info(
                "$stop received for review monitor %s: killing concurrent run %s",
                run_id,
                fix_run_id,
            )
            try:
                await self._runner.kill(fix_run_id)
            except Exception:  # noqa: BLE001
                log.exception("could not kill concurrent review run %s", fix_run_id)
                try:
                    tracker = await self._tracker_for_issue_id(issue_id)
                    await tracker.post_comment(
                        issue_id,
                        truncate_body(
                            command_rejected(
                                "$stop",
                                "could not stop active review fix-run",
                            )
                        ),
                    )
                except LinearError as e:
                    log.warning(
                        "could not post stop rejection for %s: %s",
                        issue_id,
                        e,
                    )
                return

        task = self._review_poll_run_tasks.get(run_id)
        if task is not None:
            self._review_poll_tasks.discard(task)
            task.cancel()
        self._review_poll_run_ids.discard(run_id)
        self._review_poll_run_tasks.pop(run_id, None)
        if self._review_poll_issue_ids.get(issue_id) == run_id:
            self._review_poll_issue_ids.pop(issue_id, None)
        await self._clear_review_rearm_retry(run_id)
        await db.runs.update_status(
            self._conn,
            run_id,
            "interrupted",
            ended_at=now,
            kind="cancelled",
            detail="$stop interrupted review monitor",
        )
        if fix_run_id is not None and fix_run_id != run_id:
            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "interrupted",
                ended_at=now,
                kind="cancelled",
                detail="$stop interrupted active review fix-run",
            )
            self._dispatch_run_ids.pop(issue_id, None)
            self._active_run_ids.discard(fix_run_id)
        state = await db.review_state.get(self._conn, issue_id)
        binding = await self._binding_for_review_issue_id(issue_id, state=state)
        if binding is None:
            log.warning(
                "could not persist stopped review wait for issue %s: no matching binding",
                issue_id,
            )
            return
        await self._track_review_stopped_wait(issue_id, run_id, binding)
        pr_url = state.pr_url
        if not pr_url and state.pr_number is not None:
            pr_url = f"https://github.com/{binding.github_repo}/pull/{state.pr_number}"
        body = review_stopped(
            CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=pr_url or "(no PR yet)",
                run_id=run_id,
            )
        )
        tracker = self.tracker(binding)
        try:
            await tracker.post_comment(issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("review stop confirmation failed for %s: %s", issue_id, e)

    async def _binding_for_review_issue_id(
        self, issue_id: str, *, state: db.review_state.ReviewState
    ) -> RepoBinding | None:
        cur = await self._conn.execute(
            "SELECT provider, site, team_key FROM issues WHERE id = ?",
            (issue_id,),
        )
        row = await cur.fetchone()
        team_key = str(row["team_key"]) if row is not None else ""
        tracker_ctx: TrackerContext | None = None
        if row is not None:
            provider = str(row["provider"] or "")
            site = str(row["site"] or "")
            if provider and site:
                project_key = str(row["team_key"] or "") if provider == "jira" else ""
                tracker_ctx = TrackerContext(
                    provider=provider,
                    site=site,
                    project_key=project_key,
                )
        for binding in self.config.repos:
            if tracker_ctx is not None and _tracker_context_for_binding(binding) != tracker_ctx:
                continue
            if team_key and binding.linear_team_key != team_key:
                continue
            if state.github_repo and binding.github_repo != state.github_repo:
                continue
            if state.github_repo and (binding.issue_label or "") != state.issue_label:
                continue
            if not state.github_repo and state.issue_label:
                if (binding.issue_label or "") != state.issue_label:
                    continue
            return binding
        return None

    async def _poll_review_runs(self) -> list[asyncio.Task[None]]:
        """Poll CI for each active Review monitor row.

        Review uses a live `runs` row as the durable stage monitor. Local
        fix-runs get separate `review_fix` rows so subprocess PIDs and
        interruption reconciliation never mutate the monitor row.
        """
        scheduled: list[asyncio.Task[None]] = []
        for run in await db.runs.list_live_by_stage(self._conn, stage="review"):
            if run.id in self._active_run_ids or run.id in self._review_poll_run_ids:
                continue
            if await self._review_poll_deferred_by_deliver_failed_wait(run.issue_id, run.id):
                continue
            tracker_issue_id, tracker_ctx = await self._tracker_identity_for_issue(run.issue_id)
            tracker = self.tracker(tracker_ctx)
            try:
                issue = await tracker.lookup_issue(tracker_issue_id)
            except LinearError as e:
                log.warning("could not resolve issue for review run %s: %s", run.id, e)
                continue
            state = await db.review_state.get(self._conn, run.issue_id)
            binding = self._binding_for_review(issue, state, tracker_ctx=tracker_ctx)
            if binding is None:
                log.warning(
                    "no repo binding found for active review run %s (%s)",
                    run.id,
                    issue.identifier,
                )
                await self._fail_orphaned_review_run(
                    run=run,
                    issue=issue,
                    state=state,
                    error=("review monitor no longer matches any configured repository binding"),
                )
                continue
            if not _review_issue_is_active(issue, binding):
                log.info(
                    "closing review run %s for %s because issue is in %s",
                    run.id,
                    issue.identifier,
                    issue.state_name,
                )
                await self._close_review_run(run)
                continue
            if self.config.global_max_concurrent <= 0 or binding.max_concurrent <= 0:
                log.info(
                    "review run %s for %s: dispatch capacity is zero (global=%d, binding=%d)",
                    run.id,
                    issue.identifier,
                    self.config.global_max_concurrent,
                    binding.max_concurrent,
                )
                continue
            scheduled.append(self._schedule_review_poll(run, binding, issue))
        return scheduled

    async def _review_poll_deferred_by_deliver_failed_wait(
        self, issue_id: str, review_run_id: str
    ) -> bool:
        wait = await db.operator_waits.get(self._conn, issue_id)
        if wait is None or wait.kind != db.operator_waits.KIND_DELIVER_FAILED:
            return False
        log.info(
            "skipping review run %s for %s: deliver_failed wait %s is pending",
            review_run_id,
            issue_id,
            wait.run_id,
        )
        return True

    def _schedule_review_poll(
        self, run: db.runs.Run, binding: RepoBinding, issue: LinearIssue
    ) -> asyncio.Task[None]:
        self._review_poll_run_ids.add(run.id)
        self._review_poll_issue_ids[issue.id] = run.id
        task = asyncio.create_task(self._poll_review_run_with_limits(run, binding, issue))
        self._review_poll_tasks.add(task)
        self._review_poll_run_tasks[run.id] = task
        task.add_done_callback(partial(self._review_poll_done, run_id=run.id, issue_id=issue.id))
        return task

    async def _mark_review_rearm_retry(self, run_id: str) -> None:
        self._review_rearm_retry_run_ids.add(run_id)
        await db.runs.mark_review_rearm_retry(self._conn, run_id)

    async def _clear_review_rearm_retry(self, run_id: str) -> None:
        self._review_rearm_retry_run_ids.discard(run_id)
        await db.runs.clear_review_rearm_retry(self._conn, run_id)

    async def _review_rearm_retry_pending(self, run_id: str) -> bool:
        if run_id in self._review_rearm_retry_run_ids:
            return True
        if await db.runs.has_review_rearm_retry(self._conn, run_id):
            self._review_rearm_retry_run_ids.add(run_id)
            return True
        return False

    def _clear_review_no_signal_rearm_heads(self, run_id: str) -> None:
        self._review_no_signal_rearm_heads = {
            key for key in self._review_no_signal_rearm_heads if key[0] != run_id
        }

    async def _local_review_approved_for_current_review(self, run: db.runs.Run) -> bool:
        latest_local_review = await self._latest_local_review_for_current_review(run)
        return latest_local_review is not None and latest_local_review.status == "completed"

    async def _latest_local_review_for_current_review(self, run: db.runs.Run) -> db.runs.Run | None:
        latest_implement = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=run.issue_id,
            stage="implement",
        )
        if latest_implement is None:
            return None
        latest_local_review = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=run.issue_id,
            stage="local_review",
            started_at_gte=latest_implement.started_at,
        )
        if latest_local_review is None:
            return None
        if _parse_rfc3339(latest_local_review.started_at) > _parse_rfc3339(run.started_at):
            return None
        return latest_local_review

    async def _local_review_permits_current_review(self, run: db.runs.Run) -> bool:
        latest_local_review = await self._latest_local_review_for_current_review(run)
        if latest_local_review is None:
            return False
        return latest_local_review.status in {"completed", "interrupted"}

    async def _review_retry_needs_local_gate(
        self, *, binding: RepoBinding, run: db.runs.Run
    ) -> bool:
        if not binding.resolved_local_review():
            return False
        if not binding.resolved_remote_review():
            return True
        return not await self._local_review_permits_current_review(run)

    async def _local_review_completed_for_issue(self, candidate: db.issue_prs.IssuePR) -> bool:
        """Whether a completed local-review run covers the current PR HEAD.

        Used by the merge scheduler to gate the `remote_review: false`
        review bypass: local-only bindings must have a finished local-review
        run before clean CI is treated as a merge signal.

        A completed local review from a previous PR cycle must not green-light
        a later PR for the same issue. Merge-conflict and required-check
        fix-runs (`stage = "review_fix"`) push commits the reviewer never saw,
        so a local review that predates the latest fix-run is stale and must
        not green-light the post-fix HEAD — otherwise unreviewed code merges.
        A local review after the PR was opened is valid when it is the post-fix
        rerun for the current PR.
        """
        latest_implement = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=candidate.issue_id,
            stage="implement",
        )
        if latest_implement is None or _parse_rfc3339(latest_implement.started_at) > _parse_rfc3339(
            candidate.created_at
        ):
            return False
        latest_local_review = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=candidate.issue_id,
            stage="local_review",
            started_at_gte=latest_implement.started_at,
        )
        if latest_local_review is None or latest_local_review.status != "completed":
            return False
        latest_fix = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=candidate.issue_id,
            stage="review_fix",
        )
        if latest_fix is not None and _parse_rfc3339(latest_fix.started_at) > _parse_rfc3339(
            latest_local_review.started_at
        ):
            return False
        return True

    async def _poll_review_run_with_limits(
        self,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
    ) -> None:
        if self.config.global_max_concurrent <= 0 or binding.max_concurrent <= 0:
            log.info(
                "review run %s for %s: dispatch capacity is zero (global=%d, binding=%d)",
                run.id,
                issue.identifier,
                self.config.global_max_concurrent,
                binding.max_concurrent,
            )
            return
        # Polling runs unconditionally — no semaphore. If feedback requires a
        # fix-run, the dispatch helper reserves normal capacity so review fixes
        # are scheduled ahead of fresh implementations.
        current = await self._refresh_review_poll_candidate(run, binding, issue)
        if current is None:
            return
        current_binding, current_issue = current
        rearm_retry_pending = await self._review_rearm_retry_pending(run.id)
        if await self._review_poll_deferred_by_deliver_failed_wait(run.issue_id, run.id):
            return
        rearm_done = True
        if rearm_retry_pending:
            state = await db.review_state.get(self._conn, run.issue_id)
            rearm_done = await self._retrigger_codex_review_unless_approved(
                binding=current_binding,
                issue=current_issue,
                state=state,
                require_no_signal=True,
            )
            if rearm_done:
                await self._clear_review_rearm_retry(run.id)
        if await self._review_poll_deferred_by_deliver_failed_wait(run.issue_id, run.id):
            return
        handled_feedback = await self._poll_review_run(run, current_binding, current_issue)
        if rearm_retry_pending and not rearm_done and handled_feedback:
            await self._clear_review_rearm_retry(run.id)

    async def _refresh_review_poll_candidate(
        self,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
    ) -> tuple[RepoBinding, LinearIssue] | None:
        live_review_runs = await db.runs.list_live_by_stage(self._conn, stage="review")
        if not any(live_run.id == run.id for live_run in live_review_runs):
            log.info("skipping review run %s: run is no longer live", run.id)
            return None
        if await self._review_poll_deferred_by_deliver_failed_wait(run.issue_id, run.id):
            return None
        tracker_issue_id, tracker_ctx = await self._tracker_identity_for_issue(run.issue_id)
        tracker = self.tracker(tracker_ctx)
        try:
            current = await tracker.lookup_issue(tracker_issue_id)
        except LinearError as e:
            log.warning(
                "could not revalidate %s before review polling: %s",
                issue.identifier,
                e,
            )
            return None
        state = await db.review_state.get(self._conn, run.issue_id)
        current_binding = self._binding_for_review(current, state, tracker_ctx=tracker_ctx)
        if current_binding is None:
            log.warning(
                "no repo binding found for active review run %s (%s)",
                run.id,
                current.identifier,
            )
            await self._fail_orphaned_review_run(
                run=run,
                issue=current,
                state=state,
                error=("review monitor no longer matches any configured repository binding"),
            )
            return None
        if _binding_key(current_binding) != _binding_key(binding):
            log.info(
                "skipping review run %s for %s: binding changed before polling",
                run.id,
                current.identifier,
            )
            return None
        if not _review_issue_is_active(current, current_binding):
            log.info(
                "closing review run %s for %s because issue is in %s",
                run.id,
                current.identifier,
                current.state_name,
            )
            await self._close_review_run(run)
            return None
        if (
            current.state_name == current_binding.linear_states.in_progress
            and current_binding.resolved_remote_review()
        ):
            await self._move_issue_to_review_state(binding=current_binding, issue=current)
        return current_binding, current

    def _review_poll_done(self, task: asyncio.Task[None], run_id: str, issue_id: str = "") -> None:
        self._review_poll_tasks.discard(task)
        self._review_poll_run_ids.discard(run_id)
        self._review_poll_run_tasks.pop(run_id, None)
        if issue_id and self._review_poll_issue_ids.get(issue_id) == run_id:
            self._review_poll_issue_ids.pop(issue_id, None)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("review poll task crashed for run_id=%s", run_id)

    async def _close_review_run(self, run: db.runs.Run) -> None:
        await db.runs.update_status(
            self._conn,
            run.id,
            "completed",
            ended_at=self._now().isoformat(),
        )
        await self._clear_review_rearm_retry(run.id)
        self._clear_review_no_signal_rearm_heads(run.id)

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

    async def _terminate_deliver_failed_review_monitors(
        self, issue_id: str, *, detail: str
    ) -> None:
        """Retire Review monitors created by a delivery handoff that got rejected."""
        live_review_runs = [
            run
            for run in await db.runs.list_live_by_stage(self._conn, stage="review")
            if run.issue_id == issue_id
        ]
        if not live_review_runs:
            return

        now = self._now().isoformat()
        closed_run_ids: set[str] = set()
        for run in live_review_runs:
            await db.runs.update_status(
                self._conn,
                run.id,
                "interrupted",
                ended_at=now,
                kind="cancelled",
                detail=detail,
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
            if mapped_issue_id == issue_id or mapped_run_id in closed_run_ids:
                self._review_poll_issue_ids.pop(mapped_issue_id, None)

        log.info(
            "interrupted review monitor(s) %s for issue_id=%s after deliver_failed halt",
            ", ".join(sorted(closed_run_ids)),
            issue_id,
        )

    def _cancel_deliver_failed_review_poll_tasks(self, issue_id: str) -> None:
        """Stop in-flight Review polling while delivery is parked as failed.

        The durable Review monitor row stays live so a `$retry` can adopt it
        once delivery resumes; only the current in-memory poll task is
        cancelled/suppressed.
        """
        cancelled_run_ids: set[str] = set()
        for mapped_issue_id, mapped_run_id in list(self._review_poll_issue_ids.items()):
            if mapped_issue_id != issue_id:
                continue
            task = self._review_poll_run_tasks.pop(mapped_run_id, None)
            if task is not None:
                self._review_poll_tasks.discard(task)
                if not task.done():
                    task.cancel()
            self._review_poll_run_ids.discard(mapped_run_id)
            self._review_poll_issue_ids.pop(mapped_issue_id, None)
            cancelled_run_ids.add(mapped_run_id)
        if not cancelled_run_ids:
            return
        log.info(
            "cancelled review poll task(s) %s for issue_id=%s after deliver_failed halt",
            ", ".join(sorted(cancelled_run_ids)),
            issue_id,
        )

    async def _maybe_rearm_codex_review_for_no_signal(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        state: db.review_state.ReviewState,
        head_sha: str,
    ) -> None:
        if (
            binding.review_strategy == "local"
            and await self._local_review_approved_for_current_review(run)
        ):
            log.debug(
                "skipping no-signal @codex review re-arm for %s: "
                "local reviewer approved current review cycle",
                issue.identifier,
            )
            return
        if not head_sha:
            return
        rearm_key = (run.id, head_sha)
        if rearm_key in self._review_no_signal_rearm_heads:
            return

        rearm_done = await self._retrigger_codex_review_unless_approved(
            binding=binding,
            issue=issue,
            state=state,
            require_no_signal=True,
        )
        if rearm_done:
            self._review_no_signal_rearm_heads.add(rearm_key)

    async def _poll_review_run(
        self,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
    ) -> bool:
        storage_issue_id = run.issue_id
        state = await db.review_state.get(self._conn, storage_issue_id)
        if state.pr_number is None:
            await self._fail_review_run(
                run=run,
                binding=binding,
                issue=issue,
                error="review run has no PR number",
                last_log="",
            )
            return False
        pr_number = state.pr_number

        checks = await self._fetch_review_pr_checks(
            run=run, binding=binding, issue=issue, pr_number=pr_number
        )
        if checks is None:
            return False

        head_sha = _unknown_head_ci_scope(checks)
        mergeable = ""
        try:
            view = await (await self._gh_client()).pr_view(pr_number, repo=binding.github_repo)
            head_sha = str(view.get("headRefOid") or "") or head_sha
            mergeable = str(view.get("mergeable") or "")
        except Exception as e:  # noqa: BLE001
            log.warning(
                "could not fetch PR view for %s#%d: %s",
                binding.github_repo,
                pr_number,
                e,
            )

        head_committed_at = await _commit_committed_at_or_empty(
            await self._gh_client(),
            repo=binding.github_repo,
            sha=head_sha,
        )
        ci_runs = [_review_check_from_gh(c) for c in checks.runs]
        remote_review = binding.resolved_remote_review()

        verdict, issue_comments = await self._compute_review_verdict(
            binding=binding,
            pr_number=pr_number,
            state=state,
            ci_runs=ci_runs,
            head_sha=head_sha,
            head_committed_at=head_committed_at,
            mergeable=mergeable,
            remote_review=remote_review,
        )

        if await self._review_poll_deferred_by_deliver_failed_wait(storage_issue_id, run.id):
            return False

        if remote_review:
            await self._maybe_post_codex_lgtm(
                run=run,
                binding=binding,
                issue=issue,
                state=state,
                pr_number=pr_number,
                head_committed_at=head_committed_at,
                issue_comments=issue_comments,
            )

        if remote_review and verdict.kind is VerdictKind.PENDING and verdict.rule == "no_signal":
            await self._maybe_rearm_codex_review_for_no_signal(
                run=run,
                binding=binding,
                issue=issue,
                state=state,
                head_sha=head_sha,
            )

        if verdict.kind is not VerdictKind.CHANGES_REQUESTED:
            return False

        return await self._dispatch_review_changes_requested_fix(
            run=run,
            binding=binding,
            issue=issue,
            state=state,
            checks=checks,
            verdict=verdict,
        )

    async def _fetch_review_pr_checks(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
    ) -> PRChecks | None:
        """Fetch PR CI checks, failing the run after repeated fetch errors.

        Returns None (and leaves the run unchanged, or fails it once the
        consecutive-failure limit is hit) when checks can't be fetched.
        """
        storage_issue_id = run.issue_id
        try:
            checks = await (await self._gh_client()).pr_checks(pr_number, repo=binding.github_repo)
        except GitHubError as e:
            failures = await db.review_state.bump_ci_fetch_failures(self._conn, storage_issue_id)
            log.warning(
                "gh pr checks failed for %s#%d (%d/%d): %s",
                binding.github_repo,
                pr_number,
                failures,
                CI_FETCH_FAILURE_LIMIT,
                e,
            )
            if failures >= CI_FETCH_FAILURE_LIMIT:
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"gh pr checks failed {failures} consecutive times: {e}",
                    last_log=str(e),
                )
            return None

        await db.review_state.reset_ci_fetch_failures(self._conn, storage_issue_id)
        return checks

    async def _compute_review_verdict(
        self,
        *,
        binding: RepoBinding,
        pr_number: int,
        state: db.review_state.ReviewState,
        ci_runs: list[ReviewCheckRun],
        head_sha: str,
        head_committed_at: str,
        mergeable: str,
        remote_review: bool,
    ) -> tuple[Verdict, list[dict[str, object]] | None]:
        """Evaluate CI + review signals for the current head into a verdict.

        Returns the verdict alongside the fetched PR issue comments (when the
        fully-remote branch loaded them, else None) so the caller can re-use
        them for the Codex-LGTM check.
        """
        issue_comments: list[dict[str, object]] | None = None
        # Only fetch review signals when CI is clean — Rule 1 (failing CI)
        # pre-empts all comment/review rules, so avoid the extra API calls.
        has_blocking_ci = any(
            c.required is not False
            and c.status == "completed"
            and c.conclusion in BLOCKING_CHECK_CONCLUSIONS
            for c in ci_runs
        )
        if has_blocking_ci:
            verdict = review_classifier(
                comments=[],
                ci=ci_runs,
                snapshot=ReviewSnapshot(
                    head_sha=head_sha,
                    head_committed_at=head_committed_at,
                    mergeable=mergeable,
                ),
            )
            if not should_dispatch_fix_run(
                prev_signature=state.last_trigger_signature,
                new_signature=verdict.trigger_signature,
            ):
                # Red CI normally pre-empts review signals, but once that exact
                # CI failure has already dispatched a fix-run we still need
                # to notice later review comments on the same head. Local-only
                # bindings still ignore Codex bot signals here; they only
                # honor human review-state changes.
                try:
                    raw_reviews = await (await self._gh_client()).pr_reviews(
                        pr_number, repo=binding.github_repo
                    )
                    review_signal_reviews = _reviews_from_github(raw_reviews)
                except GitHubError as e:
                    log.warning(
                        "could not fetch PR reviews for %s#%d: %s",
                        binding.github_repo,
                        pr_number,
                        e,
                    )
                    review_signal_reviews = ()

                review_signal_comments: list[ReviewComment] = []
                review_signal_reactions: tuple[Reaction, ...] = ()
                if remote_review:
                    try:
                        raw_comments = await (await self._gh_client()).pr_review_comments(
                            pr_number, repo=binding.github_repo
                        )
                        review_signal_comments = _review_comments_from_github(raw_comments)
                    except GitHubError as e:
                        log.warning(
                            "could not fetch PR review comments for %s#%d: %s",
                            binding.github_repo,
                            pr_number,
                            e,
                        )
                        review_signal_comments = []

                    try:
                        raw_reactions = await (await self._gh_client()).pr_reactions(
                            pr_number, repo=binding.github_repo
                        )
                        review_signal_reactions = _reactions_from_github(raw_reactions)
                    except GitHubError as e:
                        log.warning(
                            "could not fetch PR reactions for %s#%d: %s",
                            binding.github_repo,
                            pr_number,
                            e,
                        )
                        review_signal_reactions = ()
                else:
                    review_signal_reviews = tuple(
                        r for r in review_signal_reviews if not is_codex_author(r.user_login)
                    )

                review_verdict = review_classifier(
                    comments=review_signal_comments,
                    ci=[],
                    snapshot=ReviewSnapshot(
                        head_sha=head_sha,
                        head_committed_at=head_committed_at,
                        reviews=review_signal_reviews,
                        reactions=review_signal_reactions,
                        mergeable=mergeable,
                    ),
                )
                if review_verdict.kind is VerdictKind.CHANGES_REQUESTED:
                    verdict = review_verdict
        elif not remote_review:
            try:
                raw_reviews = await (await self._gh_client()).pr_reviews(
                    pr_number, repo=binding.github_repo
                )
                human_reviews = tuple(
                    r
                    for r in _reviews_from_github(raw_reviews)
                    if not is_codex_author(r.user_login)
                )
            except GitHubError as e:
                log.warning(
                    "could not fetch PR reviews for %s#%d: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
                human_reviews = ()

            verdict = review_classifier(
                comments=[],
                ci=ci_runs,
                snapshot=ReviewSnapshot(
                    head_sha=head_sha,
                    head_committed_at=head_committed_at,
                    reviews=human_reviews,
                    mergeable=None,
                ),
            )
        else:
            try:
                raw_reviews = await (await self._gh_client()).pr_reviews(
                    pr_number, repo=binding.github_repo
                )
                reviews: tuple[Review, ...] = _reviews_from_github(raw_reviews)
            except GitHubError as e:
                log.warning(
                    "could not fetch PR reviews for %s#%d: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
                reviews = ()

            try:
                raw_comments = await (await self._gh_client()).pr_review_comments(
                    pr_number, repo=binding.github_repo
                )
                comments: list[ReviewComment] = _review_comments_from_github(raw_comments)
            except GitHubError as e:
                log.warning(
                    "could not fetch PR review comments for %s#%d: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
                comments = []

            try:
                raw_reactions = await (await self._gh_client()).pr_reactions(
                    pr_number, repo=binding.github_repo
                )
                reactions: tuple[Reaction, ...] = _reactions_from_github(raw_reactions)
            except GitHubError as e:
                log.warning(
                    "could not fetch PR reactions for %s#%d: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
                reactions = ()

            try:
                issue_comments = await (await self._gh_client()).pr_issue_comments(
                    pr_number, repo=binding.github_repo
                )
            except GitHubError as e:
                log.warning(
                    "could not fetch PR issue comments for %s#%d: %s",
                    binding.github_repo,
                    pr_number,
                    e,
                )
                issue_comments = []

            verdict = review_classifier(
                comments=comments,
                ci=ci_runs,
                snapshot=ReviewSnapshot(
                    head_sha=head_sha,
                    head_committed_at=head_committed_at,
                    reviews=reviews,
                    reactions=(
                        *reactions,
                        *_codex_lgtm_reactions_from_issue_comments(issue_comments),
                    ),
                    mergeable=mergeable,
                ),
            )

        return verdict, issue_comments

    async def _dispatch_review_changes_requested_fix(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        state: db.review_state.ReviewState,
        checks: PRChecks,
        verdict: Verdict,
    ) -> bool:
        """Dispatch the fix-run a CHANGES_REQUESTED verdict calls for.

        Routes to the merge-conflict, failing-CI, or review-comment fix path,
        parking for approval at the iteration cap and gating on the per-issue
        token budget and the transient-retry backoff.
        """
        storage_issue_id = run.issue_id
        # Soft per-issue token-budget gate at the remote-review / merge-gate
        # fix-run dispatch boundary: park instead of dispatching the next fix.
        if await self._maybe_park_for_token_budget(storage_issue_id, run.id, binding):
            return True
        if verdict.merge_conflict:
            if not should_dispatch_fix_run(
                prev_signature=state.last_trigger_signature,
                new_signature=verdict.trigger_signature,
            ):
                return False
            if has_hit_iteration_cap(
                iteration=state.iteration, cap=self.config.review_iteration_cap
            ):
                if await self._review_poll_deferred_by_deliver_failed_wait(
                    storage_issue_id, run.id
                ):
                    return False
                await self._park_review_for_approval(
                    run=run,
                    binding=binding,
                    issue=issue,
                    trigger=verdict.trigger_signature,
                )
                return True
            if await self._agent_infra_retry_backoff_active(storage_issue_id):
                return False
            dispatched = await self._dispatch_merge_conflict_fix_run(
                run=run,
                binding=binding,
                issue=issue,
                iteration=state.iteration + 1,
            )
            if dispatched:
                await db.review_state.bump_iteration(self._conn, storage_issue_id)
                # Clear rather than set the signature: if the rebase produced no
                # new commit (HEAD SHA unchanged), we still want the next poll to
                # re-evaluate instead of being blocked by the dedup gate.
                await db.review_state.set_signature(self._conn, storage_issue_id, "")
            return dispatched
        if not should_dispatch_fix_run(
            prev_signature=state.last_trigger_signature,
            new_signature=verdict.trigger_signature,
        ):
            return False
        if has_hit_iteration_cap(iteration=state.iteration, cap=self.config.review_iteration_cap):
            if await self._review_poll_deferred_by_deliver_failed_wait(storage_issue_id, run.id):
                return False
            await self._park_review_for_approval(
                run=run,
                binding=binding,
                issue=issue,
                trigger=verdict.trigger_signature,
            )
            return True

        if await self._agent_infra_retry_backoff_active(storage_issue_id):
            return False

        iteration = state.iteration + 1
        if verdict.rule == "failing_ci":
            dispatched = await self._dispatch_ci_fix_run(
                run=run,
                binding=binding,
                issue=issue,
                checks=checks,
                verdict=verdict,
                iteration=iteration,
            )
        else:
            dispatched = await self._dispatch_review_comment_fix_run(
                run=run,
                binding=binding,
                issue=issue,
                verdict=verdict,
                iteration=iteration,
            )
        if dispatched:
            await db.review_state.bump_iteration(self._conn, storage_issue_id)
            await db.review_state.set_signature(
                self._conn, storage_issue_id, verdict.trigger_signature
            )
        return dispatched

    async def _dispatch_ci_fix_run(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        checks: PRChecks,
        verdict: Verdict,
        iteration: int,
    ) -> bool:
        if await self._review_poll_deferred_by_deliver_failed_wait(run.issue_id, run.id):
            return False
        log_tail = await self._failing_check_log_tail(
            checks=checks,
            verdict=verdict,
            repo=binding.github_repo,
        )
        trigger = (
            f"Failing required CI checks: {', '.join(verdict.failing_checks)}\n"
            f"Trigger signature: {verdict.trigger_signature}\n"
            f"Review iteration: {iteration}/{self.config.review_iteration_cap}"
        )
        prompt = review_fix_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
            trigger=trigger,
            failing_check_log_tail=log_tail,
        )
        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
        start_sha = ""

        async def on_acquire_failure(e: Exception) -> None:
            await self._fail_review_run(
                run=run,
                binding=binding,
                issue=issue,
                error=f"workspace acquire failed: {e}",
                last_log=str(e),
            )

        async def setup(workspace_path: Path) -> bool:
            nonlocal start_sha
            try:
                await _git_fetch_branch(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"could not fetch review fix-run remote HEAD for {branch}: {e}",
                    last_log=str(e),
                    auto_retry=False,
                    operator_wait=True,
                )
                return False

            start_sha = await _workspace_ref_sha(workspace_path, f"origin/{branch}")
            if not start_sha:
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
            return True

        async def body(
            workspace_path: Path,
            fix_run_id: str,
            drop_dispatch_id: Callable[[], None],
        ) -> bool:
            try:
                prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
                usage_delta, final_kind, final_returncode = await self._run_fix_agent(
                    binding=binding,
                    issue=issue,
                    run_id=fix_run_id,
                    workspace_path=workspace_path,
                    prompt=prompt,
                    prior_total=prior_total,
                )
            except Exception as e:  # noqa: BLE001
                log.exception("review fix-run execution failed for %s", issue.identifier)
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        exc=e,
                        reason=f"review fix-run execution failed: {e}",
                    ),
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"review fix-run execution failed: {e}",
                    last_log=str(e),
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
                        reason=f"review fix-run ended with {final_kind}",
                    ),
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"review fix-run ended with {final_kind}",
                    last_log="",
                )
                return False

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

            local_review_result: LoopResult | None = None
            local_only_review = (
                binding.resolved_local_review() and not binding.resolved_remote_review()
            )
            if local_only_review:
                local_review_result = await self._run_local_review_phase(
                    binding=binding,
                    issue=issue,
                    storage_issue_id=run.issue_id,
                    workspace_path=workspace_path,
                    parent_run_id=fix_run_id,
                )
                if _local_review_infra_failed(local_review_result):
                    await self._block_local_only_review_infra_failure(
                        binding=binding,
                        issue=issue,
                        storage_issue_id=run.issue_id,
                        run_id=run.id,
                        result=local_review_result,
                    )
                    return False

            try:
                await self._push_fn(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning("git push failed for review fix-run %s: %s", issue.identifier, e)
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"push failed: {e}",
                    last_log=str(e),
                )
                return False

            state = await db.review_state.get(self._conn, issue.id)
            if local_only_review:
                if _local_review_needs_approval(local_review_result):
                    await self._park_local_only_review_needs_approval(
                        run=run,
                        binding=binding,
                        issue=issue,
                        pr_url=_pr_url_for_state(
                            repo=binding.github_repo,
                            pr_number=state.pr_number,
                            pr_url=state.pr_url,
                        ),
                        result=local_review_result,
                    )
                    return True
                if (
                    local_review_result is None
                    or local_review_result.outcome != LoopOutcome.APPROVED
                ):
                    await self._fail_review_run(
                        run=run,
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
                    return False
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

        result = await self._run_fix_dispatch(
            binding=binding,
            issue=issue,
            ignored_stages=("review",),
            on_acquire_failure=on_acquire_failure,
            body=body,
            setup=setup,
        )
        return bool(result)

    async def _retrigger_codex_review_unless_approved(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        state: db.review_state.ReviewState,
        require_no_signal: bool = False,
    ) -> bool:
        """Post @codex review unless current PR state makes it unnecessary.

        Returns False only when the attempt was inconclusive and should be
        retried by a resurrection caller.
        """
        if not binding.resolved_remote_review():
            log.debug(
                "skipping automatic @codex review re-trigger for %s: remote_review disabled",
                issue.identifier,
            )
            return True
        if state.pr_number is None:
            return True
        head_sha = ""
        try:
            (
                verdict,
                head_sha,
                head_committed_at,
                issue_comments,
            ) = await self._review_verdict_and_head_for_pr(
                binding=binding,
                pr_number=state.pr_number,
                include_comments=require_no_signal,
            )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "could not classify approval before re-triggering @codex review on %s#%d: %s",
                binding.github_repo,
                state.pr_number,
                e,
            )
            if require_no_signal:
                return False
        else:
            if verdict.kind is VerdictKind.APPROVED:
                log.info(
                    "skipping @codex review re-trigger on %s#%d for %s: approval already present",
                    binding.github_repo,
                    state.pr_number,
                    issue.identifier,
                )
                return True
            if require_no_signal and verdict.kind is VerdictKind.CHANGES_REQUESTED:
                log.info(
                    "skipping @codex review re-trigger on %s#%d for %s: "
                    "current head already has review feedback (%s)",
                    binding.github_repo,
                    state.pr_number,
                    issue.identifier,
                    verdict.rule,
                )
                return True
            if require_no_signal and verdict.rule != "no_signal":
                log.info(
                    "skipping @codex review re-trigger on %s#%d for %s: "
                    "current review verdict is pending via %s",
                    binding.github_repo,
                    state.pr_number,
                    issue.identifier,
                    verdict.rule,
                )
                return True
            if require_no_signal:
                if _has_codex_review_request_after_head(
                    issue_comments,
                    head_committed_at=head_committed_at,
                ):
                    log.info(
                        "skipping duplicate @codex review re-trigger on %s#%d "
                        "for %s at %s: request comment already exists",
                        binding.github_repo,
                        state.pr_number,
                        issue.identifier,
                        head_sha,
                    )
                    return True
        posted = await self._retrigger_codex_review(
            binding=binding,
            state=state,
        )
        return posted

    async def _review_verdict_and_head_for_pr(
        self,
        *,
        binding: RepoBinding,
        pr_number: int,
        include_comments: bool = False,
    ) -> tuple[Verdict, str, str, list[dict[str, object]]]:
        view = await (await self._gh_client()).pr_view(pr_number, repo=binding.github_repo)
        head_sha = str(view.get("headRefOid") or "")
        if not head_sha:
            raise GitHubError(f"pr view missing headRefOid for {binding.github_repo}#{pr_number}")
        comments = []
        if include_comments:
            comments = await (await self._gh_client()).pr_review_comments(
                pr_number, repo=binding.github_repo
            )
        reviews = await (await self._gh_client()).pr_reviews(pr_number, repo=binding.github_repo)
        reactions = await (await self._gh_client()).pr_reactions(
            pr_number, repo=binding.github_repo
        )
        try:
            issue_comments = await (await self._gh_client()).pr_issue_comments(
                pr_number,
                repo=binding.github_repo,
            )
        except GitHubError as e:
            if include_comments:
                raise
            log.warning(
                "could not fetch PR issue comments for %s#%d: %s",
                binding.github_repo,
                pr_number,
                e,
            )
            issue_comments = []
        committed_at = await (await self._gh_client()).commit_committed_at(
            binding.github_repo, head_sha
        )

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
        return (
            review_classifier(
                comments=_review_comments_from_github(comments),
                ci=[],
                snapshot=snapshot,
            ),
            head_sha,
            committed_at,
            issue_comments,
        )

    async def _retrigger_codex_review(
        self,
        *,
        binding: RepoBinding,
        state: db.review_state.ReviewState,
    ) -> bool:
        if state.pr_number is None:
            return False
        try:
            await (await self._gh_client()).pr_comment(
                state.pr_number,
                "@codex review",
                repo=binding.github_repo,
            )
        except GitHubError as e:
            log.warning(
                "could not re-trigger @codex review on %s#%d: %s",
                binding.github_repo,
                state.pr_number,
                e,
            )
            return False
        return True

    def _format_comment_trigger(self, verdict: Verdict, iteration: int) -> str:
        cap = self.config.review_iteration_cap
        suffix = (
            f"\nTrigger signature: {verdict.trigger_signature}\nReview iteration: {iteration}/{cap}"
        )
        if verdict.codex_comments:
            parts = []
            for c in verdict.codex_comments[:5]:
                loc = f"`{c.path}`" + (f" line {c.line}" if c.line else "")
                body_snippet = c.body[:300].replace("\n", " ")
                parts.append(f"- {loc}: {body_snippet}")
            return "Reviewer inline comments:\n" + "\n".join(parts) + suffix
        if verdict.last_review_body:
            return "Reviewer feedback:\n" + verdict.last_review_body[:500] + suffix
        return "Reviewer requested changes." + suffix

    async def _dispatch_review_comment_fix_run(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        verdict: Verdict,
        iteration: int,
    ) -> bool:
        if await self._review_poll_deferred_by_deliver_failed_wait(run.issue_id, run.id):
            return False
        state = await db.review_state.get(self._conn, issue.id)
        pr_url = _pr_url_for_state(
            repo=binding.github_repo,
            pr_number=state.pr_number,
            pr_url=state.pr_url,
        )
        trigger = self._format_comment_trigger(verdict, iteration)
        prompt = review_comment_fix_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
            trigger=trigger,
        )
        tracker = self.tracker(binding)
        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
        start_sha = ""

        async def on_acquire_failure(e: Exception) -> None:
            await self._fail_review_run(
                run=run,
                binding=binding,
                issue=issue,
                error=f"workspace acquire failed: {e}",
                last_log=str(e),
            )

        async def setup(workspace_path: Path) -> bool:
            nonlocal start_sha
            try:
                await _git_fetch_branch(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"could not fetch review fix-run remote HEAD for {branch}: {e}",
                    last_log=str(e),
                    auto_retry=False,
                    operator_wait=True,
                )
                return False

            start_sha = await _workspace_ref_sha(workspace_path, f"origin/{branch}")
            if not start_sha:
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
            return True

        async def body(
            workspace_path: Path,
            fix_run_id: str,
            drop_dispatch_id: Callable[[], None],
        ) -> bool:
            nonlocal state
            # Post the "starting" comment only after dedup succeeds so we do
            # not announce a fix-run that will not execute (SYM-152).
            v = CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=pr_url,
                review_iter=iteration,
                trigger=verdict.trigger_signature[:80],
            )
            try:
                try:
                    await tracker.post_comment(issue.id, truncate_body(reviewing_feedback(v)))
                except LinearError as e:
                    log.warning(
                        "could not post reviewing_feedback comment for %s: %s",
                        issue.identifier,
                        e,
                    )
                prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
                usage_delta, final_kind, final_returncode = await self._run_fix_agent(
                    binding=binding,
                    issue=issue,
                    run_id=fix_run_id,
                    workspace_path=workspace_path,
                    prompt=prompt,
                    prior_total=prior_total,
                )
            except Exception as e:  # noqa: BLE001
                log.exception("review fix-run execution failed for %s", issue.identifier)
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        exc=e,
                        reason=f"review fix-run execution failed: {e}",
                    ),
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"review fix-run execution failed: {e}",
                    last_log=str(e),
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
                        reason=f"review fix-run ended with {final_kind}",
                    ),
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"review fix-run ended with {final_kind}",
                    last_log="",
                )
                return False

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

            try:
                await self._push_fn(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning("git push failed for review fix-run %s: %s", issue.identifier, e)
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"push failed: {e}",
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
                await tracker.post_comment(issue.id, truncate_body(fix_pushed(v_done)))
            except LinearError as e:
                log.warning("could not post fix_pushed comment for %s: %s", issue.identifier, e)

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

        result = await self._run_fix_dispatch(
            binding=binding,
            issue=issue,
            ignored_stages=("review",),
            on_acquire_failure=on_acquire_failure,
            body=body,
            setup=setup,
        )
        return bool(result)

    async def _dispatch_merge_conflict_fix_run(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        iteration: int,
    ) -> bool:
        if await self._review_poll_deferred_by_deliver_failed_wait(run.issue_id, run.id):
            return False
        base_branch = binding.base_branch
        if base_branch is None:
            try:
                base_branch = await (await self._gh_client()).repo_default_branch(
                    binding.github_repo
                )
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
        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
        start_sha = ""

        async def on_acquire_failure(e: Exception) -> None:
            await self._fail_review_run(
                run=run,
                binding=binding,
                issue=issue,
                error=f"workspace acquire failed: {e}",
                last_log=str(e),
            )

        async def setup(workspace_path: Path) -> bool:
            nonlocal start_sha
            # Step 1: orchestrator fetches origin.
            try:
                await _sync_workspace_to_remote(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning("workspace sync failed for %s: %s", issue.identifier, e)
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
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"git fetch failed: {e}",
                    last_log=str(e),
                )
                return False
            return True

        async def after_dedup(_fix_run_id: str) -> None:
            # Post the "fixing" comment once dedup has passed.
            v_start = CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=pr_url,
                review_iter=iteration,
            )
            try:
                await tracker.post_comment(issue.id, truncate_body(fixing_merge_conflict(v_start)))
            except LinearError as e:
                log.warning(
                    "could not post fixing_merge_conflict comment for %s: %s",
                    issue.identifier,
                    e,
                )

        async def body(
            workspace_path: Path,
            fix_run_id: str,
            drop_dispatch_id: Callable[[], None],
        ) -> bool:
            nonlocal state
            # Step 2: orchestrator attempts the rebase.
            upstream = f"origin/{base_branch or 'main'}"
            try:
                rebase_clean = await _git_rebase(workspace_path, upstream)
            except Exception as e:  # noqa: BLE001
                log.warning("git rebase failed for %s: %s", issue.identifier, e)
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        exc=e,
                        reason=f"git rebase failed: {e}",
                    ),
                )
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
                    error = "rebase failed with no unresolved paths"
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

            cumulative_usage = UsageDelta()
            if rebase_clean:
                # No conflicts: skip the agent entirely.
                log.info("rebase was clean for %s; skipping agent", issue.identifier)
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
                    log.warning("rebase --continue failed for %s: %s", issue.identifier, e)
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

            drop_dispatch_id()
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
                await tracker.post_comment(issue.id, truncate_body(fix_pushed(v_done)))
            except LinearError as e:
                log.warning("could not post fix_pushed comment for %s: %s", issue.identifier, e)

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

        result = await self._run_fix_dispatch(
            binding=binding,
            issue=issue,
            ignored_stages=("review",),
            on_acquire_failure=on_acquire_failure,
            body=body,
            setup=setup,
            after_dedup=after_dedup,
        )
        return bool(result)

    async def _validate_review_fix_advanced(
        self,
        *,
        run: db.runs.Run,
        fix_run_id: str,
        binding: RepoBinding,
        issue: LinearIssue,
        workspace_path: Path,
        branch: str,
        start_sha: str,
    ) -> str:
        current_sha = await _workspace_head_sha(workspace_path)
        if current_sha and current_sha != start_sha:
            return current_sha

        short_sha = (current_sha or start_sha)[:12] or "(unknown)"
        status_short = await _git_status_short(workspace_path)
        last_log = f"git status --short:\n{status_short}" if status_short else ""
        reason = f"review fix-run completed without advancing {branch}; HEAD stayed at {short_sha}"
        # Before escalating to operator wait, check whether the fix agent hit a
        # transient provider API error (exit 0, no HEAD advance). If so, requeue
        # with backoff instead of parking the issue — the review polling loop
        # will re-dispatch the fix once the backoff window elapses.
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
            # _maybe_requeue_transient_agent_failure already stamped fix_run_id
            # via _fail_run; just clear review rearm state and return.
            await self._clear_review_rearm_retry(run.id)
            self._clear_review_no_signal_rearm_heads(run.id)
            return ""
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
        await self._fail_review_run(
            run=run,
            binding=binding,
            issue=issue,
            error=reason,
            last_log=last_log,
            auto_retry=False,
            operator_wait=True,
        )
        return ""

    async def _failing_check_log_tail(
        self,
        *,
        checks: PRChecks,
        verdict: Verdict,
        repo: str,
    ) -> str:
        failing_names = set(verdict.failing_checks)
        sections: list[str] = []
        for check in checks.runs:
            if check.name not in failing_names:
                continue
            try:
                tail = await (await self._gh_client()).check_log_tail(check, repo=repo)
            except GitHubError as e:
                tail = f"(could not fetch failing-check log: {e})"
            if not tail:
                suffix = f"; see {check.link}" if check.link else ""
                tail = f"(no failing-check log excerpt available{suffix})"
            sections.append(f"## {check.name}\n\n{tail}")
        return "\n\n".join(sections)

    async def _track_review_failed_wait(
        self, issue_id: str, run_id: str, binding: RepoBinding
    ) -> None:
        self._dispatch_run_ids[issue_id] = run_id
        self._operator_wait_run_ids.add(run_id)
        self._review_failed_run_bindings[run_id] = binding
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue_id,
            run_id=run_id,
            kind=db.operator_waits.KIND_REVIEW_FAILED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )

    async def _track_review_stopped_wait(
        self, issue_id: str, run_id: str, binding: RepoBinding
    ) -> None:
        self._dispatch_run_ids[issue_id] = run_id
        self._operator_wait_run_ids.add(run_id)
        self._review_failed_run_bindings[run_id] = binding
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue_id,
            run_id=run_id,
            kind=db.operator_waits.KIND_REVIEW_STOPPED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )

    async def _handle_review_failed_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._review_failed_run_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(
                    db.operator_waits.KIND_REVIEW_FAILED,
                    db.operator_waits.KIND_REVIEW_STOPPED,
                ),
            )
            if binding is None:
                return
        tracker_issue_id, _ = await self._tracker_identity_for_issue(issue_id)
        tracker = self.tracker(binding)
        if intent.kind not in (SlashKind.RETRY, SlashKind.APPROVE):
            if intent.kind in (SlashKind.REJECT, SlashKind.STOP):
                states = await self._states_for_binding(binding)
                blocked_id = states.get(binding.linear_states.blocked)
                try:
                    issue = await tracker.lookup_issue(tracker_issue_id)
                except LinearError as e:
                    log.warning("could not look up %s for reject: %s", issue_id, e)
                    raise SlashHandlerFailure(
                        slash_text=self._slash_text(intent),
                        reason=f"could not look up issue for reject: {e}",
                    ) from e
                if blocked_id is not None:
                    try:
                        await tracker.move_issue(tracker_issue_id, blocked_id)
                    except LinearError as e:
                        log.warning("could not move %s to blocked: %s", issue.identifier, e)
                        raise SlashHandlerFailure(
                            slash_text=self._slash_text(intent),
                            reason=f"could not move issue to blocked state: {e}",
                        ) from e
                await self._clear_operator_wait(issue_id, run_id)
            else:
                log.info("slash %s for review-failed run %s ignored", intent.kind, run_id)
            return

        # $retry or $approve: restart review. Local-only retries must produce
        # a fresh local-review approval before the passive monitor can help.
        # Look up the issue BEFORE clearing the operator wait — if lookup
        # fails we want the wait (and its `_dispatch_run_ids` entry) to
        # survive so the next poll tick can retry. Clearing first would
        # make the issue invisible to slash polling on the retry.
        try:
            issue = await tracker.lookup_issue(tracker_issue_id)
        except LinearError as e:
            log.warning("could not look up %s for retry: %s", issue_id, e)
            raise SlashHandlerFailure(
                slash_text=self._slash_text(intent),
                reason=f"could not look up issue for retry: {e}",
            ) from e
        await self._resume_review_monitor(
            binding=binding,
            issue=issue,
            issue_id=issue_id,
            tracker_issue_id=tracker_issue_id,
            run_id=run_id,
        )

    async def _resume_review_monitor(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        issue_id: str,
        tracker_issue_id: str,
        run_id: str,
    ) -> None:
        """Clear the operator wait, re-create a `review` run and re-arm the
        passive review monitor.

        Shared by review-failed `$retry` and budget-exceeded `$approve`
        resumes. Re-dispatching review directly (instead of routing the issue
        back through the ready scan) is what keeps an open-PR issue clear of
        `_blocking_existing_pr` / `_park_already_has_pr`, which would otherwise
        bounce it to In Progress and strand it with the granted window wasted.
        """
        tracker = self.tracker(binding)
        await self._clear_operator_wait(issue_id, run_id)
        new_run_id = str(uuid.uuid4())
        now = self._now().isoformat()
        await db.runs.create(
            self._conn,
            id=new_run_id,
            issue_id=issue_id,
            stage="review",
            status="running",
            pid=None,
            started_at=now,
            binding_key=_binding_storage_key(binding),
        )
        run = db.runs.Run(
            id=new_run_id,
            issue_id=issue_id,
            stage="review",
            status="running",
            pid=None,
            started_at=now,
            ended_at=None,
            cost_usd=0.0,
        )
        state = await db.review_state.get(self._conn, issue_id)
        retry_local_gate = await self._review_retry_needs_local_gate(
            binding=binding,
            run=run,
        )
        if retry_local_gate:
            try:
                workspace_path = await self._workspace.acquire(binding, issue)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "workspace acquire failed for local review retry %s: %s",
                    issue.identifier,
                    e,
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"workspace acquire failed for local review retry: {e}",
                    last_log=str(e),
                    auto_retry=False,
                    operator_wait=True,
                )
                return
            try:
                local_review_result = await self._run_local_review_phase(
                    binding=binding,
                    issue=issue,
                    storage_issue_id=issue_id,
                    workspace_path=workspace_path,
                    parent_run_id=new_run_id,
                )
                local_review_permits_retry = (
                    binding.resolved_remote_review()
                    and _local_review_permits_remote(local_review_result)
                )
                if not (
                    local_review_permits_retry
                    or (
                        local_review_result is not None
                        and local_review_result.outcome == LoopOutcome.APPROVED
                    )
                ):
                    await self._fail_review_run(
                        run=run,
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
                    return
                branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
                try:
                    await self._push_fn(workspace_path, branch)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "git push failed after local review retry %s: %s",
                        issue.identifier,
                        e,
                    )
                    await self._fail_review_run(
                        run=run,
                        binding=binding,
                        issue=issue,
                        error=f"local review retry push failed: {e}",
                        last_log=str(e),
                        auto_retry=False,
                        operator_wait=True,
                    )
                    return
            finally:
                self._workspace.release(binding, issue)
        if state.pr_number is not None and binding.resolved_remote_review():
            try:
                await (await self._gh_client()).pr_comment(
                    state.pr_number, "@codex review", repo=binding.github_repo
                )
            except GitHubError as e:
                log.warning(
                    "could not re-post @codex review for %s#%d on retry: %s",
                    binding.github_repo,
                    state.pr_number,
                    e,
                )
        if binding.resolved_remote_review():
            await self._move_issue_to_review_state(binding=binding, issue=issue)
        self._schedule_review_poll(run, binding, issue)
        log.info("restarted review monitor for %s via $retry", issue.identifier)
        body = resumed(
            CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                run_id=new_run_id,
                next_stage="review",
            )
        )
        try:
            await tracker.post_comment(tracker_issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("retry comment failed for %s: %s", issue_id, e)

    async def _handle_skip_review_intent(self, issue_id: str, run_id: str) -> None:
        """Handle `$skip-review`: stop the review monitor and dispatch merge directly.

        This bypasses the Codex review verdict and is useful when the operator
        trusts the PR as-is. Valid whenever a review monitor is active for the
        issue — even if a concurrent review_fix run is the active dispatch run.
        """
        # A review_fix run may be active at the same time as the review monitor.
        # run_id may point to the fix run, not the monitor. Always look up the
        # monitor run ID directly so skip-review works regardless.
        monitor_run_id = self._review_poll_issue_ids.get(issue_id)
        if monitor_run_id is None or monitor_run_id not in self._review_poll_run_ids:
            try:
                tracker = await self._tracker_for_issue_id(issue_id)
                await tracker.post_comment(
                    issue_id,
                    truncate_body(
                        command_rejected(
                            "$skip-review",
                            "no active review monitor — cannot skip",
                        )
                    ),
                )
            except LinearError as e:
                log.warning("could not post skip-review rejection for %s: %s", issue_id, e)
            return

        tracker_ctx = await self._tracker_context_for_issue(issue_id)
        issue_tracker = self.tracker(tracker_ctx)
        try:
            issue = await issue_tracker.lookup_issue(issue_id)
        except LinearError as e:
            log.warning("could not look up %s for skip-review: %s", issue_id, e)
            return

        state = await db.review_state.get(self._conn, issue_id)
        if state.pr_number is None:
            try:
                await issue_tracker.post_comment(
                    issue_id,
                    truncate_body(command_rejected("$skip-review", "no PR found for this issue")),
                )
            except LinearError as e:
                log.warning("could not post skip-review rejection for %s: %s", issue_id, e)
            return

        binding = self._binding_for_review(issue, state, tracker_ctx=tracker_ctx)
        if binding is None:
            log.warning("no binding for skip-review on %s", issue.identifier)
            return
        tracker = self.tracker(binding)

        # A review_fix run might have been dispatched concurrently (or just
        # dispatched by the monitor task before it noticed the DB change).
        # Kill it before completing the monitor; if the process cannot be
        # stopped, leave Review active and do not race Merge against it.
        fix_run_id = self._dispatch_run_ids.get(issue_id)
        if fix_run_id is not None and fix_run_id != monitor_run_id:
            log.info(
                "skip-review: killing concurrent review_fix run %s for %s",
                fix_run_id,
                issue.identifier,
            )
            try:
                await self._runner.kill(fix_run_id)
            except Exception:  # noqa: BLE001
                log.exception("skip-review: could not kill fix run %s", fix_run_id)
                try:
                    await tracker.post_comment(
                        issue_id,
                        truncate_body(
                            command_rejected(
                                "$skip-review",
                                "could not stop active review fix-run",
                            )
                        ),
                    )
                except LinearError as e:
                    log.warning(
                        "could not post skip-review rejection for %s: %s",
                        issue.identifier,
                        e,
                    )
                return

        # Durably record the bypass *before* completing the monitor, so a
        # restart in the window before the merge run is created cannot let the
        # review-monitor resurrection re-open the feedback the operator skipped.
        await db.issue_prs.mark_review_bypassed(
            self._conn,
            issue_id=issue_id,
            github_repo=binding.github_repo,
            pr_number=state.pr_number,
        )
        # Mark the review run completed and cancel its asyncio task immediately so
        # it cannot dispatch any more fix runs mid-iteration.
        now = self._now().isoformat()
        await db.runs.update_status(self._conn, monitor_run_id, "completed", ended_at=now)
        monitor_task = self._review_poll_run_tasks.pop(monitor_run_id, None)
        if monitor_task is not None and not monitor_task.done():
            monitor_task.cancel()
        self._review_poll_run_ids.discard(monitor_run_id)
        await self._clear_review_rearm_retry(monitor_run_id)
        if self._review_poll_issue_ids.get(issue_id) == monitor_run_id:
            self._review_poll_issue_ids.pop(issue_id, None)

        if fix_run_id is not None and fix_run_id != monitor_run_id:
            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "interrupted",
                ended_at=now,
                kind="cancelled",
                detail="$approve interrupted active review fix-run for merge",
            )
            self._dispatch_run_ids.pop(issue_id, None)
            self._active_run_ids.discard(fix_run_id)

        # Dispatch merge directly, bypassing the review verdict check.
        # Reserved under `config_write_lock` so the drain guard's
        # `scheduled_slots` sample can't miss this reservation (SYM-193
        # review; see `_review_fix_dispatch_slot` in `_dispatch.py`).
        async with self._config_write_lock:
            self._schedule_merge(
                binding=binding,
                issue=issue,
                pr_number=state.pr_number,
                pr_url=state.pr_url,
                skip_review=True,
            )
        log.info(
            "skip-review: advancing %s (PR #%d) directly to merge",
            issue.identifier,
            state.pr_number,
        )

        v = CommentVars(
            stage="review",
            repo=binding.github_repo,
            issue=state.pr_number,
            pr_url=state.pr_url,
            run_id=monitor_run_id,
            next_stage="merge",
        )
        try:
            await tracker.post_comment(issue_id, truncate_body(skip_review_forced(v)))
        except LinearError as e:
            log.warning("could not post skip-review comment for %s: %s", issue.identifier, e)

    async def _resurrect_review_runs(self) -> list[asyncio.Task[None]]:
        """Restart review monitors whose PRs are still open but have no monitor.

        Two sources: review runs that died mid-flight (`list_orphaned_review_prs`),
        and review runs that *completed* without ever monitoring the GitHub
        review — e.g. `remote_review` was flipped on after the review stage
        finished, so the PR's `@codex` feedback now has no watcher
        (`list_completed_review_prs_without_monitor`). The completed case is
        gated on `remote_review` being on and the PR not yet approved, so a
        normally-approved PR awaiting merge is left alone. Guarded by
        REVIEW_RESURRECT_COOLDOWN_SECS so a persistently failing review does not
        spin at full poll speed.
        """
        scheduled: list[asyncio.Task[None]] = []
        for pr in await db.issue_prs.list_orphaned_review_prs(self._conn):
            task = await self._resurrect_one_review_monitor(
                pr, require_remote_review_unapproved=False
            )
            if task is not None:
                scheduled.append(task)
        for pr in await db.issue_prs.list_completed_review_prs_without_monitor(self._conn):
            task = await self._resurrect_one_review_monitor(
                pr, require_remote_review_unapproved=True
            )
            if task is not None:
                scheduled.append(task)
        return scheduled

    async def _resurrect_one_review_monitor(
        self,
        pr: db.issue_prs.IssuePR,
        *,
        require_remote_review_unapproved: bool,
    ) -> asyncio.Task[None] | None:
        if pr.issue_id in self._scheduled_issue_ids:
            return None
        if await db.runs.has_active(self._conn, pr.issue_id):
            return None
        # If there is already an operator wait (manual $retry pending), skip.
        if pr.issue_id in self._dispatch_run_ids:
            return None
        binding = self._binding_for_pr(pr)
        if binding is None:
            return None
        # `pr.issue_id` is the storage id; the tracker needs its own id (they
        # differ for contextual / provider-collision rows), so resolve it
        # before looking the issue up — mirroring the other poll paths.
        tracker_issue_id, _ = await self._tracker_identity_for_issue(pr.issue_id)
        tracker = self.tracker(binding)
        # Cooldown: skip if the last review run ended recently.
        last_review = await db.runs.latest_for_issue_stage(
            self._conn, issue_id=pr.issue_id, stage="review"
        )
        if last_review is not None and last_review.ended_at is not None:
            try:
                elapsed = (self._now() - _parse_rfc3339(last_review.ended_at)).total_seconds()
                if elapsed < REVIEW_RESURRECT_COOLDOWN_SECS:
                    return None
            except ValueError:
                pass
        try:
            issue = await tracker.lookup_issue(tracker_issue_id)
        except LinearError as e:
            log.warning(
                "could not look up orphaned review issue %s: %s",
                pr.identifier,
                e,
            )
            return None
        if not _review_issue_is_active(issue, binding):
            return None
        if require_remote_review_unapproved:
            # Only re-arm a completed-monitor PR when the GitHub review actually
            # matters here and is unresolved — never re-monitor an approved PR
            # that is merely awaiting merge.
            if not binding.resolved_remote_review():
                return None
            try:
                verdict = await self._review_verdict_for_pr(binding=binding, pr_number=pr.pr_number)
            except GitHubError as e:
                log.warning(
                    "could not classify review before re-arming monitor for %s#%d: %s",
                    binding.github_repo,
                    pr.pr_number,
                    e,
                )
                return None
            if verdict.kind is VerdictKind.APPROVED:
                return None
            log.info(
                "re-arming review monitor for %s (PR #%d): remote_review PR has "
                "no live monitor and is not approved (%s)",
                issue.identifier,
                pr.pr_number,
                verdict.rule or verdict.kind.value,
            )
        else:
            log.info(
                "resurrecting dead review monitor for %s (PR #%d)",
                issue.identifier,
                pr.pr_number,
            )
        now = self._now().isoformat()
        review_run_id = str(uuid.uuid4())
        # DB rows (`runs`, `review_state`, `issue_prs`) are keyed on the storage
        # id `pr.issue_id`; `issue.id` is the tracker id (it can differ for
        # contextual / provider-collision rows) and is only for tracker calls.
        await db.runs.create(
            self._conn,
            id=review_run_id,
            issue_id=pr.issue_id,
            stage="review",
            status="running",
            pid=None,
            started_at=now,
            binding_key=_binding_storage_key(binding),
        )
        state = await db.review_state.get(self._conn, pr.issue_id)
        body = resumed(
            CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                run_id=review_run_id,
                pr_url=_pr_url_for_state(
                    repo=binding.github_repo,
                    pr_number=state.pr_number,
                    pr_url=state.pr_url,
                ),
                next_stage="review",
            )
        )
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("resurrection comment failed for %s: %s", issue.identifier, e)
        run = db.runs.Run(
            id=review_run_id,
            issue_id=pr.issue_id,
            stage="review",
            status="running",
            pid=None,
            started_at=now,
            ended_at=None,
            cost_usd=0.0,
        )
        if binding.resolved_remote_review():
            await self._move_issue_to_review_state(binding=binding, issue=issue)
        task = self._schedule_review_poll(run, binding, issue)
        rearm_done = await self._retrigger_codex_review_unless_approved(
            binding=binding,
            issue=issue,
            state=state,
            require_no_signal=True,
        )
        if not rearm_done:
            await self._mark_review_rearm_retry(review_run_id)
        return task

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
    ) -> None:
        await db.runs.update_status(
            self._conn,
            run.id,
            "failed",
            ended_at=self._now().isoformat(),
            **_termination_kwargs(status="failed", reason=error),
        )
        await self._clear_review_rearm_retry(run.id)
        self._clear_review_no_signal_rearm_heads(run.id)
        tracker = self.tracker(binding)
        if operator_wait:
            await self._track_review_failed_wait(issue.id, run.id, binding)
            await self._notify_attention(
                event=EVENT_RUN_FAILED,
                issue_identifier=issue.identifier,
                issue_url=issue.url,
                dedupe_key=f"run_failed:{run.id}",
                detail=error,
            )
            try:
                states = await self._states_for_binding(binding)
                needs_approval_id = states.get(binding.linear_states.needs_approval)
                if needs_approval_id is not None:
                    await tracker.move_issue(issue.id, needs_approval_id)
                else:
                    log.warning(
                        "missing Linear needs_approval state %r for %s",
                        binding.linear_states.needs_approval,
                        issue.identifier,
                    )
            except LinearError as e:
                log.warning(
                    "could not move %s to needs_approval after review failure: %s",
                    issue.identifier,
                    e,
                )
        state = await db.review_state.get(self._conn, issue.id)
        tokens = await db.runs.tokens_for_issue(self._conn, issue.id)
        body = failed(
            CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=_pr_url_for_state(
                    repo=binding.github_repo,
                    pr_number=state.pr_number,
                    pr_url=state.pr_url,
                ),
                run_id=run.id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                error=error,
                last_log=last_log,
                auto_retry=auto_retry,
            )
        )
        if operator_wait:
            body += (
                "\nReply with `$retry` or `$approve` to resume review monitoring. "
                "Reply with `$reject` or `$stop` to leave it halted.\n"
            )
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("review failed comment failed on %s: %s", issue.identifier, e)

    async def _fail_orphaned_review_run(
        self,
        *,
        run: db.runs.Run,
        issue: LinearIssue,
        state: db.review_state.ReviewState,
        error: str,
    ) -> None:
        await db.runs.update_status(
            self._conn,
            run.id,
            "failed",
            ended_at=self._now().isoformat(),
            **_termination_kwargs(status="failed", reason=error),
        )
        await self._clear_review_rearm_retry(run.id)
        self._clear_review_no_signal_rearm_heads(run.id)
        repo = state.github_repo or "(unknown repo)"
        tokens = await db.runs.tokens_for_issue(self._conn, issue.id)
        body = failed(
            CommentVars(
                stage="review",
                repo=repo,
                issue=0,
                pr_url=_pr_url_for_state(
                    repo=repo,
                    pr_number=state.pr_number,
                    pr_url=state.pr_url,
                ),
                run_id=run.id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                error=error,
                last_log="",
                auto_retry=True,
            )
        )
        tracker = await self._tracker_for_issue_id(issue.id)
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "orphaned review failed comment failed on %s: %s",
                issue.identifier,
                e,
            )

    async def _park_review_for_approval(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        trigger: str,
    ) -> None:
        state = await db.review_state.get(self._conn, issue.id)
        tokens = await db.runs.tokens_for_issue(self._conn, issue.id)
        body = stuck_loop_escape(
            CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=0,
                pr_url=_pr_url_for_state(
                    repo=binding.github_repo,
                    pr_number=state.pr_number,
                    pr_url=state.pr_url,
                ),
                run_id=run.id,
                input_tokens=tokens.input_tokens,
                output_tokens=tokens.output_tokens,
                cache_write_tokens=tokens.cache_write_tokens,
                cache_read_tokens=tokens.cache_read_tokens,
                review_iter=state.iteration,
                trigger=trigger,
            )
        )
        tracker = self.tracker(binding)
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("stuck-loop comment failed on %s: %s", issue.identifier, e)
        try:
            states = await self._states_for_binding(binding)
            needs_approval_id = states.get(binding.linear_states.needs_approval)
            if needs_approval_id is not None:
                await tracker.move_issue(issue.id, needs_approval_id)
        except LinearError as e:
            log.warning("could not park %s for approval: %s", issue.identifier, e)
        await db.runs.update_status(
            self._conn,
            run.id,
            "completed",
            ended_at=self._now().isoformat(),
        )
        await self._clear_review_rearm_retry(run.id)

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
                raw = await (await self._gh_client()).pr_issue_comments(
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
            view = await (await self._gh_client()).pr_view(pr_number, repo=binding.github_repo)
        head_sha = str(view.get("headRefOid") or "")
        if not head_sha:
            raise GitHubError(f"pr view missing headRefOid for {binding.github_repo}#{pr_number}")

        checks = await (await self._gh_client()).pr_checks(pr_number, repo=binding.github_repo)
        ci = [_review_check_from_github(run) for run in checks.runs]
        if not binding.resolved_remote_review():
            human_reviews = tuple(
                r
                for r in _reviews_from_github(
                    await (await self._gh_client()).pr_reviews(pr_number, repo=binding.github_repo)
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

        comments = await (await self._gh_client()).pr_review_comments(
            pr_number,
            repo=binding.github_repo,
        )
        reviews = await (await self._gh_client()).pr_reviews(pr_number, repo=binding.github_repo)
        reactions = await (await self._gh_client()).pr_reactions(
            pr_number, repo=binding.github_repo
        )
        try:
            issue_comments = await (await self._gh_client()).pr_issue_comments(
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
        committed_at = await (await self._gh_client()).commit_committed_at(
            binding.github_repo, head_sha
        )

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
