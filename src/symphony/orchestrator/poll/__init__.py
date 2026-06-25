"""The always-running poll loop.

End-to-end scope (issue #7): scan each configured Linear team for issues
in the "ready" state with the configured label, then for each one:

1. Atomically insert a `runs` row (dedupe).
2. Post a 🚀 "starting" Linear comment.
3. Move the issue to the binding's `in_progress` state.
4. Acquire a per-issue workspace clone.
5. Spawn the binding's runner with the Implement prompt; stream events
   into `{log_root}/{run_id}.log` and accumulate cost / tokens.
6. On clean exit: push the branch, open a PR titled
   `[<LINEAR_ID>] <issue title>` with body `Relates to <linear-url>`,
   post a stage-transition comment, move the issue to the configured
   Review/needs-approval state, and start the Review monitor.
7. On any non-clean exit: mark the run failed; do not open a PR.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, Literal

import aiosqlite
import httpx

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
from ...agent.prompt import (
    acceptance_fix_prompt,
    implement_prompt,
    merge_conflict_rebase_fix_prompt,
    merge_prompt,
    merge_required_check_fix_prompt,
)
from ...agent.runner import RunnerSpec
from ...agent.runners.acceptance import quick_skip_trivial_acceptance, run_acceptance
from ...config import Config, RepoBinding
from ...github.branch_protection import get_required_contexts
from ...github.client import CheckRun as GitHubCheckRun
from ...github.client import GitHubError, _is_merge_conflict_error
from ...github.webhook import GitHubWebhookEvent
from ...linear.client import LinearError, comment_from_webhook_payload
from ...linear.slash import SlashIntent
from ...linear.templates import (
    CommentVars,
    acceptance_blocked,
    acceptance_rejected,
    awaiting_approval,
    budget_exceeded,
    command_rejected,
    failed,
    implement_already_satisfied,
    implement_blocked,
    resumed,
    run_started,
    stage_done,
    truncate_body,
)
from ...pipeline.acceptance_classifier import (
    AcceptanceScreenshot,
    AcceptanceVerdict,
    ExtractedCriterion,
    extract_acceptance_criteria,
    format_acceptance_criteria_comment,
    format_acceptance_verdict_comment,
)
from ...pipeline.cost_guard import (
    UsageCostEstimator,
    UsageDelta,
)
from ...pipeline.local_review import LocalVerdict, StreamApiError, extract_last_agent_message
from ...pipeline.local_review_loop import LoopOutcome, LoopResult
from ...pipeline.local_review_session import run_local_review_session
from ...pipeline.preview_resolver import (
    PreviewResolutionError,
    render_preview_url,
    resolve_preview_url,
)
from ...pipeline.review_classifier import (
    VerdictKind,
    has_hit_iteration_cap,
    should_dispatch_fix_run,
)
from ...pipeline.state_machine import classify_implement_completion, on_runner_event
from ...pipeline.taste_guide import load_taste_guide
from ...pipeline.verify import VerifyResult, run_verify_session
from ...tokens import effective_tokens
from ...tracker import (
    Comment as LinearComment,
)
from ...tracker import (
    Issue as LinearIssue,
)
from ...tracker import (
    TrackerContext,
)

# SYM-144: state + foundation methods live on `_OrchestratorBase`. The foundation
# binding/tracker free helpers, the two state dataclasses, and the `PushFn` /
# `BindingKey` aliases moved to `_base` too; re-exported here by explicit name so
# `poll.<name>` keeps resolving for the Orchestrator, `cli`, and tests.
from ._base import (
    BindingKey as BindingKey,
)
from ._base import (
    PushFn as PushFn,
)
from ._base import (
    _binding_key as _binding_key,
)
from ._base import (
    _ImplementHandoff as _ImplementHandoff,
)
from ._base import (
    _OrchestratorBase as _OrchestratorBase,
)
from ._base import (
    _PendingDelivery as _PendingDelivery,
)
from ._base import (
    _register_configured_trackers as _register_configured_trackers,
)
from ._base import (
    _state_cache_key as _state_cache_key,
)
from ._base import (
    _tracker_context_for_binding as _tracker_context_for_binding,
)

# SYM-149: the dispatch domain lives on `_DispatchMixin(_OrchestratorBase)`;
# `Orchestrator` inherits it. Re-exported by explicit name for tests.
from ._dispatch import (
    _DispatchMixin as _DispatchMixin,
)

# SYM-143: free functions moved to `_git` / `_helpers`; re-exported here by
# explicit name (redundant aliases mark them as intentional re-exports) so the
# Orchestrator, `cli`, and tests keep resolving `poll.<name>` unchanged.
from ._git import (
    _branch_ahead_of_base as _branch_ahead_of_base,
)
from ._git import (
    _default_force_push as _default_force_push,
)
from ._git import (
    _default_push as _default_push,
)
from ._git import (
    _git_abort_rebase as _git_abort_rebase,
)
from ._git import (
    _git_add_and_continue_rebase as _git_add_and_continue_rebase,
)
from ._git import (
    _git_conflicted_files as _git_conflicted_files,
)
from ._git import (
    _git_fetch as _git_fetch,
)
from ._git import (
    _git_fetch_branch as _git_fetch_branch,
)
from ._git import (
    _git_rebase as _git_rebase,
)
from ._git import (
    _git_status_short as _git_status_short,
)
from ._git import (
    _sync_workspace_to_remote as _sync_workspace_to_remote,
)
from ._git import (
    _workspace_commits_ahead as _workspace_commits_ahead,
)
from ._git import (
    _workspace_diff_size as _workspace_diff_size,
)
from ._git import (
    _workspace_dirty_files as _workspace_dirty_files,
)
from ._git import (
    _workspace_head_sha as _workspace_head_sha,
)
from ._git import (
    _workspace_ref_is_ancestor as _workspace_ref_is_ancestor,
)
from ._git import (
    _workspace_ref_landed_in_base as _workspace_ref_landed_in_base,
)
from ._git import (
    _workspace_ref_sha as _workspace_ref_sha,
)
from ._git import (
    _workspace_scrub as _workspace_scrub,
)
from ._helpers import (
    NEEDS_HUMAN_APPROVAL_LABEL as NEEDS_HUMAN_APPROVAL_LABEL,
)
from ._helpers import (
    _acceptance_degrade_note as _acceptance_degrade_note,
)
from ._helpers import (
    _acceptance_has_where_to_verify as _acceptance_has_where_to_verify,
)
from ._helpers import (
    _github_commit_url as _github_commit_url,
)
from ._helpers import (
    _needs_human_approval_label_present as _needs_human_approval_label_present,
)
from ._helpers import (
    _no_signal_head_check_state as _no_signal_head_check_state,
)
from ._helpers import (
    _normalize_acceptance_section_heading as _normalize_acceptance_section_heading,
)
from ._helpers import (
    _parse_optional_datetime as _parse_optional_datetime,
)
from ._helpers import (
    _parse_rfc3339 as _parse_rfc3339,
)
from ._helpers import (
    _pr_base_ref_from_view as _pr_base_ref_from_view,
)
from ._helpers import (
    _pr_url_for_state as _pr_url_for_state,
)
from ._helpers import (
    _pr_view_has_merge_conflict as _pr_view_has_merge_conflict,
)
from ._helpers import (
    _pr_view_is_clean_mergeable as _pr_view_is_clean_mergeable,
)
from ._helpers import (
    _pr_view_is_closed as _pr_view_is_closed,
)
from ._helpers import (
    _pr_view_is_merged as _pr_view_is_merged,
)
from ._helpers import (
    _pr_view_skips_required_check_fix as _pr_view_skips_required_check_fix,
)
from ._helpers import (
    _required_check_detail as _required_check_detail,
)
from ._helpers import (
    _required_check_trigger_signature as _required_check_trigger_signature,
)
from ._helpers import (
    _status_check_failed as _status_check_failed,
)
from ._helpers import (
    _status_check_identity as _status_check_identity,
)
from ._helpers import (
    _status_check_names as _status_check_names,
)
from ._helpers import (
    _status_check_run_id as _status_check_run_id,
)
from ._helpers import (
    _status_check_sha as _status_check_sha,
)
from ._helpers import (
    _status_check_succeeded as _status_check_succeeded,
)
from ._helpers import (
    _status_rollup_nodes as _status_rollup_nodes,
)
from ._helpers import (
    _sum_usage as _sum_usage,
)
from ._helpers import (
    build_fix_runner_command as build_fix_runner_command,
)
from ._helpers import (
    build_merge_runner_command as build_merge_runner_command,
)
from ._helpers import (
    build_pr_body as build_pr_body,
)
from ._helpers import (
    build_pr_title as build_pr_title,
)
from ._helpers import (
    build_runner_command as build_runner_command,
)
from ._helpers import (
    pr_number_from_url as pr_number_from_url,
)
from ._review import (
    _CODEX_REVIEWED_COMMIT_RE as _CODEX_REVIEWED_COMMIT_RE,
)
from ._review import (
    CI_FETCH_FAILURE_LIMIT as CI_FETCH_FAILURE_LIMIT,
)
from ._review import (
    CODEX_NO_ISSUES_MARKER as CODEX_NO_ISSUES_MARKER,
)
from ._review import (
    REVIEW_RESURRECT_COOLDOWN_SECS as REVIEW_RESURRECT_COOLDOWN_SECS,
)
from ._review import (
    SlashHandlerFailure as SlashHandlerFailure,
)
from ._review import (
    _abort_rebase_safely as _abort_rebase_safely,
)
from ._review import (
    _add_run_usage as _add_run_usage,
)
from ._review import (
    _codex_lgtm_reactions_from_issue_comments as _codex_lgtm_reactions_from_issue_comments,
)
from ._review import (
    _commit_committed_at_or_empty as _commit_committed_at_or_empty,
)
from ._review import (
    _has_codex_review_request_after_head as _has_codex_review_request_after_head,
)
from ._review import (
    _local_review_failure_log as _local_review_failure_log,
)
from ._review import (
    _local_review_infra_failed as _local_review_infra_failed,
)
from ._review import (
    _local_review_needs_approval as _local_review_needs_approval,
)
from ._review import (
    _local_review_permits_remote as _local_review_permits_remote,
)
from ._review import (
    _local_review_termination_reason as _local_review_termination_reason,
)
from ._review import (
    _reactions_from_github as _reactions_from_github,
)
from ._review import (
    _read_run_stream_api_error_obj as _read_run_stream_api_error_obj,
)
from ._review import (
    _review_check_from_gh as _review_check_from_gh,
)
from ._review import (
    _review_check_from_github as _review_check_from_github,
)
from ._review import (
    _review_comments_from_github as _review_comments_from_github,
)
from ._review import (
    _review_issue_is_active as _review_issue_is_active,
)
from ._review import (
    _ReviewMixin as _ReviewMixin,
)
from ._review import (
    _reviews_from_github as _reviews_from_github,
)
from ._review import (
    _termination_kwargs as _termination_kwargs,
)
from ._review import (
    _TerminationKwargs as _TerminationKwargs,
)
from ._review import (
    _unknown_head_ci_scope as _unknown_head_ci_scope,
)
from ._review import (
    _user_login as _user_login,
)

# SYM-145: the slash-command domain lives on `_SlashCommandsMixin(_OrchestratorBase)`;
# `Orchestrator` inherits it. The `SlashHandlerFailure` exception, the
# `MANUAL_MERGE_PARKED_RUN_PREFIX` constant, and the `_manual_merge_parked_run_id`
# helper moved alongside it; all re-exported by explicit name for tests and callers.
from ._slash_commands import (
    MANUAL_MERGE_PARKED_RUN_PREFIX as MANUAL_MERGE_PARKED_RUN_PREFIX,
)
from ._slash_commands import (
    SlashHandlerFailure as SlashHandlerFailure,
)
from ._slash_commands import (
    _manual_merge_parked_run_id as _manual_merge_parked_run_id,
)
from ._slash_commands import (
    _SlashCommandsMixin as _SlashCommandsMixin,
)

log = logging.getLogger(__name__)

MERGE_WAIT_RECONCILE_INTERVAL_SECS = 600
# Grace before an orphaned merge `needs_approval` run (operator wait gone) is
# retired — long enough to never race a freshly-created wait.
ORPHANED_MERGE_RUN_GRACE_SECS = 120
MERGED_LINEAR_STATE_RECONCILE_TICK_INTERVAL = 5
MERGED_LINEAR_STATE_RECONCILE_LOOKBACK_HOURS = 24
PARKED_CLOSED_UNMERGED_COMMENT = "🛑 PR closed without merge — marking done"
_CODE_ONLY_ACCEPTANCE_MODE = "code_only"
ACCEPTANCE_INFRA_RETRY_LIMIT = 2
# Shared, capped exponential-backoff knobs for every requeue-based infra-error
# retry path (acceptance + the agent stages). `AGENT_INFRA_RETRY_LIMIT` is the
# transient-API-error retry budget for reviewer/implement/fix runs before they
# fall through to the existing infra-failure escalation.
ACCEPTANCE_INFRA_RETRY_BASE_BACKOFF_SECS = 30
ACCEPTANCE_INFRA_RETRY_MAX_BACKOFF_SECS = 120
AGENT_INFRA_RETRY_LIMIT = 5
ACCEPTANCE_FIX_ITERATION_CAP = 1
# All transient-retry kinds share the same retry budget and backoff logic.
# TRANSIENT_API_RETRY_KIND: implement-phase failure (no work done, HEAD unchanged).
# LOCAL_REVIEW_TRANSIENT_RETRY_KIND: local-review-phase failure (implement succeeded,
#   commits intact, but reviewer got a transient 500 before verdicting).
# REVIEW_FIX_TRANSIENT_RETRY_KIND: review-stage fix agent failure (PR exists, commits
#   intact, but fix agent got a transient 500 and made no HEAD advance).
_AGENT_INFRA_RETRY_KINDS: frozenset[str] = frozenset({
    db.runs.TRANSIENT_API_RETRY_KIND,
    db.runs.LOCAL_REVIEW_TRANSIENT_RETRY_KIND,
    db.runs.REVIEW_FIX_TRANSIENT_RETRY_KIND,
})


class _AcceptancePrDiffUnavailable(RuntimeError):
    pass


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
        text = await asyncio.to_thread(
            log_path.read_text, encoding="utf-8", errors="replace"
        )
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


def _with_acceptance_degrade_note(
    verdict: AcceptanceVerdict, degrade_note: str | None
) -> AcceptanceVerdict:
    if not degrade_note:
        return verdict
    details = verdict.details.strip()
    if details.startswith(degrade_note):
        return verdict
    combined = degrade_note if not details else f"{degrade_note}\n\n{details}"
    return replace(verdict, details=combined)


def _acceptance_criterion_names(criteria: list[ExtractedCriterion]) -> list[str]:
    return [item["name"] for item in criteria if item["name"].strip()]


def _acceptance_criterion_predicates(criteria: list[ExtractedCriterion]) -> list[str]:
    return [item["predicate"] for item in criteria if item["predicate"].strip()]


def _replace_acceptance_criteria_labels(
    *,
    verdict: AcceptanceVerdict,
    criteria_names: list[str],
    criteria_predicates: list[str],
) -> AcceptanceVerdict:
    labels = dict(zip(criteria_predicates, criteria_names, strict=False))
    criterion_results = tuple(
        replace(
            item,
            criterion=labels.get(item.criterion, item.criterion),
        )
        for item in verdict.criterion_results
    )
    screenshots = tuple(
        replace(
            item,
            label=labels.get(item.label, item.label),
        )
        for item in verdict.screenshots
    )
    return replace(
        verdict,
        criteria=criteria_names,
        criterion_results=criterion_results,
        screenshots=screenshots,
    )


def _acceptance_artifact_path(workspace_path: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = workspace_path / path
    try:
        resolved = path.resolve(strict=False)
        workspace = workspace_path.resolve(strict=False)
    except RuntimeError as e:
        raise OSError(f"acceptance artifact path cannot be resolved: {raw_path}") from e
    try:
        resolved.relative_to(workspace)
    except ValueError as e:
        raise OSError(f"acceptance artifact path escapes workspace: {raw_path}") from e
    return resolved


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


# Codex's "no major issues" comment names the commit it reviewed, e.g.
# `**Reviewed commit:** ` + "`2668682eeb`". Capture that SHA so the classifier
# can require it to match the current HEAD before honouring the approval.


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
    reviewer_agent: Literal["claude", "codex"] = (
        "claude" if agent == "claude" else "codex"
    )
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


class Orchestrator(_ReviewMixin, _SlashCommandsMixin, _DispatchMixin):
    """Owns the poll loop. Dedupe is a SQLite query over the `runs` table."""

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

    def _validate_waiting_state(
        self, binding: RepoBinding, states: dict[str, str]
    ) -> None:
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





    async def run(self) -> None:
        """The single long-lived task. Cancellation-safe."""
        await self.warmup()
        await self._restore_operator_waits()
        await self._reconcile_orphaned_merge_runs(reason="startup")
        await self._reconcile_auto_recoverable_merge_waits(reason="startup")
        self._merge_wait_reconcile_task = asyncio.create_task(
            self._run_auto_recoverable_merge_wait_reconciler(self._shutdown)
        )
        self._reconcile_task = asyncio.create_task(
            self._reconciler.run(self._shutdown)
        )
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

    def _schedule_reconcile_task(
        self, awaitable: Awaitable[int], *, source: str
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(
            self._run_reconcile_task(awaitable, source=source)
        )
        self._reconcile_event_tasks.add(task)
        task.add_done_callback(self._reconcile_event_task_done)
        return task

    async def _run_reconcile_task(
        self, awaitable: Awaitable[int], *, source: str
    ) -> None:
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
        await self._restore_operator_waits()
        self._merged_linear_state_reconcile_ticks += 1
        if (
            self._merged_linear_state_reconcile_ticks
            % MERGED_LINEAR_STATE_RECONCILE_TICK_INTERVAL
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
            scheduled.extend(await self._scan_binding(binding))
        try:
            await self._poll_slash_commands()
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("slash command poll failed")
        return scheduled

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

    async def handle_github_webhook(
        self, event: GitHubWebhookEvent
    ) -> WebhookDispatchResult:
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
                source=(
                    "merge_wait.github."
                    f"{event.event_type}.{event.action or 'unknown'}"
                ),
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
            return WebhookDispatchResult(
                kind="comment", handled=False, detail="missing issue id"
            )
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
            if candidate_run_id is None or not self._slash_command_run_eligible(
                candidate_run_id
            ):
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
            return WebhookDispatchResult(
                kind="comment", handled=False, detail="no active run"
            )
        try:
            handled = await self._handle_unseen_slash_comment(
                storage_issue_id, run_id, comment
            )
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
            return WebhookDispatchResult(
                kind="issue", handled=False, detail="ignored action"
            )
        data = payload.get("data")
        if not isinstance(data, Mapping):
            return WebhookDispatchResult(
                kind="issue", handled=False, detail="missing issue data"
            )
        issue_id = data.get("id")
        if not isinstance(issue_id, str) or not issue_id:
            return WebhookDispatchResult(
                kind="issue", handled=False, detail="missing issue id"
            )
        state_changed = _linear_issue_state_changed(payload)
        issue, tracker_ctx = await self._lookup_webhook_issue(issue_id, provider=provider)
        storage_issue_id = await self._storage_issue_id_for_tracker_issue(
            issue.id, tracker_ctx
        )
        if state_changed:
            self._schedule_reconcile_task(
                self._reconciler.reconcile_linear_issue_event(
                    issue_id=storage_issue_id,
                    action=action or "update",
                ),
                source=f"linear.issue.{action or 'update'}",
            )
        old_state_id, old_state_name, new_state_id, new_state_name = (
            _linear_issue_state_transition(payload)
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
                detail=(
                    "parked manual merge revived"
                    if revived
                    else "issue is not dispatchable"
                ),
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
            self._register_operator_wait_binding(wait, binding)

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

    async def _track_acceptance_blocked_wait(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        run_id: str,
        verdict: AcceptanceVerdict,
    ) -> None:
        states: dict[str, str] = {}
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states while parking acceptance-blocked %s: %s",
                issue.identifier,
                e,
            )
        target_id = states.get(binding.linear_states.needs_approval) or states.get(
            binding.linear_states.blocked
        )
        tracker = self.tracker(binding)
        if target_id is not None:
            try:
                await tracker.move_issue(issue.id, target_id)
            except LinearError as e:
                log.warning(
                    "could not park acceptance-blocked %s: %s",
                    issue.identifier,
                    e,
                )

        body = acceptance_blocked(
            CommentVars(
                stage="acceptance",
                repo=binding.github_repo,
                issue=pr_number,
                pr_url=await self._acceptance_pr_url(issue.id),
                run_id=run_id,
                error=verdict.details,
            )
        )
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("acceptance blocked comment failed on %s: %s", issue.identifier, e)

        self._dispatch_run_ids[issue.id] = run_id
        self._operator_wait_run_ids.add(run_id)
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue.id,
            run_id=run_id,
            kind=db.operator_waits.KIND_ACCEPTANCE_BLOCKED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )

    async def _track_acceptance_rejected_wait(
        self, issue_id: str, run_id: str, binding: RepoBinding
    ) -> None:
        self._dispatch_run_ids[issue_id] = run_id
        self._operator_wait_run_ids.add(run_id)
        self._acceptance_rejected_run_bindings[run_id] = binding
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue_id,
            run_id=run_id,
            kind=db.operator_waits.KIND_ACCEPTANCE_REJECTED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )

    async def _acceptance_pr_url(self, issue_id: str) -> str:
        state = await db.acceptance_state.get(self._conn, issue_id)
        if state.pr_url:
            return state.pr_url
        if state.pr_number is not None:
            return f"#{state.pr_number}"
        return "(no PR yet)"


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

    async def _token_budget_ceiling(
        self, issue_id: str, binding: RepoBinding
    ) -> float | None:
        """Soft ceiling = `per_issue_token_budget + granted_token_budget`.

        Returns `None` when the gate is off for this binding (no global
        default and no per-binding override).
        """
        budget = binding.resolved_per_issue_token_budget(
            self.config.per_issue_token_budget
        )
        if budget is None:
            return None
        granted = await db.issues.get_granted_token_budget(self._conn, issue_id)
        return float(budget + granted)

    async def _would_exceed_token_budget(
        self, issue_id: str, binding: RepoBinding
    ) -> bool:
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
        breakdown = await db.runs.effective_tokens_by_stage_for_issue(
            self._conn, issue_id
        )
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

    async def drain_dispatch_tasks(self, *, cancel: bool = False) -> None:
        if cancel:
            await asyncio.gather(
                *(
                    self._kill_active_runner(run_id)
                    for run_id in tuple(self._active_run_ids)
                ),
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
            await db.runs.create(
                self._conn,
                id=fix_run_id,
                issue_id=issue.id,
                stage="review_fix",
                status="running",
                pid=None,
                started_at=self._now().isoformat(),
            )
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
                                "interrupted active merge run %s after required-check fix-run transient retry",
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
            await db.runs.create(
                self._conn,
                id=fix_run_id,
                issue_id=issue.id,
                stage="review_fix",
                status="running",
                pid=None,
                started_at=self._now().isoformat(),
            )
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
                binding
                for binding in matches
                if (binding.issue_label or "") == stored_label
            ]
            if len(labeled_matches) == 1:
                return labeled_matches[0]
            return None
        if len(matches) == 1:
            return matches[0]
        return None

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

    async def _acceptance_passed_for_candidate(
        self,
        candidate: db.issue_prs.IssuePR,
        binding: RepoBinding,
        pr_head_sha: str,
    ) -> bool:
        if not pr_head_sha:
            return False
        state = await db.acceptance_state.get(self._conn, candidate.issue_id)
        return (
            state.pr_number == candidate.pr_number
            and state.pr_url == candidate.pr_url
            and state.pr_head_sha == pr_head_sha
            and state.mode == binding.acceptance.mode
            and state.last_verdict == "pass"
        )

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

    async def _acceptance_infra_retry_backoff_active(self, issue_id: str) -> bool:
        state = await db.acceptance_state.get(self._conn, issue_id)
        if state.last_verdict != "infra_error" or state.infra_retries <= 0:
            return False
        latest = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=issue_id,
            stage="acceptance",
        )
        if latest is None or latest.ended_at is None:
            return False
        try:
            ended_at = _parse_rfc3339(latest.ended_at)
        except ValueError:
            return False
        retry_count = min(state.infra_retries, ACCEPTANCE_INFRA_RETRY_LIMIT)
        backoff_secs = _infra_retry_backoff_secs(retry_count)
        return self._now() < ended_at + timedelta(seconds=backoff_secs)

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
            "requeueing %s after transient API error "
            "(attempt %d/%d, backoff %ds): %s",
            issue.identifier,
            attempt,
            AGENT_INFRA_RETRY_LIMIT,
            _infra_retry_backoff_secs(min(attempt, AGENT_INFRA_RETRY_LIMIT)),
            reason,
        )
        return True

    def _schedule_acceptance(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
    ) -> asyncio.Task[None]:
        binding_key = _binding_key(binding)
        self._reserve_scheduled_slot(issue_id=issue.id, binding_key=binding_key)
        task = asyncio.create_task(
            self._acceptance_with_limits(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                pr_head_sha=pr_head_sha,
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

    async def _acceptance_with_limits(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
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
                    await self._run_acceptance_stage(
                        binding=binding,
                        issue=current,
                        pr_number=pr_number,
                        pr_url=pr_url,
                        pr_head_sha=pr_head_sha,
                    )
        except asyncio.CancelledError:
            run_id = self._dispatch_run_ids.get(issue.id)
            if run_id is not None:
                await self._fail_run(run_id, "acceptance cancelled")
            raise

    def _acceptance_preview_url(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
    ) -> str:
        if binding.acceptance.mode == "dev" and binding.acceptance.dev_port:
            return f"http://127.0.0.1:{binding.acceptance.dev_port}"
        pattern = binding.acceptance.preview_url_pattern
        if not pattern:
            return ""
        try:
            return render_preview_url(
                acceptance=binding.acceptance,
                issue_identifier=issue.identifier,
                issue_id=issue.id,
                pr_number=pr_number,
                pr_url=pr_url,
            )
        except PreviewResolutionError as e:
            log.warning(
                "could not render acceptance preview URL for %s from %r: %s",
                issue.identifier,
                pattern,
                e,
            )
            return ""

    async def _acceptance_pr_diff(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
    ) -> str:
        try:
            return await self._gh.pr_diff(pr_number, repo=binding.github_repo)
        except GitHubError as e:
            log.warning(
                "could not fetch acceptance PR diff for %s#%d on %s: %s",
                binding.github_repo,
                pr_number,
                issue.identifier,
                e,
            )
            raise _AcceptancePrDiffUnavailable(
                f"Could not fetch PR diff for {binding.github_repo}#{pr_number}: {e}"
            ) from e

    async def _post_acceptance_verdict_comment(
        self,
        *,
        binding: RepoBinding | None = None,
        issue: LinearIssue,
        pr_url: str,
        verdict: AcceptanceVerdict,
    ) -> str:
        tracker = (
            self.tracker(binding)
            if binding is not None
            else await self._tracker_for_issue_id(issue.id)
        )
        try:
            body = format_acceptance_verdict_comment(
                verdict=verdict,
                pr_url=pr_url,
            )
            comment_id = await tracker.post_comment(issue.id, truncate_body(body))
            if comment_id:
                return f"{issue.url}#comment-{comment_id}"
        except LinearError as e:
            log.warning(
                "acceptance verdict comment failed on %s: %s",
                issue.identifier,
                e,
            )
        return ""

    async def _post_acceptance_criteria_comment(
        self,
        *,
        binding: RepoBinding | None = None,
        issue: LinearIssue,
        criteria: list[ExtractedCriterion],
    ) -> None:
        tracker = (
            self.tracker(binding)
            if binding is not None
            else await self._tracker_for_issue_id(issue.id)
        )
        try:
            body = format_acceptance_criteria_comment(criteria)
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "acceptance criteria comment failed on %s: %s",
                issue.identifier,
                e,
            )

    async def _upload_acceptance_screenshots(
        self,
        *,
        binding: RepoBinding | None = None,
        issue: LinearIssue,
        workspace_path: Path,
        verdict: AcceptanceVerdict,
    ) -> AcceptanceVerdict:
        if verdict.kind not in {"pass", "reject"} or not verdict.screenshots:
            return verdict

        tracker = (
            self.tracker(binding)
            if binding is not None
            else await self._tracker_for_issue_id(issue.id)
        )
        uploaded_by_path: dict[str, str] = {}
        uploaded_screenshots: list[AcceptanceScreenshot] = []
        for screenshot in verdict.screenshots:
            try:
                path = _acceptance_artifact_path(workspace_path, screenshot.path)
                url = await tracker.upload_issue_attachment(
                    issue_uuid=issue.id,
                    path=path,
                    title=f"Acceptance screenshot: {screenshot.label}",
                )
            except (LinearError, OSError, httpx.HTTPError) as e:
                return replace(
                    verdict,
                    kind="infra_error",
                    hero_screenshot_url="",
                    screenshots=(),
                    criterion_results=(),
                    details=f"acceptance screenshot upload failed: {e}",
                )
            uploaded_by_path[screenshot.path] = url
            uploaded_screenshots.append(replace(screenshot, url=url))

        criterion_results = tuple(
            replace(
                result,
                screenshot_url=uploaded_by_path.get(
                    result.screenshot_path,
                    result.screenshot_url,
                ),
            )
            for result in verdict.criterion_results
        )
        hero_url = next(
            (
                item.url
                for item in uploaded_screenshots
                if item.kind == "hero" and item.url
            ),
            verdict.hero_screenshot_url,
        )
        return replace(
            verdict,
            hero_screenshot_url=hero_url,
            screenshots=tuple(uploaded_screenshots),
            criterion_results=criterion_results,
        )

    async def _run_acceptance_stage(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
        reset_iteration: bool = True,
    ) -> str | None:
        run_id = str(uuid.uuid4())
        inserted = await db.runs.create_if_no_active(
            self._conn,
            id=run_id,
            issue_id=issue.id,
            stage="acceptance",
            status="running",
            pid=None,
            started_at=self._now().isoformat(),
            ignored_stage="review",
        )
        if not inserted:
            return None

        self._dispatch_run_ids[issue.id] = run_id
        try:
            await self._complete_review_monitors_for_merge(issue)
            degrade_note = (
                _acceptance_degrade_note(issue.description)
                if binding.acceptance.mode != _CODE_ONLY_ACCEPTANCE_MODE
                else None
            )
            effective_mode = (
                _CODE_ONLY_ACCEPTANCE_MODE if degrade_note else binding.acceptance.mode
            )
            if degrade_note:
                log.info("%s for %s", degrade_note, issue.identifier)
            preview_url = ""
            preview_resolution_error = ""
            if not degrade_note:
                if (
                    effective_mode == "preview"
                    and binding.acceptance.preview_url_pattern
                ):
                    try:
                        preview_url = render_preview_url(
                            acceptance=binding.acceptance,
                            issue_identifier=issue.identifier,
                            issue_id=issue.id,
                            pr_number=pr_number,
                            pr_url=pr_url,
                        )
                    except PreviewResolutionError as e:
                        preview_resolution_error = str(e)
                        preview_url = e.url
                else:
                    preview_url = self._acceptance_preview_url(
                        binding=binding,
                        issue=issue,
                        pr_number=pr_number,
                        pr_url=pr_url,
                    )
            extracted_criteria = extract_acceptance_criteria(issue.description)
            criteria_names = _acceptance_criterion_names(extracted_criteria)
            criteria_predicates = _acceptance_criterion_predicates(extracted_criteria)
            await db.acceptance_state.begin_acceptance(
                self._conn,
                issue.id,
                pr_number=pr_number,
                pr_url=pr_url,
                pr_head_sha=pr_head_sha,
                mode=binding.acceptance.mode,
                preview_url=preview_url,
                extracted_criteria=json.dumps(extracted_criteria),
                reset_iteration=reset_iteration,
            )
            await self._post_acceptance_criteria_comment(
                binding=binding,
                issue=issue,
                criteria=extracted_criteria,
            )
            await self._move_issue_to_acceptance_state(binding=binding, issue=issue)

            verdict: AcceptanceVerdict | None = None
            if effective_mode not in {_CODE_ONLY_ACCEPTANCE_MODE, "dev", "preview"}:
                verdict = AcceptanceVerdict(
                    kind="pass",
                    criteria=criteria_names,
                    cost=0.0,
                    hero_screenshot_url="",
                    details=(
                        f"Acceptance mode {binding.acceptance.mode!r} is configured, "
                        "but this mode does not have a real runner in this slice. "
                        "Preserving pass-through acceptance behavior until that "
                        "mode's runner is implemented."
                    ),
                )
            elif preview_resolution_error:
                verdict = AcceptanceVerdict(
                    kind="infra_error",
                    criteria=criteria_names,
                    cost=0.0,
                    hero_screenshot_url="",
                    details=preview_resolution_error,
                    preview_url=preview_url,
                )
            elif effective_mode == "preview":
                try:
                    preview_url = await resolve_preview_url(
                        acceptance=binding.acceptance,
                        pr_number=pr_number,
                        issue_identifier=issue.identifier,
                        issue_id=issue.id,
                        pr_url=pr_url,
                    )
                except PreviewResolutionError as e:
                    verdict = AcceptanceVerdict(
                        kind="infra_error",
                        criteria=criteria_names,
                        cost=0.0,
                        hero_screenshot_url="",
                        details=str(e),
                        preview_url=e.url,
                    )

            if verdict is None:
                try:
                    pr_diff_summary = await self._acceptance_pr_diff(
                        binding=binding,
                        issue=issue,
                        pr_number=pr_number,
                    )
                except _AcceptancePrDiffUnavailable as e:
                    verdict = AcceptanceVerdict(
                        kind="infra_error",
                        criteria=criteria_names,
                        cost=0.0,
                        hero_screenshot_url="",
                        details=str(e),
                    )
                else:
                    quick_skip = (
                        quick_skip_trivial_acceptance(
                            linear_description=issue.description,
                            pr_diff_summary=pr_diff_summary,
                            criteria=criteria_names,
                        )
                        if effective_mode == _CODE_ONLY_ACCEPTANCE_MODE
                        else None
                    )
                    if quick_skip is not None:
                        verdict = quick_skip
                    else:
                        workspace_path = await self._workspace.acquire(binding, issue)
                        try:
                            verdict = await run_acceptance(
                                runner=self._runner,
                                run_id=run_id,
                                workspace_path=workspace_path,
                                mode=effective_mode,
                                linear_description=issue.description,
                                pr_diff_summary=pr_diff_summary,
                                taste_guide=load_taste_guide(
                                    binding_taste_guide=binding.acceptance.taste_guide,
                                ),
                                criteria=criteria_predicates,
                                stall_secs=binding.acceptance.time_cap_minutes * 60,
                                preview_url=preview_url,
                                dev_command=binding.acceptance.dev_command,
                                dev_port=binding.acceptance.dev_port,
                            )
                            verdict = _replace_acceptance_criteria_labels(
                                verdict=verdict,
                                criteria_names=criteria_names,
                                criteria_predicates=criteria_predicates,
                            )
                            if effective_mode in {"dev", "preview"}:
                                verdict = await self._upload_acceptance_screenshots(
                                    binding=binding,
                                    issue=issue,
                                    workspace_path=workspace_path,
                                    verdict=verdict,
                                )
                        finally:
                            self._workspace.release(binding, issue)

            verdict = _with_acceptance_degrade_note(verdict, degrade_note)

            verdict_usage = verdict.usage
            if verdict_usage.has_usage():
                verdict_usage = UsageDelta(
                    cost_usd=verdict.cost,
                    input_tokens=verdict_usage.input_tokens,
                    output_tokens=verdict_usage.output_tokens,
                    cache_write_tokens=verdict_usage.cache_write_tokens,
                    cache_read_tokens=verdict_usage.cache_read_tokens,
                )
            elif verdict.cost > 0:
                verdict_usage = UsageDelta(cost_usd=verdict.cost)
            if verdict_usage.has_usage():
                await _add_run_usage(self._conn, run_id, verdict_usage)

            comment_url = await self._post_acceptance_verdict_comment(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                verdict=verdict,
            )
            await db.acceptance_state.record_verdict(
                self._conn,
                issue.id,
                verdict=verdict.kind,
                artifacts_url=comment_url or verdict.hero_screenshot_url,
                preview_url=verdict.preview_url,
            )

            ended_at = self._now().isoformat()
            if verdict.kind == "pass":
                await db.runs.update_status(
                    self._conn,
                    run_id,
                    "completed",
                    ended_at=ended_at,
                )
                if self._dispatch_run_ids.get(issue.id) == run_id:
                    self._dispatch_run_ids.pop(issue.id, None)
                merge_issue = await self._refresh_issue_for_acceptance_merge_handoff(
                    binding, issue
                )
                if _needs_human_approval_label_present(merge_issue):
                    await self._open_merge_wait_for_human_approval_label(
                        binding=binding,
                        issue=merge_issue,
                        pr_url=pr_url,
                    )
                else:
                    await self._merge_approved_pr(
                        binding=binding,
                        issue=merge_issue,
                        pr_number=pr_number,
                        pr_url=pr_url,
                        approved_head_sha=pr_head_sha,
                    )
                return run_id

            if verdict.kind == "infra_error":
                state = await db.acceptance_state.get(self._conn, issue.id)
                if state.infra_retries >= ACCEPTANCE_INFRA_RETRY_LIMIT:
                    await db.runs.update_status(
                        self._conn,
                        run_id,
                        "failed",
                        ended_at=ended_at,
                        **_termination_kwargs(
                            status="failed",
                            reason=f"acceptance infra_error: {verdict.details}",
                        ),
                    )
                    await self._track_acceptance_blocked_wait(
                        binding=binding,
                        issue=issue,
                        pr_number=pr_number,
                        run_id=run_id,
                        verdict=verdict,
                    )
                    return run_id
                await db.acceptance_state.bump_infra_retries(self._conn, issue.id)
                await db.runs.update_status(
                    self._conn,
                    run_id,
                    "failed",
                    ended_at=ended_at,
                    **_termination_kwargs(
                        status="failed",
                        reason=f"acceptance infra_error: {verdict.details}",
                    ),
                )
                return run_id

            state = await db.acceptance_state.get(self._conn, issue.id)
            await db.runs.update_status(
                self._conn,
                run_id,
                "failed",
                ended_at=ended_at,
                **_termination_kwargs(
                    status="failed",
                    reason=f"acceptance rejected: {verdict.details}",
                ),
            )
            if state.iteration < ACCEPTANCE_FIX_ITERATION_CAP:
                dispatched = await self._dispatch_acceptance_fix_run(
                    binding=binding,
                    issue=issue,
                    pr_number=pr_number,
                    pr_url=pr_url,
                    pr_head_sha=pr_head_sha,
                    verdict=verdict,
                )
                if dispatched:
                    return run_id
                log.warning(
                    "acceptance fix-run did not advance %s; opening operator wait",
                    issue.identifier,
                )

            await self._track_acceptance_rejected_wait(issue.id, run_id, binding)
            body = acceptance_rejected(
                CommentVars(
                    stage="acceptance",
                    repo=binding.github_repo,
                    issue=pr_number,
                    pr_url=pr_url,
                    run_id=run_id,
                )
            )
            tracker = self.tracker(binding)
            try:
                await tracker.post_comment(issue.id, truncate_body(body))
            except LinearError as e:
                log.warning(
                    "acceptance rejected wait comment failed on %s: %s",
                    issue.identifier,
                    e,
                )
            return run_id
        except Exception as e:
            log.exception("acceptance stage failed for %s", issue.identifier)
            await db.runs.update_status(
                self._conn,
                run_id,
                "failed",
                ended_at=self._now().isoformat(),
                **_termination_kwargs(
                    status="failed",
                    exc=e,
                    reason=f"acceptance stage failed: {e}",
                ),
            )
            return run_id
        finally:
            if (
                self._dispatch_run_ids.get(issue.id) == run_id
                and run_id not in self._operator_wait_run_ids
            ):
                self._dispatch_run_ids.pop(issue.id, None)

    async def _dispatch_acceptance_fix_run(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
        verdict: AcceptanceVerdict,
    ) -> bool:
        await db.acceptance_state.bump_iteration(self._conn, issue.id)
        prompt = acceptance_fix_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
            acceptance_verdict=format_acceptance_verdict_comment(
                verdict=verdict,
                pr_url=pr_url,
            ),
        )

        try:
            workspace_path = await self._workspace.acquire(binding, issue)
        except Exception:  # noqa: BLE001
            log.exception("workspace acquire failed for acceptance fix-run %s", issue.identifier)
            return False

        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
        try:
            try:
                await _git_fetch_branch(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "could not fetch acceptance fix-run remote HEAD for %s: %s",
                    branch,
                    e,
                )
                return False
            start_sha = await _workspace_ref_sha(workspace_path, f"origin/{branch}")
            if not start_sha:
                start_sha = pr_head_sha
            if not start_sha:
                log.warning(
                    "could not read acceptance fix-run remote HEAD for %s",
                    branch,
                )
                return False

            fix_run_id = str(uuid.uuid4())
            await db.runs.create(
                self._conn,
                id=fix_run_id,
                issue_id=issue.id,
                stage="acceptance_fix",
                status="running",
                pid=None,
                started_at=self._now().isoformat(),
            )
            self._dispatch_run_ids[issue.id] = fix_run_id

            try:
                prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
                (
                    usage_delta,
                    final_kind,
                    final_returncode,
                ) = await self._run_acceptance_fix_agent(
                    binding=binding,
                    issue=issue,
                    run_id=fix_run_id,
                    workspace_path=workspace_path,
                    prompt=prompt,
                    prior_total=prior_total,
                )
            except Exception as e:  # noqa: BLE001
                log.exception("acceptance fix-run execution failed for %s", issue.identifier)
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        exc=e,
                        reason=f"acceptance fix-run execution failed: {e}",
                    ),
                )
                return False
            finally:
                if self._dispatch_run_ids.get(issue.id) == fix_run_id:
                    self._dispatch_run_ids.pop(issue.id, None)

            await _add_run_usage(self._conn, fix_run_id, usage_delta)

            transition = on_runner_event(
                stage="acceptance_fix",
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
                        reason=f"acceptance fix-run ended with {final_kind}",
                    ),
                )
                return False

            pushed_sha = await _workspace_head_sha(workspace_path)
            if not pushed_sha or pushed_sha == start_sha:
                short_sha = (pushed_sha or start_sha)[:12] or "(unknown)"
                status_short = await _git_status_short(workspace_path)
                log.warning(
                    "acceptance fix-run completed without advancing %s; "
                    "HEAD stayed at %s; status=%s",
                    branch,
                    short_sha,
                    status_short,
                )
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        reason=(
                            "acceptance fix-run completed without advancing "
                            f"{branch}; HEAD stayed at {short_sha}; status={status_short}"
                        ),
                    ),
                )
                return False

            try:
                await self._push_fn(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "git push failed for acceptance fix-run %s: %s",
                    issue.identifier,
                    e,
                )
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        exc=e,
                        reason=f"push failed: {e}",
                    ),
                )
                return False

            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "completed",
                ended_at=self._now().isoformat(),
            )
            await self._run_acceptance_stage(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                pr_head_sha=pushed_sha,
                reset_iteration=False,
            )
            return True
        finally:
            self._workspace.release(binding, issue)

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
                    await self._move_issue_to_review_state(
                        binding=binding, issue=issue
                    )
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

        if (
            created_review_run
            and post_codex_review
            and binding.resolved_remote_review()
        ):
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

    async def _move_issue_to_acceptance_state(
        self, *, binding: RepoBinding, issue: LinearIssue
    ) -> None:
        try:
            states = await self._states_for_binding(binding)
            acceptance_state_id = states.get(binding.linear_states.in_acceptance)
        except LinearError as e:
            log.warning(
                "could not load states while moving %s to acceptance: %s",
                issue.identifier,
                e,
            )
            return
        if acceptance_state_id is None:
            log.warning(
                "missing Linear acceptance state %r for %s",
                binding.linear_states.in_acceptance,
                issue.identifier,
            )
            return
        try:
            await self.tracker(binding).move_issue(issue.id, acceptance_state_id)
        except LinearError as e:
            log.warning(
                "could not move %s to acceptance state %r: %s",
                issue.identifier,
                binding.linear_states.in_acceptance,
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
        last_posted_at = (
            _parse_optional_datetime(mark.last_posted_at)
            if mark is not None
            else None
        )
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
                    elif ev.kind == "tick":
                        await self._record_activity_tick(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_usage=cumulative_usage,
                        )
                    elif ev.kind in ("exit", "stall_timeout", "spawn_failed"):
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
        await _record_run_model_usage(
            self._conn, run_id, log_path, codex_model=binding.codex_model
        )
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

    async def _run_acceptance_fix_agent(
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
            workspace_path=workspace_path,
            mcp_servers=binding.mcp_servers,
        )
        return await self._run_runner(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stage="acceptance_fix",
            agent=binding.agent,
            codex_model=binding.codex_model,
            binding=binding,
            issue=issue,
            activity_stage="acceptance_fix",
            prior_total=prior_total,
        )

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
            log.warning(
                "dirty-tree fix turn failed for %s: %s", issue.identifier, e
            )
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
                    elif ev.kind == "tick":
                        await self._record_activity_tick(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_usage=cumulative_usage,
                        )
                    elif ev.kind in ("exit", "stall_timeout", "spawn_failed"):
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
        await _record_run_model_usage(
            self._conn, run_id, log_path, codex_model=codex_model
        )
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
            log.warning(
                "implement blocked comment post failed on %s: %s", issue.identifier, e
            )

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
            target_state_id = states.get(
                binding.linear_states.needs_approval
            ) or states.get(binding.linear_states.blocked)
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
            ctx.local_review_result.outcome.value
            if ctx.local_review_result is not None
            else None
        )
        await self._track_deliver_failed_wait(
            storage_issue_id,
            run_id,
            binding,
            local_review_outcome=local_review_outcome,
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
            log.warning(
                "deliver_failed comment post failed on %s: %s", issue.identifier, e
            )

    async def _track_delivery_handoff_recovery_wait(
        self, ctx: _PendingDelivery
    ) -> None:
        """Persist a temporary retry target before first review handoff."""
        local_review_outcome = (
            ctx.local_review_result.outcome.value
            if ctx.local_review_result is not None
            else None
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
            log.warning(
                "could not look up %s to resume delivery: %s", issue_id, e
            )
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
            local_review_result=await self._reconstructed_local_review_result(
                run_id
            ),
            reconstructed=True,
            retry_workspace_acquired=True,
        )

    async def _reconstructed_local_review_result(
        self, run_id: str
    ) -> LoopResult | None:
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
                    "unknown persisted local_review_outcome %r for run %s; "
                    "treating as APPROVED",
                    wait.local_review_outcome,
                    run_id,
                )
        return LoopResult(outcome=outcome, iterations=0, verdicts=())


__all__ = [
    "Orchestrator",
    "WebhookDispatchResult",
    "_local_review_status_from_result",
    "build_fix_runner_command",
    "build_merge_runner_command",
    "build_pr_body",
    "build_pr_title",
    "build_runner_command",
    "pr_number_from_url",
]


def _linear_issue_state_changed(payload: Mapping[str, Any]) -> bool:
    action = str(payload.get("action") or "").casefold()
    if action and action not in {"update", "updated", "issue_updated"}:
        return False
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return False
    updated_from = payload.get("updatedFrom") or data.get("updatedFrom")
    if isinstance(updated_from, Mapping) and any(
        key in updated_from
        for key in ("state", "stateId", "state_id", "stateName", "state_name")
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


def _comment_from_webhook_payload(
    payload: Mapping[str, Any]
) -> LinearComment | None:
    return comment_from_webhook_payload(payload)
