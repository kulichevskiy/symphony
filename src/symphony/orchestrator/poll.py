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
import hashlib
import inspect
import json
import logging
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, cast

import aiosqlite
import httpx

from .. import db
from ..agent.activity import (
    ActivityPublishReason,
    ActivitySession,
    ActivitySettings,
    digest_fingerprint,
    format_activity_digest,
)
from ..agent.codex_cli import build_codex_workspace_write_command
from ..agent.codex_models import DEFAULT_CODEX_MODEL
from ..agent.process import parse_event_line
from ..agent.prompt import (
    acceptance_fix_prompt,
    implement_prompt,
    merge_conflict_fix_prompt,
    merge_conflict_rebase_fix_prompt,
    merge_prompt,
    merge_required_check_fix_prompt,
    review_comment_fix_prompt,
    review_fix_prompt,
)
from ..agent.runner import Runner, RunnerSpec
from ..agent.runners.acceptance import quick_skip_trivial_acceptance, run_acceptance
from ..agent.runners.local import LocalRunner
from ..config import Config, RepoBinding
from ..github.branch_protection import get_required_contexts
from ..github.client import CheckRun as GitHubCheckRun
from ..github.client import GitHub, GitHubError, PRChecks, _is_merge_conflict_error
from ..github.webhook import GitHubWebhookEvent
from ..linear import slash
from ..linear.blockers import is_blocked, open_blocker_ids
from ..linear.client import Linear, LinearComment, LinearError, LinearIssue
from ..linear.slash import SlashIntent, SlashKind
from ..linear.templates import (
    CommentVars,
    acceptance_blocked,
    acceptance_rejected,
    acceptance_retry_requested,
    acceptance_skipped,
    awaiting_approval,
    codex_lgtm,
    command_rejected,
    cost_cap_reached,
    cost_warning,
    failed,
    fix_pushed,
    fixing_merge_conflict,
    is_symphony_comment,
    moved_to_waiting,
    resumed,
    retry_acceptance_requested,
    review_retry_requested,
    review_stopped,
    reviewing_feedback,
    run_started,
    skip_acceptance_forced,
    skip_review_forced,
    stage_done,
    stuck_loop_escape,
    truncate_body,
)
from ..pipeline.acceptance_classifier import (
    AcceptanceScreenshot,
    AcceptanceVerdict,
    ExtractedCriterion,
    extract_acceptance_criteria,
    format_acceptance_criteria_comment,
    format_acceptance_verdict_comment,
)
from ..pipeline.cost_guard import (
    UsageCostEstimator,
    effective_cap,
    effective_warning_pct,
    evaluate_cost,
)
from ..pipeline.local_review import LocalVerdict
from ..pipeline.local_review_loop import LoopOutcome, LoopResult
from ..pipeline.local_review_session import run_local_review_session
from ..pipeline.preview_resolver import (
    PreviewResolutionError,
    render_preview_url,
    resolve_preview_url,
)
from ..pipeline.review_classifier import (
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
from ..pipeline.review_classifier import (
    CheckRun as ReviewCheckRun,
)
from ..pipeline.state_machine import on_runner_event
from ..pipeline.taste_guide import load_taste_guide
from ..workspace import Workspace
from .reconciler import Reconciler

log = logging.getLogger(__name__)

PushFn = Callable[[Path, str], Awaitable[None]]
BindingKey = tuple[str, str, str]
CI_FETCH_FAILURE_LIMIT = 5
REVIEW_RESURRECT_COOLDOWN_SECS = 120
CODEX_NO_ISSUES_MARKER = "any major issues"
MERGE_WAIT_RECONCILE_INTERVAL_SECS = 600
MERGED_LINEAR_STATE_RECONCILE_TICK_INTERVAL = 5
MERGED_LINEAR_STATE_RECONCILE_LOOKBACK_HOURS = 24
PARKED_CLOSED_UNMERGED_COMMENT = "🛑 PR closed without merge — marking done"
_CODE_ONLY_ACCEPTANCE_MODE = "code_only"
NEEDS_HUMAN_APPROVAL_LABEL = "needs-human-approval"
_ACCEPTANCE_MISSING_WHERE_TO_VERIFY_NOTE = (
    "Acceptance: degraded to code-only — no `Where to verify` in ticket description"
)
ACCEPTANCE_INFRA_RETRY_LIMIT = 2
ACCEPTANCE_INFRA_RETRY_BASE_BACKOFF_SECS = 30
ACCEPTANCE_INFRA_RETRY_MAX_BACKOFF_SECS = 120
ACCEPTANCE_FIX_ITERATION_CAP = 1
MANUAL_MERGE_PARKED_RUN_PREFIX = "manual-merge-parked:"


class _AcceptancePrDiffUnavailable(RuntimeError):
    pass


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


_UsageCostEstimator = UsageCostEstimator  # back-compat alias for internal callers


def _acceptance_has_where_to_verify(description: str) -> bool:
    for raw_line in description.splitlines():
        heading = _normalize_acceptance_section_heading(raw_line)
        if heading == "where to verify" or heading.startswith("where to verify:"):
            return True
    return False


def _normalize_acceptance_section_heading(line: str) -> str:
    line = line.strip()
    line = re.sub(r"^#{1,6}\s*", "", line)
    line = re.sub(r"\s+#{1,6}\s*$", "", line)
    line = line.strip(" *_`")
    return re.sub(r"\s+", " ", line).casefold()


def _acceptance_degrade_note(description: str) -> str | None:
    if _acceptance_has_where_to_verify(description):
        return None
    return _ACCEPTANCE_MISSING_WHERE_TO_VERIFY_NOTE


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


def _parse_optional_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return _parse_rfc3339(raw)
    except ValueError:
        return None


@dataclass(frozen=True)
class WebhookDispatchResult:
    kind: str
    handled: bool
    detail: str = ""


def build_pr_title(issue: LinearIssue) -> str:
    return f"[{issue.identifier}] {issue.title}"


def build_pr_body(issue: LinearIssue) -> str:
    """The Linear URL goes through `gh pr_create`'s `linear_url` argument
    (which appends `Relates to ...`), so the body itself is empty by
    default. Returning the URL here keeps the format pinned in tests."""
    return f"Relates to {issue.url}"


def _local_review_status_from_result(result: LoopResult | None) -> str:
    """Map a `LoopResult` to a `runs.status` literal.

    Symmetric with how Implement uses `completed` / `failed`; SKIPPED
    maps to `interrupted` so it stands out from genuine model failures
    in run-history queries (and matches `$skip-review`, which also
    leaves a row with `status='interrupted'`).
    """
    if result is None:
        return "failed"
    if result.outcome == LoopOutcome.APPROVED:
        return "completed"
    if result.outcome == LoopOutcome.SKIPPED:
        return "interrupted"
    return "failed"


def _should_post_codex_review(
    *,
    review_strategy: str,
    local_review_result: LoopResult | None,
) -> bool:
    """Decide whether to ping `@codex review` after opening the PR.

    Rules:
      - `remote`              → always post (legacy behavior).
      - `hybrid`              → always post (defense in depth: the
                                remote bot is the second pair of eyes
                                regardless of the local outcome).
      - `local` + APPROVED    → suppress; the local pre-flight already
                                approved the diff and the operator
                                opted in to single-reviewer mode.
      - `local` + anything else → post (safety net for failures,
                                exhaustion, stuck loops, cost-cap
                                breach, and operator-issued
                                `$skip-local-review`).
    """
    if review_strategy == "remote" or review_strategy == "hybrid":
        return True
    # local strategy
    if (
        local_review_result is not None
        and local_review_result.outcome == LoopOutcome.APPROVED
    ):
        return False
    return True


def build_runner_command(
    agent: str,
    prompt: str,
    *,
    max_budget_usd: float | None = None,
    codex_model: str = DEFAULT_CODEX_MODEL,
    workspace_path: Path | None = None,
) -> list[str]:
    """Per-runner argv for the Implement stage prompt."""
    if agent == "claude":
        command = [
            "claude",
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
        ]
        if max_budget_usd is not None:
            command.extend(["--max-budget-usd", f"{max_budget_usd:.4f}"])
        command.append(prompt)
        return command
    if agent == "codex":
        if workspace_path is None:
            raise ValueError("workspace_path is required for codex write runs")
        return build_codex_workspace_write_command(
            prompt=prompt,
            codex_model=codex_model,
        )
    raise ValueError(f"unknown agent {agent!r}")


def build_fix_runner_command(
    agent: str,
    prompt: str,
    *,
    max_budget_usd: float | None = None,
    codex_model: str = DEFAULT_CODEX_MODEL,
    workspace_path: Path | None = None,
) -> list[str]:
    """argv for a Review-stage fix-run.

    Fix-runs go through the binding's CLI (claude or codex), NOT through
    the GitHub `@codex review` bot. The bot is only consulted via PR
    comments; the binding's `agent` field is what drives code changes
    in response to its feedback.
    """
    return build_runner_command(
        agent,
        prompt,
        max_budget_usd=max_budget_usd,
        codex_model=codex_model,
        workspace_path=workspace_path,
    )


def build_merge_runner_command(
    agent: str,
    prompt: str,
    *,
    max_budget_usd: float | None = None,
    codex_model: str = DEFAULT_CODEX_MODEL,
    workspace_path: Path | None = None,
) -> list[str]:
    """argv for the Merge-stage final local pass."""
    return build_runner_command(
        agent,
        prompt,
        max_budget_usd=max_budget_usd,
        codex_model=codex_model,
        workspace_path=workspace_path,
    )


_PR_URL_RE = re.compile(r"/pull/(\d+)")


def pr_number_from_url(url: str) -> int | None:
    """Extract the PR number from a `gh pr create` URL.

    `gh pr create` prints `https://github.com/OWNER/REPO/pull/<N>` on
    success (sometimes with trailing whitespace). The Review-stage poll
    needs that `<N>` to post `@codex review` and to fetch the snapshot.
    """
    if not url:
        return None
    m = _PR_URL_RE.search(url.strip())
    if m is None:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _binding_key(binding: RepoBinding) -> BindingKey:
    return (
        binding.linear_team_key,
        binding.github_repo,
        binding.issue_label or "",
    )


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


def _manual_merge_parked_run_id(pr: db.issue_prs.IssuePR) -> str:
    return (
        f"{MANUAL_MERGE_PARKED_RUN_PREFIX}"
        f"{pr.issue_id}:{pr.github_repo}:{pr.pr_number}"
    )


def _review_issue_is_active(issue: LinearIssue, binding: RepoBinding) -> bool:
    return issue.state_name in {
        binding.linear_states.in_progress,
        binding.linear_states.code_review,
    }


def _merge_issue_matches_binding(issue: LinearIssue, binding: RepoBinding) -> bool:
    active_states = {
        binding.linear_states.in_progress,
        binding.linear_states.code_review,
        binding.linear_states.needs_approval,
    }
    if binding.acceptance.mode != "off":
        active_states.add(binding.linear_states.in_acceptance)
    return (
        issue.team_key == binding.linear_team_key
        and issue.state_name in active_states
        and (binding.issue_label is None or binding.issue_label in issue.labels)
    )


def _needs_human_approval_label_present(issue: LinearIssue) -> bool:
    return NEEDS_HUMAN_APPROVAL_LABEL in issue.labels


def _parse_rfc3339(s: str) -> datetime:
    """Linear timestamps end in `Z`; Python's `fromisoformat` accepts the
    `+00:00` form. Normalize before parsing."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


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
                commit_sha=str(
                    entry.get("commit_id") or entry.get("original_commit_id") or ""
                ),
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


def _codex_lgtm_reactions_from_issue_comments(
    entries: list[dict[str, object]],
) -> tuple[Reaction, ...]:
    """Treat Codex's "no major issues" PR issue comment as an approval signal.

    Codex sometimes reports the 👍 as text inside a top-level PR comment instead
    of as a GitHub reaction. The classifier already knows how to validate +1
    signals against the head commit time, so normalize this shape into the same
    representation.
    """
    reactions: list[Reaction] = []
    for entry in entries:
        login = _user_login(entry)
        body = str(entry.get("body") or "")
        created_at = str(entry.get("created_at") or entry.get("createdAt") or "")
        if not created_at:
            continue
        if is_codex_author(login) and CODEX_NO_ISSUES_MARKER in body.casefold():
            reactions.append(
                Reaction(
                    user_login=login,
                    content="+1",
                    created_at=created_at,
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
    gh: GitHub,
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


def _pr_view_is_merged(view: dict[str, object]) -> bool:
    return (
        bool(view.get("mergedAt"))
        or bool(view.get("merged"))
        or str(view.get("state") or "").upper() == "MERGED"
    )


def _pr_view_is_closed(view: dict[str, object]) -> bool:
    return str(view.get("state") or "").upper() == "CLOSED"


def _pr_view_has_merge_conflict(view: dict[str, object]) -> bool:
    mergeable = str(view.get("mergeable") or "").upper()
    merge_state = str(
        view.get("mergeStateStatus") or view.get("merge_state_status") or ""
    ).upper()
    return mergeable == "CONFLICTING" or merge_state == "DIRTY"


def _pr_view_skips_required_check_fix(view: dict[str, object]) -> bool:
    mergeable = str(view.get("mergeable") or "").upper()
    merge_state = str(
        view.get("mergeStateStatus") or view.get("merge_state_status") or ""
    ).upper()
    return (
        mergeable == "CONFLICTING"
        or merge_state in {"BEHIND", "CONFLICTING", "DIRTY"}
    )


def _pr_view_is_clean_mergeable(view: dict[str, object]) -> bool:
    mergeable = str(view.get("mergeable") or "").upper()
    merge_state = str(
        view.get("mergeStateStatus") or view.get("merge_state_status") or ""
    ).upper()
    return mergeable == "MERGEABLE" and merge_state == "CLEAN"


def _pr_base_ref_from_view(view: dict[str, object]) -> str | None:
    raw = view.get("baseRefName") or view.get("base_ref_name") or view.get("baseRef")
    if raw is None:
        return None
    base_ref = str(raw).strip()
    return base_ref or None


_REQUIRED_CHECK_FAILURE_STATES = {
    "FAILURE",
    "FAILED",
    "ERROR",
    "CANCELLED",
    "CANCELED",
    "TIMED_OUT",
    "ACTION_REQUIRED",
    "STARTUP_FAILURE",
    "STALE",
}


def _status_rollup_nodes(raw: object) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [entry for entry in raw if isinstance(entry, dict)]
    if not isinstance(raw, dict):
        return []
    nodes = raw.get("nodes")
    if isinstance(nodes, list):
        return [entry for entry in nodes if isinstance(entry, dict)]
    edges = raw.get("edges")
    if isinstance(edges, list):
        return [
            edge["node"]
            for edge in edges
            if isinstance(edge, dict) and isinstance(edge.get("node"), dict)
        ]
    contexts = raw.get("contexts")
    if isinstance(contexts, list):
        return [entry for entry in contexts if isinstance(entry, dict)]
    return []


def _status_check_identity(check: Mapping[str, object]) -> str:
    return (
        str(check.get("context") or "").strip()
        or str(check.get("name") or "").strip()
        or str(check.get("workflowName") or "").strip()
        or "(unnamed)"
    )


def _status_check_names(check: Mapping[str, object]) -> set[str]:
    names: set[str] = set()
    for key in ("context", "name", "workflowName"):
        value = str(check.get(key) or "").strip()
        if value:
            names.add(value)
    return names


def _status_check_sha(check: Mapping[str, object]) -> str:
    for key in ("sha", "commitOid", "commit_oid"):
        value = str(check.get(key) or "").strip()
        if value:
            return value
    commit = check.get("commit")
    if isinstance(commit, Mapping):
        return str(commit.get("oid") or commit.get("sha") or "").strip()
    return ""


def _status_check_failed(check: Mapping[str, object]) -> bool:
    state = str(
        check.get("state") or check.get("status") or check.get("__typename") or ""
    ).upper()
    conclusion = str(check.get("conclusion") or "").upper()
    return (
        state in _REQUIRED_CHECK_FAILURE_STATES
        or conclusion in _REQUIRED_CHECK_FAILURE_STATES
    )


def _required_check_detail(check: Mapping[str, object]) -> dict[str, object]:
    detail: dict[str, object] = {}
    for key in (
        "__typename",
        "name",
        "context",
        "workflowName",
        "state",
        "status",
        "conclusion",
        "targetUrl",
        "detailsUrl",
        "description",
    ):
        value = check.get(key)
        if value is not None:
            detail[key] = value
    run_id = _status_check_run_id(check)
    if run_id:
        detail["runId"] = run_id
    return detail


def _status_check_run_id(check: Mapping[str, object]) -> str:
    for key in ("runId", "run_id"):
        value = str(check.get(key) or "").strip()
        if value:
            return value
    workflow_run = check.get("workflowRun")
    if isinstance(workflow_run, Mapping):
        for key in ("databaseId", "database_id", "id"):
            value = str(workflow_run.get(key) or "").strip()
            if value:
                return value
    for key in ("detailsUrl", "targetUrl"):
        url = str(check.get(key) or "")
        match = re.search(r"/actions/runs/([^/?#]+)", url)
        if match is not None:
            return match.group(1)
    for key in ("databaseId", "database_id"):
        value = str(check.get(key) or "").strip()
        if value:
            return value
    return ""


def _required_check_trigger_signature(
    *,
    head_sha: str,
    failing_checks: list[dict[str, object]],
) -> str:
    contexts = sorted(_status_check_identity(check) for check in failing_checks)
    contexts_hash = hashlib.sha256("\n".join(contexts).encode("utf-8")).hexdigest()[:12]
    return f"required_check_failure:{head_sha}:{contexts_hash}"


async def _default_push(workspace_path: Path, branch: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "git",
        "push",
        "-u",
        "origin",
        branch,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git push failed: {stderr.decode(errors='replace').strip()}"
        )


async def _default_force_push(workspace_path: Path, branch: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "git",
        "push",
        "--force-with-lease",
        "-u",
        "origin",
        branch,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git push --force-with-lease failed: "
            f"{stderr.decode(errors='replace').strip()}"
        )


async def _sync_workspace_to_remote(workspace_path: Path, branch: str) -> None:
    """Fetch and hard-reset the workspace to origin/branch.

    Called before the merge agent so that local commits left behind by
    review-fix runs (which may have diverged from the remote) do not cause
    a non-fast-forward push failure later.
    """
    fetch_proc = await asyncio.create_subprocess_exec(
        "git", "fetch", "origin", branch,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await fetch_proc.communicate()
    if fetch_proc.returncode != 0:
        raise RuntimeError(
            f"git fetch failed: {stderr.decode(errors='replace').strip()}"
        )
    reset_proc = await asyncio.create_subprocess_exec(
        "git", "reset", "--hard", f"origin/{branch}",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await reset_proc.communicate()
    if reset_proc.returncode != 0:
        raise RuntimeError(
            f"git reset --hard failed: {stderr.decode(errors='replace').strip()}"
        )


async def _git_fetch(workspace_path: Path) -> None:
    """Run ``git fetch origin`` in *workspace_path*."""
    proc = await asyncio.create_subprocess_exec(
        "git", "fetch", "origin",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git fetch failed: {stderr.decode(errors='replace').strip()}"
        )


async def _git_fetch_branch(workspace_path: Path, branch: str) -> None:
    """Fetch ``origin/branch`` so remote-head validation has a fresh baseline."""
    proc = await asyncio.create_subprocess_exec(
        "git", "fetch", "origin", branch,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git fetch origin {branch} failed: {stderr.decode(errors='replace').strip()}"
        )


async def _git_status_short(workspace_path: Path) -> str:
    """Return ``git status --short`` output for failure diagnostics."""
    proc = await asyncio.create_subprocess_exec(
        "git", "status", "--short", "--untracked-files=all",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        return f"<git status failed: {stderr.decode(errors='replace').strip()}>"
    return stdout.decode(errors="replace").strip()


async def _git_rebase(workspace_path: Path, upstream: str) -> bool:
    """Run ``git rebase upstream``.

    Returns ``True`` if the rebase completed cleanly (exit code 0), ``False``
    if it stopped due to conflicts.
    """
    proc = await asyncio.create_subprocess_exec(
        "git", "rebase", upstream,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    await proc.communicate()
    return proc.returncode == 0


async def _git_abort_rebase(workspace_path: Path) -> None:
    """Abort an in-progress rebase in *workspace_path*."""
    proc = await asyncio.create_subprocess_exec(
        "git", "rebase", "--abort",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git rebase --abort failed: {stderr.decode(errors='replace').strip()}"
        )


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


async def _git_conflicted_files(workspace_path: Path) -> list[str]:
    """Return a list of paths with unresolved conflict markers."""
    proc = await asyncio.create_subprocess_exec(
        "git", "diff", "--name-only", "--diff-filter=U",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        stdin=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    return [p for p in stdout.decode().splitlines() if p]


async def _git_add_and_continue_rebase(
    workspace_path: Path, files: list[str]
) -> bool:
    """Stage *files* and run ``git rebase --continue``.

    Returns ``True`` when the rebase completed. Returns ``False`` when Git
    stopped again, which may be a later conflicting commit in a multi-commit
    rebase.
    """
    if files:
        add_proc = await asyncio.create_subprocess_exec(
            "git", "add", "--", *files,
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
        _, stderr = await add_proc.communicate()
        if add_proc.returncode != 0:
            raise RuntimeError(
                f"git add failed: {stderr.decode(errors='replace').strip()}"
            )
    import os  # noqa: PLC0415
    env = {**os.environ, "GIT_EDITOR": "true"}
    cont_proc = await asyncio.create_subprocess_exec(
        "git", "rebase", "--continue",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
        env=env,
    )
    await cont_proc.communicate()
    return cont_proc.returncode == 0


async def _workspace_head_sha(workspace_path: Path) -> str:
    """Return the HEAD commit SHA of *workspace_path*, or "" on error."""
    return await _workspace_ref_sha(workspace_path, "HEAD")


async def _workspace_ref_sha(workspace_path: Path, ref: str) -> str:
    """Return the commit SHA for *ref* in *workspace_path*, or "" on error."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "rev-parse", "--verify", ref,
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            return stdout.decode().strip()
    except Exception:  # noqa: BLE001
        pass
    return ""


def _github_commit_url(repo: str, sha: str) -> str:
    """Return a browser commit URL for *sha* in [HOST/]OWNER/REPO."""
    if not sha:
        return ""
    parts = repo.split("/")
    if len(parts) == 3:
        host, owner, name = parts
    elif len(parts) == 2:
        host = "github.com"
        owner, name = parts
    else:
        return ""
    return f"https://{host}/{owner}/{name}/commit/{sha}"


def _review_check_from_gh(run: GitHubCheckRun) -> ReviewCheckRun:
    bucket = run.bucket.lower()
    state = run.state.lower()
    status = (
        "completed"
        if bucket in {"pass", "fail", "cancel", "skipping"}
        else "in_progress"
    )
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
    failed = [
        run
        for run in checks.runs
        if run.bucket.lower() in {"fail", "cancel"}
    ]
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


def _pr_url_for_state(
    *, repo: str, pr_number: int | None, pr_url: str
) -> str:
    if pr_url:
        return pr_url
    if pr_number is not None:
        return f"https://github.com/{repo}/pull/{pr_number}"
    return "(no PR)"


class Orchestrator:
    """Owns the poll loop. Dedupe is a SQLite query over the `runs` table."""

    def __init__(
        self,
        config: Config,
        linear: Linear,
        conn: aiosqlite.Connection,
        *,
        runner: Runner | None = None,
        gh: GitHub | None = None,
        workspace: Workspace | None = None,
        push_fn: PushFn | None = None,
        force_push_fn: PushFn | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self.linear = linear
        self._conn = conn
        self._shutdown = asyncio.Event()
        self._gh: GitHub = gh if gh is not None else GitHub()
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
        # Cache of (team_key -> {state_name: state_uuid}). Re-fetched on
        # startup; never mutated at runtime.
        self._states: dict[str, dict[str, str]] = {}
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
        self._cost_cap_run_bindings: dict[str, RepoBinding] = {}
        self._implement_failed_run_bindings: dict[str, RepoBinding] = {}
        self._review_failed_run_bindings: dict[str, RepoBinding] = {}
        self._merge_needs_approval_bindings: dict[str, RepoBinding] = {}
        self._acceptance_rejected_run_bindings: dict[str, RepoBinding] = {}
        self._runs_moved_to_in_progress: set[str] = set()
        # `$skip-local-review` toggles the issue's flag here. The
        # local-review loop reads it between iterations and exits with
        # `SKIPPED`. Removed after the local-review phase returns.
        self._local_review_skip_flags: dict[str, bool] = {}
        # The session updates this with the run_id of the in-flight
        # reviewer/fixer subprocess so the slash handler can kill it
        # immediately (otherwise an operator has to wait up to
        # `stall_secs` for the current subprocess to finish naturally).
        self._local_review_active_run_ids: dict[str, str] = {}
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
            linear,
            self._gh,
            clock=clock,
        )
        self._reconcile_task: asyncio.Task[None] | None = None
        self._merge_wait_reconcile_task: asyncio.Task[None] | None = None
        self._reconcile_event_tasks: set[asyncio.Task[None]] = set()

    def _now(self) -> datetime:
        if self._clock is not None:
            return self._clock()
        return datetime.now(UTC)

    async def warmup(self) -> None:
        """One-time startup work: cache team workflow states, validate auth."""
        viewer_keys = await self.linear.viewer_team_keys()
        log.info("linear viewer sees teams: %s", viewer_keys)
        for binding in self.config.repos:
            if binding.linear_team_key not in viewer_keys:
                log.warning(
                    "team %s configured but not visible to API key — "
                    "the binding will produce no work",
                    binding.linear_team_key,
                )
                continue
            self._states[binding.linear_team_key] = await self.linear.team_states(
                binding.linear_team_key
            )
            self._validate_waiting_state(binding, self._states[binding.linear_team_key])

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

    async def _states_for_binding(self, binding: RepoBinding) -> dict[str, str]:
        states = self._states.get(binding.linear_team_key)
        if states is None:
            states = await self.linear.team_states(binding.linear_team_key)
            self._states[binding.linear_team_key] = states
        return states

    async def run(self) -> None:
        """The single long-lived task. Cancellation-safe."""
        await self.warmup()
        await self._restore_operator_waits()
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
                try:
                    await self._tick()
                except Exception:  # noqa: BLE001 — must not kill the loop
                    log.exception("poll cycle failed")
                try:
                    await asyncio.wait_for(
                        self._shutdown.wait(), timeout=self.config.poll_interval_secs
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
        self, payload: dict[str, Any]
    ) -> WebhookDispatchResult:
        """Handle a verified Linear webhook payload.

        Webhooks are just another low-latency source for the same work the
        poll loop already performs: issue state changes enter the normal
        dispatch scheduler, and comment events enter the slash-command
        handler shared with `_poll_slash_commands`.
        """
        event_type = str(payload.get("type") or "").casefold()
        if event_type == "comment":
            return await self._handle_webhook_comment(payload)
        if event_type == "issue":
            return await self._handle_webhook_issue(payload)
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
        self, payload: Mapping[str, Any]
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
        await self._restore_operator_waits()
        run_id = self._dispatch_run_ids.get(
            issue_id
        ) or self._review_poll_issue_ids.get(
            issue_id
        )
        run_id = run_id or await self._parked_manual_merge_run_id_for_issue(issue_id)
        if run_id is None or not self._slash_command_run_eligible(run_id):
            return WebhookDispatchResult(
                kind="comment", handled=False, detail="no active run"
            )
        try:
            handled = await self._handle_unseen_slash_comment(
                issue_id, run_id, comment
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
        self, payload: Mapping[str, Any]
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
        if state_changed:
            self._schedule_reconcile_task(
                self._reconciler.reconcile_linear_issue_event(
                    issue_id=issue_id,
                    action=action or "update",
                ),
                source=f"linear.issue.{action or 'update'}",
            )
        old_state_id, old_state_name, new_state_id, new_state_name = (
            _linear_issue_state_transition(payload)
        )
        issue = await self.linear.lookup_issue(issue_id)
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
        binding = self._ready_binding_for_issue(issue)
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

    def _slash_command_run_eligible(self, run_id: str) -> bool:
        return (
            run_id in self._active_run_ids
            or run_id in self._operator_wait_run_ids
            or run_id in self._review_poll_run_ids
            or run_id.startswith(MANUAL_MERGE_PARKED_RUN_PREFIX)
        )

    async def _parked_manual_merge_slash_pairs(self) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        for pr in await db.issue_prs.list_merge_candidates(self._conn):
            if pr.parked_at is None:
                continue
            if self._binding_for_pr(pr) is None:
                log.warning(
                    "cannot watch parked manual-merge PR %s#%d: no binding",
                    pr.github_repo,
                    pr.pr_number,
                )
                continue
            pairs.append((pr.issue_id, _manual_merge_parked_run_id(pr)))
        return pairs

    async def _parked_manual_merge_run_id_for_issue(
        self, issue_id: str
    ) -> str | None:
        pr = await db.issue_prs.get_for_issue(self._conn, issue_id=issue_id)
        if pr is None or pr.merged_at is not None or pr.parked_at is None:
            return None
        if self._binding_for_pr(pr) is None:
            return None
        return _manual_merge_parked_run_id(pr)

    async def _poll_slash_commands(self) -> None:
        """For each active run, fetch new comments and dispatch slash intents.

        The cursor (`(timestamp, ids_at_timestamp)`) lives in `comment_cursors`.
        We query with `gte` and drop any comment whose ID is in the cursor's
        boundary set, which both (a) avoids re-firing handled commands across
        restarts and (b) avoids losing comments tied at the boundary timestamp.
        """
        await self._restore_operator_waits()
        active_pairs = list(self._dispatch_run_ids.items())
        # Also include issues in active review polling that have no active fix run.
        dispatch_issue_ids = {iid for iid, _ in active_pairs}
        active_pairs += [
            (iid, run_id)
            for iid, run_id in self._review_poll_issue_ids.items()
            if iid not in dispatch_issue_ids
        ]
        pairs: list[tuple[str, str]] = []
        paired_issue_ids: set[str] = set()
        parked_pairs = await self._parked_manual_merge_slash_pairs()
        for issue_id, run_id in [*active_pairs, *parked_pairs]:
            if not self._slash_command_run_eligible(run_id):
                continue
            if issue_id in paired_issue_ids:
                continue
            paired_issue_ids.add(issue_id)
            pairs.append((issue_id, run_id))
        for issue_id, run_id in pairs:
            try:
                after, seen_ids = await self._resolve_comment_cursor(issue_id, run_id)
            except Exception:  # noqa: BLE001 — keep loop alive
                log.exception("failed to resolve cursor for issue %s", issue_id)
                continue
            try:
                comments = await self.linear.comments_since(issue_id, after)
            except LinearError as e:
                log.warning("comments_since failed for %s: %s", issue_id, e)
                continue
            latest_self_authored: LinearComment | None = None
            for comment in comments:
                if comment.id in seen_ids:
                    continue
                try:
                    await self._handle_unseen_slash_comment(issue_id, run_id, comment)
                except SlashHandlerFailure:
                    # Handler failed mid-transition; the rejection was posted
                    # and the comment was deliberately NOT marked seen. Stop
                    # iterating later comments for this issue so the failed
                    # command stays first-in-line on the next poll tick —
                    # otherwise a later comment could advance the cursor past
                    # the failed one, recreating the silent-drop behavior.
                    # Crucially, the self-authored cursor catch-up below is
                    # also skipped — we must not advance past any failed
                    # command.
                    latest_self_authored = None
                    break
                if comment.author_is_me:
                    # `_handle_unseen_slash_comment` deliberately did not
                    # advance the cursor for self-authored comments (so a
                    # `command_rejected` posted mid-failure can't strand the
                    # failed original). Now that the loop has reached the
                    # end without a `SlashHandlerFailure`, every prior comment
                    # in the batch was either handled or safely skipped — so
                    # we can advance the cursor past the latest self-authored
                    # one. Without this, `comments_since` would re-fetch the
                    # same bot-authored comments on every tick, growing
                    # unboundedly over long outages.
                    latest_self_authored = comment
            if latest_self_authored is not None:
                await self._advance_comment_cursor(
                    issue_id,
                    latest_self_authored.created_at,
                    {latest_self_authored.id},
                )

    async def _handle_unseen_slash_comment(
        self, issue_id: str, run_id: str, comment: LinearComment
    ) -> bool:
        """Process a single slash comment under the comment-event lock.

        Returns True when the comment was handled and persisted (marked seen
        + cursor advanced). Returns False when the comment was already seen
        (duplicate) OR carries no actionable intent for us (self-authored or
        externally mirrored — `slash.parse` filters those out). Raises
        `SlashHandlerFailure` when the handler failed mid-transition (e.g.
        `linear.move_issue` upstream error); in that case a rejection
        comment has been posted and the comment is intentionally NOT marked
        seen, so the caller MUST stop processing later comments for this
        issue (otherwise their cursor advance would leave the failed
        comment stranded).
        """
        # Self-authored comments (e.g. the `command_rejected` we post after
        # a `SlashHandlerFailure`) MUST NOT advance the cursor — otherwise a
        # rejection posted *after* a failed slash command would push the
        # cursor past the still-unprocessed original, permanently stranding
        # it. Skip without marking or advancing; the next poll's
        # `comments_since` will return it again, and we'll cheaply skip it
        # again until the failed command is retried and the cursor catches
        # up naturally. Non-self-authored comments (operator chatter,
        # external-thread mirrors, etc.) keep their pre-existing behavior of
        # marking-seen + advancing the cursor so the watermark moves
        # forward.
        if comment.author_is_me:
            return False
        async with self._comment_event_lock:
            if await db.comment_events.seen(self._conn, comment.id):
                return False
            try:
                await self._handle_slash_comments(issue_id, run_id, [comment])
            except SlashHandlerFailure as exc:
                log.warning(
                    "slash handler failed for comment %s on issue %s: %s",
                    comment.id,
                    issue_id,
                    exc.reason,
                )
                await self._post_command_rejected(
                    issue_id, exc.slash_text, exc.reason
                )
                raise
            await db.comment_events.mark(
                self._conn,
                issue_id=issue_id,
                comment_id=comment.id,
                seen_at=comment.created_at,
            )
        await self._advance_comment_cursor(issue_id, comment.created_at, {comment.id})
        return True

    async def _handle_slash_comments(
        self, issue_id: str, run_id: str, comments: list[LinearComment]
    ) -> None:
        for intent in slash.parse(comments):
            await self._handle_slash_intent(issue_id, run_id, intent)

    async def _advance_comment_cursor(
        self, issue_id: str, latest: str, latest_ids: set[str]
    ) -> None:
        try:
            stored = await db.comment_cursors.get(self._conn, issue_id)
            if stored is not None:
                stored_at, stored_ids = stored
                stored_dt = _parse_rfc3339(stored_at)
                latest_dt = _parse_rfc3339(latest)
                if stored_dt > latest_dt:
                    return
                if stored_dt == latest_dt:
                    latest_ids |= set(stored_ids)
            await db.comment_cursors.set(self._conn, issue_id, latest, latest_ids)
        except Exception:  # noqa: BLE001
            log.exception("failed to persist comment cursor for %s", issue_id)

    async def _resolve_comment_cursor(
        self, issue_id: str, run_id: str
    ) -> tuple[datetime, set[str]]:
        """Resolve the cursor, clamped to the current run's `started_at`.

        Without the clamp, a stale slash comment posted between two runs on
        the same issue would still be `> stored_cursor` when the next run
        starts, and the first poll tick could immediately kill it even though
        the command was not intended for it.
        """
        if run_id.startswith(MANUAL_MERGE_PARKED_RUN_PREFIX):
            run_started = await self._manual_merge_parked_started_at(issue_id)
        else:
            run_started = await self._run_started_at(run_id)
        stored = await db.comment_cursors.get(self._conn, issue_id)
        if stored is None:
            return run_started, set()
        stored_at, stored_ids = stored
        stored_dt = _parse_rfc3339(stored_at)
        if stored_dt < run_started:
            return run_started, set()
        return stored_dt, set(stored_ids)

    async def _run_started_at(self, run_id: str) -> datetime:
        cur = await self._conn.execute(
            "SELECT started_at FROM runs WHERE id = ?", (run_id,)
        )
        row = await cur.fetchone()
        if row is not None and row[0]:
            return _parse_rfc3339(row[0])
        return datetime(1970, 1, 1, tzinfo=UTC)

    async def _manual_merge_parked_started_at(self, issue_id: str) -> datetime:
        pr = await db.issue_prs.get_for_issue(self._conn, issue_id=issue_id)
        if pr is None or not pr.parked_at:
            return datetime(1970, 1, 1, tzinfo=UTC)
        try:
            return _parse_rfc3339(pr.parked_at)
        except ValueError:
            log.warning(
                "invalid parked_at timestamp for manual-merge PR %s#%d: %r",
                pr.github_repo,
                pr.pr_number,
                pr.parked_at,
            )
            return datetime(1970, 1, 1, tzinfo=UTC)

    async def _handle_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        if run_id.startswith(MANUAL_MERGE_PARKED_RUN_PREFIX):
            await self._handle_parked_manual_merge_slash_intent(issue_id, intent)
            return
        if run_id in self._cost_cap_run_bindings:
            await self._handle_cost_cap_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._implement_failed_run_bindings:
            await self._handle_implement_failed_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._review_failed_run_bindings:
            await self._handle_review_failed_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._merge_needs_approval_bindings:
            await self._handle_merge_needs_approval_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._acceptance_rejected_run_bindings:
            await self._handle_acceptance_rejected_slash_intent(issue_id, run_id, intent)
            return
        wait = await db.operator_waits.get_by_run_id(self._conn, run_id)
        if wait is not None:
            if wait.kind == db.operator_waits.KIND_COST_CAP:
                await self._handle_cost_cap_slash_intent(issue_id, run_id, intent)
                return
            if wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED:
                await self._handle_implement_failed_slash_intent(
                    issue_id, run_id, intent
                )
                return
            if wait.kind in (
                db.operator_waits.KIND_REVIEW_FAILED,
                db.operator_waits.KIND_REVIEW_STOPPED,
            ):
                await self._handle_review_failed_slash_intent(issue_id, run_id, intent)
                return
            if wait.kind == db.operator_waits.KIND_MERGE:
                await self._handle_merge_needs_approval_slash_intent(
                    issue_id, run_id, intent
                )
                return
            if wait.kind == db.operator_waits.KIND_ACCEPTANCE_BLOCKED:
                await self._handle_acceptance_blocked_slash_intent(
                    issue_id, run_id, intent
                )
                return
            if wait.kind == db.operator_waits.KIND_ACCEPTANCE_REJECTED:
                await self._handle_acceptance_rejected_slash_intent(
                    issue_id, run_id, intent
                )
                return
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                f"unsupported operator wait kind: {wait.kind}",
            )
            return
        if run_id in self._operator_wait_run_ids:
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                "operator wait is no longer active",
            )
            return
        if intent.kind is SlashKind.RETRY:
            monitor_run_id = self._review_poll_issue_ids.get(issue_id)
            if monitor_run_id is not None and monitor_run_id in self._review_poll_run_ids:
                await self._handle_active_review_retry_intent(
                    issue_id, monitor_run_id, intent
                )
                return
        if intent.kind is SlashKind.STOP:
            monitor_run_id = self._review_poll_issue_ids.get(issue_id)
            if monitor_run_id is not None and monitor_run_id in self._review_poll_run_ids:
                await self._stop_review_monitor(issue_id, monitor_run_id)
                return
            log.info(
                "$stop received for run %s (issue %s) — terminating runner",
                run_id,
                issue_id,
            )
            try:
                await self._runner.kill(run_id)
            except Exception:  # noqa: BLE001
                log.exception("runner.kill failed for run %s", run_id)
                raise
            return
        if intent.kind is SlashKind.SKIP_REVIEW:
            await self._handle_skip_review_intent(issue_id, run_id)
            return
        if intent.kind is SlashKind.SKIP_LOCAL_REVIEW:
            await self._handle_skip_local_review_intent(issue_id)
            return
        if intent.kind in (
            SlashKind.RETRY_ACCEPTANCE,
            SlashKind.SKIP_ACCEPTANCE,
        ):
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                "no blocked acceptance wait is active",
            )
            return
        log.info(
            "slash %s received for run %s (handler not implemented in this slice)",
            intent.kind,
            run_id,
        )
        if intent.kind is SlashKind.RETRY:
            await self._post_command_rejected(
                issue_id,
                "$retry",
                "no active retry handler for the current run state",
            )

    @staticmethod
    def _slash_text(intent: SlashIntent) -> str:
        return f"${intent.kind.value}"

    async def _post_command_rejected(
        self, issue_id: str, slash_text: str, reason: str
    ) -> None:
        try:
            await self.linear.post_comment(
                issue_id, truncate_body(command_rejected(slash_text, reason))
            )
        except LinearError as e:
            log.warning(
                "could not post %s rejection for %s: %s",
                slash_text,
                issue_id,
                e,
            )

    async def _handle_parked_manual_merge_slash_intent(
        self,
        issue_id: str,
        intent: SlashIntent,
        *,
        binding: RepoBinding | None = None,
        pr: db.issue_prs.IssuePR | None = None,
    ) -> None:
        if intent.kind is not SlashKind.APPROVE:
            log.info(
                "slash %s for parked manual-merge issue %s ignored",
                intent.kind,
                issue_id,
            )
            return
        if pr is None:
            pr = await db.issue_prs.get_for_issue(self._conn, issue_id=issue_id)
        if pr is None or pr.merged_at is not None or pr.parked_at is None:
            await self._post_command_rejected(
                issue_id,
                "$approve",
                "manual-merge parking marker is no longer active",
            )
            return
        if binding is None:
            binding = self._binding_for_pr(pr)
        if binding is None:
            await self._post_command_rejected(
                issue_id,
                "$approve",
                "no repository binding found for parked manual merge",
            )
            return
        try:
            await self._gh.pr_merge(
                pr.pr_number,
                strategy=binding.merge_strategy,
                auto=False,
                repo=binding.github_repo,
            )
        except GitHubError as e:
            log.warning(
                "manual merge failed for parked PR %s#%d on %s: %s",
                binding.github_repo,
                pr.pr_number,
                pr.identifier,
                e,
            )
            body = (
                f"manual merge failed for {pr.pr_url}: {e}\n\n"
                "The issue remains parked; reply with `$approve` to try again."
            )
            try:
                await self.linear.post_comment(issue_id, truncate_body(body))
            except LinearError as comment_error:
                log.warning(
                    "could not post manual merge failure for %s: %s",
                    pr.identifier,
                    comment_error,
                )

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
        try:
            await self._gh.pr_comment(
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
            "$retry received for active review monitor %s (issue %s); "
            "re-triggered @codex review",
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
        try:
            await self.linear.post_comment(issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("active review retry comment failed for %s: %s", issue_id, e)

    async def _handle_skip_local_review_intent(self, issue_id: str) -> None:
        """Handle `$skip-local-review`: kill the in-flight subprocess
        and ask the loop to exit.

        Order matters: set the flag first so the loop classifies the
        ensuing killed-subprocess failure as `SKIPPED` rather than
        `REVIEWER_FAILED` / `FIX_RUN_FAILED`. Then kill the active
        subprocess so the operator doesn't wait `stall_secs` for it to
        finish naturally. If no local-review is in flight, the slash
        is a no-op.
        """
        if issue_id not in self._local_review_skip_flags:
            log.info(
                "$skip-local-review for %s: no active local-review phase; "
                "ignoring",
                issue_id,
            )
            try:
                await self.linear.post_comment(
                    issue_id,
                    truncate_body(
                        command_rejected(
                            "$skip-local-review",
                            "no active local-review phase",
                        )
                    ),
                )
            except LinearError as e:
                log.warning(
                    "could not post skip-local-review rejection for %s: %s",
                    issue_id,
                    e,
                )
            return
        log.info("$skip-local-review for %s: setting skip flag", issue_id)
        self._local_review_skip_flags[issue_id] = True
        active_run_id = self._local_review_active_run_ids.get(issue_id)
        if active_run_id is not None:
            log.info(
                "$skip-local-review for %s: killing active subprocess %s",
                issue_id,
                active_run_id,
            )
            try:
                await self._runner.kill(active_run_id)
            except Exception:  # noqa: BLE001
                # A kill failure shouldn't dead-end the slash: the loop
                # will still exit at the next iteration boundary via
                # the skip flag. The runner's kill is best-effort by
                # design (`runner.py` docstring).
                log.exception(
                    "could not kill local-review subprocess %s for %s",
                    active_run_id,
                    issue_id,
                )

    async def _stop_review_monitor(self, issue_id: str, run_id: str) -> None:
        log.info("$stop received for review monitor %s (issue %s)", run_id, issue_id)
        now = datetime.now(UTC).isoformat()
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
                    await self.linear.post_comment(
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
        )
        if fix_run_id is not None and fix_run_id != run_id:
            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "interrupted",
                ended_at=now,
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
        try:
            await self.linear.post_comment(issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("review stop confirmation failed for %s: %s", issue_id, e)

    async def _handle_cost_cap_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._cost_cap_run_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(db.operator_waits.KIND_COST_CAP,),
            )
            if binding is None:
                return

        if intent.kind in (SlashKind.APPROVE, SlashKind.RETRY):
            states = await self._states_for_binding(binding)
            ready_id = states.get(binding.linear_states.ready)
            if ready_id is None:
                log.warning(
                    "could not resume cost-capped run %s: missing ready state %r",
                    run_id,
                    binding.linear_states.ready,
                )
                return
            try:
                await self.linear.move_issue(issue_id, ready_id)
            except LinearError as e:
                log.warning(
                    "could not move %s to ready for cost-cap resume: %s", issue_id, e
                )
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not move issue to ready state for resume: {e}",
                ) from e
            body = resumed(
                CommentVars(
                    stage="implement",
                    repo=binding.github_repo,
                    issue=0,
                    run_id=run_id,
                    next_stage=binding.linear_states.ready,
                )
            )
            try:
                await self.linear.post_comment(issue_id, truncate_body(body))
            except LinearError as e:
                log.warning(
                    "cost-cap resume comment failed for issue %s: %s", issue_id, e
                )
            await self._clear_operator_wait(issue_id, run_id)
            return

        if intent.kind in (SlashKind.REJECT, SlashKind.STOP):
            states = await self._states_for_binding(binding)
            blocked_id = states.get(binding.linear_states.blocked)
            if blocked_id is not None:
                try:
                    await self.linear.move_issue(issue_id, blocked_id)
                except LinearError as e:
                    log.warning(
                        "could not move %s to blocked after cost-cap stop: %s",
                        issue_id,
                        e,
                    )
                    raise SlashHandlerFailure(
                        slash_text=self._slash_text(intent),
                        reason=f"could not move issue to blocked state: {e}",
                    ) from e
            await self._clear_operator_wait(issue_id, run_id)
            return

        log.info(
            "slash %s received for cost-capped run %s (ignored)",
            intent.kind,
            run_id,
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
            created_at=datetime.now(UTC).isoformat(),
        )

    async def _handle_implement_failed_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._implement_failed_run_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(db.operator_waits.KIND_IMPLEMENT_FAILED,),
            )
            if binding is None:
                return

        states = await self._states_for_binding(binding)
        if intent.kind in (SlashKind.APPROVE, SlashKind.RETRY):
            ready_id = states.get(binding.linear_states.ready)
            if ready_id is None:
                log.warning(
                    "could not retry failed implement run %s: missing ready state %r",
                    run_id,
                    binding.linear_states.ready,
                )
                return
            try:
                await self.linear.move_issue(issue_id, ready_id)
            except LinearError as e:
                log.warning("could not move %s to ready for retry: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not move issue to ready state for retry: {e}",
                ) from e
            body = resumed(
                CommentVars(
                    stage="implement",
                    repo=binding.github_repo,
                    issue=0,
                    run_id=run_id,
                    next_stage=binding.linear_states.ready,
                )
            )
            try:
                await self.linear.post_comment(issue_id, truncate_body(body))
            except LinearError as e:
                log.warning(
                    "implement retry comment failed for issue %s: %s", issue_id, e
                )
            await self._clear_operator_wait(issue_id, run_id)
            return

        if intent.kind in (SlashKind.REJECT, SlashKind.STOP):
            blocked_id = states.get(binding.linear_states.blocked)
            if blocked_id is None:
                log.warning(
                    "could not stop failed implement run %s: missing blocked state %r",
                    run_id,
                    binding.linear_states.blocked,
                )
                try:
                    await self.linear.post_comment(
                        issue_id,
                        truncate_body(
                            command_rejected(
                                f"${intent.kind}",
                                "missing blocked state; keeping issue parked",
                            )
                        ),
                    )
                except LinearError as e:
                    log.warning(
                        "implement stop rejection comment failed for %s: %s",
                        issue_id,
                        e,
                    )
                return
            try:
                await self.linear.move_issue(issue_id, blocked_id)
            except LinearError as e:
                log.warning("could not move %s to blocked: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not move issue to blocked state: {e}",
                ) from e
            await self._clear_operator_wait(issue_id, run_id)
            return

        log.info(
            "slash %s received for failed implement run %s (ignored)",
            intent.kind,
            run_id,
        )

    async def _restore_operator_waits(self) -> None:
        waits = await db.operator_waits.list_all(self._conn)
        for wait in waits:
            if wait.kind not in (
                db.operator_waits.KIND_COST_CAP,
                db.operator_waits.KIND_IMPLEMENT_FAILED,
                db.operator_waits.KIND_REVIEW_FAILED,
                db.operator_waits.KIND_REVIEW_STOPPED,
                db.operator_waits.KIND_MERGE,
                db.operator_waits.KIND_ACCEPTANCE_BLOCKED,
                db.operator_waits.KIND_ACCEPTANCE_REJECTED,
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
                    "cannot restore operator wait for issue %s: no binding for %s/%s label=%r",
                    wait.issue_id,
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
        if wait.kind == db.operator_waits.KIND_COST_CAP:
            self._cost_cap_run_bindings[wait.run_id] = binding
        elif wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED:
            self._implement_failed_run_bindings[wait.run_id] = binding
        elif wait.kind in (
            db.operator_waits.KIND_REVIEW_FAILED,
            db.operator_waits.KIND_REVIEW_STOPPED,
        ):
            self._review_failed_run_bindings[wait.run_id] = binding
        elif wait.kind == db.operator_waits.KIND_MERGE:
            self._merge_needs_approval_bindings[wait.run_id] = binding
        elif wait.kind == db.operator_waits.KIND_ACCEPTANCE_REJECTED:
            self._acceptance_rejected_run_bindings[wait.run_id] = binding

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

    async def _binding_for_review_issue_id(
        self, issue_id: str, *, state: db.review_state.ReviewState
    ) -> RepoBinding | None:
        cur = await self._conn.execute("SELECT team_key FROM issues WHERE id = ?", (issue_id,))
        row = await cur.fetchone()
        team_key = str(row["team_key"]) if row is not None else ""
        for binding in self.config.repos:
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

    def _binding_for_operator_wait(
        self, wait: db.operator_waits.OperatorWait
    ) -> RepoBinding | None:
        for binding in self.config.repos:
            if (
                binding.linear_team_key == wait.linear_team_key
                and binding.github_repo == wait.github_repo
                and (binding.issue_label or "") == wait.issue_label
            ):
                return binding
        return None

    async def _track_operator_wait(
        self, issue_id: str, run_id: str, binding: RepoBinding
    ) -> None:
        self._dispatch_run_ids[issue_id] = run_id
        self._operator_wait_run_ids.add(run_id)
        self._cost_cap_run_bindings[run_id] = binding
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue_id,
            run_id=run_id,
            kind=db.operator_waits.KIND_COST_CAP,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=datetime.now(UTC).isoformat(),
        )

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
        if target_id is not None:
            try:
                await self.linear.move_issue(issue.id, target_id)
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
            await self.linear.post_comment(issue.id, truncate_body(body))
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
            created_at=datetime.now(UTC).isoformat(),
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
            created_at=datetime.now(UTC).isoformat(),
        )

    async def _acceptance_pr_url(self, issue_id: str) -> str:
        state = await db.acceptance_state.get(self._conn, issue_id)
        if state.pr_url:
            return state.pr_url
        if state.pr_number is not None:
            return f"#{state.pr_number}"
        return "(no PR yet)"

    async def _handle_acceptance_blocked_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = await self._restore_operator_wait_binding(
            issue_id,
            run_id,
            intent,
            expected_kinds=(db.operator_waits.KIND_ACCEPTANCE_BLOCKED,),
        )
        if binding is None:
            return

        state = await db.acceptance_state.get(self._conn, issue_id)
        if intent.kind is SlashKind.RETRY_ACCEPTANCE:
            pr_url = state.pr_url or (
                f"https://github.com/{binding.github_repo}/pull/{state.pr_number}"
                if state.pr_number is not None
                else "(no PR yet)"
            )
            states = await self._states_for_binding(binding)
            active_state_names = (
                binding.linear_states.needs_approval,
                binding.linear_states.in_acceptance,
                binding.linear_states.in_progress,
            )
            target_state_name = next(
                (name for name in dict.fromkeys(active_state_names) if states.get(name)),
                None,
            )
            if target_state_name is None:
                log.warning(
                    "could not retry blocked acceptance run %s: missing active state",
                    run_id,
                )
                await self._post_command_rejected(
                    issue_id,
                    self._slash_text(intent),
                    "missing active Linear state; keeping acceptance blocked",
                )
                return
            target_state_id = states[target_state_name]
            try:
                await self.linear.move_issue(issue_id, target_state_id)
            except LinearError as e:
                log.warning(
                    "could not move %s to %s for acceptance retry: %s",
                    issue_id,
                    target_state_name,
                    e,
                )
                await self._post_command_rejected(
                    issue_id,
                    self._slash_text(intent),
                    "could not move issue to an active Linear state; "
                    "keeping acceptance blocked",
                )
                return
            await db.acceptance_state.reset(self._conn, issue_id)
            await self._clear_operator_wait(issue_id, run_id)
            body = acceptance_retry_requested(
                CommentVars(
                    stage="acceptance",
                    repo=binding.github_repo,
                    issue=state.pr_number or 0,
                    pr_url=pr_url,
                    run_id=run_id,
                )
            )
            try:
                await self.linear.post_comment(issue_id, truncate_body(body))
            except LinearError as e:
                log.warning("acceptance retry comment failed for %s: %s", issue_id, e)
            return

        if intent.kind is SlashKind.SKIP_ACCEPTANCE:
            if state.pr_number is None:
                await self._post_command_rejected(
                    issue_id,
                    self._slash_text(intent),
                    "no PR found for blocked acceptance",
                )
                return
            try:
                issue = await self.linear.lookup_issue(issue_id)
            except LinearError as e:
                log.warning("could not look up %s for skip-acceptance: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not look up issue for skip-acceptance: {e}",
                ) from e

            await db.acceptance_state.record_verdict(
                self._conn,
                issue_id,
                verdict="pass",
                artifacts_url=state.last_artifacts_url,
            )
            await self._clear_operator_wait(issue_id, run_id)
            if _needs_human_approval_label_present(issue):
                await self._open_merge_wait_for_human_approval_label(
                    binding=binding,
                    issue=issue,
                    pr_url=state.pr_url,
                )
            else:
                self._schedule_merge(
                    binding=binding,
                    issue=issue,
                    pr_number=state.pr_number,
                    pr_url=state.pr_url,
                )
            body = acceptance_skipped(
                CommentVars(
                    stage="acceptance",
                    repo=binding.github_repo,
                    issue=state.pr_number,
                    pr_url=state.pr_url,
                    run_id=run_id,
                    next_stage="merge",
                )
            )
            try:
                await self.linear.post_comment(issue_id, truncate_body(body))
            except LinearError as e:
                log.warning("acceptance skip comment failed for %s: %s", issue_id, e)
            return

        await self._post_command_rejected(
            issue_id,
            self._slash_text(intent),
            "acceptance is blocked; use $retry-acceptance or $skip-acceptance",
        )

    async def _clear_operator_wait(self, issue_id: str, run_id: str) -> None:
        if self._dispatch_run_ids.get(issue_id) == run_id:
            self._dispatch_run_ids.pop(issue_id, None)
        self._operator_wait_run_ids.discard(run_id)
        self._cost_cap_run_bindings.pop(run_id, None)
        self._implement_failed_run_bindings.pop(run_id, None)
        self._review_failed_run_bindings.pop(run_id, None)
        self._merge_needs_approval_bindings.pop(run_id, None)
        self._acceptance_rejected_run_bindings.pop(run_id, None)
        await db.operator_waits.delete(self._conn, issue_id, run_id)

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

        try:
            issue = await self.linear.lookup_issue(wait.issue_id)
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
                await self.linear.post_comment(wait.issue_id, truncate_body(body))
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
            try:
                issue = await self.linear.lookup_issue(run.issue_id)
            except LinearError as e:
                log.warning("could not resolve issue for review run %s: %s", run.id, e)
                continue
            state = await db.review_state.get(self._conn, issue.id)
            binding = self._binding_for_review(issue, state)
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
                    error=(
                        "review monitor no longer matches any configured "
                        "repository binding"
                    ),
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
                    "review run %s for %s: dispatch capacity is zero "
                    "(global=%d, binding=%d)",
                    run.id,
                    issue.identifier,
                    self.config.global_max_concurrent,
                    binding.max_concurrent,
                )
                continue
            scheduled.append(self._schedule_review_poll(run, binding, issue))
        return scheduled

    def _binding_for_issue(self, issue: LinearIssue) -> RepoBinding | None:
        for binding in self.config.repos:
            if binding.linear_team_key != issue.team_key:
                continue
            if binding.issue_label and binding.issue_label not in issue.labels:
                continue
            return binding
        return None

    def _binding_for_review(
        self, issue: LinearIssue, state: db.review_state.ReviewState
    ) -> RepoBinding | None:
        if state.github_repo:
            for binding in self.config.repos:
                if binding.linear_team_key != issue.team_key:
                    continue
                if binding.github_repo != state.github_repo:
                    continue
                if (binding.issue_label or "") != state.issue_label:
                    continue
                return binding
            return None
        return self._binding_for_issue(issue)

    def _schedule_review_poll(
        self, run: db.runs.Run, binding: RepoBinding, issue: LinearIssue
    ) -> asyncio.Task[None]:
        self._review_poll_run_ids.add(run.id)
        self._review_poll_issue_ids[issue.id] = run.id
        task = asyncio.create_task(self._poll_review_run_with_limits(run, binding, issue))
        self._review_poll_tasks.add(task)
        self._review_poll_run_tasks[run.id] = task
        task.add_done_callback(
            partial(self._review_poll_done, run_id=run.id, issue_id=issue.id)
        )
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

    async def _local_review_approved_for_current_review(
        self, run: db.runs.Run
    ) -> bool:
        latest_implement = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=run.issue_id,
            stage="implement",
        )
        if latest_implement is None:
            return False
        latest_local_review = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=run.issue_id,
            stage="local_review",
            started_at_gte=latest_implement.started_at,
        )
        if latest_local_review is None or latest_local_review.status != "completed":
            return False
        return _parse_rfc3339(latest_local_review.started_at) <= _parse_rfc3339(
            run.started_at
        )

    async def _poll_review_run_with_limits(
        self,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
    ) -> None:
        if self.config.global_max_concurrent <= 0 or binding.max_concurrent <= 0:
            log.info(
                "review run %s for %s: dispatch capacity is zero "
                "(global=%d, binding=%d)",
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
        rearm_done = True
        if rearm_retry_pending:
            state = await db.review_state.get(self._conn, current_issue.id)
            rearm_done = await self._retrigger_codex_review_unless_approved(
                binding=current_binding,
                issue=current_issue,
                state=state,
                require_no_signal=True,
            )
            if rearm_done:
                await self._clear_review_rearm_retry(run.id)
        handled_feedback = await self._poll_review_run(
            run, current_binding, current_issue
        )
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
        try:
            current = await self.linear.lookup_issue(run.issue_id)
        except LinearError as e:
            log.warning(
                "could not revalidate %s before review polling: %s",
                issue.identifier,
                e,
            )
            return None
        state = await db.review_state.get(self._conn, current.id)
        current_binding = self._binding_for_review(current, state)
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
                error=(
                    "review monitor no longer matches any configured "
                    "repository binding"
                ),
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
        if current.state_name == current_binding.linear_states.in_progress:
            await self._move_issue_to_review_state(
                binding=current_binding, issue=current
            )
        return current_binding, current

    def _review_poll_done(
        self, task: asyncio.Task[None], run_id: str, issue_id: str = ""
    ) -> None:
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
            ended_at=datetime.now(UTC).isoformat(),
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

        now = datetime.now(UTC).isoformat()
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
        state = await db.review_state.get(self._conn, issue.id)
        if state.pr_number is None:
            await self._fail_review_run(
                run=run,
                binding=binding,
                issue=issue,
                error="review run has no PR number",
                last_log="",
            )
            return False

        try:
            checks = await self._gh.pr_checks(state.pr_number, repo=binding.github_repo)
        except GitHubError as e:
            failures = await db.review_state.bump_ci_fetch_failures(
                self._conn, issue.id
            )
            log.warning(
                "gh pr checks failed for %s#%d (%d/%d): %s",
                binding.github_repo,
                state.pr_number,
                failures,
                CI_FETCH_FAILURE_LIMIT,
                e,
            )
            if failures >= CI_FETCH_FAILURE_LIMIT:
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=(
                        "gh pr checks failed "
                        f"{failures} consecutive times: {e}"
                    ),
                    last_log=str(e),
                )
            return False

        await db.review_state.reset_ci_fetch_failures(self._conn, issue.id)

        head_sha = _unknown_head_ci_scope(checks)
        mergeable: str = ""
        try:
            view = await self._gh.pr_view(state.pr_number, repo=binding.github_repo)
            head_sha = str(view.get("headRefOid") or "") or head_sha
            mergeable = str(view.get("mergeable") or "")
        except Exception as e:  # noqa: BLE001
            log.warning(
                "could not fetch PR view for %s#%d: %s",
                binding.github_repo,
                state.pr_number,
                e,
            )

        head_committed_at = await _commit_committed_at_or_empty(
            self._gh,
            repo=binding.github_repo,
            sha=head_sha,
        )
        ci_runs = [_review_check_from_gh(c) for c in checks.runs]
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
                # CI failure has already dispatched a fix-run we still need to
                # notice later Codex/human review comments on the same head.
                try:
                    raw_reviews = await self._gh.pr_reviews(
                        state.pr_number, repo=binding.github_repo
                    )
                    review_signal_reviews = _reviews_from_github(raw_reviews)
                except GitHubError as e:
                    log.warning(
                        "could not fetch PR reviews for %s#%d: %s",
                        binding.github_repo,
                        state.pr_number,
                        e,
                    )
                    review_signal_reviews = ()

                try:
                    raw_comments = await self._gh.pr_review_comments(
                        state.pr_number, repo=binding.github_repo
                    )
                    review_signal_comments = _review_comments_from_github(raw_comments)
                except GitHubError as e:
                    log.warning(
                        "could not fetch PR review comments for %s#%d: %s",
                        binding.github_repo,
                        state.pr_number,
                        e,
                    )
                    review_signal_comments = []

                try:
                    raw_reactions = await self._gh.pr_reactions(
                        state.pr_number, repo=binding.github_repo
                    )
                    review_signal_reactions = _reactions_from_github(raw_reactions)
                except GitHubError as e:
                    log.warning(
                        "could not fetch PR reactions for %s#%d: %s",
                        binding.github_repo,
                        state.pr_number,
                        e,
                    )
                    review_signal_reactions = ()

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
        else:
            try:
                raw_reviews = await self._gh.pr_reviews(
                    state.pr_number, repo=binding.github_repo
                )
                reviews: tuple[Review, ...] = _reviews_from_github(raw_reviews)
            except GitHubError as e:
                log.warning(
                    "could not fetch PR reviews for %s#%d: %s",
                    binding.github_repo,
                    state.pr_number,
                    e,
                )
                reviews = ()

            try:
                raw_comments = await self._gh.pr_review_comments(
                    state.pr_number, repo=binding.github_repo
                )
                comments: list[ReviewComment] = _review_comments_from_github(raw_comments)
            except GitHubError as e:
                log.warning(
                    "could not fetch PR review comments for %s#%d: %s",
                    binding.github_repo,
                    state.pr_number,
                    e,
                )
                comments = []

            try:
                raw_reactions = await self._gh.pr_reactions(
                    state.pr_number, repo=binding.github_repo
                )
                reactions: tuple[Reaction, ...] = _reactions_from_github(raw_reactions)
            except GitHubError as e:
                log.warning(
                    "could not fetch PR reactions for %s#%d: %s",
                    binding.github_repo,
                    state.pr_number,
                    e,
                )
                reactions = ()

            try:
                issue_comments = await self._gh.pr_issue_comments(
                    state.pr_number, repo=binding.github_repo
                )
            except GitHubError as e:
                log.warning(
                    "could not fetch PR issue comments for %s#%d: %s",
                    binding.github_repo,
                    state.pr_number,
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

        await self._maybe_post_codex_lgtm(
            run=run,
            binding=binding,
            issue=issue,
            state=state,
            pr_number=state.pr_number,
            head_committed_at=head_committed_at,
            issue_comments=issue_comments,
        )

        if verdict.kind is VerdictKind.PENDING and verdict.rule == "no_signal":
            await self._maybe_rearm_codex_review_for_no_signal(
                run=run,
                binding=binding,
                issue=issue,
                state=state,
                head_sha=head_sha,
            )

        if verdict.kind is not VerdictKind.CHANGES_REQUESTED:
            return False
        if verdict.merge_conflict:
            if not should_dispatch_fix_run(
                prev_signature=state.last_trigger_signature,
                new_signature=verdict.trigger_signature,
            ):
                return False
            if has_hit_iteration_cap(
                iteration=state.iteration, cap=self.config.review_iteration_cap
            ):
                await self._park_review_for_approval(
                    run=run,
                    binding=binding,
                    issue=issue,
                    trigger=verdict.trigger_signature,
                )
                return True
            dispatched = await self._dispatch_merge_conflict_fix_run(
                run=run,
                binding=binding,
                issue=issue,
                iteration=state.iteration + 1,
            )
            if dispatched:
                await db.review_state.bump_iteration(self._conn, issue.id)
                # Clear rather than set the signature: if the rebase produced no
                # new commit (HEAD SHA unchanged), we still want the next poll to
                # re-evaluate instead of being blocked by the dedup gate.
                await db.review_state.set_signature(self._conn, issue.id, "")
            return dispatched
        if not should_dispatch_fix_run(
            prev_signature=state.last_trigger_signature,
            new_signature=verdict.trigger_signature,
        ):
            return False
        if has_hit_iteration_cap(
            iteration=state.iteration, cap=self.config.review_iteration_cap
        ):
            await self._park_review_for_approval(
                run=run,
                binding=binding,
                issue=issue,
                trigger=verdict.trigger_signature,
            )
            return True

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
            await db.review_state.bump_iteration(self._conn, issue.id)
            await db.review_state.set_signature(
                self._conn, issue.id, verdict.trigger_signature
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

        async with self._review_fix_dispatch_slot(binding, issue):
            try:
                workspace_path = await self._workspace.acquire(binding, issue)
            except Exception as e:  # noqa: BLE001
                log.exception(
                    "workspace acquire failed for review fix-run %s", issue.identifier
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"workspace acquire failed: {e}",
                    last_log=str(e),
                )
                return False

            branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
            try:
                await _git_fetch_branch(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                self._workspace.release(binding, issue)
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

            fix_run_id = str(uuid.uuid4())
            await db.runs.create(
                self._conn,
                id=fix_run_id,
                issue_id=issue.id,
                stage="review_fix",
                status="running",
                pid=None,
                started_at=datetime.now(UTC).isoformat(),
            )
            self._dispatch_run_ids[issue.id] = fix_run_id

            try:
                prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
                cost, final_kind, final_returncode = await self._run_fix_agent(
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
                    ended_at=datetime.now(UTC).isoformat(),
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"review fix-run execution failed: {e}",
                    last_log=str(e),
                )
                return False
            finally:
                if self._dispatch_run_ids.get(issue.id) == fix_run_id:
                    self._dispatch_run_ids.pop(issue.id, None)
                self._workspace.release(binding, issue)

            if cost > 0:
                await db.runs.add_cost(self._conn, fix_run_id, cost)

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
                    ended_at=datetime.now(UTC).isoformat(),
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
                ended_at=datetime.now(UTC).isoformat(),
            )

            try:
                await self._push_fn(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "git push failed for review fix-run %s: %s", issue.identifier, e
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"push failed: {e}",
                    last_log=str(e),
                )
                return False

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
                "could not classify approval before re-triggering @codex review "
                "on %s#%d: %s",
                binding.github_repo,
                state.pr_number,
                e,
            )
            if require_no_signal:
                return False
        else:
            if verdict.kind is VerdictKind.APPROVED:
                log.info(
                    "skipping @codex review re-trigger on %s#%d for %s: "
                    "approval already present",
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
        view = await self._gh.pr_view(pr_number, repo=binding.github_repo)
        head_sha = str(view.get("headRefOid") or "")
        if not head_sha:
            raise GitHubError(
                f"pr view missing headRefOid for {binding.github_repo}#{pr_number}"
            )
        comments = []
        if include_comments:
            comments = await self._gh.pr_review_comments(
                pr_number, repo=binding.github_repo
            )
        reviews = await self._gh.pr_reviews(pr_number, repo=binding.github_repo)
        reactions = await self._gh.pr_reactions(pr_number, repo=binding.github_repo)
        try:
            issue_comments = await self._gh.pr_issue_comments(
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
            await self._gh.pr_comment(
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
            f"\nTrigger signature: {verdict.trigger_signature}\n"
            f"Review iteration: {iteration}/{cap}"
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

        async with self._review_fix_dispatch_slot(binding, issue):
            # Post the "starting" comment once we have a slot — this ensures
            # the message is accurate ("dispatching now", not "queued").
            prior_cost = await db.runs.cost_for_issue(self._conn, issue.id)
            v = CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=pr_url,
                review_iter=iteration,
                trigger=verdict.trigger_signature[:80],
                cost=f"${prior_cost:.2f}",
            )
            try:
                await self.linear.post_comment(
                    issue.id, truncate_body(reviewing_feedback(v))
                )
            except LinearError as e:
                log.warning(
                    "could not post reviewing_feedback comment for %s: %s",
                    issue.identifier,
                    e,
                )

            try:
                workspace_path = await self._workspace.acquire(binding, issue)
            except Exception as e:  # noqa: BLE001
                log.exception(
                    "workspace acquire failed for review fix-run %s", issue.identifier
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"workspace acquire failed: {e}",
                    last_log=str(e),
                )
                return False

            branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
            try:
                await _git_fetch_branch(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                self._workspace.release(binding, issue)
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

            fix_run_id = str(uuid.uuid4())
            await db.runs.create(
                self._conn,
                id=fix_run_id,
                issue_id=issue.id,
                stage="review_fix",
                status="running",
                pid=None,
                started_at=datetime.now(UTC).isoformat(),
            )
            self._dispatch_run_ids[issue.id] = fix_run_id

            try:
                prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
                cost, final_kind, final_returncode = await self._run_fix_agent(
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
                    ended_at=datetime.now(UTC).isoformat(),
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"review fix-run execution failed: {e}",
                    last_log=str(e),
                )
                return False
            finally:
                if self._dispatch_run_ids.get(issue.id) == fix_run_id:
                    self._dispatch_run_ids.pop(issue.id, None)
                self._workspace.release(binding, issue)

            if cost > 0:
                await db.runs.add_cost(self._conn, fix_run_id, cost)

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
                    ended_at=datetime.now(UTC).isoformat(),
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
                ended_at=datetime.now(UTC).isoformat(),
            )

            try:
                await self._push_fn(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "git push failed for review fix-run %s: %s", issue.identifier, e
                )
                await self._fail_review_run(
                    run=run,
                    binding=binding,
                    issue=issue,
                    error=f"push failed: {e}",
                    last_log=str(e),
                )
                return False

            total_cost = await db.runs.cost_for_issue(self._conn, issue.id)
            v_done = CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=pr_url,
                review_iter=iteration,
                cost=f"${total_cost:.2f}",
                commit_url=_github_commit_url(binding.github_repo, pushed_sha),
            )
            try:
                await self.linear.post_comment(
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
        if dispatched:
            await db.review_state.bump_iteration(self._conn, issue.id)
            await db.review_state.set_signature(self._conn, issue.id, signature)
        return dispatched

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
    ) -> bool:
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
            cap_usd = effective_cap(
                global_cap_usd=self.config.cost_cap_per_issue_usd,
                binding_override=binding.cost_cap_usd,
            )
            if cap_usd > 0 and prior_total >= cap_usd:
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=f"required-check cost cap reached: ${prior_total:.4f}",
                    merge_run_id=merge_run_id,
                )
                return False

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
                started_at=datetime.now(UTC).isoformat(),
            )
            self._dispatch_run_ids[issue.id] = fix_run_id

            try:
                (
                    cost,
                    final_kind,
                    final_returncode,
                    cap_breached,
                ) = await self._run_required_check_fix_agent(
                    binding=binding,
                    issue=issue,
                    run_id=fix_run_id,
                    workspace_path=workspace_path,
                    prompt=prompt,
                    prior_total=prior_total,
                    cap_usd=cap_usd,
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
                    ended_at=datetime.now(UTC).isoformat(),
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

            if cost > 0:
                await db.runs.add_cost(self._conn, fix_run_id, cost)

            if cap_breached:
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=datetime.now(UTC).isoformat(),
                )
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=(
                        "required-check cost cap reached: "
                        f"${prior_total + cost:.4f}"
                    ),
                    merge_run_id=merge_run_id,
                )
                return False

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
                    ended_at=datetime.now(UTC).isoformat(),
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
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=datetime.now(UTC).isoformat(),
                )
                await self._mark_merge_required_check_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=(
                        "required-check fix-run completed without advancing "
                        f"{branch}; HEAD stayed at {short_sha}"
                    ),
                    merge_run_id=merge_run_id,
                )
                return False

            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "completed",
                ended_at=datetime.now(UTC).isoformat(),
            )

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
        cap_usd: float,
    ) -> tuple[float, str, int | None, bool]:
        warning_pct = effective_warning_pct(
            global_pct=self.config.cost_warning_pct,
            binding_override=binding.cost_warning_pct,
        )
        warning_already_fired = (
            await db.cost_marks.warning_posted_at(self._conn, issue.id) is not None
        )
        max_budget_usd: float | None = None
        if cap_usd > 0:
            max_budget_usd = cap_usd - prior_total
            if max_budget_usd <= 0:
                return 0.0, "cost_cap", None, True
        command = build_fix_runner_command(
            binding.agent,
            prompt,
            max_budget_usd=max_budget_usd,
            codex_model=binding.codex_model,
            workspace_path=workspace_path,
        )
        return await self._run_stage_command(
            binding=binding,
            issue=issue,
            command=command,
            run_id=run_id,
            workspace_path=workspace_path,
            stage="review_fix",
            prior_total=prior_total,
            cap_usd=cap_usd,
            warning_pct=warning_pct,
            warning_already_fired=warning_already_fired,
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
            cap_usd = effective_cap(
                global_cap_usd=self.config.cost_cap_per_issue_usd,
                binding_override=binding.cost_cap_usd,
            )
            if cap_usd > 0 and prior_total >= cap_usd:
                await self._mark_merge_conflict_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=(
                        "merge-conflict cost cap reached: "
                        f"${prior_total:.4f}"
                    ),
                    merge_run_id=merge_run_id,
                )
                return False

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
                started_at=datetime.now(UTC).isoformat(),
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
            command = build_codex_workspace_write_command(
                prompt=prompt,
                codex_model=binding.codex_model,
            )
            codex_binding = binding.model_copy(update={"agent": "codex"})
            warning_pct = effective_warning_pct(
                global_pct=self.config.cost_warning_pct,
                binding_override=binding.cost_warning_pct,
            )
            warning_already_fired = (
                await db.cost_marks.warning_posted_at(self._conn, issue.id) is not None
            )
            try:
                cost, final_kind, final_returncode, cap_breached = (
                    await self._run_stage_command(
                        binding=codex_binding,
                        issue=issue,
                        command=command,
                        run_id=fix_run_id,
                        workspace_path=workspace_path,
                        stage="review_fix",
                        prior_total=prior_total,
                        cap_usd=cap_usd,
                        warning_pct=warning_pct,
                        warning_already_fired=warning_already_fired,
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
                    ended_at=datetime.now(UTC).isoformat(),
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

            if cost > 0:
                await db.runs.add_cost(self._conn, fix_run_id, cost)

            total_cost = prior_total + cost
            if cap_breached:
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=datetime.now(UTC).isoformat(),
                )
                await self._mark_merge_conflict_fix_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    reason=(
                        "merge-conflict cost cap reached: "
                        f"${total_cost:.4f}"
                    ),
                    merge_run_id=merge_run_id,
                )
                return False

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
                    ended_at=datetime.now(UTC).isoformat(),
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
                ended_at=datetime.now(UTC).isoformat(),
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
                marked_at=datetime.now(UTC).isoformat(),
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

        async with self._review_fix_dispatch_slot(binding, issue):
            # Post the "fixing" comment once we have a slot.
            prior_cost = await db.runs.cost_for_issue(self._conn, issue.id)
            v_start = CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=pr_url,
                review_iter=iteration,
                cost=f"${prior_cost:.2f}",
            )
            try:
                await self.linear.post_comment(
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
            await db.runs.create(
                self._conn,
                id=fix_run_id,
                issue_id=issue.id,
                stage="review_fix",
                status="running",
                pid=None,
                started_at=datetime.now(UTC).isoformat(),
            )
            self._dispatch_run_ids[issue.id] = fix_run_id

            try:
                cost: float = 0.0
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
                        ) + cost
                        run_cost, final_kind, final_returncode = await self._run_fix_agent(
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
                            ended_at=datetime.now(UTC).isoformat(),
                        )
                        await self._fail_review_run(
                            run=run,
                            binding=binding,
                            issue=issue,
                            error=f"merge-conflict fix-run execution failed: {e}",
                            last_log=str(e),
                        )
                        return False
                    cost += run_cost

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
                            ended_at=datetime.now(UTC).isoformat(),
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
                            ended_at=datetime.now(UTC).isoformat(),
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
                                ended_at=datetime.now(UTC).isoformat(),
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

            if cost > 0:
                await db.runs.add_cost(self._conn, fix_run_id, cost)

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
                ended_at=datetime.now(UTC).isoformat(),
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

            total_cost = await db.runs.cost_for_issue(self._conn, issue.id)
            v_done = CommentVars(
                stage="review",
                repo=binding.github_repo,
                issue=state.pr_number or 0,
                pr_url=pr_url,
                review_iter=iteration,
                cost=f"${total_cost:.2f}",
                commit_url=_github_commit_url(binding.github_repo, pushed_sha),
            )
            try:
                await self.linear.post_comment(
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
        await db.runs.update_status(
            self._conn,
            fix_run_id,
            "failed",
            ended_at=datetime.now(UTC).isoformat(),
        )
        await self._fail_review_run(
            run=run,
            binding=binding,
            issue=issue,
            error=(
                f"review fix-run completed without advancing {branch}; "
                f"HEAD stayed at {short_sha}"
            ),
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
                tail = await self._gh.check_log_tail(check, repo=repo)
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
            created_at=datetime.now(UTC).isoformat(),
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
            created_at=datetime.now(UTC).isoformat(),
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
        if intent.kind not in (SlashKind.RETRY, SlashKind.APPROVE):
            if intent.kind in (SlashKind.REJECT, SlashKind.STOP):
                states = await self._states_for_binding(binding)
                blocked_id = states.get(binding.linear_states.blocked)
                try:
                    issue = await self.linear.lookup_issue(issue_id)
                except LinearError as e:
                    log.warning("could not look up %s for reject: %s", issue_id, e)
                    raise SlashHandlerFailure(
                        slash_text=self._slash_text(intent),
                        reason=f"could not look up issue for reject: {e}",
                    ) from e
                if blocked_id is not None:
                    try:
                        await self.linear.move_issue(issue_id, blocked_id)
                    except LinearError as e:
                        log.warning(
                            "could not move %s to blocked: %s", issue.identifier, e
                        )
                        raise SlashHandlerFailure(
                            slash_text=self._slash_text(intent),
                            reason=f"could not move issue to blocked state: {e}",
                        ) from e
                await self._clear_operator_wait(issue_id, run_id)
            else:
                log.info("slash %s for review-failed run %s ignored", intent.kind, run_id)
            return

        # $retry or $approve: restart the review monitor.
        # Look up the issue BEFORE clearing the operator wait — if lookup
        # fails we want the wait (and its `_dispatch_run_ids` entry) to
        # survive so the next poll tick can retry. Clearing first would
        # make the issue invisible to slash polling on the retry.
        try:
            issue = await self.linear.lookup_issue(issue_id)
        except LinearError as e:
            log.warning("could not look up %s for retry: %s", issue_id, e)
            raise SlashHandlerFailure(
                slash_text=self._slash_text(intent),
                reason=f"could not look up issue for retry: {e}",
            ) from e
        await self._clear_operator_wait(issue_id, run_id)
        new_run_id = str(uuid.uuid4())
        now = datetime.now(UTC).isoformat()
        await db.runs.create(
            self._conn,
            id=new_run_id,
            issue_id=issue_id,
            stage="review",
            status="running",
            pid=None,
            started_at=now,
        )
        state = await db.review_state.get(self._conn, issue_id)
        if state.pr_number is not None:
            try:
                await self._gh.pr_comment(
                    state.pr_number, "@codex review", repo=binding.github_repo
                )
            except GitHubError as e:
                log.warning(
                    "could not re-post @codex review for %s#%d on retry: %s",
                    binding.github_repo,
                    state.pr_number,
                    e,
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
            await self.linear.post_comment(issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("retry comment failed for %s: %s", issue_id, e)

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
                issue = await self.linear.lookup_issue(issue_id)
            except LinearError as e:
                log.warning("could not look up %s for merge reject: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not look up issue for merge reject: {e}",
                ) from e
            if blocked_id is not None:
                try:
                    await self.linear.move_issue(issue_id, blocked_id)
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
            issue = await self.linear.lookup_issue(issue_id)
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
                await self.linear.post_comment(
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

    async def _handle_acceptance_rejected_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._acceptance_rejected_run_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(db.operator_waits.KIND_ACCEPTANCE_REJECTED,),
            )
            if binding is None:
                return

        if intent.kind not in (
            SlashKind.SKIP_ACCEPTANCE,
            SlashKind.RETRY_ACCEPTANCE,
        ):
            log.info(
                "slash %s for acceptance-rejected run %s ignored",
                intent.kind,
                run_id,
            )
            return

        state = await db.acceptance_state.get(self._conn, issue_id)
        if state.pr_number is None:
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                "no PR found for acceptance state",
            )
            return

        try:
            issue = await self.linear.lookup_issue(issue_id)
        except LinearError as e:
            log.warning("could not look up %s for acceptance slash: %s", issue_id, e)
            raise SlashHandlerFailure(
                slash_text=self._slash_text(intent),
                reason=f"could not look up issue for acceptance slash: {e}",
            ) from e

        pr_number = state.pr_number
        pr_url = _pr_url_for_state(
            repo=binding.github_repo,
            pr_number=pr_number,
            pr_url=state.pr_url,
        )

        if intent.kind is SlashKind.SKIP_ACCEPTANCE:
            await db.acceptance_state.record_verdict(
                self._conn,
                issue_id,
                verdict="pass",
                artifacts_url=state.last_artifacts_url,
            )
            await self._clear_operator_wait(issue_id, run_id)
            if _needs_human_approval_label_present(issue):
                await self._open_merge_wait_for_human_approval_label(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                )
            else:
                self._schedule_merge(
                    binding=binding,
                    issue=issue,
                    pr_number=pr_number,
                    pr_url=pr_url,
                )
            body = skip_acceptance_forced(
                CommentVars(
                    stage="acceptance",
                    repo=binding.github_repo,
                    issue=pr_number,
                    pr_url=pr_url,
                    run_id=run_id,
                    next_stage="merge",
                )
            )
            try:
                await self.linear.post_comment(issue_id, truncate_body(body))
            except LinearError as e:
                log.warning("skip-acceptance comment failed for %s: %s", issue_id, e)
            return

        await self._clear_operator_wait(issue_id, run_id)
        await db.acceptance_state.reset(self._conn, issue_id)
        self._schedule_acceptance(
            binding=binding,
            issue=issue,
            pr_number=pr_number,
            pr_url=pr_url,
            pr_head_sha=state.pr_head_sha,
        )
        body = retry_acceptance_requested(
            CommentVars(
                stage="acceptance",
                repo=binding.github_repo,
                issue=pr_number,
                pr_url=pr_url,
                run_id=run_id,
                next_stage="acceptance",
            )
        )
        try:
            await self.linear.post_comment(issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("retry-acceptance comment failed for %s: %s", issue_id, e)

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
                await self.linear.post_comment(
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

        try:
            issue = await self.linear.lookup_issue(issue_id)
        except LinearError as e:
            log.warning("could not look up %s for skip-review: %s", issue_id, e)
            return

        state = await db.review_state.get(self._conn, issue_id)
        if state.pr_number is None:
            try:
                await self.linear.post_comment(
                    issue_id,
                    truncate_body(
                        command_rejected("$skip-review", "no PR found for this issue")
                    ),
                )
            except LinearError as e:
                log.warning("could not post skip-review rejection for %s: %s", issue_id, e)
            return

        binding = self._binding_for_review(issue, state)
        if binding is None:
            log.warning("no binding for skip-review on %s", issue.identifier)
            return

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
                    await self.linear.post_comment(
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

        # Mark the review run completed and cancel its asyncio task immediately so
        # it cannot dispatch any more fix runs mid-iteration.
        now = datetime.now(UTC).isoformat()
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
                self._conn, fix_run_id, "interrupted", ended_at=now
            )
            self._dispatch_run_ids.pop(issue_id, None)
            self._active_run_ids.discard(fix_run_id)

        # Dispatch merge directly, bypassing the review verdict check.
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
            await self.linear.post_comment(issue_id, truncate_body(skip_review_forced(v)))
        except LinearError as e:
            log.warning("could not post skip-review comment for %s: %s", issue.identifier, e)

    async def _resurrect_review_runs(self) -> list[asyncio.Task[None]]:
        """Restart review monitors that died but whose PRs are still open.

        Guarded by REVIEW_RESURRECT_COOLDOWN_SECS so a persistently failing
        review does not spin at full poll speed.
        """
        scheduled: list[asyncio.Task[None]] = []
        orphaned = await db.issue_prs.list_orphaned_review_prs(self._conn)
        for pr in orphaned:
            if pr.issue_id in self._scheduled_issue_ids:
                continue
            if await db.runs.has_active(self._conn, pr.issue_id):
                continue
            # If there is already an operator wait (manual $retry pending), skip.
            if pr.issue_id in self._dispatch_run_ids:
                continue
            binding = self._binding_for_pr(pr)
            if binding is None:
                continue
            # Cooldown: skip if the last review run ended recently.
            last_review = await db.runs.latest_for_issue_stage(
                self._conn, issue_id=pr.issue_id, stage="review"
            )
            if last_review is not None and last_review.ended_at is not None:
                try:
                    elapsed = (
                        datetime.now(UTC) - _parse_rfc3339(last_review.ended_at)
                    ).total_seconds()
                    if elapsed < REVIEW_RESURRECT_COOLDOWN_SECS:
                        continue
                except ValueError:
                    pass
            try:
                issue = await self.linear.lookup_issue(pr.issue_id)
            except LinearError as e:
                log.warning(
                    "could not look up orphaned review issue %s: %s",
                    pr.identifier,
                    e,
                )
                continue
            if not _review_issue_is_active(issue, binding):
                continue
            log.info(
                "resurrecting dead review monitor for %s (PR #%d)",
                issue.identifier,
                pr.pr_number,
            )
            now = datetime.now(UTC).isoformat()
            review_run_id = str(uuid.uuid4())
            await db.runs.create(
                self._conn,
                id=review_run_id,
                issue_id=issue.id,
                stage="review",
                status="running",
                pid=None,
                started_at=now,
            )
            state = await db.review_state.get(self._conn, issue.id)
            cost = await db.runs.cost_for_issue(self._conn, issue.id)
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
                    cost=f"${cost:.4f}",
                    next_stage="review",
                )
            )
            try:
                await self.linear.post_comment(issue.id, truncate_body(body))
            except LinearError as e:
                log.warning(
                    "resurrection comment failed for %s: %s", issue.identifier, e
                )
            run = db.runs.Run(
                id=review_run_id,
                issue_id=issue.id,
                stage="review",
                status="running",
                pid=None,
                started_at=now,
                ended_at=None,
                cost_usd=0.0,
            )
            await self._move_issue_to_review_state(binding=binding, issue=issue)
            scheduled.append(self._schedule_review_poll(run, binding, issue))
            rearm_done = await self._retrigger_codex_review_unless_approved(
                binding=binding,
                issue=issue,
                state=state,
                require_no_signal=True,
            )
            if not rearm_done:
                await self._mark_review_rearm_retry(review_run_id)
        return scheduled

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
            ended_at=datetime.now(UTC).isoformat(),
        )
        await self._clear_review_rearm_retry(run.id)
        self._clear_review_no_signal_rearm_heads(run.id)
        if operator_wait:
            await self._track_review_failed_wait(issue.id, run.id, binding)
            try:
                states = await self._states_for_binding(binding)
                needs_approval_id = states.get(binding.linear_states.needs_approval)
                if needs_approval_id is not None:
                    await self.linear.move_issue(issue.id, needs_approval_id)
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
        cost = await db.runs.cost_for_issue(self._conn, issue.id)
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
                cost=f"${cost:.4f}",
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
            await self.linear.post_comment(issue.id, truncate_body(body))
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
            ended_at=datetime.now(UTC).isoformat(),
        )
        await self._clear_review_rearm_retry(run.id)
        self._clear_review_no_signal_rearm_heads(run.id)
        repo = state.github_repo or "(unknown repo)"
        cost = await db.runs.cost_for_issue(self._conn, issue.id)
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
                cost=f"${cost:.4f}",
                error=error,
                last_log="",
                auto_retry=True,
            )
        )
        try:
            await self.linear.post_comment(issue.id, truncate_body(body))
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
        cost = await db.runs.cost_for_issue(self._conn, issue.id)
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
                cost=f"${cost:.4f}",
                review_iter=state.iteration,
                trigger=trigger,
            )
        )
        try:
            await self.linear.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("stuck-loop comment failed on %s: %s", issue.identifier, e)
        try:
            states = await self._states_for_binding(binding)
            needs_approval_id = states.get(binding.linear_states.needs_approval)
            if needs_approval_id is not None:
                await self.linear.move_issue(issue.id, needs_approval_id)
        except LinearError as e:
            log.warning("could not park %s for approval: %s", issue.identifier, e)
        await db.runs.update_status(
            self._conn,
            run.id,
            "completed",
            ended_at=datetime.now(UTC).isoformat(),
        )
        await self._clear_review_rearm_retry(run.id)

    async def _scan_binding(
        self, binding: RepoBinding
    ) -> list[asyncio.Task[None]]:
        scheduled: list[asyncio.Task[None]] = []
        ready_state = binding.linear_states.ready
        waiting_state = binding.linear_states.waiting
        waiting_issues: list[LinearIssue] = []
        try:
            if waiting_state is None:
                issues = await self.linear.issues_in_state(
                    binding.linear_team_key, ready_state, binding.issue_label
                )
            else:
                ready_result, waiting_result = await asyncio.gather(
                    self.linear.issues_in_state(
                        binding.linear_team_key, ready_state, binding.issue_label
                    ),
                    self.linear.issues_in_state(
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
        await self._auto_unblock_waiting(binding, waiting_issues)
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

    async def _auto_unblock_waiting(
        self, binding: RepoBinding, waiting_issues: list[LinearIssue]
    ) -> None:
        unblocked_issues = [issue for issue in waiting_issues if not is_blocked(issue)]
        if not unblocked_issues:
            return
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states before auto-unblocking waiting issues for %s: %s",
                binding.linear_team_key,
                e,
            )
            return
        ready_id = states.get(binding.linear_states.ready)
        if ready_id is None:
            log.warning(
                "could not auto-unblock waiting issues for %s: missing Linear state %r",
                binding.linear_team_key,
                binding.linear_states.ready,
            )
            return

        for issue in unblocked_issues:
            try:
                await self.linear.move_issue(issue.id, ready_id)
            except LinearError as e:
                log.warning("could not auto-unblock %s to Ready: %s", issue.identifier, e)
                continue
            log.info("auto-unblocked %s -> Ready", issue.identifier)

    def _dispatch_capacity(self, binding: RepoBinding) -> int:
        if self.config.global_max_concurrent <= 0 or binding.max_concurrent <= 0:
            return 0
        binding_key = _binding_key(binding)
        return max(
            0,
            min(
                self.config.global_max_concurrent - self._scheduled_slot_count(),
                binding.max_concurrent
                - self._scheduled_binding_counts.get(binding_key, 0),
            ),
        )

    def _scheduled_slot_count(self) -> int:
        return sum(self._scheduled_issue_refcounts.values())

    def _reserve_scheduled_slot(
        self, *, issue_id: str, binding_key: BindingKey
    ) -> None:
        self._scheduled_issue_refcounts[issue_id] = (
            self._scheduled_issue_refcounts.get(issue_id, 0) + 1
        )
        self._scheduled_issue_ids.add(issue_id)
        self._scheduled_binding_counts[binding_key] = (
            self._scheduled_binding_counts.get(binding_key, 0) + 1
        )

    def _release_scheduled_slot(
        self, *, issue_id: str, binding_key: BindingKey
    ) -> None:
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

    async def _schedule_ready_issue(
        self, binding: RepoBinding, issue: LinearIssue
    ) -> asyncio.Task[None] | None:
        async with self._schedule_lock:
            if self._dispatch_capacity(binding) <= 0:
                return None
            if issue.id in self._scheduled_issue_ids:
                return None
            if issue.id in self._dispatch_run_ids:
                return None
            if await db.runs.has_running_or_completed(self._conn, issue.id):
                return None
            pr = await db.issue_prs.get(
                self._conn,
                issue_id=issue.id,
                github_repo=binding.github_repo,
            )
            if pr is not None:
                blocking_pr, handled = await self._blocking_existing_pr(
                    binding, issue, pr
                )
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
                    "could not delete closed unmerged PR row before ready dispatch "
                    "for %s#%d",
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
                try:
                    await self.linear.move_issue(issue.id, target_id)
                except LinearError as e:
                    log.warning(
                        "could not move %s after existing PR guard for PR #%d: %s",
                        issue.identifier,
                        pr.pr_number,
                        e,
                    )
                    return

        try:
            await self.linear.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "could not comment after existing PR guard for %s PR #%d: %s",
                issue.identifier,
                pr.pr_number,
                e,
            )

    async def _park_blocked_by_deps(
        self, binding: RepoBinding, issue: LinearIssue
    ) -> None:
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

        try:
            await self.linear.move_issue(issue.id, waiting_id)
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
            await self.linear.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "could not comment after moving %s to waiting for blockers %s: %s",
                issue.identifier,
                blockers,
                e,
            )
            if ready_id is not None:
                try:
                    await self.linear.move_issue(issue.id, ready_id)
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

    def _ready_binding_for_issue(self, issue: LinearIssue) -> RepoBinding | None:
        issue_labels = set(issue.labels)
        for binding in self.config.repos:
            if binding.linear_team_key != issue.team_key:
                continue
            if issue.state_name != binding.linear_states.ready:
                continue
            if binding.issue_label and binding.issue_label not in issue_labels:
                continue
            return binding
        return None

    def _schedule_dispatch(
        self, binding: RepoBinding, issue: LinearIssue
    ) -> asyncio.Task[None]:
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

    async def _dispatch_with_limits(
        self, binding: RepoBinding, issue: LinearIssue
    ) -> None:
        key = _binding_key(binding)
        binding_sem = self._binding_dispatch_sems.setdefault(
            key,
            asyncio.Semaphore(max(binding.max_concurrent, 1)),
        )
        try:
            async with self._global_dispatch_sem:
                async with binding_sem:
                    current = await self._refresh_dispatch_candidate(binding, issue)
                    if current is None:
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
        try:
            current = await self.linear.lookup_issue(issue.id)
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
            issue = await self.linear.lookup_issue(pr.issue_id)
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
                try:
                    await self.linear.move_issue(issue.id, done_id)
                except LinearError as e:
                    log.warning(
                        "could not move %s to done after parked PR closed: %s",
                        issue.identifier,
                        e,
                    )
                    return False

            comment_key = (issue.id, binding.github_repo, pr.pr_number)
            if comment_key not in self._parked_closed_unmerged_comment_keys:
                try:
                    await self.linear.post_comment(
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
            try:
                issue = await self.linear.lookup_issue(pr.issue_id)
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
                await self.linear.move_issue(issue.id, done_id)
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
                await self.linear.post_comment(issue.id, truncate_body(body))
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
        self, issue: LinearIssue
    ) -> LinearIssue:
        try:
            return await self.linear.lookup_issue(issue.id)
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

        try:
            await self.linear.move_issue(issue.id, needs_approval_id)
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
            await self.linear.post_comment(issue.id, body)
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
            try:
                issue = await self.linear.lookup_issue(candidate.issue_id)
            except LinearError as e:
                log.warning(
                    "could not refresh %s before merge: %s",
                    candidate.identifier,
                    e,
                )
                continue
            if not _merge_issue_matches_binding(issue, binding):
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
            conflict_fix_ready = False
            if (
                verdict.kind is VerdictKind.PENDING
                and verdict.rule == "no_signal"
                and str(view.get("mergeable") or "").upper() == "MERGEABLE"
            ):
                conflict_fix_ready = await db.issue_prs.has_merge_conflict_fixed(
                    self._conn,
                    issue_id=candidate.issue_id,
                    github_repo=binding.github_repo,
                    pr_number=candidate.pr_number,
                    pr_created_at=candidate.created_at,
                    head_sha=head_sha,
                )

            if verdict.kind is VerdictKind.APPROVED or conflict_fix_ready:
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
        backoff_secs = min(
            ACCEPTANCE_INFRA_RETRY_BASE_BACKOFF_SECS * (2 ** (retry_count - 1)),
            ACCEPTANCE_INFRA_RETRY_MAX_BACKOFF_SECS,
        )
        return self._now() < ended_at + timedelta(seconds=backoff_secs)

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
        issue: LinearIssue,
        pr_url: str,
        verdict: AcceptanceVerdict,
    ) -> str:
        try:
            body = format_acceptance_verdict_comment(
                verdict=verdict,
                pr_url=pr_url,
            )
            comment_id = await self.linear.post_comment(issue.id, truncate_body(body))
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
        issue: LinearIssue,
        criteria: list[ExtractedCriterion],
    ) -> None:
        try:
            body = format_acceptance_criteria_comment(criteria)
            await self.linear.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "acceptance criteria comment failed on %s: %s",
                issue.identifier,
                e,
            )

    async def _upload_acceptance_screenshots(
        self,
        *,
        issue: LinearIssue,
        workspace_path: Path,
        verdict: AcceptanceVerdict,
    ) -> AcceptanceVerdict:
        if verdict.kind not in {"pass", "reject"} or not verdict.screenshots:
            return verdict

        uploaded_by_path: dict[str, str] = {}
        uploaded_screenshots: list[AcceptanceScreenshot] = []
        for screenshot in verdict.screenshots:
            try:
                path = _acceptance_artifact_path(workspace_path, screenshot.path)
                url = await self.linear.upload_issue_attachment(
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
            started_at=datetime.now(UTC).isoformat(),
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
                issue=issue,
                criteria=extracted_criteria,
            )
            await self._move_issue_to_acceptance_state(binding=binding, issue=issue)

            prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
            cap_usd = effective_cap(
                global_cap_usd=self.config.cost_cap_per_issue_usd,
                binding_override=binding.cost_cap_usd,
            )
            warning_pct = effective_warning_pct(
                global_pct=self.config.cost_warning_pct,
                binding_override=binding.cost_warning_pct,
            )
            warning_already_fired = (
                await db.cost_marks.warning_posted_at(self._conn, issue.id) is not None
            )

            max_budget_usd: float | None = None
            budget_limits: list[float] = []
            if cap_usd > 0:
                issue_remaining_budget_usd = cap_usd - prior_total
                if issue_remaining_budget_usd <= 0:
                    await self._handle_cap_breach(
                        binding=binding,
                        issue=issue,
                        run_id=run_id,
                        cumulative_total=prior_total,
                        stage="acceptance",
                    )
                    return run_id
                budget_limits.append(issue_remaining_budget_usd)
            if binding.acceptance.cost_cap_usd > 0:
                budget_limits.append(binding.acceptance.cost_cap_usd)
            if budget_limits:
                max_budget_usd = min(budget_limits)

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
                                max_budget_usd=max_budget_usd,
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
                                    issue=issue,
                                    workspace_path=workspace_path,
                                    verdict=verdict,
                                )
                        finally:
                            self._workspace.release(binding, issue)

            verdict = _with_acceptance_degrade_note(verdict, degrade_note)

            cap_breached = False
            if verdict.cost > 0:
                await db.runs.add_cost(self._conn, run_id, verdict.cost)
                new_total = prior_total + verdict.cost
                decision = evaluate_cost(
                    previous_total=prior_total,
                    new_total=new_total,
                    cap_usd=cap_usd,
                    warning_pct=warning_pct,
                    warning_already_fired=warning_already_fired,
                )
                if decision.fire_warning:
                    await self._post_cost_warning(
                        binding=binding,
                        issue=issue,
                        run_id=run_id,
                        stage="acceptance",
                        cumulative_total=new_total,
                        cap_usd=cap_usd,
                    )
                cap_breached = decision.cap_breached
            comment_url = await self._post_acceptance_verdict_comment(
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

            if cap_breached:
                await self._handle_cap_breach(
                    binding=binding,
                    issue=issue,
                    run_id=run_id,
                    cumulative_total=prior_total + verdict.cost,
                    stage="acceptance",
                )
                return run_id

            ended_at = datetime.now(UTC).isoformat()
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
                    issue
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
                )
                return run_id

            state = await db.acceptance_state.get(self._conn, issue.id)
            await db.runs.update_status(
                self._conn,
                run_id,
                "failed",
                ended_at=ended_at,
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
            try:
                await self.linear.post_comment(issue.id, truncate_body(body))
            except LinearError as e:
                log.warning(
                    "acceptance rejected wait comment failed on %s: %s",
                    issue.identifier,
                    e,
                )
            return run_id
        except Exception:
            log.exception("acceptance stage failed for %s", issue.identifier)
            await db.runs.update_status(
                self._conn,
                run_id,
                "failed",
                ended_at=datetime.now(UTC).isoformat(),
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
                started_at=datetime.now(UTC).isoformat(),
            )
            self._dispatch_run_ids[issue.id] = fix_run_id

            try:
                prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
                cost, final_kind, final_returncode = await self._run_acceptance_fix_agent(
                    binding=binding,
                    issue=issue,
                    run_id=fix_run_id,
                    workspace_path=workspace_path,
                    prompt=prompt,
                    prior_total=prior_total,
                )
            except Exception:  # noqa: BLE001
                log.exception("acceptance fix-run execution failed for %s", issue.identifier)
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=datetime.now(UTC).isoformat(),
                )
                return False
            finally:
                if self._dispatch_run_ids.get(issue.id) == fix_run_id:
                    self._dispatch_run_ids.pop(issue.id, None)

            if cost > 0:
                await db.runs.add_cost(self._conn, fix_run_id, cost)

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
                    ended_at=datetime.now(UTC).isoformat(),
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
                    ended_at=datetime.now(UTC).isoformat(),
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
                    ended_at=datetime.now(UTC).isoformat(),
                )
                return False

            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "completed",
                ended_at=datetime.now(UTC).isoformat(),
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
        try:
            current = await self.linear.lookup_issue(issue.id)
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

    # Substring that identifies a Codex "no issues found" issue comment.
    # Codex posts: "Didn't find any major issues. Delightful!"
    _CODEX_NO_ISSUES_MARKER = CODEX_NO_ISSUES_MARKER

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
        try:
            await self.linear.post_comment(issue.id, codex_lgtm(v))
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

        ci = [_review_check_from_github(run) for run in checks.runs]
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
                    started_at=datetime.now(UTC).isoformat(),
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
        now = datetime.now(UTC).isoformat()
        self._dispatch_run_ids[issue.id] = run_id

        await db.issues.upsert(
            self._conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        inserted = await db.runs.create_if_not_dispatched(
            self._conn,
            id=run_id,
            issue_id=issue.id,
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
        starting = run_started(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
            )
        )
        try:
            await self.linear.post_comment(issue.id, truncate_body(starting))
        except LinearError as e:
            log.warning("could not announce dispatch on %s: %s", issue.identifier, e)
            await db.runs.update_status(
                self._conn,
                run_id,
                "failed",
                ended_at=datetime.now(UTC).isoformat(),
            )
            return run_id

        # 2. Move the Linear issue to In Progress.
        try:
            await self.linear.move_issue(issue.id, in_progress_id)
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
                str(e),
                issue=issue,
                rollback_state_id=issue.state_id,
                binding=binding,
            )
            return run_id

        prior_total = await db.runs.cost_for_issue(self._conn, issue.id)

        try:
            cumulative_cost, final_kind, final_returncode, cap_breached = (
                await self._run_agent(
                    binding=binding,
                    issue=issue,
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
                rollback_state_id=issue.state_id,
                binding=binding,
            )
            return run_id
        finally:
            self._workspace.release(binding, issue)

        # 4. Persist accumulated cost.
        if cumulative_cost > 0:
            await self._conn.execute(
                "UPDATE runs SET cost_usd = ? WHERE id = ?",
                (cumulative_cost, run_id),
            )
            await self._conn.commit()

        # Cost cap breach: park the issue for operator action; do not open a PR.
        if cap_breached:
            await self._handle_cap_breach(
                binding=binding,
                issue=issue,
                run_id=run_id,
                cumulative_total=prior_total + cumulative_cost,
            )
            return run_id

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
                rollback_state_id=issue.state_id,
                binding=binding,
            )
            return run_id

        # 4.5. Local-review pre-flight. When `binding.review_strategy` is
        # `local` or `hybrid`, run the reviewer in-workspace before pushing.
        # This shortens the iteration loop dramatically: the slow part of
        # the existing flow is round-tripping each fix through GitHub for
        # the remote `@codex` bot. See `docs/local-review-flow.md`.
        local_review_result: LoopResult | None = None
        if binding.review_strategy != "remote":
            local_review_result = await self._run_local_review_phase(
                binding=binding,
                issue=issue,
                workspace_path=workspace_path,
                parent_run_id=run_id,
            )

        # 5. Push branch, open PR, post stage-transition comment.
        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
        try:
            await self._push_fn(workspace_path, branch)
        except Exception as e:  # noqa: BLE001
            log.warning("git push failed for %s: %s", issue.identifier, e)
            await self._fail_run_and_reset_issue(
                run_id,
                f"push failed: {e}",
                issue=issue,
                rollback_state_id=issue.state_id,
                binding=binding,
            )
            return run_id

        pr_url: str = ""
        base_branch = binding.base_branch
        if base_branch is None:
            try:
                base_branch = await self._gh.repo_default_branch(binding.github_repo)
            except GitHubError as e:
                log.warning(
                    "repo_default_branch failed for %s; falling back to gh default: %s",
                    issue.identifier,
                    e,
                )
        try:
            pr_url = await self._gh.pr_create(
                title=build_pr_title(issue),
                body="",
                base=base_branch,
                head=branch,
                repo=binding.github_repo,
                linear_url=issue.url,
            )
        except GitHubError as e:
            log.warning("pr_create failed for %s: %s", issue.identifier, e)
            await self._fail_run_and_reset_issue(
                run_id,
                f"pr_create failed: {e}",
                issue=issue,
                rollback_state_id=issue.state_id,
                binding=binding,
            )
            return run_id

        try:
            done_body = stage_done(
                CommentVars(
                    stage="implement",
                    next_stage="review",
                    repo=binding.github_repo,
                    issue=0,
                    pr_url=pr_url or "(no PR)",
                    run_id=run_id,
                    cost=f"${cumulative_cost:.4f}",
                )
            )
            await self.linear.post_comment(issue.id, truncate_body(done_body))
        except LinearError as e:
            log.warning("stage_done comment failed on %s: %s", issue.identifier, e)

        await db.runs.update_status(
            self._conn,
            run_id,
            "completed",
            ended_at=datetime.now(UTC).isoformat(),
        )

        # 6. Start the Review stage. In `local` mode, when the in-workspace
        #    reviewer already APPROVED, suppress the `@codex review` ping —
        #    the local pass has done the work. Any other outcome (hybrid
        #    strategy, local-reviewer failed / exhausted / stuck, or no
        #    local pre-flight at all) still pings the remote bot as a
        #    safety net so a flaky reviewer can't dead-end the PR.
        post_codex_review = _should_post_codex_review(
            review_strategy=binding.review_strategy,
            local_review_result=local_review_result,
        )
        await self._start_review_stage(
            binding=binding,
            issue=issue,
            pr_url=pr_url,
            post_codex_review=post_codex_review,
        )
        # 7. Surface the local-review verdict on the GitHub PR thread
        #    (not just on Linear) so human reviewers see the audit
        #    trail. Only fires when local-review APPROVED — failures
        #    are already covered by the @codex fallback ping. The
        #    binding-level override wins over the global config so an
        #    operator can keep one repo's PR thread quiet without
        #    disabling the feature everywhere.
        if (
            binding.resolved_post_local_review_pr_summary(
                self.config.post_local_review_pr_summary
            )
            and local_review_result is not None
            and local_review_result.outcome == LoopOutcome.APPROVED
        ):
            await self._post_local_review_pr_summary(
                binding=binding,
                pr_url=pr_url,
                reviewer_agent=binding.resolved_reviewer_agent(),
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
        now = datetime.now(UTC).isoformat()
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
                    cumulative_cost,
                    final_kind,
                    final_returncode,
                    cap_breached,
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
                )
                return run_id

            if cumulative_cost > 0:
                await self._conn.execute(
                    "UPDATE runs SET cost_usd = ? WHERE id = ?",
                    (cumulative_cost, run_id),
                )
                await self._conn.commit()

            if cap_breached:
                await self._mark_merge_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=pr_url,
                    run_id=run_id,
                    reason=(
                        "cost cap reached: "
                        f"${prior_total + cumulative_cost:.4f}"
                    ),
                )
                return run_id

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
                )
                return run_id
            if not merged:
                await db.runs.update_status(
                    self._conn,
                    run_id,
                    "completed",
                    ended_at=datetime.now(UTC).isoformat(),
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

        total_cost = await db.runs.cost_for_issue(self._conn, issue.id)
        await self.linear.move_issue(issue.id, done_id)
        final_body = stage_done(
            CommentVars(
                stage="merge",
                next_stage="done",
                repo=binding.github_repo,
                issue=0,
                pr_url=pr_url,
                run_id=run_id,
                cost=f"${total_cost:.4f}",
            )
        )
        try:
            await self.linear.post_comment(issue.id, truncate_body(final_body))
        except LinearError as e:
            log.warning(
                "could not post final merge comment for %s: %s",
                issue.identifier,
                e,
            )
        ended_at = datetime.now(UTC).isoformat()
        await db.issue_prs.mark_merged(
            self._conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            merged_at=ended_at,
        )
        await db.runs.update_status(self._conn, run_id, "done", ended_at=ended_at)
        await self._clear_operator_wait(issue.id, run_id)
        try:
            await self._workspace.cleanup(issue)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "workspace cleanup failed after merge finalization for %s: %s",
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
    ) -> None:
        if create_run:
            inserted = await db.runs.create_if_no_active(
                self._conn,
                id=run_id,
                issue_id=issue.id,
                stage="merge",
                status="running",
                pid=None,
                started_at=datetime.now(UTC).isoformat(),
                ignored_stage="review",
            )
            if not inserted:
                return

        try:
            needs_approval_id: str | None = None
            try:
                states = await self._states_for_binding(binding)
                needs_approval_id = states.get(binding.linear_states.needs_approval)
            except LinearError as e:
                log.warning(
                    "could not load states while parking %s in needs approval: %s",
                    issue.identifier,
                    e,
                )

            total_cost = await db.runs.cost_for_issue(self._conn, issue.id)
            body = awaiting_approval(
                CommentVars(
                    stage="merge",
                    next_stage="done",
                    repo=binding.github_repo,
                    issue=0,
                    pr_url=pr_url,
                    run_id=run_id,
                    cost=f"${total_cost:.4f}",
                    error=reason,
                )
            )
            if needs_approval_id is not None:
                try:
                    await self.linear.move_issue(issue.id, needs_approval_id)
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
                await self.linear.post_comment(issue.id, truncate_body(body))
            except LinearError as e:
                log.warning("needs approval comment failed on %s: %s", issue.identifier, e)
        finally:
            await db.runs.update_status(
                self._conn,
                run_id,
                "needs_approval",
                ended_at=datetime.now(UTC).isoformat(),
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
                    created_at=datetime.now(UTC).isoformat(),
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
        workspace_path: Path,
        parent_run_id: str,
    ) -> LoopResult | None:
        """Run the local-review session and surface its outcome on Linear.

        Returns the `LoopResult` so future iterations can use the verdict
        to gate downstream behavior. Today the result is logged and
        commented on Linear but does not change the flow — that ramp-up
        is intentional, so we can prove the loop converges on real diffs
        before letting it drive the PR.

        Errors here are caught and logged: a broken local-review path
        must never dead-end an issue. The remote `@codex` flow runs
        afterwards regardless.
        """
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

            reviewer_agent = binding.resolved_reviewer_agent()
            reviewer_codex_model = binding.resolved_reviewer_codex_model()
            last_message_dir = (
                self.config.log_root / "local_review" / parent_run_id
            )
            # Local-review uses its own cap (default 6) which is
            # typically lower than `review_iteration_cap` (default 12)
            # used by the remote `@codex` loop. The two loops have
            # different convergence shapes; conflating their caps was
            # forcing a compromise.
            cap = binding.resolved_local_review_iteration_cap(
                self.config.local_review_iteration_cap
            )

            await self._post_local_review_starting_comment(
                issue=issue,
                reviewer_agent=reviewer_agent,
                strategy=binding.review_strategy,
                cap=cap,
            )

            cost_cap_usd = effective_cap(
                global_cap_usd=self.config.cost_cap_per_issue_usd,
                binding_override=binding.cost_cap_usd,
            )
            # Read prior cost BEFORE creating the local_review row so
            # the local-review cost-cap check uses the implement cost
            # alone — the new row would otherwise contribute $0 and
            # noisy up the trace.
            prior_cost_usd = await db.runs.cost_for_issue(
                self._conn, issue.id
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
                issue_id=issue.id,
                stage="local_review",
                status="running",
                pid=None,
                started_at=datetime.now(UTC).isoformat(),
            )

            # Keep the implement run_id slash-eligible during the
            # local-review phase so `$skip-local-review` posted on the
            # Linear issue is picked up by `_poll_slash_commands`. The
            # `_active_run_ids` set was discarded when the implement
            # subprocess exited; we re-add it for the duration of the
            # local-review phase and clean up in `finally`.
            self._local_review_skip_flags[issue.id] = False
            self._active_run_ids.add(parent_run_id)

            def _should_skip() -> bool:
                return bool(self._local_review_skip_flags.get(issue.id))

            async def _on_iteration(
                i: int, verdict: LocalVerdict, cost_so_far: float
            ) -> None:
                await self._post_local_review_iteration_comment(
                    issue=issue,
                    iteration=i,
                    verdict=verdict,
                    cost_so_far=cost_so_far,
                )

            async def _report_active(run_id: str | None) -> None:
                if run_id is None:
                    self._local_review_active_run_ids.pop(issue.id, None)
                else:
                    self._local_review_active_run_ids[issue.id] = run_id

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
                    cap=cap,
                    stall_secs=self.config.stall_timeout_secs,
                    command_secs=self.config.command_timeout_secs,
                    last_message_dir=last_message_dir,
                    head_sha_provider=_workspace_head_sha,
                    cost_cap_usd=cost_cap_usd,
                    prior_cost_usd=prior_cost_usd,
                    should_skip=_should_skip,
                    on_iteration=_on_iteration,
                    report_active_run_id=_report_active,
                )
            finally:
                self._active_run_ids.discard(parent_run_id)
                self._local_review_skip_flags.pop(issue.id, None)
                self._local_review_active_run_ids.pop(issue.id, None)
                await self._finalize_local_review_run(
                    run_id=local_review_run_id,
                    result=result,
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
            await self._post_local_review_comment(issue=issue, result=result)
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
        self, *, run_id: str, result: LoopResult | None
    ) -> None:
        """Close the local-review `runs` row started by the phase.

        Always called from the phase's `finally`, even when the session
        raised. `result=None` means the session never returned a
        `LoopResult` (uncaught exception inside the session); mark the
        row failed with zero cost so the row reflects the abort.
        """
        cost = result.total_cost_usd if result is not None else 0.0
        if cost > 0:
            try:
                await db.runs.add_cost(self._conn, run_id, cost)
            except Exception:  # noqa: BLE001
                log.warning(
                    "could not persist local-review cost for run %s",
                    run_id,
                )
        status = _local_review_status_from_result(result)
        try:
            await db.runs.update_status(
                self._conn,
                run_id,
                status,
                ended_at=datetime.now(UTC).isoformat(),
            )
        except Exception:  # noqa: BLE001
            log.warning(
                "could not finalize local-review run %s (status=%s)",
                run_id,
                status,
            )

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
        try:
            await self.linear.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "local-review starting comment failed on %s: %s",
                issue.identifier,
                e,
            )

    async def _post_local_review_iteration_comment(
        self,
        *,
        issue: LinearIssue,
        iteration: int,
        verdict: LocalVerdict,
        cost_so_far: float,
    ) -> None:
        """Per-iteration heartbeat so a 5-minute review doesn't look dead.

        Posted right after the verdict is parsed (before any fix-run
        dispatch), so operators can see "iteration 1: changes_requested
        — first finding…" while the fix-run is still running. Short
        snippets keep the issue thread readable.
        """
        snippet = ""
        if verdict.findings:
            snippet = verdict.findings.strip().splitlines()[0][:280]
        body_parts = [
            f"**Local review iter {iteration}:** "
            f"`{verdict.kind.value}` (cost so far ${cost_so_far:.4f})",
        ]
        if snippet:
            body_parts.append(f"> {snippet}")
        body = "\n\n".join(body_parts)
        try:
            await self.linear.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "local-review iter comment failed on %s: %s",
                issue.identifier,
                e,
            )

    async def _post_local_review_comment(
        self, *, issue: LinearIssue, result: LoopResult
    ) -> None:
        outcome = result.outcome.value
        last_findings = ""
        if result.last_verdict is not None and result.last_verdict.findings:
            last_findings = result.last_verdict.findings
        body_parts = [
            f"**Local-review outcome:** `{outcome}` "
            f"(iterations={result.iterations}, "
            f"cost=${result.total_cost_usd:.4f})",
        ]
        if result.error:
            body_parts.append(f"_Error:_ {result.error}")
        if last_findings:
            body_parts.append("Last findings:\n\n" + last_findings)
        body = "\n\n".join(body_parts)
        try:
            await self.linear.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "local-review comment post failed on %s: %s", issue.identifier, e
            )

    async def _start_review_stage(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
        post_codex_review: bool = True,
    ) -> str | None:
        """Persist the review state row, optionally ping `@codex review`.

        `post_codex_review=False` is the local-review-mode entry point: the
        in-workspace reviewer already produced an `APPROVED` verdict, so
        sending the remote bot another pass would just burn cost and
        latency. State tracking, PR persistence, and the Linear state
        move still happen — the review monitor still polls CI and human
        approvals; only the bot ping is suppressed.

        Idempotent in spirit: failure to post the bot ping does not block
        the run row from being created, but is logged loudly so an
        operator can re-ping with a slash command if needed.
        """
        pr_number = pr_number_from_url(pr_url)
        await db.review_state.begin_review(
            self._conn,
            issue.id,
            pr_number=pr_number,
            pr_url=pr_url,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label,
        )
        if pr_number is None:
            log.warning(
                "could not parse PR number from %r for %s — skipping @codex review",
                pr_url,
                issue.identifier,
            )
        else:
            await db.issue_prs.upsert(
                self._conn,
                issue_id=issue.id,
                github_repo=binding.github_repo,
                binding_key=_binding_storage_key(binding),
                pr_number=pr_number,
                pr_url=pr_url,
                created_at=datetime.now(UTC).isoformat(),
            )
            if post_codex_review:
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

        await self._move_issue_to_review_state(binding=binding, issue=issue)

        review_run_id = str(uuid.uuid4())
        await db.runs.create(
            self._conn,
            id=review_run_id,
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at=datetime.now(UTC).isoformat(),
        )
        return review_run_id

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
            await self.linear.move_issue(issue.id, review_state_id)
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
            await self.linear.move_issue(issue.id, acceptance_state_id)
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
        cumulative_total: float,
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
                cumulative_total=cumulative_total,
            )

    async def _record_activity_tick(
        self,
        *,
        session: ActivitySession | None,
        binding: RepoBinding,
        issue: LinearIssue,
        cumulative_total: float,
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
            cumulative_total=cumulative_total,
            heartbeat_item_ids=due_item_ids,
        )

    async def _flush_activity(
        self,
        *,
        session: ActivitySession | None,
        binding: RepoBinding,
        issue: LinearIssue,
        cumulative_total: float,
    ) -> None:
        if session is None or not session.has_unpublished_events():
            return
        await self._publish_activity_digest(
            session=session,
            binding=binding,
            issue=issue,
            reason="final",
            now=self._now(),
            cumulative_total=cumulative_total,
        )

    async def _publish_activity_digest(
        self,
        *,
        session: ActivitySession,
        binding: RepoBinding,
        issue: LinearIssue,
        reason: ActivityPublishReason,
        now: datetime,
        cumulative_total: float,
        heartbeat_item_ids: tuple[str, ...] = (),
    ) -> bool:
        digest = session.build_digest(
            reason=reason,
            now=now,
            cumulative_cost_usd=cumulative_total,
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
        try:
            await self.linear.post_comment(issue.id, body)
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
        run_id: str,
        workspace_path: Path,
        prior_total: float,
    ) -> tuple[float, str, int | None, bool]:
        """Spawn the runner and consume events. Returns
        (cumulative_cost, final_event_kind, final_returncode, cap_breached).

        After every cost-emitting event the cumulative *issue* total
        (prior runs + this run so far) is checked against the cap and
        warning threshold. The once-per-issue cost-warning comment is
        posted the first time the threshold is crossed; on cap breach the
        runner is killed and the loop exits with `cap_breached=True` so
        the caller can park the issue at `needs_approval`.
        """
        cap_usd = effective_cap(
            global_cap_usd=self.config.cost_cap_per_issue_usd,
            binding_override=binding.cost_cap_usd,
        )
        warning_pct = effective_warning_pct(
            global_pct=self.config.cost_warning_pct,
            binding_override=binding.cost_warning_pct,
        )
        warning_already_fired = (
            await db.cost_marks.warning_posted_at(self._conn, issue.id) is not None
        )

        max_budget_usd: float | None = None
        if cap_usd > 0:
            max_budget_usd = cap_usd - prior_total
            if max_budget_usd <= 0:
                return 0.0, "cost_cap", None, True

        prompt = implement_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
        )
        command = build_runner_command(
            binding.agent,
            prompt,
            max_budget_usd=max_budget_usd,
            codex_model=binding.codex_model,
            workspace_path=workspace_path,
        )
        return await self._run_stage_command(
            binding=binding,
            issue=issue,
            command=command,
            run_id=run_id,
            workspace_path=workspace_path,
            stage="implement",
            prior_total=prior_total,
            cap_usd=cap_usd,
            warning_pct=warning_pct,
            warning_already_fired=warning_already_fired,
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
    ) -> tuple[float, str, int | None, bool]:
        cap_usd = effective_cap(
            global_cap_usd=self.config.cost_cap_per_issue_usd,
            binding_override=binding.cost_cap_usd,
        )
        warning_pct = effective_warning_pct(
            global_pct=self.config.cost_warning_pct,
            binding_override=binding.cost_warning_pct,
        )
        warning_already_fired = (
            await db.cost_marks.warning_posted_at(self._conn, issue.id) is not None
        )

        max_budget_usd: float | None = None
        if cap_usd > 0:
            max_budget_usd = cap_usd - prior_total
            if max_budget_usd <= 0:
                return 0.0, "cost_cap", None, True

        prompt = merge_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
            pr_url=pr_url,
        )
        command = build_merge_runner_command(
            binding.agent,
            prompt,
            max_budget_usd=max_budget_usd,
            codex_model=binding.codex_model,
            workspace_path=workspace_path,
        )
        return await self._run_stage_command(
            binding=binding,
            issue=issue,
            command=command,
            run_id=run_id,
            workspace_path=workspace_path,
            stage="merge",
            prior_total=prior_total,
            cap_usd=cap_usd,
            warning_pct=warning_pct,
            warning_already_fired=warning_already_fired,
        )

    async def _run_stage_command(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        command: list[str],
        run_id: str,
        workspace_path: Path,
        stage: str,
        prior_total: float,
        cap_usd: float,
        warning_pct: int,
        warning_already_fired: bool,
    ) -> tuple[float, str, int | None, bool]:
        spec = RunnerSpec(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stall_secs=self.config.stall_timeout_secs,
            command_secs=self.config.command_timeout_secs,
            stage=stage,
        )

        log_path = self.config.log_root / f"{run_id}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        cumulative_cost = 0.0
        final_kind = "exit"
        final_returncode: int | None = None
        cap_breached = False
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
                            cost_delta = cost_estimator.delta(usage)
                            previous_total = prior_total + cumulative_cost
                            cumulative_cost += cost_delta
                            new_total = prior_total + cumulative_cost
                            if cap_breached:
                                continue
                            decision = evaluate_cost(
                                previous_total=previous_total,
                                new_total=new_total,
                                cap_usd=cap_usd,
                                warning_pct=warning_pct,
                                warning_already_fired=warning_already_fired,
                            )
                            if decision.fire_warning:
                                warning_already_fired = await self._post_cost_warning(
                                    binding=binding,
                                    issue=issue,
                                    run_id=run_id,
                                    stage=stage,
                                    cumulative_total=new_total,
                                    cap_usd=cap_usd,
                                )
                            if decision.cap_breached:
                                cap_breached = True
                                await self._kill_active_runner(run_id)
                        await self._record_activity_stdout(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            line=ev.line,
                            cumulative_total=prior_total + cumulative_cost,
                        )
                    elif ev.kind == "stderr" and ev.line is not None:
                        logf.write(f"[stderr] {ev.line}\n")
                    elif ev.kind == "tick":
                        await self._record_activity_tick(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_total=prior_total + cumulative_cost,
                        )
                    elif ev.kind in ("exit", "stall_timeout", "spawn_failed"):
                        await self._flush_activity(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_total=prior_total + cumulative_cost,
                        )
                        final_kind = ev.kind
                        final_returncode = ev.returncode
                        break
        finally:
            self._active_run_ids.discard(run_id)
        return cumulative_cost, final_kind, final_returncode, cap_breached

    async def _run_fix_agent(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        run_id: str,
        workspace_path: Path,
        prompt: str,
        prior_total: float,
    ) -> tuple[float, str, int | None]:
        command = build_fix_runner_command(
            binding.agent,
            prompt,
            codex_model=binding.codex_model,
            workspace_path=workspace_path,
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
    ) -> tuple[float, str, int | None]:
        command = build_fix_runner_command(
            binding.agent,
            prompt,
            codex_model=binding.codex_model,
            workspace_path=workspace_path,
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
    ) -> tuple[float, str, int | None]:
        spec = RunnerSpec(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stall_secs=self.config.stall_timeout_secs,
            command_secs=self.config.command_timeout_secs,
            stage=stage,
        )

        log_path = self.config.log_root / f"{run_id}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        cumulative_cost = 0.0
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
                            cumulative_cost += cost_estimator.delta(usage)
                        await self._record_activity_stdout(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            line=ev.line,
                            cumulative_total=prior_total + cumulative_cost,
                        )
                    elif ev.kind == "stderr" and ev.line is not None:
                        logf.write(f"[stderr] {ev.line}\n")
                    elif ev.kind == "tick":
                        await self._record_activity_tick(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_total=prior_total + cumulative_cost,
                        )
                    elif ev.kind in ("exit", "stall_timeout", "spawn_failed"):
                        await self._flush_activity(
                            session=activity,
                            binding=binding,
                            issue=issue,
                            cumulative_total=prior_total + cumulative_cost,
                        )
                        final_kind = ev.kind
                        final_returncode = ev.returncode
                        break
        finally:
            self._active_run_ids.discard(run_id)
            if clear_pid_on_finish:
                await db.runs.update_pid(self._conn, run_id, None)
        return cumulative_cost, final_kind, final_returncode

    async def _post_cost_warning(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        run_id: str,
        stage: str,
        cumulative_total: float,
        cap_usd: float,
    ) -> bool:
        pct = int(round(cumulative_total / cap_usd * 100)) if cap_usd > 0 else 0
        body = cost_warning(
            CommentVars(
                stage=stage,
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
                cost=f"${cumulative_total:.4f}",
                pct=pct,
            )
        )
        try:
            await self.linear.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("cost_warning comment failed on %s: %s", issue.identifier, e)
            return False
        await db.cost_marks.mark_warning_posted(
            self._conn, issue.id, datetime.now(UTC).isoformat()
        )
        return True

    async def _handle_cap_breach(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        run_id: str,
        cumulative_total: float,
        stage: str = "implement",
    ) -> None:
        """Park a cost-capped issue and post a cost-cap escalation."""
        try:
            try:
                states = await self._states_for_binding(binding)
            except LinearError as e:
                log.warning(
                    "could not load states for %s after cap breach on %s: %s",
                    binding.linear_team_key,
                    issue.identifier,
                    e,
                )
                states = {}
            needs_approval_id = states.get(binding.linear_states.needs_approval)
            blocked_id = states.get(binding.linear_states.blocked)
            parked = False
            if needs_approval_id is not None:
                try:
                    await self.linear.move_issue(issue.id, needs_approval_id)
                except LinearError as e:
                    log.warning(
                        "could not move %s to needs_approval after cap breach: %s",
                        issue.identifier,
                        e,
                    )
                else:
                    parked = True
            else:
                log.warning(
                    "no needs_approval state for team %s; cannot park %s",
                    binding.linear_team_key,
                    issue.identifier,
                )
            if not parked and blocked_id is not None:
                try:
                    await self.linear.move_issue(issue.id, blocked_id)
                except LinearError as e:
                    log.warning(
                        "could not move %s to blocked after cap breach: %s",
                        issue.identifier,
                        e,
                    )
                else:
                    parked = True
            if not parked and blocked_id is None:
                log.warning(
                    "no blocked state for team %s; leaving %s out of the ready queue "
                    "after cap breach",
                    binding.linear_team_key,
                    issue.identifier,
                )
            body = cost_cap_reached(
                CommentVars(
                    stage=stage,
                    repo=binding.github_repo,
                    issue=0,
                    run_id=run_id,
                    cost=f"${cumulative_total:.4f}",
                    trigger="cost_cap",
                )
            )
            try:
                await self.linear.post_comment(issue.id, truncate_body(body))
            except LinearError as e:
                log.warning(
                    "cost_cap_reached comment failed on %s: %s", issue.identifier, e
                )
            await self._track_operator_wait(issue.id, run_id, binding)
        finally:
            await db.runs.update_status(
                self._conn,
                run_id,
                "failed",
                ended_at=datetime.now(UTC).isoformat(),
            )

    async def _fail_run(self, run_id: str, _reason: str) -> None:
        await db.runs.update_status(
            self._conn,
            run_id,
            "failed",
            ended_at=datetime.now(UTC).isoformat(),
        )

    async def _fail_run_and_reset_issue(
        self,
        run_id: str,
        reason: str,
        *,
        issue: LinearIssue,
        rollback_state_id: str,
        binding: RepoBinding | None = None,
    ) -> None:
        await self._fail_run(run_id, reason)
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
            await self.linear.move_issue(issue.id, target_state_id)
        except LinearError as e:
            log.warning(
                "could not park %s after failed dispatch: %s",
                issue.identifier,
                e,
            )
        if binding is None:
            return
        await self._track_implement_failed_wait(issue.id, run_id, binding)
        cost = await db.runs.cost_for_issue(self._conn, issue.id)
        body = failed(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
                cost=f"${cost:.4f}",
                error=reason,
            )
        )
        body += (
            "\nReply with `$retry` or `$approve` to requeue this issue. "
            "Reply with `$reject` or `$stop` to leave it halted.\n"
        )
        try:
            await self.linear.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("implement failed comment post failed on %s: %s", issue.identifier, e)


__all__ = [
    "Orchestrator",
    "WebhookDispatchResult",
    "_local_review_status_from_result",
    "_should_post_codex_review",
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
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    comment_id = data.get("id")
    body = data.get("body")
    created_at = data.get("createdAt") or payload.get("createdAt")
    if not isinstance(comment_id, str) or not comment_id:
        return None
    if not isinstance(body, str):
        return None
    if not isinstance(created_at, str) or not created_at:
        return None
    actor = payload.get("actor")
    author_name = ""
    author_is_me = False
    if isinstance(actor, Mapping):
        raw_name = actor.get("name")
        author_name = raw_name if isinstance(raw_name, str) else ""
    author_is_me = is_symphony_comment(body)
    external_thread_type: str | None = None
    ext = data.get("externalThread")
    if isinstance(ext, Mapping):
        raw_type = ext.get("type")
        external_thread_type = raw_type if isinstance(raw_type, str) else None
    return LinearComment(
        id=comment_id,
        body=body,
        created_at=created_at,
        author_name=author_name,
        author_is_me=author_is_me,
        external_thread_type=external_thread_type,
    )
