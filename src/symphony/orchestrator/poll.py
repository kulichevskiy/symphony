"""The always-running poll loop.

End-to-end scope (issue #7): scan each configured Linear team for issues
in the "ready" state with the configured label, then for each one:

1. Atomically insert a `runs` row (dedupe).
2. Post a ▶ "starting" Linear comment.
3. Move the issue to the binding's `in_progress` state.
4. Acquire a per-issue workspace clone.
5. Spawn the binding's runner with the Implement prompt; stream events
   into `{log_root}/{run_id}.log` and accumulate cost / tokens.
6. On clean exit: push the branch, open a PR titled
   `[<LINEAR_ID>] <issue title>` with body `Relates to <linear-url>`,
   post a stage-transition comment, and HALT at "In Progress" (Review +
   Merge land in their own slices).
7. On any non-clean exit: mark the run failed; do not open a PR.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

import aiosqlite

from .. import db
from ..agent.codex_models import DEFAULT_CODEX_MODEL
from ..agent.process import Usage, parse_event_line
from ..agent.prompt import implement_prompt, merge_prompt, review_fix_prompt
from ..agent.runner import Runner, RunnerSpec
from ..agent.runners.local import LocalRunner
from ..config import Config, RepoBinding
from ..github.client import CheckRun as GitHubCheckRun
from ..github.client import GitHub, GitHubError, PRChecks
from ..linear import slash
from ..linear.client import Linear, LinearComment, LinearError, LinearIssue
from ..linear.slash import SlashIntent, SlashKind
from ..linear.templates import (
    CommentVars,
    awaiting_approval,
    cost_cap_reached,
    cost_warning,
    failed,
    resumed,
    run_started,
    stage_done,
    stuck_loop_escape,
    truncate_body,
)
from ..pipeline.cost_guard import (
    effective_cap,
    effective_warning_pct,
    estimate_codex_cost_usd,
    evaluate_cost,
)
from ..pipeline.review_classifier import (
    CheckRun as ReviewCheckRun,
)
from ..pipeline.review_classifier import (
    Reaction,
    Review,
    ReviewComment,
    ReviewSnapshot,
    Verdict,
    VerdictKind,
    has_hit_iteration_cap,
    review_classifier,
    should_dispatch_fix_run,
)
from ..pipeline.state_machine import on_runner_event
from ..workspace import Workspace

log = logging.getLogger(__name__)

PushFn = Callable[[Path, str], Awaitable[None]]
BindingKey = tuple[str, str, str]
CI_FETCH_FAILURE_LIMIT = 5


@dataclass
class _UsageCostEstimator:
    agent: str
    codex_model: str
    last_estimated_input_tokens: int = 0
    last_estimated_cached_input_tokens: int = 0
    last_estimated_output_tokens: int = 0

    def delta(self, usage: Usage) -> float:
        if self.agent != "codex" or usage.cost_usd > 0:
            return usage.cost_usd
        input_delta = max(usage.input_tokens - self.last_estimated_input_tokens, 0)
        cached_input_delta = max(
            usage.cached_input_tokens - self.last_estimated_cached_input_tokens,
            0,
        )
        output_delta = max(usage.output_tokens - self.last_estimated_output_tokens, 0)
        self.last_estimated_input_tokens = max(
            self.last_estimated_input_tokens, usage.input_tokens
        )
        self.last_estimated_cached_input_tokens = max(
            self.last_estimated_cached_input_tokens,
            usage.cached_input_tokens,
        )
        self.last_estimated_output_tokens = max(
            self.last_estimated_output_tokens, usage.output_tokens
        )
        return estimate_codex_cost_usd(
            input_tokens=input_delta,
            cached_input_tokens=cached_input_delta,
            output_tokens=output_delta,
            model=self.codex_model,
        )


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


def build_runner_command(
    agent: str,
    prompt: str,
    *,
    max_budget_usd: float | None = None,
    codex_model: str = DEFAULT_CODEX_MODEL,
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
        return ["codex", "exec", "--json", "--model", codex_model, prompt]
    raise ValueError(f"unknown agent {agent!r}")


def build_fix_runner_command(
    agent: str,
    prompt: str,
    *,
    codex_model: str = DEFAULT_CODEX_MODEL,
) -> list[str]:
    """argv for a Review-stage fix-run.

    Fix-runs go through the binding's CLI (claude or codex), NOT through
    the GitHub `@codex review` bot. The bot is only consulted via PR
    comments; the binding's `agent` field is what drives code changes
    in response to its feedback.
    """
    return build_runner_command(agent, prompt, codex_model=codex_model)


def build_merge_runner_command(
    agent: str,
    prompt: str,
    *,
    max_budget_usd: float | None = None,
    codex_model: str = DEFAULT_CODEX_MODEL,
) -> list[str]:
    """argv for the Merge-stage final local pass."""
    return build_runner_command(
        agent,
        prompt,
        max_budget_usd=max_budget_usd,
        codex_model=codex_model,
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


def _review_issue_is_active(issue: LinearIssue, binding: RepoBinding) -> bool:
    return issue.state_name == binding.linear_states.in_progress


def _merge_issue_matches_binding(issue: LinearIssue, binding: RepoBinding) -> bool:
    return (
        issue.team_key == binding.linear_team_key
        and _review_issue_is_active(issue, binding)
        and (binding.issue_label is None or binding.issue_label in issue.labels)
    )


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


def _pr_view_is_merged(view: dict[str, object]) -> bool:
    return (
        bool(view.get("mergedAt"))
        or bool(view.get("merged"))
        or str(view.get("state") or "").upper() == "MERGED"
    )


def _pr_view_is_closed(view: dict[str, object]) -> bool:
    return str(view.get("state") or "").upper() == "CLOSED"


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
        # Cache of (team_key -> {state_name: state_uuid}). Re-fetched on
        # startup; never mutated at runtime.
        self._states: dict[str, dict[str, str]] = {}
        self._dispatch_tasks: set[asyncio.Task[None]] = set()
        self._scheduled_issue_ids: set[str] = set()
        self._scheduled_binding_counts: dict[BindingKey, int] = {}
        self._schedule_lock = asyncio.Lock()
        self._comment_event_lock = asyncio.Lock()
        self._active_run_ids: set[str] = set()
        self._dispatch_run_ids: dict[str, str] = {}
        self._operator_wait_run_ids: set[str] = set()
        self._cost_cap_run_bindings: dict[str, RepoBinding] = {}
        self._runs_moved_to_in_progress: set[str] = set()
        self._review_poll_tasks: set[asyncio.Task[None]] = set()
        self._review_poll_run_ids: set[str] = set()
        self._global_dispatch_sem = asyncio.Semaphore(
            max(config.global_max_concurrent, 1)
        )
        self._binding_dispatch_sems: dict[BindingKey, asyncio.Semaphore] = {}

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
            await self.drain_dispatch_tasks(cancel=True)

    async def _tick(self) -> list[asyncio.Task[None]]:
        scheduled: list[asyncio.Task[None]] = []
        await self._restore_operator_waits()
        try:
            scheduled.extend(await self._poll_review_runs())
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("review poll failed")
        for binding in self.config.repos:
            scheduled.extend(await self._scan_binding(binding))
        try:
            scheduled.extend(await self._poll_merge_candidates())
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("merge candidate poll failed")
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
        run_id = self._dispatch_run_ids.get(issue_id)
        if run_id is None or not self._slash_command_run_eligible(run_id):
            return WebhookDispatchResult(
                kind="comment", handled=False, detail="no active run"
            )
        handled = await self._handle_unseen_slash_comment(issue_id, run_id, comment)
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
        issue = await self.linear.lookup_issue(issue_id)
        binding = self._ready_binding_for_issue(issue)
        if binding is None:
            return WebhookDispatchResult(
                kind="issue", handled=False, detail="issue is not dispatchable"
            )
        task = await self._schedule_ready_issue(binding, issue)
        return WebhookDispatchResult(
            kind="issue",
            handled=task is not None,
            detail="" if task is not None else "issue is already scheduled or active",
        )

    def _slash_command_run_eligible(self, run_id: str) -> bool:
        return run_id in self._active_run_ids or run_id in self._operator_wait_run_ids

    async def _poll_slash_commands(self) -> None:
        """For each active run, fetch new comments and dispatch slash intents.

        The cursor (`(timestamp, ids_at_timestamp)`) lives in `comment_cursors`.
        We query with `gte` and drop any comment whose ID is in the cursor's
        boundary set, which both (a) avoids re-firing handled commands across
        restarts and (b) avoids losing comments tied at the boundary timestamp.
        """
        await self._restore_operator_waits()
        pairs = list(self._dispatch_run_ids.items())
        for issue_id, run_id in pairs:
            if not self._slash_command_run_eligible(run_id):
                continue
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
            for comment in comments:
                if comment.id in seen_ids:
                    continue
                await self._handle_unseen_slash_comment(issue_id, run_id, comment)

    async def _handle_unseen_slash_comment(
        self, issue_id: str, run_id: str, comment: LinearComment
    ) -> bool:
        async with self._comment_event_lock:
            if await db.comment_events.seen(self._conn, comment.id):
                return False
            await self._handle_slash_comments(issue_id, run_id, [comment])
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

    async def _handle_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        if run_id in self._cost_cap_run_bindings:
            await self._handle_cost_cap_slash_intent(issue_id, run_id, intent)
            return
        if intent.kind is SlashKind.STOP:
            log.info(
                "/stop received for run %s (issue %s) — terminating runner",
                run_id,
                issue_id,
            )
            try:
                await self._runner.kill(run_id)
            except Exception:  # noqa: BLE001
                log.exception("runner.kill failed for run %s", run_id)
                raise
            return
        log.info(
            "slash %s received for run %s (handler not implemented in this slice)",
            intent.kind,
            run_id,
        )

    async def _handle_cost_cap_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._cost_cap_run_bindings.get(run_id)
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
            await self.linear.move_issue(issue_id, ready_id)
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
                await self.linear.move_issue(issue_id, blocked_id)
            await self._clear_operator_wait(issue_id, run_id)
            return

        log.info(
            "slash %s received for cost-capped run %s (ignored)",
            intent.kind,
            run_id,
        )

    async def _restore_operator_waits(self) -> None:
        waits = await db.operator_waits.list_all(self._conn)
        for wait in waits:
            if wait.kind != db.operator_waits.KIND_COST_CAP:
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
            self._dispatch_run_ids[wait.issue_id] = wait.run_id
            self._operator_wait_run_ids.add(wait.run_id)
            self._cost_cap_run_bindings[wait.run_id] = binding

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

    async def _clear_operator_wait(self, issue_id: str, run_id: str) -> None:
        if self._dispatch_run_ids.get(issue_id) == run_id:
            self._dispatch_run_ids.pop(issue_id, None)
        self._operator_wait_run_ids.discard(run_id)
        self._cost_cap_run_bindings.pop(run_id, None)
        await db.operator_waits.delete(self._conn, issue_id, run_id)

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
        task = asyncio.create_task(self._poll_review_run_with_limits(run, binding, issue))
        self._review_poll_tasks.add(task)
        task.add_done_callback(partial(self._review_poll_done, run_id=run.id))
        return task

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
        key = _binding_key(binding)
        binding_sem = self._binding_dispatch_sems.setdefault(
            key,
            asyncio.Semaphore(max(binding.max_concurrent, 1)),
        )
        async with self._global_dispatch_sem:
            async with binding_sem:
                current = await self._refresh_review_poll_candidate(run, binding, issue)
                if current is None:
                    return
                current_binding, current_issue = current
                await self._poll_review_run(run, current_binding, current_issue)

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
        return current_binding, current

    def _review_poll_done(self, task: asyncio.Task[None], run_id: str) -> None:
        self._review_poll_tasks.discard(task)
        self._review_poll_run_ids.discard(run_id)
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

    async def _poll_review_run(
        self,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
    ) -> None:
        state = await db.review_state.get(self._conn, issue.id)
        if state.pr_number is None:
            await self._fail_review_run(
                run=run,
                binding=binding,
                issue=issue,
                error="review run has no PR number",
                last_log="",
            )
            return

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
            return

        await db.review_state.reset_ci_fetch_failures(self._conn, issue.id)

        try:
            head_sha = await self._gh.head_sha(
                state.pr_number, repo=binding.github_repo
            )
        except GitHubError as e:
            log.warning(
                "could not fetch PR head for %s#%d: %s",
                binding.github_repo,
                state.pr_number,
                e,
            )
            # Without a head SHA, scope CI dedup to the current check links so
            # new failed runs do not collapse into one permanent placeholder.
            head_sha = _unknown_head_ci_scope(checks)

        verdict = review_classifier(
            comments=[],
            ci=[_review_check_from_gh(c) for c in checks.runs],
            snapshot=ReviewSnapshot(head_sha=head_sha, head_committed_at=""),
        )
        if verdict.kind is not VerdictKind.CHANGES_REQUESTED:
            return
        if verdict.rule != "failing_ci":
            return
        if not should_dispatch_fix_run(
            prev_signature=state.last_trigger_signature,
            new_signature=verdict.trigger_signature,
        ):
            return
        if has_hit_iteration_cap(
            iteration=state.iteration, cap=self.config.review_iteration_cap
        ):
            await self._park_review_for_approval(
                run=run,
                binding=binding,
                issue=issue,
                trigger=verdict.trigger_signature,
            )
            return

        iteration = state.iteration + 1
        dispatched = await self._dispatch_ci_fix_run(
            run=run,
            binding=binding,
            issue=issue,
            checks=checks,
            verdict=verdict,
            iteration=iteration,
        )
        if dispatched:
            await db.review_state.bump_iteration(self._conn, issue.id)
            await db.review_state.set_signature(
                self._conn, issue.id, verdict.trigger_signature
            )

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
            cost, final_kind, final_returncode = await self._run_fix_agent(
                binding=binding,
                run_id=fix_run_id,
                workspace_path=workspace_path,
                prompt=prompt,
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

        await db.runs.update_status(
            self._conn,
            fix_run_id,
            "completed",
            ended_at=datetime.now(UTC).isoformat(),
        )

        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
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
        if state.pr_number is not None:
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
        return True

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

    async def _fail_review_run(
        self,
        *,
        run: db.runs.Run,
        binding: RepoBinding,
        issue: LinearIssue,
        error: str,
        last_log: str,
    ) -> None:
        await db.runs.update_status(
            self._conn,
            run.id,
            "failed",
            ended_at=datetime.now(UTC).isoformat(),
        )
        state = await db.review_state.get(self._conn, issue.id)
        cost = await db.runs.cost_for_issue(self._conn, issue.id)
        body = failed(
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
                error=error,
                last_log=last_log,
            )
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

    async def _scan_binding(
        self, binding: RepoBinding
    ) -> list[asyncio.Task[None]]:
        scheduled: list[asyncio.Task[None]] = []
        ready_state = binding.linear_states.ready
        try:
            issues = await self.linear.issues_in_state(
                binding.linear_team_key, ready_state, binding.issue_label
            )
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

    def _dispatch_capacity(self, binding: RepoBinding) -> int:
        if self.config.global_max_concurrent <= 0 or binding.max_concurrent <= 0:
            return 0
        binding_key = _binding_key(binding)
        return min(
            self.config.global_max_concurrent - len(self._scheduled_issue_ids),
            binding.max_concurrent
            - self._scheduled_binding_counts.get(binding_key, 0),
        )

    async def _schedule_ready_issue(
        self, binding: RepoBinding, issue: LinearIssue
    ) -> asyncio.Task[None] | None:
        async with self._schedule_lock:
            if self._dispatch_capacity(binding) <= 0:
                return None
            if issue.id in self._scheduled_issue_ids:
                return None
            if await db.runs.has_running_or_completed(self._conn, issue.id):
                return None
            return self._schedule_dispatch(binding, issue)

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
        self._scheduled_issue_ids.add(issue.id)
        self._scheduled_binding_counts[binding_key] = (
            self._scheduled_binding_counts.get(binding_key, 0) + 1
        )
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
            await self._mark_cancelled_dispatch(issue)
            raise
        finally:
            run_id = self._dispatch_run_ids.get(issue.id)
            if run_id is not None:
                if run_id not in self._operator_wait_run_ids:
                    self._dispatch_run_ids.pop(issue.id, None)
                self._runs_moved_to_in_progress.discard(run_id)

    async def _mark_cancelled_dispatch(self, issue: LinearIssue) -> None:
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
        self._scheduled_issue_ids.discard(issue_id)
        count = self._scheduled_binding_counts.get(binding_key, 0)
        if count <= 1:
            self._scheduled_binding_counts.pop(binding_key, None)
        else:
            self._scheduled_binding_counts[binding_key] = count - 1
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

    async def _poll_merge_candidates(self) -> list[asyncio.Task[None]]:
        """Advance approved Review PRs into Merge without operator action."""
        scheduled: list[asyncio.Task[None]] = []
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
            if await db.runs.has_active(
                self._conn,
                candidate.issue_id,
                ignored_stage="review",
            ):
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
                )
                if await self._finalize_pr_if_closed(
                    binding=binding,
                    issue=issue,
                    pr_number=candidate.pr_number,
                    pr_url=candidate.pr_url,
                    run_id=str(uuid.uuid4()),
                    create_run=True,
                    view=view,
                ):
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

            if verdict.kind is VerdictKind.APPROVED:
                if self.config.global_max_concurrent <= 0 or binding.max_concurrent <= 0:
                    continue
                binding_key = _binding_key(binding)
                if (
                    len(self._scheduled_issue_ids) >= self.config.global_max_concurrent
                    or self._scheduled_binding_counts.get(binding_key, 0)
                    >= binding.max_concurrent
                ):
                    continue
                scheduled.append(
                    self._schedule_merge(
                        binding=binding,
                        issue=issue,
                        pr_number=candidate.pr_number,
                        pr_url=candidate.pr_url,
                    )
                )
            elif verdict.merge_conflict:
                await self._mark_merge_needs_approval(
                    binding=binding,
                    issue=issue,
                    pr_url=candidate.pr_url,
                    run_id=str(uuid.uuid4()),
                    reason="merge conflict against base",
                    create_run=True,
                )
        return scheduled

    def _schedule_merge(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
    ) -> asyncio.Task[None]:
        binding_key = _binding_key(binding)
        self._scheduled_issue_ids.add(issue.id)
        self._scheduled_binding_counts[binding_key] = (
            self._scheduled_binding_counts.get(binding_key, 0) + 1
        )
        task = asyncio.create_task(
            self._merge_with_limits(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
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
        committed_at = await self._gh.commit_committed_at(binding.github_repo, head_sha)

        ci = [_review_check_from_github(run) for run in checks.runs]
        snapshot = ReviewSnapshot(
            head_sha=head_sha,
            head_committed_at=committed_at,
            reactions=_reactions_from_github(reactions),
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
            log.info(
                "submitted merge for %s#%d is not ready yet: %s",
                binding.github_repo,
                pr_number,
                reason,
            )

    async def _finalize_pr_if_closed(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
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
        poll would post a duplicate ▶. Inserting first closes that
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

        # 1. ▶ "starting" Linear comment.
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
            )
            return run_id

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

        # 6. Start the Review stage: ping `@codex review` on the PR and
        #    record the handoff. The Codex bot is the reviewer regardless
        #    of the binding's `agent` field; fix-runs spawned later go
        #    through the binding's CLI.
        await self._start_review_stage(
            binding=binding,
            issue=issue,
            pr_url=pr_url,
        )
        return run_id

    async def _merge_approved_pr(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
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
        )
        if not inserted:
            return None

        self._dispatch_run_ids[issue.id] = run_id
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

            branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
            try:
                await self._push_fn(workspace_path, branch)
                await self._gh.pr_merge(
                    pr_number,
                    strategy=binding.merge_strategy,
                    auto=True,
                    repo=binding.github_repo,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("merge failed for %s#%d: %s", binding.github_repo, pr_number, e)
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
                merged = False
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
        await self._workspace.cleanup(issue)
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

    async def _start_review_stage(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
    ) -> str | None:
        """Post `@codex review` and record a live Review monitor row.

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
                    elif ev.kind == "stderr" and ev.line is not None:
                        logf.write(f"[stderr] {ev.line}\n")
                    elif ev.kind in ("exit", "stall_timeout", "spawn_failed"):
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
        run_id: str,
        workspace_path: Path,
        prompt: str,
    ) -> tuple[float, str, int | None]:
        command = build_fix_runner_command(
            binding.agent,
            prompt,
            codex_model=binding.codex_model,
        )
        return await self._run_runner(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stage="review",
            agent=binding.agent,
            codex_model=binding.codex_model,
        )

    async def _run_runner(
        self,
        *,
        run_id: str,
        workspace_path: Path,
        command: list[str],
        stage: str,
        agent: str,
        codex_model: str = DEFAULT_CODEX_MODEL,
        clear_pid_on_finish: bool = False,
    ) -> tuple[float, str, int | None]:
        spec = RunnerSpec(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stall_secs=self.config.stall_timeout_secs,
            stage=stage,
        )

        log_path = self.config.log_root / f"{run_id}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        cumulative_cost = 0.0
        final_kind = "exit"
        final_returncode: int | None = None
        cost_estimator = _UsageCostEstimator(agent=agent, codex_model=codex_model)
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
                    elif ev.kind == "stderr" and ev.line is not None:
                        logf.write(f"[stderr] {ev.line}\n")
                    elif ev.kind in ("exit", "stall_timeout", "spawn_failed"):
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
                    stage="implement",
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
    ) -> None:
        await self._fail_run(run_id, reason)
        try:
            await self.linear.move_issue(issue.id, rollback_state_id)
        except LinearError as e:
            log.warning(
                "could not roll %s back after failed dispatch: %s",
                issue.identifier,
                e,
            )


__all__ = [
    "Orchestrator",
    "WebhookDispatchResult",
    "build_fix_runner_command",
    "build_merge_runner_command",
    "build_pr_body",
    "build_pr_title",
    "build_runner_command",
    "pr_number_from_url",
]


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
        author_is_me = bool(actor.get("isMe", False))
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
