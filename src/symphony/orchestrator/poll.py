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
import logging
import re
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

import aiosqlite

from .. import db
from ..agent.process import parse_event_line
from ..agent.prompt import implement_prompt, review_fix_prompt
from ..agent.runner import Runner, RunnerSpec
from ..agent.runners.local import LocalRunner
from ..config import Config, RepoBinding
from ..github.client import CheckRun as GitHubCheckRun
from ..github.client import GitHub, GitHubError, PRChecks
from ..linear import slash
from ..linear.client import Linear, LinearError, LinearIssue
from ..linear.slash import SlashIntent, SlashKind
from ..linear.templates import (
    CommentVars,
    failed,
    run_started,
    stage_done,
    stuck_loop_escape,
    truncate_body,
)
from ..pipeline.review_classifier import (
    CheckRun as ReviewCheckRun,
)
from ..pipeline.review_classifier import (
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


def build_pr_title(issue: LinearIssue) -> str:
    return f"[{issue.identifier}] {issue.title}"


def build_pr_body(issue: LinearIssue) -> str:
    """The Linear URL goes through `gh pr_create`'s `linear_url` argument
    (which appends `Relates to ...`), so the body itself is empty by
    default. Returning the URL here keeps the format pinned in tests."""
    return f"Relates to {issue.url}"


def build_runner_command(agent: str, prompt: str) -> list[str]:
    """Per-runner argv for the Implement stage prompt."""
    if agent == "claude":
        return [
            "claude",
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            prompt,
        ]
    if agent == "codex":
        return ["codex", "exec", "--json", prompt]
    raise ValueError(f"unknown agent {agent!r}")


def build_fix_runner_command(agent: str, prompt: str) -> list[str]:
    """argv for a Review-stage fix-run.

    Fix-runs go through the binding's CLI (claude or codex), NOT through
    the GitHub `@codex review` bot. The bot is only consulted via PR
    comments; the binding's `agent` field is what drives code changes
    in response to its feedback.
    """
    return build_runner_command(agent, prompt)


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


def _parse_rfc3339(s: str) -> datetime:
    """Linear timestamps end in `Z`; Python's `fromisoformat` accepts the
    `+00:00` form. Normalize before parsing."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


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
        self._active_run_ids: set[str] = set()
        self._dispatch_run_ids: dict[str, str] = {}
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
        try:
            scheduled.extend(await self._poll_review_runs())
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("review poll failed")
        for binding in self.config.repos:
            scheduled.extend(await self._scan_binding(binding))
        try:
            await self._poll_slash_commands()
        except Exception:  # noqa: BLE001 — must not kill the loop
            log.exception("slash command poll failed")
        return scheduled

    async def _poll_slash_commands(self) -> None:
        """For each active run, fetch new comments and dispatch slash intents.

        The cursor (`(timestamp, ids_at_timestamp)`) lives in `comment_cursors`.
        We query with `gte` and drop any comment whose ID is in the cursor's
        boundary set, which both (a) avoids re-firing handled commands across
        restarts and (b) avoids losing comments tied at the boundary timestamp.
        """
        pairs = list(self._dispatch_run_ids.items())
        for issue_id, run_id in pairs:
            if run_id not in self._active_run_ids:
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
            fresh = [c for c in comments if c.id not in seen_ids]
            if not fresh:
                continue
            fresh_with_times = [(c, _parse_rfc3339(c.created_at)) for c in fresh]
            latest_dt = max(created_at for _, created_at in fresh_with_times)
            latest_comments = [
                c for c, created_at in fresh_with_times if created_at == latest_dt
            ]
            latest = latest_comments[0].created_at
            latest_ids = {c.id for c in latest_comments}
            # If the new boundary equals the previous boundary, accumulate the
            # known IDs so we keep deduping any we already handled.
            if latest_dt == after:
                latest_ids |= seen_ids
            for intent in slash.parse(fresh):
                await self._handle_slash_intent(issue_id, run_id, intent)
            try:
                await db.comment_cursors.set(
                    self._conn, issue_id, latest, latest_ids
                )
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
        task = asyncio.create_task(self._poll_review_run(run, binding, issue))
        self._review_poll_tasks.add(task)
        task.add_done_callback(partial(self._review_poll_done, run_id=run.id))
        return task

    def _review_poll_done(self, task: asyncio.Task[None], run_id: str) -> None:
        self._review_poll_tasks.discard(task)
        self._review_poll_run_ids.discard(run_id)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("review poll task crashed for run_id=%s", run_id)

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

        iteration = await db.review_state.bump_iteration(self._conn, issue.id)
        await db.review_state.set_signature(
            self._conn, issue.id, verdict.trigger_signature
        )
        await self._dispatch_ci_fix_run(
            run=run,
            binding=binding,
            issue=issue,
            checks=checks,
            verdict=verdict,
            iteration=iteration,
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
    ) -> None:
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
            return

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
            return
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
            return

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
            return

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
        binding_key = _binding_key(binding)
        capacity = min(
            self.config.global_max_concurrent - len(self._scheduled_issue_ids),
            binding.max_concurrent
            - self._scheduled_binding_counts.get(binding_key, 0),
        )
        if capacity <= 0:
            log.info("scan %s: dispatch capacity is full", binding.linear_team_key)
            return scheduled
        for issue in issues:
            if issue.id in self._scheduled_issue_ids:
                continue
            if await db.runs.has_running_or_completed(self._conn, issue.id):
                continue
            scheduled.append(self._schedule_dispatch(binding, issue))
            if len(scheduled) >= capacity:
                break
        return scheduled

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
            run_id = self._dispatch_run_ids.pop(issue.id, None)
            if run_id is not None:
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

        try:
            cumulative_cost, final_kind, final_returncode = await self._run_agent(
                binding=binding,
                issue=issue,
                run_id=run_id,
                workspace_path=workspace_path,
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
    ) -> tuple[float, str, int | None]:
        """Spawn the runner and consume events. Returns
        (cumulative_cost, final_event_kind, final_returncode).
        """
        prompt = implement_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
        )
        command = build_runner_command(binding.agent, prompt)
        return await self._run_runner(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stage="implement",
        )

    async def _run_fix_agent(
        self,
        *,
        binding: RepoBinding,
        run_id: str,
        workspace_path: Path,
        prompt: str,
    ) -> tuple[float, str, int | None]:
        command = build_fix_runner_command(binding.agent, prompt)
        return await self._run_runner(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stage="review",
        )

    async def _run_runner(
        self,
        *,
        run_id: str,
        workspace_path: Path,
        command: list[str],
        stage: str,
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
                            cumulative_cost += usage.cost_usd
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
    "build_fix_runner_command",
    "build_pr_body",
    "build_pr_title",
    "build_runner_command",
    "pr_number_from_url",
]
