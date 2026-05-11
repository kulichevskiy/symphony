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
import logging
import re
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path

import aiosqlite

from .. import db
from ..agent.codex_models import DEFAULT_CODEX_MODEL
from ..agent.process import parse_event_line
from ..agent.prompt import implement_prompt
from ..agent.runner import Runner, RunnerSpec
from ..agent.runners.local import LocalRunner
from ..config import Config, RepoBinding
from ..github.client import GitHub, GitHubError
from ..linear import slash
from ..linear.client import Linear, LinearError, LinearIssue
from ..linear.slash import SlashIntent, SlashKind
from ..linear.templates import (
    CommentVars,
    cost_cap_reached,
    cost_warning,
    run_started,
    stage_done,
    truncate_body,
)
from ..pipeline.cost_guard import (
    effective_cap,
    effective_warning_pct,
    estimate_codex_cost_usd,
    evaluate_cost,
)
from ..pipeline.state_machine import on_runner_event
from ..workspace import Workspace

log = logging.getLogger(__name__)

PushFn = Callable[[Path, str], Awaitable[None]]
BindingKey = tuple[str, str, str]


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
        while self._dispatch_tasks:
            await asyncio.gather(
                *tuple(self._dispatch_tasks),
                return_exceptions=True,
            )

    async def _kill_active_runner(self, run_id: str) -> None:
        try:
            await self._runner.kill(run_id)
        except Exception:
            log.exception("failed to kill runner for run_id=%s", run_id)

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

    async def _start_review_stage(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_url: str,
    ) -> str | None:
        """Post `@codex review` and record a completed review handoff row.

        Idempotent in spirit: failure to post the bot ping does not block
        the run row from being created, but is logged loudly so an
        operator can re-ping with a slash command if needed.
        """
        await db.review_state.reset(self._conn, issue.id)
        pr_number = pr_number_from_url(pr_url)
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
            status="completed",
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
        spec = RunnerSpec(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stall_secs=self.config.stall_timeout_secs,
            stage="implement",
        )

        log_path = self.config.log_root / f"{run_id}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        cumulative_cost = 0.0
        final_kind = "exit"
        final_returncode: int | None = None
        cap_breached = False
        last_estimated_input_tokens = 0
        last_estimated_cached_input_tokens = 0
        last_estimated_output_tokens = 0
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
                            cost_delta = usage.cost_usd
                            if binding.agent == "codex" and cost_delta <= 0:
                                input_delta = max(
                                    usage.input_tokens - last_estimated_input_tokens, 0
                                )
                                cached_input_delta = max(
                                    usage.cached_input_tokens
                                    - last_estimated_cached_input_tokens,
                                    0,
                                )
                                output_delta = max(
                                    usage.output_tokens - last_estimated_output_tokens,
                                    0,
                                )
                                last_estimated_input_tokens = max(
                                    last_estimated_input_tokens, usage.input_tokens
                                )
                                last_estimated_cached_input_tokens = max(
                                    last_estimated_cached_input_tokens,
                                    usage.cached_input_tokens,
                                )
                                last_estimated_output_tokens = max(
                                    last_estimated_output_tokens, usage.output_tokens
                                )
                                cost_delta = estimate_codex_cost_usd(
                                    input_tokens=input_delta,
                                    cached_input_tokens=cached_input_delta,
                                    output_tokens=output_delta,
                                    model=binding.codex_model,
                                )
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

    async def _post_cost_warning(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        run_id: str,
        cumulative_total: float,
        cap_usd: float,
    ) -> bool:
        pct = int(round(cumulative_total / cap_usd * 100)) if cap_usd > 0 else 0
        body = cost_warning(
            CommentVars(
                stage="implement",
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
    "build_fix_runner_command",
    "build_pr_body",
    "build_pr_title",
    "build_runner_command",
    "pr_number_from_url",
]
