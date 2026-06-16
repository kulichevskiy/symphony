"""End-to-end Implement dispatch with mocked external surfaces.

Drag a Linear issue into `ready` → orchestrator clones the workspace,
posts 🚀 comment, moves the issue to `in_progress`, spawns the agent
runner with the implement prompt, parses cost/tokens from streaming
JSON, persists run state to SQLite, opens a PR with the right title /
body, posts a stage-transition comment, and **halts** at "In Progress".

Review and Merge are out of scope for this slice.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.github.client import GitHubError
from symphony.linear.client import LinearComment, LinearIssue
from symphony.linear.slash import SlashIntent, SlashKind
from symphony.orchestrator.poll import Orchestrator, _ImplementHandoff
from symphony.pipeline.local_review_loop import LoopOutcome, LoopResult

from ._workspace_helpers import advance_head


class _FakeRunner:
    def __init__(
        self, events: list[RunnerEvent], *, commit_on_implement: bool = False
    ) -> None:
        self.events = events
        self.commit_on_implement = commit_on_implement
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        # Simulate the agent committing its work so the completion gate sees
        # HEAD advance over the branch base.
        if self.commit_on_implement and spec.stage == "implement":
            advance_head(spec.workspace_path)
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        pass


class _ExplodingRunner:
    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        if False:
            yield RunnerEvent(kind="exit", returncode=0)
        raise RuntimeError("agent stream exploded")

    async def kill(self, run_id: str) -> None:
        pass


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _no_review_binding(*, auto_merge: bool) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        local_review=False,
        remote_review=False,
        auto_merge=auto_merge,
        linear_states=LinearStates(
            ready="Todo",
            local_code_review="",
            code_review="",
            needs_approval="Needs Approval",
        ),
    )


def _issue(
    *,
    state_id: str = "state-todo",
    state_name: str = "Todo",
) -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add authentication",
        description="Need OAuth login for the dashboard.",
        url="https://linear.app/team/issue/ENG-1",
        state_id=state_id,
        state_name=state_name,
        state_type="unstarted",
        team_key="ENG",
        labels=["feature", "backend"],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


async def _scan_and_wait(orch: Orchestrator, binding: RepoBinding) -> None:
    tasks = await orch._scan_binding(binding)  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_implement_dispatch_full_flow(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        log_root = tmp_path / "logs"
        cfg = Config(
            repos=[_binding()],
            log_root=log_root,
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()

        result_line = json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "result": "Implemented OAuth login and committed.\n\nSYMPHONY_DONE",
                "total_cost_usd": 0.42,
                "usage": {
                    "input_tokens": 100,
                    "cache_creation_input_tokens": 30,
                    "cache_read_input_tokens": 40,
                    "output_tokens": 50,
                },
                "modelUsage": {
                    "claude-opus-4-8[1m]": {
                        "inputTokens": 100,
                        "outputTokens": 50,
                        "cacheCreationInputTokens": 30,
                        "cacheReadInputTokens": 40,
                    }
                },
            }
        )
        events = [
            RunnerEvent(kind="started", pid=4242),
            RunnerEvent(kind="stdout", line=json.dumps({"type": "system"})),
            RunnerEvent(kind="stdout", line=result_line),
            RunnerEvent(kind="exit", returncode=0),
        ]
        runner = _FakeRunner(events, commit_on_implement=True)

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # 🚀 starting comment + stage-transition comment.
        assert linear.post_comment.await_count == 2

        # Issue enters Implement, then moves into the configured Review lane
        # after the PR is opened.
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]

        # Workspace was acquired + released.
        workspace.acquire.assert_awaited_once()
        workspace.release.assert_called_once()

        # Runner was spawned with the implement prompt for the right agent.
        assert runner.captured_spec is not None
        assert runner.captured_spec.workspace_path == workspace_path
        assert runner.captured_spec.stage == "implement"
        assert runner.captured_spec.command[0] == "claude"
        prompt_arg = runner.captured_spec.command[-1]
        assert "Add authentication" in prompt_arg
        assert "Need OAuth login" in prompt_arg
        assert "feature" in prompt_arg
        assert "backend" in prompt_arg

        # Branch was pushed before PR open.
        push_fn.assert_awaited_once()

        # PR opened with the prescribed title and body.
        gh.ensure_pr.assert_awaited_once()
        kwargs = gh.ensure_pr.await_args.kwargs
        assert kwargs["title"] == "[ENG-1] Add authentication"
        assert kwargs["repo"] == "org/repo"
        assert kwargs["base"] == "trunk"
        assert kwargs["head"] == "symphony/eng-1"
        assert kwargs["linear_url"] == "https://linear.app/team/issue/ENG-1"
        gh.repo_default_branch.assert_awaited_once_with("org/repo")

        # Per-issue cost accumulated from streaming JSON.
        cost = await db.runs.cost_for_issue(conn, "iss-1")
        assert cost == pytest.approx(0.42)

        # Per-run log file exists at {log_root}/{run_id}.log and captured
        # the streaming JSON.
        logs = list(log_root.glob("*.log"))
        assert len(logs) == 1
        log_text = logs[0].read_text()
        assert '"type": "result"' in log_text or '"type":"result"' in log_text

        # Two run rows: the completed Implement run, and the live Review
        # monitor row recorded immediately after pinging `@codex review`
        # on the PR.
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 2
        assert history[0].stage == "implement"
        assert history[0].status == "completed"
        assert history[0].termination_kind == ""
        assert history[0].termination_detail == ""
        assert history[0].pid == 4242
        assert history[0].input_tokens == 100
        assert history[0].output_tokens == 50
        assert history[0].cache_write_tokens == 30
        assert history[0].cache_read_tokens == 40

        # Per-(provider, model) attribution written at run end, summing back
        # to the run-level token columns (±0 for Claude's exact split).
        per_model = await db.run_model_usage.list_for_run(conn, history[0].id)
        assert len(per_model) == 1
        assert per_model[0].provider == "claude"
        assert per_model[0].model == "claude-opus-4-8[1m]"
        assert per_model[0].input_tokens == history[0].input_tokens
        assert per_model[0].output_tokens == history[0].output_tokens
        assert per_model[0].cache_write_tokens == history[0].cache_write_tokens
        assert per_model[0].cache_read_tokens == history[0].cache_read_tokens

        assert history[1].stage == "review"
        assert history[1].status == "running"
        assert await db.runs.has_running_or_completed(conn, "iss-1") is True
        gh.pr_comment.assert_awaited_with(42, "@codex review", repo="org/repo")
    finally:
        await conn.close()


@pytest.mark.parametrize("auto_merge", [True, False])
@pytest.mark.asyncio
async def test_false_false_review_binding_opens_pr_without_review_stage(
    tmp_path: Path,
    auto_merge: bool,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=auto_merge)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            commit_on_implement=True,
        )

        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Add authentication",
            team_key="ENG",
        )
        await db.review_state.begin_review(
            conn,
            "iss-1",
            pr_number=41,
            pr_url="https://github.com/org/repo/pull/41",
            github_repo="org/repo",
            issue_label="feature",
        )
        await db.review_state.set_signature(conn, "iss-1", "codex_inline:stale")
        await db.review_state.bump_iteration(conn, "iss-1")
        await db.review_state.bump_ci_fetch_failures(conn, "iss-1")
        await db.review_state.set_codex_lgtm_comment_id(conn, "iss-1", "comment-41")

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        gh.ensure_pr.assert_awaited_once()
        gh.pr_comment.assert_not_awaited()
        assert linear.move_issue.await_args_list == [call("iss-1", "state-progress")]

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement"]
        assert history[0].status == "completed"

        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert len(candidates) == 1
        assert candidates[0].pr_number == 42
        assert candidates[0].github_repo == "org/repo"
        assert candidates[0].binding_key

        review = await db.review_state.get(conn, "iss-1")
        assert review.pr_number == 42
        assert review.pr_url == "https://github.com/org/repo/pull/42"
        assert review.github_repo == "org/repo"
        assert review.issue_label == ""
        assert review.iteration == 0
        assert review.last_trigger_signature == ""
        assert review.ci_fetch_failures == 0
        assert review.codex_lgtm_comment_id == ""
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_dispatch_falls_back_when_base_lookup_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(side_effect=GitHubError("boom"))

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner(
                [RunnerEvent(kind="exit", returncode=0)], commit_on_implement=True
            ),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        gh.ensure_pr.assert_awaited_once()
        assert gh.ensure_pr.await_args.kwargs["base"] is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        # Implement run completes, Review monitor is recorded.
        assert [r.stage for r in history] == ["implement", "review"]
        assert history[0].status == "completed"
        assert history[1].status == "running"
    finally:
        await conn.close()


def _blocked_runner(message: str) -> _FakeRunner:
    result_line = json.dumps(
        {"type": "result", "subtype": "success", "result": message}
    )
    return _FakeRunner(
        [
            RunnerEvent(kind="started", pid=4242),
            RunnerEvent(kind="stdout", line=result_line),
            RunnerEvent(kind="exit", returncode=0),
        ],
        # No commit: a blocked agent leaves HEAD where it was.
        commit_on_implement=False,
    )


async def _run_blocked_dispatch(
    tmp_path: Path, conn: object, runner: _FakeRunner
) -> object:
    cfg = Config(
        repos=[_binding()],
        log_root=tmp_path / "logs",
        workspace_root=tmp_path / "ws",
        db_path=tmp_path / "s.sqlite",
    )
    linear = AsyncMock()
    linear.issues_in_state = AsyncMock(return_value=[_issue()])
    linear.lookup_issue = AsyncMock(return_value=_issue())
    linear.post_comment = AsyncMock(return_value="cmt-1")
    linear.move_issue = AsyncMock()

    workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
    workspace_path.mkdir(parents=True)
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=workspace_path)
    workspace.release = MagicMock()

    gh = MagicMock()
    gh.ensure_pr = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="trunk")
    push_fn = AsyncMock()

    orch = Orchestrator(
        cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
    )
    orch._states = {"ENG": _states()}  # noqa: SLF001
    await _scan_and_wait(orch, cfg.repos[0])
    return gh, push_fn


@pytest.mark.asyncio
async def test_implement_blocked_marker_captures_reason_and_skips_push(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        reason = "authorize the Supabase MCP at https://example.com/oauth then $retry"
        runner = _blocked_runner(f"Set up the scaffold.\n\nSYMPHONY_BLOCKED: {reason}")
        gh, push_fn = await _run_blocked_dispatch(tmp_path, conn, runner)

        # A blocked run never opens a PR or pushes.
        gh.ensure_pr.assert_not_awaited()
        push_fn.assert_not_awaited()

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].stage == "implement"
        assert history[0].termination_kind == "blocked"
        # Reason captured verbatim on the run record.
        assert history[0].termination_detail == reason
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_blocked_classifier_fallback_for_mch14(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        # Verbatim MCH-14 final message: no marker, no commits — the classifier
        # fallback must recognise the human-action ask as blocked, not completed.
        message = (
            "I need you to authorize the Supabase MCP server before I can "
            "continue. Please open the URL and approve access, then let me know."
        )
        runner = _blocked_runner(message)
        gh, push_fn = await _run_blocked_dispatch(tmp_path, conn, runner)

        gh.ensure_pr.assert_not_awaited()
        push_fn.assert_not_awaited()

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].termination_kind == "blocked"
        assert "authorize" in history[0].termination_detail.lower()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_blocked_run_opens_wait_then_retry_resumes_fresh_run_with_handoff(
    tmp_path: Path,
) -> None:
    """blocked → IMPLEMENT_BLOCKED wait + verbatim handoff comment; survives a
    daemon restart; `$retry` clears the wait and dispatches a FRESH implement
    run in the same workspace whose prompt carries the handoff block and which
    sees the prior run's uncommitted work."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        reason = "authorize the Supabase MCP at https://example.com/oauth then $retry"

        def _leave_uncommitted_work(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                (workspace_path / "wip.py").write_text("partial work\n")

        blocked_runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {
                            "type": "result",
                            "subtype": "success",
                            "result": f"Scaffolded the client.\n\nSYMPHONY_BLOCKED: {reason}",
                        }
                    ),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_leave_uncommitted_work,
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=blocked_runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # Blocked run parks the issue — no PR.
        gh.ensure_pr.assert_not_awaited()

        # A dedicated IMPLEMENT_BLOCKED wait was opened (not IMPLEMENT_FAILED).
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_BLOCKED

        # The handoff comment reproduces the verbatim human-action ask + $retry.
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any(reason in body and "$retry" in body for body in posted), (
            "expected a blocked handoff comment with the verbatim reason"
        )

        # Uncommitted work is left in the workspace.
        assert (workspace_path / "wip.py").exists()

        # --- Daemon restart: the wait is reloaded from SQLite. ---
        retry_comment = LinearComment(
            id="c-retry",
            body="$retry token=sk-operator-123",
            created_at="2026-05-10T01:00:00+00:00",
            author_name="user",
            author_is_me=False,
            external_thread_type=None,
        )
        fresh_runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=5151),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )
        restarted_linear = AsyncMock()
        restarted_linear.issues_in_state = AsyncMock(return_value=[_issue()])
        restarted_linear.lookup_issue = AsyncMock(return_value=_issue())
        restarted_linear.comments_since = AsyncMock(return_value=[retry_comment])
        restarted_linear.move_issue = AsyncMock(return_value=None)
        restarted_linear.post_comment = AsyncMock(return_value="cmt-2")
        restarted = Orchestrator(
            cfg,
            restarted_linear,
            conn,
            runner=fresh_runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        restarted._states = {"ENG": _states()}  # noqa: SLF001

        # `$retry` moves the issue back to Ready and clears the wait.
        await restarted._poll_slash_commands()  # noqa: SLF001
        restarted_linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
        assert await db.operator_waits.get(conn, "iss-1") is None

        # The fresh implement run reuses the same workspace; the prior
        # uncommitted work is still there.
        assert (workspace_path / "wip.py").exists()

        # Dispatch the fresh run; its prompt carries the handoff block.
        await restarted._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001
        assert fresh_runner.specs, "expected a fresh implement run to be dispatched"
        fresh_prompt = fresh_runner.specs[-1].command[-1]
        assert reason in fresh_prompt
        assert "token=sk-operator-123" in fresh_prompt
        assert "git status" in fresh_prompt
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_dispatch_marks_failed_on_runner_error(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        events = [
            RunnerEvent(kind="stderr", line="boom"),
            RunnerEvent(kind="exit", returncode=2),
        ]
        runner = _FakeRunner(events)

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # No PR opened on failure.
        gh.ensure_pr.assert_not_awaited()
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        assert history[0].termination_kind == "agent_nonzero_exit"
        assert history[0].termination_kind != "unknown"
        assert "return code 2" in history[0].termination_detail
        assert history[0].exit_returncode == 2
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
        assert wait.run_id == history[0].id
        # Failure comment is now posted so the operator knows what went wrong.
        posted_bodies = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any("Implement stage failed" in b for b in posted_bodies), (
            "expected a failed() comment to be posted"
        )
        assert any("$retry" in b for b in posted_bodies)
        assert not any("Will auto-retry shortly." in b for b in posted_bodies)

        # Even if Linear still reports the issue in the ready lane before the
        # state move is visible, the durable operator wait suppresses a retry
        # loop in the same process.
        assert await orch._scan_binding(cfg.repos[0]) == []  # noqa: SLF001

        # After a daemon restart, the persisted wait is restored and `$retry`
        # clears it by moving the issue back to Ready for the next poll.
        retry_comment = LinearComment(
            id="c-retry",
            body="$retry",
            created_at="2026-05-10T01:00:00+00:00",
            author_name="user",
            author_is_me=False,
            external_thread_type=None,
        )
        restarted_linear = AsyncMock()
        restarted_linear.comments_since = AsyncMock(return_value=[retry_comment])
        restarted_linear.move_issue = AsyncMock(return_value=None)
        restarted_linear.post_comment = AsyncMock(return_value="cmt-2")
        restarted = Orchestrator(
            cfg,
            restarted_linear,
            conn,
            runner=_FakeRunner([]),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        restarted._states = {"ENG": _states()}  # noqa: SLF001

        await restarted._poll_slash_commands()  # noqa: SLF001

        restarted_linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_after_agent_failure_with_commits_runs_agent_before_publish(
    tmp_path: Path,
) -> None:
    """A failed agent run can leave commits; `$retry` must still re-run the
    implementer instead of treating branch-ahead as safe to publish."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        def _commit_then_fail(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                (workspace_path / "partial.py").write_text("print('partial')\n")
                _git(workspace_path, "add", "-A")
                _git(workspace_path, "commit", "-m", "partial agent work")

        first_runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="stderr", line="boom"),
                RunnerEvent(kind="exit", returncode=2),
            ],
            on_run=_commit_then_fail,
        )
        first_linear = AsyncMock()
        first_linear.issues_in_state = AsyncMock(return_value=[_issue()])
        first_linear.lookup_issue = AsyncMock(return_value=_issue())
        first_linear.post_comment = AsyncMock(return_value="cmt-1")
        first_linear.move_issue = AsyncMock()
        first_push = AsyncMock()
        first_orch = Orchestrator(
            cfg,
            first_linear,
            conn,
            runner=first_runner,
            gh=gh,
            workspace=workspace,
            push_fn=first_push,
        )
        first_orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(first_orch, binding)

        assert [s.stage for s in first_runner.specs] == ["implement"]
        first_push.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        failed_run = history[0]
        assert failed_run.status == "failed"
        assert failed_run.termination_kind == "agent_nonzero_exit"
        assert await db.operator_waits.get(conn, "iss-1") is not None

        await first_orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            "iss-1",
            failed_run.id,
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T01:00:00+00:00",
                text="$retry",
            ),
        )
        assert await db.operator_waits.get(conn, "iss-1") is None

        def _commit_retry(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "retry work")

        retry_runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=5151),
                RunnerEvent(
                    kind="stdout",
                    line=_done_result_line("Retry implemented.\n\nSYMPHONY_DONE"),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit_retry,
        )
        retry_linear = AsyncMock()
        retry_linear.issues_in_state = AsyncMock(return_value=[_issue()])
        retry_linear.lookup_issue = AsyncMock(return_value=_issue())
        retry_linear.post_comment = AsyncMock(return_value="cmt-2")
        retry_linear.move_issue = AsyncMock()
        retry_push = AsyncMock()
        retry_orch = Orchestrator(
            cfg,
            retry_linear,
            conn,
            runner=retry_runner,
            gh=gh,
            workspace=workspace,
            push_fn=retry_push,
        )
        retry_orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(retry_orch, binding)

        assert [s.stage for s in retry_runner.specs] == ["implement"]
        retry_push.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_dispatch_marks_failed_on_runner_exception(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_ExplodingRunner(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        gh.ensure_pr.assert_not_awaited()
        workspace.release.assert_called_once()
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_manual_dispatch_failure_rolls_back_to_original_state(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        issue = _issue(state_id="state-blocked", state_name="Blocked")
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _FakeRunner(
            [
                RunnerEvent(kind="stderr", line="boom"),
                RunnerEvent(kind="exit", returncode=2),
            ]
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(cfg.repos[0], issue)  # noqa: SLF001

        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-blocked"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failed_implement_stop_keeps_wait_when_blocked_state_missing(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(
            update={
                "linear_states": LinearStates(
                    ready="Todo", code_review="Needs Approval", blocked="Missing"
                )
            }
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Add authentication",
            team_key="ENG",
        )
        await db.runs.create(
            conn,
            id="failed-run",
            issue_id="iss-1",
            stage="implement",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(cfg, linear, conn, runner=_FakeRunner([]), gh=MagicMock())
        orch._states = {"ENG": {"Todo": "state-todo"}}  # noqa: SLF001
        await orch._track_implement_failed_wait(  # noqa: SLF001
            "iss-1",
            "failed-run",
            binding,
        )

        await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            "iss-1",
            "failed-run",
            SlashIntent(
                kind=SlashKind.STOP,
                comment_id="c-stop",
                created_at="2026-05-10T00:05:00+00:00",
            ),
        )

        linear.move_issue.assert_not_awaited()
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
        assert orch._dispatch_run_ids["iss-1"] == "failed-run"  # noqa: SLF001
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any("missing blocked state" in body for body in posted)
    finally:
        await conn.close()


class _RecordingRunner:
    """Like `_FakeRunner` but keeps every spec and supports a per-run hook."""

    def __init__(
        self,
        events: list[RunnerEvent],
        on_run: Callable[[RunnerSpec], None] | None = None,
    ) -> None:
        self.events = events
        self.specs: list[RunnerSpec] = []
        self.on_run = on_run

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.specs.append(spec)
        if self.on_run is not None:
            self.on_run(spec)
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        pass


def _git(workspace: Path, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-c", "user.name=t",
            "-c", "user.email=t@example.com",
            *args,
        ],
        cwd=workspace,
        check=True,
        capture_output=True,
    )


def _init_git_workspace(workspace: Path) -> None:
    _git(workspace, "init", "-q")
    _git(workspace, "commit", "--allow-empty", "-m", "init")


def _dirty_gate_fixture(tmp_path: Path) -> dict[str, object]:
    cfg = Config(
        repos=[_binding()],
        log_root=tmp_path / "logs",
        workspace_root=tmp_path / "ws",
        db_path=tmp_path / "s.sqlite",
    )
    linear = AsyncMock()
    linear.issues_in_state = AsyncMock(return_value=[_issue()])
    linear.lookup_issue = AsyncMock(return_value=_issue())
    linear.post_comment = AsyncMock(return_value="cmt-1")
    linear.move_issue = AsyncMock()

    workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
    workspace_path.mkdir(parents=True)
    _init_git_workspace(workspace_path)
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=workspace_path)
    workspace.release = MagicMock()

    gh = MagicMock()
    gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
    gh.pr_comment = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="trunk")

    return {
        "cfg": cfg,
        "linear": linear,
        "workspace_path": workspace_path,
        "workspace": workspace,
        "gh": gh,
        "push_fn": AsyncMock(),
    }


@pytest.mark.asyncio
async def test_dirty_tree_blocks_push_after_one_failed_fix_turn(
    tmp_path: Path,
) -> None:
    """Uncommitted files + a fix turn that doesn't clean up → no push, no
    PR, implement run fails into the operator-wait path with the file
    list on Linear."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        fx = _dirty_gate_fixture(tmp_path)
        workspace_path: Path = fx["workspace_path"]  # type: ignore[assignment]
        (workspace_path / "feature.py").write_text("print('hi')\n")

        def _advance_head_on_implement(spec: RunnerSpec) -> None:
            # The agent committed its work (HEAD advances) but left feature.py
            # uncommitted; the empty commit satisfies the completion gate so
            # the dirty-tree gate is the one that blocks the push.
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "agent work")

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_advance_head_on_implement,
        )
        orch = Orchestrator(
            fx["cfg"],
            fx["linear"],
            conn,
            runner=runner,
            gh=fx["gh"],
            workspace=fx["workspace"],
            push_fn=fx["push_fn"],
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, fx["cfg"].repos[0])  # type: ignore[union-attr]

        fx["push_fn"].assert_not_awaited()  # type: ignore[union-attr]
        fx["gh"].ensure_pr.assert_not_awaited()  # type: ignore[union-attr]

        # Exactly one fix turn after the implement turn.
        assert len(runner.specs) == 2
        fix_prompt = runner.specs[1].command[-1]
        assert "uncommitted" in fix_prompt
        assert "feature.py" in fix_prompt

        history = await db.runs.history_for_issue(conn, "iss-1")
        by_stage = {r.stage: r for r in history}
        assert by_stage["implement"].status == "failed"
        assert "implement_fix" in by_stage

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED

        posted = [
            str(c.args[1])
            for c in fx["linear"].post_comment.await_args_list  # type: ignore[union-attr]
        ]
        assert any("feature.py" in body for body in posted), (
            "expected the uncommitted file list in a Linear comment"
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_dirty_tree_fix_turn_commits_and_push_proceeds(
    tmp_path: Path,
) -> None:
    """The one fix turn commits the leftovers → push and PR happen."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        fx = _dirty_gate_fixture(tmp_path)
        workspace_path: Path = fx["workspace_path"]  # type: ignore[assignment]
        (workspace_path / "feature.py").write_text("print('hi')\n")

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )

        def _commit_on_fix_turn(spec: RunnerSpec) -> None:
            # Implement turn: agent commits its work (HEAD advances) but leaves
            # feature.py uncommitted, so the completion gate passes and the
            # dirty-tree gate's single fix turn cleans up the leftover.
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "agent work")
            if len(runner.specs) == 2:
                _git(workspace_path, "add", "-A")
                _git(workspace_path, "commit", "-m", "commit leftovers")

        runner.on_run = _commit_on_fix_turn

        orch = Orchestrator(
            fx["cfg"],
            fx["linear"],
            conn,
            runner=runner,
            gh=fx["gh"],
            workspace=fx["workspace"],
            push_fn=fx["push_fn"],
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, fx["cfg"].repos[0])  # type: ignore[union-attr]

        assert len(runner.specs) == 2
        fx["push_fn"].assert_awaited_once()  # type: ignore[union-attr]
        fx["gh"].ensure_pr.assert_awaited_once()  # type: ignore[union-attr]

        history = await db.runs.history_for_issue(conn, "iss-1")
        by_stage = {r.stage: r for r in history}
        assert by_stage["implement"].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_clean_tree_skips_fix_turn_entirely(tmp_path: Path) -> None:
    """Clean working tree → no extra agent turn, push proceeds."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        fx = _dirty_gate_fixture(tmp_path)
        workspace_path: Path = fx["workspace_path"]  # type: ignore[assignment]

        def _advance_head_on_implement(spec: RunnerSpec) -> None:
            # Agent committed its work and left a clean tree; the empty commit
            # satisfies the completion gate so push proceeds with no fix turn.
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "agent work")

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_advance_head_on_implement,
        )
        orch = Orchestrator(
            fx["cfg"],
            fx["linear"],
            conn,
            runner=runner,
            gh=fx["gh"],
            workspace=fx["workspace"],
            push_fn=fx["push_fn"],
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, fx["cfg"].repos[0])  # type: ignore[union-attr]

        assert len(runner.specs) == 1
        fx["push_fn"].assert_awaited_once()  # type: ignore[union-attr]
        fx["gh"].ensure_pr.assert_awaited_once()  # type: ignore[union-attr]
    finally:
        await conn.close()


def _done_result_line(message: str) -> str:
    return json.dumps({"type": "result", "subtype": "success", "result": message})


async def _seed_publish_failed_implement_run(
    conn, *, run_id: str = "publish-failed-run"
) -> None:
    issue = _issue()
    await db.issues.upsert(
        conn,
        id=issue.id,
        identifier=issue.identifier,
        title=issue.title,
        team_key=issue.team_key,
    )
    await db.runs.create(
        conn,
        id=run_id,
        issue_id=issue.id,
        stage="implement",
        status="running",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )
    await db.runs.update_status(
        conn,
        run_id,
        "failed",
        ended_at="2026-05-10T00:01:00+00:00",
        kind=db.runs.PUBLISH_FAILED_KIND,
        detail="push failed: boom",
    )


@pytest.mark.asyncio
async def test_branch_already_ahead_short_circuits_to_publish(tmp_path: Path) -> None:
    """An implement (re)dispatch on a branch already ahead of base skips the
    agent and the completion gate entirely and proceeds straight to the
    agent-free publish step (push + ensure_pr)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        # HEAD is one commit ahead of `trunk` (the resolved base): this can
        # happen on a fresh dispatch when the branch already carries work.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        # Would advance HEAD / record a spec if the agent ran — it must not.
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        # The agent never ran: no completion gate, no fix turns.
        assert runner.specs == []
        # Publish ran: branch pushed, PR ensured (idempotently).
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [r.stage for r in history] == ["implement"]
        assert history[-1].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_setup_failure_releases_workspace_and_fails_run(
    tmp_path: Path,
) -> None:
    """If setup after acquire fails before the branch decision, the workspace
    is released and the implement run is marked failed instead of staying
    live."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._resolve_base_branch = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=RuntimeError("base boom")
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        workspace.release.assert_called_once()
        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        assert "base boom" in history[0].termination_detail
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_with_pending_handoff_runs_agent_and_consumes_prompt(
    tmp_path: Path,
) -> None:
    """A blocked-run `$retry` handoff is an explicit reason to run the agent
    even when the branch is already ahead of base; the handoff must be included
    in the prompt and consumed by that same orchestrator."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior blocked work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        def _commit_handoff_retry(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "handoff retry")

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(
                    kind="stdout",
                    line=_done_result_line("Resumed and finished.\n\nSYMPHONY_DONE"),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit_handoff_retry,
        )
        push_fn = AsyncMock()
        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._implement_handoffs["iss-1"] = _ImplementHandoff(  # noqa: SLF001
            blocked_reason="authorize the deployment OAuth URL",
            operator_comment="$retry token=available",
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert [s.stage for s in runner.specs] == ["implement"]
        prompt = runner.specs[0].command[-1]
        assert "authorize the deployment OAuth URL" in prompt
        assert "$retry token=available" in prompt
        assert "iss-1" not in orch._implement_handoffs  # noqa: SLF001
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_releases_workspace_when_gate_raises(
    tmp_path: Path,
) -> None:
    """If a pre-push gate raises on the short-circuit path, the workspace is
    still released, the run is failed/parked, and publish (push + ensure_pr)
    never runs."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        # HEAD one commit ahead of `trunk`: the short-circuit branch is taken.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        # A gate blows up (e.g. a subprocess/db error inside verify/dirty-tree).
        orch._run_prepush_gates = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=RuntimeError("gate boom")
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        # The workspace was released *before* the gate ran, so the raise could
        # not leak it; the run failed closed before publish.
        workspace.release.assert_called_once()
        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].status == "failed"
        assert "gate boom" in history[-1].termination_detail
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_halts_before_publish_when_gate_fails(
    tmp_path: Path,
) -> None:
    """If a pre-push gate halts the run (proceed=False) on the short-circuit
    path, the workspace is released and publish (push + ensure_pr) never runs;
    the run returns without raising."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        # A gate halts the run (recorded its own state) and returns proceed=False.
        orch._run_prepush_gates = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=(False, None)
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        workspace.release.assert_called_once()
        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_resume_at_publish_after_delivery_failure_skips_agent(
    tmp_path: Path,
) -> None:
    """A push failure parks the run; the re-dispatch resumes at publish —
    the branch is already ahead of base, so the agent is skipped and only
    push + ensure_pr run."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        # Base only; the first run's agent will advance HEAD over `trunk`.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        def _commit_on_implement(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                (workspace_path / "feature.py").write_text("print('hi')\n")
                _git(workspace_path, "add", "-A")
                _git(workspace_path, "commit", "-m", "agent work")

        runner1 = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(
                    kind="stdout",
                    line=_done_result_line("Implemented it.\n\nSYMPHONY_DONE"),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit_on_implement,
        )
        failing_push = AsyncMock(side_effect=RuntimeError("push boom"))

        linear1 = AsyncMock()
        linear1.post_comment = AsyncMock(return_value="cmt-1")
        linear1.move_issue = AsyncMock()
        orch1 = Orchestrator(
            cfg, linear1, conn, runner=runner1, gh=gh, workspace=workspace,
            push_fn=failing_push,
        )
        orch1._states = {"ENG": _states()}  # noqa: SLF001

        await orch1._dispatch_one(binding, _issue())  # noqa: SLF001

        # First run: agent ran, push attempted and failed, no PR.
        assert [s.stage for s in runner1.specs] == ["implement"]
        failing_push.assert_awaited_once()
        gh.ensure_pr.assert_not_awaited()
        h1 = await db.runs.history_for_issue(conn, "iss-1")
        assert h1[0].status == "failed"

        # --- Resume: re-dispatch with a healthy push. The branch is ahead of
        #     base now, so the agent must not run again. ---
        runner2 = _RecordingRunner(
            [RunnerEvent(kind="started", pid=5151), RunnerEvent(kind="exit", returncode=0)]
        )
        good_push = AsyncMock()
        linear2 = AsyncMock()
        linear2.post_comment = AsyncMock(return_value="cmt-2")
        linear2.move_issue = AsyncMock()
        orch2 = Orchestrator(
            cfg, linear2, conn, runner=runner2, gh=gh, workspace=workspace,
            push_fn=good_push,
        )
        orch2._states = {"ENG": _states()}  # noqa: SLF001

        await orch2._dispatch_one(binding, _issue())  # noqa: SLF001

        # The agent was skipped on resume.
        assert runner2.specs == []
        good_push.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        h2 = await db.runs.history_for_issue(conn, "iss-1")
        completed = [r for r in h2 if r.stage == "implement" and r.status == "completed"]
        assert len(completed) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_publish_failed_retry_without_resolved_base_runs_agent_not_publish(
    tmp_path: Path,
) -> None:
    """A publish-failed retry is not enough to publish when the base cannot be
    resolved and the current checkout cannot prove deliverable commits."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        await _seed_publish_failed_implement_run(conn)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(side_effect=GitHubError("boom"))

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=5151), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()
        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert [s.stage for s in runner.specs] == ["implement"]
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()

        history = await db.runs.history_for_issue(conn, "iss-1")
        impl = [r for r in history if r.stage == "implement"]
        assert impl[-1].status == "failed"
        assert "completion contract" in impl[-1].termination_detail
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_publish_failed_retry_with_base_and_no_ahead_runs_agent_not_publish(
    tmp_path: Path,
) -> None:
    """A prior publish_failed run is not enough to publish when a resolved
    base proves the current branch has no commits to deliver."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        await _seed_publish_failed_implement_run(conn)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=5151), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()
        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert [s.stage for s in runner.specs] == ["implement"]
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        impl = [r for r in history if r.stage == "implement"]
        assert impl[-1].status == "failed"
        assert "completion contract" in impl[-1].termination_detail
    finally:
        await conn.close()


def _head_sha(workspace: Path) -> str:
    out = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workspace, check=True, capture_output=True
    )
    return out.stdout.decode().strip()


def _local_review_binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        local_review=True,
        remote_review=False,
        auto_merge=False,
        # The PR-summary post path needs verdicts on the result; this test
        # only cares about routing, so keep the thread quiet.
        post_local_review_pr_summary=False,
        linear_states=LinearStates(
            ready="Todo",
            local_code_review="Local Code Review",
            code_review="",
            needs_approval="Needs Approval",
        ),
    )


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_reruns_local_review_not_parked(
    tmp_path: Path,
) -> None:
    """Branch-ahead short-circuit with a `local_review` binding: the pre-push
    gates re-run, so publish gets a real APPROVED verdict — NOT the `None` its
    handoff would mis-read as "local-only review did not approve" and park.
    The implementer agent is still skipped."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _local_review_binding()
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        # HEAD already one commit ahead of `trunk`: a prior run committed.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")
        await _seed_publish_failed_implement_run(conn)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        # The reused branch is re-reviewed; drive it to APPROVED so the
        # routing decision under test is exercised without a real reviewer.
        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=LoopResult(
                outcome=LoopOutcome.APPROVED, iterations=1, verdicts=()
            )
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        # The implementer agent never ran (short-circuit), but the local
        # review gate did re-run on the reused workspace.
        assert runner.specs == []
        orch._run_local_review_phase.assert_awaited_once()  # noqa: SLF001
        assert (  # noqa: SLF001
            orch._run_local_review_phase.await_args.kwargs["allow_fixes"] is False
        )
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        history = await db.runs.history_for_issue(conn, "iss-1")
        impl = [r for r in history if r.stage == "implement"]
        assert impl and impl[-1].status == "completed"
        # The review stage was started and NOT failed as "did not approve".
        review = [r for r in history if r.stage == "review"]
        assert review, "an approved local-only PR should start the review stage"
        assert all(r.status != "failed" for r in review)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_records_verify_pass(
    tmp_path: Path,
) -> None:
    """Branch-ahead short-circuit with a `verify_cmd` binding re-runs the
    verify gate, so the green SHA is recorded — the merge gate still treats
    the pushed head as verified instead of routing to operator approval."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            agent="claude",
            branch_prefix="symphony",
            local_review=False,
            remote_review=False,
            auto_merge=False,
            verify_cmd="true",
            linear_states=LinearStates(
                ready="Todo",
                local_code_review="",
                code_review="",
                needs_approval="Needs Approval",
            ),
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")
        await _seed_publish_failed_implement_run(conn)
        head = _head_sha(workspace_path)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        # The implementer agent never ran; `verify_cmd="true"` runs as a
        # subprocess (no fix turn → no runner spec) and passes.
        assert runner.specs == []
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        # The merge gate keys off the recorded green SHA for the pushed head.
        assert await db.issue_prs.has_verify_passed(
            conn, issue_id="iss-1", github_repo="org/repo", head_sha=head
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_dirty_tree_fails_without_fix_turn(
    tmp_path: Path,
) -> None:
    """Dirty branch-ahead resume fails closed instead of spawning the
    dirty-tree implement_fix agent."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")
        (workspace_path / "leftover.txt").write_text("uncommitted\n")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].status == "failed"
        assert "working tree dirty during publish resume" in history[-1].termination_detail
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_verify_failure_skips_fix_turn(
    tmp_path: Path,
) -> None:
    """Red verify on branch-ahead resume fails closed without a verify_fix
    runner turn."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            agent="claude",
            branch_prefix="symphony",
            local_review=False,
            remote_review=False,
            auto_merge=False,
            verify_cmd="false",
            linear_states=LinearStates(
                ready="Todo",
                local_code_review="",
                code_review="",
                needs_approval="Needs Approval",
            ),
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        impl = [r for r in history if r.stage == "implement"]
        assert impl[-1].status == "failed"
        assert "fix turn disabled for publish resume" in impl[-1].termination_detail
    finally:
        await conn.close()


def test_pr_title_and_body_format() -> None:
    """The PR title format is `[<LINEAR_ID>] <issue title>` and the body
    contains `Relates to <linear-url>`. Verified by introspecting the
    helper that builds them so the format stays pinned even when the
    full e2e flow is mocked."""
    from symphony.orchestrator.poll import build_pr_body, build_pr_title

    assert build_pr_title(_issue()) == "[ENG-1] Add authentication"
    body = build_pr_body(_issue())
    assert "https://linear.app/team/issue/ENG-1" in body
