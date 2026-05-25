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
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.github.client import GitHubError
from symphony.linear.client import LinearComment, LinearError, LinearIssue
from symphony.linear.slash import SlashIntent, SlashKind
from symphony.orchestrator.poll import Orchestrator


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
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
        linear_states=LinearStates(ready="Todo"),
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
        gh.pr_create = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()

        result_line = json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "total_cost_usd": 0.42,
                "usage": {"input_tokens": 100, "output_tokens": 50},
            }
        )
        events = [
            RunnerEvent(kind="started", pid=4242),
            RunnerEvent(kind="stdout", line=json.dumps({"type": "system"})),
            RunnerEvent(kind="stdout", line=result_line),
            RunnerEvent(kind="exit", returncode=0),
        ]
        runner = _FakeRunner(events)

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
        gh.pr_create.assert_awaited_once()
        kwargs = gh.pr_create.await_args.kwargs
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
        assert history[0].pid == 4242
        assert history[1].stage == "review"
        assert history[1].status == "running"
        assert await db.runs.has_running_or_completed(conn, "iss-1") is True
        gh.pr_comment.assert_awaited_with(42, "@codex review", repo="org/repo")
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
        gh.pr_create = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(side_effect=GitHubError("boom"))

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner([RunnerEvent(kind="exit", returncode=0)]),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        gh.pr_create.assert_awaited_once()
        assert gh.pr_create.await_args.kwargs["base"] is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        # Implement run completes, Review monitor is recorded.
        assert [r.stage for r in history] == ["implement", "review"]
        assert history[0].status == "completed"
        assert history[1].status == "running"
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
        gh.pr_create = AsyncMock()
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
        gh.pr_create.assert_not_awaited()
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
async def test_implement_failed_retry_move_failure_keeps_command_unseen(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
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
        retry_comment = LinearComment(
            id="c-retry",
            body="$retry",
            created_at="2026-05-10T00:05:00+00:00",
            author_name="user",
            author_is_me=False,
            external_thread_type=None,
        )

        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[retry_comment])
        linear.move_issue = AsyncMock(side_effect=[LinearError("move down"), None])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(cfg, linear, conn, runner=_FakeRunner([]), gh=MagicMock())
        orch._states = {"ENG": _states()}  # noqa: SLF001
        await orch._track_implement_failed_wait(  # noqa: SLF001
            "iss-1",
            "failed-run",
            binding,
        )

        with pytest.raises(RuntimeError, match="move down"):
            await orch._poll_slash_commands()  # noqa: SLF001

        assert not await db.comment_events.seen(conn, "c-retry")
        assert await db.comment_cursors.get(conn, "iss-1") is None
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.run_id == "failed-run"
        rejected_bodies = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any("`$retry` ignored" in body and "move down" in body for body in rejected_bodies)

        await orch._poll_slash_commands()  # noqa: SLF001

        assert await db.comment_events.seen(conn, "c-retry")
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-todo"),
            call("iss-1", "state-todo"),
        ]
        posted_bodies = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any("Resumed" in body for body in posted_bodies)
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
        gh.pr_create = AsyncMock()
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

        gh.pr_create.assert_not_awaited()
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
        gh.pr_create = AsyncMock()
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
            update={"linear_states": LinearStates(ready="Todo", blocked="Missing")}
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

        with pytest.raises(RuntimeError, match="missing blocked state"):
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
        assert any(
            "`$stop` ignored" in body and "missing blocked state" in body
            for body in posted
        )
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
