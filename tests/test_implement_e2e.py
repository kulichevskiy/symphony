"""End-to-end Implement dispatch with mocked external surfaces.

Drag a Linear issue into `ready` → orchestrator clones the workspace,
posts ▶ comment, moves the issue to `in_progress`, spawns the agent
runner with the implement prompt, parses cost/tokens from streaming
JSON, persists run state to SQLite, opens a PR with the right title /
body, posts a stage-transition comment, and **halts** at "In Progress".

Review and Merge are out of scope for this slice.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
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


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        linear_states=LinearStates(ready="Todo"),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add authentication",
        description="Need OAuth login for the dashboard.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-todo",
        state_name="Todo",
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
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.repo_clone = AsyncMock()

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

        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001

        # ▶ starting comment + stage-transition comment.
        assert linear.post_comment.await_count == 2

        # Issue moved to In Progress (and never further — Implement halts here).
        linear.move_issue.assert_awaited_once_with("iss-1", "state-progress")

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
        assert kwargs["head"] == "symphony/eng-1"
        assert "base" not in kwargs
        assert kwargs["linear_url"] == "https://linear.app/team/issue/ENG-1"

        # Per-issue cost accumulated from streaming JSON.
        cost = await db.runs.cost_for_issue(conn, "iss-1")
        assert cost == pytest.approx(0.42)

        # Per-run log file exists at {log_root}/{run_id}.log and captured
        # the streaming JSON.
        logs = list(log_root.glob("*.log"))
        assert len(logs) == 1
        log_text = logs[0].read_text()
        assert '"type": "result"' in log_text or '"type":"result"' in log_text

        # Run row halted at in_progress: status=completed, stage=implement,
        # and only one run row exists (no Review or Merge run was created).
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].stage == "implement"
        assert history[0].status == "completed"
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
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock()

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

        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001

        # No PR opened on failure.
        gh.pr_create.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
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
