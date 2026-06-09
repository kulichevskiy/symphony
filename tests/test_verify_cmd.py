"""Per-binding `verify_cmd`: build/test gate in the workspace before push.

A binding may declare `verify_cmd` (e.g. `pnpm build && pnpm test`). After
the last code-mutating stage (local review when enabled, implement
otherwise) and before push, the command runs in the workspace. Red gets
one implementer fix turn seeded with the failing output tail, then a
re-run; still red fails closed: no push, no PR, operator wait with the
failure tail on Linear. No `verify_cmd` → behavior unchanged.
"""

from __future__ import annotations

import asyncio
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
from symphony.pipeline.verify import (
    VerifyResult,
    run_verify_command,
    run_verify_session,
)


class _StagedRunner:
    """Returns scripted events keyed by `RunnerSpec.stage`."""

    def __init__(self, scripts: dict[str, list[list[RunnerEvent]]]) -> None:
        self._scripts = {k: list(v) for k, v in scripts.items()}
        self.captured: list[RunnerSpec] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured.append(spec)
        bucket = self._scripts.get(spec.stage)
        if not bucket:
            raise AssertionError(
                f"unexpected stage {spec.stage!r}; remaining={self._scripts}"
            )
        events = bucket.pop(0)

        async def gen() -> AsyncIterator[RunnerEvent]:
            for ev in events:
                yield ev

        return gen()

    async def kill(self, run_id: str) -> None:
        pass


# --- config ---------------------------------------------------------------


def test_verify_cmd_defaults_to_none() -> None:
    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo"),
    )
    assert binding.verify_cmd is None


def test_verify_cmd_and_timeout_parse() -> None:
    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        verify_cmd="pnpm build && pnpm test",
        verify_timeout_secs=600,
        linear_states=LinearStates(ready="Todo"),
    )
    assert binding.verify_cmd == "pnpm build && pnpm test"
    assert binding.resolved_verify_timeout_secs(1800) == 600


def test_verify_timeout_falls_back_to_global_default() -> None:
    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        verify_cmd="make check",
        linear_states=LinearStates(ready="Todo"),
    )
    assert binding.resolved_verify_timeout_secs(1800) == 1800


# --- run_verify_command (real subprocess) ----------------------------------


@pytest.mark.asyncio
async def test_run_verify_command_green(tmp_path: Path) -> None:
    ok, output = await run_verify_command(tmp_path, "echo all-good", 30)
    assert ok
    assert "all-good" in output


@pytest.mark.asyncio
async def test_run_verify_command_red_captures_output(tmp_path: Path) -> None:
    ok, output = await run_verify_command(
        tmp_path, "echo boom-stdout; echo boom-stderr >&2; exit 3", 30
    )
    assert not ok
    assert "boom-stdout" in output
    assert "boom-stderr" in output


@pytest.mark.asyncio
async def test_run_verify_command_times_out(tmp_path: Path) -> None:
    ok, output = await run_verify_command(tmp_path, "sleep 30", 1)
    assert not ok
    assert "timed out" in output


# --- run_verify_session ----------------------------------------------------


def _session_kwargs(runner: _StagedRunner, command_runner) -> dict:  # type: ignore[no-untyped-def]
    return {
        "runner": runner,
        "workspace_path": Path("/tmp/ws"),
        "verify_cmd": "pnpm build && pnpm test",
        "timeout_secs": 60,
        "parent_run_id": "run-1",
        "issue_title": "Add auth",
        "issue_body": "OAuth login.",
        "labels": ["feature"],
        "implementer_agent": "claude",
        "implementer_codex_model": "gpt-5.3-codex",
        "stall_secs": 60,
        "command_runner": command_runner,
    }


@pytest.mark.asyncio
async def test_verify_session_green_first_run_skips_fix_turn() -> None:
    async def command_runner(
        path: Path, cmd: str, timeout_secs: int
    ) -> tuple[bool, str]:
        return True, "all green"

    runner = _StagedRunner({})
    result = await run_verify_session(
        **_session_kwargs(runner, command_runner)
    )
    assert result.ok
    assert not result.fix_attempted
    assert runner.captured == []


@pytest.mark.asyncio
async def test_verify_session_red_then_fix_then_green() -> None:
    outcomes = [(False, "FAIL src/a.test.ts\nbuild error TS2345"), (True, "ok")]

    async def command_runner(
        path: Path, cmd: str, timeout_secs: int
    ) -> tuple[bool, str]:
        return outcomes.pop(0)

    runner = _StagedRunner(
        {
            "verify_fix": [
                [
                    RunnerEvent(kind="started", pid=1),
                    RunnerEvent(kind="exit", returncode=0),
                ]
            ]
        }
    )
    result = await run_verify_session(
        **_session_kwargs(runner, command_runner)
    )
    assert result.ok
    assert result.fix_attempted
    assert [s.stage for s in runner.captured] == ["verify_fix"]
    # The fix turn must see the tail of the failing output.
    prompt = runner.captured[0].command[-1]
    assert "build error TS2345" in prompt
    assert "pnpm build && pnpm test" in prompt


@pytest.mark.asyncio
async def test_verify_session_still_red_after_fix_fails_closed() -> None:
    outcomes = [(False, "first failure"), (False, "still red: TS2345 tail")]

    async def command_runner(
        path: Path, cmd: str, timeout_secs: int
    ) -> tuple[bool, str]:
        return outcomes.pop(0)

    runner = _StagedRunner(
        {
            "verify_fix": [
                [
                    RunnerEvent(kind="started", pid=1),
                    RunnerEvent(kind="exit", returncode=0),
                ]
            ]
        }
    )
    result = await run_verify_session(
        **_session_kwargs(runner, command_runner)
    )
    assert not result.ok
    assert result.fix_attempted
    assert "still red: TS2345 tail" in result.tail
    assert result.error


@pytest.mark.asyncio
async def test_verify_session_fix_run_spawn_failure_fails_closed() -> None:
    async def command_runner(
        path: Path, cmd: str, timeout_secs: int
    ) -> tuple[bool, str]:
        return False, "red output"

    runner = _StagedRunner(
        {"verify_fix": [[RunnerEvent(kind="spawn_failed", error="no claude")]]}
    )
    result = await run_verify_session(
        **_session_kwargs(runner, command_runner)
    )
    assert not result.ok
    assert result.fix_attempted
    assert "red output" in result.tail


# --- orchestrator e2e ------------------------------------------------------


def _verify_binding(verify_cmd: str) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        verify_cmd=verify_cmd,
        linear_states=LinearStates(ready="Todo", code_review="In Review"),
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
        labels=["feature"],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "In Review": "state-review",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


def _implement_script() -> list[RunnerEvent]:
    return [
        RunnerEvent(kind="started", pid=4242),
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "result",
                    "subtype": "success",
                    "total_cost_usd": 0.01,
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                }
            ),
        ),
        RunnerEvent(kind="exit", returncode=0),
    ]


async def _scan_and_wait(orch: Orchestrator, binding: RepoBinding) -> None:
    tasks = await orch._scan_binding(binding)  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)


def _orch_fixtures(tmp_path: Path, binding: RepoBinding):  # type: ignore[no-untyped-def]
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
    gh.pr_create = AsyncMock(return_value="https://github.com/org/repo/pull/42")
    gh.pr_comment = AsyncMock()
    gh.repo_clone = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="trunk")
    push_fn = AsyncMock()
    return cfg, linear, workspace_path, workspace, gh, push_fn


@pytest.mark.asyncio
async def test_failing_verify_cmd_blocks_push_and_pr(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _verify_binding("echo BUILD_BROKEN_MARKER; exit 1")
        cfg, linear, workspace_path, workspace, gh, push_fn = _orch_fixtures(
            tmp_path, binding
        )
        runner = _StagedRunner(
            {
                "implement": [_implement_script()],
                "verify_fix": [
                    [
                        RunnerEvent(kind="started", pid=1),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )
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

        # One fix turn was attempted, seeded with the failing tail.
        assert [s.stage for s in runner.captured] == [
            "implement",
            "verify_fix",
        ]
        assert "BUILD_BROKEN_MARKER" in runner.captured[1].command[-1]

        # Fail-closed: no push, no PR, issue blocked, operator wait.
        push_fn.assert_not_awaited()
        gh.pr_create.assert_not_awaited()
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-bl" in move_targets

        history = await db.runs.history_for_issue(conn, "iss-1")
        implement_rows = [h for h in history if h.stage == "implement"]
        assert len(implement_rows) == 1
        assert implement_rows[0].status == "failed"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED

        # Failure tail reaches Linear.
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("BUILD_BROKEN_MARKER" in body for body in posted)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_passing_verify_cmd_push_proceeds(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _verify_binding("true")
        cfg, linear, workspace_path, workspace, gh, push_fn = _orch_fixtures(
            tmp_path, binding
        )
        # No `verify_fix` bucket: a green verify must not dispatch a fix turn.
        runner = _StagedRunner({"implement": [_implement_script()]})
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

        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.pr_create.assert_awaited_once()
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_verify_session_crash_fails_closed(tmp_path: Path) -> None:
    """An exception inside the verify phase must block the push, not skip
    the gate: the gate exists to stop unbuildable code from shipping."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _verify_binding("true")
        cfg, linear, workspace_path, workspace, gh, push_fn = _orch_fixtures(
            tmp_path, binding
        )
        runner = _StagedRunner({"implement": [_implement_script()]})
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

        async def _exploding_session(**_: object) -> VerifyResult:
            raise RuntimeError("verify machinery blew up")

        import symphony.orchestrator.poll as poll_mod

        original = poll_mod.run_verify_session
        poll_mod.run_verify_session = _exploding_session  # type: ignore[assignment]
        try:
            await _scan_and_wait(orch, binding)
        finally:
            poll_mod.run_verify_session = original  # type: ignore[assignment]

        push_fn.assert_not_awaited()
        gh.pr_create.assert_not_awaited()
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-bl" in move_targets
    finally:
        await conn.close()
