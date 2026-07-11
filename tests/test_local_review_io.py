"""`collect_runner_output` adapter — driving the Runner protocol to a string."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.pipeline.local_review_io import collect_runner_output


class _FakeRunner:
    """Minimal Runner implementation that emits a scripted event stream."""

    def __init__(self, events: list[RunnerEvent]) -> None:
        self._events = events
        self.kill_calls: list[str] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        async def gen() -> AsyncIterator[RunnerEvent]:
            for ev in self._events:
                yield ev

        return gen()

    async def kill(self, run_id: str) -> None:
        self.kill_calls.append(run_id)


def _spec() -> RunnerSpec:
    return RunnerSpec(
        run_id="rid-1",
        workspace_path=Path("/tmp/does-not-matter"),
        command=["echo", "hi"],
    )


@pytest.mark.asyncio
async def test_collects_stdout_lines_in_order_and_reports_exit() -> None:
    runner = _FakeRunner(
        [
            RunnerEvent(kind="started", pid=42),
            RunnerEvent(kind="stdout", line="line one"),
            RunnerEvent(kind="stdout", line="line two"),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )
    out = await collect_runner_output(runner, _spec())
    assert out.stdout == "line one\nline two"
    assert out.stderr == ""
    assert out.terminal_kind == "exit"
    assert out.returncode == 0
    assert out.ok_exit is True
    assert out.spawn_error is None
    assert out.stall_timeout is False


@pytest.mark.asyncio
async def test_captures_stderr_separately() -> None:
    runner = _FakeRunner(
        [
            RunnerEvent(kind="stdout", line="hello"),
            RunnerEvent(kind="stderr", line="warning: x"),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )
    out = await collect_runner_output(runner, _spec())
    assert out.stdout == "hello"
    assert out.stderr == "warning: x"


@pytest.mark.asyncio
async def test_stall_timeout_terminates_collection() -> None:
    runner = _FakeRunner(
        [
            RunnerEvent(kind="stdout", line="partial"),
            RunnerEvent(kind="stall_timeout"),
            # Any post-terminal event must be ignored.
            RunnerEvent(kind="stdout", line="should-not-appear"),
        ]
    )
    out = await collect_runner_output(runner, _spec())
    assert out.stdout == "partial"
    assert out.terminal_kind == "stall_timeout"
    assert out.stall_timeout is True
    assert out.ok_exit is False


@pytest.mark.asyncio
async def test_spawn_failure_surfaces_error() -> None:
    runner = _FakeRunner(
        [RunnerEvent(kind="spawn_failed", error="FileNotFoundError: codex")],
    )
    out = await collect_runner_output(runner, _spec())
    assert out.stdout == ""
    assert out.terminal_kind == "spawn_failed"
    assert out.spawn_error == "FileNotFoundError: codex"
    assert out.ok_exit is False


@pytest.mark.asyncio
async def test_nonzero_exit_is_not_ok_but_output_still_collected() -> None:
    """A reviewer that exits non-zero may still have emitted a usable
    final agent message — the verdict parser is what decides whether
    the output is meaningful, not the exit code."""
    runner = _FakeRunner(
        [
            RunnerEvent(kind="stdout", line='{"type":"item.completed"}'),
            RunnerEvent(kind="exit", returncode=2),
        ]
    )
    out = await collect_runner_output(runner, _spec())
    assert out.stdout == '{"type":"item.completed"}'
    assert out.returncode == 2
    assert out.terminal_kind == "exit"
    assert out.ok_exit is False


@pytest.mark.asyncio
async def test_tick_events_are_dropped() -> None:
    runner = _FakeRunner(
        [
            RunnerEvent(kind="tick"),
            RunnerEvent(kind="stdout", line="real"),
            RunnerEvent(kind="tick"),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )
    out = await collect_runner_output(runner, _spec())
    assert out.stdout == "real"


@pytest.mark.asyncio
async def test_tees_stdout_and_stderr_to_log_path(tmp_path: Path) -> None:
    """With `log_path`, every line is written as it arrives — stdout
    verbatim, stderr prefixed `[stderr] ` — matching the orchestrator's
    implement-run tee. The in-memory collection is unaffected."""
    log_path = tmp_path / "logs" / "rid-1.log"
    runner = _FakeRunner(
        [
            RunnerEvent(kind="stdout", line="line one"),
            RunnerEvent(kind="stderr", line="warning: x"),
            RunnerEvent(kind="stdout", line="line two"),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )
    out = await collect_runner_output(runner, _spec(), log_path=log_path)
    # In-memory collection unchanged (stdout/stderr kept separate).
    assert out.stdout == "line one\nline two"
    assert out.stderr == "warning: x"
    # The tee'd file interleaves them in arrival order, stderr prefixed.
    assert log_path.read_text(encoding="utf-8") == (
        "line one\n[stderr] warning: x\nline two\n"
    )


@pytest.mark.asyncio
async def test_no_log_path_writes_nothing(tmp_path: Path) -> None:
    runner = _FakeRunner(
        [RunnerEvent(kind="stdout", line="x"), RunnerEvent(kind="exit", returncode=0)]
    )
    await collect_runner_output(runner, _spec())
    assert os.listdir(tmp_path) == []
