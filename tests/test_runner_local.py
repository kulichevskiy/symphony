"""Light tests for LocalRunner.

We exercise the happy path (echo command exits 0 with stdout) and the
stall path (sleep longer than stall_secs is killed). Real agent JSON
parsing is exercised in iteration 4+ once the prompt + parser exist.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.agent.runners.local import LocalRunner


@pytest.mark.asyncio
async def test_runner_streams_stdout_and_exits_clean(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r1",
        workspace_path=tmp_path,
        command=["sh", "-c", "printf 'hello\\nworld\\n'; exit 0"],
        stall_secs=10,
    )
    events = [ev async for ev in runner.run(spec)]
    started = [e for e in events if e.kind == "started"]
    assert started and started[0].pid is not None
    stdout = [e.line for e in events if e.kind == "stdout"]
    assert stdout == ["hello", "world"]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_drains_tail_output_after_process_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_readline = asyncio.StreamReader.readline

    async def delayed_readline(self: asyncio.StreamReader) -> bytes:
        await asyncio.sleep(0.4)
        return await original_readline(self)

    monkeypatch.setattr(asyncio.StreamReader, "readline", delayed_readline)
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-tail",
        workspace_path=tmp_path,
        command=["sh", "-c", "printf 'tail\\n'"],
        stall_secs=10,
    )
    events = [ev async for ev in runner.run(spec)]
    stdout = [e.line for e in events if e.kind == "stdout"]
    assert stdout == ["tail"]


@pytest.mark.asyncio
async def test_runner_exits_when_child_holds_pipe_open(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-held-pipe",
        workspace_path=tmp_path,
        command=[
            sys.executable,
            "-c",
            (
                "import subprocess; "
                "print('tail', flush=True); "
                "subprocess.Popen(['sleep', '30'])"
            ),
        ],
        stall_secs=60,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=10)
    stdout = [e.line for e in events if e.kind == "stdout"]
    assert stdout == ["tail"]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_kills_on_stall(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r2",
        workspace_path=tmp_path,
        command=["sh", "-c", "sleep 30"],
        stall_secs=1,
    )
    kinds: list[str] = []
    async for ev in runner.run(spec):
        kinds.append(ev.kind)
        if ev.kind in ("exit", "stall_timeout"):
            break
    assert "stall_timeout" in kinds


@pytest.mark.asyncio
async def test_runner_kill_terminates(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r3",
        workspace_path=tmp_path,
        command=["sh", "-c", "sleep 30"],
        stall_secs=60,
    )

    async def consume() -> list[str]:
        out: list[str] = []
        async for ev in runner.run(spec):
            out.append(ev.kind)
            if ev.kind in ("exit", "stall_timeout"):
                break
        return out

    consumer = asyncio.create_task(consume())
    # Give the subprocess a moment to actually spawn.
    await asyncio.sleep(0.2)
    await runner.kill("r3")
    kinds = await asyncio.wait_for(consumer, timeout=10)
    assert "exit" in kinds


async def _collect_events(runner: LocalRunner, spec: RunnerSpec) -> list[RunnerEvent]:
    return [ev async for ev in runner.run(spec)]
