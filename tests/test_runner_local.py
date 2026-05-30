"""Light tests for LocalRunner.

We exercise the happy path (echo command exits 0 with stdout) and the
stall path (sleep longer than stall_secs is killed). Real agent JSON
parsing is exercised in iteration 4+ once the prompt + parser exist.
"""

from __future__ import annotations

import asyncio
import json
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
    original_read = asyncio.StreamReader.read

    async def delayed_read(self: asyncio.StreamReader, n: int = -1) -> bytes:
        await asyncio.sleep(0.4)
        return await original_read(self, n)

    monkeypatch.setattr(asyncio.StreamReader, "read", delayed_read)
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
async def test_runner_waits_when_process_closes_stdio_before_exit(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-closed-stdio",
        workspace_path=tmp_path,
        command=["sh", "-c", "exec >/dev/null 2>/dev/null; sleep 30"],
        stall_secs=1,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=10)
    assert events[-1].kind == "stall_timeout"
    assert not any(e.kind == "exit" and e.returncode is None for e in events)


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
async def test_runner_does_not_stall_while_command_in_flight(tmp_path: Path) -> None:
    # Agent emits item.started for a command_execution, then is silent past
    # stall_secs while its tool runs, then emits item.completed and exits.
    # The in-flight command must extend the deadline to command_secs so the
    # healthy run is not killed as a false-positive stall (the 2026-05-26
    # incident: a broad rg ran >stall_secs with no agent output).
    started = '{"type":"item.started","item":{"id":"c1","type":"command_execution"}}'
    completed = '{"type":"item.completed","item":{"id":"c1","type":"command_execution"}}'
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-inflight",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            f"printf '%s\\n' '{started}'; sleep 3; printf '%s\\n' '{completed}'; exit 0",
        ],
        stall_secs=1,
        command_secs=30,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=15)
    kinds = [e.kind for e in events]
    assert "stall_timeout" not in kinds
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_delivers_large_command_completion_line(tmp_path: Path) -> None:
    script = """
import json
import sys
import time

started = {"type": "item.started", "item": {"id": "c-large", "type": "command_execution"}}
completed = {
    "type": "item.completed",
    "item": {
        "id": "c-large",
        "type": "command_execution",
        "aggregated_output": "x" * (17 * 1024 * 1024),
    },
}
for event in (started, completed):
    sys.stdout.write(json.dumps(event) + "\\n")
    sys.stdout.flush()
time.sleep(4.0)
"""
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-large-completion",
        workspace_path=tmp_path,
        command=[sys.executable, "-c", script],
        stall_secs=5,
        command_secs=3,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=20)

    stdout_lines = [e.line for e in events if e.kind == "stdout"]
    completed_lines = [
        line
        for line in stdout_lines
        if (payload := json.loads(line)).get("type") == "item.completed"
        and payload.get("item", {}).get("id") == "c-large"
    ]
    assert completed_lines
    assert len(completed_lines[0].encode()) > 16 * 1024 * 1024
    assert "stall_timeout" not in [e.kind for e in events]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_recovers_when_stream_reports_oversized_line(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _ValueErrorThenPayloadStream:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload
            self._offset = 0
            self._raised = False

        async def read(self, n: int = -1) -> bytes:
            if not self._raised:
                self._raised = True
                raise ValueError("Separator is not found, and chunk exceed the limit")
            if self._offset >= len(self._payload):
                return b""
            if n < 0:
                n = len(self._payload) - self._offset
            chunk = self._payload[self._offset : self._offset + n]
            self._offset += len(chunk)
            return chunk

    class _FakeProcess:
        stdout = _ValueErrorThenPayloadStream(b"x" * (4 * 1024 * 1024 + 1) + b"\nnormal\n")
        stderr = None
        pid = None
        returncode: int | None = None

        async def wait(self) -> int:
            self.returncode = 0
            return 0

    async def fake_create_subprocess_exec(*_args: object, **_kwargs: object) -> _FakeProcess:
        return _FakeProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-overrun",
        workspace_path=tmp_path,
        command=["fake-agent"],
        stall_secs=10,
    )

    with caplog.at_level("WARNING", logger="symphony.agent.runners.local"):
        events = await asyncio.wait_for(_collect_events(runner, spec), timeout=10)

    assert [e.line for e in events if e.kind == "stdout"] == ["normal"]
    assert "stall_timeout" not in [e.kind for e in events]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0
    assert "skipping oversized stdout line" in caplog.text


@pytest.mark.asyncio
async def test_runner_recognises_legacy_command_execution_shape(tmp_path: Path) -> None:
    # Older codex builds emit `item_type` (on the item or on the event) instead
    # of `item.type`. activity.parse_codex_activity_line already accepts that;
    # the watchdog must too, or a legacy-shape tool call falls back to
    # stall_secs and gets killed by the very false-positive this PR fixes.
    started = (
        '{"type":"item.started","item":{"id":"c1","item_type":"command_execution"}}'
    )
    completed = (
        '{"type":"item.completed","item":{"id":"c1","item_type":"command_execution"}}'
    )
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-legacy",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            f"printf '%s\\n' '{started}'; sleep 3; printf '%s\\n' '{completed}'; exit 0",
        ],
        stall_secs=1,
        command_secs=30,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=15)
    kinds = [e.kind for e in events]
    assert "stall_timeout" not in kinds
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_stalls_when_command_exceeds_command_secs(tmp_path: Path) -> None:
    # The in-flight extension is bounded: a command that never completes
    # within command_secs is still killed (backstop against a deadlocked
    # agent that emitted item.started but hangs forever).
    #
    # command_secs < stall_secs so the cap, not the stall window, must be
    # the binding deadline. With the prior `max(stall, command)` formula
    # this test passed only because `sleep 30` outran stall_secs too.
    started = '{"type":"item.started","item":{"id":"c1","type":"command_execution"}}'
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-inflight-cap",
        workspace_path=tmp_path,
        command=["sh", "-c", f"printf '%s\\n' '{started}'; sleep 8"],
        stall_secs=30,
        command_secs=1,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=15)
    assert events[-1].kind == "stall_timeout"


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
