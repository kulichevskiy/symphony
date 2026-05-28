"""LocalRunner — subprocess on the orchestrator host.

Mirrors the load-bearing parts of the Rust `agent/process.rs`:

- Separate stdout / stderr pumps that notify a stall watchdog on every line.
- PID-based liveness for the watchdog (status fields lie during fix-runs;
  PIDs don't — see docs/python-port-research.md §13.1).
- SIGTERM on stall, then SIGKILL after a short grace period. The Rust code
  only sends SIGTERM; in Python we add the grace+kill because asyncio
  doesn't always reap zombies cleanly otherwise.
- `kill()` is callable from another coroutine for `$stop` and shutdown.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from contextlib import suppress

from ..runner import RunnerEvent, RunnerSpec

_STREAM_DRAIN_SECS = 2.0
_WATCHDOG_POLL_SECS = 1.0


class _Heartbeat:
    """Tracks runner liveness for the stall watchdog.

    `last_line` is the monotonic time of the most recent stdout/stderr line.
    `_cmd_starts` maps an in-flight codex `command_execution` item id to the
    time it started, so the watchdog can extend its deadline while a tool
    call is genuinely running (the agent emits no output in that window).
    """

    def __init__(self, last_line: float) -> None:
        self.last_line = last_line
        self._cmd_starts: dict[str, float] = {}

    def observe(self, line: str) -> None:
        """Parse one codex JSON-stream line and track command_execution spans.

        Accepts both the canonical shape (`item.type == "command_execution"`)
        and the legacy shape that puts `item_type` on the item or the outer
        event — same fields `activity.parse_codex_activity_line` recognises.
        """
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            return
        if not isinstance(event, dict):
            return
        kind = event.get("type")
        if kind not in ("item.started", "item.completed"):
            return
        item = event.get("item") if isinstance(event.get("item"), dict) else {}
        item_type = item.get("type") or item.get("item_type") or event.get("item_type")
        if item_type != "command_execution":
            return
        item_id = item.get("id") or event.get("item_id") or event.get("id")
        if not isinstance(item_id, str):
            return
        if kind == "item.started":
            self._cmd_starts.setdefault(item_id, self.last_line)
        else:  # item.completed
            self._cmd_starts.pop(item_id, None)

    def deadline(self, now: float, stall_secs: float, command_secs: float) -> float:
        """Effective time by which fresh activity must have occurred.

        While at least one `command_execution` is in flight, `command_secs`
        is the hard outer cap on that single command — measured from its
        own start, not from `last_line`. The stall window only applies in
        the gaps between commands.
        """
        if self._cmd_starts:
            oldest = min(self._cmd_starts.values())
            return oldest + command_secs
        return self.last_line + stall_secs


class LocalRunner:
    """Runs agent CLIs as subprocesses on this host.

    One instance is shared across the process; per-run state lives in
    `_active` keyed by `run_id` so `kill(run_id)` can find the right
    process.
    """

    def __init__(self) -> None:
        self._active: dict[str, asyncio.subprocess.Process] = {}
        self._pending_kills: set[str] = set()

    async def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        env = {**os.environ, **spec.env}
        try:
            proc = await asyncio.create_subprocess_exec(
                *spec.command,
                cwd=spec.workspace_path,
                env=env,
                start_new_session=True,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
            )
        except (OSError, FileNotFoundError) as e:
            yield RunnerEvent(kind="spawn_failed", error=f"{type(e).__name__}: {e}")
            return

        self._active[spec.run_id] = proc
        if spec.run_id in self._pending_kills:
            self._pending_kills.discard(spec.run_id)
            with suppress(ProcessLookupError):
                _terminate_process_group(proc.pid)
        yield RunnerEvent(kind="started", pid=proc.pid)
        stalled = asyncio.Event()
        events: asyncio.Queue[RunnerEvent] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        hb = _Heartbeat(last_line=loop.time())

        async def pump(stream: asyncio.StreamReader | None, kind: str) -> None:
            if stream is None:
                return
            while True:
                try:
                    raw = await stream.readline()
                except Exception:  # noqa: BLE001 — pump must not crash the run
                    break
                if not raw:
                    break
                line = raw.decode(errors="replace").rstrip("\n")
                hb.last_line = loop.time()
                if kind == "stdout":
                    hb.observe(line)
                await events.put(RunnerEvent(kind=kind, line=line))  # type: ignore[arg-type]

        async def watchdog() -> None:
            # Poll-based: a run is "alive" if the agent printed a line within
            # `stall_secs`, OR it has a tool call in flight that started less
            # than `command_secs` ago. The second clause is what keeps a long
            # innocent subprocess (broad rg, pnpm install) from tripping the
            # stall — the agent emits no stdout while waiting on its own tool.
            while True:
                await asyncio.sleep(_WATCHDOG_POLL_SECS)
                if proc.returncode is not None:
                    return
                now = loop.time()
                deadline = hb.deadline(now, spec.stall_secs, spec.command_secs)
                if now < deadline:
                    continue
                # PID-based liveness: don't trust status fields here.
                if not _pid_alive(proc.pid):
                    return
                stalled.set()
                _terminate_process_group(proc.pid)
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except TimeoutError:
                    with suppress(ProcessLookupError):
                        _kill_process_group(proc.pid)
                    with suppress(Exception):
                        await proc.wait()
                return

        stdout_task = asyncio.create_task(pump(proc.stdout, "stdout"))
        stderr_task = asyncio.create_task(pump(proc.stderr, "stderr"))
        watch_task = asyncio.create_task(watchdog())
        wait_task = asyncio.create_task(proc.wait())
        drain_deadline: float | None = None
        cleaned_process_group = False

        try:
            while True:
                # Drain queued events; if process has exited and queue is empty, stop.
                try:
                    ev = await asyncio.wait_for(events.get(), timeout=0.25)
                except TimeoutError:
                    process_done = (
                        wait_task.done()
                        or proc.returncode is not None
                        or not _pid_alive(proc.pid)
                    )
                    if process_done:
                        # Flush tail output without letting inherited pipe handles
                        # keep the runner open after the parent process is gone.
                        if not cleaned_process_group:
                            with suppress(ProcessLookupError):
                                _terminate_process_group(proc.pid)
                            cleaned_process_group = True
                        if stdout_task.done() and stderr_task.done():
                            break
                        if drain_deadline is None:
                            drain_deadline = loop.time() + _STREAM_DRAIN_SECS
                        if loop.time() >= drain_deadline:
                            break
                        continue
                    drain_deadline = None
                    yield RunnerEvent(kind="tick")
                    continue
                yield ev

            for task in (stdout_task, stderr_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            if not wait_task.done():
                with suppress(TimeoutError):
                    await asyncio.wait_for(wait_task, timeout=0.25)
            if not wait_task.done():
                wait_task.cancel()
                with suppress(asyncio.CancelledError):
                    await wait_task
            watch_task.cancel()
            with suppress(asyncio.CancelledError):
                await watch_task

            if stalled.is_set():
                yield RunnerEvent(kind="stall_timeout")
            else:
                yield RunnerEvent(kind="exit", returncode=proc.returncode)
        finally:
            self._active.pop(spec.run_id, None)

    async def kill(self, run_id: str) -> None:
        proc = self._active.get(run_id)
        if proc is None:
            self._pending_kills.add(run_id)
            return
        if proc.returncode is not None:
            return
        with suppress(ProcessLookupError):
            _terminate_process_group(proc.pid)
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except TimeoutError:
            with suppress(ProcessLookupError):
                _kill_process_group(proc.pid)
            with suppress(Exception):
                await proc.wait()


def _pid_alive(pid: int | None) -> bool:
    """POSIX liveness check; returns False if the PID is unknown or zombie."""
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_process_group(pid: int | None) -> None:
    if pid is None:
        return
    os.killpg(pid, 15)


def _kill_process_group(pid: int | None) -> None:
    if pid is None:
        return
    os.killpg(pid, 9)
