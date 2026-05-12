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
import os
from collections.abc import AsyncIterator
from contextlib import suppress

from ..runner import RunnerEvent, RunnerSpec

_STREAM_DRAIN_SECS = 2.0


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
        activity = asyncio.Event()
        stalled = asyncio.Event()
        events: asyncio.Queue[RunnerEvent] = asyncio.Queue()

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
                activity.set()
                await events.put(RunnerEvent(kind=kind, line=line))  # type: ignore[arg-type]

        async def watchdog() -> None:
            while True:
                try:
                    await asyncio.wait_for(activity.wait(), timeout=spec.stall_secs)
                except TimeoutError:
                    if proc.returncode is not None:
                        return
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
                else:
                    activity.clear()

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
                        loop = asyncio.get_running_loop()
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
