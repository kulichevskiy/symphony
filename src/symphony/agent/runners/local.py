"""LocalRunner — subprocess on the orchestrator host.

Mirrors the load-bearing parts of the Rust `agent/process.rs`:

- Separate stdout / stderr pumps that notify a stall watchdog on every line.
- PID-based liveness for the watchdog (status fields lie during fix-runs;
  PIDs don't — see docs/python-port-research.md §13.1).
- SIGTERM on stall, then SIGKILL after a short grace period. The Rust code
  only sends SIGTERM; in Python we add the grace+kill because asyncio
  doesn't always reap zombies cleanly otherwise.
- `kill()` is callable from another coroutine for `/stop` and shutdown.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import suppress

from ..runner import RunnerEvent, RunnerSpec


class LocalRunner:
    """Runs agent CLIs as subprocesses on this host.

    One instance is shared across the process; per-run state lives in
    `_active` keyed by `run_id` so `kill(run_id)` can find the right
    process.
    """

    def __init__(self) -> None:
        self._active: dict[str, asyncio.subprocess.Process] = {}

    async def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        env = {**os.environ, **spec.env}
        try:
            proc = await asyncio.create_subprocess_exec(
                *spec.command,
                cwd=spec.workspace_path,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
            )
        except (OSError, FileNotFoundError) as e:
            yield RunnerEvent(kind="spawn_failed", error=f"{type(e).__name__}: {e}")
            return

        self._active[spec.run_id] = proc
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
                    proc.terminate()
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=5.0)
                    except TimeoutError:
                        with suppress(ProcessLookupError):
                            proc.kill()
                    return
                else:
                    activity.clear()

        stdout_task = asyncio.create_task(pump(proc.stdout, "stdout"))
        stderr_task = asyncio.create_task(pump(proc.stderr, "stderr"))
        watch_task = asyncio.create_task(watchdog())
        wait_task = asyncio.create_task(proc.wait())

        try:
            while True:
                # Drain queued events; if process has exited and queue is empty, stop.
                try:
                    ev = await asyncio.wait_for(events.get(), timeout=0.25)
                except TimeoutError:
                    if wait_task.done() and stdout_task.done() and stderr_task.done():
                        break
                    continue
                yield ev

            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
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
        if proc is None or proc.returncode is not None:
            return
        with suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except TimeoutError:
            with suppress(ProcessLookupError):
                proc.kill()


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
