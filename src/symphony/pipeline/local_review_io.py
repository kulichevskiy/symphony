"""Adapter between the `Runner` protocol and the local-review parser.

The Runner protocol streams `RunnerEvent`s (stdout / stderr / tick / exit
/ stall_timeout / spawn_failed) but the local-review parser wants a
single stdout *string*. This module sits between them.

Kept separate from `local_review_loop` because the loop is pure policy
(no I/O, no runner), and kept separate from the orchestrator because the
orchestrator already drowns in 5k+ lines. Three small modules beat one
branching one.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from ..agent.process import Usage, parse_event_line
from ..agent.runner import Runner, RunnerSpec

# stderr lines are teed with this prefix so the JSONL stream parser (which
# ignores non-JSON lines) skips them, matching the orchestrator's
# implement-run tee in `poll/_base.py`.
_STDERR_PREFIX = "[stderr] "


def open_run_log(log_path: Path | None) -> IO[str] | None:
    """Open `log_path` for append-teeing, or return None when unset.

    Parent dirs are created on demand — same lazy `mkdir` the orchestrator
    does before an implement run's tee.
    """
    if log_path is None:
        return None
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return log_path.open("a", encoding="utf-8")


def tee_run_log(logf: IO[str] | None, line: str, *, stderr: bool = False) -> None:
    """Append one line to the run log, flushing so `tail -f` sees it live."""
    if logf is None:
        return
    logf.write(f"{_STDERR_PREFIX}{line}\n" if stderr else f"{line}\n")
    logf.flush()


@dataclass(frozen=True)
class CollectedRunnerOutput:
    """Single-pass collection of a runner invocation.

    `terminal_kind` is the kind of the terminal event (one of
    `exit | stall_timeout | wall_clock_timeout | spawn_failed`). The caller decides what
    counts as "ok" — for the reviewer, any terminal that produced a
    parseable verdict is fine; the loop's UNPARSEABLE fallback handles
    short-circuited runs.
    """

    stdout: str
    stderr: str
    terminal_kind: str
    returncode: int | None
    spawn_error: str | None
    stall_timeout: bool

    @property
    def ok_exit(self) -> bool:
        return self.terminal_kind == "exit" and self.returncode == 0


async def collect_runner_output(
    runner: Runner,
    spec: RunnerSpec,
    *,
    usage_handler: Callable[[Usage], object] | None = None,
    log_path: Path | None = None,
) -> CollectedRunnerOutput:
    """Drive a `Runner.run(spec)` to completion and collect its output.

    All stdout lines are concatenated (preserving order) so the
    downstream `parse_local_review_output` can scan for JSONL events.
    Stderr is captured too for diagnostic messages; we do not depend on
    it for verdict classification.

    `usage_handler`, when supplied, is invoked synchronously on every
    stdout line that parses into a `Usage` event (claude `result`,
    codex `token_count` / `turn.completed`). Callers feed this into a
    `UsageCostEstimator` to bill the local-review subprocess against
    the issue's cumulative cost cap. The handler's return value is
    ignored — typed as `object` so callers can pass methods that
    return a useful value (e.g. `UsageCostEstimator.delta` returns the
    cost delta) without an adapter.

    `log_path`, when set, receives every line as it arrives (stdout
    verbatim, stderr prefixed `[stderr] `) so the run's
    `{log_root}/{run_id}.log` grows in real time — the same tee the
    orchestrator does for implement runs. Strictly additive: the
    in-memory collection returned here is unchanged.
    """
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    terminal_kind = "exit"
    returncode: int | None = None
    spawn_error: str | None = None
    stall_timeout = False

    logf = open_run_log(log_path)
    try:
        async for event in runner.run(spec):
            if event.kind == "stdout" and event.line is not None:
                stdout_parts.append(event.line)
                tee_run_log(logf, event.line)
                if usage_handler is not None:
                    usage = parse_event_line(event.line)
                    if usage is not None:
                        usage_handler(usage)
            elif event.kind == "stderr" and event.line is not None:
                stderr_parts.append(event.line)
                tee_run_log(logf, event.line, stderr=True)
            elif event.kind == "exit":
                terminal_kind = "exit"
                returncode = event.returncode
                break
            elif event.kind in ("stall_timeout", "wall_clock_timeout"):
                # Both are watchdog kills (silence vs. absolute wall-clock cap);
                # reuse the `stall_timeout` flag so the run fails closed.
                terminal_kind = event.kind
                stall_timeout = True
                break
            elif event.kind == "spawn_failed":
                terminal_kind = "spawn_failed"
                spawn_error = event.error
                break
    finally:
        if logf is not None:
            logf.close()

    return CollectedRunnerOutput(
        stdout="\n".join(stdout_parts),
        stderr="\n".join(stderr_parts),
        terminal_kind=terminal_kind,
        returncode=returncode,
        spawn_error=spawn_error,
        stall_timeout=stall_timeout,
    )


__all__ = [
    "CollectedRunnerOutput",
    "collect_runner_output",
    "open_run_log",
    "tee_run_log",
]
