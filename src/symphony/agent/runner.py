"""Runner protocol — abstracts execution venue.

The orchestrator handles the pipeline state machine; runners handle one
agent run end-to-end. Splitting at this seam lets us add `E2BRunner` /
`DaytonaRunner` later (docs §15) without restructuring the pipeline.

Events are streamed back as an async iterator rather than collected into a
list because long-running stages (Implement can take 10+ minutes) need live
progress so we can post hourly Linear updates and notice stalls.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol


@dataclass
class RunnerSpec:
    """What the orchestrator hands a runner."""

    run_id: str
    workspace_path: (
        Path  # already-cloned dir on disk for LocalRunner; descriptor for sandbox runners
    )
    command: list[str]  # ["claude", "--print", "--output-format", "stream-json", ...]
    env: dict[str, str] = field(default_factory=dict)
    stall_secs: float = 300
    stage: str = ""  # implement|review|merge — telemetry only


@dataclass
class RunnerEvent:
    kind: Literal[
        "started",
        "stdout",
        "stderr",
        "tick",
        "exit",
        "stall_timeout",
        "spawn_failed",
    ]
    line: str | None = None
    returncode: int | None = None
    error: str | None = None
    pid: int | None = None


class Runner(Protocol):
    """One agent run, abstracted over execution venue.

    Lifecycle:
    - `run(spec)` returns an async iterator. The runner spawns the
      subprocess (or sandbox), yields one `RunnerEvent` per stdout/stderr
      line, then yields a single terminal event (`exit` | `stall_timeout` |
      `spawn_failed`) and stops.
    - `kill(run_id)` is a best-effort terminator for the case where the
      orchestrator decides to abort mid-run (user `$stop`, shutdown).
    """

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]: ...

    async def kill(self, run_id: str) -> None: ...
