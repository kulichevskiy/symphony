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
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from ..credentials import RunCredentials
    from .control_channel import ControlHandler


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
    # Outer cap for a single agent tool call (command_execution). While the
    # agent has a command in flight the stall watchdog measures against this
    # instead of `stall_secs`, so a long-but-healthy subprocess (broad rg,
    # pnpm install, pytest) isn't killed as a false-positive stall.
    command_secs: float = 1800
    # Absolute wall-clock backstop measured from run start, independent of the
    # stall/per-command heartbeat. A confused-but-chatty agent keeps fresh
    # output flowing — so `stall_secs` never trips and there's no in-flight
    # command to hit `command_secs` — yet stays wedged indefinitely (incident
    # SYM-148). This cap kills it regardless. 0 disables (no cap).
    wall_clock_secs: float = 0
    stage: str = ""  # implement|review|merge — telemetry only
    # Credentials resolved DB-first (OAuth in UI 4/7). When set, the runner
    # materializes them into a private, torn-down home for the run — GH_TOKEN +
    # a git credential helper, and the Linear bearer — so a run drives off the
    # UI-stored connection instead of the ambient env/volume. `None` (the
    # default) leaves the run on whatever the inherited env already provides.
    credentials: RunCredentials | None = None
    # The GitHub host the materialized git credential store scopes to
    # (`credentials.github_token` otherwise; irrelevant when unset). Comes
    # from the binding's `[HOST/]OWNER/REPO` `github_repo` — a GHE binding's
    # host isn't `github.com`, and a credential store written for the wrong
    # host never matches on push (SYM-199 review fix).
    github_host: str = "github.com"
    # Control-channel mode (SYM-235). Setting `prompt` switches the run from a
    # monologue to a conversation: the prompt is delivered as a message on
    # stdin instead of riding in `command`, stdin stays open for the run, and
    # the agent's control requests are answered through `control_handler`.
    # Leaving it `None` keeps the one-directional shape every other spawn site
    # still uses — stdin is /dev/null and no control traffic is possible.
    prompt: str | None = None
    # Answers the agent's control requests, or returns None to refuse (which
    # ends the run rather than making the agent wait out its retry window).
    # The runner never inspects what travels through here, so it stays ignorant
    # of tokens; the dispenser is what plugs in.
    control_handler: ControlHandler | None = None


@dataclass
class RunnerEvent:
    kind: Literal[
        "started",
        "stdout",
        "stderr",
        "tick",
        "exit",
        "stall_timeout",
        "wall_clock_timeout",
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
