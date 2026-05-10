"""Pure transition decisions for the pipeline state machine.

Given the current stage and the terminal runner event, return the next
run status, the Linear state to move the issue to (if any), and whether
the pipeline halts here.

Issue #7 only wires Implement; on success it halts at "In Progress"
because Review and Merge land in their own slices. The signature
(`stage` is a parameter) is shaped so Review / Merge transitions slot in
later without changing call sites.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

RunStatus = Literal["running", "completed", "failed"]


@dataclass(frozen=True)
class Transition:
    next_run_status: RunStatus
    next_linear_state: str | None
    halt: bool


def on_runner_event(
    *, stage: str, event_kind: str, returncode: int | None
) -> Transition:
    if event_kind == "exit" and returncode == 0:
        if stage == "implement":
            # Implement halts at "In Progress" for this slice; Review and
            # Merge will hand back a non-halting transition later.
            return Transition(
                next_run_status="completed", next_linear_state=None, halt=True
            )
        return Transition(
            next_run_status="completed", next_linear_state=None, halt=True
        )
    return Transition(next_run_status="failed", next_linear_state=None, halt=True)
