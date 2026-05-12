"""Pure transition decisions for the pipeline state machine.

The state machine is a pure function: given the current stage and the
terminal runner event (exit / stall_timeout / spawn_failed), return the
next run status, the Linear state to move the issue to (if any), and
whether the pipeline halts here.

The state machine does not move Linear itself; the orchestrator performs
stage handoff side effects after a successful runner transition.
"""

from __future__ import annotations

import pytest

from symphony.pipeline.state_machine import Transition, on_runner_event


@pytest.mark.parametrize(
    "stage,event_kind,returncode,expected",
    [
        # Implement clean exit completes the runner-owned part of the stage.
        (
            "implement",
            "exit",
            0,
            Transition(next_run_status="completed", next_linear_state=None, halt=True),
        ),
        # Non-zero exit is a hard failure regardless of stage.
        (
            "implement",
            "exit",
            2,
            Transition(next_run_status="failed", next_linear_state=None, halt=True),
        ),
        # Stall timeout — watchdog killed the process.
        (
            "implement",
            "stall_timeout",
            None,
            Transition(next_run_status="failed", next_linear_state=None, halt=True),
        ),
        # Spawn failed (binary missing, ENOENT, etc).
        (
            "implement",
            "spawn_failed",
            None,
            Transition(next_run_status="failed", next_linear_state=None, halt=True),
        ),
    ],
)
def test_on_runner_event_implement_transitions(
    stage: str, event_kind: str, returncode: int | None, expected: Transition
) -> None:
    assert on_runner_event(stage=stage, event_kind=event_kind, returncode=returncode) == expected


def test_transition_is_frozen_dataclass() -> None:
    from dataclasses import FrozenInstanceError

    t = Transition(next_run_status="completed", next_linear_state=None, halt=True)
    with pytest.raises(FrozenInstanceError):
        t.next_run_status = "failed"  # type: ignore[misc]
