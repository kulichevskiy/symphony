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

from symphony.pipeline.state_machine import (
    Transition,
    classify_termination,
    on_runner_event,
)


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


@pytest.mark.parametrize(
    "kwargs,expected_kind,detail_part",
    [
        (
            {"status": "failed", "final_kind": "exit", "returncode": 2},
            "agent_nonzero_exit",
            "return code 2",
        ),
        (
            {"status": "failed", "final_kind": "stall_timeout"},
            "stall_timeout",
            "stall_timeout",
        ),
        (
            {"status": "failed", "final_kind": "spawn_failed", "reason": "ENOENT"},
            "spawn_failed",
            "ENOENT",
        ),
        (
            {"status": "failed", "final_kind": "cost_cap", "cap_breached": True},
            "cost_cap",
            "cost cap",
        ),
        (
            {"status": "failed", "exc": RuntimeError("agent stream exploded")},
            "execution_error",
            "agent stream exploded",
        ),
        (
            {"status": "failed", "reason": "fix-run completed without advancing branch"},
            "validation_failed",
            "without advancing",
        ),
        (
            {"status": "failed", "reason": "push failed: rejected"},
            "push_failed",
            "rejected",
        ),
        (
            {"status": "failed", "reason": "rebase --continue failed: conflicts"},
            "rebase_failed",
            "conflicts",
        ),
        (
            {"status": "failed", "reason": "move_issue failed: 500"},
            "tracker_error",
            "move_issue",
        ),
        (
            {"status": "interrupted", "reason": "dispatch cancelled"},
            "cancelled",
            "cancelled",
        ),
        (
            {"status": "interrupted", "reason": "Host restarted; pid 999 died"},
            "orphaned",
            "Host restarted",
        ),
        (
            {"status": "interrupted", "reason": "superseded by newer PR"},
            "superseded",
            "superseded",
        ),
        (
            {"status": "needs_approval", "reason": "manual merge required"},
            "awaiting_human_merge",
            "manual merge",
        ),
        (
            {
                "status": "needs_approval",
                "cap_breached": True,
                "reason": "cost cap reached: $1.2500",
            },
            "cost_cap",
            "cost cap",
        ),
        (
            {
                "status": "needs_approval",
                "final_kind": "stall_timeout",
                "reason": "merge runner ended with stall_timeout",
            },
            "stall_timeout",
            "stall_timeout",
        ),
        (
            {
                "status": "needs_approval",
                "final_kind": "spawn_failed",
                "reason": "merge runner ended with spawn_failed",
            },
            "spawn_failed",
            "spawn_failed",
        ),
        (
            {"status": "needs_approval", "final_kind": "exit", "returncode": 2},
            "agent_nonzero_exit",
            "return code 2",
        ),
        (
            {"status": "failed", "reason": ""},
            "unknown",
            "failed",
        ),
    ],
)
def test_classify_termination_covers_terminal_enums(
    kwargs: dict[str, object], expected_kind: str, detail_part: str
) -> None:
    kind, detail = classify_termination(**kwargs)
    assert kind == expected_kind
    assert detail_part in detail


def test_classify_termination_success_is_empty() -> None:
    assert classify_termination(status="completed") == ("", "")
