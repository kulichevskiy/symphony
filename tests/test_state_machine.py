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
    ImplementCompletion,
    Transition,
    classify_blocked_final_message,
    classify_implement_completion,
    classify_termination,
    on_runner_event,
    parse_completion_marker,
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


# --- Completion gate: SYMPHONY_DONE / SYMPHONY_BLOCKED marker + HEAD advance ---

# Verbatim final message from MCH-14: the agent ended its turn politely
# blocked on a human OAuth action. Today it exits rc=0 and is mislabeled
# `completed`; the gate must classify it `blocked`.
MCH14_FINAL_MESSAGE = (
    "I need you to authorize the Supabase MCP server before I can continue. "
    "Please open the following URL and approve access, then let me know once "
    "you've done so and I'll resume the implementation."
)


def test_parse_completion_marker_done() -> None:
    marker = parse_completion_marker("All set.\n\nSYMPHONY_DONE")
    assert marker.kind == "done"
    assert marker.blocked_reason == ""


def test_parse_completion_marker_blocked_captures_reason_verbatim() -> None:
    reason = "authorize the Supabase MCP server at https://example.com/oauth"
    marker = parse_completion_marker(f"Did some work.\n\nSYMPHONY_BLOCKED: {reason}")
    assert marker.kind == "blocked"
    assert marker.blocked_reason == reason


def test_parse_completion_marker_absent() -> None:
    assert parse_completion_marker("No marker here at all.").kind is None


def test_parse_completion_marker_last_marker_wins() -> None:
    # The agent may quote the contract from the prompt earlier; the operative
    # marker is the final one.
    text = "Contract: emit SYMPHONY_DONE or SYMPHONY_BLOCKED: x\n\nSYMPHONY_DONE"
    assert parse_completion_marker(text).kind == "done"


def test_parse_completion_marker_blocked_last_wins_over_quoted_body() -> None:
    # Regression: greedy DOTALL regex would match the first SYMPHONY_BLOCKED:
    # occurrence and swallow everything including the operative one.
    text = (
        "...I will emit SYMPHONY_BLOCKED: <exactly what a human must do> if needed.\n\n"
        "SYMPHONY_BLOCKED: authorize the OAuth URL at https://x"
    )
    marker = parse_completion_marker(text)
    assert marker.kind == "blocked"
    assert marker.blocked_reason == "authorize the OAuth URL at https://x"


def test_parse_completion_marker_done_after_blocked_in_body() -> None:
    # "Work committed. SYMPHONY_DONE — also note: SYMPHONY_BLOCKED: nothing really"
    # The BLOCKED is on the same line AFTER the DONE text, but operative marker
    # is the BLOCKED on its own line only if it appears after DONE's line.
    # Here DONE is on its own line first, BLOCKED is quoted inline — DONE wins.
    text = "Work committed.\n\nSYMPHONY_DONE\n\nalso note: SYMPHONY_BLOCKED: nothing really"
    # BLOCKED is NOT on its own line (it's preceded by "also note: "), so only
    # the anchored DONE matches → kind == "done".
    marker = parse_completion_marker(text)
    assert marker.kind == "done"


# Path 1: SYMPHONY_DONE + HEAD advanced -> completed (today's happy path).
def test_classify_done_marker_with_head_advance_completes() -> None:
    spy: list[str] = []
    completion = classify_implement_completion(
        final_message="Implemented.\n\nSYMPHONY_DONE",
        head_advanced=True,
        classifier=lambda m: (spy.append(m), ("ambiguous", ""))[1],
    )
    assert completion == ImplementCompletion(outcome="completed", blocked_reason="")
    assert spy == []  # classifier must NOT run when a marker is present


# Path 2: SYMPHONY_BLOCKED -> blocked, reason captured verbatim, no classifier.
def test_classify_blocked_marker_captures_reason_verbatim() -> None:
    spy: list[str] = []
    reason = "authorize the Supabase MCP server, then reply $continue"
    completion = classify_implement_completion(
        final_message=f"SYMPHONY_BLOCKED: {reason}",
        head_advanced=False,
        classifier=lambda m: (spy.append(m), ("ambiguous", ""))[1],
    )
    assert completion.outcome == "blocked"
    assert completion.blocked_reason == reason
    assert spy == []


# A blocked marker wins even when HEAD advanced (partial work then blocked).
def test_classify_blocked_marker_wins_over_head_advance() -> None:
    completion = classify_implement_completion(
        final_message="SYMPHONY_BLOCKED: need a secret from you",
        head_advanced=True,
    )
    assert completion.outcome == "blocked"
    assert completion.blocked_reason == "need a secret from you"


# Path 3: no marker AND no commits -> classifier fallback -> blocked (MCH-14).
def test_classify_no_marker_no_commits_runs_classifier_blocked() -> None:
    completion = classify_implement_completion(
        final_message=MCH14_FINAL_MESSAGE,
        head_advanced=False,
    )
    assert completion.outcome == "blocked"
    assert "authorize" in completion.blocked_reason.lower()


# Path 4: no marker, no commits, ambiguous message -> failed.
def test_classify_no_marker_no_commits_ambiguous_fails() -> None:
    completion = classify_implement_completion(
        final_message="I looked around the repo and read some files.",
        head_advanced=False,
    )
    assert completion.outcome == "failed"


# Acceptance: a _DONE_SIGNALS phrase (classifier "done" verdict) in an
# unmarked, zero-commit final message must NOT classify as completed — same
# criterion as the SYMPHONY_DONE-without-commits branch, via the fallback path.
def test_classify_no_marker_no_commits_done_verdict_not_completed() -> None:
    completion = classify_implement_completion(
        final_message="Nothing to change.",
        head_advanced=False,
    )
    assert completion.outcome != "completed"


# Acceptance: rc=0 without commits and without SYMPHONY_DONE never completes.
def test_classify_done_marker_without_commits_not_completed() -> None:
    completion = classify_implement_completion(
        final_message="Nothing to change.\n\nSYMPHONY_DONE",
        head_advanced=False,
    )
    assert completion.outcome != "completed"


# Commits but a forgotten marker: ground-truth commits complete it; the
# classifier fallback must NOT run because HEAD advanced.
def test_classify_no_marker_with_commits_completes_without_classifier() -> None:
    spy: list[str] = []
    completion = classify_implement_completion(
        final_message="Forgot the marker but I committed the fix.",
        head_advanced=True,
        classifier=lambda m: (spy.append(m), ("ambiguous", ""))[1],
    )
    assert completion.outcome == "completed"
    assert spy == []


# --- already_satisfied (no-op done: scope delivered elsewhere) --------------


def test_parse_completion_marker_already_done_captures_ref_verbatim() -> None:
    ref = "f483299 (adjust/adjust_os#291)"
    marker = parse_completion_marker(f"Scope already landed.\n\nSYMPHONY_ALREADY_DONE: {ref}")
    assert marker.kind == "already_done"
    assert marker.already_done_ref == ref


def test_parse_completion_marker_already_done_does_not_match_plain_done() -> None:
    # SYMPHONY_ALREADY_DONE must not be mistaken for the plain SYMPHONY_DONE
    # marker (substring collision guard).
    marker = parse_completion_marker("SYMPHONY_ALREADY_DONE: abc1234")
    assert marker.kind == "already_done"


def test_parse_completion_marker_last_marker_wins_already_over_done() -> None:
    text = "SYMPHONY_DONE quoted in prompt\n\nSYMPHONY_ALREADY_DONE: abc1234"
    assert parse_completion_marker(text).kind == "already_done"


# Already-done marker + no HEAD advance -> distinct already_satisfied outcome,
# NOT failed. This is the whole point of the issue: a no-op implement whose
# scope already landed elsewhere should close as done, not halt on an operator.
def test_classify_already_done_marker_no_head_advance_is_already_satisfied() -> None:
    completion = classify_implement_completion(
        final_message="Verified all criteria.\n\nSYMPHONY_ALREADY_DONE: f483299",
        head_advanced=False,
    )
    assert completion.outcome == "already_satisfied"
    assert completion.already_satisfied_ref == "f483299"


# Ground truth wins: if the agent both claims already-done AND committed,
# real commits make it a normal completion.
def test_classify_already_done_marker_with_head_advance_completes() -> None:
    completion = classify_implement_completion(
        final_message="SYMPHONY_ALREADY_DONE: f483299",
        head_advanced=True,
    )
    assert completion.outcome == "completed"


# Guard preserved: a plain SYMPHONY_DONE with no commits and no explicit
# already-done signal must STILL be failed, never already_satisfied.
def test_classify_plain_done_no_commits_is_not_already_satisfied() -> None:
    completion = classify_implement_completion(
        final_message="Everything already satisfied.\n\nSYMPHONY_DONE",
        head_advanced=False,
    )
    assert completion.outcome == "failed"


# --- re-confirmed branch (SYM-161): conservative re-run convergence ---------
# A killed-then-redispatched implement re-runs the agent; it re-confirms the
# work (SYMPHONY_DONE) but makes no new commit (HEAD not advanced). When the
# branch already carries deliverable commits (ahead of base) on a clean tree,
# that confirmation must complete the run instead of re-parking it.
def test_classify_done_not_advanced_branch_ahead_clean_completes() -> None:
    completion = classify_implement_completion(
        final_message="Re-confirmed: 216 tests pass.\n\nSYMPHONY_DONE",
        head_advanced=False,
        branch_ahead_of_base=True,
        tree_clean=True,
    )
    assert completion.outcome == "completed"


# No-op guard preserved: branch NOT ahead -> still failed (claimed done,
# produced nothing on the branch).
def test_classify_done_not_advanced_branch_not_ahead_fails() -> None:
    completion = classify_implement_completion(
        final_message="SYMPHONY_DONE",
        head_advanced=False,
        branch_ahead_of_base=False,
        tree_clean=True,
    )
    assert completion.outcome == "failed"


# Dirty tree owns the dirty case: branch ahead but tree dirty -> still failed.
def test_classify_done_not_advanced_branch_ahead_dirty_fails() -> None:
    completion = classify_implement_completion(
        final_message="SYMPHONY_DONE",
        head_advanced=False,
        branch_ahead_of_base=True,
        tree_clean=False,
    )
    assert completion.outcome == "failed"


def test_classify_blocked_final_message_detects_human_action_ask() -> None:
    kind, reason = classify_blocked_final_message(MCH14_FINAL_MESSAGE)
    assert kind == "blocked"
    assert reason.strip() != ""


def test_classify_blocked_final_message_ambiguous() -> None:
    kind, _ = classify_blocked_final_message("Explored the code paths.")
    assert kind == "ambiguous"
