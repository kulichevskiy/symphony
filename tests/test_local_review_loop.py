"""Policy of the local-review loop, tested with injected fakes."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from symphony.pipeline.local_review import (
    VERDICT_APPROVED_MARKER,
    VERDICT_CHANGES_REQUESTED_MARKER,
    LocalVerdict,
)
from symphony.pipeline.local_review_loop import (
    FixerOutput,
    LoopOutcome,
    ReviewerOutput,
    run_local_review_loop,
)


def _codex_jsonl(text: str) -> str:
    """Minimal codex JSONL stream ending in an `agent_message`."""
    return json.dumps(
        {
            "type": "item.completed",
            "item": {"id": "i", "type": "agent_message", "text": text},
        }
    )


@dataclass
class _ReviewerScript:
    """Drive `reviewer(i)` deterministically.

    `messages[i]` is the final agent text. `head_shas[i]` is the SHA the
    reviewer "saw" — the fixer is expected to advance the head, so each
    iteration should normally see a different SHA.
    """

    messages: list[str]
    head_shas: list[str] = field(default_factory=list)
    fail_on: set[int] = field(default_factory=set)
    fail_by_call: set[int] = field(default_factory=set)
    message_by_call: bool = False
    calls: list[int] = field(default_factory=list)

    async def __call__(self, i: int) -> ReviewerOutput:
        self.calls.append(i)
        message_index = len(self.calls) - 1 if self.message_by_call else i
        if i in self.fail_on or len(self.calls) - 1 in self.fail_by_call:
            return ReviewerOutput(
                stdout="", head_sha="", ok=False, error="reviewer crashed"
            )
        return ReviewerOutput(
            stdout=_codex_jsonl(self.messages[message_index]),
            head_sha=(
                self.head_shas[message_index] if self.head_shas else f"sha{i}"
            ),
        )


@dataclass
class _FixerScript:
    fail_on: set[int] = field(default_factory=set)
    received: list[LocalVerdict] = field(default_factory=list)

    async def __call__(self, i: int, verdict: LocalVerdict) -> FixerOutput:
        self.received.append(verdict)
        if i in self.fail_on:
            return FixerOutput(ok=False, error="fix-run died")
        return FixerOutput(ok=True)


# --- happy paths -------------------------------------------------------


@pytest.mark.asyncio
async def test_first_review_approves_short_circuits_loop() -> None:
    reviewer = _ReviewerScript(messages=[f"all good\n{VERDICT_APPROVED_MARKER}"])
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert result.iterations == 1
    assert reviewer.calls == [0]
    assert fixer.received == []  # never paid for a fix-run


@pytest.mark.asyncio
async def test_fix_then_approve_runs_full_cycle() -> None:
    reviewer = _ReviewerScript(
        messages=[
            f"## Findings\n- bug A\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            f"now correct\n{VERDICT_APPROVED_MARKER}",
        ],
        head_shas=["sha-a", "sha-b"],  # fixer should advance the head
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert result.iterations == 2
    assert len(fixer.received) == 1
    assert "bug A" in fixer.received[0].findings


# --- exhaustion --------------------------------------------------------


@pytest.mark.asyncio
async def test_exhausts_when_cap_hit_with_distinct_findings_each_round() -> None:
    reviewer = _ReviewerScript(
        messages=[
            f"## Findings\n- bug 1\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            f"## Findings\n- bug 2\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            f"## Findings\n- bug 3\n{VERDICT_CHANGES_REQUESTED_MARKER}",
        ],
        head_shas=["a", "b", "c"],
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=3,
    )
    assert result.outcome == LoopOutcome.EXHAUSTED
    assert result.iterations == 3
    # Reviewer ran cap times; the fixer also ran cap times so the branch
    # carries the best-effort fix when an operator picks it up at the
    # Needs Approval escalation. The unverified-fixed state beats the
    # known-broken state for handoff.
    assert len(reviewer.calls) == 3
    assert len(fixer.received) == 3


@pytest.mark.asyncio
async def test_zero_cap_returns_exhausted_immediately() -> None:
    reviewer = _ReviewerScript(messages=[])
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=0,
    )
    assert result.outcome == LoopOutcome.EXHAUSTED
    assert result.iterations == 0
    assert reviewer.calls == []


# --- stuck-loop dedup --------------------------------------------------


@pytest.mark.asyncio
async def test_stuck_loop_when_reviewer_repeats_same_signature() -> None:
    """If the reviewer fixates on the same bug after a fix-run, escalate.

    Same findings → same findings signature; the dedup gate
    short-circuits rather than burning another fix-run on identical
    findings.
    """
    message = f"## Findings\n- bug X\n{VERDICT_CHANGES_REQUESTED_MARKER}"
    reviewer = _ReviewerScript(
        messages=[message, message],
        head_shas=["sha-stale", "sha-stale"],  # fixer somehow didn't bump head
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.STUCK_LOOP
    assert result.iterations == 2
    assert len(fixer.received) == 1  # only the first fix-run actually ran


@pytest.mark.asyncio
async def test_same_findings_but_new_head_sha_triggers_stuck() -> None:
    """Same unresolved findings after a fix commit are non-convergence."""
    message = f"## Findings\n- bug Y\n{VERDICT_CHANGES_REQUESTED_MARKER}"
    reviewer = _ReviewerScript(
        messages=[message, message],
        head_shas=["sha-1", "sha-2"],
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.STUCK_LOOP
    assert result.iterations == 2
    assert len(fixer.received) == 1
    assert result.verdicts[0].trigger_signature != result.verdicts[1].trigger_signature
    assert result.verdicts[0].findings_signature == result.verdicts[1].findings_signature


# --- failure modes ----------------------------------------------------


@pytest.mark.asyncio
async def test_reviewer_subprocess_failure_aborts_loop() -> None:
    reviewer = _ReviewerScript(messages=["unused"], fail_on={0})
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.iterations == 1
    assert result.error == "reviewer crashed"
    assert reviewer.calls == [0, 0]
    assert fixer.received == []


@pytest.mark.asyncio
async def test_reviewer_subprocess_failure_retried_once_then_approved() -> None:
    reviewer = _ReviewerScript(
        messages=[f"recovered\n{VERDICT_APPROVED_MARKER}"],
        fail_by_call={0},
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert result.iterations == 1
    assert reviewer.calls == [0, 0]
    assert fixer.received == []


@pytest.mark.asyncio
async def test_unparseable_review_treated_as_reviewer_failure() -> None:
    """No verdict marker → reviewer failed to follow instructions.

    Better to escalate than to silently approve or guess.
    """
    reviewer = _ReviewerScript(
        messages=["I have opinions but forgot the marker."]
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.iterations == 1
    assert result.error == "reviewer emitted no verdict marker"


@pytest.mark.asyncio
async def test_unparseable_review_retried_once_then_approved() -> None:
    reviewer = _ReviewerScript(
        messages=[
            "I have opinions but forgot the marker.",
            f"fixed on retry\n{VERDICT_APPROVED_MARKER}",
        ],
        head_shas=["same-head", "same-head"],
        message_by_call=True,
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert result.iterations == 1
    assert reviewer.calls == [0, 0]
    assert fixer.received == []
    assert result.last_verdict is not None
    assert result.last_verdict.kind.value == "approved"


@pytest.mark.asyncio
async def test_unparseable_review_retried_once_then_reviewer_failed() -> None:
    reviewer = _ReviewerScript(
        messages=[
            "I have opinions but forgot the marker.",
            "Still no marker.",
        ],
        head_shas=["same-head", "same-head"],
        message_by_call=True,
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.iterations == 1
    assert reviewer.calls == [0, 0]
    assert result.error == "reviewer emitted no verdict marker"
    assert fixer.received == []


@pytest.mark.asyncio
async def test_fix_run_failure_aborts_loop() -> None:
    reviewer = _ReviewerScript(
        messages=[
            f"## Findings\n- bug Z\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            "unused",
        ],
    )
    fixer = _FixerScript(fail_on={0})
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.FIX_RUN_FAILED
    assert result.iterations == 1
    assert result.error == "fix-run died"


# --- verdict bookkeeping ---------------------------------------------


@pytest.mark.asyncio
async def test_on_iteration_fires_once_per_verdict_in_order() -> None:
    reviewer = _ReviewerScript(
        messages=[
            f"## Findings\n- A\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            f"## Findings\n- B\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            f"ok\n{VERDICT_APPROVED_MARKER}",
        ],
        head_shas=["s1", "s2", "s3"],
    )
    fixer = _FixerScript()

    fired: list[tuple[int, str, float]] = []

    async def on_iter(i: int, verdict: LocalVerdict, cost: float) -> None:
        fired.append((i, verdict.kind.value, cost))

    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
        on_iteration=on_iter,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert [f[0] for f in fired] == [0, 1, 2]
    assert [f[1] for f in fired] == [
        "changes_requested",
        "changes_requested",
        "approved",
    ]


@pytest.mark.asyncio
async def test_on_iteration_exceptions_dont_break_loop() -> None:
    """A flaky Linear post must not kill the local-review pipeline."""
    reviewer = _ReviewerScript(
        messages=[f"good\n{VERDICT_APPROVED_MARKER}"],
        head_shas=["s1"],
    )
    fixer = _FixerScript()

    async def on_iter(i: int, verdict: LocalVerdict, cost: float) -> None:
        raise RuntimeError("linear is on fire")

    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
        on_iteration=on_iter,
    )
    assert result.outcome == LoopOutcome.APPROVED


@pytest.mark.asyncio
async def test_reviewer_failure_classified_as_reviewer_failed() -> None:
    reviewer = _ReviewerScript(
        messages=["unused"],
        fail_on={0},
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED


@pytest.mark.asyncio
async def test_verdicts_collected_in_order() -> None:
    reviewer = _ReviewerScript(
        messages=[
            f"## Findings\n- A\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            f"## Findings\n- B\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            f"ok\n{VERDICT_APPROVED_MARKER}",
        ],
        head_shas=["s1", "s2", "s3"],
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert len(result.verdicts) == 3
    assert result.last_verdict is not None
    assert result.last_verdict.kind.value == "approved"
