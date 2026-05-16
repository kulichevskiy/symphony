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
    calls: list[int] = field(default_factory=list)

    async def __call__(self, i: int) -> ReviewerOutput:
        self.calls.append(i)
        if i in self.fail_on:
            return ReviewerOutput(
                stdout="", head_sha="", ok=False, error="reviewer crashed"
            )
        return ReviewerOutput(
            stdout=_codex_jsonl(self.messages[i]),
            head_sha=self.head_shas[i] if self.head_shas else f"sha{i}",
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

    Same head_sha + same findings → same signature; the dedup gate
    short-circuits rather than burning another fix-run on the identical
    trigger.
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
async def test_same_findings_but_new_head_sha_does_not_trigger_stuck() -> None:
    """If the fix-run advanced HEAD, the signature changes — loop continues."""
    message = f"## Findings\n- bug Y\n{VERDICT_CHANGES_REQUESTED_MARKER}"
    reviewer = _ReviewerScript(
        messages=[message, message, f"good\n{VERDICT_APPROVED_MARKER}"],
        head_shas=["sha-1", "sha-2", "sha-3"],
    )
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert result.iterations == 3
    assert len(fixer.received) == 2


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
async def test_skip_predicate_exits_before_first_review() -> None:
    """An operator who flips the skip flag before the loop starts gets
    zero subprocess calls — the predicate is checked at the iteration
    boundary, including iteration 0."""
    reviewer = _ReviewerScript(messages=[f"unused\n{VERDICT_APPROVED_MARKER}"])
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
        should_skip=lambda: True,
    )
    assert result.outcome == LoopOutcome.SKIPPED
    assert result.iterations == 0
    assert reviewer.calls == []
    assert fixer.received == []
    assert "$skip-local-review" in (result.error or "")


@pytest.mark.asyncio
async def test_skip_predicate_exits_between_iterations() -> None:
    """Flag flips after the first round; the loop exits before the
    second reviewer call."""
    skip_flag = {"flipped": False}

    def should_skip() -> bool:
        return skip_flag["flipped"]

    reviewer = _ReviewerScript(
        messages=[
            f"## Findings\n- A\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            "unused",
        ],
        head_shas=["s1", "s2"],
    )

    @dataclass
    class _FlippingFixer:
        received: list[LocalVerdict] = field(default_factory=list)

        async def __call__(self, i: int, verdict: LocalVerdict) -> FixerOutput:
            self.received.append(verdict)
            # Operator posts `$skip-local-review` while the fix-run is
            # finishing. Simulate that by flipping the flag here.
            skip_flag["flipped"] = True
            return FixerOutput(ok=True)

    fixer = _FlippingFixer()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
        should_skip=should_skip,
    )
    assert result.outcome == LoopOutcome.SKIPPED
    assert result.iterations == 1
    assert len(reviewer.calls) == 1
    assert len(fixer.received) == 1


@pytest.mark.asyncio
async def test_skip_predicate_returning_false_does_not_short_circuit() -> None:
    """The predicate is called every iteration; if it returns False the
    loop continues normally."""
    reviewer = _ReviewerScript(messages=[f"good\n{VERDICT_APPROVED_MARKER}"])
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
        should_skip=lambda: False,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert result.iterations == 1


@pytest.mark.asyncio
async def test_killed_reviewer_classified_as_skipped_not_reviewer_failed() -> None:
    """When the slash handler kills the reviewer subprocess, the runner
    reports a failure terminal. The skip flag set BEFORE the kill must
    take priority so the audit trail shows operator intent."""
    skip_flag = {"set": False}

    @dataclass
    class _KilledReviewer:
        calls: list[int] = field(default_factory=list)

        async def __call__(self, i: int) -> ReviewerOutput:
            self.calls.append(i)
            # Simulate the orchestrator setting the skip flag then
            # killing the subprocess. From the reviewer's perspective
            # the subprocess failed.
            skip_flag["set"] = True
            return ReviewerOutput(
                stdout="", head_sha="sha-1", ok=False, error="killed"
            )

    reviewer = _KilledReviewer()
    fixer = _FixerScript()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
        should_skip=lambda: skip_flag["set"],
    )
    assert result.outcome == LoopOutcome.SKIPPED
    # The reviewer was called once (and "killed"); no fix-run dispatched.
    assert len(reviewer.calls) == 1
    assert len(fixer.received) == 0


@pytest.mark.asyncio
async def test_killed_fixer_classified_as_skipped_not_fix_run_failed() -> None:
    """Same priority on the fixer side: a killed fix-run with the skip
    flag set surfaces as SKIPPED, not FIX_RUN_FAILED."""
    skip_flag = {"set": False}

    reviewer = _ReviewerScript(
        messages=[
            f"## Findings\n- bug\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            "unused",
        ],
        head_shas=["s1", "s2"],
    )

    @dataclass
    class _KilledFixer:
        received: list[LocalVerdict] = field(default_factory=list)

        async def __call__(self, i: int, verdict: LocalVerdict) -> FixerOutput:
            self.received.append(verdict)
            skip_flag["set"] = True
            return FixerOutput(ok=False, error="killed")

    fixer = _KilledFixer()
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
        should_skip=lambda: skip_flag["set"],
    )
    assert result.outcome == LoopOutcome.SKIPPED
    assert len(fixer.received) == 1


@pytest.mark.asyncio
async def test_reviewer_failure_without_skip_still_classified_as_reviewer_failed() -> None:
    """Sanity: priority logic doesn't accidentally convert all failures
    to SKIPPED — only those that coincide with an operator skip."""
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
        should_skip=lambda: False,
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
