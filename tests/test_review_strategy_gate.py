"""`_should_post_codex_review` — the gate that lets `local` mode skip the bot."""

from __future__ import annotations

from symphony.orchestrator.poll import _should_post_codex_review
from symphony.pipeline.local_review import LocalVerdict, LocalVerdictKind
from symphony.pipeline.local_review_loop import LoopOutcome, LoopResult


def _result(outcome: LoopOutcome) -> LoopResult:
    return LoopResult(
        outcome=outcome,
        iterations=1,
        verdicts=(
            LocalVerdict(
                kind=LocalVerdictKind.APPROVED
                if outcome == LoopOutcome.APPROVED
                else LocalVerdictKind.CHANGES_REQUESTED,
                trigger_signature="sig",
            ),
        ),
    )


# --- remote: always post ----------------------------------------------


def test_remote_strategy_always_posts_even_with_local_approval() -> None:
    # `remote` should not have a local result, but defend the contract.
    assert (
        _should_post_codex_review(
            review_strategy="remote",
            local_review_result=None,
        )
        is True
    )
    assert (
        _should_post_codex_review(
            review_strategy="remote",
            local_review_result=_result(LoopOutcome.APPROVED),
        )
        is True
    )


# --- hybrid: always post (defense in depth) ---------------------------


def test_hybrid_posts_after_local_approval() -> None:
    """Hybrid mode wants both checks. The local loop tightens the fix
    cycle; the remote bot is the second pair of eyes before merge."""
    assert (
        _should_post_codex_review(
            review_strategy="hybrid",
            local_review_result=_result(LoopOutcome.APPROVED),
        )
        is True
    )


def test_hybrid_posts_after_local_failure() -> None:
    assert (
        _should_post_codex_review(
            review_strategy="hybrid",
            local_review_result=_result(LoopOutcome.EXHAUSTED),
        )
        is True
    )


# --- local: skip when approved, post on every other outcome ----------


def test_local_skips_when_local_review_approved() -> None:
    assert (
        _should_post_codex_review(
            review_strategy="local",
            local_review_result=_result(LoopOutcome.APPROVED),
        )
        is False
    )


def test_local_falls_back_to_codex_on_exhausted() -> None:
    """If the local loop hit its cap without converging, fall back to
    the remote bot so the PR doesn't dead-end on an unverified state."""
    assert (
        _should_post_codex_review(
            review_strategy="local",
            local_review_result=_result(LoopOutcome.EXHAUSTED),
        )
        is True
    )


def test_local_falls_back_to_codex_on_stuck_loop() -> None:
    assert (
        _should_post_codex_review(
            review_strategy="local",
            local_review_result=_result(LoopOutcome.STUCK_LOOP),
        )
        is True
    )


def test_local_falls_back_to_codex_on_fix_run_failed() -> None:
    assert (
        _should_post_codex_review(
            review_strategy="local",
            local_review_result=_result(LoopOutcome.FIX_RUN_FAILED),
        )
        is True
    )


def test_local_falls_back_to_codex_on_reviewer_failed() -> None:
    """A crashed reviewer is the worst case — definitely don't dead-end."""
    assert (
        _should_post_codex_review(
            review_strategy="local",
            local_review_result=_result(LoopOutcome.REVIEWER_FAILED),
        )
        is True
    )


def test_local_falls_back_to_codex_when_operator_skipped() -> None:
    """`$skip-local-review` means "I don't trust the local pass on this
    one" — give them the remote bot's verdict instead of silently
    auto-approving."""
    assert (
        _should_post_codex_review(
            review_strategy="local",
            local_review_result=_result(LoopOutcome.SKIPPED),
        )
        is True
    )


def test_local_falls_back_to_codex_on_cost_cap_breach() -> None:
    """If the local loop tripped the cost cap, the remote bot is the
    safety net — it costs nothing extra (OpenAI-hosted) and gives the
    operator a verdict to act on."""
    assert (
        _should_post_codex_review(
            review_strategy="local",
            local_review_result=_result(LoopOutcome.COST_CAP_BREACHED),
        )
        is True
    )


def test_local_falls_back_to_codex_when_result_is_none() -> None:
    """An exception inside `_run_local_review_phase` returns None.
    That's a "we don't know" — keep the remote bot as the safety net."""
    assert (
        _should_post_codex_review(
            review_strategy="local",
            local_review_result=None,
        )
        is True
    )
