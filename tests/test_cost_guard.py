"""Pure cost-cap / cost-warning decision tests.

The cost guard is a pure function fed tick-by-tick with running totals.
These tests walk synthetic cost trajectories and verify:

  - the warning fires exactly once when the threshold is first crossed,
  - the breach signal fires on or above the cap,
  - per-binding overrides win over global defaults, and
  - a disabled cap (<= 0) suppresses breach signaling.
"""

from __future__ import annotations

from symphony.pipeline.cost_guard import (
    CostDecision,
    effective_cap,
    effective_warning_pct,
    estimate_codex_cost_usd,
    evaluate_cost,
)


def test_decision_below_threshold_is_quiet() -> None:
    d = evaluate_cost(
        previous_total=0.0,
        new_total=5.0,
        cap_usd=15.0,
        warning_pct=75,
        warning_already_fired=False,
    )
    assert d == CostDecision(fire_warning=False, cap_breached=False)


def test_warning_fires_when_threshold_first_crossed() -> None:
    d = evaluate_cost(
        previous_total=10.0,
        new_total=12.0,
        cap_usd=15.0,
        warning_pct=75,
        warning_already_fired=False,
    )
    assert d.fire_warning is True
    assert d.cap_breached is False


def test_warning_does_not_refire_once_emitted() -> None:
    d = evaluate_cost(
        previous_total=11.5,
        new_total=12.5,
        cap_usd=15.0,
        warning_pct=75,
        warning_already_fired=True,
    )
    assert d.fire_warning is False


def test_cap_breach_at_or_above_cap() -> None:
    at = evaluate_cost(
        previous_total=14.0,
        new_total=15.0,
        cap_usd=15.0,
        warning_pct=75,
        warning_already_fired=True,
    )
    assert at.cap_breached is True
    over = evaluate_cost(
        previous_total=14.0,
        new_total=20.0,
        cap_usd=15.0,
        warning_pct=75,
        warning_already_fired=True,
    )
    assert over.cap_breached is True


def test_unmarked_warning_retries_after_threshold() -> None:
    """If no warning mark was persisted, later ticks above the threshold
    should still ask the caller to post the warning. This lets transient
    comment failures retry instead of permanently suppressing the notice."""
    d = evaluate_cost(
        previous_total=12.0,
        new_total=12.5,
        cap_usd=15.0,
        warning_pct=75,
        warning_already_fired=False,
    )
    assert d.fire_warning is True


def test_warning_and_breach_can_fire_in_same_tick() -> None:
    d = evaluate_cost(
        previous_total=0.0,
        new_total=20.0,
        cap_usd=15.0,
        warning_pct=75,
        warning_already_fired=False,
    )
    assert d.fire_warning is True
    assert d.cap_breached is True


def test_cap_zero_or_negative_disables_breach() -> None:
    zero = evaluate_cost(
        previous_total=5.0,
        new_total=10.0,
        cap_usd=0.0,
        warning_pct=75,
        warning_already_fired=False,
    )
    assert zero.cap_breached is False
    neg = evaluate_cost(
        previous_total=5.0,
        new_total=10.0,
        cap_usd=-1.0,
        warning_pct=75,
        warning_already_fired=False,
    )
    assert neg.cap_breached is False


def test_synthetic_trajectory_only_warns_once() -> None:
    """Walk a sequence of (previous, new) totals and confirm the warning
    fires exactly once and the breach is detected at the right tick."""
    trajectory = [
        (0.0, 3.0),
        (3.0, 7.0),
        (7.0, 11.0),  # below 11.25 threshold
        (11.0, 12.0),  # crosses threshold
        (12.0, 13.5),
        (13.5, 14.9),
        (14.9, 15.5),  # cap breach
    ]
    fired = False
    warning_at: int | None = None
    breach_at: int | None = None
    for i, (prev, new) in enumerate(trajectory):
        d = evaluate_cost(
            previous_total=prev,
            new_total=new,
            cap_usd=15.0,
            warning_pct=75,
            warning_already_fired=fired,
        )
        if d.fire_warning:
            assert warning_at is None, "warning fired more than once"
            warning_at = i
            fired = True
        if d.cap_breached and breach_at is None:
            breach_at = i
    assert warning_at == 3
    assert breach_at == 6


def test_effective_cap_uses_binding_override_when_set() -> None:
    assert effective_cap(global_cap_usd=15.0, binding_override=None) == 15.0
    assert effective_cap(global_cap_usd=15.0, binding_override=25.0) == 25.0
    # An explicit zero must not be silently swapped for the global default.
    assert effective_cap(global_cap_usd=15.0, binding_override=0.0) == 0.0


def test_effective_warning_pct_uses_binding_override_when_set() -> None:
    assert effective_warning_pct(global_pct=75, binding_override=None) == 75
    assert effective_warning_pct(global_pct=75, binding_override=50) == 50


def test_estimates_codex_cost_from_tokens() -> None:
    cost = estimate_codex_cost_usd(
        input_tokens=1_000_000,
        cached_input_tokens=200_000,
        output_tokens=100_000,
    )
    assert round(cost, 6) == 2.025
