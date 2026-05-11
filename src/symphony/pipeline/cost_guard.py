"""Pure cost-cap / cost-warning decisions.

The orchestrator accumulates per-issue cumulative cost as the runner
emits stream-json events. After each tick it asks `evaluate_cost`
whether to fire the once-per-issue warning and whether the per-issue
cap has been breached. Keeping the decision pure makes synthetic
trajectories easy to test and lets the orchestrator stay focused on I/O.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..agent.codex_models import DEFAULT_CODEX_MODEL, pricing_for_codex_model


@dataclass(frozen=True)
class CostDecision:
    fire_warning: bool
    cap_breached: bool


def estimate_codex_cost_usd(
    *,
    input_tokens: int,
    output_tokens: int,
    cached_input_tokens: int = 0,
    model: str = DEFAULT_CODEX_MODEL,
) -> float:
    """Estimate Codex USD cost from token usage when CLI output has no price."""
    pricing = pricing_for_codex_model(model)
    cached = max(cached_input_tokens, 0)
    billable_input = max(input_tokens - cached, 0)
    return (
        (billable_input * pricing.input_usd_per_million_tokens)
        + (cached * pricing.cached_input_usd_per_million_tokens)
        + (max(output_tokens, 0) * pricing.output_usd_per_million_tokens)
    ) / 1_000_000


def evaluate_cost(
    *,
    previous_total: float,
    new_total: float,
    cap_usd: float,
    warning_pct: int,
    warning_already_fired: bool,
) -> CostDecision:
    """Pure decision: should the warning be posted, and did cost breach the cap?

    `warning_already_fired` is the persisted "we have posted the cost
    warning for this issue" flag. Until that mark exists, keep asking the
    caller to post the warning once the total is at or above the threshold;
    this lets transient Linear failures retry on later ticks or runs.
    """
    threshold = cap_usd * (warning_pct / 100.0)
    fire_warning = (
        not warning_already_fired
        and cap_usd > 0
        and new_total >= threshold
    )
    cap_breached = cap_usd > 0 and new_total >= cap_usd
    return CostDecision(fire_warning=fire_warning, cap_breached=cap_breached)


def effective_cap(*, global_cap_usd: float, binding_override: float | None) -> float:
    """Per-binding override wins, including an explicit `0` (disabled)."""
    if binding_override is None:
        return global_cap_usd
    return binding_override


def effective_warning_pct(*, global_pct: int, binding_override: int | None) -> int:
    if binding_override is None:
        return global_pct
    return binding_override


__all__ = [
    "CostDecision",
    "effective_cap",
    "effective_warning_pct",
    "estimate_codex_cost_usd",
    "evaluate_cost",
]
