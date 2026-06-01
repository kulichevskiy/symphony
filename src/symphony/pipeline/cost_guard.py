"""Pure cost-cap / cost-warning decisions.

The orchestrator accumulates per-issue cumulative cost as the runner
emits stream-json events. After each tick it asks `evaluate_cost`
whether to fire the once-per-issue warning and whether the per-issue
cap has been breached. Keeping the decision pure makes synthetic
trajectories easy to test and lets the orchestrator stay focused on I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..agent.codex_models import DEFAULT_CODEX_MODEL, pricing_for_codex_model
from ..agent.process import Usage


@dataclass(frozen=True)
class CostDecision:
    fire_warning: bool
    cap_breached: bool


@dataclass(frozen=True)
class UsageDelta:
    cost_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0

    def has_usage(self) -> bool:
        return (
            self.cost_usd != 0
            or self.input_tokens != 0
            or self.output_tokens != 0
            or self.cache_write_tokens != 0
            or self.cache_read_tokens != 0
        )


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


@dataclass
class UsageCostEstimator:
    """Maps stream-json `Usage` events to dollar deltas.

    Claude reports `total_cost_usd` directly so deltas are trivial.
    Codex reports cumulative *token* counts per turn and never prices
    the run itself, so this estimator keeps the running max per token
    bucket and prices the delta via `estimate_codex_cost_usd`. Sharing
    one estimator across multiple subprocess calls (e.g. across local-
    review iterations) preserves the cumulative-token invariant so
    each successive call only pays for *new* tokens.
    """

    agent: str
    codex_model: str
    last_estimated_input_tokens: int = 0
    last_estimated_cache_write_tokens: int = 0
    last_estimated_cache_read_tokens: int = 0
    last_estimated_output_tokens: int = 0
    total_cost_usd: float = field(default=0.0, init=False)
    total_input_tokens: int = field(default=0, init=False)
    total_output_tokens: int = field(default=0, init=False)
    total_cache_write_tokens: int = field(default=0, init=False)
    total_cache_read_tokens: int = field(default=0, init=False)

    def delta(self, usage: Usage) -> UsageDelta:
        if self.agent != "codex" or usage.cost_usd > 0:
            self.total_cost_usd += usage.cost_usd
            delta = UsageDelta(
                cost_usd=usage.cost_usd,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_write_tokens=usage.cache_write_tokens,
                cache_read_tokens=usage.cache_read_tokens,
            )
            self.total_input_tokens += delta.input_tokens
            self.total_output_tokens += delta.output_tokens
            self.total_cache_write_tokens += delta.cache_write_tokens
            self.total_cache_read_tokens += delta.cache_read_tokens
            return delta
        input_delta = max(
            usage.input_tokens - self.last_estimated_input_tokens, 0
        )
        cache_write_delta = max(
            usage.cache_write_tokens - self.last_estimated_cache_write_tokens,
            0,
        )
        cache_read_delta = max(
            usage.cache_read_tokens - self.last_estimated_cache_read_tokens,
            0,
        )
        output_delta = max(
            usage.output_tokens - self.last_estimated_output_tokens, 0
        )
        self.last_estimated_input_tokens = max(
            self.last_estimated_input_tokens, usage.input_tokens
        )
        self.last_estimated_cache_write_tokens = max(
            self.last_estimated_cache_write_tokens,
            usage.cache_write_tokens,
        )
        self.last_estimated_cache_read_tokens = max(
            self.last_estimated_cache_read_tokens,
            usage.cache_read_tokens,
        )
        self.last_estimated_output_tokens = max(
            self.last_estimated_output_tokens, usage.output_tokens
        )
        cost = estimate_codex_cost_usd(
            input_tokens=input_delta,
            cached_input_tokens=cache_read_delta,
            output_tokens=output_delta,
            model=self.codex_model,
        )
        self.total_cost_usd += cost
        delta = UsageDelta(
            cost_usd=cost,
            input_tokens=input_delta,
            output_tokens=output_delta,
            cache_write_tokens=cache_write_delta,
            cache_read_tokens=cache_read_delta,
        )
        self.total_input_tokens += delta.input_tokens
        self.total_output_tokens += delta.output_tokens
        self.total_cache_write_tokens += delta.cache_write_tokens
        self.total_cache_read_tokens += delta.cache_read_tokens
        return delta


__all__ = [
    "CostDecision",
    "UsageDelta",
    "UsageCostEstimator",
    "effective_cap",
    "effective_warning_pct",
    "estimate_codex_cost_usd",
    "evaluate_cost",
]
