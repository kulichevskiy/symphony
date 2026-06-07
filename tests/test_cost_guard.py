"""Codex cost-estimation tests.

`estimate_codex_cost_usd` is the only pricing surface left in the cost
guard: Codex reports cumulative token counts but never prices the run,
so we estimate it ourselves for the internal `runs.cost_usd` column.
"""

from __future__ import annotations

import pytest

from symphony.pipeline.cost_guard import estimate_codex_cost_usd


def test_estimates_codex_cost_from_tokens() -> None:
    cost = estimate_codex_cost_usd(
        input_tokens=1_000_000,
        cached_input_tokens=200_000,
        output_tokens=100_000,
        model="gpt-5.1-codex",
    )
    assert round(cost, 6) == 2.025


def test_estimates_codex_cost_rejects_unknown_model() -> None:
    with pytest.raises(ValueError, match="missing Codex pricing"):
        estimate_codex_cost_usd(
            input_tokens=1_000,
            output_tokens=1_000,
            model="future-codex",
        )
