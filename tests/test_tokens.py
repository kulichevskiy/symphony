"""Shared effective-token weighting helper."""

from __future__ import annotations

from symphony.tokens import (
    CACHE_READ_WEIGHT,
    CACHE_WRITE_WEIGHT,
    effective_tokens,
)


def test_effective_tokens_weights_cache_traffic() -> None:
    # input + output + cache_write*1.25 + cache_read*0.1
    assert (
        effective_tokens(
            input_tokens=10,
            output_tokens=20,
            cache_write_tokens=8,
            cache_read_tokens=10,
        )
        == 10 + 20 + 8 * 1.25 + 10 * 0.1
    )  # == 41.0


def test_effective_tokens_all_zero_is_zero() -> None:
    assert effective_tokens(0, 0, 0, 0) == 0.0


def test_weights_match_per_issue_budget() -> None:
    # The same weighting the per-issue token budget gates on (SYM-130).
    assert CACHE_WRITE_WEIGHT == 1.25
    assert CACHE_READ_WEIGHT == 0.1
