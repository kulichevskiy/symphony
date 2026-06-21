"""Effective (weighted) token total — the single per-issue spend unit.

The per-issue token budget (SYM-130) does not gate on raw token counts: it
weights cache traffic, because a cache *write* costs more than fresh input
while a cache *read* costs far less. Every operator surface that reports
spend (Linear comments, CLI, UI) must show the same weighted figure the
budget gates on, so the weighting lives here once and is reused everywhere.

The frontend mirrors these weights in `frontend/src/lib/format.ts`
(`effectiveTokens`); keep the two in sync.
"""

from __future__ import annotations

# Same weighting as the per-issue token budget (SYM-130).
CACHE_WRITE_WEIGHT = 1.25
CACHE_READ_WEIGHT = 0.1


def effective_tokens(
    input_tokens: int,
    output_tokens: int,
    cache_write_tokens: int,
    cache_read_tokens: int,
) -> float:
    """Weighted token total: in + out + cache_write*1.25 + cache_read*0.1."""
    return (
        input_tokens
        + output_tokens
        + cache_write_tokens * CACHE_WRITE_WEIGHT
        + cache_read_tokens * CACHE_READ_WEIGHT
    )


__all__ = ["CACHE_READ_WEIGHT", "CACHE_WRITE_WEIGHT", "effective_tokens"]
