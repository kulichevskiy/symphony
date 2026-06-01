"""Stream-JSON cost / token parser shared by all runners.

`claude --output-format stream-json` emits one JSON object per stdout
line; the terminal `result` event carries `total_cost_usd` plus per-call
token usage. Codex emits usage in `token_count` or `turn.completed`
events but does not price the run itself, so cost is reported as 0 and
the caller falls back to per-token pricing if it needs a dollar figure.
"""

from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Usage:
    cost_usd: float
    input_tokens: int
    output_tokens: int
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0


def _int_or_none(value: object) -> int | None:
    if value is None or value == "":
        return 0
    if not isinstance(value, int | float | str):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return 0.0
    if not isinstance(value, int | float | str):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _usage_from_mapping(usage: object, *, cost_usd: float = 0.0) -> Usage | None:
    if not isinstance(usage, dict):
        usage = {}
    input_tokens = _int_or_none(usage.get("input_tokens"))
    output_tokens = _int_or_none(usage.get("output_tokens"))
    cache_write_tokens = _int_or_none(
        usage.get("cache_write_tokens", usage.get("cache_creation_input_tokens"))
    )
    cache_read_tokens = _int_or_none(
        usage.get(
            "cache_read_tokens",
            usage.get("cache_read_input_tokens", usage.get("cached_input_tokens")),
        )
    )
    if input_tokens is None or output_tokens is None:
        return None
    if cache_write_tokens is None:
        cache_write_tokens = 0
    if cache_read_tokens is None:
        cache_read_tokens = 0
    return Usage(
        cost_usd=cost_usd,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_write_tokens=cache_write_tokens,
        cache_read_tokens=cache_read_tokens,
    )


def parse_event_line(line: str) -> Usage | None:
    """Return the usage encoded in a single stream-json event, or None
    if the line is non-JSON, isn't a usage event, or carries no usage."""
    if not line:
        return None
    try:
        obj = json.loads(line)
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    kind = obj.get("type")
    if kind == "result":
        cost = _float_or_none(obj.get("total_cost_usd"))
        if cost is None:
            return None
        return _usage_from_mapping(obj.get("usage"), cost_usd=cost)
    if kind == "token_count":
        info = obj.get("info") or {}
        if not isinstance(info, dict):
            info = {}
        return _usage_from_mapping(info.get("total_token_usage"))
    if kind == "turn.completed":
        return _usage_from_mapping(obj.get("usage"))
    return None
