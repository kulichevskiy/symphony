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


def _usage_from_mapping(usage: object, *, cost_usd: float = 0.0) -> Usage:
    if not isinstance(usage, dict):
        usage = {}
    return Usage(
        cost_usd=cost_usd,
        input_tokens=int(usage.get("input_tokens") or 0),
        output_tokens=int(usage.get("output_tokens") or 0),
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
        cost = float(obj.get("total_cost_usd") or 0.0)
        return _usage_from_mapping(obj.get("usage"), cost_usd=cost)
    if kind == "token_count":
        info = obj.get("info") or {}
        if not isinstance(info, dict):
            info = {}
        return _usage_from_mapping(info.get("total_token_usage"))
    if kind == "turn.completed":
        return _usage_from_mapping(obj.get("usage"))
    return None
