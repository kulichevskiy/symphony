"""Cost / token parser shared by all runners.

Both `claude --output-format stream-json` and codex's equivalent emit one
JSON object per stdout line. The parser inspects the line and returns a
`Usage` (cost in USD + token counts) when the line is a usage-bearing
event, otherwise None. No IO, no state.
"""

from __future__ import annotations

import json

from symphony.agent.process import Usage, parse_event_line


def test_parses_claude_result_event() -> None:
    line = json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "total_cost_usd": 0.42,
            "usage": {
                "input_tokens": 1000,
                "cache_creation_input_tokens": 800,
                "cache_read_input_tokens": 600,
                "output_tokens": 200,
            },
        }
    )
    assert parse_event_line(line) == Usage(
        cost_usd=0.42,
        input_tokens=1000,
        output_tokens=200,
        cache_write_tokens=800,
        cache_read_tokens=600,
    )


def test_returns_none_on_non_usage_lines() -> None:
    assert parse_event_line(json.dumps({"type": "system", "subtype": "init"})) is None
    assert parse_event_line(json.dumps({"type": "assistant"})) is None


def test_returns_none_on_garbage_input() -> None:
    assert parse_event_line("not json") is None
    assert parse_event_line("") is None
    # JSON but not an object.
    assert parse_event_line(json.dumps([1, 2, 3])) is None


def test_returns_none_on_malformed_numeric_usage() -> None:
    assert (
        parse_event_line(
            json.dumps(
                {
                    "type": "result",
                    "total_cost_usd": "oops",
                    "usage": {"input_tokens": 1, "output_tokens": 2},
                }
            )
        )
        is None
    )
    assert (
        parse_event_line(
            json.dumps(
                {
                    "type": "turn.completed",
                    "usage": {"input_tokens": "oops", "output_tokens": 2},
                }
            )
        )
        is None
    )
    assert (
        parse_event_line(
            json.dumps(
                {
                    "type": "token_count",
                    "info": {
                        "total_token_usage": {
                            "input_tokens": 1,
                            "output_tokens": "oops",
                        }
                    },
                }
            )
        )
        is None
    )


def test_parses_codex_token_count_event() -> None:
    """Codex streams a `token_count` event with a `total_token_usage`
    object instead of a per-result cost. We capture the token counts so
    the per-issue token budget can be enforced; cost is reported as 0
    when the upstream tool doesn't price it for us."""
    line = json.dumps(
        {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": 1500,
                    "cached_input_tokens": 1200,
                    "output_tokens": 300,
                    "total_tokens": 1800,
                }
            },
        }
    )
    usage = parse_event_line(line)
    assert usage is not None
    assert usage.input_tokens == 1500
    assert usage.cache_read_tokens == 1200
    assert usage.cache_write_tokens == 0
    assert usage.output_tokens == 300


def test_parses_codex_turn_completed_usage() -> None:
    """Codex `exec --json` reports final usage on `turn.completed`."""
    line = json.dumps(
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 2100,
                "cached_input_tokens": 400,
                "output_tokens": 550,
                "total_tokens": 2650,
            },
        }
    )
    assert parse_event_line(line) == Usage(
        cost_usd=0.0,
        input_tokens=2100,
        output_tokens=550,
        cache_read_tokens=400,
    )


def test_usage_cost_accumulates_on_caller() -> None:
    """Multiple result events in a single run are summed by the caller.
    The parser is stateless: each call returns the usage for that line.
    """
    line1 = json.dumps(
        {"type": "result", "total_cost_usd": 0.10, "usage": {"input_tokens": 1, "output_tokens": 1}}
    )
    line2 = json.dumps(
        {"type": "result", "total_cost_usd": 0.05, "usage": {"input_tokens": 2, "output_tokens": 2}}
    )
    u1 = parse_event_line(line1)
    u2 = parse_event_line(line2)
    assert u1 is not None and u2 is not None
    assert round(u1.cost_usd + u2.cost_usd, 4) == 0.15
