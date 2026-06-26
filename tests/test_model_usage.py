"""Reusable per-(provider, model) usage parser shared by live-write and
the historical backfill.

`parse_model_usage` takes the stream-json log lines of a single run and
returns one `ModelUsage` row per (provider, model). Claude carries an
exact per-model split in `result.modelUsage`; Codex reports only
cumulative token counts per process, attributed to the model resolved
from config (or `unknown` when it can't be determined).
"""

from __future__ import annotations

import json

from symphony.agent.model_usage import ModelUsage, parse_model_usage


def test_claude_result_model_usage_split() -> None:
    line = json.dumps(
        {
            "type": "result",
            "total_cost_usd": 0.42,
            "modelUsage": {
                "claude-opus-4-8[1m]": {
                    "inputTokens": 1000,
                    "outputTokens": 200,
                    "cacheCreationInputTokens": 800,
                    "cacheReadInputTokens": 600,
                },
                "claude-haiku-4-5": {
                    "inputTokens": 10,
                    "outputTokens": 2,
                    "cacheCreationInputTokens": 0,
                    "cacheReadInputTokens": 5,
                },
            },
        }
    )
    usages = parse_model_usage([line], codex_model="gpt-5.1-codex")
    assert set(usages) == {
        ModelUsage(
            provider="claude",
            model="claude-opus-4-8[1m]",
            input_tokens=1000,
            output_tokens=200,
            cache_write_tokens=800,
            cache_read_tokens=600,
        ),
        ModelUsage(
            provider="claude",
            model="claude-haiku-4-5",
            input_tokens=10,
            output_tokens=2,
            cache_write_tokens=0,
            cache_read_tokens=5,
        ),
    }


def test_claude_sums_model_usage_across_result_events() -> None:
    """Multiple subprocesses append to one run log; each emits its own
    cumulative `result`. The per-model totals are the sum across them."""
    lines = [
        json.dumps(
            {
                "type": "result",
                "modelUsage": {
                    "claude-opus-4-8": {
                        "inputTokens": 100,
                        "outputTokens": 10,
                        "cacheCreationInputTokens": 0,
                        "cacheReadInputTokens": 0,
                    }
                },
            }
        ),
        json.dumps(
            {
                "type": "result",
                "modelUsage": {
                    "claude-opus-4-8": {
                        "inputTokens": 50,
                        "outputTokens": 5,
                        "cacheCreationInputTokens": 0,
                        "cacheReadInputTokens": 0,
                    }
                },
            }
        ),
    ]
    usages = parse_model_usage(lines, codex_model="gpt-5.1-codex")
    assert usages == [
        ModelUsage(
            provider="claude",
            model="claude-opus-4-8",
            input_tokens=150,
            output_tokens=15,
        )
    ]


def test_codex_attributed_to_config_model() -> None:
    lines = [
        json.dumps({"type": "thread.started"}),
        json.dumps(
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
        ),
    ]
    usages = parse_model_usage(lines, codex_model="gpt-5.5")
    assert usages == [
        ModelUsage(
            provider="codex",
            model="gpt-5.5",
            input_tokens=1500,
            output_tokens=300,
            cache_write_tokens=0,
            cache_read_tokens=1200,
        )
    ]


def test_codex_token_count_is_cumulative_within_process() -> None:
    """Codex `token_count` is a running total; only the last one per
    process counts (no double-add of intermediate snapshots)."""
    lines = [
        json.dumps({"type": "thread.started"}),
        json.dumps(
            {
                "type": "token_count",
                "info": {"total_token_usage": {"input_tokens": 100, "output_tokens": 10}},
            }
        ),
        json.dumps(
            {
                "type": "token_count",
                "info": {"total_token_usage": {"input_tokens": 400, "output_tokens": 40}},
            }
        ),
    ]
    usages = parse_model_usage(lines, codex_model="gpt-5.1-codex")
    assert usages == [
        ModelUsage(
            provider="codex",
            model="gpt-5.1-codex",
            input_tokens=400,
            output_tokens=40,
        )
    ]


def test_codex_sums_cumulative_totals_across_processes() -> None:
    """Two codex subprocesses in one log: each resets its cumulative
    token_count at its own `thread.started`, so the run total is the sum
    of each process's last snapshot."""
    lines = [
        json.dumps({"type": "thread.started"}),
        json.dumps(
            {
                "type": "token_count",
                "info": {"total_token_usage": {"input_tokens": 300, "output_tokens": 30}},
            }
        ),
        json.dumps({"type": "thread.started"}),
        json.dumps(
            {
                "type": "token_count",
                "info": {"total_token_usage": {"input_tokens": 200, "output_tokens": 20}},
            }
        ),
    ]
    usages = parse_model_usage(lines, codex_model="gpt-5.1-codex")
    assert usages == [
        ModelUsage(
            provider="codex",
            model="gpt-5.1-codex",
            input_tokens=500,
            output_tokens=50,
        )
    ]


def test_codex_unknown_model_fallback() -> None:
    lines = [
        json.dumps(
            {
                "type": "turn.completed",
                "usage": {"input_tokens": 10, "output_tokens": 2},
            }
        )
    ]
    usages = parse_model_usage(lines, codex_model=None)
    assert usages == [
        ModelUsage(provider="codex", model="unknown", input_tokens=10, output_tokens=2)
    ]


def test_ignores_non_usage_and_garbage_lines() -> None:
    lines = [
        "not json",
        "",
        json.dumps([1, 2, 3]),
        json.dumps({"type": "system", "subtype": "init"}),
        json.dumps({"type": "assistant"}),
    ]
    assert parse_model_usage(lines, codex_model="gpt-5.1-codex") == []


def test_result_without_model_usage_yields_nothing() -> None:
    line = json.dumps({"type": "result", "total_cost_usd": 0.1, "usage": {"input_tokens": 5}})
    assert parse_model_usage([line], codex_model="gpt-5.1-codex") == []
