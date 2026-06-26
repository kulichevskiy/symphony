"""Per-(provider, model) token attribution parsed from a run's logs.

`parse_model_usage` consumes the stream-json stdout lines of a single run
(one or more subprocess invocations appended to the same log) and returns
one `ModelUsage` row per (provider, model). It is the reusable foundation
shared by the orchestrator's live-write at run end and the historical
backfill.

Provider is inferred from event shape:

* **Claude** emits a terminal `result` event carrying `modelUsage` — an
  exact per-model split (`inputTokens` / `outputTokens` /
  `cacheCreationInputTokens` / `cacheReadInputTokens`). Each subprocess
  emits its own cumulative `result`, so per-model totals are summed
  across `result` events.
* **Codex** emits `thread.started` / `token_count` / `turn.completed`
  events. `token_count` carries a *cumulative* `total_token_usage` within
  one process, so only the last snapshot per process counts; `thread.started`
  marks a process boundary so multiple codex processes in one log sum
  correctly. Codex usage is attributed to the model resolved from config
  (`codex_model`), or `unknown` when it can't be determined.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass

CLAUDE_PROVIDER = "claude"
CODEX_PROVIDER = "codex"
UNKNOWN_MODEL = "unknown"


@dataclass(frozen=True)
class ModelUsage:
    provider: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0


def _int(value: object) -> int:
    if isinstance(value, bool) or value is None:
        return 0
    if isinstance(value, int | float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _loads(line: str) -> dict[str, object] | None:
    text = line.strip()
    if not text.startswith("{"):
        return None
    try:
        obj = json.loads(text)
    except (ValueError, TypeError):
        return None
    return obj if isinstance(obj, dict) else None


def _codex_tokens(usage: dict[str, object]) -> tuple[int, int, int, int]:
    # Codex reports total input (cached + uncached) and a `cached_input_tokens`
    # bucket; there is no separate cache-write count.
    return (
        _int(usage.get("input_tokens")),
        _int(usage.get("output_tokens")),
        0,
        _int(usage.get("cached_input_tokens")),
    )


def parse_model_usage(lines: Iterable[str], *, codex_model: str | None) -> list[ModelUsage]:
    """Return per-(provider, model) usage for one run's stream-json log."""
    claude: dict[str, list[int]] = {}
    codex_sum = [0, 0, 0, 0]
    codex_seen = False
    codex_current: tuple[int, int, int, int] | None = None

    def commit_codex() -> None:
        nonlocal codex_current
        if codex_current is not None:
            for i in range(4):
                codex_sum[i] += codex_current[i]
            codex_current = None

    for line in lines:
        obj = _loads(line)
        if obj is None:
            continue
        kind = obj.get("type")
        if kind == "result":
            model_usage = obj.get("modelUsage")
            if isinstance(model_usage, dict):
                for model, usage in model_usage.items():
                    if not isinstance(usage, dict):
                        continue
                    acc = claude.setdefault(str(model), [0, 0, 0, 0])
                    acc[0] += _int(usage.get("inputTokens"))
                    acc[1] += _int(usage.get("outputTokens"))
                    acc[2] += _int(usage.get("cacheCreationInputTokens"))
                    acc[3] += _int(usage.get("cacheReadInputTokens"))
        elif kind == "thread.started":
            commit_codex()
            codex_seen = True
        elif kind == "token_count":
            codex_seen = True
            info = obj.get("info")
            usage = info.get("total_token_usage") if isinstance(info, dict) else None
            if isinstance(usage, dict):
                codex_current = _codex_tokens(usage)
        elif kind == "turn.completed":
            codex_seen = True
            usage = obj.get("usage")
            if isinstance(usage, dict):
                codex_current = _codex_tokens(usage)

    commit_codex()

    result = [
        ModelUsage(
            provider=CLAUDE_PROVIDER,
            model=model,
            input_tokens=acc[0],
            output_tokens=acc[1],
            cache_write_tokens=acc[2],
            cache_read_tokens=acc[3],
        )
        for model, acc in claude.items()
    ]
    if codex_seen and any(codex_sum):
        result.append(
            ModelUsage(
                provider=CODEX_PROVIDER,
                model=codex_model or UNKNOWN_MODEL,
                input_tokens=codex_sum[0],
                output_tokens=codex_sum[1],
                cache_write_tokens=codex_sum[2],
                cache_read_tokens=codex_sum[3],
            )
        )
    return result


__all__ = [
    "CLAUDE_PROVIDER",
    "CODEX_PROVIDER",
    "UNKNOWN_MODEL",
    "ModelUsage",
    "parse_model_usage",
]
