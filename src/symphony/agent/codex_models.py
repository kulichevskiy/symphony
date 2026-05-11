"""Codex model pins and token pricing for local runner cost enforcement."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CodexModelPricing:
    input_usd_per_million_tokens: float
    cached_input_usd_per_million_tokens: float
    output_usd_per_million_tokens: float


DEFAULT_CODEX_MODEL = "gpt-5.1-codex"

CODEX_MODEL_PRICING_USD_PER_MILLION_TOKENS: dict[str, CodexModelPricing] = {
    "gpt-5.5": CodexModelPricing(
        input_usd_per_million_tokens=5.0,
        cached_input_usd_per_million_tokens=0.5,
        output_usd_per_million_tokens=30.0,
    ),
    "gpt-5.1-codex": CodexModelPricing(
        input_usd_per_million_tokens=1.25,
        cached_input_usd_per_million_tokens=0.125,
        output_usd_per_million_tokens=10.0,
    ),
    "gpt-5.1-codex-max": CodexModelPricing(
        input_usd_per_million_tokens=1.25,
        cached_input_usd_per_million_tokens=0.125,
        output_usd_per_million_tokens=10.0,
    ),
}

SUPPORTED_CODEX_MODELS = frozenset(CODEX_MODEL_PRICING_USD_PER_MILLION_TOKENS)


def pricing_for_codex_model(model: str) -> CodexModelPricing:
    try:
        return CODEX_MODEL_PRICING_USD_PER_MILLION_TOKENS[model]
    except KeyError as e:
        raise ValueError(f"missing Codex pricing for model {model!r}") from e


__all__ = [
    "CODEX_MODEL_PRICING_USD_PER_MILLION_TOKENS",
    "DEFAULT_CODEX_MODEL",
    "SUPPORTED_CODEX_MODELS",
    "CodexModelPricing",
    "pricing_for_codex_model",
]
