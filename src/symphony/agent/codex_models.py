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
    "gpt-5.6-sol": CodexModelPricing(
        input_usd_per_million_tokens=5.0,
        cached_input_usd_per_million_tokens=0.5,
        output_usd_per_million_tokens=30.0,
    ),
    "gpt-5.6-terra": CodexModelPricing(
        input_usd_per_million_tokens=2.5,
        cached_input_usd_per_million_tokens=0.25,
        output_usd_per_million_tokens=15.0,
    ),
    "gpt-5.6-luna": CodexModelPricing(
        input_usd_per_million_tokens=1.0,
        cached_input_usd_per_million_tokens=0.1,
        output_usd_per_million_tokens=6.0,
    ),
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

STATIC_CODEX_EFFORTS_BY_MODEL: dict[str, tuple[str, ...]] = {
    "gpt-5.6-sol": ("low", "medium", "high", "xhigh", "max", "ultra"),
    "gpt-5.6-terra": ("low", "medium", "high", "xhigh", "max", "ultra"),
    "gpt-5.6-luna": ("low", "medium", "high", "xhigh", "max"),
    "gpt-5.5": ("low", "medium", "high", "xhigh"),
    "gpt-5.1-codex": ("minimal", "low", "medium", "high"),
    "gpt-5.1-codex-max": ("minimal", "low", "medium", "high"),
}
SUPPORTED_CODEX_EFFORTS = frozenset(
    effort for efforts in STATIC_CODEX_EFFORTS_BY_MODEL.values() for effort in efforts
)


def pricing_for_codex_model(model: str) -> CodexModelPricing:
    try:
        return CODEX_MODEL_PRICING_USD_PER_MILLION_TOKENS[model]
    except KeyError as e:
        raise ValueError(f"missing Codex pricing for model {model!r}") from e


__all__ = [
    "CODEX_MODEL_PRICING_USD_PER_MILLION_TOKENS",
    "DEFAULT_CODEX_MODEL",
    "STATIC_CODEX_EFFORTS_BY_MODEL",
    "SUPPORTED_CODEX_EFFORTS",
    "SUPPORTED_CODEX_MODELS",
    "CodexModelPricing",
    "pricing_for_codex_model",
]
