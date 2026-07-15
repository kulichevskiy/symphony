"""Live claude capability source for the preflight `(model, effort)` check.

Unlike codex — whose effort scale is a fixed enum pinned in `codex_models` —
claude's per-model effort support is read from the Anthropic Models API
`capabilities.effort.<level>` tree at preflight time. This is an *online*
check, run manually via `symphony preflight`; daemon boot stays structural
(`Config.load` only) and never reaches the network.
"""

from __future__ import annotations

import os

import httpx

CLAUDE_MODELS_API_BASE = "https://api.anthropic.com"
_ANTHROPIC_VERSION = "2023-06-01"

# The `claude` CLI's own short `--model` aliases are a CLI-only convenience —
# the Models API has no such alias and only resolves full IDs, so a bare
# `/v1/models/opus` 404s. Map each alias to the full ID it currently backs so
# the online capability check queries something the API actually knows.
#
# This is only a guess of what the alias resolves to. An org enforcing a
# Claude Code model allowlist can pin an alias to an older version than the
# one hard-coded here (docs: family aliases resolve to the newest version the
# allowlist permits — https://docs.anthropic.com/en/docs/claude-code/model-config),
# so this default can validate a different model than the one the CLI
# actually runs. `_alias_override_env` lets an operator in that situation tell
# us the real pinned ID instead.
_CLAUDE_ALIAS_MODEL_IDS: dict[str, str] = {
    "opus": "claude-opus-4-8",
    "sonnet": "claude-sonnet-5",
    "haiku": "claude-haiku-4-5-20251001",
}


def _alias_override_env(alias: str) -> str:
    return f"SYMPHONY_CLAUDE_ALIAS_{alias.upper()}"


def _resolve_alias_model_id(model: str) -> str:
    """Resolve a CLI-only alias to the full model ID to check capabilities for.

    Checks `SYMPHONY_CLAUDE_ALIAS_<ALIAS>` (e.g. `SYMPHONY_CLAUDE_ALIAS_SONNET
    =claude-sonnet-4-6`) first, for an operator whose org allowlist pins the
    alias away from our hard-coded guess; falls back to
    `_CLAUDE_ALIAS_MODEL_IDS`, then the model name itself for a full `claude-*`
    ID.
    """
    override = os.environ.get(_alias_override_env(model))
    if override:
        return override
    return _CLAUDE_ALIAS_MODEL_IDS.get(model, model)


async def fetch_claude_effort_capabilities(
    model: str, api_key: str | None = None
) -> list[str] | None:
    """Return the effort levels claude `model` supports, in API order.

    Reads the Models API `capabilities.effort.<level>` tree for `model`,
    resolving a short `opus`/`sonnet`/`haiku` CLI alias to its current full
    model ID first (see `_resolve_alias_model_id`) — the Models API doesn't
    recognize the bare alias. A full `claude-*` ID passes through unchanged.
    `api_key` is the key to authenticate with; when `None` it falls back to
    the process env
    `ANTHROPIC_API_KEY`. Preflight resolves the key from the process env OR a
    binding's `env:` mapping and passes it in, so a key supplied only through a
    binding still drives validation.

    Returns `None` when no key is available anywhere. The daemon can run claude
    via the CLI's own auth (OAuth in the containerized deployment) with no API
    key present — the caller treats `None` as "cannot validate, skip with a
    warning" rather than a hard failure, letting such a deployment still pass
    preflight. A genuinely-broken pair is still caught structurally at
    `Config.load`.

    Raises `ValueError` — not a bare `httpx` error — when a key IS available but
    the request fails (auth, network, timeout), so preflight reports a clean
    message instead of a raw traceback. Also raises `ValueError` when the
    response carries no effort tree, so an absent/empty tree reads as "cannot
    validate" rather than "supports zero efforts" (which would falsely reject a
    structurally valid pair).
    """
    key = api_key if api_key is not None else os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None
    api_model = _resolve_alias_model_id(model)
    try:
        async with httpx.AsyncClient(base_url=CLAUDE_MODELS_API_BASE, timeout=30) as client:
            resp = await client.get(
                f"/v1/models/{api_model}",
                headers={
                    "x-api-key": key,
                    "anthropic-version": _ANTHROPIC_VERSION,
                },
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        raise ValueError(
            f"Models API returned HTTP {e.response.status_code} for claude "
            f"model {model!r}; cannot validate its effort capabilities"
        ) from e
    except httpx.HTTPError as e:
        raise ValueError(
            f"could not reach the Models API to validate claude model {model!r}: {e}"
        ) from e
    effort_tree = (data.get("capabilities") or {}).get("effort") or {}
    if not effort_tree:
        raise ValueError(
            f"Models API returned no effort capabilities for claude model "
            f"{model!r}; cannot validate"
        )
    # Each level's value carries a support flag (e.g. `null`/`{"supported":
    # false}` for an unsupported level); a bare `{}` (no flag at all, as in
    # the common case) means supported. Keep only levels the model actually
    # supports — otherwise an unsupported level's mere presence in the tree
    # would pass the caller's membership check.
    return [
        level
        for level, meta in effort_tree.items()
        if meta is not None and (not isinstance(meta, dict) or meta.get("supported", True))
    ]


__all__ = ["CLAUDE_MODELS_API_BASE", "fetch_claude_effort_capabilities"]
