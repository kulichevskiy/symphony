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


async def fetch_claude_effort_capabilities(model: str) -> list[str]:
    """Return the effort levels claude `model` supports, in API order.

    Reads the Models API `capabilities.effort.<level>` tree for `model`
    (`opus`/`sonnet`/`haiku` aliases flow through unchanged). Requires
    `ANTHROPIC_API_KEY`; raises `ValueError` — not a bare `httpx` error — when
    the key is missing or the request fails (auth, network, timeout), so
    preflight can report a clean message instead of a raw traceback.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise ValueError(
            "ANTHROPIC_API_KEY is empty; cannot query the Models API to "
            f"validate claude model {model!r}"
        )
    try:
        async with httpx.AsyncClient(
            base_url=CLAUDE_MODELS_API_BASE, timeout=30
        ) as client:
            resp = await client.get(
                f"/v1/models/{model}",
                headers={
                    "x-api-key": api_key,
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
            f"could not reach the Models API to validate claude model "
            f"{model!r}: {e}"
        ) from e
    effort_tree = (data.get("capabilities") or {}).get("effort") or {}
    return list(effort_tree)


__all__ = ["CLAUDE_MODELS_API_BASE", "fetch_claude_effort_capabilities"]
