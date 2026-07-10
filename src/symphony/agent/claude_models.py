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


async def fetch_claude_effort_capabilities(model: str) -> list[str] | None:
    """Return the effort levels claude `model` supports, in API order.

    Reads the Models API `capabilities.effort.<level>` tree for `model`
    (`opus`/`sonnet`/`haiku` aliases flow through unchanged).

    Returns `None` when `ANTHROPIC_API_KEY` is unset. The daemon runs claude via
    the CLI's own auth (OAuth in the containerized deployment), so no API key is
    present there — the caller treats `None` as "cannot validate, skip with a
    warning" rather than a hard failure, letting such a deployment still pass
    preflight. A genuinely-broken pair is still caught structurally at
    `Config.load`.

    Raises `ValueError` — not a bare `httpx` error — when the key IS present but
    the request fails (auth, network, timeout), so preflight reports a clean
    message instead of a raw traceback. Also raises `ValueError` when the
    response carries no effort tree, so an absent/empty tree reads as "cannot
    validate" rather than "supports zero efforts" (which would falsely reject a
    structurally valid pair).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None
    try:
        async with httpx.AsyncClient(base_url=CLAUDE_MODELS_API_BASE, timeout=30) as client:
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
            f"could not reach the Models API to validate claude model {model!r}: {e}"
        ) from e
    effort_tree = (data.get("capabilities") or {}).get("effort") or {}
    if not effort_tree:
        raise ValueError(
            f"Models API returned no effort capabilities for claude model "
            f"{model!r}; cannot validate"
        )
    return list(effort_tree)


__all__ = ["CLAUDE_MODELS_API_BASE", "fetch_claude_effort_capabilities"]
