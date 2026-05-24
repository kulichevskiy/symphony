"""Branch protection helpers built on the local gh CLI."""

from __future__ import annotations

import json
from collections.abc import MutableMapping
from urllib.parse import quote

from .client import GitHub

RequiredContextCache = MutableMapping[tuple[str, str], tuple[str, ...]]


async def get_required_contexts(
    repo: str,
    base: str,
    *,
    gh: GitHub | None = None,
    cache: RequiredContextCache | None = None,
) -> tuple[str, ...]:
    """Return required status-check contexts for repo/base.

    The caller can pass a per-poll cache so multiple PRs in the same binding do
    not re-fetch identical branch protection data.
    """
    key = (repo, base)
    if cache is not None and key in cache:
        return cache[key]

    client = gh or GitHub()
    host_args, owner_repo = client._api_repo(repo)  # noqa: SLF001
    branch = quote(base, safe="")
    out = await client._run(  # noqa: SLF001
        [
            "api",
            *host_args,
            f"repos/{owner_repo}/branches/{branch}/protection",
            "--jq",
            ".required_status_checks.contexts // []",
        ]
    )
    contexts = _parse_required_contexts(out)
    if cache is not None:
        cache[key] = contexts
    return contexts


def _parse_required_contexts(raw: str) -> tuple[str, ...]:
    text = raw.strip()
    if not text:
        return ()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return tuple(line.strip() for line in text.splitlines() if line.strip())
    if isinstance(parsed, list):
        return tuple(str(item) for item in parsed if str(item).strip())
    if isinstance(parsed, str) and parsed.strip():
        return (parsed.strip(),)
    return ()
