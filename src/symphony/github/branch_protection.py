"""Branch protection helpers built on the local gh CLI."""

from __future__ import annotations

import json
from collections.abc import Iterable, MutableMapping
from urllib.parse import quote

from .client import GitHub

RequiredContextCache = MutableMapping[tuple[str, str], tuple[str, ...]]
_REQUIRED_CONTEXTS_JQ = (
    "[.required_status_checks.contexts[]?, "
    ".required_status_checks.checks[]?.context?] "
    '| map(select(type == "string" and length > 0))'
)


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
            _REQUIRED_CONTEXTS_JQ,
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
        return _normalize_required_contexts(text.splitlines())
    if isinstance(parsed, list):
        return _normalize_required_contexts(parsed)
    if isinstance(parsed, str) and parsed.strip():
        return (parsed.strip(),)
    return ()


def _normalize_required_contexts(items: Iterable[object]) -> tuple[str, ...]:
    contexts: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if not text or text in seen:
            continue
        contexts.append(text)
        seen.add(text)
    return tuple(contexts)
