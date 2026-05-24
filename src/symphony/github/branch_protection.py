"""Required-check helpers built on the local gh CLI."""

from __future__ import annotations

from collections.abc import Iterable, MutableMapping

from .client import GitHub

RequiredContextCache = MutableMapping[tuple[str, str], tuple[str, ...]]


async def get_required_contexts(
    repo: str,
    pr: int | str,
    *,
    gh: GitHub | None = None,
    cache: RequiredContextCache | None = None,
) -> tuple[str, ...]:
    """Return required status-check contexts for repo/pr.

    `gh pr checks --required` exposes the merge-gating checks without requiring
    branch-protection admin access. The caller can pass a per-poll cache so
    repeated checks for the same PR do not shell out twice.
    """
    key = (repo, str(pr))
    if cache is not None and key in cache:
        return cache[key]

    client = gh or GitHub()
    checks = await client.pr_checks(pr, repo=repo)
    contexts = _normalize_required_contexts(run.name for run in checks.runs)
    if cache is not None:
        cache[key] = contexts
    return contexts


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
