"""Taste-guide loader for Acceptance-stage prompts."""

from __future__ import annotations

import os
from pathlib import Path

_GLOBAL_TASTE_GUIDE = "taste-guide.md"


def load_taste_guide(
    *,
    binding_taste_guide: str | Path | None,
    repo_root: Path | None = None,
) -> str:
    """Load global guide first, then optional per-binding guide."""
    root = (repo_root or Path.cwd()).resolve()
    paths = [root / _GLOBAL_TASTE_GUIDE]
    if binding_taste_guide:
        paths.append(_resolve_binding_path(binding_taste_guide, repo_root=root))

    parts: list[str] = []
    for path in paths:
        if path.is_file():
            content = path.read_text(encoding="utf-8").strip()
            if content:
                parts.append(content)
    return "\n\n".join(parts)


def _resolve_binding_path(path: str | Path, *, repo_root: Path) -> Path:
    expanded = Path(os.path.expandvars(os.path.expanduser(str(path))))
    if expanded.is_absolute():
        return expanded
    return repo_root / expanded


__all__ = ["load_taste_guide"]
