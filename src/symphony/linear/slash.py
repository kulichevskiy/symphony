"""Slash-command parser for inbound Linear comments.

Per `docs/python-port-research.md` §13.2 and the prior doc's §35 Strategy 1
(slash-command-only), v1 only acts on `/approve|reject|retry|stop|skip-review`
exactly. Free-form steering is *not* dispatched in v1 because we don't have
a safe authorship allowlist on the GitHub side; defer to v1.1.

Filter rules:
- `external_thread_type is None` → comment was authored natively in Linear
  (not mirrored from GitHub). Mirrored comments are picked up by the
  GitHub-side review poll instead, so acting on them here would double-fire.
- `author_is_me` → Symphony itself posted it; ignore.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import LinearComment

_PATTERN = re.compile(r"^\s*/(approve|reject|retry|stop|skip-review)\b", re.IGNORECASE)
_THUMBS_UP = {"👍", ":+1:", ":+1"}
_THUMBS_UP_EMOJI = "👍"
_VARIATION_SELECTORS = {"\ufe0e", "\ufe0f"}


class SlashKind(StrEnum):
    APPROVE = "approve"
    REJECT = "reject"
    RETRY = "retry"
    STOP = "stop"
    SKIP_REVIEW = "skip-review"


@dataclass
class SlashIntent:
    kind: SlashKind
    comment_id: str
    created_at: str


def _is_thumbs_up(body: str) -> bool:
    if body in _THUMBS_UP:
        return True
    normalized = "".join(
        ch
        for ch in body
        if ch not in _VARIATION_SELECTORS and not 0x1F3FB <= ord(ch) <= 0x1F3FF
    )
    return normalized == _THUMBS_UP_EMOJI


def parse(comments: list[LinearComment]) -> list[SlashIntent]:
    """Pure function: filter and classify. No I/O."""
    out: list[SlashIntent] = []
    for c in comments:
        if c.author_is_me:
            continue
        if c.external_thread_type is not None:
            # Mirrored from elsewhere; the originating side's poll handles it.
            continue
        body = (c.body or "").strip()
        if _is_thumbs_up(body):
            out.append(
                SlashIntent(
                    kind=SlashKind.APPROVE,
                    comment_id=c.id,
                    created_at=c.created_at,
                )
            )
            continue
        m = _PATTERN.match(body)
        if not m:
            continue
        kind = SlashKind(m.group(1).lower())
        out.append(SlashIntent(kind=kind, comment_id=c.id, created_at=c.created_at))
    return out
