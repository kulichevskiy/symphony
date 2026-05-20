"""Acceptance verdict classifier.

The Acceptance agent is asked to end its final message with a stable footer.
The classifier reads the Claude stream-json transcript, extracts the final
message and terminal cost, then turns that into an `AcceptanceVerdict`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Literal

AcceptanceVerdictKind = Literal["pass", "reject", "infra_error"]

ACCEPTANCE_FOOTER_PASS = "<!-- symphony-acceptance-verdict: pass -->"
ACCEPTANCE_FOOTER_REJECT = "<!-- symphony-acceptance-verdict: reject -->"
ACCEPTANCE_FOOTER_INFRA_ERROR = (
    "<!-- symphony-acceptance-verdict: infra_error -->"
)

_FOOTER_RE = re.compile(
    r"<!--\s*symphony-acceptance-verdict:\s*"
    r"(?P<kind>pass|reject|infra_error)\s*-->",
    re.IGNORECASE,
)
_COMMENT_DETAILS_LIMIT = 2500


@dataclass(frozen=True)
class AcceptanceVerdict:
    kind: AcceptanceVerdictKind
    criteria: list[str]
    cost: float
    hero_screenshot_url: str
    details: str = ""


def acceptance_footer(kind: AcceptanceVerdictKind) -> str:
    if kind == "pass":
        return ACCEPTANCE_FOOTER_PASS
    if kind == "reject":
        return ACCEPTANCE_FOOTER_REJECT
    return ACCEPTANCE_FOOTER_INFRA_ERROR


def acceptance_classifier(
    *,
    transcript: str,
    criteria: list[str] | None = None,
    cost: float | None = None,
) -> AcceptanceVerdict:
    message, parsed_cost = _last_claude_result(transcript)
    if not message:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=parsed_cost if cost is None else cost,
            hero_screenshot_url="",
            details="Acceptance agent did not emit a final message.",
        )
    verdict_text = message
    match = list(_FOOTER_RE.finditer(verdict_text))
    if not match:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=parsed_cost if cost is None else cost,
            hero_screenshot_url="",
            details="Acceptance agent did not emit a verdict footer.",
        )
    kind = match[-1].group("kind").lower()
    details = _strip_footer(verdict_text).strip()
    return AcceptanceVerdict(
        kind=kind,  # type: ignore[arg-type]
        criteria=list(criteria or []),
        cost=parsed_cost if cost is None else cost,
        hero_screenshot_url="",
        details=details,
    )


def format_acceptance_verdict_comment(
    *, verdict: AcceptanceVerdict, pr_url: str
) -> str:
    details = verdict.details.strip()
    if len(details) > _COMMENT_DETAILS_LIMIT:
        details = details[:_COMMENT_DETAILS_LIMIT] + "\n...[truncated]"
    body = (
        f"**Acceptance verdict:** `{verdict.kind}`\n\n"
        f"- PR: {pr_url}\n"
        f"- Cost: ${verdict.cost:.4f}\n"
    )
    if details:
        body += f"\n{details}\n"
    return f"{body}\n{acceptance_footer(verdict.kind)}"


def _last_claude_result(transcript: str) -> tuple[str, float]:
    message = ""
    cost = 0.0
    for raw in transcript.splitlines():
        line = raw.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        if event.get("type") == "result":
            result = event.get("result")
            if isinstance(result, str):
                message = result
            parsed_cost = _float_or_none(event.get("total_cost_usd"))
            if parsed_cost is not None:
                cost = parsed_cost
        elif event.get("type") == "assistant":
            for text in _assistant_text_blocks(event):
                message = text
    return message, cost


def _assistant_text_blocks(event: dict[str, object]) -> list[str]:
    message = event.get("message")
    if not isinstance(message, dict):
        return []
    content = message.get("content")
    if not isinstance(content, list):
        return []
    out: list[str] = []
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "text":
            continue
        text = block.get("text")
        if isinstance(text, str) and text:
            out.append(text)
    return out


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return 0.0
    if not isinstance(value, int | float | str):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _strip_footer(text: str) -> str:
    return _FOOTER_RE.sub("", text).strip()


__all__ = [
    "ACCEPTANCE_FOOTER_INFRA_ERROR",
    "ACCEPTANCE_FOOTER_PASS",
    "ACCEPTANCE_FOOTER_REJECT",
    "AcceptanceVerdict",
    "AcceptanceVerdictKind",
    "acceptance_classifier",
    "acceptance_footer",
    "format_acceptance_verdict_comment",
]
