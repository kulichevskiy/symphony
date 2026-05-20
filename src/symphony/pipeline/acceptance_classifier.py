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


@dataclass(frozen=True)
class _ParsedTranscript:
    message: str
    cost: float
    infra_error_details: str = ""


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
    parsed = _parse_claude_transcript(transcript)
    verdict_cost = parsed.cost if cost is None else cost
    if parsed.infra_error_details:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=verdict_cost,
            hero_screenshot_url="",
            details=parsed.infra_error_details,
        )
    if not parsed.message:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=verdict_cost,
            hero_screenshot_url="",
            details="Acceptance agent did not emit a final message.",
        )
    verdict_text = parsed.message
    match = list(_FOOTER_RE.finditer(verdict_text))
    if not match:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=verdict_cost,
            hero_screenshot_url="",
            details="Acceptance agent did not emit a verdict footer.",
        )
    kind = match[-1].group("kind").lower()
    details = _strip_footer(verdict_text).strip()
    return AcceptanceVerdict(
        kind=kind,  # type: ignore[arg-type]
        criteria=list(criteria or []),
        cost=verdict_cost,
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


def _parse_claude_transcript(transcript: str) -> _ParsedTranscript:
    message = ""
    cost = 0.0
    infra_error_details = ""
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
        if not infra_error_details:
            infra_error_details = _infra_error_details(event)
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
    return _ParsedTranscript(
        message=message,
        cost=cost,
        infra_error_details=infra_error_details,
    )


def _infra_error_details(event: dict[str, object]) -> str:
    tool_failure = _tool_failure_details(event)
    if tool_failure:
        return f"Acceptance agent reported tool failure: {tool_failure}"

    if event.get("type") != "result":
        return ""
    subtype = str(event.get("subtype") or "").lower()
    text = _event_text(event)
    signal = " ".join((subtype, text)).lower()
    if subtype and subtype != "success" and _is_cap_or_timeout_signal(signal):
        return text or f"Acceptance runner reported {subtype}."
    if _is_agent_infra_text(signal):
        return text or f"Acceptance runner reported {subtype}."
    return ""


def _tool_failure_details(event: dict[str, object]) -> str:
    for block in _content_blocks(event):
        if block.get("type") != "tool_result" or block.get("is_error") is not True:
            continue
        text = _block_text(block)
        detail = _single_line(text)
        signal = detail.lower()
        if _is_agent_infra_text(signal) or _is_explicit_cap_signal(signal):
            return detail or "tool_result marked is_error"
    return ""


def _content_blocks(event: dict[str, object]) -> list[dict[str, object]]:
    candidates: list[object] = []
    message = event.get("message")
    if isinstance(message, dict):
        candidates.append(message.get("content"))
    candidates.append(event.get("content"))

    blocks: list[dict[str, object]] = []
    for candidate in candidates:
        if isinstance(candidate, list):
            blocks.extend(block for block in candidate if isinstance(block, dict))
        elif isinstance(candidate, dict):
            blocks.append(candidate)
    return blocks


def _block_text(block: dict[str, object]) -> str:
    content = block.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(str(item["text"]))
        return "\n".join(parts)
    text = block.get("text")
    return text if isinstance(text, str) else ""


def _event_text(event: dict[str, object]) -> str:
    for key in ("result", "message", "error"):
        value = event.get(key)
        if isinstance(value, str):
            return _single_line(value)
    return ""


def _single_line(text: str, *, limit: int = 500) -> str:
    normalized = " ".join(text.strip().split())
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit] + "...[truncated]"


def _is_cap_or_timeout_signal(text: str) -> bool:
    return any(
        needle in text
        for needle in (
            "cost cap",
            "cost-cap",
            "max budget",
            "max-budget",
            "maximum budget",
            "time cap",
            "time-cap",
            "timeout",
            "timed out",
            "stall_timeout",
        )
    )


def _is_explicit_cap_signal(text: str) -> bool:
    return any(
        needle in text
        for needle in (
            "cost cap",
            "cost-cap",
            "max budget",
            "max-budget",
            "maximum budget",
            "time cap",
            "time-cap",
        )
    )


def _is_agent_infra_text(text: str) -> bool:
    return (
        ("playwright" in text and ("timeout" in text or "timed out" in text))
        or ("npm install" in text and ("hang" in text or "hung" in text))
        or "dev server failed" in text
        or "preview 404" in text
    )


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
