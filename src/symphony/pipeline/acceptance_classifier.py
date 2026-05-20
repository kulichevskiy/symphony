"""Acceptance verdict classifier.

The Acceptance agent is asked to end its final message with a stable footer.
The classifier reads the Claude stream-json transcript, extracts the final
message and terminal cost, then turns that into an `AcceptanceVerdict`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Literal, TypedDict

AcceptanceVerdictKind = Literal["pass", "reject", "infra_error"]

ACCEPTANCE_FOOTER_PASS = "<!-- symphony-acceptance-verdict: pass -->"
ACCEPTANCE_FOOTER_REJECT = "<!-- symphony-acceptance-verdict: reject -->"
ACCEPTANCE_FOOTER_INFRA_ERROR = (
    "<!-- symphony-acceptance-verdict: infra_error -->"
)
ACCEPTANCE_REASON_QUICK_SKIP_TRIVIAL = "quick_skip_trivial"

_FOOTER_RE = re.compile(
    r"<!--\s*symphony-acceptance-verdict:\s*"
    r"(?P<kind>pass|reject|infra_error)"
    r"(?:\s+reason\s*=\s*(?P<reason>[a-z0-9_:-]+))?\s*-->",
    re.IGNORECASE,
)
_COMMENT_DETAILS_LIMIT = 2500
ACCEPTANCE_CRITERIA_COMMENT_HEADER = "### Symphony extracted acceptance criteria"
ACCEPTANCE_CRITERIA_COMMENT_MARKER = "<!-- symphony-acceptance-criteria -->"
_CHECKBOX_RE = re.compile(
    r"^\s*(?:[-*+]|\d+[.)])\s+\[[ xX]\]\s+(?P<text>.+?)\s*$"
)
_LIST_ITEM_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+(?P<text>.+?)\s*$")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_CRITERIA_HEADING_RE = re.compile(
    r"^(?:acceptance\s+criteria|acceptance\s+checklist|criteria|checklist)"
    r"(?:$|\W.*)",
    re.I,
)
_NON_CRITERIA_HEADING_RE = re.compile(
    r"^(?:"
    r"non[-\s]+criteria\b.*|"
    r"what\s+to\s+build|where\s+to\s+verify|out\s+of\s+scope|"
    r"description|summary|notes?|implementation|context|tasks?|todo"
    r")(?:\s*(?::|-)\s*.*)?$",
    re.I,
)
_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
_MARKDOWN_STRONG_RE = re.compile(r"(\*\*|__)(?P<text>.+?)\1")
_MARKDOWN_CODE_RE = re.compile(r"`(?P<text>[^`]+)`")


class ExtractedCriterion(TypedDict):
    name: str
    predicate: str


@dataclass(frozen=True)
class AcceptanceVerdict:
    kind: AcceptanceVerdictKind
    criteria: list[str]
    cost: float
    hero_screenshot_url: str
    details: str = ""
    reason: str = ""


@dataclass(frozen=True)
class _ParsedTranscript:
    message: str
    cost: float
    infra_error_details: str = ""
    terminal_infra_error_details: str = ""


def acceptance_footer(
    kind: AcceptanceVerdictKind,
    *,
    reason: str | None = None,
) -> str:
    reason_suffix = f" reason={reason}" if reason else ""
    if kind == "pass":
        if reason_suffix:
            return f"<!-- symphony-acceptance-verdict: pass{reason_suffix} -->"
        return ACCEPTANCE_FOOTER_PASS
    if kind == "reject":
        if reason_suffix:
            return f"<!-- symphony-acceptance-verdict: reject{reason_suffix} -->"
        return ACCEPTANCE_FOOTER_REJECT
    if reason_suffix:
        return f"<!-- symphony-acceptance-verdict: infra_error{reason_suffix} -->"
    return ACCEPTANCE_FOOTER_INFRA_ERROR


def acceptance_classifier(
    *,
    transcript: str,
    criteria: list[str] | None = None,
    cost: float | None = None,
) -> AcceptanceVerdict:
    parsed = _parse_claude_transcript(transcript)
    verdict_cost = parsed.cost if cost is None else cost
    if parsed.terminal_infra_error_details:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=verdict_cost,
            hero_screenshot_url="",
            details=parsed.terminal_infra_error_details,
        )
    if not parsed.message:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=verdict_cost,
            hero_screenshot_url="",
            details=(
                parsed.infra_error_details
                or "Acceptance agent did not emit a final message."
            ),
        )
    verdict_text = parsed.message
    match = list(_FOOTER_RE.finditer(verdict_text))
    if not match:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=verdict_cost,
            hero_screenshot_url="",
            details=(
                parsed.infra_error_details
                or "Acceptance agent did not emit a verdict footer."
            ),
        )
    kind = match[-1].group("kind").lower()
    reason = match[-1].group("reason") or ""
    details = _strip_footer(verdict_text).strip()
    if kind != "pass" and parsed.infra_error_details:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=verdict_cost,
            hero_screenshot_url="",
            details=parsed.infra_error_details,
        )
    return AcceptanceVerdict(
        kind=kind,  # type: ignore[arg-type]
        criteria=list(criteria or []),
        cost=verdict_cost,
        hero_screenshot_url="",
        details=details,
        reason=reason,
    )


def extract_acceptance_criteria(linear_description: str) -> list[ExtractedCriterion]:
    criteria: list[ExtractedCriterion] = []
    seen: set[str] = set()
    in_criteria_section = False
    criteria_heading_level: int | None = None
    blocked_nested_heading_level: int | None = None
    list_item_indent: int | None = None
    current_criterion_name = ""
    current_criterion_parts: list[str] = []

    def flush_current_criterion() -> None:
        nonlocal current_criterion_name
        if not current_criterion_parts:
            return
        _append_criterion(
            criteria,
            seen,
            " ".join(current_criterion_parts),
            name_text=current_criterion_name,
        )
        current_criterion_name = ""
        current_criterion_parts.clear()

    for raw_line in linear_description.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue

        heading = _heading(stripped)
        if heading is not None:
            flush_current_criterion()
            list_item_indent = None
            heading_level, heading_title = heading
            if (
                in_criteria_section
                and criteria_heading_level is not None
                and heading_level > criteria_heading_level
            ):
                if (
                    blocked_nested_heading_level is not None
                    and heading_level <= blocked_nested_heading_level
                ):
                    blocked_nested_heading_level = None
                if _is_non_criteria_heading(heading_title):
                    blocked_nested_heading_level = heading_level
                continue
            blocked_nested_heading_level = None
            if _is_criteria_heading(heading_title):
                in_criteria_section = True
                criteria_heading_level = heading_level
                list_item_indent = None
            else:
                in_criteria_section = False
                criteria_heading_level = None
                list_item_indent = None
            continue

        if not in_criteria_section or blocked_nested_heading_level is not None:
            continue
        line = raw_line.rstrip()
        checkbox_match = _CHECKBOX_RE.match(line)
        item_match = checkbox_match or _LIST_ITEM_RE.match(line)
        if item_match:
            item_indent = _leading_indent_width(line)
            item_text = item_match.group("text")
            if list_item_indent is None or item_indent <= list_item_indent:
                flush_current_criterion()
                list_item_indent = item_indent
                current_criterion_name = item_text
            current_criterion_parts.append(item_text)
            continue

        if (
            current_criterion_parts
            and list_item_indent is not None
            and _leading_indent_width(raw_line) > list_item_indent
        ):
            current_criterion_parts.append(stripped)

    flush_current_criterion()
    return criteria


def format_acceptance_criteria_comment(
    criteria: list[ExtractedCriterion],
) -> str:
    body = f"{ACCEPTANCE_CRITERIA_COMMENT_HEADER}\n\n"
    if criteria:
        body += "Symphony will check these criteria before posting the verdict:\n\n"
        for item in criteria:
            body += f"- **{item['name']}**: {item['predicate']}\n"
    else:
        body += "No verifiable criteria - falling back to description match.\n"
    return f"{body}\n{ACCEPTANCE_CRITERIA_COMMENT_MARKER}"


def format_acceptance_verdict_comment(
    *, verdict: AcceptanceVerdict, pr_url: str
) -> str:
    details = verdict.details.strip()
    if len(details) > _COMMENT_DETAILS_LIMIT:
        details = details[:_COMMENT_DETAILS_LIMIT] + "\n...[truncated]"
    prefix = ""
    if (
        verdict.kind == "pass"
        and verdict.reason == ACCEPTANCE_REASON_QUICK_SKIP_TRIVIAL
    ):
        prefix = "**Acceptance: skipped - trivial change.**\n\n"
    body = (
        f"**Acceptance verdict:** `{verdict.kind}`\n\n"
        f"- PR: {pr_url}\n"
        f"- Cost: ${verdict.cost:.4f}\n"
    )
    if verdict.reason:
        body += f"- Reason: `{verdict.reason}`\n"
    body += _criteria_breakdown(verdict)
    if details:
        body += f"\n{details}\n"
    return f"{prefix}{body}\n{acceptance_footer(verdict.kind, reason=verdict.reason)}"


def _parse_claude_transcript(transcript: str) -> _ParsedTranscript:
    message = ""
    cost = 0.0
    infra_error_details = ""
    terminal_infra_error_details = ""
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
        if not terminal_infra_error_details:
            terminal_infra_error_details = _terminal_infra_error_details(event)
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
        terminal_infra_error_details=terminal_infra_error_details,
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


def _terminal_infra_error_details(event: dict[str, object]) -> str:
    if event.get("type") != "result":
        return ""
    subtype = str(event.get("subtype") or "").lower()
    if not subtype or subtype == "success":
        return ""
    text = _event_text(event)
    signal = " ".join((subtype, text)).lower()
    if _is_cap_or_timeout_signal(signal):
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


def _heading(line: str) -> tuple[int, str] | None:
    match = _HEADING_RE.match(line)
    if not match:
        return None
    level = len(line) - len(line.lstrip("#"))
    return level, _clean_markdown(match.group("title")).casefold()


def _is_criteria_heading(heading: str) -> bool:
    return bool(_CRITERIA_HEADING_RE.search(heading))


def _is_non_criteria_heading(heading: str) -> bool:
    return bool(_NON_CRITERIA_HEADING_RE.search(heading))


def _leading_indent_width(text: str) -> int:
    width = 0
    for char in text:
        if char == " ":
            width += 1
        elif char == "\t":
            width += 4
        else:
            break
    return width


def _append_criterion(
    criteria: list[ExtractedCriterion],
    seen: set[str],
    raw_text: str,
    *,
    name_text: str | None = None,
) -> None:
    predicate = _clean_markdown(raw_text)
    if not predicate:
        return
    key = predicate.casefold()
    if key in seen:
        return
    seen.add(key)
    name_source = _clean_markdown(name_text) if name_text else predicate
    criteria.append(
        {
            "name": _criterion_name(name_source) or _criterion_name(predicate),
            "predicate": predicate,
        }
    )


def _clean_markdown(text: str) -> str:
    cleaned = _MARKDOWN_LINK_RE.sub(r"\1", text)
    cleaned = _MARKDOWN_STRONG_RE.sub(r"\g<text>", cleaned)
    cleaned = _MARKDOWN_CODE_RE.sub(r"\g<text>", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" \t-")


def _criterion_name(predicate: str) -> str:
    name = predicate.rstrip(".:;!?").strip()
    return name or predicate


def _criteria_breakdown(verdict: AcceptanceVerdict) -> str:
    body = "\n**Criteria breakdown:**\n"
    criteria = verdict.criteria
    if not criteria:
        return body + "- No verifiable criteria - falling back to description match.\n"
    if (
        verdict.kind == "pass"
        and verdict.reason == ACCEPTANCE_REASON_QUICK_SKIP_TRIVIAL
    ):
        for criterion in criteria:
            body += f"- **{criterion}**: not checked because acceptance was skipped as trivial.\n"
        return body
    if verdict.kind == "infra_error":
        for criterion in criteria:
            body += (
                f"- **{criterion}**: not checked because the acceptance run "
                "failed before review completed.\n"
            )
        return body
    for criterion in criteria:
        body += f"- **{criterion}**: included in the overall acceptance review.\n"
    return body


__all__ = [
    "ACCEPTANCE_CRITERIA_COMMENT_HEADER",
    "ACCEPTANCE_CRITERIA_COMMENT_MARKER",
    "ACCEPTANCE_FOOTER_INFRA_ERROR",
    "ACCEPTANCE_FOOTER_PASS",
    "ACCEPTANCE_FOOTER_REJECT",
    "ACCEPTANCE_REASON_QUICK_SKIP_TRIVIAL",
    "AcceptanceVerdict",
    "AcceptanceVerdictKind",
    "ExtractedCriterion",
    "acceptance_classifier",
    "acceptance_footer",
    "extract_acceptance_criteria",
    "format_acceptance_criteria_comment",
    "format_acceptance_verdict_comment",
]
