"""Cross-cutting + domain-shaped pure helpers for the poll loop (SYM-143).

Pure move out of ``poll/__init__.py`` — bodies are unchanged. Holds generic
utilities (usage/time/command builders) plus the ``pr_view`` / ``status_check``
/ ``required_check`` predicate families, co-located here until a dedicated
domain module exists. Re-exported by the package ``__init__``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict

import aiosqlite

from ... import db
from ...agent.claude_cli import (
    BUILDER_SETTING_SOURCES,
    claude_builder_allowed_tools,
    claude_builder_settings,
)
from ...agent.codex_cli import build_codex_workspace_write_command
from ...agent.codex_models import DEFAULT_CODEX_MODEL
from ...config import ResolvedRole
from ...pipeline.cost_guard import UsageDelta
from ...pipeline.local_review_loop import LoopResult
from ...pipeline.state_machine import classify_termination
from ...tracker import Issue as LinearIssue

_ACCEPTANCE_MISSING_WHERE_TO_VERIFY_NOTE = (
    "Acceptance: degraded to code-only — no `Where to verify` in ticket description"
)

# GitHub rejects PR bodies past this size, so an oversized ticket description
# is cut rather than failing delivery.
PR_BODY_MAX_CHARS = 65_536
# `gh pr create --body` is handed to execve() as a single argv value, which
# Linux caps well under 128 KiB regardless of character count (e.g. CJK text
# can blow the byte budget long before the char budget). Stay comfortably
# under that ceiling.
PR_BODY_MAX_BYTES = 120_000
_PR_BODY_TRUNCATION_NOTICE = "Description truncated; see the source ticket"
# Matches a CommonMark fence marker line: 0-3 spaces of indent, then a run of
# 3+ backticks or tildes, then the rest of the line (info string when
# opening, must be blank when closing). Used to track fence open/close state
# when truncating, so a cut that lands inside a ```, ~~~, or 4+-backtick
# fence gets closed with a matching marker instead of leaving it dangling.
_FENCE_MARKER_RE = re.compile(r"^ {0,3}(?P<marker>`{3,}|~{3,})(?P<rest>.*)$")
# A block quote marker: 0-3 spaces indent, `>`, optional single trailing
# space/tab, one or more levels deep (`> > `). Used to detect a fenced code
# block nested inside a block quote, which `_FENCE_MARKER_RE` alone misses
# since it only allows plain leading spaces before the fence run.
_BLOCKQUOTE_PREFIX_RE = re.compile(r"^(?: {0,3}>[ \t]?)+")
# An HTML comment delimiter. CommonMark comments don't nest: the first `-->`
# after an `<!--` closes it, regardless of any `<!--` seen in between. Used to
# track comment open/close state when truncating, so a cut that lands inside
# an otherwise-valid `<!-- ... -->` doesn't strand the truncation notice and
# footer inside the (now unterminated) comment.
_HTML_COMMENT_TOKEN_RE = re.compile(r"<!--|-->")
# CommonMark indented code block: 4+ spaces or a tab, then non-whitespace.
_INDENTED_CODE_LINE_RE = re.compile(r"^(?: {4,}|\t)\S")
# Linear rich links: `<issue …>ENG-1</issue>`, `<pull-request … />`. The bare
# attrs branch excludes quotes *and whitespace* so it can never overlap with
# either the quoted branches or the `\s+` separator between attrs — each
# attr token is matched exactly once, so a long whitespace run (or an
# unbalanced quote) can't be rescanned once per attrs split. `selfclose`
# short-circuits the trailing text/close-tag group entirely, so a
# self-closing tag can never reach forward and swallow a later same-name
# tag. The text group's lookahead is barred from crossing another same-name
# tag (open or close), so an unpaired/url-less tag matches on its own
# instead of stretching to the next real tag's close — this also bounds the
# forward scan per tag.
_RICH_LINK_TAG_RE = re.compile(
    r"<(?P<tag>issue|pull-request)"
    r"(?P<attrs>(?:\s+(?:\"[^\"]*\"|'[^']*'|[^<>\"'\s/]|/(?!\s*>))+)*)"
    r"\s*(?P<selfclose>/)?>"
    r"(?(selfclose)|(?:(?P<text>(?:[^<]|<(?!/?(?P=tag)\b))*)</(?P=tag)>)?)",
    re.IGNORECASE,
)
_TAG_ATTR_RE = re.compile(r"([\w-]+)\s*=\s*(?:\"([^\"]*)\"|'([^']*)')")


def _sum_usage(left: UsageDelta, right: UsageDelta) -> UsageDelta:
    return UsageDelta(
        cost_usd=left.cost_usd + right.cost_usd,
        input_tokens=left.input_tokens + right.input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
        cache_write_tokens=left.cache_write_tokens + right.cache_write_tokens,
        cache_read_tokens=left.cache_read_tokens + right.cache_read_tokens,
    )


def _acceptance_has_where_to_verify(description: str) -> bool:
    for raw_line in description.splitlines():
        heading = _normalize_acceptance_section_heading(raw_line)
        if heading == "where to verify" or heading.startswith("where to verify:"):
            return True
    return False


def _normalize_acceptance_section_heading(line: str) -> str:
    line = line.strip()
    line = re.sub(r"^#{1,6}\s*", "", line)
    line = re.sub(r"\s+#{1,6}\s*$", "", line)
    line = line.strip(" *_`")
    return re.sub(r"\s+", " ", line).casefold()


def _acceptance_degrade_note(description: str) -> str | None:
    if _acceptance_has_where_to_verify(description):
        return None
    return _ACCEPTANCE_MISSING_WHERE_TO_VERIFY_NOTE


def _parse_optional_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return _parse_rfc3339(raw)
    except ValueError:
        return None


def build_pr_title(issue: LinearIssue) -> str:
    return f"[{issue.identifier}] {issue.title}"


def build_pr_body(issue: LinearIssue) -> str:
    """Body for a newly created PR: the ticket description plus a footer.

    Built from the issue snapshot the stage already holds — no tracker read.
    The description is trusted Markdown and passes through untouched (Jira and
    Linear alike); the only rewrite turns Linear `<issue …>` / `<pull-request …>`
    rich-link tags into ordinary Markdown links. An empty description leaves
    only the footer. An oversized description is cut at a line boundary and
    labelled, so the footer survives and `gh pr create` still accepts the body.
    """
    footer = f"Relates to {issue.url}"
    description = _trim_boundary_blank_lines(_linear_rich_links_to_markdown(issue.description))
    if not description:
        return footer
    description = _close_full_dangling_fence(description)
    footer = f"---\n\n{footer}"
    body = f"{description}\n\n{footer}"
    if len(body) <= PR_BODY_MAX_CHARS and len(body.encode("utf-8")) <= PR_BODY_MAX_BYTES:
        return body
    tail = f"\n\n{_PR_BODY_TRUNCATION_NOTICE}\n\n{footer}"
    char_budget = PR_BODY_MAX_CHARS - len(tail)
    byte_budget = PR_BODY_MAX_BYTES - len(tail.encode("utf-8"))
    kept = _head_lines_within(description, char_budget, byte_budget)
    kept = _close_dangling_markup(kept, char_budget, byte_budget)
    return f"{kept}{tail}"


def _close_full_dangling_fence(text: str) -> str:
    """Append the fence closer if `text` itself ends inside an unterminated bare fence.

    Covers the direct-return path (a body that fits without truncation): a
    ticket description that never closes a fence it opened would otherwise
    swallow the `---`/tracker-link footer inside that code block. The
    truncation path doesn't need this — it closes a fence at the cut point
    via `_close_dangling_markup` regardless of whether the source description
    was itself well-formed.
    """
    lines = text.split("\n")
    closer = _fence_states_per_line(lines)[-1]
    if closer is None:
        return text
    return f"{text}\n{closer}"


def _trim_boundary_blank_lines(text: str) -> str:
    """Drop leading/trailing all-blank lines without touching interior indentation.

    `.strip()` eats leading whitespace from the very start of the string,
    which would strip an indented Markdown code block's opening line right
    along with it (turning it into unindented prose while later lines in the
    same block stay indented). Only whole blank lines at each edge are
    dropped, so a description that opens or closes with an indented line
    keeps its indentation.
    """
    lines = text.split("\n")
    start = 0
    while start < len(lines) and lines[start].strip() == "":
        start += 1
    end = len(lines)
    while end > start and lines[end - 1].strip() == "":
        end -= 1
    return "\n".join(lines[start:end])


def _fence_states_per_line(lines: list[str]) -> list[str | None]:
    """Per-prefix-length fence closer, for `lines[0:i+1]` at index `i`.

    Tracks CommonMark fence rules across ``` and ~~~ fences of any length
    (3+): a fence is closed only by a same-character run at least as long as
    its opener, indented by at most 3 spaces, with nothing but trailing
    whitespace after it — so a 3-backtick line inside a fence opened with 4
    backticks (or a same-length line with trailing text) doesn't falsely
    close it. Computed once via a single forward scan, so a caller trimming
    the tail one line at a time can look up each prefix's state in O(1)
    instead of rescanning the retained text on every trimmed line.
    """
    states: list[str | None] = []
    open_char: str | None = None
    open_len = 0
    for line in lines:
        m = _FENCE_MARKER_RE.match(line)
        if m is not None:
            marker, rest = m["marker"], m["rest"]
            char, run_len = marker[0], len(marker)
            if open_char is None:
                if not (char == "`" and "`" in rest):  # backtick info string forbids a backtick
                    open_char, open_len = char, run_len
            elif char == open_char and run_len >= open_len and rest.strip() == "":
                open_char, open_len = None, 0
        states.append(open_char * open_len if open_char is not None else None)
    return states


def _html_comment_states_per_line(lines: list[str], fence_states: list[str | None]) -> list[bool]:
    """Per-prefix-length HTML-comment-open flag for `lines[0:i+1]` at index `i`.

    Mirrors `_fence_states_per_line`'s single-forward-scan shape: comment
    state carries across lines, toggling on each `<!--`/`-->` token in
    document order (CommonMark comments don't nest, so a `-->` always closes
    regardless of intervening `<!--`). `fence_states` (aligned 1:1 with
    `lines`) skips lines covered by an open bare fence, since a literal
    `<!--`/`-->` inside fenced code is not an HTML comment.
    """
    states: list[bool] = []
    open_comment = False
    for line, fence_state in zip(lines, fence_states, strict=True):
        if fence_state is None:
            for token in _HTML_COMMENT_TOKEN_RE.findall(line):
                open_comment = token == "<!--"
        states.append(open_comment)
    return states


def _close_dangling_markup(kept: str, char_budget: int, byte_budget: int) -> str:
    """Append closer(s) for a fence and/or HTML comment `kept` was cut inside of.

    Trims further lines off the tail until whichever closers apply fit
    alongside the truncation notice/footer that follows. Handling both in one
    pass (rather than closing the fence, then separately checking for a
    dangling comment) means a further trim triggered by the comment closer
    can't undo the fence closer that a prior, longer `n` already accounted
    for.
    """
    lines = kept.split("\n")
    fence_states = _fence_states_per_line(lines)
    comment_states = _html_comment_states_per_line(lines, fence_states)
    char_len = [0] * (len(lines) + 1)
    byte_len = [0] * (len(lines) + 1)
    for i, line in enumerate(lines):
        sep = 1 if i else 0
        char_len[i + 1] = char_len[i] + sep + len(line)
        byte_len[i + 1] = byte_len[i] + sep + len(line.encode("utf-8"))

    n = len(lines)
    while n > 0:
        parts = [p for p in (fence_states[n - 1], "-->" if comment_states[n - 1] else None) if p]
        if not parts:
            break
        closer = "\n".join(parts)
        if (
            char_len[n] + 1 + len(closer) <= char_budget
            and byte_len[n] + 1 + len(closer.encode("utf-8")) <= byte_budget
        ):
            return "\n".join(lines[:n]) + f"\n{closer}"
        n -= 1
    return "\n".join(lines[:n])


def _head_lines_within(text: str, char_budget: int, byte_budget: int) -> str:
    """Longest line-boundary prefix of `text` that fits both budgets.

    Bodies are also handed to `gh` as a single argv value, so a byte ceiling
    matters as much as the char ceiling (Unicode text can blow the byte
    budget well before the char budget). Falls back to a hard cut when even
    the first line is too long, so a single-line description cannot defeat
    either limit.
    """
    kept: list[str] = []
    used_chars = 0
    used_bytes = 0
    for line in text.split("\n"):
        sep = 1 if kept else 0
        used_chars += len(line) + sep
        used_bytes += len(line.encode("utf-8")) + sep
        if used_chars > char_budget or used_bytes > byte_budget:
            break
        kept.append(line)
    if kept:
        return "\n".join(kept)
    return _char_prefix_within_bytes(text, char_budget, byte_budget)


def _char_prefix_within_bytes(text: str, char_budget: int, byte_budget: int) -> str:
    """Prefix of `text` within `char_budget` chars, cut further to fit `byte_budget`."""
    prefix = text[: max(char_budget, 0)]
    encoded = prefix.encode("utf-8")
    if len(encoded) <= byte_budget:
        return prefix
    return encoded[: max(byte_budget, 0)].decode("utf-8", errors="ignore")


def _escape_markdown_link_label(label: str) -> str:
    """Escape `\\`, `[`, `]` so a user-controlled label can't break `[label](url)`."""
    return label.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


# Inline code span: a backtick run, then anything but a newline or that same
# run, up to the next occurrence of that exact run — mirrors CommonMark's
# "matching backtick count" delimiter rule closely enough to keep a literal
# `<issue …>` example inside inline code from being rewritten. The leading
# `(?<!`)` anchors the run to its start so a long backtick run is only ever
# attempted once (not once per backtick in it), and the possessive `++`/`*+`
# quantifiers forbid backtracking into an already-matched run — together
# these keep a long backtick run that never finds its match linear instead of
# quadratic. The trailing `(?!`)` rejects a closing run that is longer than
# the opener (e.g. opening on a single backtick but only finding a run of
# two): CommonMark requires the closer to be a backtick string of *exactly*
# the opener's length, not merely at least that long, so a too-long run is no
# closer at all and the possessive body can't backtrack to try a shorter one.
_INLINE_CODE_SPAN_RE = re.compile(r"(?<!`)(?P<tick>`++)(?:(?!(?P=tick))[^\n])*+(?P=tick)(?!`)")


def _fence_open(line: str) -> tuple[str, int] | None:
    """(char, length) if *line* opens a bare (non-block-quoted) fence, else None."""
    m = _FENCE_MARKER_RE.match(line)
    if m is None or (m["marker"][0] == "`" and "`" in m["rest"]):
        return None
    return m["marker"][0], len(m["marker"])


def _fence_closes(rest: str, fence_char: str, fence_len: int) -> bool:
    m = _FENCE_MARKER_RE.match(rest)
    return (
        m is not None
        and m["marker"][0] == fence_char
        and len(m["marker"]) >= fence_len
        and m["rest"].strip() == ""
    )


def _quoted_fence_open(line: str) -> tuple[str, str, int] | None:
    """(block-quote prefix, char, length) if *line* opens a fence nested in a
    block quote (e.g. `> \\`\\`\\``), else None."""
    bq = _BLOCKQUOTE_PREFIX_RE.match(line)
    if bq is None or not bq[0]:
        return None
    opened = _fence_open(line[bq.end() :])
    if opened is None:
        return None
    return bq[0], opened[0], opened[1]


def _split_fenced_code_blocks(text: str) -> list[tuple[bool, list[str]]]:
    """Partition *text* into line runs, tagging Markdown code regions.

    Covers the three shapes that can hide a literal `<issue …>` /
    `<pull-request …>` example from being mistaken for a real rich link:
    a bare fenced (``` / ~~~) code block, a fence nested one level inside a
    block quote (`> \\`\\`\\``), and a 4-space/tab indented code block.
    Reuses the same fence open/close rules as `_fence_states_per_line`.
    """
    blocks: list[tuple[bool, list[str]]] = []
    current: list[str] = []
    mode = "prose"
    fence_char = ""
    fence_len = 0
    quote_prefix = ""
    prev_blank = True

    def flush(next_mode: str) -> None:
        nonlocal current, mode
        if current:
            blocks.append((mode != "prose", current))
        current = []
        mode = next_mode

    for line in text.split("\n"):
        if mode == "fence":
            current.append(line)
            if _fence_closes(line, fence_char, fence_len):
                flush("prose")
            prev_blank = False
            continue
        if mode == "quoted_fence":
            if line.startswith(quote_prefix):
                current.append(line)
                if _fence_closes(line[len(quote_prefix) :], fence_char, fence_len):
                    flush("prose")
                prev_blank = False
                continue
            flush("prose")
            # falls through to re-process `line` as prose below
        if mode == "indent":
            if line.strip() == "":
                current.append(line)
                prev_blank = True
                continue
            if _INDENTED_CODE_LINE_RE.match(line):
                current.append(line)
                prev_blank = False
                continue
            flush("prose")
            # falls through to re-process `line` as prose below

        opened = _fence_open(line)
        if opened is not None:
            flush("fence")
            fence_char, fence_len = opened
            current.append(line)
            prev_blank = False
            continue
        quoted = _quoted_fence_open(line)
        if quoted is not None:
            flush("quoted_fence")
            quote_prefix, fence_char, fence_len = quoted
            current.append(line)
            prev_blank = False
            continue
        if line.strip() == "":
            current.append(line)
            prev_blank = True
            continue
        if prev_blank and _INDENTED_CODE_LINE_RE.match(line):
            flush("indent")
            current.append(line)
            prev_blank = False
            continue
        current.append(line)
        prev_blank = False
    if current:
        blocks.append((mode != "prose", current))
    return blocks


def _linear_rich_links_to_markdown(description: str) -> str:
    """Rewrite Linear rich-link tags as `[label](url)`.

    Linear serializes inline issue / PR references as pseudo-HTML tags that
    GitHub renders as unlinked text (or nothing at all when self-closing).
    Label preference: tag text, then `identifier`, then `title`, then the URL.
    A tag without a `url` attribute is not a Linear rich link (or is
    malformed) and passes through verbatim. Markdown code regions — fenced
    blocks, fences nested in a block quote, indented code blocks, and inline
    code spans — are skipped entirely, so a literal tag-shaped example inside
    any of them is never rewritten.
    """

    def replace(match: re.Match[str]) -> str:
        attrs: dict[str, str] = {
            name.lower(): double or single
            for name, double, single in _TAG_ATTR_RE.findall(match["attrs"] or "")
        }
        url = attrs.get("url", "")
        if not url:
            return match[0]
        text = (match["text"] or "").strip()
        raw_label = text or attrs.get("identifier") or attrs.get("title") or url
        return f"[{_escape_markdown_link_label(raw_label)}]({url})"

    def rewrite_outside_inline_code(text: str) -> str:
        pieces: list[str] = []
        last = 0
        for span in _INLINE_CODE_SPAN_RE.finditer(text):
            pieces.append(_RICH_LINK_TAG_RE.sub(replace, text[last : span.start()]))
            pieces.append(span[0])
            last = span.end()
        pieces.append(_RICH_LINK_TAG_RE.sub(replace, text[last:]))
        return "".join(pieces)

    return "\n".join(
        "\n".join(block) if is_code else rewrite_outside_inline_code("\n".join(block))
        for is_code, block in _split_fenced_code_blocks(description)
    )


def role_codex_model(role: ResolvedRole) -> str:
    """Codex `--model` for a resolved role — see `ResolvedRole.codex_model_arg`."""
    return role.codex_model_arg()


def role_claude_model(role: ResolvedRole) -> str | None:
    """Claude `--model` for a resolved role — see `ResolvedRole.claude_model_arg`."""
    return role.claude_model_arg()


def role_attribution_codex_model(role: ResolvedRole) -> str | None:
    """Usage-attribution codex model — see `ResolvedRole.attribution_codex_model`."""
    return role.attribution_codex_model()


def build_runner_command(
    agent: str,
    prompt: str,
    *,
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    effort: str | None = None,
    workspace_path: Path | None = None,
    mcp_servers: Mapping[str, Any] | None = None,
) -> list[str]:
    """Per-runner argv for the Implement stage prompt.

    `mcp_servers` is the binding's MCP allowlist. Claude spawns always run
    `--strict-mcp-config` so the agent only sees servers the binding
    explicitly grants — none by default. Codex MCP wiring lives in its own
    config.toml and is unaffected.

    `claude_model` is the resolved `implement` role's Claude model: set →
    `--model <alias>`, unset → no flag (CLI default). It is ignored for codex.

    `effort` is the resolved role's reasoning effort: for claude it becomes a
    dedicated `--effort <level>` flag; for codex it becomes
    `--config model_reasoning_effort="<v>"`. Unset → no flag (CLI default).
    """
    if agent == "claude":
        command = [
            "claude",
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            "--strict-mcp-config",
            # PreToolUse deny-hook: hard-block the background-task machinery a
            # one-shot dispatch cannot honor (SYM-224). --setting-sources ""
            # keeps our inline hook the only settings layer, so no project
            # settings can disable it.
            "--setting-sources",
            BUILDER_SETTING_SOURCES,
            "--settings",
            claude_builder_settings(),
            # Pre-approve the full builder surface, plus mcp__<name>__* for
            # each MCP server the binding grants — see agent/claude_cli.py
            # for why a fresh containerized auth volume otherwise auto-denies
            # every mutation. Prompt rides behind `--` (SYM-42 idiom).
            "--allowedTools",
            claude_builder_allowed_tools(mcp_servers),
        ]
        if mcp_servers:
            command.extend(["--mcp-config", json.dumps({"mcpServers": dict(mcp_servers)})])
        if claude_model is not None:
            command.extend(["--model", claude_model])
        if effort is not None:
            command.extend(["--effort", effort])
        command.extend(["--", prompt])
        return command
    if agent == "codex":
        if workspace_path is None:
            raise ValueError("workspace_path is required for codex write runs")
        return build_codex_workspace_write_command(
            prompt=prompt,
            codex_model=codex_model,
            effort=effort,
        )
    raise ValueError(f"unknown agent {agent!r}")


def build_fix_runner_command(
    agent: str,
    prompt: str,
    *,
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    effort: str | None = None,
    workspace_path: Path | None = None,
    mcp_servers: Mapping[str, Any] | None = None,
) -> list[str]:
    """argv for a Review-stage fix-run.

    Fix-runs go through the binding's CLI (claude or codex), NOT through
    the GitHub `@codex review` bot. The bot is only consulted via PR
    comments; the resolved `fix` role's agent is what drives code changes
    in response to its feedback.

    `claude_model` is the resolved `fix` role's Claude model: set →
    `--model <alias>`, unset → no flag (CLI default). It is ignored for codex.
    `effort` is the resolved role's reasoning effort, threaded through the
    same way `build_runner_command` does (SYM-192).
    """
    return build_runner_command(
        agent,
        prompt,
        codex_model=codex_model,
        claude_model=claude_model,
        effort=effort,
        workspace_path=workspace_path,
        mcp_servers=mcp_servers,
    )


def build_merge_runner_command(
    agent: str,
    prompt: str,
    *,
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    effort: str | None = None,
    workspace_path: Path | None = None,
    mcp_servers: Mapping[str, Any] | None = None,
) -> list[str]:
    """argv for the Merge-stage final local pass."""
    return build_runner_command(
        agent,
        prompt,
        codex_model=codex_model,
        claude_model=claude_model,
        effort=effort,
        workspace_path=workspace_path,
        mcp_servers=mcp_servers,
    )


_PR_URL_RE = re.compile(r"/pull/(\d+)")


def pr_number_from_url(url: str) -> int | None:
    """Extract the PR number from a `gh pr create` URL.

    `gh pr create` prints `https://github.com/OWNER/REPO/pull/<N>` on
    success (sometimes with trailing whitespace). The Review-stage poll
    needs that `<N>` to post `@codex review` and to fetch the snapshot.
    """
    if not url:
        return None
    m = _PR_URL_RE.search(url.strip())
    if m is None:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _parse_rfc3339(s: str) -> datetime:
    """Linear timestamps end in `Z`; Python's `fromisoformat` accepts the
    `+00:00` form. Normalize before parsing."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _pr_view_is_merged(view: dict[str, object]) -> bool:
    return (
        bool(view.get("mergedAt"))
        or bool(view.get("merged"))
        or str(view.get("state") or "").upper() == "MERGED"
    )


def _pr_view_is_closed(view: dict[str, object]) -> bool:
    return str(view.get("state") or "").upper() == "CLOSED"


def _pr_view_has_merge_conflict(view: dict[str, object]) -> bool:
    mergeable = str(view.get("mergeable") or "").upper()
    merge_state = str(view.get("mergeStateStatus") or view.get("merge_state_status") or "").upper()
    return mergeable == "CONFLICTING" or merge_state == "DIRTY"


def _pr_view_skips_required_check_fix(view: dict[str, object]) -> bool:
    mergeable = str(view.get("mergeable") or "").upper()
    merge_state = str(view.get("mergeStateStatus") or view.get("merge_state_status") or "").upper()
    return mergeable == "CONFLICTING" or merge_state in {"BEHIND", "CONFLICTING", "DIRTY"}


def _pr_view_is_clean_mergeable(view: dict[str, object]) -> bool:
    mergeable = str(view.get("mergeable") or "").upper()
    merge_state = str(view.get("mergeStateStatus") or view.get("merge_state_status") or "").upper()
    return mergeable == "MERGEABLE" and merge_state == "CLEAN"


def _pr_base_ref_from_view(view: dict[str, object]) -> str | None:
    raw = view.get("baseRefName") or view.get("base_ref_name") or view.get("baseRef")
    if raw is None:
        return None
    base_ref = str(raw).strip()
    return base_ref or None


_REQUIRED_CHECK_FAILURE_STATES = {
    "FAILURE",
    "FAILED",
    "ERROR",
    "CANCELLED",
    "CANCELED",
    "TIMED_OUT",
    "ACTION_REQUIRED",
    "STARTUP_FAILURE",
    "STALE",
}


def _dict_list(raw: object) -> list[dict[str, Any]] | None:
    if not isinstance(raw, list):
        return None
    return [entry for entry in raw if isinstance(entry, dict)]


def _edge_nodes(raw: object) -> list[dict[str, Any]] | None:
    edges = _dict_list(raw)
    if edges is None:
        return None
    return [edge["node"] for edge in edges if isinstance(edge.get("node"), dict)]


def _status_rollup_nodes(raw: object) -> list[dict[str, Any]]:
    direct = _dict_list(raw)
    if direct is not None:
        return direct
    if isinstance(raw, dict):
        return _mapping_status_rollup_nodes(raw)
    return []


def _mapping_status_rollup_nodes(raw: dict[str, object]) -> list[dict[str, Any]]:
    nodes = _dict_list(raw.get("nodes"))
    if nodes is not None:
        return nodes
    edges = _edge_nodes(raw.get("edges"))
    if edges is not None:
        return edges
    return _dict_list(raw.get("contexts")) or []


def _status_check_identity(check: Mapping[str, object]) -> str:
    return (
        str(check.get("context") or "").strip()
        or str(check.get("name") or "").strip()
        or str(check.get("workflowName") or "").strip()
        or "(unnamed)"
    )


def _status_check_names(check: Mapping[str, object]) -> set[str]:
    names: set[str] = set()
    for key in ("context", "name", "workflowName"):
        value = str(check.get(key) or "").strip()
        if value:
            names.add(value)
    return names


def _status_check_sha(check: Mapping[str, object]) -> str:
    for key in ("sha", "commitOid", "commit_oid"):
        value = str(check.get(key) or "").strip()
        if value:
            return value
    commit = check.get("commit")
    if isinstance(commit, Mapping):
        return str(commit.get("oid") or commit.get("sha") or "").strip()
    return ""


def _status_check_failed(check: Mapping[str, object]) -> bool:
    state = str(check.get("state") or check.get("status") or check.get("__typename") or "").upper()
    conclusion = str(check.get("conclusion") or "").upper()
    return state in _REQUIRED_CHECK_FAILURE_STATES or conclusion in _REQUIRED_CHECK_FAILURE_STATES


# Terminal-success states across both rollup shapes: a `StatusContext` reports
# `state`, a `CheckRun` reports `status`+`conclusion`. SKIPPED/NEUTRAL count as
# non-blocking passes (GitHub treats them as green for branch protection).
_STATUS_CHECK_SUCCESS_STATES = {"SUCCESS", "NEUTRAL", "SKIPPED"}


def _status_check_succeeded(check: Mapping[str, object]) -> bool:
    """True only when *check* has completed successfully (SYM-108).

    A `CheckRun` that has not reached `COMPLETED` is still in flight, so it is
    neither a success nor a failure — the caller treats it as pending.
    """
    if _status_check_failed(check):
        return False
    status = str(check.get("status") or "").upper()
    if status and status != "COMPLETED":
        return False
    conclusion = str(check.get("conclusion") or "").upper()
    if conclusion:
        return conclusion in _STATUS_CHECK_SUCCESS_STATES
    state = str(check.get("state") or "").upper()
    if state:
        return state in _STATUS_CHECK_SUCCESS_STATES
    return False


def _no_signal_head_check_state(view: dict[str, object]) -> str:
    """Classify the CI rollup on the PR head for the no_signal merge gate.

    Returns "green" (≥1 check, all complete and successful), "failed" (≥1
    check failed), "pending" (≥1 check, none failed but some still running),
    or "none" (no check reports on the head). SYM-108: a clean no_signal
    bypass merges only on "green"; "none" needs a verify_cmd/opt-in; "pending"
    keeps polling; "failed" defers to the review/required-check fix path.
    """
    head_sha = str(view.get("headRefOid") or "")
    nodes: list[dict[str, Any]] = []
    for check in _status_rollup_nodes(view.get("statusCheckRollup")):
        check_sha = _status_check_sha(check)
        if check_sha and head_sha and check_sha != head_sha:
            continue
        nodes.append(check)
    if not nodes:
        return "none"
    if any(_status_check_failed(check) for check in nodes):
        return "failed"
    if all(_status_check_succeeded(check) for check in nodes):
        return "green"
    return "pending"


def _required_check_detail(check: Mapping[str, object]) -> dict[str, object]:
    detail: dict[str, object] = {}
    for key in (
        "__typename",
        "name",
        "context",
        "workflowName",
        "state",
        "status",
        "conclusion",
        "targetUrl",
        "detailsUrl",
        "description",
    ):
        value = check.get(key)
        if value is not None:
            detail[key] = value
    run_id = _status_check_run_id(check)
    if run_id:
        detail["runId"] = run_id
    return detail


def _status_check_run_id(check: Mapping[str, object]) -> str:
    for key in ("runId", "run_id"):
        value = str(check.get(key) or "").strip()
        if value:
            return value
    workflow_run = check.get("workflowRun")
    if isinstance(workflow_run, Mapping):
        for key in ("databaseId", "database_id", "id"):
            value = str(workflow_run.get(key) or "").strip()
            if value:
                return value
    for key in ("detailsUrl", "targetUrl"):
        url = str(check.get(key) or "")
        match = re.search(r"/actions/runs/([^/?#]+)", url)
        if match is not None:
            return match.group(1)
    for key in ("databaseId", "database_id"):
        value = str(check.get(key) or "").strip()
        if value:
            return value
    return ""


def _required_check_trigger_signature(
    *,
    head_sha: str,
    failing_checks: list[dict[str, object]],
) -> str:
    contexts = sorted(_status_check_identity(check) for check in failing_checks)
    contexts_hash = hashlib.sha256("\n".join(contexts).encode("utf-8")).hexdigest()[:12]
    return f"required_check_failure:{head_sha}:{contexts_hash}"


def _github_commit_url(repo: str, sha: str) -> str:
    """Return a browser commit URL for *sha* in [HOST/]OWNER/REPO."""
    if not sha:
        return ""
    parts = repo.split("/")
    if len(parts) == 3:
        host, owner, name = parts
    elif len(parts) == 2:
        host = "github.com"
        owner, name = parts
    else:
        return ""
    return f"https://{host}/{owner}/{name}/commit/{sha}"


def _pr_url_for_state(*, repo: str, pr_number: int | None, pr_url: str) -> str:
    if pr_url:
        return pr_url
    if pr_number is not None:
        return f"https://github.com/{repo}/pull/{pr_number}"
    return "(no PR)"


# SYM-145: relocated from `poll/__init__.py` so the slash-command mixin and the
# package `__init__` can share it without a circular import.
NEEDS_HUMAN_APPROVAL_LABEL = "needs-human-approval"


def _needs_human_approval_label_present(issue: LinearIssue) -> bool:
    return NEEDS_HUMAN_APPROVAL_LABEL in issue.labels


async def _add_run_usage(conn: aiosqlite.Connection, run_id: str, usage: UsageDelta) -> None:
    if not usage.has_usage():
        return
    await db.runs.add_usage(
        conn,
        run_id,
        cost_usd=usage.cost_usd,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cache_write_tokens=usage.cache_write_tokens,
        cache_read_tokens=usage.cache_read_tokens,
    )


class _TerminationKwargs(TypedDict):
    kind: str
    detail: str
    returncode: int | None


def _termination_kwargs(
    *,
    status: str,
    final_kind: str | None = None,
    returncode: int | None = None,
    exc: BaseException | str | None = None,
    reason: str | None = None,
) -> _TerminationKwargs:
    kind, detail = classify_termination(
        status=status,
        final_kind=final_kind,
        returncode=returncode,
        exc=exc,
        reason=reason,
    )
    return {"kind": kind, "detail": detail, "returncode": returncode}


def _local_review_termination_reason(result: LoopResult | None) -> str:
    if result is None:
        return "local-review session failed"
    if result.error:
        return result.error
    return f"local-review ended with {result.outcome.value}"
