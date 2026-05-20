"""Acceptance runner for Claude Code headless mode."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from symphony.agent.process import parse_event_line
from symphony.agent.runner import Runner, RunnerEvent, RunnerSpec
from symphony.pipeline.acceptance_classifier import (
    ACCEPTANCE_FOOTER_PASS,
    ACCEPTANCE_FOOTER_REJECT,
    ACCEPTANCE_REASON_QUICK_SKIP_TRIVIAL,
    AcceptanceVerdict,
    acceptance_classifier,
)
from symphony.pipeline.local_review_io import CollectedRunnerOutput

_DIFF_LIMIT_CHARS = 60_000
_CODE_ONLY_MODE = "code_only"
_QUICK_SKIP_DETAILS = "No user-visible behavior described in the ticket or PR diff."
_DOC_EXTENSIONS = {".adoc", ".md", ".mdx", ".rst", ".txt"}
_DOC_FILENAMES = {
    "authors",
    "changelog",
    "codeowners",
    "contributing",
    "license",
    "notice",
    "readme",
    "security",
}
_DEP_FILENAMES = {
    "cargo.lock",
    "cargo.toml",
    "gemfile",
    "gemfile.lock",
    "go.mod",
    "go.sum",
    "package-lock.json",
    "package.json",
    "pnpm-lock.yaml",
    "poetry.lock",
    "pyproject.toml",
    "requirements-dev.txt",
    "requirements.txt",
    "uv.lock",
    "yarn.lock",
}
_DESCRIPTION_TEMPLATE_HEADING_RE = re.compile(
    r"^#+\s*("
    r"what to build|where to verify|acceptance criteria|out of scope|"
    r"description|summary|notes?|implementation|context"
    r")\s*$",
    re.IGNORECASE,
)
_MARKDOWN_HEADING_RE = re.compile(r"^#+\s*")
_MARKDOWN_LIST_RE = re.compile(r"^(?:[-*+]|\d+[.)])\s+")
_MARKDOWN_CHECKBOX_RE = re.compile(r"^\[[ xX]\]\s+")
_EXPLICIT_TRIVIAL_DESCRIPTION_LINE_RE = re.compile(
    r"(?:"
    r"no\s+(?:user[- ]visible\s+)?behaviou?r(?:\s+change)?|"
    r"(?:internal\s+)?refactor(?:\s+[\w./`'\" -]+)?\s+"
    r"(?:only|with\s+no\s+behaviou?r\s+change|no\s+behaviou?r\s+change)"
    r")\.?",
    re.IGNORECASE,
)
_NON_TRIVIAL_DESCRIPTION_RE = re.compile(
    r"\b("
    r"api|endpoint|ui|ux|screen|page|button|form|modal|dialog|"
    r"workflow|flow|user|customer|operator|admin|login|auth|oauth|"
    r"payment|checkout|feature|behaviou?r|visual|visible|browser|"
    r"playwright|preview|server|database|db|persist|state|render|"
    r"click|submit"
    r")\b",
    re.IGNORECASE,
)
_TRIVIAL_DESCRIPTION_LINE_RES = (
    re.compile(
        r"[\w./`'\" -]*(?:typo|spelling|grammar|copyedit|copy edit)"
        r"[\w./`'\" -]*\.?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:update|fix|edit|improve|refresh)?\s*"
        r"(?:docs?|documentation|readme|changelog|license|notice|comments?)"
        r"(?:\s+(?:only|typo|spelling|grammar|copy|copyedit|copy edit|"
        r"links?|wording|text|reference|examples?))*\.?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:bump|update|upgrade)\s+"
        r"(?:[\w./`'\"-]+\s+)?"
        r"(?:dependencies|dependency|deps?|packages?|lockfile|version)"
        r"(?:\s+[\w./`'\" -]+)?\.?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:rename|move)\s+[\w./`'\" -]+\s+(?:to|as)\s+[\w./`'\" -]+\.?",
        re.IGNORECASE,
    ),
)
_DEPENDENCY_DESCRIPTION_RE = re.compile(
    r"\b(dependency|dependencies|dep bump|bump|upgrade|update|lockfile|version)\b",
    re.IGNORECASE,
)
_INTERNAL_ONLY_DESCRIPTION_RE = re.compile(
    r"\b("
    r"internal refactor|refactor only|rename only|no behavior change|"
    r"no behaviour change|no user-visible behavior|no user visible behavior"
    r")\b",
    re.IGNORECASE,
)
_USER_FACING_PATH_PARTS = {
    "api",
    "app",
    "components",
    "frontend",
    "pages",
    "routes",
    "screens",
    "ui",
    "views",
}
_CLAUDE_ACCEPTANCE_PERMISSION_MODE = "default"
_CLAUDE_ACCEPTANCE_DISALLOWED_TOOLS = ",".join(
    (
        "Bash",
        "Read",
        "Edit",
        "Write",
        "MultiEdit",
        "Glob",
        "Grep",
        "LS",
        "NotebookRead",
        "NotebookEdit",
        "WebFetch",
        "WebSearch",
        "TodoWrite",
        "Task",
    )
)


async def run_acceptance(
    *,
    runner: Runner,
    run_id: str,
    workspace_path: Path,
    mode: str,
    linear_description: str,
    pr_diff_summary: str,
    taste_guide: str = "",
    criteria: list[str] | None = None,
    stall_secs: float = 300,
    max_budget_usd: float | None = None,
) -> AcceptanceVerdict:
    if mode != _CODE_ONLY_MODE:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=0.0,
            hero_screenshot_url="",
            details=_unsupported_mode_details(mode),
        )

    quick_skip = quick_skip_trivial_acceptance(
        linear_description=linear_description,
        pr_diff_summary=pr_diff_summary,
        criteria=criteria,
    )
    if quick_skip is not None:
        return quick_skip

    prompt = build_acceptance_prompt(
        mode=mode,
        linear_description=linear_description,
        pr_diff_summary=pr_diff_summary,
        taste_guide=taste_guide,
        criteria=criteria,
    )
    spec = RunnerSpec(
        run_id=run_id,
        workspace_path=workspace_path,
        command=build_acceptance_command(
            prompt=prompt,
            max_budget_usd=max_budget_usd,
        ),
        stall_secs=stall_secs,
        stage="acceptance",
    )
    acceptance_run = await _collect_acceptance_output(
        runner,
        spec,
        max_budget_usd=max_budget_usd,
        wall_clock_secs=stall_secs,
    )
    collected = acceptance_run.output
    if acceptance_run.abort_details:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=acceptance_run.cost,
            hero_screenshot_url="",
            details=acceptance_run.abort_details,
        )
    if not collected.ok_exit:
        parsed = acceptance_classifier(
            transcript=collected.stdout,
            criteria=criteria,
        )
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=max(parsed.cost, acceptance_run.cost),
            hero_screenshot_url="",
            details=_failed_run_details(
                collected,
                parsed_details=(
                    parsed.details if parsed.kind == "infra_error" else ""
                ),
                time_cap_secs=stall_secs,
            ),
        )
    return acceptance_classifier(
        transcript=collected.stdout,
        criteria=criteria,
    )


@dataclass(frozen=True)
class _AcceptanceRunOutput:
    output: CollectedRunnerOutput
    abort_details: str = ""
    cost: float = 0.0


async def _collect_acceptance_output(
    runner: Runner,
    spec: RunnerSpec,
    *,
    max_budget_usd: float | None,
    wall_clock_secs: float,
) -> _AcceptanceRunOutput:
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    terminal_kind = "exit"
    returncode: int | None = None
    spawn_error: str | None = None
    stall_timeout = False
    tracked_cost = 0.0
    started_at = time.monotonic()
    iterator: AsyncIterator[RunnerEvent] = runner.run(spec).__aiter__()

    async def abort(details: str) -> _AcceptanceRunOutput:
        await runner.kill(spec.run_id)
        await _close_iterator(iterator)
        return _AcceptanceRunOutput(
            output=CollectedRunnerOutput(
                stdout="\n".join(stdout_parts),
                stderr="\n".join(stderr_parts),
                terminal_kind="abort",
                returncode=None,
                spawn_error=None,
                stall_timeout=False,
            ),
            abort_details=details,
            cost=tracked_cost,
        )

    while True:
        remaining = wall_clock_secs - (time.monotonic() - started_at)
        if remaining <= 0:
            return await abort(_time_cap_exceeded_details(wall_clock_secs))

        next_event: asyncio.Task[RunnerEvent] = asyncio.create_task(
            _next_runner_event(iterator)
        )
        done, _pending = await asyncio.wait({next_event}, timeout=remaining)
        if not done:
            await runner.kill(spec.run_id)
            next_event.cancel()
            with suppress(asyncio.CancelledError):
                await next_event
            await _close_iterator(iterator)
            return _AcceptanceRunOutput(
                output=CollectedRunnerOutput(
                    stdout="\n".join(stdout_parts),
                    stderr="\n".join(stderr_parts),
                    terminal_kind="abort",
                    returncode=None,
                    spawn_error=None,
                    stall_timeout=False,
                ),
                abort_details=_time_cap_exceeded_details(wall_clock_secs),
                cost=tracked_cost,
            )

        try:
            event = next_event.result()
        except StopAsyncIteration:
            break

        if event.kind == "stdout" and event.line is not None:
            stdout_parts.append(event.line)
            usage = parse_event_line(event.line)
            if usage is not None:
                tracked_cost = max(tracked_cost, usage.cost_usd)
                if _cost_cap_reached(max_budget_usd, tracked_cost):
                    return await abort(
                        _cost_cap_exceeded_details(max_budget_usd, tracked_cost)
                    )
        elif event.kind == "stderr" and event.line is not None:
            stderr_parts.append(event.line)
        elif event.kind == "exit":
            terminal_kind = "exit"
            returncode = event.returncode
            break
        elif event.kind == "stall_timeout":
            terminal_kind = "stall_timeout"
            stall_timeout = True
            break
        elif event.kind == "spawn_failed":
            terminal_kind = "spawn_failed"
            spawn_error = event.error
            break

    await _close_iterator(iterator)
    return _AcceptanceRunOutput(
        output=CollectedRunnerOutput(
            stdout="\n".join(stdout_parts),
            stderr="\n".join(stderr_parts),
            terminal_kind=terminal_kind,
            returncode=returncode,
            spawn_error=spawn_error,
            stall_timeout=stall_timeout,
        ),
        cost=tracked_cost,
    )


async def _close_iterator(iterator: object) -> None:
    aclose = getattr(iterator, "aclose", None)
    if aclose is None:
        return
    with suppress(Exception):  # noqa: BLE001
        await aclose()


async def _next_runner_event(
    iterator: AsyncIterator[RunnerEvent],
) -> RunnerEvent:
    return await iterator.__anext__()


def _cost_cap_reached(max_budget_usd: float | None, cost_usd: float) -> bool:
    return (
        max_budget_usd is not None
        and max_budget_usd > 0
        and cost_usd >= max_budget_usd
    )


def build_acceptance_command(
    *, prompt: str, max_budget_usd: float | None = None
) -> list[str]:
    command = [
        "claude",
        "--print",
        "--output-format",
        "stream-json",
        "--verbose",
        "--permission-mode",
        _CLAUDE_ACCEPTANCE_PERMISSION_MODE,
        "--disallowedTools",
        _CLAUDE_ACCEPTANCE_DISALLOWED_TOOLS,
    ]
    if max_budget_usd is not None:
        command.extend(["--max-budget-usd", f"{max_budget_usd:.4f}"])
    command.append(prompt)
    return command


def build_acceptance_prompt(
    *,
    mode: str,
    linear_description: str,
    pr_diff_summary: str,
    taste_guide: str = "",
    criteria: list[str] | None = None,
) -> str:
    if mode != _CODE_ONLY_MODE:
        raise ValueError(_unsupported_mode_details(mode))

    description = linear_description.strip() or "(no Linear description)"
    diff = _truncate_diff(pr_diff_summary.strip() or "(no PR diff available)")
    taste_guide_section = _taste_guide_section(taste_guide)
    criteria_section = _criteria_section(criteria)
    return (
        "You are Symphony's Acceptance-stage agent. Your only job is to "
        "decide whether the PR diff satisfies the Linear ticket description.\n\n"
        "# Mode\n\n"
        f"mode: {mode}\n\n"
        "# Mode-specific instructions for code_only\n\n"
        "## First phase: quick-skip decision\n\n"
        "- Read only the Linear description and PR diff summary first.\n"
        "- Decide trivial / non-trivial before any other acceptance work.\n"
        "- Classify as trivial only when there is no user-visible behavior "
        "worth verifying, such as docs-only typos, dependency bumps, renames, "
        "or internal refactors with no behavior change.\n"
        "- If trivial, skip criteria extraction, Playwright, browser "
        "automation, dev servers, previews, screenshots, tests, and taste "
        "guides. Emit a pass verdict with reason quick_skip_trivial using "
        "this exact footer:\n"
        "<!-- symphony-acceptance-verdict: pass reason=quick_skip_trivial -->\n"
        "- If in doubt, classify as non-trivial and proceed with the full "
        "code_only check below.\n\n"
        "## Full code_only check for non-trivial tickets\n\n"
        "- Compare only the Linear description and the PR diff summary below.\n"
        "- Reject when the diff obviously implements something different from "
        "the requested behavior.\n"
        "- Pass when the diff and description agree at the code level.\n"
        "- If the taste guide below contains hard rules and the PR diff violates "
        "one, reject and cite the specific taste-guide rule.\n"
        "- Do not run Playwright, browser automation, a dev server, or tests.\n"
        "- Do not inspect screenshots or preview URLs.\n"
        "- Do not modify files, commit, push, or merge anything.\n\n"
        f"{taste_guide_section}"
        f"{criteria_section}"
        "# Linear description\n\n"
        f"{description}\n\n"
        "# PR diff summary\n\n"
        "```diff\n"
        f"{diff}\n"
        "```\n\n"
        "# Response format\n\n"
        "Write a short rationale. End your final message with EXACTLY ONE of "
        "these footers on its own line:\n\n"
        f"{ACCEPTANCE_FOOTER_PASS}\n"
        f"{ACCEPTANCE_FOOTER_REJECT}\n"
    )


def _taste_guide_section(taste_guide: str) -> str:
    content = taste_guide.strip()
    if not content:
        return ""
    return (
        "# Taste guide\n\n"
        "Global guide content appears first. Per-binding guide content appears "
        "after it and may add stricter rules, but it cannot override or silence "
        "global hard rules.\n\n"
        f"{content}\n\n"
    )


def _criteria_section(criteria: list[str] | None) -> str:
    items = [item.strip() for item in criteria or [] if item.strip()]
    if not items:
        return (
            "# Extracted acceptance criteria\n\n"
            "No verifiable criteria were extracted; fall back to matching the "
            "overall Linear description against the PR diff.\n\n"
        )
    body = "# Extracted acceptance criteria\n\n"
    for item in items:
        body += f"- {item}\n"
    return f"{body}\n"


def quick_skip_trivial_acceptance(
    *,
    linear_description: str,
    pr_diff_summary: str,
    criteria: list[str] | None = None,
) -> AcceptanceVerdict | None:
    """Return a pass verdict for obvious no-behavior tickets.

    This deliberately handles only low-risk cases. Ambiguous tickets fall
    through to the full acceptance agent prompt.
    """
    description = linear_description.strip()
    diff = pr_diff_summary.strip()
    if not description or not diff:
        return None
    paths = _changed_paths(diff)
    if not paths or not _is_exclusively_trivial_description(description):
        return None

    lower_description = description.casefold()
    if all(_is_doc_path(path) for path in paths):
        return _quick_skip_verdict(criteria)
    if (
        all(_is_dependency_path(path) for path in paths)
        and _DEPENDENCY_DESCRIPTION_RE.search(lower_description)
    ):
        return _quick_skip_verdict(criteria)
    if (
        _INTERNAL_ONLY_DESCRIPTION_RE.search(lower_description)
        and not any(_looks_user_facing_path(path) for path in paths)
    ):
        return _quick_skip_verdict(criteria)
    return None


def _is_exclusively_trivial_description(description: str) -> bool:
    lines = _meaningful_description_lines(description)
    return bool(lines) and all(_is_trivial_description_line(line) for line in lines)


def _meaningful_description_lines(description: str) -> list[str]:
    lines: list[str] = []
    in_fence = False
    for raw_line in description.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            lines.append(line)
            continue
        if _DESCRIPTION_TEMPLATE_HEADING_RE.fullmatch(line):
            continue
        line = _MARKDOWN_HEADING_RE.sub("", line).strip()
        line = _MARKDOWN_LIST_RE.sub("", line).strip()
        line = _MARKDOWN_CHECKBOX_RE.sub("", line).strip()
        if line:
            lines.append(line)
    return lines


def _is_trivial_description_line(line: str) -> bool:
    if _EXPLICIT_TRIVIAL_DESCRIPTION_LINE_RE.fullmatch(line):
        return True
    if _NON_TRIVIAL_DESCRIPTION_RE.search(line):
        return False
    return any(pattern.fullmatch(line) for pattern in _TRIVIAL_DESCRIPTION_LINE_RES)


def _quick_skip_verdict(criteria: list[str] | None) -> AcceptanceVerdict:
    return AcceptanceVerdict(
        kind="pass",
        criteria=list(criteria or []),
        cost=0.0,
        hero_screenshot_url="",
        details=_QUICK_SKIP_DETAILS,
        reason=ACCEPTANCE_REASON_QUICK_SKIP_TRIVIAL,
    )


def _changed_paths(diff: str) -> list[str]:
    paths: list[str] = []
    for line in diff.splitlines():
        if not line.startswith("diff --git "):
            continue
        path = _diff_git_new_path(line[len("diff --git ") :])
        if path is None:
            continue
        if path and path != "/dev/null":
            paths.append(path)
    return paths


def _diff_git_new_path(header: str) -> str | None:
    prefix = "a/"
    separator = " b/"
    if not header.startswith(prefix):
        return None

    rest = header[len(prefix) :]
    offset = 0
    while True:
        index = rest.find(separator, offset)
        if index == -1:
            break
        old_path = rest[:index]
        new_path = rest[index + len(separator) :]
        if old_path == new_path:
            return new_path
        offset = index + 1

    index = rest.find(separator)
    if index == -1:
        return None
    new_path = rest[index + len(separator) :]
    if separator in new_path:
        return None
    return new_path


def _is_doc_path(path: str) -> bool:
    path_obj = Path(path)
    parts = {part.casefold() for part in path_obj.parts}
    if {"doc", "docs", "documentation"} & parts:
        return True
    stem = path_obj.stem.casefold()
    suffix = path_obj.suffix.casefold()
    return stem in _DOC_FILENAMES or suffix in _DOC_EXTENSIONS


def _is_dependency_path(path: str) -> bool:
    return Path(path).name.casefold() in _DEP_FILENAMES


def _looks_user_facing_path(path: str) -> bool:
    parts = {part.casefold() for part in Path(path).parts}
    return bool(parts & _USER_FACING_PATH_PARTS)


def _truncate_diff(diff: str) -> str:
    if len(diff) <= _DIFF_LIMIT_CHARS:
        return diff
    return diff[:_DIFF_LIMIT_CHARS] + "\n...[truncated]"


def _unsupported_mode_details(mode: str) -> str:
    return (
        f"Acceptance mode {mode!r} is not supported by the Claude code-only "
        "runner; only 'code_only' can run without dev or preview artifacts."
    )


def _failed_run_details(
    collected: CollectedRunnerOutput,
    *,
    parsed_details: str = "",
    time_cap_secs: float = 0.0,
) -> str:
    if parsed_details:
        return _prefix_cap_details(parsed_details)
    if collected.terminal_kind == "spawn_failed":
        return f"Acceptance runner spawn_failed: {collected.spawn_error or 'unknown'}"
    if collected.stall_timeout:
        return _time_cap_exceeded_details(time_cap_secs)
    return f"Acceptance runner exited rc={collected.returncode}."


def _cost_cap_exceeded_details(max_budget_usd: float | None, cost_usd: float) -> str:
    cap = 0.0 if max_budget_usd is None else max_budget_usd
    return (
        "cost_cap_exceeded: acceptance cost "
        f"${cost_usd:.4f} reached cap ${cap:.4f}."
    )


def _time_cap_exceeded_details(time_cap_secs: float) -> str:
    minutes = time_cap_secs / 60.0
    return (
        "time_cap_exceeded: acceptance exceeded wall-clock cap "
        f"of {minutes:.2f} minutes ({time_cap_secs:.1f}s)."
    )


def _prefix_cap_details(details: str) -> str:
    lower = details.lower()
    if "cost_cap_exceeded" in lower or "time_cap_exceeded" in lower:
        return details
    if "cost" in lower and ("cap" in lower or "budget" in lower):
        return f"cost_cap_exceeded: {details}"
    if "timeout" in lower or "timed out" in lower or "time cap" in lower:
        return f"time_cap_exceeded: {details}"
    return details


__all__ = [
    "build_acceptance_command",
    "build_acceptance_prompt",
    "quick_skip_trivial_acceptance",
    "run_acceptance",
]
