"""Acceptance runner for Claude Code headless mode."""

from __future__ import annotations

import re
from pathlib import Path

from symphony.agent.runner import Runner, RunnerSpec
from symphony.pipeline.acceptance_classifier import (
    ACCEPTANCE_FOOTER_PASS,
    ACCEPTANCE_FOOTER_REJECT,
    ACCEPTANCE_REASON_QUICK_SKIP_TRIVIAL,
    AcceptanceVerdict,
    acceptance_classifier,
)
from symphony.pipeline.local_review_io import CollectedRunnerOutput, collect_runner_output

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
_TRIVIAL_DESCRIPTION_RE = re.compile(
    r"\b("
    r"typo|readme|docs?|documentation|comment|copyedit|copy edit|"
    r"dependency|dependencies|dep bump|bump|lockfile|rename|"
    r"internal refactor|refactor only|no behavior change|no behaviour change|"
    r"no user-visible behavior|no user visible behavior"
    r")\b",
    re.IGNORECASE,
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
    stall_secs: int = 300,
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
    collected = await collect_runner_output(runner, spec)
    if not collected.ok_exit:
        parsed = acceptance_classifier(
            transcript=collected.stdout,
            criteria=criteria,
        )
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=parsed.cost,
            hero_screenshot_url="",
            details=_failed_run_details(collected),
        )
    return acceptance_classifier(
        transcript=collected.stdout,
        criteria=criteria,
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
) -> str:
    if mode != _CODE_ONLY_MODE:
        raise ValueError(_unsupported_mode_details(mode))

    description = linear_description.strip() or "(no Linear description)"
    diff = _truncate_diff(pr_diff_summary.strip() or "(no PR diff available)")
    taste_guide_section = _taste_guide_section(taste_guide)
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
    if not paths or not _TRIVIAL_DESCRIPTION_RE.search(description):
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
        parts = line.split()
        if len(parts) < 4:
            continue
        path = parts[3]
        if path.startswith("b/"):
            path = path[2:]
        if path and path != "/dev/null":
            paths.append(path)
    return paths


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


def _failed_run_details(collected: CollectedRunnerOutput) -> str:
    if collected.terminal_kind == "spawn_failed":
        return f"Acceptance runner spawn_failed: {collected.spawn_error or 'unknown'}"
    if collected.stall_timeout:
        return "Acceptance runner stalled before completing successfully."
    return f"Acceptance runner exited rc={collected.returncode}."


__all__ = [
    "build_acceptance_command",
    "build_acceptance_prompt",
    "quick_skip_trivial_acceptance",
    "run_acceptance",
]
