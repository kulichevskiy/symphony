"""Acceptance runner for Claude Code headless mode."""

from __future__ import annotations

from pathlib import Path

from symphony.agent.runner import Runner, RunnerSpec
from symphony.pipeline.acceptance_classifier import (
    ACCEPTANCE_FOOTER_PASS,
    ACCEPTANCE_FOOTER_REJECT,
    AcceptanceVerdict,
    acceptance_classifier,
)
from symphony.pipeline.local_review_io import collect_runner_output

_DIFF_LIMIT_CHARS = 60_000


async def run_acceptance(
    *,
    runner: Runner,
    run_id: str,
    workspace_path: Path,
    mode: str,
    linear_description: str,
    pr_diff_summary: str,
    criteria: list[str] | None = None,
    stall_secs: int = 300,
    max_budget_usd: float | None = None,
) -> AcceptanceVerdict:
    prompt = build_acceptance_prompt(
        mode=mode,
        linear_description=linear_description,
        pr_diff_summary=pr_diff_summary,
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
) -> str:
    description = linear_description.strip() or "(no Linear description)"
    diff = _truncate_diff(pr_diff_summary.strip() or "(no PR diff available)")
    return (
        "You are Symphony's Acceptance-stage agent. Your only job is to "
        "decide whether the PR diff satisfies the Linear ticket description.\n\n"
        "# Mode\n\n"
        f"mode: {mode}\n\n"
        "# Mode-specific instructions for code_only\n\n"
        "- Compare only the Linear description and the PR diff summary below.\n"
        "- Reject when the diff obviously implements something different from "
        "the requested behavior.\n"
        "- Pass when the diff and description agree at the code level.\n"
        "- Do not run Playwright, browser automation, a dev server, or tests.\n"
        "- Do not inspect screenshots, preview URLs, or a taste guide.\n"
        "- Do not modify files, commit, push, or merge anything.\n\n"
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


def _truncate_diff(diff: str) -> str:
    if len(diff) <= _DIFF_LIMIT_CHARS:
        return diff
    return diff[:_DIFF_LIMIT_CHARS] + "\n...[truncated]"


__all__ = [
    "build_acceptance_command",
    "build_acceptance_prompt",
    "run_acceptance",
]
