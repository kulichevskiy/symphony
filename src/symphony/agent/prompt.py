"""Per-stage prompt builders.

Pure functions of (issue, binding). One module per stage's prompt makes
template diffs reviewable; for now Implement is the only one wired.
"""

from __future__ import annotations


def implement_prompt(*, issue_title: str, issue_body: str, labels: list[str]) -> str:
    """Build the system+user prompt for the Implement stage.

    The body shows the agent the entire Linear issue context so it can
    decide on its own scope. It also reminds the agent to commit on the
    current branch — the orchestrator pushes after the run, but does not
    do its own commits.
    """
    label_line = ", ".join(labels) if labels else "(no labels)"
    body = issue_body.strip() if issue_body else "(no description)"
    return (
        "You are Symphony's Implement-stage agent.\n"
        "Make the code changes that satisfy the following Linear issue.\n\n"
        "# Issue\n\n"
        f"## Title\n{issue_title}\n\n"
        f"## Labels\n{label_line}\n\n"
        f"## Description\n{body}\n\n"
        "# Working agreement\n\n"
        "- Make the smallest change that satisfies the issue.\n"
        "- Commit your changes on the current branch (do not push).\n"
        "- Follow strict TDD: write a failing test first, then the code.\n"
        "- Do not edit unrelated files.\n"
    )
