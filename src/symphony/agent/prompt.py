"""Per-stage prompt builders.

Pure functions of (issue, binding). One module per stage's prompt makes
template diffs reviewable.
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


def merge_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    pr_url: str,
) -> str:
    """Build the prompt for the Merge stage's final local pass."""
    label_line = ", ".join(labels) if labels else "(no labels)"
    body = issue_body.strip() if issue_body else "(no description)"
    return (
        "You are Symphony's Merge-stage agent.\n"
        "The PR has passed review and required CI. Do one final local cleanup "
        "pass before the orchestrator merges it.\n\n"
        "# Issue\n\n"
        f"## Title\n{issue_title}\n\n"
        f"## Labels\n{label_line}\n\n"
        f"## Description\n{body}\n\n"
        f"## PR\n{pr_url}\n\n"
        "# Working agreement\n\n"
        "- Inspect the current branch for obvious final commit work only.\n"
        "- If a small final fix is needed, make it and commit it on the current branch.\n"
        "- If no change is needed, exit successfully without creating a commit.\n"
        "- Do not merge the PR, push, or edit unrelated files.\n"
    )
