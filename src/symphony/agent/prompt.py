"""Per-stage prompt builders.

Pure functions of (issue, binding). One module per stage's prompt makes
template diffs reviewable.
"""

from __future__ import annotations

REVIEW_LOG_TAIL_BYTES = 12_000


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


def review_fix_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    trigger: str,
    failing_check_log_tail: str,
) -> str:
    """Build the prompt for a Review-stage fix-run.

    The CI log excerpt intentionally comes first: agents often overweight the
    top of the prompt, and issue #12 requires the red-check tail to be
    prepended to the fix-run prompt.
    """
    label_line = ", ".join(labels) if labels else "(no labels)"
    body = issue_body.strip() if issue_body else "(no description)"
    log_tail = failing_check_log_tail.strip() or "(no failing-check log excerpt available)"
    log_tail = _tail_utf8(log_tail, max_bytes=REVIEW_LOG_TAIL_BYTES)
    return (
        "# Failing check log tail\n\n"
        "```\n"
        f"{log_tail}\n"
        "```\n\n"
        "You are Symphony's Review-stage fix-run agent.\n"
        "Fix the current branch so the Review trigger below is resolved.\n\n"
        "# Review trigger\n\n"
        f"{trigger}\n\n"
        "# Issue\n\n"
        f"## Title\n{issue_title}\n\n"
        f"## Labels\n{label_line}\n\n"
        f"## Description\n{body}\n\n"
        "# Working agreement\n\n"
        "- Make the smallest change that resolves the failing review signal.\n"
        "- Commit your changes on the current branch (do not push).\n"
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


def _tail_utf8(text: str, *, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    if max_bytes <= 0:
        return ""
    suffix = b"\n...[truncated]\n"
    if len(suffix) >= max_bytes:
        return suffix[:max_bytes].decode("utf-8", errors="ignore")
    tail = encoded[-(max_bytes - len(suffix)) :].decode("utf-8", errors="ignore")
    return suffix.decode("utf-8") + tail
