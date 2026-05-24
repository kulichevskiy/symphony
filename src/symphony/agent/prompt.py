"""Per-stage prompt builders.

Pure functions of (issue, binding). One module per stage's prompt makes
template diffs reviewable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

REVIEW_LOG_TAIL_BYTES = 12_000
REQUIRED_CHECK_LOG_TAIL_LINES = 200


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


def review_comment_fix_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    trigger: str,
) -> str:
    """Build the prompt for a Review-stage fix-run triggered by reviewer comments."""
    label_line = ", ".join(labels) if labels else "(no labels)"
    body = issue_body.strip() if issue_body else "(no description)"
    return (
        "You are Symphony's Review-stage fix-run agent.\n"
        "Address the reviewer feedback on the current branch.\n\n"
        "# Review trigger\n\n"
        f"{trigger}\n\n"
        "# Issue\n\n"
        f"## Title\n{issue_title}\n\n"
        f"## Labels\n{label_line}\n\n"
        f"## Description\n{body}\n\n"
        "# Working agreement\n\n"
        "- Make the smallest change that addresses the reviewer feedback.\n"
        "- Commit your changes on the current branch (do not push).\n"
        "- Do not edit unrelated files.\n"
    )


def acceptance_fix_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    acceptance_verdict: str,
) -> str:
    """Build the prompt for an Acceptance-stage fix-run."""
    label_line = ", ".join(labels) if labels else "(no labels)"
    body = issue_body.strip() if issue_body else "(no description)"
    verdict = acceptance_verdict.strip() or "(no acceptance verdict details available)"
    return (
        "You are Symphony's Acceptance-stage fix-run agent.\n"
        "The acceptance agent rejected the current PR because of a product/UX "
        "mismatch, not a code-review defect. Treat this as a fresh "
        "implement-style attempt to make the product behavior match the "
        "Linear issue.\n\n"
        "# Acceptance verdict\n\n"
        f"{verdict}\n\n"
        "# Issue\n\n"
        f"## Title\n{issue_title}\n\n"
        f"## Labels\n{label_line}\n\n"
        f"## Description\n{body}\n\n"
        "# Working agreement\n\n"
        "- Make the smallest change that resolves the product/UX mismatch.\n"
        "- Commit your changes on the current branch (do not push).\n"
        "- Follow strict TDD where practical: reproduce the mismatch first, "
        "then fix it.\n"
        "- Do not edit unrelated files.\n"
    )


def merge_conflict_fix_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    base_branch: str,
    conflicted_files: list[str],
) -> str:
    """Build the prompt for a Review-stage merge-conflict fix-run.

    The orchestrator has already run ``git fetch`` and ``git rebase``; this
    prompt tells the agent to resolve the conflict markers left in the listed
    files.  No git commands should be run by the agent.
    """
    label_line = ", ".join(labels) if labels else "(no labels)"
    body = issue_body.strip() if issue_body else "(no description)"
    files_list = "\n".join(f"- {f}" for f in conflicted_files) if conflicted_files else "(none)"
    return (
        "You are Symphony's merge-conflict resolver.\n"
        f"The orchestrator has started `git rebase origin/{base_branch}` and it stopped\n"
        "because the following files have conflict markers. Your only job is to resolve\n"
        "the `<<<<<<<` / `=======` / `>>>>>>>` markers in each file.\n\n"
        "# Conflicted files\n\n"
        f"{files_list}\n\n"
        "# How to resolve\n\n"
        "For each file above:\n"
        "1. Read the full file content.\n"
        "2. For every conflict block, decide which side to keep (or merge both sides).\n"
        "3. Write the resolved content back — no `<<<<<<<`, `=======`, or `>>>>>>>`"
        " lines.\n\n"
        "Do NOT run any git commands. The orchestrator will stage and continue the"
        " rebase after you finish.\n\n"
        "# Issue\n\n"
        f"## Title\n{issue_title}\n\n"
        f"## Labels\n{label_line}\n\n"
        f"## Description\n{body}\n\n"
        "# Working agreement\n\n"
        "- Resolve conflicts so the PR's intended feature logic is preserved while\n"
        "  integrating any new upstream changes from the base branch.\n"
        "- Do not edit files that are not in the conflicted list above.\n"
        "- Do not run git commands.\n"
    )


def merge_conflict_rebase_fix_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    pr_number: int,
    base_ref: str,
) -> str:
    """Build the prompt for a merge-stage conflict rebase fix-run."""
    label_line = ", ".join(labels) if labels else "(no labels)"
    body = issue_body.strip() if issue_body else "(no description)"
    return (
        "You are Symphony's merge-conflict fix-run agent.\n"
        f"PR #{pr_number} has merge conflicts against `{base_ref}`. Rebase the "
        f"branch onto `origin/{base_ref}`, resolve conflicts, run the test+lint "
        "gates, and push.\n\n"
        "# Issue\n\n"
        f"## Title\n{issue_title}\n\n"
        f"## Labels\n{label_line}\n\n"
        f"## Description\n{body}\n\n"
        "# Working agreement\n\n"
        "- Fetch the latest remote refs before rebasing.\n"
        "- Preserve the PR's intended behavior while integrating upstream changes.\n"
        "- Commit the resolved rebase result if needed, run the repo's test+lint "
        "gates, and push the updated branch.\n"
        "- Do not merge the PR or edit unrelated files.\n"
    )


def merge_required_check_fix_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    pr_number: int,
    head_sha: str,
    merge_error: str,
    failing_checks: Sequence[Mapping[str, object]],
    action_log_tail: str,
    trigger_signature: str = "",
    iteration: str = "",
) -> str:
    """Build the prompt for required-status-check merge fix-runs."""
    label_line = ", ".join(labels) if labels else "(no labels)"
    body = issue_body.strip() if issue_body else "(no description)"
    log_tail = _tail_lines(
        action_log_tail.strip() or "(no GitHub Actions failed-log excerpt available)",
        max_lines=REQUIRED_CHECK_LOG_TAIL_LINES,
    )
    template = _load_prompt_template(
        "merge_required_check_fix.md",
        fallback=(
            "You are Symphony's merge-required-check fix-run agent.\n"
            "GitHub branch protection is blocking PR #{pr_number} because a "
            "required status check is failing.\n\n"
            "# Merge Failure\n\n{merge_error}\n\n"
            "# PR\n\n- PR: #{pr_number}\n- Head SHA: {head_sha}\n"
            "- Trigger signature: {trigger_signature}\n"
            "- Review iteration: {iteration}\n\n"
            "# Required Failing Checks\n\n{failing_checks}\n\n"
            "# Failed GitHub Actions Log Tail\n\n```\n{action_log_tail}\n```\n\n"
            "# Issue\n\n## Title\n{issue_title}\n\n## Labels\n{labels}\n\n"
            "## Description\n{issue_body}\n\n"
            "# Working Agreement\n\n"
            "- Make the smallest change that makes the required check pass.\n"
            "- For StatusContext failures such as Vercel or custom webhooks, "
            "fetch the URL shown above and use it as the primary failure source.\n"
            "- For GitHub Actions failures, use the failed log tail above before "
            "fetching more logs.\n"
            "- Commit your changes on the current branch (do not push).\n"
            "- Do not merge the PR or edit unrelated files.\n"
        ),
    )
    return template.format(
        issue_title=issue_title,
        issue_body=body,
        labels=label_line,
        pr_number=pr_number,
        head_sha=head_sha or "(unknown)",
        merge_error=merge_error.strip() or "(no gh merge error captured)",
        trigger_signature=trigger_signature or "(not recorded)",
        iteration=iteration or "(not recorded)",
        failing_checks=_format_required_check_details(failing_checks),
        action_log_tail=log_tail,
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
        "The PR has passed Codex review on the current HEAD and required CI. "
        "Treat the diff as approved and do one final local housekeeping check "
        "before the orchestrator merges it.\n\n"
        "# Issue\n\n"
        f"## Title\n{issue_title}\n\n"
        f"## Labels\n{label_line}\n\n"
        f"## Description\n{body}\n\n"
        f"## PR\n{pr_url}\n\n"
        "# Working agreement\n\n"
        "- You may only edit lockfiles (package-lock.json, pnpm-lock.yaml, "
        "yarn.lock, uv.lock, poetry.lock, Cargo.lock, go.sum, Gemfile.lock), "
        "generated build manifests, or .changeset/CHANGELOG-style housekeeping "
        "that the repo's contribution rules explicitly require.\n"
        "- Do not edit any source files, tests, configs, schemas, or migrations "
        "under any circumstance.\n"
        "- If you believe a source or test change is needed, do not edit it; "
        "exit successfully without creating a commit so the merge will pause "
        "for human adjudication.\n"
        "- If no housekeeping change is needed, exit successfully without "
        "creating a commit.\n"
        "- Do not merge the PR, push, or edit unrelated files.\n"
    )


def _load_prompt_template(name: str, *, fallback: str) -> str:
    path = Path(__file__).resolve().parents[3] / "prompts" / name
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return fallback


def _format_required_check_details(checks: Sequence[Mapping[str, object]]) -> str:
    if not checks:
        return "(no failing required checks supplied)"
    entries: list[str] = []
    for idx, check in enumerate(checks, start=1):
        name = _check_value(check, "name") or _check_value(check, "context")
        parts = [
            f"## {idx}. {name or '(unnamed check)'}",
            f"- Type: {_check_value(check, '__typename') or '(unknown)'}",
            f"- Context: {_check_value(check, 'context') or '(none)'}",
            f"- State: {_check_value(check, 'state') or '(unknown)'}",
        ]
        conclusion = _check_value(check, "conclusion")
        if conclusion:
            parts.append(f"- Conclusion: {conclusion}")
        description = _check_value(check, "description")
        if description:
            parts.append(f"- Description: {description}")
        target_url = _check_value(check, "targetUrl")
        if target_url:
            parts.append(f"- Target URL: {target_url}")
        details_url = _check_value(check, "detailsUrl")
        if details_url:
            parts.append(f"- Details URL: {details_url}")
        run_id = _check_value(check, "runId")
        if run_id:
            parts.append(f"- Run ID: {run_id}")
        entries.append("\n".join(parts))
    return "\n\n".join(entries)


def _check_value(check: Mapping[str, object], key: str) -> str:
    value = check.get(key)
    if value is None:
        return ""
    return str(value)


def _tail_lines(text: str, *, max_lines: int) -> str:
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return text
    return "\n".join(["...[truncated]", *lines[-max_lines:]])


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
