"""Per-stage prompt builders. Pure functions of (issue, binding)."""

from __future__ import annotations

from symphony.agent.prompt import (
    acceptance_fix_prompt,
    implement_prompt,
    merge_prompt,
    merge_required_check_fix_prompt,
)


def test_implement_prompt_includes_title_body_and_labels() -> None:
    prompt = implement_prompt(
        issue_title="Add OAuth login",
        issue_body="Users should sign in via Google.",
        labels=["feature", "auth"],
    )
    assert "Add OAuth login" in prompt
    assert "Users should sign in via Google." in prompt
    assert "feature" in prompt
    assert "auth" in prompt


def test_implement_prompt_handles_empty_labels() -> None:
    prompt = implement_prompt(
        issue_title="Fix typo",
        issue_body="",
        labels=[],
    )
    assert "Fix typo" in prompt
    # Empty body / labels must not crash; the prompt should still render.
    assert prompt.strip() != ""


def test_implement_prompt_is_deterministic() -> None:
    a = implement_prompt(issue_title="t", issue_body="b", labels=["x"])
    b = implement_prompt(issue_title="t", issue_body="b", labels=["x"])
    assert a == b


def test_acceptance_fix_prompt_frames_product_mismatch_not_code_review() -> None:
    prompt = acceptance_fix_prompt(
        issue_title="Improve onboarding",
        issue_body="The signup flow should explain the workspace step.",
        labels=["product", "ux"],
        acceptance_verdict=(
            "The PR adds validation but still does not explain the workspace step."
        ),
    )

    assert "Acceptance-stage fix-run agent" in prompt
    assert "product/UX mismatch" in prompt
    assert "not a code-review defect" in prompt
    assert "The PR adds validation" in prompt
    assert "Review-stage fix-run agent" not in prompt


def test_merge_prompt_limits_final_edits_to_housekeeping_files() -> None:
    prompt = merge_prompt(
        issue_title="Update generated artifacts",
        issue_body="The PR is ready to merge.",
        labels=["improvement"],
        pr_url="https://github.com/example/repo/pull/123",
    )

    assert "lockfiles" in prompt
    assert "generated build manifests" in prompt
    assert ".changeset/CHANGELOG-style housekeeping" in prompt
    assert "Do not edit any source files, tests, configs, schemas, or migrations" in prompt
    assert "source or test change is needed" in prompt
    assert "exit successfully without creating a commit" in prompt
    assert "merge will pause" in prompt


def test_merge_required_check_fix_prompt_includes_status_context_and_merge_error() -> None:
    prompt = merge_required_check_fix_prompt(
        issue_title="Fix deploy",
        issue_body="The save flow must ship.",
        labels=["bug"],
        pr_number=273,
        head_sha="abc123",
        merge_error=(
            "gh pr merge 273 --squash --repo vibecamp-org/vibecamp exited 1: "
            "the base branch policy prohibits the merge"
        ),
        failing_checks=[
            {
                "__typename": "StatusContext",
                "name": "Vercel",
                "context": "Vercel",
                "targetUrl": "https://vercel.com/vibecamp/deployments/123",
                "description": "Deployment failed.",
                "state": "FAILURE",
            }
        ],
        action_log_tail="",
    )

    assert "merge-required-check fix-run agent" in prompt
    assert "PR #273" in prompt
    assert "abc123" in prompt
    assert "Vercel" in prompt
    assert "https://vercel.com/vibecamp/deployments/123" in prompt
    assert "Deployment failed." in prompt
    assert "base branch policy prohibits the merge" in prompt
    assert "fetch the URL" in prompt
