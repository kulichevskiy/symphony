"""Per-stage prompt builders. Pure functions of (issue, binding)."""

from __future__ import annotations

from symphony.agent.prompt import implement_prompt


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
