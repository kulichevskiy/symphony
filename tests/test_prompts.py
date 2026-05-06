from pathlib import Path

from symphony.github import CheckRun, Issue, IssueComment, ReviewComment, TrackedIssue
from symphony.prompts import make_env, render

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


def test_round1_renders_minimal():
    env = make_env(PROMPTS_DIR)
    issue = Issue(
        number=42,
        title="Do the thing",
        body="Fix the widget.",
        labels=["auto"],
        comments=[],
    )
    out = render(
        env,
        "round1.md.j2",
        {
            "issue": issue,
            "repo": type("R", (), {"owner": "ak", "name": "symphony", "default_branch": "main"}),
            "worktree_path": "/tmp/wt",
            "satisfied_deps": [],
        },
    )
    assert "issue #42: Do the thing" in out
    assert "Fix the widget." in out
    assert "auto/42" in out
    assert "ak/symphony" in out
    # No deps section when list is empty
    assert "Satisfied dependencies" not in out
    assert "Issue thread" not in out


def test_round1_includes_comments_and_deps():
    env = make_env(PROMPTS_DIR)
    issue = Issue(
        number=3,
        title="m2",
        body="...",
        labels=["auto"],
        comments=[IssueComment(author="ak", body="bump")],
    )
    deps = [
        TrackedIssue(
            number=1, title="scaffold", state="CLOSED", state_reason="COMPLETED",
            pr_url="https://x/pr/8",
        ),
        TrackedIssue(
            number=2, title="spike", state="CLOSED", state_reason="COMPLETED",
            pr_url=None,
        ),
    ]
    out = render(
        env,
        "round1.md.j2",
        {
            "issue": issue,
            "repo": type("R", (), {"owner": "ak", "name": "symphony", "default_branch": "main"}),
            "worktree_path": "/tmp/wt",
            "satisfied_deps": deps,
        },
    )
    assert "Issue thread" in out
    assert "> ak: bump" in out
    assert "Satisfied dependencies" in out
    assert "- #1 scaffold (PR https://x/pr/8)" in out
    assert "- #2 spike (PR merged)" in out


def _render_review(**ctx):
    env = make_env(PROMPTS_DIR)
    base = {"sha": "deadbee", "comments": [], "ci_failures": [], "review_body": ""}
    base.update(ctx)
    return render(env, "review.md.j2", base)


def test_review_renders_review_body_when_no_inline_comments():
    # Regression: human review with a body but no inline comments must
    # land its actionable text in the prompt — otherwise the agent gets
    # a no-feedback retry and churns toward auto-stuck.
    out = _render_review(review_body="Please refactor the parser to handle empty input.")
    assert "Reviewer note" in out
    assert "Please refactor the parser to handle empty input." in out
    assert "Inline comments" not in out
    assert "CI failures" not in out


def test_review_renders_ci_link_field():
    # Regression: template must use CheckRun.link, not the old
    # `.details_url`. With Jinja's StrictUndefined the wrong attribute
    # would crash render.
    out = _render_review(
        ci_failures=[CheckRun(name="test", bucket="fail", state="FAILURE", link="https://ci/run/123")],
    )
    assert "CI failures" in out
    assert "https://ci/run/123" in out


def test_review_renders_inline_comments_and_ci():
    out = _render_review(
        comments=[
            ReviewComment(
                id=1,
                user_login="bot",
                path="src/x.py",
                line=42,
                body="fix this",
                commit_sha="deadbee",
                created_at="2026-05-06T07:30:00Z",
            )
        ],
        ci_failures=[CheckRun(name="lint", bucket="fail", state="FAILURE", link=None)],
    )
    assert "[src/x.py:42] fix this" in out
    assert "lint (see PR checks)" in out
