from pathlib import Path

from symphony.github import Issue, IssueComment, TrackedIssue
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
