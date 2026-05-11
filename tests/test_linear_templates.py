from __future__ import annotations

from symphony.linear.templates import CommentVars, run_started


def test_run_started_comment_uses_emoji_marker() -> None:
    body = run_started(
        CommentVars(stage="implement", repo="org/repo", issue=0, run_id="run-1")
    )

    assert body.startswith("🚀 **Implement starting**")
