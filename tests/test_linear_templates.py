from __future__ import annotations

from symphony.linear.templates import CommentVars, failed, run_started


def test_run_started_comment_uses_emoji_marker() -> None:
    body = run_started(
        CommentVars(stage="implement", repo="org/repo", issue=0, run_id="run-1")
    )

    assert body.startswith("🚀 **Implement starting**")


def test_failed_comment_only_promises_retry_when_enabled() -> None:
    no_retry = failed(
        CommentVars(stage="implement", repo="org/repo", issue=0, error="boom")
    )
    retry = failed(
        CommentVars(
            stage="review",
            repo="org/repo",
            issue=42,
            error="boom",
            auto_retry=True,
        )
    )

    assert "Will auto-retry shortly." not in no_retry
    assert "Will auto-retry shortly." in retry
