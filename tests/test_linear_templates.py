from __future__ import annotations

import pytest

from symphony.linear.templates import (
    CommentVars,
    awaiting_approval,
    failed,
    fix_pushed,
    implement_blocked,
    run_started,
    stage_done,
    stuck_loop_escape,
)


def test_run_started_comment_uses_emoji_marker() -> None:
    body = run_started(
        CommentVars(stage="implement", repo="org/repo", issue=0, run_id="run-1")
    )

    assert body.startswith("🚀 **Implement starting**")


def _token_vars(stage: str) -> CommentVars:
    return CommentVars(
        stage=stage,
        repo="org/repo",
        issue=42,
        next_stage="next",
        input_tokens=10,
        output_tokens=20,
        cache_write_tokens=8,
        cache_read_tokens=10,
    )


@pytest.mark.parametrize(
    ("template", "stage"),
    [
        (stage_done, "implement"),
        (awaiting_approval, "merge"),
        (stuck_loop_escape, "review"),
        (failed, "review"),
        (fix_pushed, "review"),
    ],
)
def test_token_block_replaces_cost(template, stage) -> None:
    body = template(_token_vars(stage))

    # No dollar cost line anymore — only the token breakdown.
    assert "Cost" not in body
    assert "cost" not in body
    assert "$0" not in body
    # Raw breakdown: in/out/cache w/r.
    assert "Tokens: in 10 · out 20 · cache w 8 / r 10" in body
    # Effective (weighted) total — the unit the per-issue budget gates on:
    # 10 + 20 + 8*1.25 + 10*0.1 = 41.
    assert "eff 41" in body


def test_implement_blocked_comment_states_verbatim_ask_and_retry() -> None:
    reason = "authorize the Supabase MCP at https://example.com/oauth then approve"
    body = implement_blocked(
        CommentVars(
            stage="implement",
            repo="org/repo",
            issue=0,
            run_id="run-1",
            error=reason,
        )
    )
    # Glanceable emoji marker for phone push notifications.
    assert body[0].isascii() is False or body.startswith("🔒")
    # The agent's SYMPHONY_BLOCKED reason is reproduced verbatim.
    assert reason in body
    # Tells the operator exactly how to resume.
    assert "$retry" in body
    # Tells the operator their prior work is preserved.
    assert "preserved" in body.lower() or "uncommitted" in body.lower()


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
