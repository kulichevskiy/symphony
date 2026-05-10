"""Comment bodies are hard-capped at 4 KB before being posted to Linear.

Linear silently rejects very long comment bodies; truncating in the
caller is cheaper than catching a 400 mid-dispatch and gives us
deterministic behaviour for log-tail fields embedded in `failed`
comments.
"""

from __future__ import annotations

from symphony.linear.templates import COMMENT_BYTE_LIMIT, truncate_body


def test_short_body_is_returned_unchanged() -> None:
    body = "hello"
    assert truncate_body(body) == body


def test_oversize_body_is_truncated_under_limit() -> None:
    body = "x" * (COMMENT_BYTE_LIMIT * 2)
    out = truncate_body(body)
    assert len(out.encode("utf-8")) <= COMMENT_BYTE_LIMIT


def test_truncation_marker_is_appended() -> None:
    body = "x" * (COMMENT_BYTE_LIMIT * 2)
    out = truncate_body(body)
    assert out.endswith("…[truncated]") or "truncated" in out


def test_default_limit_is_four_kilobytes() -> None:
    assert COMMENT_BYTE_LIMIT == 4096


def test_tiny_limit_still_respects_byte_budget() -> None:
    for limit in range(0, 8):
        out = truncate_body("x" * 100, limit=limit)
        assert len(out.encode("utf-8")) <= limit
