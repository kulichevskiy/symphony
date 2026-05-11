from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from symphony.linear.client import Linear


def _comment(cid: str, created_at: str = "2026-05-10T12:00:00+00:00") -> dict[str, Any]:
    return {
        "id": cid,
        "body": "/stop",
        "createdAt": created_at,
        "user": {"name": "user", "isMe": False},
        "externalThread": None,
    }


def _comments_page(
    nodes: list[dict[str, Any]], *, has_next: bool, end_cursor: str | None
) -> dict[str, Any]:
    return {
        "issue": {
            "comments": {
                "pageInfo": {
                    "hasNextPage": has_next,
                    "endCursor": end_cursor,
                },
                "nodes": nodes,
            }
        }
    }


@pytest.mark.asyncio
async def test_comments_since_paginates_all_matching_comments() -> None:
    linear = Linear("test-key")
    calls: list[dict[str, Any]] = []
    pages = [
        _comments_page([_comment("c1")], has_next=True, end_cursor="cursor-1"),
        _comments_page([_comment("c2")], has_next=False, end_cursor=None),
    ]

    async def fake_query(_gql: str, variables: dict[str, Any]) -> dict[str, Any]:
        calls.append(variables)
        return pages.pop(0)

    linear._query = fake_query  # type: ignore[method-assign]
    try:
        comments = await linear.comments_since(
            "iss-1", datetime(2026, 5, 10, 12, tzinfo=UTC)
        )
    finally:
        await linear.aclose()

    assert [c.id for c in comments] == ["c1", "c2"]
    assert [call["cursor"] for call in calls] == [None, "cursor-1"]
