from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from symphony.linear import queries
from symphony.linear.client import Linear, LinearIssue


def _comment(cid: str, created_at: str = "2026-05-10T12:00:00+00:00") -> dict[str, Any]:
    return {
        "id": cid,
        "body": "$stop",
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


@pytest.mark.asyncio
async def test_issue_external_snapshot_returns_latest_comments_desc() -> None:
    linear = Linear("test-key")
    calls: list[tuple[str, dict[str, Any]]] = []

    def comment(idx: int) -> dict[str, Any]:
        return {
            "id": f"c{idx}",
            "body": f"comment {idx}",
            "createdAt": f"2026-05-17T11:0{idx}:00Z",
            "user": {"name": f"User {idx}"},
        }

    async def fake_query(gql: str, variables: dict[str, Any]) -> dict[str, Any]:
        calls.append((gql, variables))
        if gql == queries.ISSUE_EXTERNAL_SNAPSHOT:
            return {
                "issue": {
                    "id": "iss-1",
                    "identifier": "ENG-1",
                    "url": "https://linear.app/issue/ENG-1/title",
                    "updatedAt": "2026-05-17T11:10:00Z",
                    "state": {"name": "Done"},
                    "labels": {"nodes": [{"name": "symphony"}]},
                    "comments": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "ignored"},
                        "nodes": [
                            comment(0),
                            comment(1),
                            comment(2),
                            comment(3),
                            comment(4),
                            comment(5),
                        ],
                    },
                }
            }
        raise AssertionError(f"unexpected query: {gql}")

    linear._query = fake_query  # type: ignore[method-assign]
    try:
        snapshot = await linear.issue_external_snapshot("iss-1")
    finally:
        await linear.aclose()

    assert snapshot["state"] == "Done"
    assert snapshot["updated_at"] == "2026-05-17T11:10:00Z"
    assert snapshot["labels"] == ["symphony"]
    assert [comment["comment_id"] for comment in snapshot["comments"]] == [
        "c5",
        "c4",
        "c3",
        "c2",
        "c1",
    ]
    assert calls == [
        (
            queries.ISSUE_EXTERNAL_SNAPSHOT,
            {"id": "iss-1"},
        )
    ]


@pytest.mark.asyncio
async def test_move_issue_logs_issue_identifier_and_target_state(
    caplog: pytest.LogCaptureFixture,
) -> None:
    linear = Linear("test-key")

    async def fake_query(gql: str, variables: dict[str, Any]) -> dict[str, Any]:
        assert gql == queries.UPDATE_ISSUE_STATE
        assert variables == {"id": "iss-1", "stateId": "state-done"}
        return {
            "issueUpdate": {
                "success": True,
                "issue": {
                    "identifier": "ENG-1",
                    "state": {"id": "state-done", "name": "Done"},
                },
            }
        }

    linear._query = fake_query  # type: ignore[method-assign]
    try:
        with caplog.at_level("INFO", logger="symphony.linear.client"):
            await linear.move_issue("iss-1", "state-done")
    finally:
        await linear.aclose()

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "move_issue ENG-1" in message
        and "Done" in message
        and "state-done" in message
        and "caller=" in message
        for message in messages
    )


def test_comments_since_uses_linear_filter_timestamp_type() -> None:
    assert "$after: DateTimeOrDuration!" in queries.ISSUE_COMMENTS_SINCE
    assert "$after: DateTime!" not in queries.ISSUE_COMMENTS_SINCE


@pytest.mark.asyncio
async def test_upload_issue_attachment_uses_linear_file_upload_and_attachment_create(
    tmp_path: Path,
) -> None:
    screenshot = tmp_path / "hero.png"
    screenshot.write_bytes(b"png-bytes")
    linear = Linear("test-key")
    calls: list[tuple[str, dict[str, Any]]] = []
    put_calls: list[tuple[str, dict[str, str], bytes]] = []

    async def fake_query(gql: str, variables: dict[str, Any]) -> dict[str, Any]:
        calls.append((gql, variables))
        if gql == queries.FILE_UPLOAD:
            return {
                "fileUpload": {
                    "success": True,
                    "uploadFile": {
                        "uploadUrl": "https://upload.linear.app/signed",
                        "assetUrl": "https://uploads.linear.app/hero.png",
                        "headers": [
                            {"key": "Content-Type", "value": "image/png"},
                            {"key": "Cache-Control", "value": "public, max-age=31536000"},
                        ],
                    },
                }
            }
        if gql == queries.CREATE_ATTACHMENT:
            return {
                "attachmentCreate": {
                    "success": True,
                    "attachment": {"id": "att-1"},
                }
            }
        raise AssertionError(f"unexpected query: {gql}")

    async def fake_put(
        url: str, *, content: bytes, headers: dict[str, str]
    ) -> Any:
        put_calls.append((url, headers, content))

        class _Response:
            def raise_for_status(self) -> None:
                return None

        return _Response()

    linear._query = fake_query  # type: ignore[method-assign]
    linear._put_file = fake_put  # type: ignore[method-assign]
    try:
        url = await linear.upload_issue_attachment(
            issue_uuid="iss-1",
            path=screenshot,
            title="Acceptance screenshot: Primary verified view",
        )
    finally:
        await linear.aclose()

    assert url == "https://uploads.linear.app/hero.png"
    assert calls == [
        (
            queries.FILE_UPLOAD,
            {
                "contentType": "image/png",
                "filename": "hero.png",
                "size": len(b"png-bytes"),
            },
        ),
        (
            queries.CREATE_ATTACHMENT,
            {
                "input": {
                    "issueId": "iss-1",
                    "title": "Acceptance screenshot: Primary verified view",
                    "url": "https://uploads.linear.app/hero.png",
                }
            },
        ),
    ]
    assert put_calls == [
        (
            "https://upload.linear.app/signed",
            {
                "Content-Type": "image/png",
                "Cache-Control": "public, max-age=31536000",
            },
            b"png-bytes",
        )
    ]


def _issue_node() -> dict[str, Any]:
    return {
        "id": "iss-1",
        "identifier": "ENG-1",
        "title": "Blocked work",
        "description": "",
        "url": "https://linear.app/x",
        "state": {"id": "state-todo", "name": "Todo", "type": "unstarted"},
        "team": {"key": "ENG"},
        "labels": {"nodes": [{"name": "symphony"}]},
        "relations": {
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": [
                {
                    "type": "blocked_by",
                    "relatedIssue": {
                        "id": "iss-2",
                        "identifier": "ENG-2",
                        "archivedAt": None,
                        "state": {"type": "started"},
                    },
                },
                {
                    "type": "blocks",
                    "relatedIssue": {
                        "id": "iss-ignored",
                        "identifier": "ENG-99",
                        "archivedAt": None,
                        "state": {"type": "started"},
                    },
                },
            ]
        },
        "inverseRelations": {
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": [
                {
                    "type": "blocks",
                    "issue": {
                        "id": "iss-3",
                        "identifier": "WEB-3",
                        "archivedAt": "2026-05-16T00:00:00Z",
                        "state": {"type": "unstarted"},
                    },
                }
            ]
        },
    }


def test_linear_issue_parses_direct_and_inverse_blockers() -> None:
    issue = LinearIssue.from_node(_issue_node())

    assert [blocker.identifier for blocker in issue.blocked_by] == ["ENG-2", "WEB-3"]
    assert [blocker.state_type for blocker in issue.blocked_by] == [
        "started",
        "unstarted",
    ]
    assert [blocker.archived for blocker in issue.blocked_by] == [False, True]


def test_issue_queries_fetch_relations_for_dependency_gating() -> None:
    assert "relations(first: 50, includeArchived: true)" in queries.ISSUES_IN_STATE
    assert "inverseRelations(first: 50, includeArchived: true)" in queries.ISSUES_IN_STATE
    assert "pageInfo { hasNextPage endCursor }" in queries.ISSUES_IN_STATE
    assert "relations(first: 50, includeArchived: true)" in queries.LOOKUP_ISSUE
    assert "query IssueRelationsPage" in queries.ISSUE_RELATIONS_PAGE
    assert "query IssueInverseRelationsPage" in queries.ISSUE_INVERSE_RELATIONS_PAGE


@pytest.mark.asyncio
async def test_issues_in_state_paginates_truncated_blocker_relations() -> None:
    linear = Linear("test-key")
    calls: list[tuple[str, dict[str, Any]]] = []
    first_node = _issue_node()
    first_node["relations"]["pageInfo"] = {
        "hasNextPage": True,
        "endCursor": "rel-cursor",
    }
    first_node["inverseRelations"]["pageInfo"] = {
        "hasNextPage": True,
        "endCursor": "inv-cursor",
    }

    async def fake_query(gql: str, variables: dict[str, Any]) -> dict[str, Any]:
        calls.append((gql, variables))
        if gql == queries.ISSUES_IN_STATE_NO_LABEL:
            return {"issues": {"nodes": [first_node]}}
        if gql == queries.ISSUE_RELATIONS_PAGE:
            assert variables == {"id": "iss-1", "cursor": "rel-cursor"}
            return {
                "issue": {
                    "relations": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            {
                                "type": "blockedBy",
                                "relatedIssue": {
                                    "id": "iss-4",
                                    "identifier": "ENG-4",
                                    "archivedAt": None,
                                    "state": {"type": "triage"},
                                },
                            }
                        ],
                    }
                }
            }
        if gql == queries.ISSUE_INVERSE_RELATIONS_PAGE:
            assert variables == {"id": "iss-1", "cursor": "inv-cursor"}
            return {
                "issue": {
                    "inverseRelations": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            {
                                "type": "blocks",
                                "issue": {
                                    "id": "iss-5",
                                    "identifier": "WEB-5",
                                    "archivedAt": None,
                                    "state": {"type": "started"},
                                },
                            }
                        ],
                    }
                }
            }
        raise AssertionError(f"unexpected query: {gql}")

    linear._query = fake_query  # type: ignore[method-assign]
    try:
        issues = await linear.issues_in_state("ENG", "Todo")
    finally:
        await linear.aclose()

    assert len(issues) == 1
    assert [blocker.identifier for blocker in issues[0].blocked_by] == [
        "ENG-2",
        "WEB-3",
        "ENG-4",
        "WEB-5",
    ]
    assert [call[0] for call in calls] == [
        queries.ISSUES_IN_STATE_NO_LABEL,
        queries.ISSUE_RELATIONS_PAGE,
        queries.ISSUE_INVERSE_RELATIONS_PAGE,
    ]
