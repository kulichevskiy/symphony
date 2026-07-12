"""Linear GraphQL client.

Hand-rolled over `httpx` rather than `gql` — the surface is small (~7 ops)
and a typed-codegen client adds maintenance without removing any actual
boilerplate.

Personal API keys go in the `Authorization` header **without** the `Bearer`
prefix. OAuth tokens *do* use `Bearer`; mixing them up is the most common
first-call failure.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import mimetypes
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from ..tracker import Blocker, Comment, Issue
from . import queries
from .templates import is_symphony_comment, mark_symphony_comment

log = logging.getLogger(__name__)

ENDPOINT = "https://api.linear.app/graphql"


class LinearError(RuntimeError):
    """Raised on any non-2xx, transport error, or `errors[]` in the body."""


class LinearIssue(Issue):
    @classmethod
    def from_node(cls, node: dict[str, Any]) -> LinearIssue:
        return cls(
            id=node["id"],
            identifier=node["identifier"],
            title=node["title"],
            description=node.get("description") or "",
            url=node["url"],
            state_id=node["state"]["id"],
            state_name=node["state"]["name"],
            state_type=node["state"]["type"],
            team_key=node["team"]["key"],
            labels=[lbl["name"] for lbl in node.get("labels", {}).get("nodes", [])],
            blocked_by=_blocked_by_from_node(node),
            updated_at=str(node.get("updatedAt") or ""),
        )


class LinearComment(Comment):
    @classmethod
    def from_node(cls, node: dict[str, Any]) -> LinearComment:
        user = node.get("user") or {}
        ext = node.get("externalThread")
        body = node["body"]
        return cls(
            id=node["id"],
            body=body,
            created_at=node["createdAt"],
            author_name=user.get("name", ""),
            author_is_me=is_symphony_comment(body),
            external_thread_type=ext["type"] if ext else None,
        )


def comment_from_webhook_payload(payload: Mapping[str, Any]) -> LinearComment | None:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    comment_id = data.get("id")
    body = data.get("body")
    created_at = data.get("createdAt") or payload.get("createdAt")
    if not isinstance(comment_id, str) or not comment_id:
        return None
    if not isinstance(body, str):
        return None
    if not isinstance(created_at, str) or not created_at:
        return None
    actor = payload.get("actor")
    author_name = ""
    if isinstance(actor, Mapping):
        raw_name = actor.get("name")
        author_name = raw_name if isinstance(raw_name, str) else ""
    external_thread_type: str | None = None
    ext = data.get("externalThread")
    if isinstance(ext, Mapping):
        raw_type = ext.get("type")
        external_thread_type = raw_type if isinstance(raw_type, str) else None
    return LinearComment(
        id=comment_id,
        body=body,
        created_at=created_at,
        author_name=author_name,
        author_is_me=is_symphony_comment(body),
        external_thread_type=external_thread_type,
    )


class LinearTracker:
    """Async Linear client.

    One HTTP client is reused across the process; request timeouts are
    bounded so a hung Linear API doesn't seize the orchestrator.
    """

    def __init__(self, api_key: str, *, timeout: float = 20.0) -> None:
        if not api_key:
            raise ValueError("LINEAR_API_KEY is empty; orchestrator can't run headless")
        self._client = httpx.AsyncClient(
            base_url="https://api.linear.app",
            headers={"Authorization": api_key},  # NOT "Bearer"
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> LinearTracker:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    # ---- low-level ----

    async def _query(self, gql: str, variables: dict[str, Any]) -> dict[str, Any]:
        try:
            r = await self._client.post("/graphql", json={"query": gql, "variables": variables})
        except httpx.HTTPError as e:
            raise LinearError(f"transport error: {e}") from e
        if r.status_code >= 500 or r.status_code == 429:
            raise LinearError(f"server error {r.status_code}: {r.text[:200]}")
        body: dict[str, Any] = r.json()
        if "errors" in body and body["errors"]:
            raise LinearError(f"graphql errors: {body['errors']}")
        data: dict[str, Any] = body["data"]
        return data

    # ---- high-level ----

    async def lookup_issue(self, identifier_or_uuid: str) -> LinearIssue:
        """Resolve an identifier or UUID to a full issue. One round-trip."""
        data = await self._query(queries.LOOKUP_ISSUE, {"id": identifier_or_uuid})
        node = data.get("issue")
        if not node:
            raise LinearError(f"issue not found: {identifier_or_uuid}")
        return await self._issue_from_node(node)

    async def issues_in_state(
        self, team_key: str, state_name: str, label: str | None = None
    ) -> list[LinearIssue]:
        """Source query for dispatch.

        Linear's `issues(filter: { labels: { name: { eq: ... } } })` requires
        the label argument to be non-null at validation time, so we use a
        separate query when no label is configured.
        """
        issues: list[LinearIssue] = []
        cursor: str | None = None
        while True:
            if label:
                data = await self._query(
                    queries.ISSUES_IN_STATE,
                    {
                        "team": team_key,
                        "stateName": state_name,
                        "label": label,
                        "cursor": cursor,
                    },
                )
            else:
                data = await self._query(
                    queries.ISSUES_IN_STATE_NO_LABEL,
                    {"team": team_key, "stateName": state_name, "cursor": cursor},
                )
            connection = data["issues"]
            for node in connection["nodes"]:
                issues.append(await self._issue_from_node(node))
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                break
        return issues

    async def comments_since(self, issue_uuid: str, after: datetime) -> list[LinearComment]:
        """Inbound steering source. Caller passes a dedupe cursor."""
        comments: list[LinearComment] = []
        cursor: str | None = None
        while True:
            data = await self._query(
                queries.ISSUE_COMMENTS_SINCE,
                {"id": issue_uuid, "after": after.isoformat(), "cursor": cursor},
            )
            issue = data.get("issue") or {}
            connection = issue.get("comments") or {}
            comments.extend(LinearComment.from_node(n) for n in connection.get("nodes") or [])

            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                break
        return comments

    async def issue_external_snapshot(self, identifier_or_uuid: str) -> dict[str, Any]:
        """Current Linear issue state plus recent comments for the UI."""
        data = await self._query(
            queries.ISSUE_EXTERNAL_SNAPSHOT,
            {"id": identifier_or_uuid},
        )
        node = data.get("issue")
        if not node:
            raise LinearError(f"issue not found: {identifier_or_uuid}")

        comments: list[dict[str, Any]] = []
        connection = node.get("comments") or {}
        comments.extend(connection.get("nodes") or [])

        comments.sort(key=lambda comment: str(comment.get("createdAt") or ""), reverse=True)
        issue_url = str(node.get("url") or "")
        return {
            "state": (node.get("state") or {}).get("name"),
            "updated_at": node.get("updatedAt"),
            "labels": [
                str(label.get("name"))
                for label in (node.get("labels") or {}).get("nodes", [])
                if label.get("name")
            ],
            "comments": [
                {
                    "author": str((comment.get("user") or {}).get("name") or ""),
                    "ts": comment.get("createdAt"),
                    "body": comment.get("body") or "",
                    "comment_id": comment.get("id"),
                    "url": f"{issue_url}#comment-{comment.get('id')}" if issue_url else None,
                }
                for comment in comments[:5]
            ],
        }

    async def post_comment(self, issue_uuid: str, body: str) -> str:
        """Returns the comment id (useful for threading later)."""
        data = await self._query(
            queries.CREATE_COMMENT,
            {"input": {"issueId": issue_uuid, "body": mark_symphony_comment(body)}},
        )
        result = data["commentCreate"]
        if not result.get("success"):
            raise LinearError(f"commentCreate returned success=false: {result}")
        comment_id: str = result["comment"]["id"]
        return comment_id

    async def upload_issue_attachment(
        self,
        *,
        issue_uuid: str,
        path: Path,
        title: str,
    ) -> str:
        """Upload a file into Linear storage and link it as an issue attachment."""
        content = await asyncio.to_thread(path.read_bytes)
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        data = await self._query(
            queries.FILE_UPLOAD,
            {
                "contentType": content_type,
                "filename": path.name,
                "size": len(content),
            },
        )
        result = data["fileUpload"]
        if not result.get("success"):
            raise LinearError(f"fileUpload returned success=false: {result}")
        upload_file = result.get("uploadFile") or {}
        upload_url = str(upload_file.get("uploadUrl") or "")
        asset_url = str(upload_file.get("assetUrl") or "")
        if not upload_url or not asset_url:
            raise LinearError("fileUpload did not return uploadUrl and assetUrl")
        headers = {
            str(item["key"]): str(item["value"])
            for item in upload_file.get("headers") or []
            if isinstance(item, dict) and item.get("key") is not None
        }
        response = await self._put_file(upload_url, content=content, headers=headers)
        if not 200 <= response.status_code < 300:
            raise LinearError(
                f"file upload returned HTTP {response.status_code}: {response.text[:200]}"
            )
        data = await self._query(
            queries.CREATE_ATTACHMENT,
            {
                "input": {
                    "issueId": issue_uuid,
                    "title": title,
                    "url": asset_url,
                }
            },
        )
        result = data["attachmentCreate"]
        if not result.get("success"):
            raise LinearError(f"attachmentCreate returned success=false: {result}")
        return asset_url

    async def _put_file(
        self,
        url: str,
        *,
        content: bytes,
        headers: dict[str, str],
    ) -> httpx.Response:
        async with httpx.AsyncClient(timeout=self._client.timeout) as client:
            return await client.put(url, content=content, headers=headers)

    async def move_issue(self, issue_id_or_identifier: str, state_id: str) -> None:
        data = await self._query(
            queries.UPDATE_ISSUE_STATE,
            {"id": issue_id_or_identifier, "stateId": state_id},
        )
        result = data["issueUpdate"]
        if not result.get("success"):
            raise LinearError("issueUpdate returned success=false")
        issue = result.get("issue") or {}
        state = issue.get("state") or {}
        issue_identifier = str(issue.get("identifier") or issue_id_or_identifier)
        state_name = str(state.get("name") or state_id)
        logged_state_id = str(state.get("id") or state_id)
        frame = inspect.currentframe()
        caller_frame = frame.f_back if frame is not None else None
        caller = "unknown"
        if caller_frame is not None:
            caller = f"{caller_frame.f_code.co_filename}:{caller_frame.f_lineno}"
        del frame
        log.info(
            "move_issue %s → %s (%s) caller=%s",
            issue_identifier,
            state_name,
            logged_state_id,
            caller,
        )

    async def team_states(self, team_key: str) -> dict[str, str]:
        """Return name -> state UUID for a team.

        Cached upstream; this is only called at startup (and on a forced
        refresh) so we don't burn the Linear rate budget re-fetching states.
        """
        data = await self._query(queries.TEAM_STATES, {"key": team_key})
        team = data.get("team")
        if not team:
            raise LinearError(f"team not found: {team_key}")
        return {n["name"]: n["id"] for n in team["states"]["nodes"]}

    async def viewer_team_keys(self) -> list[str]:
        """For the §10.4 preflight check: confirm the API key sees every
        team the operator wants Symphony to watch."""
        data = await self._query(queries.VIEWER_TEAMS, {})
        viewer = data.get("viewer") or {}
        return [t["key"] for t in (viewer.get("teams") or {}).get("nodes", [])]

    async def _issue_from_node(self, node: dict[str, Any]) -> LinearIssue:
        issue = LinearIssue.from_node(node)
        issue.blocked_by.extend(
            await self._remaining_blockers_from_paginated_relations(issue, node)
        )
        return issue

    async def _remaining_blockers_from_paginated_relations(
        self, issue: LinearIssue, node: dict[str, Any]
    ) -> list[Blocker]:
        seen = {blocker.id for blocker in issue.blocked_by}
        blockers: list[Blocker] = []
        blockers.extend(
            await self._remaining_relation_blockers(
                issue.id,
                node,
                connection_name="relations",
                query=queries.ISSUE_RELATIONS_PAGE,
                inverse=False,
                seen=seen,
            )
        )
        blockers.extend(
            await self._remaining_relation_blockers(
                issue.id,
                node,
                connection_name="inverseRelations",
                query=queries.ISSUE_INVERSE_RELATIONS_PAGE,
                inverse=True,
                seen=seen,
            )
        )
        return blockers

    async def _remaining_relation_blockers(
        self,
        issue_id: str,
        node: dict[str, Any],
        *,
        connection_name: str,
        query: str,
        inverse: bool,
        seen: set[str],
    ) -> list[Blocker]:
        page_info = _relation_page_info(node, connection_name)
        if not page_info.get("hasNextPage"):
            return []
        cursor = page_info.get("endCursor")
        blockers: list[Blocker] = []
        while cursor:
            data = await self._query(query, {"id": issue_id, "cursor": cursor})
            issue_node = data.get("issue") or {}
            connection = issue_node.get(connection_name) or {}
            blockers.extend(
                _blockers_from_relation_nodes(
                    connection.get("nodes") or [],
                    inverse=inverse,
                    seen=seen,
                )
            )
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return blockers


def _normalized_relation_type(raw: str) -> str:
    return raw.replace("_", "").replace("-", "").casefold()


def _blocker_from_issue_node(node: dict[str, Any] | None) -> Blocker | None:
    if not node:
        return None
    state = node.get("state") or {}
    return Blocker(
        id=node["id"],
        identifier=node["identifier"],
        state_type=state.get("type", ""),
        archived=bool(node.get("archivedAt")),
    )


def _blockers_from_relation_nodes(
    nodes: list[dict[str, Any]], *, inverse: bool, seen: set[str]
) -> list[Blocker]:
    blockers: list[Blocker] = []
    for relation in nodes:
        relation_type = _normalized_relation_type(str(relation.get("type") or ""))
        if inverse:
            if relation_type != "blocks":
                continue
            blocker = _blocker_from_issue_node(relation.get("issue"))
        else:
            if relation_type != "blockedby":
                continue
            blocker = _blocker_from_issue_node(relation.get("relatedIssue"))
        if blocker is not None and blocker.id not in seen:
            blockers.append(blocker)
            seen.add(blocker.id)
    return blockers


def _blocked_by_from_node(node: dict[str, Any]) -> list[Blocker]:
    seen: set[str] = set()
    blockers = _blockers_from_relation_nodes(
        (node.get("relations") or {}).get("nodes", []),
        inverse=False,
        seen=seen,
    )
    blockers.extend(
        _blockers_from_relation_nodes(
            (node.get("inverseRelations") or {}).get("nodes", []),
            inverse=True,
            seen=seen,
        )
    )
    return blockers


def _relation_page_info(node: dict[str, Any], connection_name: str) -> dict[str, Any]:
    connection = node.get(connection_name) or {}
    page_info: dict[str, Any] = connection.get("pageInfo") or {}
    return page_info


Linear = LinearTracker
