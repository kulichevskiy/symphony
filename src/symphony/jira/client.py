"""Jira issue-tracker client scaffold."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from ..tracker import Comment, Issue


class JiraError(RuntimeError):
    """Raised on Jira transport, auth, or response errors."""


class JiraTracker:
    """Async Jira client implementing the shared issue-tracker protocol."""

    def __init__(
        self,
        *,
        base_url: str,
        email: str,
        api_token: str,
        webhook_secret: str = "",
        project_key: str = "",
        states: Mapping[str, str] | None = None,
        timeout: float = 20.0,
    ) -> None:
        if not base_url:
            raise ValueError("JIRA_BASE_URL is empty")
        if not email:
            raise ValueError("JIRA_EMAIL is empty")
        if not api_token:
            raise ValueError("JIRA_API_TOKEN is empty")
        self.base_url = base_url.rstrip("/")
        self.email = email
        self.webhook_secret = webhook_secret
        self.project_key = project_key
        self._states = dict(states or {})
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            auth=(email, api_token),
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> JiraTracker:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def viewer_team_keys(self) -> list[str]:
        return [self.project_key] if self.project_key else []

    async def team_states(self, team_key: str) -> dict[str, str]:
        if self.project_key and team_key != self.project_key:
            return {}
        return dict(self._states)

    async def lookup_issue(self, identifier_or_uuid: str) -> Issue:
        raise NotImplementedError("Jira tracker operations are not implemented yet")

    async def issues_in_state(
        self, team_key: str, state_name: str, label: str | None = None
    ) -> Sequence[Issue]:
        return []

    async def comments_since(self, issue_uuid: str, after: datetime) -> Sequence[Comment]:
        raise NotImplementedError("Jira tracker operations are not implemented yet")

    async def post_comment(self, issue_uuid: str, body: str) -> str:
        raise NotImplementedError("Jira tracker operations are not implemented yet")

    async def move_issue(self, issue_id_or_identifier: str, state_id: str) -> None:
        raise NotImplementedError("Jira tracker operations are not implemented yet")

    async def upload_issue_attachment(
        self,
        *,
        issue_uuid: str,
        path: Path,
        title: str,
    ) -> str:
        raise NotImplementedError("Jira tracker operations are not implemented yet")
