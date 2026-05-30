"""Issue tracker abstraction shared by orchestration code."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ..config import RepoBinding, Secrets

DEFAULT_PROVIDER = "linear"
DEFAULT_SITE = "default"

TrackerKey = tuple[str, str]
StateCacheKey = tuple[str, str, str]


@dataclass(frozen=True)
class TrackerContext:
    provider: str = DEFAULT_PROVIDER
    site: str = DEFAULT_SITE


@dataclass
class Blocker:
    id: str
    identifier: str
    state_type: str
    archived: bool


@dataclass
class Issue:
    id: str
    identifier: str
    title: str
    description: str
    url: str
    state_id: str
    state_name: str
    state_type: str
    team_key: str
    labels: list[str] = field(default_factory=list)
    blocked_by: list[Blocker] = field(default_factory=list)
    updated_at: str = ""


@dataclass
class Comment:
    id: str
    body: str
    created_at: str
    author_name: str
    author_is_me: bool
    external_thread_type: str | None


@runtime_checkable
class IssueTracker(Protocol):
    async def viewer_team_keys(self) -> list[str]: ...

    async def team_states(self, team_key: str) -> dict[str, str]: ...

    async def lookup_issue(self, identifier_or_uuid: str) -> Issue: ...

    async def issues_in_state(
        self, team_key: str, state_name: str, label: str | None = None
    ) -> Sequence[Issue]: ...

    async def comments_since(self, issue_uuid: str, after: datetime) -> Sequence[Comment]: ...

    async def post_comment(self, issue_uuid: str, body: str) -> str: ...

    async def move_issue(self, issue_id_or_identifier: str, state_id: str) -> None: ...

    async def upload_issue_attachment(
        self,
        *,
        issue_uuid: str,
        path: Path,
        title: str,
    ) -> str: ...


class TrackerRegistry:
    def __init__(self) -> None:
        self._trackers: dict[TrackerKey, IssueTracker] = {}

    def register(
        self,
        provider: str,
        site: str,
        tracker: IssueTracker,
    ) -> None:
        self._trackers[(provider, site)] = tracker

    def resolve(self, ctx: TrackerContext | None = None) -> IssueTracker:
        key = (
            ctx.provider if ctx is not None else DEFAULT_PROVIDER,
            ctx.site if ctx is not None else DEFAULT_SITE,
        )
        try:
            return self._trackers[key]
        except KeyError as exc:
            raise KeyError(f"no issue tracker registered for {key}") from exc


def for_binding(
    binding: RepoBinding,
    secrets: Secrets,
    *,
    registry: TrackerRegistry | None = None,
) -> IssueTracker:
    """Build the concrete tracker for a binding and optionally register it."""
    if binding.provider == "linear":
        from ..linear.client import LinearTracker

        tracker: IssueTracker = LinearTracker(secrets.linear_api_key)
    elif binding.provider == "jira":
        from ..jira.client import JiraTracker

        tracker = JiraTracker(
            base_url=binding.base_url or secrets.jira_base_url,
            email=secrets.jira_email,
            api_token=secrets.jira_api_token,
            webhook_secret=secrets.jira_webhook_secret,
        )
    else:
        raise ValueError(f"unsupported issue tracker provider {binding.provider!r}")

    if registry is not None:
        registry.register(binding.tracker_provider, binding.tracker_site, tracker)
    return tracker
