from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from .eventdesk import Campaign

_ENDPOINT = "https://api.linear.app/graphql"
_TRANSIENT_QUERY_ATTEMPTS = 5
_RETRY_BACKOFF_SECONDS = 1.0

_TEAM = """
query BenchTeam($id: String!) {
  team(id: $id) { id states { nodes { id name } } }
}
"""

_CREATE_ISSUE = """
mutation BenchIssue($input: IssueCreateInput!) {
  issueCreate(input: $input) { success issue { id identifier url } }
}
"""

_CREATE_RELATION = """
mutation BenchRelation($input: IssueRelationCreateInput!) {
  issueRelationCreate(input: $input) { success }
}
"""

_CAMPAIGN_ISSUES = """
query BenchCampaignIssues($teamId: ID!, $titlePrefix: String!) {
  issues(
    first: 50
    filter: {team: {id: {eq: $teamId}}, title: {startsWith: $titlePrefix}}
  ) { nodes { id identifier url title } }
}
"""

_ISSUE_RELATIONS = """
query BenchIssueRelations($id: String!) {
  issue(id: $id) { relations { nodes { type relatedIssue { id } } } }
}
"""

_ISSUE_STATE = """
query BenchIssueState($id: String!) {
  issue(id: $id) { id identifier state { name type } }
}
"""


class LinearSandboxError(RuntimeError):
    pass


@dataclass(frozen=True)
class LinearCampaign:
    issue_ids: tuple[str, ...]
    issue_identifiers: tuple[str, ...]
    issue_urls: tuple[str, ...]


@dataclass(frozen=True)
class LinearIssueState:
    id: str
    identifier: str
    name: str
    type: str


class LinearSandbox:
    def __init__(self, authorization: str, *, routing_label_id: str, timeout: float = 30) -> None:
        if not routing_label_id:
            raise ValueError("routing_label_id must not be empty")
        self._client = httpx.AsyncClient(headers={"Authorization": authorization}, timeout=timeout)
        self._routing_label_id = routing_label_id
        self._issues: dict[str, dict[str, dict[str, str]]] = {}
        self._relations: set[tuple[str, str]] = set()

    async def __aenter__(self) -> LinearSandbox:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self._client.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _query(
        self,
        query: str,
        variables: dict[str, Any],
        *,
        retry_transient: bool = False,
    ) -> dict[str, Any]:
        attempts = _TRANSIENT_QUERY_ATTEMPTS if retry_transient else 1
        for attempt in range(attempts):
            try:
                response = await self._client.post(
                    _ENDPOINT, json={"query": query, "variables": variables}
                )
                response.raise_for_status()
                break
            except httpx.HTTPStatusError as exc:
                transient = exc.response.status_code == 429 or exc.response.status_code >= 500
                if transient and attempt + 1 < attempts:
                    await asyncio.sleep(_RETRY_BACKOFF_SECONDS * 2**attempt)
                    continue
                raise LinearSandboxError(f"Linear request failed: {exc}") from exc
            except httpx.RequestError as exc:
                if attempt + 1 < attempts:
                    await asyncio.sleep(_RETRY_BACKOFF_SECONDS * 2**attempt)
                    continue
                raise LinearSandboxError(f"Linear request failed: {exc}") from exc
        else:  # pragma: no cover - every final failure raises above
            raise AssertionError("unreachable")
        body = response.json()
        errors = body.get("errors")
        if errors:
            raise LinearSandboxError(f"Linear GraphQL errors: {errors}")
        data = body.get("data")
        if not isinstance(data, dict):
            raise LinearSandboxError("Linear returned no data")
        return data

    @staticmethod
    def _successful(data: dict[str, Any], field: str) -> dict[str, Any]:
        payload = data.get(field)
        if not isinstance(payload, dict) or payload.get("success") is not True:
            raise LinearSandboxError(f"Linear {field} returned success=false")
        return payload

    async def create_campaign(
        self,
        *,
        team_id: str,
        label: str,
        repo_url: str,
        campaign: Campaign,
    ) -> LinearCampaign:
        team_data = await self._query(_TEAM, {"id": team_id}, retry_transient=True)
        team = team_data.get("team")
        if not isinstance(team, dict):
            raise LinearSandboxError(f"Linear team not found: {team_id}")
        states = (team.get("states") or {}).get("nodes") or []
        todo_id = next((state["id"] for state in states if state.get("name") == "Todo"), None)
        if todo_id is None:
            raise LinearSandboxError("BENCH team has no Todo state")

        issue_by_key = self._issues.setdefault(label, {})
        expected_titles = {f"[{label}] {ticket.title}": ticket.key for ticket in campaign.tickets}
        existing_data = await self._query(
            _CAMPAIGN_ISSUES,
            {"teamId": team_id, "titlePrefix": f"[{label}]"},
            retry_transient=True,
        )
        existing_nodes = (existing_data.get("issues") or {}).get("nodes") or []
        seen_keys: set[str] = set()
        for issue in existing_nodes:
            if not isinstance(issue, dict):
                continue
            key = expected_titles.get(str(issue.get("title", "")))
            if key is None:
                continue
            if key in seen_keys:
                raise LinearSandboxError(f"duplicate benchmark issue for {label}: {key}")
            seen_keys.add(key)
            issue_by_key[key] = {
                "id": str(issue["id"]),
                "identifier": str(issue["identifier"]),
                "url": str(issue["url"]),
            }
        for ticket in campaign.tickets:
            if ticket.key in issue_by_key:
                continue
            description = f"{ticket.description}\n\nBenchmark repository: {repo_url}\n"
            issue_payload = self._successful(
                await self._query(
                    _CREATE_ISSUE,
                    {
                        "input": {
                            "teamId": team_id,
                            "stateId": todo_id,
                            "labelIds": [self._routing_label_id],
                            "title": f"[{label}] {ticket.title}",
                            "description": description,
                        }
                    },
                ),
                "issueCreate",
            )
            issue = issue_payload["issue"]
            issue_by_key[ticket.key] = {
                "id": issue["id"],
                "identifier": issue["identifier"],
                "url": issue["url"],
            }

        for ticket in campaign.tickets:
            for blocker_key in ticket.blocked_by:
                relation_key = (issue_by_key[blocker_key]["id"], issue_by_key[ticket.key]["id"])
                if relation_key in self._relations:
                    continue
                relation_data = await self._query(
                    _ISSUE_RELATIONS,
                    {"id": relation_key[0]},
                    retry_transient=True,
                )
                relation_issue = relation_data.get("issue") or {}
                relation_nodes = (relation_issue.get("relations") or {}).get("nodes") or []
                already_exists = any(
                    isinstance(relation, dict)
                    and relation.get("type") == "blocks"
                    and isinstance(relation.get("relatedIssue"), dict)
                    and str(relation["relatedIssue"].get("id")) == relation_key[1]
                    for relation in relation_nodes
                )
                if already_exists:
                    self._relations.add(relation_key)
                    continue
                self._successful(
                    await self._query(
                        _CREATE_RELATION,
                        {
                            "input": {
                                "issueId": issue_by_key[blocker_key]["id"],
                                "relatedIssueId": issue_by_key[ticket.key]["id"],
                                "type": "blocks",
                            }
                        },
                    ),
                    "issueRelationCreate",
                )
                self._relations.add(relation_key)

        ordered = [issue_by_key[ticket.key] for ticket in campaign.tickets]
        return LinearCampaign(
            issue_ids=tuple(issue["id"] for issue in ordered),
            issue_identifiers=tuple(issue["identifier"] for issue in ordered),
            issue_urls=tuple(issue["url"] for issue in ordered),
        )

    async def issue_states(self, issue_ids: tuple[str, ...]) -> tuple[LinearIssueState, ...]:
        states: list[LinearIssueState] = []
        for issue_id in issue_ids:
            data = await self._query(_ISSUE_STATE, {"id": issue_id}, retry_transient=True)
            issue = data.get("issue")
            if not isinstance(issue, dict):
                raise LinearSandboxError(f"Linear issue not found: {issue_id}")
            state = issue.get("state")
            if not isinstance(state, dict):
                raise LinearSandboxError(f"Linear issue has no state: {issue_id}")
            states.append(
                LinearIssueState(
                    id=str(issue["id"]),
                    identifier=str(issue["identifier"]),
                    name=str(state["name"]),
                    type=str(state["type"]),
                )
            )
        return tuple(states)
