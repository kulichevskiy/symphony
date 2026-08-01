import json

import httpx
import pytest
import respx

from symphony.bench import linear as linear_module
from symphony.bench.eventdesk import eventdesk_campaign
from symphony.bench.linear import LinearSandbox


@pytest.mark.asyncio
@respx.mock
async def test_linear_sandbox_creates_labeled_campaign_and_real_dependency_chain() -> None:
    responses = [
        httpx.Response(
            200,
            json={
                "data": {
                    "team": {
                        "id": "team-id",
                        "states": {"nodes": [{"id": "todo-id", "name": "Todo"}]},
                    }
                }
            },
        ),
    ]
    responses.extend(
        httpx.Response(
            200,
            json={"data": {"issues": {"nodes": []}}},
        )
        for _ in range(1)
    )
    responses.extend(
        httpx.Response(
            200,
            json={
                "data": {
                    "issueCreate": {
                        "success": True,
                        "issue": {
                            "id": f"issue-{index}",
                            "identifier": f"BENCH-{index}",
                            "url": f"https://linear.app/BENCH-{index}",
                        },
                    }
                }
            },
        )
        for index in range(1, 7)
    )
    for _ in range(5):
        responses.extend(
            (
                httpx.Response(
                    200,
                    json={"data": {"issue": {"relations": {"nodes": []}}}},
                ),
                httpx.Response(
                    200,
                    json={"data": {"issueRelationCreate": {"success": True}}},
                ),
            )
        )
    route = respx.post("https://api.linear.app/graphql").mock(side_effect=responses)

    async with LinearSandbox("Bearer token", routing_label_id="label-id") as sandbox:
        result = await sandbox.create_campaign(
            team_id="team-id",
            label="EXP-1-A1",
            repo_url="https://github.com/kulichevskiy/EXP-1-A1",
            campaign=eventdesk_campaign(),
        )

    assert result.issue_identifiers == tuple(f"BENCH-{index}" for index in range(1, 7))
    calls = [json.loads(call.request.content) for call in route.calls]
    issue_calls = [call for call in calls if "mutation BenchIssue" in call["query"]]
    relation_calls = [call for call in calls if "mutation BenchRelation" in call["query"]]
    assert all(call["variables"]["input"]["labelIds"] == ["label-id"] for call in issue_calls)
    assert [call["variables"]["input"] for call in relation_calls] == [
        {"issueId": "issue-1", "relatedIssueId": "issue-2", "type": "blocks"},
        {"issueId": "issue-2", "relatedIssueId": "issue-3", "type": "blocks"},
        {"issueId": "issue-3", "relatedIssueId": "issue-4", "type": "blocks"},
        {"issueId": "issue-4", "relatedIssueId": "issue-5", "type": "blocks"},
        {"issueId": "issue-5", "relatedIssueId": "issue-6", "type": "blocks"},
    ]
    assert all("issueLabelCreate" not in call["query"] for call in calls)


@pytest.mark.asyncio
@respx.mock
async def test_campaign_retry_reconciles_issue_created_before_response_was_lost() -> None:
    campaign = eventdesk_campaign()
    first_title = f"[EXP-RETRY] {campaign.tickets[0].title}"
    team = httpx.Response(
        200,
        json={
            "data": {
                "team": {
                    "id": "team-id",
                    "states": {"nodes": [{"id": "todo-id", "name": "Todo"}]},
                }
            }
        },
    )
    existing_first = {
        "id": "issue-1",
        "identifier": "BENCH-1",
        "url": "https://linear.app/BENCH-1",
        "title": first_title,
    }
    responses: list[httpx.Response | Exception] = [
        team,
        httpx.Response(200, json={"data": {"issues": {"nodes": []}}}),
        httpx.ReadTimeout("response lost"),
        team,
        httpx.Response(200, json={"data": {"issues": {"nodes": [existing_first]}}}),
    ]
    responses.extend(
        httpx.Response(
            200,
            json={
                "data": {
                    "issueCreate": {
                        "success": True,
                        "issue": {
                            "id": f"issue-{index}",
                            "identifier": f"BENCH-{index}",
                            "url": f"https://linear.app/BENCH-{index}",
                        },
                    }
                }
            },
        )
        for index in range(2, 7)
    )
    for _ in range(5):
        responses.extend(
            (
                httpx.Response(200, json={"data": {"issue": {"relations": {"nodes": []}}}}),
                httpx.Response(200, json={"data": {"issueRelationCreate": {"success": True}}}),
            )
        )
    route = respx.post("https://api.linear.app/graphql").mock(side_effect=responses)

    async with LinearSandbox("Bearer token", routing_label_id="label-id") as sandbox:
        with pytest.raises(linear_module.LinearSandboxError):
            await sandbox.create_campaign(
                team_id="team-id",
                label="EXP-RETRY",
                repo_url="https://github.com/kulichevskiy/EXP-RETRY",
                campaign=campaign,
            )
        result = await sandbox.create_campaign(
            team_id="team-id",
            label="EXP-RETRY",
            repo_url="https://github.com/kulichevskiy/EXP-RETRY",
            campaign=campaign,
        )

    calls = [json.loads(call.request.content) for call in route.calls]
    assert result.issue_identifiers == tuple(f"BENCH-{index}" for index in range(1, 7))
    assert sum("mutation BenchIssue" in call["query"] for call in calls) == 6


@pytest.mark.asyncio
@respx.mock
async def test_campaign_retry_reconciles_relation_created_before_response_was_lost() -> None:
    campaign = eventdesk_campaign()
    issues = [
        {
            "id": f"issue-{index}",
            "identifier": f"BENCH-{index}",
            "url": f"https://linear.app/BENCH-{index}",
            "title": f"[EXP-RELATION] {ticket.title}",
        }
        for index, ticket in enumerate(campaign.tickets, 1)
    ]
    team = httpx.Response(
        200,
        json={
            "data": {
                "team": {
                    "id": "team-id",
                    "states": {"nodes": [{"id": "todo-id", "name": "Todo"}]},
                }
            }
        },
    )
    responses: list[httpx.Response | Exception] = [
        team,
        httpx.Response(200, json={"data": {"issues": {"nodes": []}}}),
    ]
    responses.extend(
        httpx.Response(
            200,
            json={"data": {"issueCreate": {"success": True, "issue": issue}}},
        )
        for issue in issues
    )
    responses.extend(
        (
            httpx.Response(200, json={"data": {"issue": {"relations": {"nodes": []}}}}),
            httpx.ReadTimeout("response lost"),
            team,
            httpx.Response(200, json={"data": {"issues": {"nodes": issues}}}),
            httpx.Response(
                200,
                json={
                    "data": {
                        "issue": {
                            "relations": {
                                "nodes": [{"type": "blocks", "relatedIssue": {"id": "issue-2"}}]
                            }
                        }
                    }
                },
            ),
        )
    )
    for _ in range(4):
        responses.extend(
            (
                httpx.Response(200, json={"data": {"issue": {"relations": {"nodes": []}}}}),
                httpx.Response(200, json={"data": {"issueRelationCreate": {"success": True}}}),
            )
        )
    route = respx.post("https://api.linear.app/graphql").mock(side_effect=responses)

    async with LinearSandbox("Bearer token", routing_label_id="label-id") as sandbox:
        with pytest.raises(linear_module.LinearSandboxError):
            await sandbox.create_campaign(
                team_id="team-id",
                label="EXP-RELATION",
                repo_url="https://github.com/kulichevskiy/EXP-RELATION",
                campaign=campaign,
            )
        result = await sandbox.create_campaign(
            team_id="team-id",
            label="EXP-RELATION",
            repo_url="https://github.com/kulichevskiy/EXP-RELATION",
            campaign=campaign,
        )

    calls = [json.loads(call.request.content) for call in route.calls]
    assert len(result.issue_ids) == 6
    assert sum("mutation BenchRelation" in call["query"] for call in calls) == 5


@pytest.mark.asyncio
@respx.mock
async def test_issue_states_retries_transient_linear_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(linear_module, "_RETRY_BACKOFF_SECONDS", 0, raising=False)
    route = respx.post("https://api.linear.app/graphql").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(
                200,
                json={
                    "data": {
                        "issue": {
                            "id": "issue-1",
                            "identifier": "BENCH-1",
                            "state": {"name": "In Progress", "type": "started"},
                        }
                    }
                },
            ),
        ]
    )

    async with LinearSandbox("Bearer token", routing_label_id="label-id") as sandbox:
        states = await sandbox.issue_states(("issue-1",))

    assert states[0].identifier == "BENCH-1"
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_issue_states_refreshes_authorization_once_after_401() -> None:
    route = respx.post("https://api.linear.app/graphql").mock(
        side_effect=[
            httpx.Response(401),
            httpx.Response(
                200,
                json={
                    "data": {
                        "issue": {
                            "id": "issue-1",
                            "identifier": "BENCH-1",
                            "state": {"name": "In Progress", "type": "started"},
                        }
                    }
                },
            ),
        ]
    )
    refreshes = 0

    async def refresh_authorization() -> str:
        nonlocal refreshes
        refreshes += 1
        return "Bearer fresh"

    async with LinearSandbox(
        "Bearer stale",
        routing_label_id="label-id",
        authorization_resolver=refresh_authorization,
    ) as sandbox:
        states = await sandbox.issue_states(("issue-1",))

    assert states[0].identifier == "BENCH-1"
    assert refreshes == 1
    assert route.call_count == 2
    assert route.calls[0].request.headers["Authorization"] == "Bearer stale"
    assert route.calls[1].request.headers["Authorization"] == "Bearer fresh"


@pytest.mark.asyncio
@respx.mock
async def test_issue_states_does_not_loop_when_refreshed_authorization_is_rejected() -> None:
    route = respx.post("https://api.linear.app/graphql").mock(
        side_effect=[httpx.Response(401), httpx.Response(401)]
    )
    refreshes = 0

    async def refresh_authorization() -> str:
        nonlocal refreshes
        refreshes += 1
        return "Bearer fresh"

    async with LinearSandbox(
        "Bearer stale",
        routing_label_id="label-id",
        authorization_resolver=refresh_authorization,
    ) as sandbox:
        with pytest.raises(linear_module.LinearSandboxError, match="401 Unauthorized"):
            await sandbox.issue_states(("issue-1",))

    assert refreshes == 1
    assert route.call_count == 2
