import json

import httpx
import pytest
import respx

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
        httpx.Response(
            200,
            json={
                "data": {
                    "issueLabelCreate": {
                        "success": True,
                        "issueLabel": {"id": "campaign-label-id", "name": "EXP-1-A1"},
                    }
                }
            },
        ),
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
        for index in range(1, 7)
    )
    responses.extend(
        httpx.Response(
            200,
            json={"data": {"issueRelationCreate": {"success": True}}},
        )
        for _ in range(5)
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
    assert calls[1]["variables"]["input"] == {
        "teamId": "team-id",
        "name": "EXP-1-A1",
        "color": "#5E6AD2",
    }
    assert all(
        call["variables"]["input"]["labelIds"] == ["label-id", "campaign-label-id"]
        for call in calls[2:8]
    )
    assert [call["variables"]["input"] for call in calls[8:]] == [
        {"issueId": "issue-1", "relatedIssueId": "issue-2", "type": "blocks"},
        {"issueId": "issue-2", "relatedIssueId": "issue-3", "type": "blocks"},
        {"issueId": "issue-3", "relatedIssueId": "issue-4", "type": "blocks"},
        {"issueId": "issue-4", "relatedIssueId": "issue-5", "type": "blocks"},
        {"issueId": "issue-5", "relatedIssueId": "issue-6", "type": "blocks"},
    ]
    assert "issueLabelCreate" in calls[1]["query"]
