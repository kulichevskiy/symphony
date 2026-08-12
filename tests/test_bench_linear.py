import asyncio
import json
from datetime import UTC, datetime

import httpx
import pytest
import respx

from symphony.bench import linear as linear_module
from symphony.bench.campaign import Campaign, CampaignTicket, feedback_inbox_campaign
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
        httpx.Response(200, json={"data": {"projects": {"nodes": []}}}),
        httpx.Response(
            200,
            json={
                "data": {
                    "projectCreate": {
                        "success": True,
                        "project": {
                            "id": "project-id",
                            "name": "Feedback Inbox V1 · 2026-08-02 · 1",
                            "url": "https://linear.app/project-id",
                        },
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
        for index in range(1, 3)
    )
    for _ in range(1):
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
    responses.append(
        httpx.Response(
            200,
            json={
                "data": {
                    "projectUpdateCreate": {
                        "success": True,
                        "projectUpdate": {"id": "update-id"},
                    }
                }
            },
        )
    )
    route = respx.post("https://api.linear.app/graphql").mock(side_effect=responses)

    async with LinearSandbox(
        "Bearer token",
        routing_label_id="label-id",
        clock=lambda: datetime(2026, 8, 2, tzinfo=UTC),
    ) as sandbox:
        result = await sandbox.create_campaign(
            team_id="team-id",
            label="EXP-1-A1",
            repo_url="https://github.com/kulichevskiy/EXP-1-A1",
            campaign=feedback_inbox_campaign(),
            project_description=(
                "## Hypothesis\nThe revised review process will finish the sample project.\n\n"
                "## Experiment design\nRun one isolated copy of the project."
            ),
        )
        await sandbox.publish_project_update(
            project_id=result.project_id,
            health="onTrack",
            body="The experiment started with one isolated run of version A.",
        )

    assert result.issue_identifiers == tuple(f"BENCH-{index}" for index in range(1, 3))
    calls = [json.loads(call.request.content) for call in route.calls]
    issue_calls = [call for call in calls if "mutation BenchIssue" in call["query"]]
    project_calls = [call for call in calls if "mutation BenchProject(" in call["query"]]
    relation_calls = [call for call in calls if "mutation BenchRelation" in call["query"]]
    update_calls = [call for call in calls if "mutation BenchProjectUpdate" in call["query"]]
    assert all(call["variables"]["input"]["labelIds"] == ["label-id"] for call in issue_calls)
    assert [call["variables"]["input"] for call in project_calls] == [
        {
            "name": "Feedback Inbox V1 · 2026-08-02 · 1",
            "teamIds": ["team-id"],
            "content": (
                "## Hypothesis\nThe revised review process will finish the sample project.\n\n"
                "## Experiment design\nRun one isolated copy of the project."
            ),
        }
    ]
    assert [call["variables"]["input"] for call in update_calls] == [
        {
            "projectId": "project-id",
            "health": "onTrack",
            "body": "The experiment started with one isolated run of version A.",
        }
    ]
    assert all(call["variables"]["input"]["projectId"] == "project-id" for call in issue_calls)
    assert [call["variables"]["input"] for call in relation_calls] == [
        {"issueId": "issue-1", "relatedIssueId": "issue-2", "type": "blocks"},
    ]
    assert all("issueLabelCreate" not in call["query"] for call in calls)


@pytest.mark.asyncio
@respx.mock
async def test_linear_sandbox_does_not_repeat_the_same_project_chronicle_entry() -> None:
    route = respx.post("https://api.linear.app/graphql").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": {
                    "projectUpdateCreate": {
                        "success": True,
                        "projectUpdate": {"id": "update-id"},
                    }
                }
            },
        )
    )

    async with LinearSandbox("Bearer token", routing_label_id="label-id") as sandbox:
        await sandbox.publish_project_update(
            project_id="project-id", health="onTrack", body="Experiment started."
        )
        await sandbox.publish_project_update(
            project_id="project-id", health="onTrack", body="Experiment started."
        )

    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_linear_sandbox_reconciles_durable_project_chronicle_marker() -> None:
    marker = "<!-- symphony-bench-event:EXP-1:started -->"
    route = respx.post("https://api.linear.app/graphql").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": {
                    "project": {"projectUpdates": {"nodes": [{"body": f"Started.\n\n{marker}"}]}}
                }
            },
        )
    )

    async with LinearSandbox("Bearer token", routing_label_id="label-id") as sandbox:
        await sandbox.publish_project_update(
            project_id="project-id",
            health="onTrack",
            body="Started.",
            event_key="EXP-1:started",
        )

    calls = [json.loads(call.request.content) for call in route.calls]
    assert len(calls) == 1
    assert "query BenchProjectUpdates" in calls[0]["query"]


@pytest.mark.asyncio
@respx.mock
async def test_campaign_retry_reconciles_issue_created_before_response_was_lost() -> None:
    campaign = feedback_inbox_campaign()
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
        httpx.Response(200, json={"data": {"projects": {"nodes": []}}}),
        httpx.Response(
            200,
            json={
                "data": {
                    "projectCreate": {
                        "success": True,
                        "project": {"id": "project-id", "name": "project", "url": "url"},
                    }
                }
            },
        ),
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
        for index in range(2, 3)
    )
    for _ in range(1):
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
    assert result.issue_identifiers == tuple(f"BENCH-{index}" for index in range(1, 3))
    assert sum("mutation BenchIssue" in call["query"] for call in calls) == 2


@pytest.mark.asyncio
@respx.mock
async def test_campaign_retry_reconciles_relation_created_before_response_was_lost() -> None:
    campaign = feedback_inbox_campaign()
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
        httpx.Response(200, json={"data": {"projects": {"nodes": []}}}),
        httpx.Response(
            200,
            json={
                "data": {
                    "projectCreate": {
                        "success": True,
                        "project": {"id": "project-id", "name": "project", "url": "url"},
                    }
                }
            },
        ),
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
    for _ in range(0):
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
    assert len(result.issue_ids) == 2
    assert sum("mutation BenchRelation" in call["query"] for call in calls) == 1


@pytest.mark.asyncio
@respx.mock
async def test_campaign_retry_reuses_project_created_before_response_was_lost() -> None:
    campaign = Campaign(
        name="Feedback Inbox V1",
        tickets=(CampaignTicket(key="one", title="One", description="Do one"),),
    )
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
    project = {
        "id": "project-id",
        "name": "Feedback Inbox V1 · 2026-08-02 · RETRY",
        "url": "https://linear.app/project-id",
    }
    route = respx.post("https://api.linear.app/graphql").mock(
        side_effect=[
            team,
            httpx.Response(200, json={"data": {"projects": {"nodes": []}}}),
            httpx.ReadTimeout("response lost"),
            team,
            httpx.Response(200, json={"data": {"projects": {"nodes": [project]}}}),
            httpx.Response(200, json={"data": {"issues": {"nodes": []}}}),
            httpx.Response(
                200,
                json={
                    "data": {
                        "issueCreate": {
                            "success": True,
                            "issue": {
                                "id": "issue-1",
                                "identifier": "BENCH-1",
                                "url": "https://linear.app/BENCH-1",
                            },
                        }
                    }
                },
            ),
        ]
    )

    async with LinearSandbox("Bearer token", routing_label_id="label-id") as sandbox:
        with pytest.raises(linear_module.LinearSandboxError):
            await sandbox.create_campaign(
                team_id="team-id",
                label="EXP-RETRY-A1",
                repo_url="https://github.com/kulichevskiy/EXP-RETRY-A1",
                campaign=campaign,
            )
        result = await sandbox.create_campaign(
            team_id="team-id",
            label="EXP-RETRY-A1",
            repo_url="https://github.com/kulichevskiy/EXP-RETRY-A1",
            campaign=campaign,
        )

    calls = [json.loads(call.request.content) for call in route.calls]
    issue_call = next(call for call in calls if "mutation BenchIssue" in call["query"])
    assert result.issue_identifiers == ("BENCH-1",)
    assert issue_call["variables"]["input"]["projectId"] == "project-id"
    assert sum("mutation BenchProject" in call["query"] for call in calls) == 1


@pytest.mark.asyncio
@respx.mock
async def test_parallel_trials_reuse_one_experiment_project() -> None:
    campaign = Campaign(
        name="Feedback Inbox V1",
        tickets=(CampaignTicket(key="one", title="One", description="Do one"),),
    )
    project: dict[str, str] | None = None
    project_creates = 0
    issue_project_ids: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal project, project_creates
        payload = json.loads(request.content)
        query = payload["query"]
        variables = payload["variables"]
        if "query BenchTeam" in query:
            return httpx.Response(
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
        if "query BenchProjects" in query:
            return httpx.Response(
                200,
                json={"data": {"projects": {"nodes": [project] if project else []}}},
            )
        if "mutation BenchProject" in query:
            project_creates += 1
            project = {
                "id": "project-id",
                "name": variables["input"]["name"],
                "url": "https://linear.app/project-id",
            }
            return httpx.Response(
                200,
                json={"data": {"projectCreate": {"success": True, "project": project}}},
            )
        if "query BenchCampaignIssues" in query:
            return httpx.Response(200, json={"data": {"issues": {"nodes": []}}})
        if "mutation BenchIssue" in query:
            issue_project_ids.append(variables["input"]["projectId"])
            identifier = f"BENCH-{len(issue_project_ids)}"
            return httpx.Response(
                200,
                json={
                    "data": {
                        "issueCreate": {
                            "success": True,
                            "issue": {
                                "id": f"issue-{len(issue_project_ids)}",
                                "identifier": identifier,
                                "url": f"https://linear.app/{identifier}",
                            },
                        }
                    }
                },
            )
        raise AssertionError(query)

    respx.post("https://api.linear.app/graphql").mock(side_effect=respond)
    first = LinearSandbox("Bearer token", routing_label_id="label-id")
    second = LinearSandbox("Bearer token", routing_label_id="label-id")
    try:
        await asyncio.gather(
            first.create_campaign(
                team_id="team-id",
                label="EXP-PARALLEL-A1",
                repo_url="https://github.com/example/a",
                campaign=campaign,
            ),
            second.create_campaign(
                team_id="team-id",
                label="EXP-PARALLEL-B1",
                repo_url="https://github.com/example/b",
                campaign=campaign,
            ),
        )
    finally:
        await first.aclose()
        await second.aclose()

    assert project_creates == 1
    assert issue_project_ids == ["project-id", "project-id"]


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
