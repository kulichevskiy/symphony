from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import Linear, LinearError
from symphony.orchestrator.poll import Orchestrator
from symphony.orchestrator.reconciler import Reconciler
from symphony.tracker import Issue, TrackerContext


def _binding(team_key: str = "ENG") -> RepoBinding:
    return RepoBinding(
        linear_team_key=team_key,
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _issue() -> Issue:
    return Issue(
        id="iss-1",
        identifier="ENG-1",
        title="Test issue",
        description="",
        url="https://linear.test/ENG-1",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["symphony"],
    )


def test_linear_tracker_implements_issue_tracker_protocol() -> None:
    from symphony.tracker import IssueTracker

    linear = Linear("test-key")
    try:
        assert isinstance(linear, IssueTracker)
    finally:
        # Avoid leaking the underlying httpx client from this structural check.
        import asyncio

        asyncio.run(linear.aclose())


def test_orchestrator_and_reconciler_do_not_store_linear_client_attrs() -> None:
    assert "self.linear" not in inspect.getsource(Orchestrator)
    assert "self._linear" not in inspect.getsource(Reconciler)


def test_binding_identity_includes_tracker_context() -> None:
    from symphony.orchestrator import poll as poll_module
    from symphony.orchestrator import reconciler as reconciler_module

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_provider = "linear-alt"
    secondary_binding.tracker_site = "secondary"

    assert poll_module._binding_key(default_binding) != poll_module._binding_key(  # noqa: SLF001
        secondary_binding
    )
    assert poll_module._binding_storage_key(  # noqa: SLF001
        default_binding
    ) != poll_module._binding_storage_key(secondary_binding)  # noqa: SLF001
    assert reconciler_module._binding_storage_key(  # noqa: SLF001
        default_binding
    ) != reconciler_module._binding_storage_key(secondary_binding)  # noqa: SLF001


def test_reconciler_registers_configured_tracker_contexts() -> None:
    binding = _binding()
    binding.tracker_site = "secondary"
    tracker = MagicMock()

    reconciler = Reconciler(
        Config(repos=[binding]),
        MagicMock(),
        tracker,
        MagicMock(),
    )

    assert reconciler.tracker(TrackerContext()) is tracker
    assert reconciler.tracker(TrackerContext(provider="linear", site="secondary")) is tracker


@pytest.mark.asyncio
async def test_warmup_caches_states_by_provider_site_and_team(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db
    from symphony.tracker import DEFAULT_PROVIDER, DEFAULT_SITE

    binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_site = "secondary"
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        linear = AsyncMock()
        linear.viewer_team_keys = AsyncMock(return_value=["ENG"])
        linear.team_states = AsyncMock(return_value={"Todo": "state-default"})
        secondary = AsyncMock()
        secondary.viewer_team_keys = AsyncMock(return_value=["ENG"])
        secondary.team_states = AsyncMock(return_value={"Todo": "state-secondary"})
        orch = Orchestrator(
            Config(repos=[binding, secondary_binding]),
            linear,
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )
        orch._trackers.register("linear", "secondary", secondary)  # noqa: SLF001

        await orch.warmup()

        assert orch._states == {  # noqa: SLF001
            (DEFAULT_PROVIDER, DEFAULT_SITE, "ENG"): {"Todo": "state-default"},
            (DEFAULT_PROVIDER, "secondary", "ENG"): {"Todo": "state-secondary"},
        }
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_warmup_registers_configured_tracker_context(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    binding = _binding()
    binding.tracker_provider = "linear-alt"
    binding.tracker_site = "secondary"
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        tracker = AsyncMock()
        tracker.viewer_team_keys = AsyncMock(return_value=["ENG"])
        tracker.team_states = AsyncMock(return_value={"Todo": "state-secondary"})
        orch = Orchestrator(
            Config(repos=[binding]),
            tracker,
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )

        await orch.warmup()

        assert orch.tracker(binding) is tracker
        assert orch._states == {  # noqa: SLF001
            ("linear-alt", "secondary", "ENG"): {"Todo": "state-secondary"},
        }
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_binding_scoped_lookup_uses_binding_tracker(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    binding = _binding()
    binding.tracker_site = "secondary"
    issue = _issue()
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(
            side_effect=AssertionError("default tracker used")
        )
        secondary_tracker = AsyncMock()
        secondary_tracker.lookup_issue = AsyncMock(return_value=issue)
        orch = Orchestrator(
            Config(repos=[binding]),
            default_tracker,
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )
        orch._trackers.register("linear", "secondary", secondary_tracker)  # noqa: SLF001

        refreshed = await orch._refresh_dispatch_candidate(binding, issue)  # noqa: SLF001

        assert refreshed == issue
        secondary_tracker.lookup_issue.assert_awaited_once_with("iss-1")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_issue_webhook_uses_recorded_tracker_context(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_site = "secondary"
    issue = _issue()
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider="linear",
            site="secondary",
        )
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(
            side_effect=AssertionError("default tracker used")
        )
        secondary_tracker = AsyncMock()
        secondary_tracker.lookup_issue = AsyncMock(return_value=issue)
        orch = Orchestrator(
            Config(repos=[default_binding, secondary_binding]),
            default_tracker,
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )
        orch._trackers.register("linear", "secondary", secondary_tracker)  # noqa: SLF001
        scheduled_task = object()
        orch._schedule_dispatch = MagicMock(return_value=scheduled_task)  # type: ignore[method-assign]  # noqa: SLF001

        result = await orch.handle_linear_webhook(
            {
                "type": "Issue",
                "action": "update",
                "data": {"id": issue.id},
            }
        )

        assert result.handled is True
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        default_tracker.lookup_issue.assert_not_awaited()
        orch._schedule_dispatch.assert_called_once_with(secondary_binding, issue)  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_unseen_issue_webhook_probes_configured_tracker_contexts(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_site = "secondary"
    issue = _issue()
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(
            side_effect=LinearError(f"issue not found: {issue.id}")
        )
        secondary_tracker = AsyncMock()
        secondary_tracker.lookup_issue = AsyncMock(return_value=issue)
        orch = Orchestrator(
            Config(repos=[default_binding, secondary_binding]),
            default_tracker,
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )
        orch._trackers.register("linear", "secondary", secondary_tracker)  # noqa: SLF001
        scheduled_task = object()
        orch._schedule_dispatch = MagicMock(return_value=scheduled_task)  # type: ignore[method-assign]  # noqa: SLF001

        result = await orch.handle_linear_webhook(
            {
                "type": "Issue",
                "action": "create",
                "data": {"id": issue.id},
            }
        )

        assert result.handled is True
        default_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        orch._schedule_dispatch.assert_called_once_with(secondary_binding, issue)  # noqa: SLF001
    finally:
        await conn.close()


def test_poll_no_longer_computes_author_is_me_from_comment_body() -> None:
    source = inspect.getsource(Orchestrator)
    assert "is_symphony_comment" not in source
