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
async def test_issue_upsert_treats_tracker_context_as_identity(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        default_storage_id = await db.issues.upsert(
            conn,
            id="shared-issue-id",
            identifier="ENG-1",
            title="Default issue",
            team_key="ENG",
            provider="linear",
            site="default",
        )
        secondary_storage_id = await db.issues.upsert(
            conn,
            id="shared-issue-id",
            identifier="ENG-2",
            title="Secondary issue",
            team_key="ENG",
            provider="linear",
            site="secondary",
        )
        await db.issues.upsert(
            conn,
            id="shared-issue-id",
            identifier="ENG-2",
            title="Secondary issue updated",
            team_key="ENG",
            provider="linear",
            site="secondary",
        )

        cur = await conn.execute(
            """
            SELECT id, tracker_issue_id, provider, site, identifier, title, team_key
              FROM issues
             WHERE tracker_issue_id = ?
             ORDER BY site ASC
            """,
            ("shared-issue-id",),
        )
        rows = await cur.fetchall()

        assert default_storage_id == "shared-issue-id"
        assert secondary_storage_id != default_storage_id
        assert [dict(row) for row in rows] == [
            {
                "id": "shared-issue-id",
                "tracker_issue_id": "shared-issue-id",
                "provider": "linear",
                "site": "default",
                "identifier": "ENG-1",
                "title": "Default issue",
                "team_key": "ENG",
            },
            {
                "id": secondary_storage_id,
                "tracker_issue_id": "shared-issue-id",
                "provider": "linear",
                "site": "secondary",
                "identifier": "ENG-2",
                "title": "Secondary issue updated",
                "team_key": "ENG",
            },
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_dispatch_uses_scoped_issue_id_returned_by_upsert(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_provider = "linear-alt"
    secondary_binding.tracker_site = "secondary"
    issue = _issue()
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier="ENG-0",
            title="Default issue",
            team_key=issue.team_key,
            provider=default_binding.tracker_provider,
            site=default_binding.tracker_site,
        )
        orch = Orchestrator(
            Config(repos=[default_binding, secondary_binding]),
            AsyncMock(),
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )
        orch._states_for_binding = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=LinearError("states unavailable")
        )

        run_id = await orch._dispatch_one(secondary_binding, issue)  # noqa: SLF001

        scoped_issue_id = db.issues.contextual_id(
            id=issue.id,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        cur = await conn.execute(
            "SELECT issue_id, status FROM runs WHERE id = ?",
            (run_id,),
        )
        row = await cur.fetchone()
        assert row is not None
        assert dict(row) == {"issue_id": scoped_issue_id, "status": "failed"}
        assert orch._dispatch_run_ids == {scoped_issue_id: run_id}  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_operator_wait_restore_uses_persisted_tracker_context(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_site = "secondary"
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Test issue",
            team_key="ENG",
            provider="linear",
            site="secondary",
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id="iss-1",
            stage="implement",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="run-1",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-05-10T01:00:00+00:00",
            tracker_provider="linear",
            tracker_site="secondary",
        )
        orch = Orchestrator(
            Config(repos=[default_binding, secondary_binding]),
            AsyncMock(),
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )

        await orch._restore_operator_waits()  # noqa: SLF001

        assert orch._implement_failed_run_bindings["run-1"] is secondary_binding  # noqa: SLF001
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


@pytest.mark.asyncio
async def test_unseen_issue_webhook_probes_non_default_tracker_provider(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_provider = "linear-alt"
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
        orch._trackers.register("linear-alt", "secondary", secondary_tracker)  # noqa: SLF001
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


@pytest.mark.asyncio
async def test_issue_webhook_provider_context_limits_unseen_lookup(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_provider = "linear-alt"
    secondary_binding.tracker_site = "secondary"
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
            Config(repos=[default_binding, secondary_binding]),
            default_tracker,
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )
        orch._trackers.register("linear-alt", "secondary", secondary_tracker)  # noqa: SLF001
        scheduled_task = object()
        orch._schedule_dispatch = MagicMock(return_value=scheduled_task)  # type: ignore[method-assign]  # noqa: SLF001

        result = await orch.handle_linear_webhook(
            {
                "type": "Issue",
                "action": "create",
                "data": {"id": issue.id},
            },
            provider="linear-alt",
        )

        assert result.handled is True
        default_tracker.lookup_issue.assert_not_awaited()
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        orch._schedule_dispatch.assert_called_once_with(secondary_binding, issue)  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_binding_lookup_uses_stored_tracker_context(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_provider = "linear-alt"
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
            provider="linear-alt",
            site="secondary",
        )
        await db.review_state.begin_review(
            conn,
            issue.id,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label=None,
        )
        orch = Orchestrator(
            Config(repos=[default_binding, secondary_binding]),
            AsyncMock(),
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )

        state = await db.review_state.get(conn, issue.id)
        binding = await orch._binding_for_review_issue_id(issue.id, state=state)  # noqa: SLF001

        assert binding is secondary_binding
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_uses_stored_tracker_context_for_rebound_binding(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    from symphony import db

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_provider = "linear-alt"
    secondary_binding.tracker_site = "secondary"
    issue = _issue()
    issue.state_name = "Needs Approval"
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider="linear-alt",
            site="secondary",
        )
        await db.review_state.begin_review(
            conn,
            issue.id,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label=None,
        )
        await db.runs.create(
            conn,
            id="review-run",
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
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
        orch._trackers.register(  # noqa: SLF001
            "linear-alt", "secondary", secondary_tracker
        )
        scheduled_task = object()
        orch._schedule_review_poll = MagicMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=scheduled_task
        )

        scheduled = await orch._poll_review_runs()  # noqa: SLF001

        assert scheduled == [scheduled_task]
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        default_tracker.lookup_issue.assert_not_awaited()
        args = orch._schedule_review_poll.call_args.args  # noqa: SLF001
        assert args[1] is secondary_binding
        assert args[2] is issue
    finally:
        await conn.close()


def test_poll_no_longer_computes_author_is_me_from_comment_body() -> None:
    source = inspect.getsource(Orchestrator)
    assert "is_symphony_comment" not in source
