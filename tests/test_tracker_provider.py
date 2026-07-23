from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from symphony.config import Config, LinearStates, RepoBinding, ResolvedRole, Secrets, TrackerStates
from symphony.linear.client import Linear, LinearError
from symphony.linear.slash import SlashIntent, SlashKind
from symphony.orchestrator.poll import Orchestrator
from symphony.orchestrator.reconciler import Reconciler
from symphony.pipeline.cost_guard import UsageDelta
from symphony.tracker import Comment, Issue, TrackerContext


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


def _comment(body: str) -> Comment:
    return Comment(
        id="c-command",
        body=body,
        created_at="2026-05-10T12:00:00+00:00",
        author_name="operator",
        author_is_me=False,
        external_thread_type=None,
    )


def _intent(kind: SlashKind) -> SlashIntent:
    return SlashIntent(
        kind=kind,
        comment_id="c-command",
        created_at="2026-05-10T12:00:00+00:00",
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


def test_for_binding_builds_linear_or_jira_tracker(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from symphony.jira.client import JiraTracker
    from symphony.linear.client import LinearTracker
    from symphony.tracker import TrackerRegistry, for_binding

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test")
    monkeypatch.setenv("JIRA_BASE_URL", "https://jira.example.test")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.test")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    secrets = Secrets()
    linear_binding = _binding()
    jira_binding = RepoBinding(
        provider="jira",
        project_key="SYM",
        base_url="https://jira.example.test",
        github_repo="org/repo",
        states=TrackerStates(ready="To Do", code_review="In Review"),
    )
    registry = TrackerRegistry()

    linear = for_binding(linear_binding, secrets, registry=registry)
    jira = for_binding(jira_binding, secrets, registry=registry)
    try:
        assert isinstance(linear, LinearTracker)
        assert isinstance(jira, JiraTracker)
        assert registry.resolve(TrackerContext(provider="linear", site="default")) is linear
        assert (
            registry.resolve(TrackerContext(provider="jira", site="https://jira.example.test"))
            is jira
        )
    finally:
        import asyncio

        asyncio.run(linear.aclose())
        asyncio.run(jira.aclose())


def test_for_binding_registers_jira_secret_base_url(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from symphony.jira.client import JiraTracker
    from symphony.tracker import TrackerRegistry, for_binding

    monkeypatch.setenv("JIRA_BASE_URL", "https://jira.example.test")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.test")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    secrets = Secrets()
    binding = RepoBinding(
        provider="jira",
        project_key="SYM",
        github_repo="org/repo",
        states=TrackerStates(ready="To Do", code_review="In Review"),
    )
    registry = TrackerRegistry()

    jira = for_binding(binding, secrets, registry=registry)
    try:
        assert isinstance(jira, JiraTracker)
        assert binding.tracker_site == "https://jira.example.test"
        assert (
            registry.resolve(TrackerContext(provider="jira", site="https://jira.example.test"))
            is jira
        )
    finally:
        import asyncio

        asyncio.run(jira.aclose())


def test_for_binding_registers_same_site_jira_projects_separately(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    from symphony.tracker import TrackerRegistry, context_for_binding, for_binding

    monkeypatch.setenv("JIRA_BASE_URL", "https://jira.example.test")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.test")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    secrets = Secrets()
    sym_binding = RepoBinding(
        provider="jira",
        project_key="SYM",
        base_url="https://jira.example.test",
        github_repo="org/repo",
        states=TrackerStates(ready="To Do", code_review="In Review"),
    )
    ops_binding = RepoBinding(
        provider="jira",
        project_key="OPS",
        base_url="https://jira.example.test",
        github_repo="org/ops",
        states=TrackerStates(ready="To Do", code_review="In Review"),
    )
    registry = TrackerRegistry()

    sym = for_binding(sym_binding, secrets, registry=registry)
    ops = for_binding(ops_binding, secrets, registry=registry)
    try:
        assert registry.resolve(context_for_binding(sym_binding)) is sym
        assert registry.resolve(context_for_binding(ops_binding)) is ops
        with pytest.raises(KeyError, match="provide project_key"):
            registry.resolve(TrackerContext(provider="jira", site="https://jira.example.test"))
    finally:
        import asyncio

        asyncio.run(sym.aclose())
        asyncio.run(ops.aclose())


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
async def test_orchestrator_uses_supplied_tracker_registry(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db
    from symphony.tracker import TrackerRegistry

    linear_binding = _binding()
    jira_binding = RepoBinding(
        provider="jira",
        project_key="SYM",
        base_url="https://jira.example.test",
        github_repo="org/repo",
        states=TrackerStates(ready="To Do", code_review="In Review"),
    )
    linear = AsyncMock()
    jira = AsyncMock()
    registry = TrackerRegistry()
    registry.register("linear", "default", linear)
    registry.register("jira", "https://jira.example.test", jira)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        orch = Orchestrator(
            Config(repos=[linear_binding, jira_binding]),
            registry,
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )

        assert orch.tracker(linear_binding) is linear
        assert orch.tracker(jira_binding) is jira
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_jira_binding_does_not_crash_warmup_or_tick(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from symphony import db
    from symphony.jira.client import JiraTracker
    from symphony.tracker import TrackerRegistry, for_binding

    monkeypatch.setenv("JIRA_BASE_URL", "https://jira.example.test")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.test")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    binding = RepoBinding(
        provider="jira",
        project_key="SYM",
        base_url="https://jira.example.test",
        github_repo="org/repo",
        states=TrackerStates(ready="To Do", code_review="In Review"),
    )
    registry = TrackerRegistry()
    jira = for_binding(binding, Secrets(), registry=registry)
    assert isinstance(jira, JiraTracker)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        orch = Orchestrator(
            Config(repos=[binding]),
            registry,
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )

        await orch.warmup()
        scheduled = await orch._tick()  # noqa: SLF001

        assert scheduled == []
        assert (
            orch._states[("jira", "https://jira.example.test", "SYM")][  # noqa: SLF001
                "To Do"
            ]
            == "To Do"
        )
    finally:
        await jira.aclose()
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
async def test_dispatch_success_persists_followup_state_under_scoped_issue_id(tmp_path) -> None:  # type: ignore[no-untyped-def]
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
        tracker = AsyncMock()
        tracker.team_states = AsyncMock(
            return_value={
                "Todo": "state-todo",
                "In Progress": "state-progress",
                "Needs Approval": "state-review",
            }
        )
        tracker.post_comment = AsyncMock(return_value="cmt-1")
        tracker.move_issue = AsyncMock()
        gh = MagicMock()
        gh.repo_default_branch = AsyncMock(return_value="main")
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "workspace")
        workspace.release = MagicMock()
        orch = Orchestrator(
            Config(repos=[default_binding, secondary_binding]),
            tracker,
            conn,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._run_agent = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=(UsageDelta(cost_usd=0.25), "exit", 0, ResolvedRole(agent="claude"))
        )

        # `_run_agent` is mocked, so simulate the agent advancing HEAD so the
        # completion gate classifies the run as completed.
        with patch(
            # SYM-150: the completion gate reads `_workspace_head_sha` from
            # `poll._lifecycle`, where `_dispatch_one` now lives.
            "symphony.orchestrator.poll._lifecycle._workspace_head_sha",
            AsyncMock(side_effect=["base-sha", "advanced-sha"]),
        ):
            run_id = await orch._dispatch_one(secondary_binding, issue)  # noqa: SLF001

        scoped_issue_id = db.issues.contextual_id(
            id=issue.id,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        cur = await conn.execute("SELECT issue_id, stage, status FROM runs ORDER BY stage")
        rows = [dict(row) for row in await cur.fetchall()]
        assert rows == [
            {"issue_id": scoped_issue_id, "stage": "implement", "status": "completed"},
            {"issue_id": scoped_issue_id, "stage": "review", "status": "running"},
        ]
        cur = await conn.execute("SELECT issue_id, pr_number, github_repo FROM review_state")
        assert [dict(row) for row in await cur.fetchall()] == [
            {
                "issue_id": scoped_issue_id,
                "pr_number": 42,
                "github_repo": "org/repo",
            }
        ]
        pr = await db.issue_prs.get_for_issue(conn, issue_id=scoped_issue_id)
        assert pr is not None
        assert pr.pr_number == 42
        assert await db.issue_prs.get_for_issue(conn, issue_id=issue.id) is None
        assert run_id is not None
        assert orch._dispatch_run_ids[scoped_issue_id] == run_id  # noqa: SLF001
        assert all(call.args[0] == issue.id for call in tracker.post_comment.await_args_list)
        assert all(call.args[0] == issue.id for call in tracker.move_issue.await_args_list)
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
async def test_reconciler_translates_scoped_storage_id_before_tracker_lookup(tmp_path) -> None:  # type: ignore[no-untyped-def]
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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=scoped_issue_id,
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id=scoped_issue_id,
            run_id="run-1",
            kind=db.operator_waits.KIND_MERGE,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-05-10T01:00:00+00:00",
            tracker_provider=secondary_binding.tracker_provider,
            tracker_site=secondary_binding.tracker_site,
        )
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
        secondary_tracker = AsyncMock()
        secondary_tracker.lookup_issue = AsyncMock(return_value=issue)
        reconciler = Reconciler(
            Config(repos=[default_binding, secondary_binding]),
            conn,
            default_tracker,
            MagicMock(),
        )
        reconciler._trackers.register(  # noqa: SLF001
            "linear-alt", "secondary", secondary_tracker
        )

        observed = await reconciler.reconcile_issue(scoped_issue_id, reason="test")

        assert observed == 2
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        default_tracker.lookup_issue.assert_not_awaited()
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
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
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
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
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
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
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
async def test_issue_webhook_provider_context_wins_over_default_storage_id(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
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
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
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
async def test_state_webhook_reconciles_scoped_storage_issue_id(tmp_path) -> None:  # type: ignore[no-untyped-def]
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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
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
        orch._schedule_dispatch = MagicMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=scheduled_task
        )
        spy = MagicMock()
        spy.reconcile_linear_issue_event = AsyncMock(return_value=2)
        orch._reconciler = spy  # noqa: SLF001

        result = await orch.handle_linear_webhook(
            {
                "type": "Issue",
                "action": "update",
                "updatedFrom": {"stateId": "old-state"},
                "data": {"id": issue.id, "state": {"id": issue.state_id}},
            },
            provider="linear-alt",
        )
        await orch.drain_reconcile_event_tasks()

        assert result.handled is True
        spy.reconcile_linear_issue_event.assert_awaited_once_with(
            issue_id=scoped_issue_id,
            action="update",
        )
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        default_tracker.lookup_issue.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_slash_polling_fetches_comments_with_tracker_issue_id_for_scoped_issue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        default_tracker = AsyncMock()
        default_tracker.comments_since = AsyncMock(
            side_effect=AssertionError("default tracker used")
        )
        secondary_tracker = AsyncMock()
        secondary_tracker.comments_since = AsyncMock(return_value=[])
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
        orch._review_poll_issue_ids[scoped_issue_id] = "review-run"  # noqa: SLF001
        orch._review_poll_run_ids.add("review-run")  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        default_tracker.comments_since.assert_not_awaited()
        secondary_tracker.comments_since.assert_awaited_once()
        assert secondary_tracker.comments_since.await_args.args[0] == issue.id
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_failed_retry_moves_tracker_issue_id_for_scoped_issue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=scoped_issue_id,
            stage="implement",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id=scoped_issue_id,
            run_id="run-1",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key=secondary_binding.linear_team_key,
            github_repo=secondary_binding.github_repo,
            issue_label=secondary_binding.issue_label or "",
            created_at="2026-05-10T01:00:00+00:00",
            tracker_provider=secondary_binding.tracker_provider,
            tracker_site=secondary_binding.tracker_site,
        )
        default_tracker = AsyncMock()
        default_tracker.comments_since = AsyncMock(
            side_effect=AssertionError("default tracker used")
        )
        secondary_tracker = AsyncMock()
        secondary_tracker.comments_since = AsyncMock(return_value=[_comment("$retry")])
        secondary_tracker.team_states = AsyncMock(return_value={"Todo": "state-todo"})
        secondary_tracker.move_issue = AsyncMock()
        secondary_tracker.post_comment = AsyncMock(return_value="c-resumed")
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

        await orch._poll_slash_commands()  # noqa: SLF001

        default_tracker.comments_since.assert_not_awaited()
        secondary_tracker.comments_since.assert_awaited_once()
        assert secondary_tracker.comments_since.await_args.args[0] == issue.id
        secondary_tracker.move_issue.assert_awaited_once_with(issue.id, "state-todo")
        secondary_tracker.post_comment.assert_awaited_once()
        assert secondary_tracker.post_comment.await_args.args[0] == issue.id
        assert scoped_issue_id not in orch._dispatch_run_ids  # noqa: SLF001
        assert await db.operator_waits.get(conn, scoped_issue_id) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_failed_retry_uses_tracker_issue_id_for_scoped_issue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        await db.runs.create(
            conn,
            id="review-run",
            issue_id=scoped_issue_id,
            stage="review",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.review_state.begin_review(
            conn,
            scoped_issue_id,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo=secondary_binding.github_repo,
            issue_label=secondary_binding.issue_label,
        )
        await db.operator_waits.upsert(
            conn,
            issue_id=scoped_issue_id,
            run_id="review-run",
            kind=db.operator_waits.KIND_REVIEW_FAILED,
            linear_team_key=secondary_binding.linear_team_key,
            github_repo=secondary_binding.github_repo,
            issue_label=secondary_binding.issue_label or "",
            created_at="2026-05-10T01:00:00+00:00",
            tracker_provider=secondary_binding.tracker_provider,
            tracker_site=secondary_binding.tracker_site,
        )
        default_tracker = AsyncMock()
        default_tracker.comments_since = AsyncMock(
            side_effect=AssertionError("default tracker used")
        )
        secondary_tracker = AsyncMock()
        secondary_tracker.comments_since = AsyncMock(return_value=[_comment("$retry")])
        secondary_tracker.lookup_issue = AsyncMock(return_value=issue)
        secondary_tracker.team_states = AsyncMock(return_value={"Needs Approval": "state-review"})
        secondary_tracker.move_issue = AsyncMock()
        secondary_tracker.post_comment = AsyncMock(return_value="c-resumed")
        gh = MagicMock()
        gh.pr_comment = AsyncMock()
        orch = Orchestrator(
            Config(repos=[default_binding, secondary_binding]),
            default_tracker,
            conn,
            gh=gh,
            workspace=MagicMock(),
        )
        orch._trackers.register(  # noqa: SLF001
            "linear-alt", "secondary", secondary_tracker
        )
        orch._schedule_review_poll = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        default_tracker.comments_since.assert_not_awaited()
        secondary_tracker.comments_since.assert_awaited_once()
        assert secondary_tracker.comments_since.await_args.args[0] == issue.id
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        secondary_tracker.move_issue.assert_awaited_once_with(issue.id, "state-review")
        secondary_tracker.post_comment.assert_awaited_once()
        assert secondary_tracker.post_comment.await_args.args[0] == issue.id
        assert scoped_issue_id not in orch._dispatch_run_ids  # noqa: SLF001
        assert await db.operator_waits.get(conn, scoped_issue_id) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_cap_reject_uses_tracker_issue_id_for_scoped_issue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    """SYM-114 review: a review-cap park's `$reject` must look up/move the
    tracker's own issue id, not Symphony's internal (scoped) storage id."""
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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        await db.runs.create(
            conn,
            id="review-run",
            issue_id=scoped_issue_id,
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.review_state.begin_review(
            conn,
            scoped_issue_id,
            pr_number=166,
            pr_url="https://github.com/org/repo/pull/166",
            github_repo=secondary_binding.github_repo,
            issue_label=secondary_binding.issue_label,
        )
        await db.operator_waits.upsert(
            conn,
            issue_id=scoped_issue_id,
            run_id="review-run",
            kind=db.operator_waits.KIND_REVIEW_CAP,
            linear_team_key=secondary_binding.linear_team_key,
            github_repo=secondary_binding.github_repo,
            issue_label=secondary_binding.issue_label or "",
            created_at="2026-05-10T01:00:00+00:00",
            tracker_provider=secondary_binding.tracker_provider,
            tracker_site=secondary_binding.tracker_site,
        )
        default_tracker = AsyncMock()
        default_tracker.comments_since = AsyncMock(
            side_effect=AssertionError("default tracker used")
        )
        secondary_tracker = AsyncMock()
        secondary_tracker.comments_since = AsyncMock(return_value=[_comment("$reject")])
        secondary_tracker.lookup_issue = AsyncMock(return_value=issue)
        secondary_tracker.team_states = AsyncMock(return_value={"Blocked": "state-blocked"})
        secondary_tracker.move_issue = AsyncMock()
        secondary_tracker.post_comment = AsyncMock(return_value="c-resumed")
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
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        secondary_tracker.move_issue.assert_awaited_once_with(issue.id, "state-blocked")
        assert await db.operator_waits.get(conn, scoped_issue_id) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_blocked_retry_moves_tracker_issue_id_for_scoped_issue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=scoped_issue_id,
            stage="acceptance",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id=scoped_issue_id,
            run_id="run-1",
            kind=db.operator_waits.KIND_ACCEPTANCE_BLOCKED,
            linear_team_key=secondary_binding.linear_team_key,
            github_repo=secondary_binding.github_repo,
            issue_label=secondary_binding.issue_label or "",
            created_at="2026-05-10T01:00:00+00:00",
            tracker_provider=secondary_binding.tracker_provider,
            tracker_site=secondary_binding.tracker_site,
        )
        default_tracker = AsyncMock()
        default_tracker.move_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
        secondary_tracker = AsyncMock()
        secondary_tracker.team_states = AsyncMock(
            return_value={secondary_binding.linear_states.needs_approval: "state-review"}
        )
        secondary_tracker.move_issue = AsyncMock()
        secondary_tracker.post_comment = AsyncMock(return_value="c-resumed")
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

        await orch._handle_acceptance_blocked_slash_intent(  # noqa: SLF001
            scoped_issue_id,
            "run-1",
            _intent(SlashKind.RETRY_ACCEPTANCE),
        )

        default_tracker.move_issue.assert_not_awaited()
        secondary_tracker.move_issue.assert_awaited_once_with(issue.id, "state-review")
        secondary_tracker.post_comment.assert_awaited_once()
        assert secondary_tracker.post_comment.await_args.args[0] == issue.id
        assert await db.operator_waits.get(conn, scoped_issue_id) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_blocked_skip_uses_tracker_issue_id_for_scoped_issue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=scoped_issue_id,
            stage="acceptance",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.acceptance_state.begin_acceptance(
            conn,
            scoped_issue_id,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="abc123",
            mode="code_only",
            preview_url="",
            extracted_criteria="",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id=scoped_issue_id,
            run_id="run-1",
            kind=db.operator_waits.KIND_ACCEPTANCE_BLOCKED,
            linear_team_key=secondary_binding.linear_team_key,
            github_repo=secondary_binding.github_repo,
            issue_label=secondary_binding.issue_label or "",
            created_at="2026-05-10T01:00:00+00:00",
            tracker_provider=secondary_binding.tracker_provider,
            tracker_site=secondary_binding.tracker_site,
        )
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
        secondary_tracker = AsyncMock()
        secondary_tracker.lookup_issue = AsyncMock(return_value=issue)
        secondary_tracker.post_comment = AsyncMock(return_value="c-skipped")
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
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._handle_acceptance_blocked_slash_intent(  # noqa: SLF001
            scoped_issue_id,
            "run-1",
            _intent(SlashKind.SKIP_ACCEPTANCE),
        )

        default_tracker.lookup_issue.assert_not_awaited()
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        secondary_tracker.post_comment.assert_awaited_once()
        assert secondary_tracker.post_comment.await_args.args[0] == issue.id
        assert await db.operator_waits.get(conn, scoped_issue_id) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_wait_reconcile_uses_tracker_issue_id_for_scoped_issue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    from symphony import db
    from symphony.orchestrator import poll as poll_module
    from symphony.pipeline.review_classifier import Verdict, VerdictKind

    default_binding = _binding()
    secondary_binding = _binding()
    secondary_binding.tracker_provider = "linear-alt"
    secondary_binding.tracker_site = "secondary"
    issue = _issue()
    issue.state_name = secondary_binding.linear_states.code_review
    issue.state_id = "state-review"
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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        await db.runs.create(
            conn,
            id="merge-run",
            issue_id=scoped_issue_id,
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T00:02:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id=scoped_issue_id,
            run_id="merge-run",
            kind=db.operator_waits.KIND_MERGE,
            linear_team_key=secondary_binding.linear_team_key,
            github_repo=secondary_binding.github_repo,
            issue_label=secondary_binding.issue_label or "",
            created_at="2026-05-10T00:03:00+00:00",
            tracker_provider=secondary_binding.tracker_provider,
            tracker_site=secondary_binding.tracker_site,
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=scoped_issue_id,
            github_repo=secondary_binding.github_repo,
            binding_key=poll_module._binding_storage_key(secondary_binding),
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:01:00+00:00",
        )
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
        secondary_tracker = AsyncMock()
        secondary_tracker.lookup_issue = AsyncMock(return_value=issue)
        secondary_tracker.post_comment = AsyncMock(return_value="c-reconcile")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "main",
                "mergedAt": None,
            }
        )
        orch = Orchestrator(
            Config(repos=[default_binding, secondary_binding]),
            default_tracker,
            conn,
            gh=gh,
            workspace=MagicMock(),
        )
        orch._trackers.register(  # noqa: SLF001
            "linear-alt", "secondary", secondary_tracker
        )
        orch._review_verdict_for_pr = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="test_approved")
        )
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        assert await orch._reconcile_auto_recoverable_merge_waits() == 1  # noqa: SLF001

        default_tracker.lookup_issue.assert_not_awaited()
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
        secondary_tracker.post_comment.assert_awaited_once()
        assert secondary_tracker.post_comment.await_args.args[0] == issue.id
        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_candidate_refresh_uses_tracker_issue_id_for_scoped_issue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    from symphony import db
    from symphony.orchestrator import poll as poll_module

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
        scoped_issue_id = await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        await db.runs.create(
            conn,
            id="review-run",
            issue_id=scoped_issue_id,
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=scoped_issue_id,
            github_repo=secondary_binding.github_repo,
            binding_key=poll_module._binding_storage_key(secondary_binding),
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:01:00+00:00",
        )
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
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

        scheduled = await orch._poll_merge_candidates()  # noqa: SLF001

        assert scheduled == []
        default_tracker.lookup_issue.assert_not_awaited()
        secondary_tracker.lookup_issue.assert_awaited_once_with(issue.id)
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
    secondary_binding.issue_label = "symphony"
    issue = _issue()
    issue.labels = []
    issue.state_name = "Needs Approval"
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
        scoped_issue_id = await db.issues.upsert(
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
            scoped_issue_id,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label="symphony",
        )
        await db.runs.create(
            conn,
            id="review-run",
            issue_id=scoped_issue_id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        default_tracker = AsyncMock()
        default_tracker.lookup_issue = AsyncMock(side_effect=AssertionError("default tracker used"))
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
        assert args[0].issue_id == scoped_issue_id
        assert args[1] is secondary_binding
        assert args[2] is issue
    finally:
        await conn.close()


def test_poll_no_longer_computes_author_is_me_from_comment_body() -> None:
    source = inspect.getsource(Orchestrator)
    assert "is_symphony_comment" not in source
