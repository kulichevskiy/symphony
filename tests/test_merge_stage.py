"""Orchestrator-level Merge stage tests."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.github.client import CheckRun, GitHub, GitHubError, PRChecks
from symphony.linear.client import LinearError, LinearIssue
from symphony.orchestrator.poll import (
    Orchestrator,
    _binding_storage_key,
    _required_check_trigger_signature,
    _status_check_failed,
    _status_check_run_id,
)
from symphony.pipeline.cost_guard import UsageDelta
from symphony.pipeline.local_review_loop import LoopOutcome, LoopResult
from symphony.pipeline.review_classifier import Verdict, VerdictKind


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.kill_calls: list[str] = []
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        self.kill_calls.append(run_id)


@pytest.mark.parametrize("conclusion", ["STARTUP_FAILURE", "STALE"])
def test_status_check_failed_includes_terminal_failure_conclusions(
    conclusion: str,
) -> None:
    assert _status_check_failed({"state": "COMPLETED", "conclusion": conclusion})


def test_status_check_run_id_prefers_workflow_run_over_check_run_database_id() -> None:
    check = {
        "__typename": "CheckRun",
        "databaseId": 456,
        "workflowRun": {"databaseId": 123},
        "detailsUrl": "https://github.com/org/repo/actions/runs/789/job/101112",
    }

    assert _status_check_run_id(check) == "123"


def test_status_check_run_id_prefers_actions_url_over_check_run_database_id() -> None:
    check = {
        "__typename": "CheckRun",
        "databaseId": 456,
        "detailsUrl": "https://github.com/org/repo/actions/runs/789/job/101112",
    }

    assert _status_check_run_id(check) == "789"


class _BlockingRunner:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        self.started.set()
        yield RunnerEvent(kind="started", pid=123)
        await self.release.wait()
        yield RunnerEvent(kind="exit", returncode=0)

    async def kill(self, run_id: str) -> None:
        self.release.set()


class _CommittingRunner:
    def __init__(self) -> None:
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter(spec)

    async def _aiter(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        yield RunnerEvent(kind="started", pid=123)
        (spec.workspace_path / "merge-agent.txt").write_text(
            "merge agent final fix\n",
            encoding="utf-8",
        )
        await _git(spec.workspace_path, "add", "merge-agent.txt")
        await _git(spec.workspace_path, "commit", "-m", "merge agent final fix")
        yield RunnerEvent(kind="exit", returncode=0)

    async def kill(self, _run_id: str) -> None:
        return None


async def _git(workspace_path: Path, *args: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        "git",
        *args,
        cwd=workspace_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    assert proc.returncode == 0, (
        f"git {' '.join(args)} failed with {proc.returncode}: "
        f"{stderr.decode().strip()}"
    )
    return stdout.decode().strip()


async def _poll_and_wait(orch: Orchestrator) -> None:
    tasks = await orch._poll_merge_candidates()  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)


async def _dispatch_rebase_fix_with_started_callback(**kwargs: object) -> bool:
    on_started = cast(
        Callable[[str], Awaitable[None]] | None,
        kwargs.get("on_started"),
    )
    if on_started is not None:
        await on_started("review-fix-run")
    return True


def _binding(
    *,
    agent: str = "codex",
    issue_label: str | None = None,
    branch_prefix: str = "symphony",
    auto_merge: bool = True,
) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent=agent,  # type: ignore[arg-type]
        issue_label=issue_label,
        branch_prefix=branch_prefix,
        auto_merge=auto_merge,
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _split_review_binding(*, auto_merge: bool = False) -> RepoBinding:
    return _binding(agent="claude", auto_merge=auto_merge).model_copy(
        update={
            "linear_states": LinearStates(
                ready="Todo",
                code_review="In Review",
                needs_approval="Needs Input",
            )
        }
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-progress",
        state_name="In Progress",
        state_type="started",
        team_key="ENG",
        labels=["feature"],
    )


def _done_issue() -> LinearIssue:
    issue = _issue()
    issue.state_id = "state-done"
    issue.state_name = "Done"
    issue.state_type = "completed"
    return issue


def _ready_issue(issue_id: str = "iss-2", identifier: str = "ENG-2") -> LinearIssue:
    return LinearIssue(
        id=issue_id,
        identifier=identifier,
        title="Fresh task",
        description="Start later.",
        url=f"https://linear.app/team/issue/{identifier}",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["feature"],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "In Review": "state-review",
        "Needs Input": "state-input",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


def _issue_in_review() -> LinearIssue:
    issue = _issue()
    issue.state_id = "state-review"
    issue.state_name = "In Review"
    return issue


async def _seed_review_candidate(
    conn, *, binding_key: str = ""
) -> None:  # type: ignore[no-untyped-def]
    await db.issues.upsert(
        conn,
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        team_key="ENG",
    )
    await db.runs.create(
        conn,
        id="implement",
        issue_id="iss-1",
        stage="implement",
        status="completed",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
        cost_usd=0.50,
    )
    await db.runs.create(
        conn,
        id="review",
        issue_id="iss-1",
        stage="review",
        status="completed",
        pid=None,
        started_at="2026-05-10T00:01:00+00:00",
    )
    await db.issue_prs.upsert(
        conn,
        issue_id="iss-1",
        github_repo="org/repo",
        binding_key=binding_key,
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        created_at="2026-05-10T00:01:00+00:00",
    )


async def _seed_merged_pr(
    conn, *, merged_at: str, binding_key: str = ""
) -> None:  # type: ignore[no-untyped-def]
    await db.issues.upsert(
        conn,
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        team_key="ENG",
    )
    await db.issue_prs.upsert(
        conn,
        issue_id="iss-1",
        github_repo="org/repo",
        binding_key=binding_key,
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        created_at="2026-05-10T00:01:00+00:00",
    )
    await db.issue_prs.mark_merged(
        conn,
        issue_id="iss-1",
        github_repo="org/repo",
        merged_at=merged_at,
    )


async def _seed_merge_operator_wait(conn) -> None:  # type: ignore[no-untyped-def]
    await db.issues.upsert(
        conn,
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        team_key="ENG",
    )
    await db.runs.create(
        conn,
        id="merge-run",
        issue_id="iss-1",
        stage="merge",
        status="needs_approval",
        pid=None,
        started_at="2026-05-10T00:02:00+00:00",
    )
    await db.operator_waits.upsert(
        conn,
        issue_id="iss-1",
        run_id="merge-run",
        kind=db.operator_waits.KIND_MERGE,
        linear_team_key="ENG",
        github_repo="org/repo",
        issue_label="",
        created_at="2026-05-10T00:03:00+00:00",
    )
    await db.issue_prs.upsert(
        conn,
        issue_id="iss-1",
        github_repo="org/repo",
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        created_at="2026-05-10T00:01:00+00:00",
    )


def _make_merge_wait_orchestrator(
    conn,
    *,
    gh_view: dict[str, object],
    review_verdict: Verdict | None = None,
    linear: AsyncMock | None = None,
) -> Orchestrator:  # type: ignore[no-untyped-def]
    gh = MagicMock()
    gh.pr_view = AsyncMock(return_value=gh_view)
    if linear is None:
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
    orch = Orchestrator(
        Config(repos=[_binding()]),
        linear,
        conn,
        runner=MagicMock(),
        gh=gh,
        workspace=MagicMock(),
        push_fn=AsyncMock(),
    )
    if review_verdict is None:
        review_verdict = Verdict(kind=VerdictKind.APPROVED, rule="test_approved")
    orch._review_verdict_for_pr = AsyncMock(return_value=review_verdict)  # type: ignore[method-assign]  # noqa: SLF001
    return orch


@pytest.mark.asyncio
async def test_reconcile_merge_wait_conflict_dispatches_rebase_fix(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_merge_operator_wait(conn)
        await db.runs.create(
            conn,
            id="review-run",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:30+00:00",
        )
        conflict_view = {
            "headRefOid": "abc123",
            "mergeable": "CONFLICTING",
            "mergeStateStatus": "DIRTY",
            "baseRefName": "main",
            "mergedAt": None,
        }
        orch = _make_merge_wait_orchestrator(conn, gh_view=conflict_view)
        orch._dispatch_merge_conflict_rebase_fix_run = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=True
        )

        assert await orch._reconcile_auto_recoverable_merge_waits() == 1  # noqa: SLF001
        await orch.drain_dispatch_tasks()

        orch._dispatch_merge_conflict_rebase_fix_run.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._dispatch_merge_conflict_rebase_fix_run.await_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["pr_number"] == 42
        assert kwargs["pr_url"] == "https://github.com/org/repo/pull/42"
        assert kwargs["view"] == conflict_view
        assert kwargs["merge_run_id"] == "merge-run"
        comment = orch.linear.post_comment.await_args.args[1]
        assert "merge-conflict rebase fix-run" in comment
        assert "no `$approve` needed" in comment
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [(run.id, run.status) for run in history if run.id == "review-run"] == [
            ("review-run", "completed")
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_merged_issue_linear_drift_moves_back_to_done(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    now = datetime(2026, 5, 19, 20, tzinfo=UTC)
    merged_at = (now - timedelta(minutes=6)).isoformat()
    try:
        await _seed_merged_pr(conn, merged_at=merged_at)
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock(return_value=None)
        linear.post_comment = AsyncMock(return_value="cmt-1")
        cfg = Config(repos=[_binding()], db_path=tmp_path / "s.sqlite")
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
            clock=lambda: now,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        assert await orch._reconcile_merged_issues_linear_state() == 1  # noqa: SLF001

        linear.lookup_issue.assert_awaited_once_with("iss-1")
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        linear.post_comment.assert_awaited_once()
        assert linear.post_comment.await_args.args[0] == "iss-1"
        body = linear.post_comment.await_args.args[1]
        assert (
            f"♻️ Linear status drifted back to In Progress after merge — "
            f"re-moving to Done. PR #42 was merged at {merged_at}."
        ) in body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_merged_issue_linear_drift_dedupes_comment(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    now = datetime(2026, 5, 19, 20, tzinfo=UTC)
    try:
        await _seed_merged_pr(
            conn,
            merged_at=(now - timedelta(minutes=6)).isoformat(),
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock(return_value=None)
        linear.post_comment = AsyncMock(return_value="cmt-1")
        cfg = Config(repos=[_binding()], db_path=tmp_path / "s.sqlite")
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
            clock=lambda: now,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        assert await orch._reconcile_merged_issues_linear_state() == 1  # noqa: SLF001
        assert await orch._reconcile_merged_issues_linear_state() == 1  # noqa: SLF001

        assert linear.move_issue.await_count == 2
        linear.post_comment.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_merged_issue_linear_done_state_noops(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    now = datetime(2026, 5, 19, 20, tzinfo=UTC)
    try:
        await _seed_merged_pr(
            conn,
            merged_at=(now - timedelta(minutes=6)).isoformat(),
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_done_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock()
        cfg = Config(repos=[_binding()], db_path=tmp_path / "s.sqlite")
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
            clock=lambda: now,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        assert await orch._reconcile_merged_issues_linear_state() == 0  # noqa: SLF001

        linear.lookup_issue.assert_awaited_once_with("iss-1")
        linear.move_issue.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_merged_issue_linear_state_ignores_old_merges(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    now = datetime(2026, 5, 19, 20, tzinfo=UTC)
    try:
        await _seed_merged_pr(
            conn,
            merged_at=(now - timedelta(hours=25)).isoformat(),
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock()
        cfg = Config(repos=[_binding()], db_path=tmp_path / "s.sqlite")
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
            clock=lambda: now,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        assert await orch._reconcile_merged_issues_linear_state() == 0  # noqa: SLF001

        linear.lookup_issue.assert_not_awaited()
        linear.move_issue.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_tick_runs_merged_issue_reconciler_every_fifth_tick(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()], db_path=tmp_path / "s.sqlite")
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._restore_operator_waits = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._poll_merge_candidates = AsyncMock(return_value=[])  # type: ignore[method-assign]  # noqa: SLF001
        orch._poll_review_runs = AsyncMock(return_value=[])  # type: ignore[method-assign]  # noqa: SLF001
        orch._resurrect_review_runs = AsyncMock(return_value=[])  # type: ignore[method-assign]  # noqa: SLF001
        orch._scan_binding = AsyncMock(return_value=[])  # type: ignore[method-assign]  # noqa: SLF001
        orch._poll_slash_commands = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._reconcile_merged_issues_linear_state = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=0
        )

        for _ in range(4):
            await orch._tick()  # noqa: SLF001
        orch._reconcile_merged_issues_linear_state.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001

        await orch._tick()  # noqa: SLF001

        orch._reconcile_merged_issues_linear_state.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_merge_wait_clean_dispatches_fresh_merge(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_merge_operator_wait(conn)
        orch = _make_merge_wait_orchestrator(
            conn,
            gh_view={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "main",
                "mergedAt": None,
            },
        )
        scheduled = asyncio.create_task(asyncio.sleep(0))
        orch._schedule_merge = MagicMock(return_value=scheduled)  # type: ignore[method-assign]  # noqa: SLF001

        assert await orch._reconcile_auto_recoverable_merge_waits() == 1  # noqa: SLF001
        await scheduled

        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._review_verdict_for_pr.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._schedule_merge.call_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["pr_number"] == 42
        assert kwargs["pr_url"] == "https://github.com/org/repo/pull/42"
        assert callable(kwargs["on_started"])
        comment = orch.linear.post_comment.await_args.args[1]
        assert "clean merge retry" in comment
        assert "no `$approve` needed" in comment
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_merge_wait_clean_no_signal_does_not_schedule_merge(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_merge_operator_wait(conn)
        orch = _make_merge_wait_orchestrator(
            conn,
            gh_view={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "main",
                "mergedAt": None,
            },
            review_verdict=Verdict(kind=VerdictKind.PENDING, rule="no_signal"),
        )
        scheduled = asyncio.create_task(asyncio.sleep(0))
        orch._schedule_merge = MagicMock(return_value=scheduled)  # type: ignore[method-assign]  # noqa: SLF001

        dispatched = await orch._reconcile_auto_recoverable_merge_waits()  # noqa: SLF001
        await scheduled

        assert dispatched == 0
        orch._review_verdict_for_pr.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
        orch.linear.post_comment.assert_not_awaited()
        assert await db.operator_waits.get(conn, "iss-1") is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_merge_wait_clean_retires_live_review_monitor(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_merge_operator_wait(conn)
        await db.runs.create(
            conn,
            id="review-run",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:30+00:00",
        )
        orch = _make_merge_wait_orchestrator(
            conn,
            gh_view={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "main",
                "mergedAt": None,
            },
        )
        scheduled = asyncio.create_task(asyncio.sleep(0))
        orch._schedule_merge = MagicMock(return_value=scheduled)  # type: ignore[method-assign]  # noqa: SLF001

        assert await orch._reconcile_auto_recoverable_merge_waits() == 1  # noqa: SLF001
        await scheduled

        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [(run.id, run.status) for run in history if run.id == "review-run"] == [
            ("review-run", "completed")
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_merge_wait_blocked_leaves_wait_untouched(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_merge_operator_wait(conn)
        orch = _make_merge_wait_orchestrator(
            conn,
            gh_view={
                "headRefOid": "abc123",
                "mergeable": "BLOCKED",
                "mergeStateStatus": "BLOCKED",
                "baseRefName": "main",
                "mergedAt": None,
            },
        )
        orch._dispatch_merge_conflict_rebase_fix_run = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        assert await orch._reconcile_auto_recoverable_merge_waits() == 0  # noqa: SLF001

        orch._dispatch_merge_conflict_rebase_fix_run.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
        orch.linear.post_comment.assert_not_awaited()
        assert await db.operator_waits.get(conn, "iss-1") is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_merge_wait_second_tick_blocked_by_live_run(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_merge_operator_wait(conn)
        orch = _make_merge_wait_orchestrator(
            conn,
            gh_view={
                "headRefOid": "abc123",
                "mergeable": "CONFLICTING",
                "mergeStateStatus": "DIRTY",
                "baseRefName": "main",
                "mergedAt": None,
            },
        )
        started = asyncio.Event()
        release = asyncio.Event()

        async def dispatch_once(**_kwargs: object) -> bool:
            await db.runs.create(
                conn,
                id="review-fix-running",
                issue_id="iss-1",
                stage="review_fix",
                status="running",
                pid=None,
                started_at="2026-05-10T00:04:00+00:00",
            )
            started.set()
            await release.wait()
            await db.runs.update_status(
                conn,
                "review-fix-running",
                "completed",
                ended_at="2026-05-10T00:05:00+00:00",
            )
            return True

        orch._dispatch_merge_conflict_rebase_fix_run = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=dispatch_once
        )

        assert await orch._reconcile_auto_recoverable_merge_waits() == 1  # noqa: SLF001
        await asyncio.wait_for(started.wait(), timeout=1)
        assert await orch._reconcile_auto_recoverable_merge_waits() == 0  # noqa: SLF001
        release.set()
        await orch.drain_dispatch_tasks()

        orch._dispatch_merge_conflict_rebase_fix_run.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        assert orch.linear.post_comment.await_count == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_run_reconciles_merge_waits_once_after_startup_restore(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        orch = _make_merge_wait_orchestrator(
            conn,
            gh_view={
                "mergeable": "BLOCKED",
                "mergeStateStatus": "BLOCKED",
            },
        )
        orch.warmup = AsyncMock()  # type: ignore[method-assign]
        orch._restore_operator_waits = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._reconcile_auto_recoverable_merge_waits = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=0
        )

        async def wait_for_shutdown(shutdown: asyncio.Event) -> None:
            await shutdown.wait()

        orch._reconciler.run = AsyncMock(side_effect=wait_for_shutdown)  # type: ignore[method-assign]  # noqa: SLF001

        async def stop_after_first_tick() -> list[asyncio.Task[None]]:
            await orch.shutdown()
            return []

        orch._tick = AsyncMock(side_effect=stop_after_first_tick)  # type: ignore[method-assign]  # noqa: SLF001

        await asyncio.wait_for(orch.run(), timeout=1)

        orch._restore_operator_waits.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._reconcile_auto_recoverable_merge_waits.assert_any_await(  # type: ignore[attr-defined]  # noqa: SLF001
            reason="startup"
        )
    finally:
        await conn.close()


def _write_fake_gh(
    tmp_path: Path, *, auto_merge_disabled: bool = False
) -> tuple[Path, Path]:
    calls = tmp_path / "gh-calls.jsonl"
    merged_flag = tmp_path / "merged.flag"
    shim = tmp_path / "gh"
    pr_view = {
        "number": 42,
        "title": "Add auth",
        "state": "OPEN",
        "url": "https://github.com/org/repo/pull/42",
        "headRefName": "symphony/eng-1",
        "headRefOid": "abc123",
        "mergeable": "MERGEABLE",
        "isDraft": False,
        "mergedAt": None,
    }
    checks = [
        {
            "name": "test",
            "state": "SUCCESS",
            "bucket": "pass",
            "link": None,
        }
    ]
    reviews = [
        {
            "user": {"login": "reviewer"},
            "state": "APPROVED",
            "commit_id": "abc123",
            "submitted_at": "2026-05-10T00:03:00Z",
            "body": "ship it",
        }
    ]
    commit = {"commit": {"committer": {"date": "2026-05-10T00:02:00Z"}}}
    shim.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "from pathlib import Path\n"
        f"calls = {str(calls)!r}\n"
        f"merged_flag = Path({str(merged_flag)!r})\n"
        f"pr_view = json.loads({json.dumps(json.dumps(pr_view))})\n"
        f"checks = json.loads({json.dumps(json.dumps(checks))})\n"
        f"reviews = json.loads({json.dumps(json.dumps(reviews))})\n"
        f"commit = json.loads({json.dumps(json.dumps(commit))})\n"
        f"auto_merge_disabled = {auto_merge_disabled!r}\n"
        "argv = sys.argv[1:]\n"
        "with open(calls, 'a') as f:\n"
        "    f.write(json.dumps({'argv': argv}) + '\\n')\n"
        "joined = ' '.join(argv)\n"
        "if argv[:2] == ['pr', 'view']:\n"
        "    if merged_flag.exists():\n"
        "        pr_view['state'] = 'MERGED'\n"
        "        pr_view['mergedAt'] = '2026-05-10T00:04:00Z'\n"
        "    sys.stdout.write(json.dumps(pr_view)); sys.exit(0)\n"
        "if argv[:2] == ['pr', 'checks']:\n"
        "    sys.stdout.write(json.dumps(checks)); sys.exit(0)\n"
        "if 'repos/org/repo/pulls/42/comments' in joined:\n"
        "    sys.stdout.write('[]'); sys.exit(0)\n"
        "if 'repos/org/repo/pulls/42/reviews' in joined:\n"
        "    sys.stdout.write(json.dumps(reviews)); sys.exit(0)\n"
        "if 'repos/org/repo/issues/42/comments' in joined:\n"
        "    sys.stdout.write('[]'); sys.exit(0)\n"
        "if 'repos/org/repo/issues/42/reactions' in joined:\n"
        "    sys.stdout.write('[]'); sys.exit(0)\n"
        "if 'repos/org/repo/commits/abc123' in joined:\n"
        "    sys.stdout.write(json.dumps(commit)); sys.exit(0)\n"
        "if argv[:3] == ['pr', 'merge', '42']:\n"
        "    if auto_merge_disabled and '--auto' in argv:\n"
        "        sys.stderr.write('GraphQL: enablePullRequestAutoMerge must be true')\n"
        "        sys.exit(1)\n"
        "    merged_flag.write_text('1')\n"
        "    sys.exit(0)\n"
        "sys.stderr.write('unexpected gh call: ' + joined)\n"
        "sys.exit(1)\n"
    )
    shim.chmod(0o755)
    return shim, calls


def _read_calls(calls: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in calls.read_text().splitlines()]


@pytest.mark.asyncio
async def test_merge_candidate_uses_recorded_binding_key(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        target = _binding(
            agent="codex",
            issue_label="backend",
            branch_prefix="backend",
        )
        await _seed_review_candidate(conn, binding_key=_binding_storage_key(target))
        cfg = Config(
            repos=[
                _binding(
                    agent="claude",
                    issue_label="frontend",
                    branch_prefix="frontend",
                ),
                target,
            ],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        candidate = (await db.issue_prs.list_merge_candidates(conn))[0]

        assert orch._binding_for_pr(candidate) == target  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_candidate_falls_back_when_recorded_binding_key_is_stale(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn, binding_key="old-shape:backend")
        target = _binding(
            agent="codex",
            issue_label="backend",
            branch_prefix="backend",
        )
        cfg = Config(
            repos=[target],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        candidate = (await db.issue_prs.list_merge_candidates(conn))[0]

        assert orch._binding_for_pr(candidate) == target  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_binding_key_fallback_uses_recorded_label(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(
            conn,
            binding_key='["ENG","org/repo","backend","legacy"]',
        )
        target = _binding(
            agent="codex",
            issue_label="backend",
            branch_prefix="backend",
        )
        cfg = Config(
            repos=[
                _binding(
                    agent="claude",
                    issue_label="frontend",
                    branch_prefix="frontend",
                ),
                target,
            ],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        candidate = (await db.issue_prs.list_merge_candidates(conn))[0]

        assert orch._binding_for_pr(candidate) == target  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_ambiguous_binding_fallback_returns_none(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn, binding_key="old-shape:backend")
        cfg = Config(
            repos=[
                _binding(
                    agent="claude",
                    issue_label="frontend",
                    branch_prefix="frontend",
                ),
                _binding(
                    agent="codex",
                    issue_label="backend",
                    branch_prefix="backend",
                ),
            ],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        candidate = (await db.issue_prs.list_merge_candidates(conn))[0]

        assert orch._binding_for_pr(candidate) is None  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_green_review_and_ci_auto_merges_with_fake_gh(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        shim, calls_log = _write_fake_gh(tmp_path)
        result_line = json.dumps({"type": "result", "total_cost_usd": 0.25})
        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=123),
                RunnerEvent(kind="stdout", line=result_line),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock(return_value=None)
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock(return_value=None)
        linear.post_comment = AsyncMock(return_value="cmt-1")
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="codex")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=GitHub(gh_path=str(shim)),
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "merge"
        assert runner.captured_spec.command[0] == "codex"
        push_fn.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [r.stage for r in history] == ["implement", "review", "merge"]
        assert history[-1].status == "done"
        assert await db.runs.cost_for_issue(conn, "iss-1") == pytest.approx(0.75)

        comment_body = linear.post_comment.await_args.args[1]
        assert "Merge" in comment_body
        assert "Done" in comment_body
        assert "https://github.com/org/repo/pull/42" in comment_body
        assert "$0.7500" in comment_body

        calls = _read_calls(calls_log)
        merge_call = next(c for c in calls if c["argv"][:3] == ["pr", "merge", "42"])
        assert "--squash" in merge_call["argv"]
        assert "--auto" in merge_call["argv"]
        assert "--repo" in merge_call["argv"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_merge_disabled_degrades_to_sync_merge_with_fake_gh(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        shim, calls_log = _write_fake_gh(tmp_path, auto_merge_disabled=True)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock(return_value=None)
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock(return_value=None)
        linear.post_comment = AsyncMock(return_value="cmt-1")
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="codex")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=GitHub(gh_path=str(shim)),
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        push_fn.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        assert await db.operator_waits.get(conn, "iss-1") is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"

        calls = _read_calls(calls_log)
        merge_calls = [c for c in calls if c["argv"][:3] == ["pr", "merge", "42"]]
        assert len(merge_calls) == 2
        first = merge_calls[0]["argv"]
        second = merge_calls[1]["argv"]
        assert "--auto" in first
        assert "--auto" not in second
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_merge_false_parks_approved_mergeable_pr_for_manual_merge(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    review_task: asyncio.Task[bool] | None = None
    try:
        await _seed_review_candidate(conn)
        await db.runs.update_status(conn, "review", "running")
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude", auto_merge=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        review_task = asyncio.create_task(asyncio.Event().wait())
        orch._review_poll_tasks.add(review_task)  # noqa: SLF001
        orch._review_poll_run_ids.add("review")  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = "review"  # noqa: SLF001
        orch._review_poll_run_tasks["review"] = review_task  # noqa: SLF001

        await _poll_and_wait(orch)
        await asyncio.gather(review_task, return_exceptions=True)

        gh.pr_merge.assert_not_awaited()
        push_fn.assert_not_awaited()
        workspace.acquire.assert_not_awaited()
        assert runner.captured_spec is None
        assert review_task.cancelled()
        assert "review" not in orch._review_poll_run_ids  # noqa: SLF001
        assert "iss-1" not in orch._review_poll_issue_ids  # noqa: SLF001
        assert "review" not in orch._review_poll_run_tasks  # noqa: SLF001
        assert review_task not in orch._review_poll_tasks  # noqa: SLF001
        history = await db.runs.history_for_issue(conn, "iss-1")
        review_run = next(r for r in history if r.id == "review")
        assert review_run.status == "completed"
        assert review_run.ended_at is not None
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        linear.post_comment.assert_awaited_once_with(
            "iss-1",
            "✅ review passed, ready for manual merge: "
            "https://github.com/org/repo/pull/42",
        )
        pr = await db.issue_prs.get(
            conn, issue_id="iss-1", github_repo="org/repo"
        )
        assert pr is not None
        assert pr.parked_at is not None
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert len(candidates) == 1
        assert candidates[0].parked_at == pr.parked_at
    finally:
        if review_task is not None and not review_task.done():
            review_task.cancel()
            await asyncio.gather(review_task, return_exceptions=True)
        await conn.close()


@pytest.mark.parametrize(("global_cap", "binding_cap"), [(0, 2), (2, 0)])
@pytest.mark.asyncio
async def test_auto_merge_false_parks_without_dispatch_capacity(
    tmp_path: Path,
    global_cap: int,
    binding_cap: int,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_merge = AsyncMock()
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        binding = _binding(agent="claude", auto_merge=False).model_copy(
            update={"max_concurrent": binding_cap}
        )

        cfg = Config(
            repos=[binding],
            global_max_concurrent=global_cap,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._review_verdict_for_pr = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="test_approved")
        )

        await _poll_and_wait(orch)

        gh.pr_merge.assert_not_awaited()
        assert runner.captured_spec is None
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        linear.post_comment.assert_awaited_once_with(
            "iss-1",
            "✅ review passed, ready for manual merge: "
            "https://github.com/org/repo/pull/42",
        )
        pr = await db.issue_prs.get(
            conn, issue_id="iss-1", github_repo="org/repo"
        )
        assert pr is not None
        assert pr.parked_at is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_merge_false_manual_merge_parking_is_idempotent(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude", auto_merge=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner([RunnerEvent(kind="exit", returncode=0)]),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)
        first_pr = await db.issue_prs.get(
            conn, issue_id="iss-1", github_repo="org/repo"
        )
        assert first_pr is not None
        first_parked_at = first_pr.parked_at

        await _poll_and_wait(orch)

        gh.pr_merge.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        linear.post_comment.assert_awaited_once()
        second_pr = await db.issue_prs.get(
            conn, issue_id="iss-1", github_repo="org/repo"
        )
        assert second_pr is not None
        assert second_pr.parked_at == first_parked_at
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_merge_false_parked_pr_close_moves_issue_done_once(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        parked_issue = _issue()
        parked_issue.state_id = "state-na"
        parked_issue.state_name = "Needs Approval"
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            side_effect=[_issue(), parked_issue, _done_issue()]
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergeStateStatus": "CLEAN",
                    "state": "OPEN",
                    "mergedAt": None,
                },
                {
                    "headRefOid": "abc123",
                    "mergeable": "UNKNOWN",
                    "mergeStateStatus": "UNKNOWN",
                    "state": "CLOSED",
                    "mergedAt": None,
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude", auto_merge=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner([RunnerEvent(kind="exit", returncode=0)]),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._review_verdict_for_pr = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="test_approved")
        )

        await _poll_and_wait(orch)
        pr = await db.issue_prs.get(
            conn, issue_id="iss-1", github_repo="org/repo"
        )
        assert pr is not None
        assert pr.parked_at is not None

        await _poll_and_wait(orch)
        await _poll_and_wait(orch)

        gh.pr_merge.assert_not_awaited()
        assert gh.pr_view.await_count == 2
        linear.move_issue.assert_has_awaits(
            [call("iss-1", "state-na"), call("iss-1", "state-done")]
        )
        assert linear.post_comment.await_count == 2
        comments = [args.args[1] for args in linear.post_comment.await_args_list]
        assert "ready for manual merge" in comments[0]
        assert comments[1] == "🛑 PR closed without merge — marking done"
        merge_run = await db.runs.latest_for_issue_stage(
            conn,
            issue_id="iss-1",
            stage="merge",
            started_at_gte="2026-05-10T00:01:00+00:00",
        )
        assert merge_run is None
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is None
        assert (
            await db.issue_prs.get(conn, issue_id="iss-1", github_repo="org/repo")
            is None
        )
        assert await db.issue_prs.list_merge_candidates(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_merge_false_parked_pr_close_retries_comment_after_done_move(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        parked = await db.issue_prs.mark_parked_for_manual_merge(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            parked_at="2026-05-10T00:05:00+00:00",
        )
        assert parked
        parked_issue = _issue()
        parked_issue.state_id = "state-na"
        parked_issue.state_name = "Needs Approval"
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(side_effect=[parked_issue, _done_issue()])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(
            side_effect=[LinearError("comments down"), "cmt-1"]
        )
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "UNKNOWN",
                "mergeStateStatus": "UNKNOWN",
                "state": "CLOSED",
                "mergedAt": None,
            }
        )
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude", auto_merge=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        linear.post_comment.assert_awaited_once_with(
            "iss-1", "🛑 PR closed without merge — marking done"
        )
        assert (
            await db.issue_prs.get(conn, issue_id="iss-1", github_repo="org/repo")
        ) is not None

        await _poll_and_wait(orch)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        assert linear.post_comment.await_count == 2
        assert linear.post_comment.await_args.args[1] == (
            "🛑 PR closed without merge — marking done"
        )
        assert (
            await db.issue_prs.get(conn, issue_id="iss-1", github_repo="org/repo")
        ) is None
        assert gh.pr_view.await_count == 2
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_needs_input_to_review_revives_parked_manual_merge_pr(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _split_review_binding(auto_merge=False)
        await _seed_review_candidate(conn, binding_key=_binding_storage_key(binding))
        parked = await db.issue_prs.mark_parked_for_manual_merge(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            parked_at="2026-05-10T00:02:00+00:00",
        )
        assert parked

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        gh = MagicMock()
        view: dict[str, object] = {
            "headRefOid": "abc123",
            "mergeable": "CONFLICTING",
            "mergeStateStatus": "DIRTY",
            "baseRefName": "main",
            "state": "OPEN",
            "mergedAt": None,
        }
        gh.pr_view = AsyncMock(return_value=view)

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._reconciler = MagicMock()  # noqa: SLF001
        orch._reconciler.reconcile_linear_issue_event = AsyncMock(return_value=2)  # type: ignore[attr-defined]
        dispatch = AsyncMock(side_effect=_dispatch_rebase_fix_with_started_callback)
        orch._dispatch_merge_conflict_rebase_fix_run = dispatch  # noqa: SLF001

        payload = {
            "type": "Issue",
            "action": "update",
            "webhookTimestamp": int(datetime(2026, 5, 10, tzinfo=UTC).timestamp() * 1000),
            "updatedFrom": {"stateId": "state-input"},
            "data": {"id": "iss-1", "state": {"id": "state-review"}},
        }
        result = await orch.handle_linear_webhook(payload)
        await orch.drain_reconcile_event_tasks()
        await orch.drain_dispatch_tasks()

        assert result.handled is True
        dispatch.assert_awaited_once()
        await_args = dispatch.await_args
        assert await_args is not None
        kwargs = await_args.kwargs
        assert kwargs["binding"] == binding
        assert kwargs["issue"].state_name == "In Review"
        assert kwargs["pr_number"] == 42
        assert kwargs["pr_url"] == "https://github.com/org/repo/pull/42"
        assert kwargs["view"] == view
        assert callable(kwargs["on_started"])
        pr = await db.issue_prs.get(conn, issue_id="iss-1", github_repo="org/repo")
        assert pr is not None
        assert pr.parked_at is None

        await orch.handle_linear_webhook(payload)
        await orch.drain_reconcile_event_tasks()
        await orch.drain_dispatch_tasks()

        dispatch.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.parametrize(
    ("view", "expected_status", "expected_move"),
    [
        (
            {
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "main",
                "state": "MERGED",
                "mergedAt": "2026-05-10T00:03:00+00:00",
            },
            "done",
            ("iss-1", "state-done"),
        ),
        (
            {
                "headRefOid": "abc123",
                "mergeable": "UNKNOWN",
                "mergeStateStatus": "UNKNOWN",
                "baseRefName": "main",
                "state": "CLOSED",
                "mergedAt": None,
            },
            "needs_approval",
            ("iss-1", "state-input"),
        ),
    ],
)
@pytest.mark.asyncio
async def test_webhook_needs_input_to_review_finalizes_inactive_parked_pr(
    tmp_path: Path,
    view: dict[str, object],
    expected_status: str,
    expected_move: tuple[str, str],
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _split_review_binding(auto_merge=False)
        await _seed_review_candidate(conn, binding_key=_binding_storage_key(binding))
        parked_at = "2026-05-10T00:02:00+00:00"
        parked = await db.issue_prs.mark_parked_for_manual_merge(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            parked_at=parked_at,
        )
        assert parked

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        gh = MagicMock()
        gh.pr_view = AsyncMock(return_value=view)
        workspace = MagicMock()
        workspace.cleanup = AsyncMock()

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._reconciler = MagicMock()  # noqa: SLF001
        orch._reconciler.reconcile_linear_issue_event = AsyncMock(return_value=2)  # type: ignore[attr-defined]
        dispatch = AsyncMock(return_value=True)
        orch._dispatch_merge_conflict_rebase_fix_run = dispatch  # noqa: SLF001

        payload = {
            "type": "Issue",
            "action": "update",
            "webhookTimestamp": int(datetime(2026, 5, 10, tzinfo=UTC).timestamp() * 1000),
            "updatedFrom": {"stateId": "state-input"},
            "data": {"id": "iss-1", "state": {"id": "state-review"}},
        }
        await orch.handle_linear_webhook(payload)
        await orch.drain_reconcile_event_tasks()
        await orch.drain_dispatch_tasks()

        dispatch.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with(*expected_move)
        merge_run = await db.runs.latest_for_issue_stage(
            conn,
            issue_id="iss-1",
            stage="merge",
            started_at_gte="2026-05-10T00:01:00+00:00",
        )
        assert merge_run is not None
        assert merge_run.status == expected_status
        pr = await db.issue_prs.get(conn, issue_id="iss-1", github_repo="org/repo")
        assert pr is not None
        if expected_status == "done":
            assert pr.merged_at is not None
        else:
            wait = await db.operator_waits.get(conn, "iss-1")
            assert wait is not None
            assert wait.kind == db.operator_waits.KIND_MERGE
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_poll_needs_input_to_review_revives_parked_manual_merge_pr(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _split_review_binding(auto_merge=False)
        await _seed_review_candidate(conn, binding_key=_binding_storage_key(binding))
        parked = await db.issue_prs.mark_parked_for_manual_merge(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            parked_at="2026-05-10T00:02:00+00:00",
        )
        assert parked

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        gh = MagicMock()
        view = {
            "headRefOid": "abc123",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "baseRefName": "main",
            "state": "OPEN",
            "mergedAt": None,
        }
        gh.pr_view = AsyncMock(return_value=view)

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        dispatch = AsyncMock(side_effect=_dispatch_rebase_fix_with_started_callback)
        orch._dispatch_merge_conflict_rebase_fix_run = dispatch  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        dispatch.assert_awaited_once()
        await_args = dispatch.await_args
        assert await_args is not None
        kwargs = await_args.kwargs
        assert callable(kwargs["on_started"])
        pr = await db.issue_prs.get(conn, issue_id="iss-1", github_repo="org/repo")
        assert pr is not None
        assert pr.parked_at is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_parked_manual_merge_marker_survives_queued_revival(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    task: asyncio.Task[None] | None = None
    try:
        binding = _split_review_binding(auto_merge=False)
        await _seed_review_candidate(conn, binding_key=_binding_storage_key(binding))
        parked = await db.issue_prs.mark_parked_for_manual_merge(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            parked_at="2026-05-10T00:02:00+00:00",
        )
        assert parked

        workspace_path = tmp_path / "ws" / "org_repo" / "ENG-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()
        view: dict[str, object] = {
            "headRefOid": "abc123",
            "mergeable": "CONFLICTING",
            "mergeStateStatus": "DIRTY",
            "baseRefName": "main",
            "state": "OPEN",
            "mergedAt": None,
        }
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                **view,
                "headRefOid": "fixedsha",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
            }
        )
        cfg = Config(
            repos=[binding],
            global_max_concurrent=1,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=_FakeRunner([RunnerEvent(kind="exit", returncode=0)]),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        await orch._global_dispatch_sem.acquire()  # noqa: SLF001
        try:
            candidate = await db.issue_prs.get(
                conn,
                issue_id="iss-1",
                github_repo="org/repo",
            )
            assert candidate is not None
            task = await orch._schedule_parked_manual_merge_revival_if_requested(  # noqa: SLF001
                binding=binding,
                issue=_issue_in_review(),
                candidate=candidate,
                view=view,
            )
            assert task is not None
            for _ in range(10):
                await asyncio.sleep(0)
                if orch._scheduled_issue_refcounts.get("iss-1") == 1:  # noqa: SLF001
                    break

            assert orch._scheduled_issue_refcounts.get("iss-1") == 1  # noqa: SLF001
            assert task.done() is False
            queued_pr = await db.issue_prs.get(
                conn,
                issue_id="iss-1",
                github_repo="org/repo",
            )
            assert queued_pr is not None
            assert queued_pr.parked_at == "2026-05-10T00:02:00+00:00"
            assert (
                await db.runs.latest_for_issue_stage(
                    conn,
                    issue_id="iss-1",
                    stage="review_fix",
                )
                is None
            )
        finally:
            orch._global_dispatch_sem.release()  # noqa: SLF001

        assert task is not None
        await asyncio.wait_for(task, timeout=1)
        pr = await db.issue_prs.get(conn, issue_id="iss-1", github_repo="org/repo")
        assert pr is not None
        assert pr.parked_at is None
        review_fix = await db.runs.latest_for_issue_stage(
            conn,
            issue_id="iss-1",
            stage="review_fix",
        )
        assert review_fix is not None
        assert review_fix.status == "completed"
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await conn.close()


@pytest.mark.asyncio
async def test_auto_merge_false_parks_before_human_approval_wait(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        issue = _issue()
        issue.labels = ["feature", "needs-human-approval"]
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=issue)
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude", auto_merge=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner([RunnerEvent(kind="exit", returncode=0)]),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._review_verdict_for_pr = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="test_approved")
        )

        await _poll_and_wait(orch)

        gh.pr_merge.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        linear.post_comment.assert_awaited_once_with(
            "iss-1",
            "✅ review passed, ready for manual merge: "
            "https://github.com/org/repo/pull/42",
        )
        assert await db.operator_waits.get(conn, "iss-1") is None
        pr = await db.issue_prs.get(
            conn, issue_id="iss-1", github_repo="org/repo"
        )
        assert pr is not None
        assert pr.parked_at is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_merge_false_retries_when_manual_merge_move_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock(side_effect=[LinearError("move down"), None])
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude", auto_merge=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner([RunnerEvent(kind="exit", returncode=0)]),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._review_verdict_for_pr = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="test_approved")
        )

        await _poll_and_wait(orch)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        linear.post_comment.assert_not_awaited()
        pr = await db.issue_prs.get(
            conn, issue_id="iss-1", github_repo="org/repo"
        )
        assert pr is not None
        assert pr.parked_at is None
        assert (await db.issue_prs.list_merge_candidates(conn))[0].pr_number == 42

        await _poll_and_wait(orch)

        assert linear.move_issue.await_count == 2
        linear.post_comment.assert_awaited_once()
        pr = await db.issue_prs.get(
            conn, issue_id="iss-1", github_repo="org/repo"
        )
        assert pr is not None
        assert pr.parked_at is not None
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert len(candidates) == 1
        assert candidates[0].parked_at == pr.parked_at
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_candidate_skips_when_issue_left_active_state(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        paused = _issue()
        paused.state_name = "Blocked"
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=paused)
        gh = MagicMock()
        gh.pr_view = AsyncMock()
        workspace = MagicMock()
        workspace.acquire = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001

        linear.lookup_issue.assert_awaited_once_with("iss-1")
        gh.pr_view.assert_not_awaited()
        workspace.acquire.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_candidate_skips_when_binding_label_no_longer_matches(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(agent="claude", issue_label="backend")
        await _seed_review_candidate(conn, binding_key=_binding_storage_key(binding))
        linear = MagicMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        gh = MagicMock()
        gh.pr_view = AsyncMock(return_value={})
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org_srepo" / "eng-1")

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001

        linear.lookup_issue.assert_awaited_once_with("iss-1")
        gh.pr_view.assert_not_awaited()
        workspace.acquire.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_queued_merge_revalidates_issue_before_execution(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        paused = _issue()
        paused.state_name = "Blocked"
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(side_effect=[_issue(), paused])
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        workspace = MagicMock()
        workspace.acquire = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        await _poll_and_wait(orch)

        assert [call.args[0] for call in linear.lookup_issue.await_args_list] == [
            "iss-1",
            "iss-1",
        ]
        workspace.acquire.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_verdict_treats_issue_comment_fetch_failure_as_empty(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(agent="claude")
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(side_effect=GitHubError("boom"))
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")

        orch = Orchestrator(
            cfg,
            MagicMock(),
            conn,
            runner=MagicMock(),
            gh=gh,
        )
        verdict = await orch._review_verdict_for_pr(  # noqa: SLF001
            binding=binding,
            pr_number=45,
            view={"headRefOid": "abc123", "mergeable": "MERGEABLE"},
        )

        assert verdict.kind == "approved"
        assert verdict.rule == "human_approved"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_approved_merge_runs_in_background(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.runs.update_status(conn, "review", "running")
        await db.runs.create(
            conn,
            id="old-submitted-merge",
            issue_id="iss-1",
            stage="merge",
            status="completed",
            pid=None,
            started_at="2026-05-09T00:00:00+00:00",
        )
        runner = _BlockingRunner()
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:04:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await asyncio.wait_for(
            orch._poll_merge_candidates(),  # noqa: SLF001
            timeout=0.2,
        )

        assert len(tasks) == 1
        await asyncio.wait_for(runner.started.wait(), timeout=1)
        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "merge"
        assert not tasks[0].done()
        assert await db.runs.has_active(conn, "iss-1") is True
        linear.move_issue.assert_not_awaited()

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001

        runner.release.set()
        await asyncio.gather(*tasks)

        push_fn.assert_awaited_once()
        gh.pr_merge.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_tick_schedules_merge_before_new_implementation_when_capacity_is_full(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        binding = _binding().model_copy(update={"max_concurrent": 1})
        cfg = Config(
            repos=[binding],
            global_max_concurrent=1,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.issues_in_state = AsyncMock(return_value=[_ready_issue()])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.comments_since = AsyncMock(return_value=[])

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()

        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:05:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "ship it",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="main")
        gh.pr_create = AsyncMock()
        gh.pr_comment = AsyncMock()

        runner = _BlockingRunner()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._tick()  # noqa: SLF001
        try:
            await asyncio.wait_for(runner.started.wait(), timeout=1)
            assert runner.captured_spec is not None
            assert runner.captured_spec.stage == "merge"

            cur = await conn.execute("SELECT 1 FROM runs WHERE issue_id = 'iss-2'")
            assert await cur.fetchone() is None
        finally:
            runner.release.set()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_agent_new_commit_requires_fresh_review_before_merge(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.review_state.begin_review(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label=None,
        )

        workspace_path = tmp_path / "ws" / "org" / "eng-1"
        workspace_path.mkdir(parents=True)
        await _git(workspace_path, "init")
        await _git(workspace_path, "config", "user.email", "test@example.com")
        await _git(workspace_path, "config", "user.name", "Test User")
        (workspace_path / "README.md").write_text("base\n", encoding="utf-8")
        await _git(workspace_path, "add", "README.md")
        await _git(workspace_path, "commit", "-m", "base")
        await _git(workspace_path, "checkout", "-b", "symphony/eng-1")
        approved_head_sha = await _git(workspace_path, "rev-parse", "HEAD")

        runner = _CommittingRunner()
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        pr_view_calls = 0

        async def pr_view(
            _pr_number: int, *, repo: str, include_status_checks: bool = False
        ) -> dict[str, object]:
            nonlocal pr_view_calls
            assert repo == "org/repo"
            assert include_status_checks in {False, True}
            pr_view_calls += 1
            head_sha = approved_head_sha
            if pr_view_calls > 1:
                head_sha = await _git(workspace_path, "rev-parse", "HEAD")
            return {
                "headRefOid": head_sha,
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "mergedAt": None,
            }

        gh.pr_view = AsyncMock(side_effect=pr_view)
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": approved_head_sha,
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_comment = AsyncMock()
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        new_head_sha = await _git(workspace_path, "rev-parse", "HEAD")
        assert new_head_sha != approved_head_sha
        push_fn.assert_awaited_once()
        gh.pr_merge.assert_not_awaited()
        gh.pr_comment.assert_awaited_once_with(42, "@codex review", repo="org/repo")
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any(
            f"merge-agent pushed unreviewed HEAD {new_head_sha}" in body
            for body in bodies
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_codex_no_issues_issue_comment_advances_merge(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:05:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(
            return_value=[
                {
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "P2 Badge - derive duplicate alerts from the final rows.",
                    "commit_id": "abc123",
                    "original_commit_id": "abc123",
                    "created_at": "2026-05-10T00:03:00Z",
                    "path": "app.py",
                    "line": 12,
                }
            ]
        )
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Codex Review: Didn't find any major issues. :+1:",
                    "created_at": "2026-05-10T00:04:00Z",
                }
            ]
        )
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "merge"
        push_fn.assert_awaited_once()
        gh.pr_merge.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_merge_submission_waits_until_pr_reports_merged(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:04:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        push_fn = AsyncMock()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_merge.assert_awaited_once()
        push_fn.assert_awaited_once()
        linear.move_issue.assert_not_awaited()
        workspace.cleanup.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "completed"
        assert (await db.issue_prs.list_merge_candidates(conn))[0].pr_number == 42

        await _poll_and_wait(orch)

        gh.pr_merge.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].status == "done"
        assert await db.issue_prs.list_merge_candidates(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_cleanup_failure_still_marks_done(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock(side_effect=RuntimeError("cleanup down"))
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:04:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"
        assert await db.issue_prs.list_merge_candidates(conn) == []
        comment_body = linear.post_comment.await_args.args[1]
        assert "Done" in comment_body
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_submitted_merge_regression_moves_to_needs_approval(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.runs.create(
            conn,
            id="merge",
            issue_id="iss-1",
            stage="merge",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:02:00+00:00",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="FAILURE", bucket="fail")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert await db.issue_prs.list_merge_candidates(conn) == []
        comment_body = linear.post_comment.await_args.args[1]
        assert "required CI failed: test" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_agent_enforces_issue_cost_cap(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        result_line = json.dumps(
            {
                "type": "result",
                "total_cost_usd": 0.75,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
        )
        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=123),
                RunnerEvent(kind="stdout", line=result_line),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=1.0,
            cost_warning_pct=75,
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        assert runner.kill_calls
        push_fn.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        workspace.cleanup.assert_not_awaited()
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Cost notice" in body for body in bodies)
        assert any("cost cap reached: $1.2500" in body for body in bodies)
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert history[-1].termination_kind == "cost_cap"
        assert "cost cap reached" in history[-1].termination_detail
        assert history[-1].cost_usd == pytest.approx(0.75)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_externally_merged_candidate_finishes_before_review_classification(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        workspace = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "MERGED",
                "mergedAt": "2026-05-10T00:04:00Z",
            }
        )
        gh.pr_checks = AsyncMock(return_value=PRChecks())
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="")
        gh.pr_merge = AsyncMock(return_value=None)

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_checks.assert_not_awaited()
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reviews.assert_not_awaited()
        gh.pr_reactions.assert_not_awaited()
        gh.commit_committed_at.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        linear.lookup_issue.assert_awaited_once_with("iss-1")
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"
        assert await db.issue_prs.list_merge_candidates(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_externally_merged_candidate_records_done_when_final_comment_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        workspace = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(side_effect=LinearError("comments down"))
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "MERGED",
                "mergedAt": "2026-05-10T00:04:00Z",
            }
        )
        gh.pr_checks = AsyncMock()
        gh.pr_review_comments = AsyncMock()
        gh.pr_reviews = AsyncMock()
        gh.pr_reactions = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock()
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_checks.assert_not_awaited()
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reviews.assert_not_awaited()
        gh.pr_reactions.assert_not_awaited()
        gh.commit_committed_at.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"
        assert await db.issue_prs.list_merge_candidates(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_externally_merged_candidate_closes_run_when_done_move_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        workspace = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock(side_effect=[LinearError("move down"), None])
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "MERGED",
                "mergedAt": "2026-05-10T00:04:00Z",
            }
        )
        gh.pr_checks = AsyncMock()
        gh.pr_review_comments = AsyncMock()
        gh.pr_reviews = AsyncMock()
        gh.pr_reactions = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock()
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_checks.assert_not_awaited()
        workspace.cleanup.assert_not_awaited()
        assert linear.move_issue.await_count == 2
        linear.move_issue.assert_any_await("iss-1", "state-done")
        linear.move_issue.assert_any_await("iss-1", "state-na")
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert await db.runs.has_active(conn, "iss-1") is False
        assert await db.issue_prs.list_merge_candidates(conn) == []
        comment_body = linear.post_comment.await_args.args[1]
        assert "merge finalization failed: move down" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_closed_candidate_moves_to_needs_approval_before_review_classification(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        workspace = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "CLOSED",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock()
        gh.pr_review_comments = AsyncMock()
        gh.pr_reviews = AsyncMock()
        gh.pr_reactions = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock()
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_checks.assert_not_awaited()
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reviews.assert_not_awaited()
        gh.pr_reactions.assert_not_awaited()
        gh.commit_committed_at.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        workspace.cleanup.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert await db.issue_prs.list_merge_candidates(conn) == []
        comment_body = linear.post_comment.await_args.args[1]
        assert "pull request closed before merge" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_failure_moves_issue_to_needs_approval(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={"headRefOid": "abc123", "mergeable": "MERGEABLE"}
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[
                    CheckRun(
                        name="test",
                        state="SUCCESS",
                        bucket="pass",
                    )
                ]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock(side_effect=GitHubError("branch protection blocked"))

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        workspace.cleanup.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        comment_body = linear.post_comment.await_args.args[1]
        assert "branch protection blocked" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_conflicting_pr_precheck_dispatches_rebase_fix_not_needs_approval(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "CONFLICTING",
                "mergeStateStatus": "DIRTY",
                "baseRefName": "release/1.2",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock()
        gh.pr_review_comments = AsyncMock()
        gh.pr_reviews = AsyncMock()
        gh.pr_reactions = AsyncMock()
        gh.pr_issue_comments = AsyncMock()
        gh.commit_committed_at = AsyncMock()
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude", auto_merge=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        await _poll_and_wait(orch)

        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "review_fix"
        assert runner.captured_spec.command[0] == "codex"
        prompt = runner.captured_spec.command[-1]
        assert "PR #42 has merge conflicts against `release/1.2`" in prompt
        assert "Rebase the branch onto `origin/release/1.2`" in prompt
        gh.pr_checks.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        linear.move_issue.assert_not_awaited()
        assert await db.operator_waits.get(conn, "iss-1") is None
        pr = await db.issue_prs.get(
            conn, issue_id="iss-1", github_repo="org/repo"
        )
        assert pr is not None
        assert pr.parked_at is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review", "review_fix"]
        assert history[-1].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_fix_reenters_merge_on_next_poll(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        conflict_view = {
            "headRefOid": "abc123",
            "mergeable": "CONFLICTING",
            "mergeStateStatus": "DIRTY",
            "baseRefName": "release/1.2",
            "mergedAt": None,
        }
        clean_view = {
            "headRefOid": "def456",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "baseRefName": "release/1.2",
            "state": "OPEN",
            "mergedAt": None,
        }
        merged_view = {
            **clean_view,
            "state": "MERGED",
            "mergedAt": "2026-05-10T00:04:00Z",
        }
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[conflict_view, clean_view, clean_view, clean_view, merged_view]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:03:00Z")
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)
        assert gh.pr_merge.await_count == 0
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review", "review_fix"]
        assert await db.issue_prs.has_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_created_at="2026-05-10T00:01:00+00:00",
            head_sha="def456",
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_merge.assert_awaited_once()
        push_fn.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        assert await db.operator_waits.get(conn, "iss-1") is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"
        assert not await db.issue_prs.has_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_created_at="2026-05-10T00:01:00+00:00",
            head_sha="def456",
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_merge_conflict_fix_marker_does_not_bypass_new_pr_cycle(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await conn.execute(
            """
            INSERT INTO merge_conflict_fix_marks (
                issue_id, github_repo, pr_number, pr_created_at, head_sha, marked_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "iss-1",
                "org/repo",
                41,
                "2026-05-09T00:01:00+00:00",
                "abc123",
                "2026-05-09T00:02:00+00:00",
            ),
        )
        await conn.commit()

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "main",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(cfg, linear, conn, runner=_FakeRunner([]), gh=gh)

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001
        gh.pr_merge.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_merge_conflict_fix_marker_does_not_bypass_new_head(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await conn.execute(
            """
            INSERT INTO merge_conflict_fix_marks (
                issue_id, github_repo, pr_number, pr_created_at, head_sha, marked_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "iss-1",
                "org/repo",
                42,
                "2026-05-10T00:01:00+00:00",
                "old-head",
                "2026-05-10T00:02:00+00:00",
            ),
        )
        await conn.commit()

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "new-head",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "main",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:03:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(cfg, linear, conn, runner=_FakeRunner([]), gh=gh)

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001
        gh.pr_merge.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_fix_marker_survives_when_merge_capacity_full(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        assert await db.issue_prs.mark_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            head_sha="abc123",
            marked_at="2026-05-10T00:02:00+00:00",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "main",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")

        cfg = Config(
            repos=[_binding(agent="claude")],
            global_max_concurrent=0,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(cfg, linear, conn, runner=_FakeRunner([]), gh=gh)

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001
        assert await db.issue_prs.has_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_created_at="2026-05-10T00:01:00+00:00",
            head_sha="abc123",
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_fix_marker_survives_when_scheduled_merge_bails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        assert await db.issue_prs.mark_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            head_sha="abc123",
            marked_at="2026-05-10T00:02:00+00:00",
        )

        inactive_issue = _issue()
        inactive_issue.state_name = "Done"
        inactive_issue.state_type = "completed"

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(side_effect=[_issue(), inactive_issue])
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "main",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner([]),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        await _poll_and_wait(orch)

        gh.pr_merge.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review"]
        assert await db.issue_prs.has_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_created_at="2026-05-10T00:01:00+00:00",
            head_sha="abc123",
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_exception_dispatches_rebase_fix_not_needs_approval(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        approved_view = {
            "headRefOid": "abc123",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "baseRefName": "release/1.2",
            "mergedAt": None,
        }
        gh.pr_view = AsyncMock(return_value=approved_view)
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock(
            side_effect=GitHubError("merge conflict between abc123 and base")
        )
        push_fn = AsyncMock()
        binding = _binding(agent="claude").model_copy(update={"max_concurrent": 1})

        cfg = Config(
            repos=[binding],
            global_max_concurrent=1,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )

        await orch._review_fix_sem.acquire()  # noqa: SLF001
        try:
            await asyncio.wait_for(_poll_and_wait(orch), timeout=1)
        finally:
            orch._review_fix_sem.release()  # noqa: SLF001

        push_fn.assert_awaited_once()
        gh.pr_merge.assert_awaited_once()
        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "review_fix"
        assert runner.captured_spec.command[0] == "codex"
        prompt = runner.captured_spec.command[-1]
        assert "PR #42 has merge conflicts against `release/1.2`" in prompt
        linear.move_issue.assert_not_awaited()
        workspace.cleanup.assert_not_awaited()
        assert await db.operator_waits.get(conn, "iss-1") is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "merge",
            "review_fix",
        ]
        assert next(run for run in history if run.stage == "merge").status == "interrupted"
        assert history[-1].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_fix_marks_fixed_head_before_interrupting_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.runs.create(
            conn,
            id="merge-wait",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T00:03:00+00:00",
        )

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "fixedsha",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "release/1.2",
                "mergedAt": None,
            }
        )

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
        )

        events: list[tuple[str, bool]] = []
        original_mark = db.issue_prs.mark_merge_conflict_fixed
        original_interrupt = db.runs.interrupt_stale_merge_needs_approval

        async def recording_mark_merge_conflict_fixed(
            *args: object,
            **kwargs: object,
        ) -> bool:
            result = await original_mark(*args, **kwargs)  # type: ignore[arg-type]
            events.append(("mark", result))
            return result

        async def recording_interrupt_stale_merge_needs_approval(
            conn_arg,
            *,
            issue_id: str,
            github_repo: str,
            pr_number: int,
            before: str | None = None,
        ) -> int:  # type: ignore[no-untyped-def]
            marker_exists = await db.issue_prs.has_merge_conflict_fixed(
                conn_arg,
                issue_id=issue_id,
                github_repo=github_repo,
                pr_number=pr_number,
                pr_created_at="2026-05-10T00:01:00+00:00",
                head_sha="fixedsha",
            )
            events.append(("interrupt", marker_exists))
            return await original_interrupt(
                conn_arg,
                issue_id=issue_id,
                github_repo=github_repo,
                pr_number=pr_number,
                before=before,
            )

        monkeypatch.setattr(
            db.issue_prs,
            "mark_merge_conflict_fixed",
            recording_mark_merge_conflict_fixed,
        )
        monkeypatch.setattr(
            db.runs,
            "interrupt_stale_merge_needs_approval",
            recording_interrupt_stale_merge_needs_approval,
        )

        result = await orch._dispatch_merge_conflict_rebase_fix_run(  # noqa: SLF001
            binding=_binding(agent="claude"),
            issue=_issue(),
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            view={"baseRefName": "release/1.2"},
            merge_run_id="merge-wait",
            dispatch_capacity_held=True,
        )

        assert result is True
        assert events == [("mark", True), ("interrupt", True)]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_fix_interrupts_all_stale_merge_needs_approval(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        for idx in range(3):
            await db.runs.create(
                conn,
                id=f"stale-merge-{idx}",
                issue_id="iss-1",
                stage="merge",
                status="needs_approval",
                pid=None,
                started_at=f"2026-05-10T00:0{idx + 2}:00+00:00",
            )

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "fixedsha",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "release/1.2",
                "mergedAt": None,
            }
        )

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
        )

        result = await orch._dispatch_merge_conflict_rebase_fix_run(  # noqa: SLF001
            binding=_binding(agent="claude"),
            issue=_issue(),
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            view={"baseRefName": "release/1.2"},
            merge_run_id="stale-merge-1",
            dispatch_capacity_held=True,
        )

        assert result is True
        history = await db.runs.history_for_issue(conn, "iss-1")
        stale_merges = [run for run in history if run.id.startswith("stale-merge-")]
        assert {run.status for run in stale_merges} == {"interrupted"}
        assert all(run.ended_at is not None for run in stale_merges)
        assert await db.issue_prs.has_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_created_at="2026-05-10T00:01:00+00:00",
            head_sha="fixedsha",
        )
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert [candidate.pr_number for candidate in candidates] == [42]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_fix_interrupts_stale_merge_when_marker_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        for idx in range(3):
            await db.runs.create(
                conn,
                id=f"stale-merge-{idx}",
                issue_id="iss-1",
                stage="merge",
                status="needs_approval",
                pid=None,
                started_at=f"2026-05-10T00:0{idx + 2}:00+00:00",
            )

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "fixedsha",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "baseRefName": "release/1.2",
                "mergedAt": None,
            }
        )

        monkeypatch.setattr(
            db.issue_prs,
            "mark_merge_conflict_fixed",
            AsyncMock(return_value=False),
        )

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
        )

        result = await orch._dispatch_merge_conflict_rebase_fix_run(  # noqa: SLF001
            binding=_binding(agent="claude"),
            issue=_issue(),
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            view={"baseRefName": "release/1.2"},
            merge_run_id="stale-merge-1",
            dispatch_capacity_held=True,
        )

        assert result is True
        history = await db.runs.history_for_issue(conn, "iss-1")
        stale_merges = [run for run in history if run.id.startswith("stale-merge-")]
        assert {run.status for run in stale_merges} == {"interrupted"}
        assert all(run.ended_at is not None for run in stale_merges)
        assert not await db.issue_prs.has_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_created_at="2026-05-10T00:01:00+00:00",
            head_sha="fixedsha",
        )
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert [candidate.pr_number for candidate in candidates] == [42]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_precheck_after_merge_agent_dispatches_rebase_fix(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        approved_view = {
            "headRefOid": "abc123",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "baseRefName": "release/1.2",
            "mergedAt": None,
        }
        conflict_view = {
            "headRefOid": "abc123",
            "mergeable": "CONFLICTING",
            "mergeStateStatus": "DIRTY",
            "baseRefName": "release/1.2",
            "mergedAt": None,
        }
        fixed_view = {
            **approved_view,
            "headRefOid": "def456",
        }
        gh = MagicMock()
        gh.pr_view = AsyncMock(side_effect=[approved_view, conflict_view, fixed_view])
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()
        binding = _binding(agent="claude").model_copy(update={"max_concurrent": 1})

        cfg = Config(
            repos=[binding],
            global_max_concurrent=1,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )

        await asyncio.wait_for(_poll_and_wait(orch), timeout=1)

        push_fn.assert_awaited_once()
        gh.pr_merge.assert_not_awaited()
        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "review_fix"
        prompt = runner.captured_spec.command[-1]
        assert "PR #42 has merge conflicts against `release/1.2`" in prompt
        assert await db.operator_waits.get(conn, "iss-1") is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "merge",
            "review_fix",
        ]
        assert next(run for run in history if run.stage == "merge").status == "interrupted"
        assert history[-1].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_dispatch_closes_active_review_monitor_before_needs_approval(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    review_task: asyncio.Task[bool] | None = None
    try:
        await _seed_review_candidate(conn)
        await db.runs.update_status(conn, "review", "running")

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={"headRefOid": "abc123", "mergeable": "MERGEABLE"}
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[
                    CheckRun(
                        name="test",
                        state="SUCCESS",
                        bucket="pass",
                    )
                ]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock(side_effect=GitHubError("branch protection blocked"))

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        review_task = asyncio.create_task(asyncio.Event().wait())
        orch._review_poll_tasks.add(review_task)  # noqa: SLF001
        orch._review_poll_run_ids.add("review")  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = "review"  # noqa: SLF001
        orch._review_poll_run_tasks["review"] = review_task  # noqa: SLF001

        await _poll_and_wait(orch)
        await asyncio.gather(review_task, return_exceptions=True)

        assert review_task.cancelled()
        assert "review" not in orch._review_poll_run_ids  # noqa: SLF001
        assert "iss-1" not in orch._review_poll_issue_ids  # noqa: SLF001
        assert "review" not in orch._review_poll_run_tasks  # noqa: SLF001
        assert review_task not in orch._review_poll_tasks  # noqa: SLF001

        history = await db.runs.history_for_issue(conn, "iss-1")
        review_run = next(r for r in history if r.id == "review")
        assert review_run.status == "completed"
        assert review_run.ended_at is not None
        merge_run = next(r for r in history if r.stage == "merge")
        assert merge_run.status == "needs_approval"

        gh.pr_review_comments = AsyncMock(
            return_value=[
                {
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Late inline comment on the approved head",
                    "commit_id": "abc123",
                    "original_commit_id": "abc123",
                    "created_at": "2026-05-10T00:05:00Z",
                    "path": "app.py",
                    "line": 1,
                }
            ]
        )
        assert await orch._poll_review_runs() == []  # noqa: SLF001
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert not [r for r in history if r.stage == "review_fix"]
        gh.pr_review_comments.assert_not_awaited()
    finally:
        if review_task is not None and not review_task.done():
            review_task.cancel()
            await asyncio.gather(review_task, return_exceptions=True)
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_precheck_does_not_need_state_lookup(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.team_states = AsyncMock(side_effect=LinearError("states down"))
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "CONFLICTING",
                "mergeStateStatus": "DIRTY",
                "baseRefName": "main",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[
                    CheckRun(
                        name="test",
                        state="SUCCESS",
                        bucket="pass",
                    )
                ]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        await _poll_and_wait(orch)

        linear.team_states.assert_not_awaited()
        linear.move_issue.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "review_fix"
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review", "review_fix"]
        assert history[-1].status == "completed"
        assert await db.runs.has_active(conn, "iss-1") is False
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_required_status_failure_precheck_dispatches_fix_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "UNSTABLE",
                "baseRefName": "main",
                "mergedAt": None,
                "statusCheckRollup": [
                    {
                        "__typename": "StatusContext",
                        "context": "Vercel",
                        "state": "FAILURE",
                        "targetUrl": "https://vercel.com/org/repo/deployments/123",
                        "description": "Deployment failed.",
                    }
                ],
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="ci", state="SUCCESS", bucket="pass")]
            )
        )
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._review_verdict_for_pr = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="approved")
        )
        orch._dispatch_merge_required_check_fix_run = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=True
        )
        scheduled_merge = asyncio.create_task(asyncio.sleep(0))
        orch._schedule_merge = MagicMock(return_value=scheduled_merge)  # type: ignore[method-assign]  # noqa: SLF001
        required_contexts = AsyncMock(return_value=("Vercel",))
        monkeypatch.setattr(
            "symphony.orchestrator.poll.get_required_contexts",
            required_contexts,
            raising=False,
        )

        await _poll_and_wait(orch)
        await scheduled_merge

        gh.pr_view.assert_awaited_once_with(  # type: ignore[attr-defined]
            42,
            repo="org/repo",
            include_status_checks=True,
        )
        required_contexts.assert_awaited_once()
        assert required_contexts.await_args.args == ("org/repo", 42)
        assert required_contexts.await_args.kwargs["gh"] is gh
        assert isinstance(required_contexts.await_args.kwargs["cache"], dict)
        orch._dispatch_merge_required_check_fix_run.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._dispatch_merge_required_check_fix_run.await_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["pr_number"] == 42
        assert kwargs["pr_url"] == "https://github.com/org/repo/pull/42"
        assert kwargs["head_sha"] == "abc123"
        assert kwargs["merge_error"] == "required status check failed before merge"
        assert [check["context"] for check in kwargs["failing_checks"]] == ["Vercel"]
        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
        gh.pr_merge.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_required_status_failure_precheck_falls_back_when_deduped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.review_state.set_signature(
            conn,
            "iss-1",
            _required_check_trigger_signature(
                head_sha="abc123",
                failing_checks=[
                    {
                        "__typename": "StatusContext",
                        "context": "Vercel",
                        "state": "FAILURE",
                    }
                ],
            ),
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "UNSTABLE",
                "baseRefName": "main",
                "mergedAt": None,
                "statusCheckRollup": [
                    {
                        "__typename": "StatusContext",
                        "context": "Vercel",
                        "state": "FAILURE",
                        "targetUrl": "https://vercel.com/org/repo/deployments/123",
                        "description": "Deployment failed.",
                    }
                ],
            }
        )

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._review_verdict_for_pr = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="approved")
        )
        orch._dispatch_merge_required_check_fix_run = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=True
        )
        scheduled_merge = asyncio.create_task(asyncio.sleep(0))
        orch._schedule_merge = MagicMock(return_value=scheduled_merge)  # type: ignore[method-assign]  # noqa: SLF001
        required_contexts = AsyncMock(return_value=("Vercel",))
        monkeypatch.setattr(
            "symphony.orchestrator.poll.get_required_contexts",
            required_contexts,
            raising=False,
        )

        await _poll_and_wait(orch)
        await scheduled_merge

        required_contexts.assert_awaited_once()
        assert required_contexts.await_args.args == ("org/repo", 42)
        orch._dispatch_merge_required_check_fix_run.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_optional_status_failure_precheck_preserves_merge_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "UNSTABLE",
                "baseRefName": "main",
                "mergedAt": None,
                "statusCheckRollup": [
                    {
                        "__typename": "StatusContext",
                        "context": "Vercel",
                        "state": "FAILURE",
                        "targetUrl": "https://vercel.com/org/repo/deployments/123",
                        "description": "Deployment failed.",
                    }
                ],
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="ci", state="SUCCESS", bucket="pass")]
            )
        )
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._review_verdict_for_pr = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="approved")
        )
        orch._dispatch_merge_required_check_fix_run = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=True
        )
        scheduled_merge = asyncio.create_task(asyncio.sleep(0))
        orch._schedule_merge = MagicMock(return_value=scheduled_merge)  # type: ignore[method-assign]  # noqa: SLF001
        required_contexts = AsyncMock(return_value=("ci",))
        monkeypatch.setattr(
            "symphony.orchestrator.poll.get_required_contexts",
            required_contexts,
            raising=False,
        )

        scheduled = await orch._poll_merge_candidates()  # noqa: SLF001
        await asyncio.gather(*scheduled)
        await scheduled_merge

        gh.pr_view.assert_awaited_once_with(  # type: ignore[attr-defined]
            42,
            repo="org/repo",
            include_status_checks=True,
        )
        required_contexts.assert_awaited_once()
        assert required_contexts.await_args.args == ("org/repo", 42)
        orch._dispatch_merge_required_check_fix_run.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_required_status_failure_cost_cap_parks_merge_needs_approval(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        workspace = MagicMock()
        workspace.acquire = AsyncMock()
        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=0.25,
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        dispatched = await orch._dispatch_merge_required_check_fix_if_allowed(  # noqa: SLF001
            binding=_binding(agent="claude"),
            issue=_issue(),
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            head_sha="abc123",
            failing_checks=[
                {
                    "__typename": "StatusContext",
                    "context": "Vercel",
                    "state": "FAILURE",
                }
            ],
            merge_error="gh pr merge failed",
        )

        assert dispatched is False
        workspace.acquire.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_MERGE
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert "required-check cost cap reached: $0.5000" in (
            linear.post_comment.await_args.args[1]
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_required_check_fix_reruns_local_review_before_push_for_local_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        binding = _binding(agent="claude").model_copy(
            update={"local_review": True, "remote_review": False}
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_comment = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        monkeypatch.setattr(
            "symphony.orchestrator.poll._git_fetch_branch",
            AsyncMock(return_value=None),
        )
        monkeypatch.setattr(
            "symphony.orchestrator.poll._workspace_ref_sha",
            AsyncMock(return_value="start-sha"),
        )
        monkeypatch.setattr(
            "symphony.orchestrator.poll._workspace_head_sha",
            AsyncMock(return_value="fixed-sha"),
        )
        orch._run_required_check_fix_agent = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=(UsageDelta(cost_usd=0.01), "exit", 0, False)
        )

        async def rerun_local_review(**kwargs: object) -> LoopResult:
            assert kwargs["parent_run_id"] != "implement"
            assert push_fn.await_count == 0
            await db.runs.create(
                conn,
                id="post-required-check-local-review",
                issue_id="iss-1",
                stage="local_review",
                status="completed",
                pid=None,
                started_at=datetime.now(UTC).isoformat(),
            )
            return LoopResult(
                outcome=LoopOutcome.APPROVED,
                iterations=1,
                verdicts=(),
            )

        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=rerun_local_review
        )

        dispatched = await orch._dispatch_merge_required_check_fix_run(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            head_sha="abc123",
            failing_checks=[
                {
                    "__typename": "StatusContext",
                    "context": "Vercel",
                    "state": "FAILURE",
                }
            ],
            merge_error="required status check failed before merge",
            trigger_signature="required_check_failure:abc123:vercel",
            iteration=1,
        )

        assert dispatched is True
        orch._run_local_review_phase.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.pr_comment.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history][-2:] == [
            "review_fix",
            "local_review",
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_required_check_fix_parks_when_post_fix_local_review_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        binding = _binding(agent="claude").model_copy(
            update={"local_review": True, "remote_review": False}
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_comment = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        monkeypatch.setattr(
            "symphony.orchestrator.poll._git_fetch_branch",
            AsyncMock(return_value=None),
        )
        monkeypatch.setattr(
            "symphony.orchestrator.poll._workspace_ref_sha",
            AsyncMock(return_value="start-sha"),
        )
        monkeypatch.setattr(
            "symphony.orchestrator.poll._workspace_head_sha",
            AsyncMock(return_value="fixed-sha"),
        )
        orch._run_required_check_fix_agent = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=(UsageDelta(cost_usd=0.01), "exit", 0, False)
        )
        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=LoopResult(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=1,
                verdicts=(),
                error="reviewer still found issues",
            )
        )

        dispatched = await orch._dispatch_merge_required_check_fix_run(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            head_sha="abc123",
            failing_checks=[
                {
                    "__typename": "StatusContext",
                    "context": "Vercel",
                    "state": "FAILURE",
                }
            ],
            merge_error="required status check failed before merge",
            trigger_signature="required_check_failure:abc123:vercel",
            iteration=1,
        )

        assert dispatched is False
        orch._run_local_review_phase.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        push_fn.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_MERGE
        assert "post-required-check local-only review did not approve" in (
            linear.post_comment.await_args.args[1]
        )
    finally:
        await conn.close()


# --- remote_review: false merge bypass ----------------------------------
#
# `remote_review: false` bindings never receive an `@codex` approval, so the
# merge scheduler must treat a clean no_signal verdict (after CI + mergeability
# pass) as the merge signal. Local-only bindings additionally require a
# completed local-review loop; no-review bindings gate on CI alone.


def _no_signal_view() -> dict[str, object]:
    return {
        "headRefOid": "abc123",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
        "state": "OPEN",
        "mergedAt": None,
    }


def _make_poll_merge_orchestrator(
    conn,  # type: ignore[no-untyped-def]
    *,
    binding: RepoBinding,
    verdict: Verdict,
) -> Orchestrator:
    gh = MagicMock()
    gh.pr_view = AsyncMock(return_value=_no_signal_view())
    linear = AsyncMock()
    linear.lookup_issue = AsyncMock(return_value=_issue())
    linear.move_issue = AsyncMock(return_value=None)
    linear.post_comment = AsyncMock(return_value="cmt-1")
    orch = Orchestrator(
        Config(repos=[binding]),
        linear,
        conn,
        runner=MagicMock(),
        gh=gh,
        workspace=MagicMock(),
        push_fn=AsyncMock(),
    )
    orch._states = {"ENG": _states()}  # noqa: SLF001
    orch._finalize_pr_if_closed = AsyncMock(return_value=False)  # type: ignore[method-assign]  # noqa: SLF001
    orch._required_check_failures_for_view = AsyncMock(return_value=[])  # type: ignore[method-assign]  # noqa: SLF001
    orch._review_verdict_for_pr = AsyncMock(return_value=verdict)  # type: ignore[method-assign]  # noqa: SLF001
    return orch


@pytest.mark.asyncio
async def test_no_review_binding_merges_clean_ci_no_signal(tmp_path: Path) -> None:
    """false/false: clean CI + mergeable no_signal merges without any review."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        binding = _binding().model_copy(
            update={"local_review": False, "remote_review": False}
        )
        orch = _make_poll_merge_orchestrator(
            conn,
            binding=binding,
            verdict=Verdict(kind=VerdictKind.PENDING, rule="no_signal"),
        )
        scheduled = asyncio.create_task(asyncio.sleep(0))
        orch._schedule_merge = MagicMock(return_value=scheduled)  # type: ignore[method-assign]  # noqa: SLF001

        await _poll_and_wait(orch)

        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._schedule_merge.call_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["pr_number"] == 42
        # Not an @codex approval, so the merge run skips the review gate.
        assert kwargs["skip_review"] is True
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_only_binding_merges_after_local_review_completed(
    tmp_path: Path,
) -> None:
    """true/false: a completed local-review loop unblocks the no_signal merge."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.runs.create(
            conn,
            id="local-review",
            issue_id="iss-1",
            stage="local_review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:30+00:00",
        )
        binding = _binding().model_copy(
            update={"local_review": True, "remote_review": False}
        )
        orch = _make_poll_merge_orchestrator(
            conn,
            binding=binding,
            verdict=Verdict(kind=VerdictKind.PENDING, rule="no_signal"),
        )
        scheduled = asyncio.create_task(asyncio.sleep(0))
        orch._schedule_merge = MagicMock(return_value=scheduled)  # type: ignore[method-assign]  # noqa: SLF001

        await _poll_and_wait(orch)

        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        assert orch._schedule_merge.call_args.kwargs["pr_number"] == 42  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_only_binding_waits_when_local_review_predates_pr_cycle(
    tmp_path: Path,
) -> None:
    """true/false: a local review from a prior PR cycle cannot approve this PR."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.runs.create(
            conn,
            id="stale-local-review",
            issue_id="iss-1",
            stage="local_review",
            status="completed",
            pid=None,
            started_at="2026-05-09T23:59:00+00:00",
        )
        binding = _binding().model_copy(
            update={"local_review": True, "remote_review": False}
        )
        orch = _make_poll_merge_orchestrator(
            conn,
            binding=binding,
            verdict=Verdict(kind=VerdictKind.PENDING, rule="no_signal"),
        )
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await _poll_and_wait(orch)

        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_only_binding_waits_without_completed_local_review(
    tmp_path: Path,
) -> None:
    """true/false: no completed local review means no_signal stays unmerged."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        binding = _binding().model_copy(
            update={"local_review": True, "remote_review": False}
        )
        orch = _make_poll_merge_orchestrator(
            conn,
            binding=binding,
            verdict=Verdict(kind=VerdictKind.PENDING, rule="no_signal"),
        )
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await _poll_and_wait(orch)

        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_only_binding_waits_when_local_review_predates_fix(
    tmp_path: Path,
) -> None:
    """true/false: no_signal must wait when a fix-run has no fresh local review."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.runs.create(
            conn,
            id="local-review",
            issue_id="iss-1",
            stage="local_review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:30+00:00",
        )
        # A fix-run pushed a new HEAD the in-workspace reviewer never saw.
        await db.runs.create(
            conn,
            id="review-fix",
            issue_id="iss-1",
            stage="review_fix",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:05:00+00:00",
        )
        binding = _binding().model_copy(
            update={"local_review": True, "remote_review": False}
        )
        orch = _make_poll_merge_orchestrator(
            conn,
            binding=binding,
            verdict=Verdict(kind=VerdictKind.PENDING, rule="no_signal"),
        )
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await _poll_and_wait(orch)

        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_only_binding_merges_when_local_review_follows_fix(
    tmp_path: Path,
) -> None:
    """true/false: post-fix local review covers the clean no_signal PR."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.runs.create(
            conn,
            id="review-fix",
            issue_id="iss-1",
            stage="review_fix",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:05:00+00:00",
        )
        await db.runs.create(
            conn,
            id="post-fix-local-review",
            issue_id="iss-1",
            stage="local_review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:06:00+00:00",
        )
        binding = _binding().model_copy(
            update={"local_review": True, "remote_review": False}
        )
        orch = _make_poll_merge_orchestrator(
            conn,
            binding=binding,
            verdict=Verdict(kind=VerdictKind.PENDING, rule="no_signal"),
        )
        scheduled = asyncio.create_task(asyncio.sleep(0))
        orch._schedule_merge = MagicMock(return_value=scheduled)  # type: ignore[method-assign]  # noqa: SLF001

        await _poll_and_wait(orch)

        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        assert orch._schedule_merge.call_args.kwargs["pr_number"] == 42  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()
