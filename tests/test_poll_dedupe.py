"""The poll loop dedupes via SQLite, not the old in-memory `_dispatched`
dict. Re-scanning an issue that already has an active run must not
re-dispatch."""

from __future__ import annotations

import asyncio
import inspect
import itertools
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import aiosqlite
import pytest

from symphony import db
from symphony.agent.runner import Runner, RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearError, LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.tracker import DEFAULT_PROVIDER, DEFAULT_SITE


def test_orchestrator_no_longer_uses_in_memory_dispatched_dict() -> None:
    src = inspect.getsource(Orchestrator)
    assert "self._dispatched" not in src, (
        "the in-memory dedupe ledger must be replaced by a SQLite query"
    )


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        pass


class _BlockingRunner:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.killed = asyncio.Event()
        self.run_id: str | None = None
        self.killed_run_id: str | None = None
        self._forever = asyncio.Event()

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        return self._aiter(spec)

    async def _aiter(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.run_id = spec.run_id
        self.started.set()
        yield RunnerEvent(kind="started", pid=4242)
        await self._forever.wait()

    async def kill(self, run_id: str) -> None:
        self.killed_run_id = run_id
        self.killed.set()


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _issue(
    uid: str = "iss-1",
    ident: str = "ENG-1",
    *,
    state_name: str = "Todo",
    labels: list[str] | None = None,
) -> LinearIssue:
    return LinearIssue(
        id=uid,
        identifier=ident,
        title="t",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name=state_name,
        state_type="unstarted",
        team_key="ENG",
        labels=labels or [],
    )


@pytest.fixture(autouse=True)
def _advancing_head(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fake workspace is not a git repo. Simulate a successful agent
    advancing HEAD so the Implement completion gate classifies rc=0 runs as
    completed (returns a fresh SHA on every call, so HEAD always advances)."""
    counter = itertools.count()

    async def _head(workspace_path: object) -> str:
        return f"sha-{next(counter)}"

    # SYM-150: the implement completion gate reads `_workspace_head_sha` from
    # `poll._lifecycle`; other stages still read it from `poll`. Patch both.
    monkeypatch.setattr("symphony.orchestrator.poll._workspace_head_sha", _head)
    monkeypatch.setattr("symphony.orchestrator.poll._lifecycle._workspace_head_sha", _head)


def _make_orch(
    cfg: Config,
    linear: AsyncMock,
    conn: object,
    *,
    runner: Runner | None = None,
) -> Orchestrator:
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=Path("/dev/null"))
    workspace.release = MagicMock()
    gh = MagicMock()
    gh.ensure_pr = AsyncMock(return_value="https://example.invalid/pr/1")
    gh.pr_comment = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="main")
    push_fn = AsyncMock()
    orch = Orchestrator(
        cfg,
        linear,
        conn,  # type: ignore[arg-type]
        runner=runner or _FakeRunner([RunnerEvent(kind="exit", returncode=0)]),
        gh=gh,
        workspace=workspace,
        push_fn=push_fn,
    )
    orch._states = {"ENG": {"Todo": "state-todo", "In Progress": "state-progress"}}  # noqa: SLF001
    linear.lookup_issue = AsyncMock(return_value=_issue())
    return orch


async def _scan_and_wait(orch: Orchestrator, binding: RepoBinding) -> None:
    tasks = await orch._scan_binding(binding)  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_scan_schedules_dispatch_without_waiting(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue(), _issue("iss-2", "ENG-2")])

        orch = _make_orch(cfg, linear, conn)
        started = asyncio.Event()
        release = asyncio.Event()

        async def _slow_dispatch(binding: RepoBinding, issue: LinearIssue) -> str | None:
            started.set()
            await release.wait()
            return issue.id

        orch._dispatch_one = AsyncMock(side_effect=_slow_dispatch)  # type: ignore[method-assign]  # noqa: SLF001

        tasks = await asyncio.wait_for(
            orch._scan_binding(cfg.repos[0]),  # noqa: SLF001
            timeout=0.2,
        )

        assert len(tasks) == 2
        await asyncio.wait_for(started.wait(), timeout=1)
        release.set()
        await asyncio.gather(*tasks)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_paused_dispatch_starts_no_new_runs(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue(), _issue("iss-2", "ENG-2")])

        orch = _make_orch(cfg, linear, conn)
        orch._dispatch_one = AsyncMock(return_value="run-x")  # type: ignore[method-assign]  # noqa: SLF001

        assert orch.is_dispatch_paused() is False

        # Paused: scanning ready issues schedules no new runs.
        orch.set_dispatch_paused(True)
        assert orch.is_dispatch_paused() is True
        paused_tasks = await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert paused_tasks == []
        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001

        # Resume: normal dispatch is restored.
        orch.set_dispatch_paused(False)
        assert orch.is_dispatch_paused() is False
        tasks = await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert len(tasks) == 2
        await asyncio.gather(*tasks)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_pause_toggled_while_dispatch_task_waits_on_limits(tmp_path: Path) -> None:
    """Regression: pause toggled after `_schedule_ready_issue` has already
    returned a task, but before that task acquires dispatch capacity and
    reaches `_dispatch_one`, must still prevent the run from starting."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])

        orch = _make_orch(cfg, linear, conn)
        orch._dispatch_one = AsyncMock(return_value="run-x")  # type: ignore[method-assign]  # noqa: SLF001

        # Simulate the global dispatch slot being fully occupied by other
        # in-flight work, forcing the newly scheduled task to wait on the
        # semaphore instead of dispatching immediately.
        for _ in range(cfg.global_max_concurrent):
            await orch._global_dispatch_sem.acquire()  # noqa: SLF001

        tasks = await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert len(tasks) == 1
        await asyncio.sleep(0)  # let the task start waiting on the semaphore

        # Pause is toggled while the task is still waiting for capacity.
        orch.set_dispatch_paused(True)

        # Free the slot: the waiting task can now acquire it, but must
        # notice the pause before calling `_dispatch_one`.
        orch._global_dispatch_sem.release()  # noqa: SLF001
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_pause_toggled_while_refreshing_dispatch_candidate(tmp_path: Path) -> None:
    """Regression: pause toggled while the dispatch task is awaiting
    `_refresh_dispatch_candidate` (a Linear API round-trip) must still
    prevent `_dispatch_one` from running once that await returns."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])

        orch = _make_orch(cfg, linear, conn)
        orch._dispatch_one = AsyncMock(return_value="run-x")  # type: ignore[method-assign]  # noqa: SLF001

        refreshing = asyncio.Event()
        resume_refresh = asyncio.Event()
        real_refresh = orch._refresh_dispatch_candidate  # noqa: SLF001

        async def _slow_refresh(binding: object, issue: LinearIssue) -> LinearIssue | None:
            refreshing.set()
            await resume_refresh.wait()
            return await real_refresh(binding, issue)  # noqa: SLF001

        orch._refresh_dispatch_candidate = _slow_refresh  # type: ignore[method-assign]  # noqa: SLF001

        tasks = await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert len(tasks) == 1
        await asyncio.wait_for(refreshing.wait(), timeout=1)

        # Pause is toggled while the task is mid-refresh.
        orch.set_dispatch_paused(True)
        resume_refresh.set()

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_pause_toggled_while_dispatch_one_upserts_issue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: pause toggled while `_dispatch_one` is awaiting
    `db.issues.upsert` (before the `runs` row is inserted) must still
    prevent the run from being created once that await returns."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])

        orch = _make_orch(cfg, linear, conn)

        upserting = asyncio.Event()
        resume_upsert = asyncio.Event()
        real_upsert = db.issues.upsert

        async def _slow_upsert(
            conn: aiosqlite.Connection,
            *,
            id: str,
            identifier: str,
            title: str,
            team_key: str,
            provider: str = DEFAULT_PROVIDER,
            site: str = DEFAULT_SITE,
        ) -> str:
            upserting.set()
            await resume_upsert.wait()
            return await real_upsert(
                conn,
                id=id,
                identifier=identifier,
                title=title,
                team_key=team_key,
                provider=provider,
                site=site,
            )

        monkeypatch.setattr("symphony.orchestrator.poll._lifecycle.db.issues.upsert", _slow_upsert)

        tasks = await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert len(tasks) == 1
        await asyncio.wait_for(upserting.wait(), timeout=1)

        # Pause is toggled while `_dispatch_one` is mid-upsert, i.e. after
        # the last pre-dispatch pause check but before the `runs` row is
        # inserted.
        orch.set_dispatch_paused(True)
        resume_upsert.set()

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

        assert await db.runs.has_running_or_completed(conn, "iss-1") is False
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_shutdown_kills_and_cancels_active_dispatch(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding], poll_interval_secs=300)
        linear = AsyncMock()
        linear.viewer_team_keys = AsyncMock(return_value=["ENG"])
        linear.team_states = AsyncMock(
            return_value={"Todo": "state-todo", "In Progress": "state-progress"}
        )
        linear.issues_in_state = AsyncMock(return_value=[_issue()])

        runner = _BlockingRunner()
        orch = _make_orch(cfg, linear, conn, runner=runner)

        run_task = asyncio.create_task(orch.run())
        await asyncio.wait_for(runner.started.wait(), timeout=1)
        await orch.shutdown()
        await asyncio.wait_for(run_task, timeout=1)

        assert runner.killed_run_id == runner.run_id
        assert runner.killed.is_set()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.status for run in history] == ["failed"]
        assert history[0].ended_at is not None
        assert await db.runs.has_running_or_completed(conn, "iss-1") is False
        assert linear.move_issue.await_args_list[-1] == call("iss-1", "state-todo")
        assert orch._dispatch_tasks == set()  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_queued_dispatch_revalidates_ready_state_before_running(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        first = _issue()
        second = _issue("iss-2", "ENG-2")
        stale_second = _issue("iss-2", "ENG-2", state_name="In Progress")
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[first, second])

        orch = _make_orch(cfg, linear, conn)
        linear.lookup_issue = AsyncMock(side_effect=[first, stale_second])
        started = asyncio.Event()
        release = asyncio.Event()

        async def _slow_dispatch(binding: RepoBinding, issue: LinearIssue) -> str | None:
            started.set()
            await release.wait()
            return issue.id

        orch._dispatch_one = AsyncMock(side_effect=_slow_dispatch)  # type: ignore[method-assign]  # noqa: SLF001

        tasks = await orch._scan_binding(binding)  # noqa: SLF001
        await asyncio.wait_for(started.wait(), timeout=1)
        release.set()
        await asyncio.gather(*tasks)

        assert orch._dispatch_one.await_count == 1  # noqa: SLF001
        orch._dispatch_one.assert_awaited_once_with(binding, first)  # noqa: SLF001
        assert linear.lookup_issue.await_args_list == [call("iss-1"), call("iss-2")]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_empty_issue_label_is_not_required_during_revalidation(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(update={"issue_label": ""})
        cfg = Config(repos=[binding])
        issue = _issue()
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[issue])

        orch = _make_orch(cfg, linear, conn)
        linear.lookup_issue = AsyncMock(return_value=issue)
        orch._dispatch_one = AsyncMock(return_value=issue.id)  # type: ignore[method-assign]  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        linear.issues_in_state.assert_awaited_once_with("ENG", "Todo", "")
        orch._dispatch_one.assert_awaited_once_with(binding, issue)  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_scan_caps_scheduled_tasks_to_available_slots(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(update={"max_concurrent": 1})
        cfg = Config(repos=[binding], global_max_concurrent=1)
        issues = [_issue(f"iss-{n}", f"ENG-{n}") for n in range(3)]
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=issues)

        orch = _make_orch(cfg, linear, conn)
        linear.lookup_issue = AsyncMock(return_value=issues[0])
        started = asyncio.Event()
        release = asyncio.Event()

        async def _slow_dispatch(binding: RepoBinding, issue: LinearIssue) -> str | None:
            started.set()
            await release.wait()
            return issue.id

        orch._dispatch_one = AsyncMock(side_effect=_slow_dispatch)  # type: ignore[method-assign]  # noqa: SLF001

        tasks = await orch._scan_binding(binding)  # noqa: SLF001
        assert len(tasks) == 1

        await asyncio.wait_for(started.wait(), timeout=1)
        assert await orch._scan_binding(binding) == []  # noqa: SLF001

        release.set()
        await asyncio.gather(*tasks)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_scan_skips_issues_with_running_run(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG")
        await db.runs.create(
            conn,
            id="running",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        await _scan_and_wait(orch, cfg.repos[0])
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_schedule_ready_issue_parks_issue_when_pr_already_merged(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        orch._states = {  # noqa: SLF001
            "ENG": {
                "Todo": "state-todo",
                "In Progress": "state-progress",
                "Done": "state-done",
            }
        }
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key="ENG",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            pr_number=101,
            pr_url="https://github.com/org/repo/pull/101",
            created_at="2026-05-19T12:00:00+00:00",
        )
        await db.issue_prs.mark_merged(
            conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            merged_at="2026-05-19T13:15:00+00:00",
        )

        task = await orch._schedule_ready_issue(binding, issue)  # noqa: SLF001
        if task is not None:
            await task

        assert await db.runs.history_for_issue(conn, issue.id) == []
        linear.move_issue.assert_awaited_once_with(issue.id, "state-done")
        linear.post_comment.assert_awaited_once()
        comment_body = linear.post_comment.await_args.args[1]
        assert "PR #101" in comment_body
        assert "already merged" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_schedule_ready_issue_ignores_pr_exists_for_other_repo(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue()
        pr_repo = "org/previous-repo"
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        orch._dispatch_one = AsyncMock(return_value=issue.id)  # type: ignore[method-assign]  # noqa: SLF001
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key="ENG",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=issue.id,
            github_repo=pr_repo,
            pr_number=108,
            pr_url="https://github.com/org/previous-repo/pull/108",
            created_at="2026-05-19T18:00:00+00:00",
        )
        await db.issue_prs.mark_merged(
            conn,
            issue_id=issue.id,
            github_repo=pr_repo,
            merged_at="2026-05-19T18:15:00+00:00",
        )

        task = await orch._schedule_ready_issue(binding, issue)  # noqa: SLF001
        if task is not None:
            await task

        assert task is not None
        orch._dispatch_one.assert_awaited_once_with(binding, issue)  # noqa: SLF001
        linear.move_issue.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
        assert await db.issue_prs.get(conn, issue_id=issue.id, github_repo=pr_repo) is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_schedule_ready_issue_ignores_closed_unmerged_pr_row(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_view = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value={"state": "CLOSED", "mergedAt": None, "merged": False}
        )
        orch._dispatch_one = AsyncMock(return_value=issue.id)  # type: ignore[method-assign]  # noqa: SLF001
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key="ENG",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            pr_number=107,
            pr_url="https://github.com/org/repo/pull/107",
            created_at="2026-05-19T17:02:00+00:00",
        )

        task = await orch._schedule_ready_issue(binding, issue)  # noqa: SLF001
        if task is not None:
            await task

        assert task is not None
        orch._gh.pr_view.assert_awaited_once_with(  # noqa: SLF001
            107, repo=binding.github_repo
        )
        assert (
            await db.issue_prs.get(conn, issue_id=issue.id, github_repo=binding.github_repo) is None
        )
        orch._dispatch_one.assert_awaited_once_with(binding, issue)  # noqa: SLF001
        linear.move_issue.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.parametrize("failure", ["states", "missing_state", "move"])
@pytest.mark.asyncio
async def test_schedule_ready_issue_does_not_comment_when_pr_guard_move_fails(
    tmp_path: Path,
    failure: str,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        if failure == "states":
            orch._states_for_binding = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
                side_effect=LinearError("states down")
            )
        elif failure == "missing_state":
            orch._states = {  # noqa: SLF001
                "ENG": {
                    "Todo": "state-todo",
                    "In Progress": "state-progress",
                }
            }
        else:
            orch._states = {  # noqa: SLF001
                "ENG": {
                    "Todo": "state-todo",
                    "In Progress": "state-progress",
                    "Done": "state-done",
                }
            }
            linear.move_issue = AsyncMock(side_effect=LinearError("move down"))

        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key="ENG",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            pr_number=101,
            pr_url="https://github.com/org/repo/pull/101",
            created_at="2026-05-19T12:00:00+00:00",
        )
        await db.issue_prs.mark_merged(
            conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            merged_at="2026-05-19T13:15:00+00:00",
        )

        task = await orch._schedule_ready_issue(binding, issue)  # noqa: SLF001
        if task is not None:
            await task

        assert await db.runs.history_for_issue(conn, issue.id) == []
        if failure == "move":
            linear.move_issue.assert_awaited_once_with(issue.id, "state-done")
        else:
            linear.move_issue.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_schedule_ready_issue_parks_issue_when_pr_still_open(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        orch._states = {  # noqa: SLF001
            "ENG": {
                "Todo": "state-todo",
                "In Progress": "state-progress",
                "Done": "state-done",
            }
        }
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key="ENG",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            pr_number=107,
            pr_url="https://github.com/org/repo/pull/107",
            created_at="2026-05-19T17:02:00+00:00",
        )

        orch._gh.pr_view = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value={"state": "OPEN", "mergedAt": None, "merged": False}
        )
        task = await orch._schedule_ready_issue(binding, issue)  # noqa: SLF001
        if task is not None:
            await task

        assert await db.runs.history_for_issue(conn, issue.id) == []
        orch._gh.pr_view.assert_awaited_once_with(  # noqa: SLF001
            107, repo=binding.github_repo
        )
        linear.move_issue.assert_awaited_once_with(issue.id, "state-progress")
        linear.post_comment.assert_awaited_once()
        comment_body = linear.post_comment.await_args.args[1]
        assert "PR #107" in comment_body
        assert "still open" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_run_row_is_persisted_before_post_comment(tmp_path: Path) -> None:
    """Dedupe correctness: the `runs` row must exist before the first
    Linear write so a crash after `post_comment` can't leave the issue
    dispatched-but-unrecorded. Asserted by inspecting the DB from inside
    the mocked `post_comment`."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])

        observed: dict[str, bool] = {}

        async def _post(issue_id: str, body: str) -> str:
            observed.setdefault(
                "had_active_when_first_post",
                await db.runs.has_active(conn, issue_id),
            )
            return "cmt-1"

        linear.post_comment = AsyncMock(side_effect=_post)

        orch = _make_orch(cfg, linear, conn)
        await _scan_and_wait(orch, cfg.repos[0])

        assert observed.get("had_active_when_first_post") is True
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failed_announce_clears_dedupe_so_next_tick_retries(
    tmp_path: Path,
) -> None:
    """If the 🚀 `post_comment` raises, the run row must be marked
    non-live so the next poll can retry. Otherwise a transient Linear
    error would jam the issue forever behind its own dedupe row."""
    from symphony.linear.client import LinearError

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        # First scan's 🚀 comment raises; second scan succeeds.
        linear.post_comment = AsyncMock(side_effect=[LinearError("boom"), "cmt-1", "cmt-2"])

        orch = _make_orch(cfg, linear, conn)

        await _scan_and_wait(orch, cfg.repos[0])
        # The failed announce row exists but is no longer live, so dedupe
        # lets the next tick retry.
        assert await db.runs.has_active(conn, "iss-1") is False

        await _scan_and_wait(orch, cfg.repos[0])
        # Second tick re-announces and proceeds (>= 2 total post_comment calls).
        assert linear.post_comment.await_count >= 2
        history = await db.runs.history_for_issue(conn, "iss-1")
        # Latest run is the Review row opened after Implement succeeded;
        # the Implement row before it should be marked completed.
        implement_runs = [r for r in history if r.stage == "implement"]
        assert any(r.status == "completed" for r in implement_runs)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failed_state_move_clears_dedupe_so_next_tick_retries(
    tmp_path: Path,
) -> None:
    """If the Linear move to In Progress fails, do not continue to a completed
    run while the issue is still in the ready state."""
    from symphony.linear.client import LinearError

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(side_effect=LinearError("boom"))

        orch = _make_orch(cfg, linear, conn)
        orch._states = {"ENG": {"Todo": "state-todo", "In Progress": "state-progress"}}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])
        assert await db.runs.has_running_or_completed(conn, "iss-1") is False

        await _scan_and_wait(orch, cfg.repos[0])
        assert linear.post_comment.await_count == 2

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.status for run in history] == ["failed", "failed"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_missing_in_progress_state_clears_dedupe_so_next_tick_retries(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)
        orch._states = {"ENG": {}}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])
        assert await db.runs.has_running_or_completed(conn, "iss-1") is False
        linear.post_comment.assert_not_awaited()

        await _scan_and_wait(orch, cfg.repos[0])
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.status for run in history] == ["failed", "failed"]
    finally:
        await conn.close()
