from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import Blocker, LinearError, LinearIssue
from symphony.orchestrator.poll import Orchestrator


class _FakeRunner:
    def run(self, _spec):  # type: ignore[no-untyped-def]
        return self._aiter()

    async def _aiter(self):  # type: ignore[no-untyped-def]
        yield RunnerEvent(kind="exit", returncode=0)

    async def kill(self, _run_id: str) -> None:
        pass


def _binding(*, waiting: str | None = "Waiting") -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo", waiting=waiting),
    )


def _states(*, include_waiting: bool = True) -> dict[str, str]:
    states = {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Needs Approval": "state-na",
        "Blocked": "state-blocked",
        "Done": "state-done",
    }
    if include_waiting:
        states["Waiting"] = "state-waiting"
    return states


def _blocker(
    identifier: str,
    state_type: str,
    *,
    archived: bool = False,
) -> Blocker:
    return Blocker(
        id=f"id-{identifier}",
        identifier=identifier,
        state_type=state_type,
        archived=archived,
    )


def _issue(
    blockers: list[Blocker],
    *,
    id: str = "iss-1",
    identifier: str = "ENG-1",
    state_id: str = "state-todo",
    state_name: str = "Todo",
    state_type: str = "unstarted",
) -> LinearIssue:
    return LinearIssue(
        id=id,
        identifier=identifier,
        title="Blocked work",
        description="",
        url=f"https://linear.app/team/issue/{identifier}/blocked-work",
        state_id=state_id,
        state_name=state_name,
        state_type=state_type,
        team_key="ENG",
        labels=["symphony"],
        blocked_by=blockers,
    )


def _scan_results(
    ready_issues: list[LinearIssue],
    waiting_issues: list[LinearIssue] | None = None,
) -> AsyncMock:
    return AsyncMock(side_effect=[ready_issues, [] if waiting_issues is None else waiting_issues])


def _make_orch(
    cfg: Config,
    linear: AsyncMock,
    conn: object,
    *,
    include_waiting: bool = True,
) -> Orchestrator:
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=Path("/dev/null"))
    workspace.release = MagicMock()
    gh = MagicMock()
    gh.pr_create = AsyncMock(return_value="https://github.com/org/repo/pull/1")
    gh.pr_comment = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="main")
    orch = Orchestrator(
        cfg,
        linear,
        conn,  # type: ignore[arg-type]
        runner=_FakeRunner(),
        gh=gh,
        workspace=workspace,
        push_fn=AsyncMock(),
    )
    orch._states = {"ENG": _states(include_waiting=include_waiting)}  # noqa: SLF001
    orch._dispatch_one = AsyncMock(return_value=None)  # type: ignore[method-assign]  # noqa: SLF001
    return orch


async def _scan_and_wait(orch: Orchestrator, binding: RepoBinding) -> None:
    tasks = await orch._scan_binding(binding)  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize("state_type", ["backlog", "unstarted", "started", "triage"])
async def test_pickup_with_open_blocker_moves_to_waiting_without_run(
    tmp_path: Path,
    state_type: str,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue([_blocker("WEB-99", state_type)])
        linear = AsyncMock()
        linear.issues_in_state = _scan_results([issue])
        linear.lookup_issue = AsyncMock(return_value=issue)
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        await _scan_and_wait(orch, binding)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-waiting")
        linear.post_comment.assert_awaited_once()
        body = linear.post_comment.await_args.args[1]
        assert "Moved to Waiting" in body
        assert "WEB-99" in body
        assert "Automatic return-to-ready" in body
        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history == []
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state_type", "archived"),
    [("completed", False), ("canceled", False), ("started", True)],
)
async def test_pickup_with_closed_or_archived_blocker_starts_normally(
    tmp_path: Path,
    state_type: str,
    archived: bool,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue([_blocker("ENG-2", state_type, archived=archived)])
        linear = AsyncMock()
        linear.issues_in_state = _scan_results([issue])
        linear.lookup_issue = AsyncMock(return_value=issue)
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        await _scan_and_wait(orch, binding)

        linear.issues_in_state.assert_has_awaits(
            [call("ENG", "Todo", None), call("ENG", "Waiting", None)]
        )
        orch._dispatch_one.assert_awaited_once_with(binding, issue)  # noqa: SLF001
        linear.move_issue.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_waiting_none_preserves_old_pickup_behavior(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(waiting=None)
        cfg = Config(repos=[binding])
        issue = _issue([_blocker("ENG-2", "started")])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[issue])
        linear.lookup_issue = AsyncMock(return_value=issue)
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        await _scan_and_wait(orch, binding)

        orch._dispatch_one.assert_awaited_once_with(binding, issue)  # noqa: SLF001
        linear.move_issue.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_warmup_raises_when_waiting_state_missing(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        linear = AsyncMock()
        linear.viewer_team_keys = AsyncMock(return_value=["ENG"])
        linear.team_states = AsyncMock(return_value=_states(include_waiting=False))

        orch = _make_orch(cfg, linear, conn, include_waiting=False)

        with pytest.raises(LinearError, match="Waiting"):
            await orch.warmup()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_waiting_move_failure_does_not_dispatch_or_comment(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue([_blocker("ENG-2", "started")])
        linear = AsyncMock()
        linear.issues_in_state = _scan_results([issue])
        linear.move_issue = AsyncMock(side_effect=LinearError("move failed"))
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        with caplog.at_level(logging.WARNING):
            await _scan_and_wait(orch, binding)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-waiting")
        linear.post_comment.assert_not_awaited()
        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history == []
        assert "could not move ENG-1 to waiting" in caplog.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_waiting_comment_failure_rolls_issue_back_to_ready(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue([_blocker("ENG-2", "started")])
        linear = AsyncMock()
        linear.issues_in_state = _scan_results([issue])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(side_effect=LinearError("comment failed"))

        orch = _make_orch(cfg, linear, conn)

        with caplog.at_level(logging.WARNING):
            await _scan_and_wait(orch, binding)

        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-waiting"),
            call("iss-1", "state-todo"),
        ]
        linear.post_comment.assert_awaited_once()
        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history == []
        assert "could not comment after moving ENG-1 to waiting" in caplog.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_scan_queries_ready_and_waiting_in_parallel(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        ready_started = asyncio.Event()
        waiting_started = asyncio.Event()

        async def _issues_in_state(
            _team_key: str, state_name: str, _label: str | None
        ) -> list[LinearIssue]:
            if state_name == "Todo":
                ready_started.set()
                await waiting_started.wait()
            elif state_name == "Waiting":
                waiting_started.set()
                await ready_started.wait()
            return []

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(side_effect=_issues_in_state)
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        await asyncio.wait_for(_scan_and_wait(orch, binding), timeout=1)

        assert ready_started.is_set()
        assert waiting_started.is_set()
        assert linear.issues_in_state.await_args_list == [
            call("ENG", "Todo", None),
            call("ENG", "Waiting", None),
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_state", ["Todo", "Waiting"])
async def test_ready_or_waiting_scan_failure_aborts_scan(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    failed_state: str,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])

        async def _issues_in_state(
            _team_key: str, state_name: str, _label: str | None
        ) -> list[LinearIssue]:
            if state_name == failed_state:
                raise LinearError(f"{state_name} failed")
            return []

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(side_effect=_issues_in_state)
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        with caplog.at_level(logging.WARNING):
            await _scan_and_wait(orch, binding)

        linear.move_issue.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
        assert "scan failed for ENG" in caplog.text
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state_type", "archived"),
    [("completed", False), ("canceled", False), ("started", True)],
)
async def test_auto_unblock_waiting_with_closed_or_archived_blocker_moves_to_ready(
    tmp_path: Path,
    state_type: str,
    archived: bool,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue(
            [_blocker("ENG-2", state_type, archived=archived)],
            state_id="state-waiting",
            state_name="Waiting",
        )
        linear = AsyncMock()
        linear.issues_in_state = _scan_results([], [issue])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        await _scan_and_wait(orch, binding)

        linear.issues_in_state.assert_has_awaits(
            [call("ENG", "Todo", None), call("ENG", "Waiting", None)]
        )
        linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
        linear.post_comment.assert_not_awaited()
        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
        assert orch._known_waiting_issue_ids == {"iss-1"}  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_unblock_waiting_with_open_blocker_stays_waiting(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue(
            [_blocker("ENG-2", "started"), _blocker("ENG-3", "completed")],
            state_id="state-waiting",
            state_name="Waiting",
        )
        linear = AsyncMock()
        linear.issues_in_state = _scan_results([], [issue])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        await _scan_and_wait(orch, binding)

        linear.move_issue.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_unblock_waiting_with_multiple_closed_blockers_moves_to_ready(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue = _issue(
            [
                _blocker("ENG-2", "completed"),
                _blocker("ENG-3", "canceled"),
                _blocker("ENG-4", "started", archived=True),
            ],
            state_id="state-waiting",
            state_name="Waiting",
        )
        linear = AsyncMock()
        linear.issues_in_state = _scan_results([], [issue])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        await _scan_and_wait(orch, binding)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
        linear.post_comment.assert_not_awaited()
        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_unblock_move_failure_logs_and_continues(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        first = _issue(
            [_blocker("ENG-10", "completed")],
            id="iss-1",
            identifier="ENG-1",
            state_id="state-waiting",
            state_name="Waiting",
        )
        second = _issue(
            [_blocker("ENG-20", "completed")],
            id="iss-2",
            identifier="ENG-2",
            state_id="state-waiting",
            state_name="Waiting",
        )
        linear = AsyncMock()
        linear.issues_in_state = _scan_results([], [first, second])
        linear.move_issue = AsyncMock(side_effect=[LinearError("move failed"), None])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        with caplog.at_level(logging.WARNING):
            await _scan_and_wait(orch, binding)

        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-todo"),
            call("iss-2", "state-todo"),
        ]
        linear.post_comment.assert_not_awaited()
        orch._dispatch_one.assert_not_awaited()  # noqa: SLF001
        assert "could not auto-unblock ENG-1 to Ready" in caplog.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_unblock_cascade_chain_across_ticks(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(repos=[binding])
        issue15_ready_blocked = _issue(
            [_blocker("VIB-14", "unstarted")],
            id="iss-15",
            identifier="VIB-15",
        )
        issue14_waiting_blocked = _issue(
            [_blocker("VIB-13", "started")],
            id="iss-14",
            identifier="VIB-14",
            state_id="state-waiting",
            state_name="Waiting",
        )
        issue14_waiting_unblocked = _issue(
            [_blocker("VIB-13", "completed")],
            id="iss-14",
            identifier="VIB-14",
            state_id="state-waiting",
            state_name="Waiting",
        )
        issue14_ready = _issue(
            [_blocker("VIB-13", "completed")],
            id="iss-14",
            identifier="VIB-14",
        )
        issue15_waiting_blocked = _issue(
            [_blocker("VIB-14", "started")],
            id="iss-15",
            identifier="VIB-15",
            state_id="state-waiting",
            state_name="Waiting",
        )
        issue15_waiting_unblocked = _issue(
            [_blocker("VIB-14", "completed")],
            id="iss-15",
            identifier="VIB-15",
            state_id="state-waiting",
            state_name="Waiting",
        )
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(
            side_effect=[
                [issue15_ready_blocked],
                [issue14_waiting_blocked],
                [],
                [issue14_waiting_unblocked],
                [issue14_ready],
                [issue15_waiting_blocked],
                [],
                [issue15_waiting_unblocked],
            ]
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.lookup_issue = AsyncMock(return_value=issue14_ready)

        orch = _make_orch(cfg, linear, conn)

        await _scan_and_wait(orch, binding)
        await _scan_and_wait(orch, binding)
        await _scan_and_wait(orch, binding)
        await _scan_and_wait(orch, binding)

        assert linear.move_issue.await_args_list == [
            call("iss-15", "state-waiting"),
            call("iss-14", "state-todo"),
            call("iss-15", "state-todo"),
        ]
        linear.post_comment.assert_awaited_once()
        orch._dispatch_one.assert_awaited_once_with(binding, issue14_ready)  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_only_opted_in_binding_scans_waiting_on_same_team(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        waiting_binding = _binding()
        plain_binding = _binding(waiting=None)
        cfg = Config(repos=[waiting_binding, plain_binding])
        linear = AsyncMock()
        linear.issues_in_state = _scan_results([], [])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        await _scan_and_wait(orch, waiting_binding)

        assert linear.issues_in_state.await_args_list == [
            call("ENG", "Todo", None),
            call("ENG", "Waiting", None),
        ]

        linear.issues_in_state = AsyncMock(return_value=[])
        await _scan_and_wait(orch, plain_binding)

        linear.issues_in_state.assert_awaited_once_with("ENG", "Todo", None)
    finally:
        await conn.close()
