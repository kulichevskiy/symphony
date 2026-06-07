from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from symphony import db
from symphony.agent.activity import (
    ActivityEvent,
    ActivitySession,
    ActivitySettings,
    format_activity_digest,
    parse_codex_activity_line,
)
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.pipeline.cost_guard import UsageDelta


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for event in self.events:
            yield event

    async def kill(self, run_id: str) -> None:
        pass


class _Clock:
    def __init__(self, times: list[datetime]) -> None:
        self.times = times
        self.index = 0

    def __call__(self) -> datetime:
        if self.index >= len(self.times):
            return self.times[-1]
        value = self.times[self.index]
        self.index += 1
        return value


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="codex",
        branch_prefix="symphony",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add activity comments",
        description="Need Codex activity reporting.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-progress",
        state_name="In Progress",
        state_type="started",
        team_key="ENG",
        labels=["symphony"],
    )


def _line(kind: str, item: dict[str, object]) -> str:
    return json.dumps({"type": kind, "item": item})


def test_parses_codex_command_and_file_activity(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    command_line = _line(
        "item.started",
        {
            "id": "cmd-1",
            "type": "command_execution",
            "command": ["python", str(workspace / "src/app.py"), "API_KEY=secret"],
        },
    )
    event = parse_codex_activity_line(command_line, workspace)
    assert event == ActivityEvent(
        kind="command_started",
        item_id="cmd-1",
        command="python ./src/app.py API_KEY=[redacted]",
    )

    file_line = _line(
        "item.completed",
        {
            "id": "file-1",
            "type": "file_change",
            "path": str(workspace / "src/symphony/activity.py"),
        },
    )
    file_event = parse_codex_activity_line(file_line, workspace)
    assert file_event == ActivityEvent(
        kind="file_changed",
        item_id="file-1",
        file_path="src/symphony/activity.py",
    )

    changes_line = _line(
        "item.completed",
        {
            "id": "file-2",
            "type": "file_change",
            "changes": [{"path": str(workspace / "tests/test_activity.py")}],
        },
    )
    changes_event = parse_codex_activity_line(changes_line, workspace)
    assert changes_event == ActivityEvent(
        kind="file_changed",
        item_id="file-2",
        file_path="tests/test_activity.py",
    )

    multi_changes_line = _line(
        "item.completed",
        {
            "id": "file-3",
            "type": "file_change",
            "changes": [
                {"path": str(workspace / "src/one.py")},
                {"path": str(workspace / "src/two.py")},
            ],
        },
    )
    multi_changes_event = parse_codex_activity_line(multi_changes_line, workspace)
    assert multi_changes_event == ActivityEvent(
        kind="file_changed",
        item_id="file-3",
        file_path="src/one.py",
        file_paths=("src/one.py", "src/two.py"),
    )

    legacy_line = _line(
        "item.started",
        {
            "id": "cmd-legacy",
            "item_type": "command_execution",
            "command": "pytest",
        },
    )
    legacy_event = parse_codex_activity_line(legacy_line, workspace)
    assert legacy_event == ActivityEvent(
        kind="command_started",
        item_id="cmd-legacy",
        command="pytest",
    )

    assert (
        parse_codex_activity_line(
            json.dumps({"type": "todo_list", "items": []}),
            workspace,
        )
        is None
    )


def test_activity_session_threshold_rate_limit_and_reset(tmp_path: Path) -> None:
    session = ActivitySession(
        settings=ActivitySettings(
            interval_secs=300,
            min_interval_secs=120,
            event_threshold=2,
        ),
        run_id="run-1",
        stage="implement",
        workspace_path=tmp_path,
    )
    start = datetime(2026, 5, 11, 10, 0, tzinfo=UTC)

    session.record_event(
        ActivityEvent(
            kind="command_started",
            item_id="cmd-1",
            command="uv run pytest",
        ),
        start,
    )
    assert session.due_reason(start + timedelta(seconds=119), last_posted_at=None) is None

    session.record_event(
        ActivityEvent(kind="file_changed", item_id="file-1", file_path="src/app.py"),
        start + timedelta(seconds=120),
    )
    assert session.due_reason(start + timedelta(seconds=120), last_posted_at=None) == "threshold"
    session.mark_published()
    assert session.has_unpublished_events() is False
    assert session.due_reason(start + timedelta(seconds=600), last_posted_at=start) is None

    session.record_event(
        ActivityEvent(kind="file_changed", item_id="file-2", file_path="src/next.py"),
        start + timedelta(seconds=130),
    )
    assert (
        session.due_reason(
            start + timedelta(seconds=419),
            last_posted_at=start + timedelta(seconds=120),
        )
        is None
    )
    assert (
        session.due_reason(
            start + timedelta(seconds=420),
            last_posted_at=start + timedelta(seconds=120),
        )
        is None
    )
    assert (
        session.due_reason(
            start + timedelta(seconds=430),
            last_posted_at=start + timedelta(seconds=120),
        )
        == "interval"
    )


def test_activity_session_interval_anchors_to_first_unpublished_event(
    tmp_path: Path,
) -> None:
    session = ActivitySession(
        settings=ActivitySettings(
            interval_secs=300,
            min_interval_secs=120,
            event_threshold=20,
        ),
        run_id="run-1",
        stage="implement",
        workspace_path=tmp_path,
    )
    start = datetime(2026, 5, 11, 10, 0, tzinfo=UTC)
    first_after_idle = start + timedelta(seconds=900)

    session.record_event(
        ActivityEvent(kind="file_changed", item_id="file-1", file_path="src/app.py"),
        first_after_idle,
    )

    assert (
        session.due_reason(
            first_after_idle + timedelta(seconds=1),
            last_posted_at=start,
        )
        is None
    )
    assert (
        session.due_reason(
            first_after_idle + timedelta(seconds=300),
            last_posted_at=start,
        )
        == "interval"
    )


def test_activity_session_long_running_heartbeat_repeats_by_command_id(
    tmp_path: Path,
) -> None:
    session = ActivitySession(
        settings=ActivitySettings(
            long_running_secs=300,
            long_running_repeat_secs=600,
        ),
        run_id="run-1",
        stage="implement",
        workspace_path=tmp_path,
    )
    start = datetime(2026, 5, 11, 10, 0, tzinfo=UTC)
    session.record_event(
        ActivityEvent(kind="command_started", item_id="cmd-1", command="npm test"),
        start,
    )

    assert (
        session.heartbeat_due_item_ids(
            start + timedelta(seconds=299),
            last_heartbeat_at_by_item={},
        )
        == ()
    )
    assert session.has_heartbeat_candidate(start + timedelta(seconds=299)) is False
    first_due = start + timedelta(seconds=300)
    assert session.has_heartbeat_candidate(first_due) is True
    assert session.heartbeat_due_item_ids(first_due, last_heartbeat_at_by_item={}) == ("cmd-1",)
    assert (
        session.heartbeat_due_item_ids(
            first_due + timedelta(seconds=599),
            last_heartbeat_at_by_item={"cmd-1": first_due},
        )
        == ()
    )
    assert session.heartbeat_due_item_ids(
        first_due + timedelta(seconds=600),
        last_heartbeat_at_by_item={"cmd-1": first_due},
    ) == ("cmd-1",)


def test_activity_digest_is_compact_sanitized_and_limited(tmp_path: Path) -> None:
    workspace = tmp_path / "ws"
    workspace.mkdir()
    session = ActivitySession(
        settings=ActivitySettings(include_failed_output_lines=2),
        run_id="run-1",
        stage="review_fix",
        workspace_path=workspace,
    )
    now = datetime(2026, 5, 11, 10, 0, tzinfo=UTC)
    session.record_event(
        ActivityEvent(kind="command_started", item_id="cmd-1", command="pytest"),
        now,
    )
    session.record_event(
        ActivityEvent(
            kind="command_completed",
            item_id="cmd-1",
            command="pytest",
            exit_code=1,
            output_lines=(
                f"failed at {workspace}/tests/test_app.py TOKEN=supersecret",
                "AssertionError: nope",
                "third line omitted",
            ),
        ),
        now + timedelta(seconds=10),
    )
    session.record_event(
        ActivityEvent(
            kind="file_changed",
            item_id="file-1",
            file_path="src/app.py",
            file_paths=("src/app.py", "tests/test_app.py"),
        ),
        now + timedelta(seconds=11),
    )

    body = format_activity_digest(
        session.build_digest(
            reason="final",
            now=now + timedelta(seconds=12),
            input_tokens=1200,
            output_tokens=340,
            cache_write_tokens=50,
            cache_read_tokens=10,
        )
    )

    assert "Run ID: `run-1`" in body
    assert "Review Fix" in body
    assert "Completed commands: **1**" in body
    assert "`pytest` exited `1`" in body
    assert "TOKEN=[redacted]" in body
    assert "AssertionError: nope" in body
    assert "third line omitted" not in body
    assert "src/app.py" in body
    assert "tests/test_app.py" in body
    assert str(workspace) not in body
    assert "supersecret" not in body
    assert "$" not in body
    assert "Tokens: in 1200 · out 340 · cache w 50 / r 10 · total 1600" in body


@pytest.mark.asyncio
async def test_activity_comment_marks_persist_publish_and_heartbeat_state(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Activity",
            team_key="ENG",
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-11T10:00:00+00:00",
        )

        await db.activity_comments.record_event(
            conn,
            run_id="run-1",
            occurred_at="2026-05-11T10:00:01+00:00",
        )
        await db.activity_comments.record_event(
            conn,
            run_id="run-1",
            occurred_at="2026-05-11T10:00:02+00:00",
        )
        mark = await db.activity_comments.get(conn, "run-1")
        assert mark is not None
        assert mark.event_count_since_post == 2
        assert mark.first_unpublished_at == "2026-05-11T10:00:01+00:00"

        await db.activity_comments.mark_published(
            conn,
            run_id="run-1",
            posted_at="2026-05-11T10:05:00+00:00",
            fingerprint="abc123",
        )
        mark = await db.activity_comments.get(conn, "run-1")
        assert mark is not None
        assert mark.event_count_since_post == 0
        assert mark.first_unpublished_at is None
        assert mark.last_fingerprint == "abc123"

        await db.activity_comments.mark_heartbeat(
            conn,
            run_id="run-1",
            item_id="cmd-1",
            posted_at="2026-05-11T10:05:00+00:00",
        )
        assert (
            await db.activity_comments.last_heartbeat_at(
                conn,
                run_id="run-1",
                item_id="cmd-1",
            )
            == "2026-05-11T10:05:00+00:00"
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_orchestrator_final_flushes_unpublished_activity_events(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=issue.id,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-11T10:00:00+00:00",
        )
        workspace = tmp_path / "ws"
        workspace.mkdir()
        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=123),
                RunnerEvent(
                    kind="stdout",
                    line=_line(
                        "item.started",
                        {
                            "id": "cmd-1",
                            "type": "command_execution",
                            "command": "uv run pytest",
                        },
                    ),
                ),
                RunnerEvent(
                    kind="stdout",
                    line=_line(
                        "item.completed",
                        {
                            "id": "file-1",
                            "type": "file_change",
                            "path": str(workspace / "src/app.py"),
                        },
                    ),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "workspaces",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(cfg, linear, conn, runner=runner)

        await orch._run_stage_command(  # noqa: SLF001
            binding=_binding(),
            issue=issue,
            command=["codex"],
            run_id="run-1",
            workspace_path=workspace,
            stage="implement",
            prior_total=1.0,
        )

        linear.post_comment.assert_awaited_once()
        body = linear.post_comment.await_args.args[1]
        assert "Activity digest" in body
        assert "Run ID: `run-1`" in body
        assert "Running commands: `uv run pytest`" in body
        assert "`src/app.py`" in body
        mark = await db.activity_comments.get(conn, "run-1")
        assert mark is not None
        assert mark.event_count_since_post == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_orchestrator_posts_long_running_heartbeat_without_new_output(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=issue.id,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-11T10:00:00+00:00",
        )
        workspace = tmp_path / "ws"
        workspace.mkdir()
        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=123),
                RunnerEvent(
                    kind="stdout",
                    line=_line(
                        "item.started",
                        {
                            "id": "cmd-1",
                            "type": "command_execution",
                            "command": "npm test",
                        },
                    ),
                ),
                RunnerEvent(kind="tick"),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "workspaces",
            db_path=tmp_path / "s.sqlite",
            activity_comment_interval_secs=3600,
            activity_comment_event_threshold=99,
            activity_comment_long_running_secs=300,
        )
        start = datetime(2026, 5, 11, 10, 0, tzinfo=UTC)
        clock = _Clock(
            [
                start,
                start + timedelta(seconds=301),
                start + timedelta(seconds=302),
            ]
        )
        orch = Orchestrator(cfg, linear, conn, runner=runner, clock=clock)

        await orch._run_stage_command(  # noqa: SLF001
            binding=_binding(),
            issue=issue,
            command=["codex"],
            run_id="run-1",
            workspace_path=workspace,
            stage="implement",
            prior_total=0.0,
        )

        linear.post_comment.assert_awaited_once()
        body = linear.post_comment.await_args.args[1]
        assert "Running commands: `npm test` (5m 1s)" in body
        assert (
            await db.activity_comments.last_heartbeat_at(
                conn,
                run_id="run-1",
                item_id="cmd-1",
            )
            == "2026-05-11T10:05:01+00:00"
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_orchestrator_tick_skips_heartbeat_db_before_candidate(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        start = datetime(2026, 5, 11, 10, 0, tzinfo=UTC)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "workspaces",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            clock=lambda: start + timedelta(seconds=10),
        )
        session = ActivitySession(
            settings=ActivitySettings(long_running_secs=300),
            run_id="run-1",
            stage="implement",
            workspace_path=tmp_path,
        )
        heartbeat_marks = AsyncMock(return_value={})
        monkeypatch.setattr(db.activity_comments, "heartbeat_marks", heartbeat_marks)

        await orch._record_activity_tick(  # noqa: SLF001
            session=session,
            binding=_binding(),
            issue=_issue(),
            cumulative_usage=UsageDelta(),
        )
        heartbeat_marks.assert_not_awaited()

        session.record_event(
            ActivityEvent(kind="command_started", item_id="cmd-1", command="pytest"),
            start,
        )
        await orch._record_activity_tick(  # noqa: SLF001
            session=session,
            binding=_binding(),
            issue=_issue(),
            cumulative_usage=UsageDelta(),
        )
        heartbeat_marks.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_orchestrator_tick_caches_heartbeat_marks_between_repeats(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        start = datetime(2026, 5, 11, 10, 0, tzinfo=UTC)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "workspaces",
            db_path=tmp_path / "s.sqlite",
        )
        clock = _Clock(
            [
                start + timedelta(seconds=301),
                start + timedelta(seconds=302),
            ]
        )
        linear = AsyncMock()
        linear.post_comment = AsyncMock()
        orch = Orchestrator(cfg, linear, conn, clock=clock)
        session = ActivitySession(
            settings=ActivitySettings(long_running_secs=300, long_running_repeat_secs=600),
            run_id="run-1",
            stage="implement",
            workspace_path=tmp_path,
        )
        session.record_event(
            ActivityEvent(kind="command_started", item_id="cmd-1", command="pytest"),
            start,
        )
        heartbeat_marks = AsyncMock(
            return_value={"cmd-1": "2026-05-11T10:05:00+00:00"}
        )
        monkeypatch.setattr(db.activity_comments, "heartbeat_marks", heartbeat_marks)

        await orch._record_activity_tick(  # noqa: SLF001
            session=session,
            binding=_binding(),
            issue=_issue(),
            cumulative_usage=UsageDelta(),
        )
        await orch._record_activity_tick(  # noqa: SLF001
            session=session,
            binding=_binding(),
            issue=_issue(),
            cumulative_usage=UsageDelta(),
        )

        heartbeat_marks.assert_awaited_once()
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()
