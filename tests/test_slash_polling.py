"""Slash-command polling tests.

Issue #10: on every poll tick the orchestrator fetches `comments_since`
for each active run, dispatches intents (e.g. `/stop` kills the runner),
and persists the cursor so a restart does not re-fire old commands.

Filter regressions (self-author, externalThread) are pure-function tested
in `test_slash.py`; here we re-verify them through the orchestrator wiring
to lock the end-to-end path.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearComment, LinearIssue
from symphony.orchestrator.poll import Orchestrator


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo"),
    )


def _issue(uid: str = "iss-1", ident: str = "ENG-1") -> LinearIssue:
    return LinearIssue(
        id=uid,
        identifier=ident,
        title="t",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=[],
    )


def _comment(
    body: str,
    *,
    cid: str = "c1",
    created_at: str = "2026-05-10T12:00:00+00:00",
    is_me: bool = False,
    external_thread_type: str | None = None,
) -> LinearComment:
    return LinearComment(
        id=cid,
        body=body,
        created_at=created_at,
        author_name="user",
        author_is_me=is_me,
        external_thread_type=external_thread_type,
    )


def _make_orch(cfg: Config, linear: AsyncMock, conn: object) -> Orchestrator:
    runner = MagicMock()
    runner.kill = AsyncMock()
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=Path("/dev/null"))
    workspace.release = MagicMock()
    gh = MagicMock()
    push_fn = AsyncMock()
    orch = Orchestrator(
        cfg,
        linear,
        conn,  # type: ignore[arg-type]
        runner=runner,
        gh=gh,
        workspace=workspace,
        push_fn=push_fn,
    )
    orch._states = {  # noqa: SLF001
        "ENG": {
            "Todo": "state-todo",
            "In Progress": "state-progress",
            "Blocked": "state-blocked",
        }
    }
    return orch


async def _seed_active_run(conn: object, *, issue_id: str, run_id: str) -> None:
    await db.issues.upsert(
        conn,  # type: ignore[arg-type]
        id=issue_id,
        identifier="ENG-1",
        title="t",
        team_key="ENG",
    )
    await db.runs.create(
        conn,  # type: ignore[arg-type]
        id=run_id,
        issue_id=issue_id,
        stage="implement",
        status="running",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )


@pytest.mark.asyncio
async def test_stop_intent_kills_active_runner(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("/stop")])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._runner.kill.assert_awaited_once_with("run-1")  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cursor_persisted_after_fetch(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(
            return_value=[
                _comment("noise", cid="c1", created_at="2026-05-10T11:00:00+00:00"),
                _comment("/stop", cid="c2", created_at="2026-05-10T12:00:00+00:00"),
            ]
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        cursor = await db.comment_cursors.get(conn, "iss-1")
        assert cursor == ("2026-05-10T12:00:00+00:00", ["c2"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_boundary_tied_comment_not_double_fired(tmp_path: Path) -> None:
    """Tick 1 sees one comment at T; tick 2 re-fetches it (gte) and must
    drop it via the cursor's boundary-id set rather than re-firing."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        boundary = "2026-05-10T12:00:00+00:00"
        linear.comments_since = AsyncMock(
            return_value=[_comment("/stop", cid="c1", created_at=boundary)]
        )
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001
        assert orch._runner.kill.await_count == 1  # type: ignore[attr-defined]  # noqa: SLF001

        # Tick 2 — gte returns the same comment plus a new same-timestamp
        # comment that wasn't visible on tick 1 (e.g. pagination split).
        # The already-handled c1 must be deduped; only c2 should fire.
        linear.comments_since.return_value = [
            _comment("/stop", cid="c1", created_at=boundary),
            _comment("/stop", cid="c2", created_at=boundary),
        ]
        await orch._poll_slash_commands()  # noqa: SLF001
        # One additional kill — for c2 only, not c1.
        assert orch._runner.kill.await_count == 2  # type: ignore[attr-defined]  # noqa: SLF001

        cursor = await db.comment_cursors.get(conn, "iss-1")
        assert cursor is not None
        last_at, last_ids = cursor
        assert last_at == boundary
        assert sorted(last_ids) == ["c1", "c2"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_cursor_clamped_to_run_start(tmp_path: Path) -> None:
    """A stale `/stop` posted between runs (after run A ended, before run B
    started) must NOT be replayed against run B."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        # Seed a stored cursor from run A that predates run B's start.
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.comment_cursors.set(
            conn, "iss-1", "2026-05-10T08:00:00+00:00", ["old"]
        )
        # Run B starts at T2; the stale /stop sits between cursor and run start.
        await db.runs.create(
            conn,
            id="run-b",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T10:00:00+00:00",
        )
        orch._active_run_ids.add("run-b")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-b"  # noqa: SLF001

        # Linear is queried with `after >= run_started`; assert that here, and
        # return only stale comments before run_started to confirm they would
        # be filtered out by the API. We assert the after timestamp is run B's
        # start, not the stored (older) cursor.
        linear.comments_since = AsyncMock(return_value=[])

        await orch._poll_slash_commands()  # noqa: SLF001

        assert linear.comments_since.await_count == 1
        after_arg = linear.comments_since.await_args.args[1]
        assert after_arg.isoformat() == "2026-05-10T10:00:00+00:00"
        orch._runner.kill.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cursor_boundary_uses_datetime_order(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(
            return_value=[
                _comment(
                    "noise",
                    cid="offset",
                    created_at="2026-05-10T12:30:00+01:00",
                ),
                _comment("noise", cid="utc", created_at="2026-05-10T12:00:00Z"),
            ]
        )
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        cursor = await db.comment_cursors.get(conn, "iss-1")
        assert cursor == ("2026-05-10T12:00:00Z", ["utc"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_handler_failure_does_not_advance_cursor(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("/stop", cid="c1")])
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        orch._handle_slash_intent = AsyncMock(  # type: ignore[method-assign]
            side_effect=RuntimeError("boom")
        )
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        with pytest.raises(RuntimeError, match="boom"):
            await orch._poll_slash_commands()  # noqa: SLF001

        assert await db.comment_cursors.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_self_authored_stop_is_ignored(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(
            return_value=[_comment("/stop", is_me=True)]
        )
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._runner.kill.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_mirrored_from_github_stop_is_ignored(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(
            return_value=[
                _comment("/stop", external_thread_type="githubPullRequest")
            ]
        )
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._runner.kill.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cursor_advances_across_ticks(tmp_path: Path) -> None:
    """Second tick must pass the persisted cursor to `comments_since` so old
    comments are not re-fetched after an orchestrator restart."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(
            return_value=[
                _comment("noise", created_at="2026-05-10T11:00:00+00:00"),
            ]
        )
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001
        first_after = linear.comments_since.await_args_list[0].args[1]

        linear.comments_since.return_value = []
        await orch._poll_slash_commands()  # noqa: SLF001
        second_after = linear.comments_since.await_args_list[1].args[1]

        assert isinstance(first_after, datetime)
        assert isinstance(second_after, datetime)
        assert second_after > first_after
        # Cursor was advanced to the most recent observed comment.
        assert second_after.isoformat() == "2026-05-10T11:00:00+00:00"
    finally:
        await conn.close()
