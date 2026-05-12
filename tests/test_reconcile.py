"""Startup reconciliation: dead-PID runs flip to `interrupted` and we
post a Linear comment telling the user to `$retry`."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from symphony import db
from symphony.orchestrator.reconcile import reconcile


@pytest.mark.asyncio
async def test_reconcile_marks_dead_pids_interrupted_and_comments(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-alive", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.issues.upsert(
            conn, id="iss-dead", identifier="ENG-2", title="t", team_key="ENG"
        )

        # Live PID — current python process. Reconcile must NOT touch this row.
        await db.runs.create(
            conn,
            id="alive",
            issue_id="iss-alive",
            stage="implement",
            status="running",
            pid=os.getpid(),
            started_at="2026-05-10T00:00:00+00:00",
        )
        # Almost-certainly-dead PID. macOS PIDs cap at 99998 by default and the
        # value would have to be in-use right now AND owned by us to fool kill(0).
        dead_pid = 999_999
        await db.runs.create(
            conn,
            id="dead",
            issue_id="iss-dead",
            stage="implement",
            status="running",
            pid=dead_pid,
            started_at="2026-05-10T00:00:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        flipped = await reconcile(conn, linear)
        assert flipped == 1

        linear.post_comment.assert_awaited_once()
        call = linear.post_comment.await_args
        assert call is not None
        # First positional arg is the issue UUID; second is the body.
        assert call.args[0] == "iss-dead"
        body = call.args[1]
        assert "$retry" in body

        # Live row stays live; dead row no longer appears as live.
        rows = await db.runs.list_live_with_pid(conn)
        assert [r.id for r in rows] == ["alive"]

        cur = await conn.execute("SELECT status FROM runs WHERE id=?", ("dead",))
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == "interrupted"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_treats_eperm_pid_as_alive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PID owned by another user/session raises PermissionError from
    `os.kill(pid, 0)`. That means the process exists — reconcile must NOT
    flip the run to `interrupted`, otherwise we'd invite `$retry` while a
    real worker is still running and risk duplicate execution."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-foreign", identifier="ENG-3", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="foreign",
            issue_id="iss-foreign",
            stage="implement",
            status="running",
            pid=4242,
            started_at="2026-05-10T00:00:00+00:00",
        )

        def fake_kill(pid: int, sig: int) -> None:
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(os, "kill", fake_kill)

        linear = AsyncMock()
        linear.post_comment = AsyncMock()
        flipped = await reconcile(conn, linear)

        assert flipped == 0
        linear.post_comment.assert_not_awaited()
        cur = await conn.execute("SELECT status FROM runs WHERE id=?", ("foreign",))
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == "running"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_treats_unexpected_oserror_as_alive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`os.kill(pid, 0)` can raise OSErrors other than ProcessLookupError /
    PermissionError — `EINVAL` for a bad PID value, plus platform-specific
    quirks. Reconcile runs at startup, so letting those propagate would
    prevent the orchestrator from booting. Treat as alive and continue."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-weird", identifier="ENG-4", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="weird",
            issue_id="iss-weird",
            stage="implement",
            status="running",
            pid=123,
            started_at="2026-05-10T00:00:00+00:00",
        )

        def fake_kill(pid: int, sig: int) -> None:
            raise OSError(22, "Invalid argument")  # EINVAL

        monkeypatch.setattr(os, "kill", fake_kill)

        linear = AsyncMock()
        linear.post_comment = AsyncMock()
        flipped = await reconcile(conn, linear)

        assert flipped == 0
        linear.post_comment.assert_not_awaited()
        cur = await conn.execute("SELECT status FROM runs WHERE id=?", ("weird",))
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == "running"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_no_live_runs_is_a_noop(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        linear = AsyncMock()
        linear.post_comment = AsyncMock()
        flipped = await reconcile(conn, linear)
        assert flipped == 0
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()
