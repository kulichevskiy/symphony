"""Startup reconciliation: dead-PID runs flip to `interrupted` and we
post a Linear comment telling the user to `/retry`."""

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
        assert "/retry" in body

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
