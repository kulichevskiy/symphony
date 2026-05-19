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
async def test_reconcile_marks_pidless_live_review_runs_interrupted_and_comments(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-review", identifier="ENG-5", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="pidless-review",
            issue_id="iss-review",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.issues.upsert(
            conn,
            id="iss-implement",
            identifier="ENG-6",
            title="t",
            team_key="ENG",
        )
        await db.runs.create(
            conn,
            id="pidless-implement",
            issue_id="iss-implement",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        flipped = await reconcile(conn, linear)
        assert flipped == 1

        linear.post_comment.assert_awaited_once()
        call = linear.post_comment.await_args
        assert call is not None
        assert call.args[0] == "iss-review"
        assert "Host restarted" in call.args[1]

        cur = await conn.execute(
            "SELECT status, ended_at FROM runs WHERE id=?", ("pidless-review",)
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == db.runs.INTERRUPTED_STATUS
        assert row[1] is not None

        cur = await conn.execute(
            "SELECT status, ended_at FROM runs WHERE id=?", ("pidless-implement",)
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == db.runs.LIVE_STATUSES[0]
        assert row[1] is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_preserves_retry_for_pidless_review_without_issue_pr(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id="iss-review",
            identifier="ENG-5",
            title="t",
            team_key="ENG",
        )
        await db.review_state.begin_review(
            conn,
            "iss-review",
            pr_number=None,
            pr_url="not-a-github-pr-url",
            github_repo="org/repo",
            issue_label="backend",
        )
        await db.runs.create(
            conn,
            id="pidless-review",
            issue_id="iss-review",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        flipped = await reconcile(conn, linear)
        assert flipped == 1

        wait = await db.operator_waits.get(conn, "iss-review")
        assert wait is not None
        assert wait.run_id == "pidless-review"
        assert wait.kind == db.operator_waits.KIND_REVIEW_FAILED
        assert wait.linear_team_key == "ENG"
        assert wait.github_repo == "org/repo"
        assert wait.issue_label == "backend"

        linear.post_comment.assert_awaited_once()
        body = linear.post_comment.await_args.args[1]
        assert "$retry" in body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_ignores_stale_issue_pr_for_pidless_review_retry(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id="iss-review",
            identifier="ENG-7",
            title="t",
            team_key="ENG",
        )
        await db.review_state.begin_review(
            conn,
            "iss-review",
            pr_number=None,
            pr_url="not-a-github-pr-url",
            github_repo="org/repo",
            issue_label="backend",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-review",
            github_repo="org/repo",
            pr_number=41,
            pr_url="https://github.com/org/repo/pull/41",
            created_at="2026-05-09T00:00:00+00:00",
        )
        await db.issue_prs.mark_merged(
            conn,
            issue_id="iss-review",
            github_repo="org/repo",
            merged_at="2026-05-09T01:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="pidless-review",
            issue_id="iss-review",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        flipped = await reconcile(conn, linear)
        assert flipped == 1

        cur = await conn.execute(
            "SELECT status, ended_at FROM runs WHERE id=?", ("pidless-review",)
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == db.runs.INTERRUPTED_STATUS
        assert row[1] is not None

        wait = await db.operator_waits.get(conn, "iss-review")
        assert wait is not None
        assert wait.run_id == "pidless-review"
        assert wait.kind == db.operator_waits.KIND_REVIEW_FAILED

        linear.post_comment.assert_awaited_once()
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
