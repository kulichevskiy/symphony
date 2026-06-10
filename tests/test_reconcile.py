"""Startup reconciliation: dead-PID runs flip to `interrupted` and we
post a Linear comment telling the user to `$retry`."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from symphony import db
from symphony.config import LinearStates, RepoBinding
from symphony.linear.client import LinearError
from symphony.orchestrator.reconcile import reconcile
from symphony.tracker import TrackerContext


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

        cur = await conn.execute(
            """
            SELECT status, termination_kind, termination_detail
            FROM runs WHERE id=?
            """,
            ("dead",),
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == "interrupted"
        assert row["termination_kind"] == "orphaned"
        assert "pid 999999" in row["termination_detail"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_posts_comment_through_persisted_tracker_context(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id="iss-secondary",
            identifier="ALT-2",
            title="t",
            team_key="ALT",
            provider="linear-alt",
            site="secondary",
        )
        await db.runs.create(
            conn,
            id="dead-secondary",
            issue_id="iss-secondary",
            stage="implement",
            status="running",
            pid=999_999,
            started_at="2026-05-10T00:00:00+00:00",
        )

        default_tracker = AsyncMock()
        default_tracker.post_comment = AsyncMock(
            side_effect=AssertionError("default tracker used")
        )
        secondary_tracker = AsyncMock()
        secondary_tracker.post_comment = AsyncMock(return_value="cmt-1")
        contexts: list[TrackerContext] = []

        def tracker(ctx: TrackerContext) -> AsyncMock:
            contexts.append(ctx)
            if ctx == TrackerContext(provider="linear-alt", site="secondary"):
                return secondary_tracker
            return default_tracker

        flipped = await reconcile(conn, tracker)

        assert flipped == 1
        assert contexts == [TrackerContext(provider="linear-alt", site="secondary")]
        secondary_tracker.post_comment.assert_awaited_once()
        call = secondary_tracker.post_comment.await_args
        assert call is not None
        assert call.args[0] == "iss-secondary"
        default_tracker.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_posts_comment_with_tracker_issue_id_for_scoped_issue(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id="shared-issue-id",
            identifier="ENG-1",
            title="Default issue",
            team_key="ENG",
            provider="linear",
            site="default",
        )
        scoped_issue_id = await db.issues.upsert(
            conn,
            id="shared-issue-id",
            identifier="ALT-2",
            title="Secondary issue",
            team_key="ALT",
            provider="linear-alt",
            site="secondary",
        )
        await db.runs.create(
            conn,
            id="dead-secondary",
            issue_id=scoped_issue_id,
            stage="implement",
            status="running",
            pid=999_999,
            started_at="2026-05-10T00:00:00+00:00",
        )

        default_tracker = AsyncMock()
        default_tracker.post_comment = AsyncMock(
            side_effect=AssertionError("default tracker used")
        )
        secondary_tracker = AsyncMock()
        secondary_tracker.post_comment = AsyncMock(return_value="cmt-1")

        def tracker(ctx: TrackerContext) -> AsyncMock:
            if ctx == TrackerContext(provider="linear-alt", site="secondary"):
                return secondary_tracker
            return default_tracker

        flipped = await reconcile(conn, tracker)

        assert flipped == 1
        secondary_tracker.post_comment.assert_awaited_once()
        call = secondary_tracker.post_comment.await_args
        assert call is not None
        assert call.args[0] == "shared-issue-id"
        assert call.args[0] != scoped_issue_id
        default_tracker.post_comment.assert_not_awaited()
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
            """
            SELECT status, ended_at, termination_kind, termination_detail
            FROM runs WHERE id=?
            """,
            ("pidless-review",),
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == db.runs.INTERRUPTED_STATUS
        assert row[1] is not None
        assert row["termination_kind"] == "orphaned"
        assert "pidless review" in row["termination_detail"]

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
async def test_reconcile_recovers_pidless_local_review_run(tmp_path: Path) -> None:
    """A `local_review` run is in-process (no PID) and lives at stage
    `local_review`, so neither pid sweep nor the `review`-only pidless sweep
    catches it. Reconcile must flip it `interrupted` AND re-dispatch the issue
    from `ready` — the automated equivalent of the manual Todo move that was
    needed before — so the issue is not wedged in "Local Code Review"."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-local", identifier="ENG-9", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="pidless-local-review",
            issue_id="iss-local",
            stage="local_review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            agent="codex",
            branch_prefix="symphony",
            linear_states=LinearStates(ready="Todo"),
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.team_states = AsyncMock(return_value={"Todo": "state-ready"})
        linear.move_issue = AsyncMock()

        flipped = await reconcile(conn, linear, bindings=[binding])
        assert flipped == 1

        # Row flips to interrupted with an ended_at and orphaned kind.
        cur = await conn.execute(
            """
            SELECT status, ended_at, termination_kind, termination_detail
            FROM runs WHERE id=?
            """,
            ("pidless-local-review",),
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == db.runs.INTERRUPTED_STATUS
        assert row[1] is not None
        assert row["termination_kind"] == "orphaned"
        assert "local review" in row["termination_detail"]

        # Recovery path triggered: the issue is moved back to the ready state
        # so the next poll re-dispatches a fresh implement.
        linear.move_issue.assert_awaited_once_with("iss-local", "state-ready")

        # Host-restart comment posted. It must NOT tell the operator to reply
        # `$retry` — a local_review orphan has no retry handler, so re-dispatch
        # is automatic and the comment says so.
        linear.post_comment.assert_awaited_once()
        body = linear.post_comment.await_args.args[1]
        assert "Host restarted" in body
        assert "$retry" not in body
        assert "No action needed" in body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_leaves_local_review_live_when_move_fails(
    tmp_path: Path,
) -> None:
    """If `move_issue` raises (flaky Linear call at startup), the run must stay
    live — NOT flipped to `interrupted` — so a later reconcile retries it on
    the still-live row. Flipping it now would strand the issue in "Local Code
    Review" with no live run and no working `$retry` handler: exactly the bug
    this PR exists to rescue would be re-introduced by a single flaky call."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-local", identifier="ENG-9", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="pidless-local-review",
            issue_id="iss-local",
            stage="local_review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            agent="codex",
            branch_prefix="symphony",
            linear_states=LinearStates(ready="Todo"),
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.team_states = AsyncMock(return_value={"Todo": "state-ready"})
        linear.move_issue = AsyncMock(side_effect=LinearError("flaky move"))

        flipped = await reconcile(conn, linear, bindings=[binding])
        assert flipped == 0

        # Run stays live so a later reconcile retries it.
        rows = await db.runs.list_live_local_review_without_pid(conn)
        assert [r.id for r in rows] == ["pidless-local-review"]
        cur = await conn.execute(
            "SELECT status, ended_at FROM runs WHERE id=?", ("pidless-local-review",)
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == "running"
        assert row[1] is None

        # No misleading comment posted, and no half-finished operator-wait.
        linear.post_comment.assert_not_awaited()
        assert await db.operator_waits.get(conn, "iss-local") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_recovers_vib198_implement_plus_local_review(
    tmp_path: Path,
) -> None:
    """VIB-198 layout: a pidless `local_review` run coexists with its
    pid-bearing `implement` run on the same issue. The pid sweep flips the
    dead-pid implement run and comments; the local_review sweep re-dispatches
    and comments — two "Host restarted" comments on the one issue, both runs
    interrupted, the issue moved back to ready exactly once."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-local", identifier="ENG-9", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="implement",
            issue_id="iss-local",
            stage="implement",
            status="running",
            pid=999_999,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="local-review",
            issue_id="iss-local",
            stage="local_review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )

        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            agent="codex",
            branch_prefix="symphony",
            linear_states=LinearStates(ready="Todo"),
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.team_states = AsyncMock(return_value={"Todo": "state-ready"})
        linear.move_issue = AsyncMock()

        flipped = await reconcile(conn, linear, bindings=[binding])
        assert flipped == 2

        # Both runs flipped interrupted.
        cur = await conn.execute(
            "SELECT id, status FROM runs WHERE issue_id=? ORDER BY id", ("iss-local",)
        )
        statuses = {r["id"]: r["status"] for r in await cur.fetchall()}
        assert statuses == {
            "implement": db.runs.INTERRUPTED_STATUS,
            "local-review": db.runs.INTERRUPTED_STATUS,
        }

        # The issue is moved back to ready once — by the local_review sweep.
        linear.move_issue.assert_awaited_once_with("iss-local", "state-ready")

        # Two host-restart comments land on the one issue (pid sweep + this one).
        assert linear.post_comment.await_count == 2
        for call in linear.post_comment.await_args_list:
            assert call.args[0] == "iss-local"
            assert "Host restarted" in call.args[1]
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
