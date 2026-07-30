"""Startup reconciliation: dead-PID runs flip to `interrupted` and we
post a Linear comment telling the user to `$retry`."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from symphony import db
from symphony.config import LinearStates, RepoBinding
from symphony.linear.client import LinearError
from symphony.orchestrator.reconcile import reconcile
from symphony.tracker import TrackerContext, TrackerRegistry


@pytest.mark.asyncio
async def test_reconcile_marks_dead_pids_interrupted_and_comments(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(conn, id="iss-alive", identifier="ENG-1", title="t", team_key="ENG")
        await db.issues.upsert(conn, id="iss-dead", identifier="ENG-2", title="t", team_key="ENG")

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
        default_tracker.post_comment = AsyncMock(side_effect=AssertionError("default tracker used"))
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
        default_tracker.post_comment = AsyncMock(side_effect=AssertionError("default tracker used"))
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
        await db.issues.upsert(conn, id="iss-review", identifier="ENG-5", title="t", team_key="ENG")
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
async def test_reconcile_supersedes_pidless_review_on_merged_pr_without_minting_wait(
    tmp_path: Path,
) -> None:
    """SYM-231: a pidless review monitor whose own PR merged out-of-band must be
    superseded by the merged-PR sweep, not re-parked by
    `_preserve_pidless_review_retry_path` — the pidless-review sweep runs after
    the merged-PR sweep specifically so this row is gone by the time it looks,
    and re-manufacturing a `review_failed` wait here is exactly the residue
    SYM-231 exists to remove.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T02:00:00+00:00"
    try:
        await db.issues.upsert(
            conn,
            id="iss-review",
            identifier="ENG-8",
            title="t",
            team_key="ENG",
        )
        await db.review_state.begin_review(
            conn,
            "iss-review",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label="backend",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-review",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-review",
            github_repo="org/repo",
            pr_number=42,
            merged_at=merged_at,
        )
        await db.runs.create(
            conn,
            id="pidless-review",
            issue_id="iss-review",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:30:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        flipped = await reconcile(conn, linear)
        assert flipped == 1

        cur = await conn.execute(
            "SELECT status, termination_kind FROM runs WHERE id=?", ("pidless-review",)
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == db.runs.SUPERSEDED_STATUS
        assert row["termination_kind"] == "pr_merged"

        assert await db.operator_waits.get(conn, "iss-review") is None
        linear.post_comment.assert_not_awaited()
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
        await db.issues.upsert(conn, id="iss-local", identifier="ENG-9", title="t", team_key="ENG")
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
        await db.issues.upsert(conn, id="iss-local", identifier="ENG-9", title="t", team_key="ENG")
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
        await db.issues.upsert(conn, id="iss-local", identifier="ENG-9", title="t", team_key="ENG")
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
async def test_reconcile_redispatches_jira_local_review_with_two_projects(
    tmp_path: Path,
) -> None:
    """Two jira trackers share one provider/site, distinguished only by
    `project_key`. Re-dispatching the orphaned `local_review` must build the
    `TrackerContext` WITH `project_key`, or `TrackerRegistry.resolve` raises
    `KeyError("multiple issue trackers registered ...")`, the re-dispatch is
    swallowed, and the jira issue stays stranded — the exact bug this PR fixes,
    re-introduced for jira. Drives reconcile through a real registry so the
    `project_key` resolution path (invisible to the bare-AsyncMock tests) is
    actually exercised."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id="iss-jira",
            identifier="PROJ-9",
            title="t",
            team_key="PROJ",
            provider="jira",
            site="acme",
        )
        await db.runs.create(
            conn,
            id="pidless-local-review",
            issue_id="iss-jira",
            stage="local_review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        binding = RepoBinding(
            linear_team_key="PROJ",
            github_repo="org/repo",
            agent="codex",
            branch_prefix="symphony",
            provider="jira",
            tracker_site="acme",
            linear_states=LinearStates(ready="Todo"),
        )

        # Two jira trackers under the same provider/site, distinct project_key.
        proj = AsyncMock()
        proj.post_comment = AsyncMock(return_value="cmt-1")
        proj.team_states = AsyncMock(return_value={"Todo": "state-ready"})
        proj.move_issue = AsyncMock()
        other = AsyncMock()
        other.move_issue = AsyncMock()

        registry = TrackerRegistry()
        registry.register("jira", "acme", proj, project_key="PROJ")
        registry.register("jira", "acme", other, project_key="OTHER")

        flipped = await reconcile(conn, registry, bindings=[binding])
        assert flipped == 1

        # Re-dispatched on the correct project's tracker — not stranded.
        proj.move_issue.assert_awaited_once_with("iss-jira", "state-ready")
        other.move_issue.assert_not_awaited()
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
        await db.issues.upsert(conn, id="iss-weird", identifier="ENG-4", title="t", team_key="ENG")
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
async def test_reconcile_collapses_duplicate_same_stage_runs(tmp_path: Path) -> None:
    """Two live runs for the same (issue_id, stage) — a race past SYM-152, a
    crash, or a manual dispatch. Reconcile keeps the oldest live, terminates the
    younger's process and flips it `superseded` with a duplicate-stage kind."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(conn, id="iss-dup", identifier="ENG-10", title="t", team_key="ENG")
        # Use distinct fake PIDs with an injected pid_alive so the orphan sweep
        # leaves both alive and only the duplicate-collapse pass acts.
        await db.runs.create(
            conn,
            id="older",
            issue_id="iss-dup",
            stage="implement",
            status="running",
            pid=101,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="younger",
            issue_id="iss-dup",
            stage="implement",
            status="running",
            pid=102,
            started_at="2026-05-10T00:01:00+00:00",
        )

        terminated: list[int] = []

        def _fake_terminate(pid: int) -> bool:
            terminated.append(pid)
            return True

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        flipped = await reconcile(
            conn,
            linear,
            pid_alive=lambda _: True,
            terminate_pid=_fake_terminate,
        )
        assert flipped == 1

        # The younger duplicate's distinct pid was terminated.
        assert terminated == [102]

        # Exactly one live run remains — the oldest.
        rows = await db.runs.list_live_with_pid(conn)
        assert [r.id for r in rows] == ["older"]

        cur = await conn.execute(
            """
            SELECT status, ended_at, termination_kind, termination_detail
            FROM runs WHERE id=?
            """,
            ("younger",),
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == db.runs.SUPERSEDED_STATUS
        assert row[1] is not None
        assert row["termination_kind"] == db.runs.DUPLICATE_STAGE_KIND
        assert "older" in row["termination_detail"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_duplicate_shared_pid_skips_termination(tmp_path: Path) -> None:
    """When both duplicate rows share the same PID, terminating it would kill the
    survivor. Reconcile skips the kill but still marks the duplicate superseded."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-shared", identifier="ENG-20", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="older",
            issue_id="iss-shared",
            stage="implement",
            status="running",
            pid=101,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="younger",
            issue_id="iss-shared",
            stage="implement",
            status="running",
            pid=101,
            started_at="2026-05-10T00:01:00+00:00",
        )

        terminated: list[int] = []

        def _fake_terminate_shared(pid: int) -> bool:
            terminated.append(pid)
            return True

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        flipped = await reconcile(
            conn,
            linear,
            pid_alive=lambda _: True,
            terminate_pid=_fake_terminate_shared,
        )
        assert flipped == 1

        # Shared PID must not be terminated — killing it would also kill the survivor.
        assert terminated == []

        rows = await db.runs.list_live_with_pid(conn)
        assert [r.id for r in rows] == ["older"]

        cur = await conn.execute(
            "SELECT status, termination_kind FROM runs WHERE id=?", ("younger",)
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == db.runs.SUPERSEDED_STATUS
        assert row["termination_kind"] == db.runs.DUPLICATE_STAGE_KIND
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_leaves_distinct_stages_for_one_issue_untouched(
    tmp_path: Path,
) -> None:
    """One issue with a live `implement` and a live `local_review` run is a
    legitimate layout, not a duplicate — the collapse must not touch either."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-stages", identifier="ENG-11", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="implement",
            issue_id="iss-stages",
            stage="implement",
            status="running",
            pid=os.getpid(),
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="local-review",
            issue_id="iss-stages",
            stage="local_review",
            status="running",
            pid=os.getpid(),
            started_at="2026-05-10T00:01:00+00:00",
        )

        terminated: list[int] = []

        def _fake_terminate_stages(pid: int) -> bool:
            terminated.append(pid)
            return True

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        flipped = await reconcile(conn, linear, terminate_pid=_fake_terminate_stages)
        assert flipped == 0
        assert terminated == []

        rows = await db.runs.list_live_with_pid(conn)
        assert sorted(r.id for r in rows) == ["implement", "local-review"]
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


@pytest.mark.asyncio
async def test_reconcile_retires_stale_runs_for_merged_pr(tmp_path: Path) -> None:
    """SYM-231 startup sweep: an issue whose PR is merged (typically by hand,
    outside Symphony) must not keep runs at `running`/`needs_approval` — those
    show the finished issue as active forever. Pre-fix residue self-heals on the
    next boot instead of needing a manual SQL pass."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    try:
        for issue_id, identifier in (
            ("iss-merged", "ENG-20"),
            ("iss-open", "ENG-21"),
            ("iss-live", "ENG-22"),
        ):
            await db.issues.upsert(
                conn, id=issue_id, identifier=identifier, title="t", team_key="ENG"
            )

        async def _seed_pr(issue_id: str, repo: str, pr_number: int, merged: str | None) -> None:
            await db.issue_prs.upsert(
                conn,
                issue_id=issue_id,
                github_repo=repo,
                pr_number=pr_number,
                pr_url=f"https://github.com/{repo}/pull/{pr_number}",
                created_at="2026-05-10T00:00:00+00:00",
            )
            if merged is not None:
                assert await db.issue_prs.update_merged(
                    conn,
                    issue_id=issue_id,
                    github_repo=repo,
                    pr_number=pr_number,
                    merged_at=merged,
                )

        # Residue: parked merge run + orphaned pidless review monitor, both from
        # before the merge.
        await _seed_pr("iss-merged", "org/repo", 42, merged_at)
        await db.runs.create(
            conn,
            id="stale-merge",
            issue_id="iss-merged",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T10:00:00+00:00",
        )
        # Parked *after* the merge (SYM-114/SYM-231 review): Symphony can park a
        # needs_approval wait after the PR was already merged externally. With
        # no operator wait and no PR left open, nothing can ever revisit this
        # row, so it must self-heal too rather than surviving because it
        # happens to have started after the merge timestamp.
        await db.runs.create(
            conn,
            id="post-merge",
            issue_id="iss-merged",
            stage="implement",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T12:00:00+00:00",
        )
        # Another PR still open for the issue: work is in flight, leave it alone.
        await _seed_pr("iss-open", "org/repo", 43, merged_at)
        await _seed_pr("iss-open", "org/other", 44, None)
        await db.runs.create(
            conn,
            id="open-pr-merge",
            issue_id="iss-open",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T10:00:00+00:00",
        )
        # Live process on a merged issue: not bookkeeping residue, don't touch.
        await _seed_pr("iss-live", "org/repo", 45, merged_at)
        await db.runs.create(
            conn,
            id="live-run",
            issue_id="iss-live",
            stage="implement",
            status="running",
            pid=os.getpid(),
            started_at="2026-05-10T10:00:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        assert await reconcile(conn, linear) == 2

        runs = {}
        for issue_id in ("iss-merged", "iss-open", "iss-live"):
            for run in await db.runs.history_for_issue(conn, issue_id):
                runs[run.id] = (run.status, run.termination_kind)
    finally:
        await conn.close()

    assert runs["stale-merge"] == (db.runs.SUPERSEDED_STATUS, "pr_merged")
    assert runs["post-merge"] == (db.runs.SUPERSEDED_STATUS, "pr_merged")
    assert runs["open-pr-merge"][0] == "needs_approval"
    assert runs["live-run"][0] == "running"


@pytest.mark.asyncio
async def test_reconcile_retire_for_merged_pr_preserves_ended_at(tmp_path: Path) -> None:
    """SYM-231 review: a parked `needs_approval` merge run already carries its
    genuine end timestamp and termination reason (set when `poll/_merge.py`
    parked it). Retiring it at startup must not clobber either with this
    sweep's generic `pr_merged` reason and the reconcile time — it should
    flip status only via `supersede_preserving_termination`, mirroring
    `reconciler.py`'s `_supersede_run`."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    real_ended_at = "2026-05-10T10:30:00+00:00"
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-50", title="t", team_key="ENG")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            merged_at=merged_at,
        )
        await db.runs.create(
            conn,
            id="stale-merge",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T10:00:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "stale-merge",
            "needs_approval",
            ended_at=real_ended_at,
            kind="awaiting_human_merge",
            detail="parked for manual merge",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        now = datetime.fromisoformat("2026-06-01T00:00:00+00:00")
        assert await reconcile(conn, linear, clock=lambda: now) == 1

        run = (await db.runs.history_for_issue(conn, "iss-1"))[0]
        assert run.status == db.runs.SUPERSEDED_STATUS
        assert run.ended_at == real_ended_at, f"ended_at clobbered: {run.ended_at}"
        assert run.termination_kind == "awaiting_human_merge"
        assert run.termination_detail == "parked for manual merge"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_supersedes_dead_pid_run_on_merged_pr_instead_of_interrupting(
    tmp_path: Path,
) -> None:
    """SYM-231 review: the dead-pid sweep used to run before the merged-PR
    retirement sweep, so a `running` row whose pid died on an issue whose PR
    is already merged got flipped to `interrupted`/`orphaned` (and a `$retry`
    comment posted on a finished issue) before the merged-PR sweep ever saw
    it — `list_unretired_for_merged_prs` only selects `running`/`needs_approval`
    rows, so the now-`interrupted` row falls through both sweeps and
    `ui/status.py` renders the issue FAILED forever with no open PR to explain
    it. The merged-PR sweep must run first."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-51", title="t", team_key="ENG")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            merged_at=merged_at,
        )
        dead_pid = 999_999
        await db.runs.create(
            conn,
            id="dead-pid-run",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=dead_pid,
            started_at="2026-05-10T10:00:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        assert await reconcile(conn, linear) == 1

        run = (await db.runs.history_for_issue(conn, "iss-1"))[0]
        assert run.status == db.runs.SUPERSEDED_STATUS
        assert run.termination_kind == "pr_merged"
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_clears_stale_operator_wait_for_merged_pr(tmp_path: Path) -> None:
    """SYM-231 acceptance criterion 1: retiring the run is not enough — the
    issue's `operator_waits` row must go too, or `ui/status.py` (which checks
    the wait before the run) still renders the finished issue as active."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-30", title="t", team_key="ENG")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            merged_at=merged_at,
        )
        await db.runs.create(
            conn,
            id="stale-merge",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T10:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="stale-merge",
            kind=db.operator_waits.KIND_MERGE,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            created_at="2026-05-10T10:01:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        assert await reconcile(conn, linear) == 1

        run = await db.runs.history_for_issue(conn, "iss-1")
        assert run[0].status == db.runs.SUPERSEDED_STATUS
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_leaves_wait_created_after_merge_alone(tmp_path: Path) -> None:
    """A wait created after the merge belongs to a later cycle (e.g. a fresh
    implement_failed park on a re-dispatched run) and must survive the sweep
    even though the issue's earlier PR is merged — and so must the run it is
    still parked on: `_clear_stale_merged_wait` refuses to delete this wait,
    so superseding its run out from under it would strand a live park on a
    `superseded` run (SYM-231 review)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-31", title="t", team_key="ENG")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            merged_at=merged_at,
        )
        await db.runs.create(
            conn,
            id="stale-merge",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T10:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="stale-merge",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            created_at="2026-05-10T12:00:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        assert await reconcile(conn, linear) == 0

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
        run = (await db.runs.history_for_issue(conn, "iss-1"))[0]
        assert run.status == "needs_approval"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_clears_stale_wait_on_already_failed_run_for_merged_pr(
    tmp_path: Path,
) -> None:
    """SYM-231 acceptance criterion 3: the issue's own example rows (SYM-226/
    227/228/218) are an `implement_failed` wait sitting on a `failed` — not
    `running`/`needs_approval` — implement run of an issue whose PR is merged.
    `_retire_runs_for_merged_prs` only clears a wait alongside a run it
    retires, and `list_unretired_for_merged_prs` never selects a `failed` run,
    so this residue needs its own sweep to self-heal at startup."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-40", title="t", team_key="ENG")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            merged_at=merged_at,
        )
        await db.runs.create(
            conn,
            id="implement-failed",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T09:00:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "implement-failed",
            "failed",
            ended_at="2026-05-10T09:30:00+00:00",
            kind="agent_error",
            detail="agent crashed",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="implement-failed",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            created_at="2026-05-10T09:01:00+00:00",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        assert await reconcile(conn, linear) == 1

        run = (await db.runs.history_for_issue(conn, "iss-1"))[0]
        assert run.status == db.runs.SUPERSEDED_STATUS
        assert run.termination_kind == "agent_error"
        assert run.termination_detail == "agent crashed"
        assert run.ended_at == "2026-05-10T09:30:00+00:00"
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_skips_wait_with_malformed_created_at(tmp_path: Path) -> None:
    """SYM-231 review: `_retire_stale_waits_for_merged_prs` parses every wait
    row in the DB via `list_all`, so a single bad legacy row (e.g. one written
    by a manual SQL cleanup that stored garbage instead of a timestamp) must
    not raise and abort the whole startup sweep — it should just be skipped,
    leaving that wait alone rather than propagating the parse error."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-43", title="t", team_key="ENG")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            merged_at=merged_at,
        )
        await db.runs.create(
            conn,
            id="implement-failed",
            issue_id="iss-1",
            stage="implement",
            status="failed",
            pid=None,
            started_at="2026-05-10T09:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="implement-failed",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            created_at="not-a-timestamp",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        assert await reconcile(conn, linear) == 0

        run = (await db.runs.history_for_issue(conn, "iss-1"))[0]
        assert run.status == "failed"
        assert await db.operator_waits.get(conn, "iss-1") is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_leaves_opted_out_binding_wait_and_run_alone(tmp_path: Path) -> None:
    """SYM-231 review: an operator who disabled reconcile for a repo
    (`reconcile_enabled=False`) must not have its parked wait or runs
    retired on every boot, mirroring `reconciler.py`'s live-path opt-out
    guard (`_retire_merged_issue_bookkeeping`)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-41", title="t", team_key="ENG")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            merged_at=merged_at,
        )
        await db.runs.create(
            conn,
            id="stale-merge",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T10:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="stale-merge",
            kind=db.operator_waits.KIND_MERGE,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            created_at="2026-05-10T10:01:00+00:00",
        )

        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            reconcile_enabled=False,
            linear_states=LinearStates(ready="Todo"),
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        assert await reconcile(conn, linear, bindings=[binding]) == 0

        run = (await db.runs.history_for_issue(conn, "iss-1"))[0]
        assert run.status == "needs_approval"
        assert await db.operator_waits.get(conn, "iss-1") is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_leaves_sibling_repo_wait_alone_even_when_enabled(tmp_path: Path) -> None:
    """SYM-231 review: unlike the opted-out case above, `org/repo`'s binding
    here has `reconcile_enabled=True` — the only thing that should protect its
    wait and run is that the merged PR belongs to a different repo
    (`org/other`) and `org/repo` has no `issue_prs` row of its own.
    `_stale_merged_wait_is_eligible` used to look up the issue's merged PR via
    `get_for_issue`, which returns whichever PR row sorts first regardless of
    repo — so `org/other`'s merge would delete `org/repo`'s wait and supersede
    its run even though `org/repo`'s own work is untouched. Covers both boot
    sweeps: `_retire_runs_for_merged_prs` (the `needs_approval` run) and
    `_retire_stale_waits_for_merged_prs` (the wait itself)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-44", title="t", team_key="ENG")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/other",
            pr_number=99,
            pr_url="https://github.com/org/other/pull/99",
            created_at="2026-05-10T00:00:00+00:00",
            binding_key='["ENG","org/other","frontend"]',
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/other",
            pr_number=99,
            merged_at=merged_at,
        )
        await db.runs.create(
            conn,
            id="stale-merge",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T10:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="stale-merge",
            kind=db.operator_waits.KIND_MERGE,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            created_at="2026-05-10T10:01:00+00:00",
        )

        bindings = [
            RepoBinding(
                linear_team_key="ENG",
                github_repo="org/repo",
                issue_label="symphony",
                reconcile_enabled=True,
                linear_states=LinearStates(ready="Todo"),
            ),
            RepoBinding(
                linear_team_key="ENG",
                github_repo="org/other",
                issue_label="frontend",
                reconcile_enabled=True,
                linear_states=LinearStates(ready="Todo"),
            ),
        ]

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        assert await reconcile(conn, linear, bindings=bindings) == 0

        run = (await db.runs.history_for_issue(conn, "iss-1"))[0]
        assert run.status == "needs_approval"
        assert await db.operator_waits.get(conn, "iss-1") is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reconcile_leaves_opted_out_binding_residue_alone_with_no_wait(
    tmp_path: Path,
) -> None:
    """SYM-231 review: SYM-114's shape (no operator wait at all) on a
    `reconcile_enabled=False` repo must be left alone entirely too — the
    opt-out check above only ever consulted a wait, so residue with no wait
    at all was swept on every boot despite the docstring's promise. Resolves
    the binding from the issue's own `issue_prs` row instead."""
    conn = await db.connect(tmp_path / "s.sqlite")
    merged_at = "2026-05-10T11:00:00+00:00"
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-42", title="t", team_key="ENG")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
            binding_key='["ENG","org/repo","symphony"]',
        )
        await db.issue_prs.update_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            merged_at=merged_at,
        )
        await db.runs.create(
            conn,
            id="stale-merge",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T10:00:00+00:00",
        )

        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            reconcile_enabled=False,
            linear_states=LinearStates(ready="Todo"),
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        assert await reconcile(conn, linear, bindings=[binding]) == 0

        run = (await db.runs.history_for_issue(conn, "iss-1"))[0]
        assert run.status == "needs_approval"
    finally:
        await conn.close()
