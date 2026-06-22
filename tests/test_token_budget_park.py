"""Per-issue token-budget soft escalation tests.

A soft, per-issue effective-token budget parks a runaway issue in Needs
Approval at a dispatch boundary instead of dispatching its next run. The live
agent is never killed. `$approve`/👍 grants another budget window and resumes;
`$reject` blocks. Off by default (`per_issue_token_budget=None`).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.linear.slash import SlashIntent, SlashKind
from symphony.orchestrator.poll import Orchestrator

_BUDGET = 1_000


def _binding(per_issue_token_budget: int | None = None) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        per_issue_token_budget=per_issue_token_budget,
        linear_states=LinearStates(ready="Todo"),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="t",
        description="",
        url="https://linear.app/x",
        state_id="state-na",
        state_name="Needs Approval",
        state_type="started",
        team_key="ENG",
        labels=[],
    )


def _make_orch(cfg: Config, linear: AsyncMock, conn: object) -> Orchestrator:
    orch = Orchestrator(
        cfg,
        linear,
        conn,  # type: ignore[arg-type]
        runner=MagicMock(),
        gh=MagicMock(),
        workspace=MagicMock(),
        push_fn=AsyncMock(),
    )
    orch._states = {  # noqa: SLF001
        "ENG": {
            "Todo": "state-todo",
            "In Progress": "state-progress",
            "Needs Approval": "state-na",
            "Blocked": "state-blocked",
        }
    }
    return orch


def _intent(kind: SlashKind) -> SlashIntent:
    return SlashIntent(
        kind=kind,
        comment_id="c-cmd",
        created_at="2026-05-10T01:00:00+00:00",
    )


async def _seed_run_with_tokens(
    conn: object,
    *,
    run_id: str,
    stage: str,
    input_tokens: int,
) -> None:
    await db.runs.create(
        conn,  # type: ignore[arg-type]
        id=run_id,
        issue_id="iss-1",
        stage=stage,
        status="completed",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )
    await db.runs.add_usage(
        conn,  # type: ignore[arg-type]
        run_id,
        cost_usd=0.0,
        input_tokens=input_tokens,
    )


async def _seed_issue(conn: object) -> None:
    await db.issues.upsert(
        conn,  # type: ignore[arg-type]
        id="iss-1",
        identifier="ENG-1",
        title="t",
        team_key="ENG",
    )


@pytest.mark.asyncio
async def test_guard_off_by_default_never_parks(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])  # no global, no per-binding budget
        orch = _make_orch(cfg, AsyncMock(), conn)
        await _seed_issue(conn)
        await _seed_run_with_tokens(
            conn, run_id="r-impl", stage="implement", input_tokens=10_000_000
        )
        parked = await orch._maybe_park_for_token_budget(  # noqa: SLF001
            "iss-1", "r-monitor", cfg.repos[0]
        )
        assert parked is False
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_guard_trips_at_boundary_and_parks(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding(per_issue_token_budget=_BUDGET)])
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()
        orch = _make_orch(cfg, linear, conn)
        await _seed_issue(conn)
        # implement 700 effective + review_fix 400 effective = 1100 >= 1000.
        await _seed_run_with_tokens(
            conn, run_id="r-impl", stage="implement", input_tokens=700
        )
        await _seed_run_with_tokens(
            conn, run_id="r-fix", stage="review_fix", input_tokens=400
        )
        # The boundary run (e.g. the live review monitor) is not killed.
        await db.runs.create(
            conn,
            id="r-monitor",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T02:00:00+00:00",
        )

        parked = await orch._maybe_park_for_token_budget(  # noqa: SLF001
            "iss-1", "r-monitor", cfg.repos[0]
        )

        assert parked is True
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_BUDGET_EXCEEDED
        assert wait.run_id == "r-monitor"
        # Parked in Needs Approval, comment posted with the breakdown.
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        body = str(linear.post_comment.await_args_list[0].args[1])
        assert "Token budget exceeded" in body
        assert "1,100" in body  # effective tokens used
        assert "1,000" in body  # ceiling
        assert "implement" in body and "review_fix" in body
        # Binding registered so the slash command routes after a restart-free run.
        assert orch._budget_exceeded_run_bindings["r-monitor"] is cfg.repos[0]  # noqa: SLF001
        # No active run blocks re-dispatch after approval.
        assert not await db.runs.has_active(conn, "iss-1")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_guard_under_budget_does_not_park(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding(per_issue_token_budget=_BUDGET)])
        orch = _make_orch(cfg, AsyncMock(), conn)
        await _seed_issue(conn)
        await _seed_run_with_tokens(
            conn, run_id="r-impl", stage="implement", input_tokens=500
        )
        parked = await orch._maybe_park_for_token_budget(  # noqa: SLF001
            "iss-1", "r-monitor", cfg.repos[0]
        )
        assert parked is False
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_approve_grants_window_and_resumes_repeatably(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding(per_issue_token_budget=_BUDGET)])
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        orch = _make_orch(cfg, linear, conn)
        await _seed_issue(conn)
        await db.runs.create(
            conn,
            id="r-monitor",
            issue_id="iss-1",
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="r-monitor",
            kind=db.operator_waits.KIND_BUDGET_EXCEEDED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-05-10T00:00:00+00:00",
        )

        await orch._handle_budget_exceeded_slash_intent(  # noqa: SLF001
            "iss-1", "r-monitor", _intent(SlashKind.APPROVE)
        )

        # One more budget window granted (repeatable), wait cleared, resumed.
        assert await db.issues.get_granted_token_budget(conn, "iss-1") == _BUDGET
        linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
        assert await db.operator_waits.get(conn, "iss-1") is None

        # Repeat after a second trip: the grant accumulates.
        await db.runs.create(
            conn,
            id="r-monitor-2",
            issue_id="iss-1",
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T03:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="r-monitor-2",
            kind=db.operator_waits.KIND_BUDGET_EXCEEDED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-05-10T03:00:00+00:00",
        )
        await orch._handle_budget_exceeded_slash_intent(  # noqa: SLF001
            "iss-1", "r-monitor-2", _intent(SlashKind.APPROVE)
        )
        assert await db.issues.get_granted_token_budget(conn, "iss-1") == 2 * _BUDGET
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_approve_with_open_pr_resumes_review_not_bounced(tmp_path: Path) -> None:
    """$approve on a budget park at a review/merge boundary must re-arm the
    review monitor directly. Routing through the ready scan would hit the
    existing-PR guard, which bounces an open-PR issue to In Progress and
    strands it — the granted window wasted and review never resuming.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding(per_issue_token_budget=_BUDGET)])
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_comment = AsyncMock()  # noqa: SLF001
        await _seed_issue(conn)
        # An open (unmerged) PR exists — the park happened at a review boundary.
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.review_state.begin_review(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label="",
        )
        await db.runs.create(
            conn,
            id="r-monitor",
            issue_id="iss-1",
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="r-monitor",
            kind=db.operator_waits.KIND_BUDGET_EXCEEDED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-05-10T00:00:00+00:00",
        )

        await orch._handle_budget_exceeded_slash_intent(  # noqa: SLF001
            "iss-1", "r-monitor", _intent(SlashKind.APPROVE)
        )

        # Window granted + wait cleared, as before.
        assert await db.issues.get_granted_token_budget(conn, "iss-1") == _BUDGET
        assert await db.operator_waits.get(conn, "iss-1") is None
        # A fresh `review` run is dispatched (the review monitor is re-armed).
        live_review = await db.runs.list_live_by_stage(conn, stage="review")
        assert [r.id for r in live_review] != []
        assert any(r.id != "r-monitor" for r in live_review)
        # Crucially: NOT bounced to In Progress by the existing-PR guard.
        moved_states = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-progress" not in moved_states
    finally:
        for task in list(orch._review_poll_tasks):  # noqa: SLF001
            task.cancel()
        await asyncio.gather(*orch._review_poll_tasks, return_exceptions=True)  # noqa: SLF001
        await conn.close()


@pytest.mark.asyncio
async def test_reject_blocks_and_does_not_grant(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding(per_issue_token_budget=_BUDGET)])
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        orch = _make_orch(cfg, linear, conn)
        await _seed_issue(conn)
        await db.runs.create(
            conn,
            id="r-monitor",
            issue_id="iss-1",
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="r-monitor",
            kind=db.operator_waits.KIND_BUDGET_EXCEEDED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-05-10T00:00:00+00:00",
        )

        await orch._handle_budget_exceeded_slash_intent(  # noqa: SLF001
            "iss-1", "r-monitor", _intent(SlashKind.REJECT)
        )

        linear.move_issue.assert_awaited_once_with("iss-1", "state-blocked")
        assert await db.operator_waits.get(conn, "iss-1") is None
        # No budget granted on reject.
        assert await db.issues.get_granted_token_budget(conn, "iss-1") == 0
    finally:
        await conn.close()
