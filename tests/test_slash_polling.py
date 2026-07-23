"""Slash-command polling tests.

Issue #10: on every poll tick the orchestrator fetches `comments_since`
for each active run, dispatches intents (e.g. `$stop` kills the runner),
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
from symphony.github import GitHubError
from symphony.linear.client import LinearComment, LinearError, LinearIssue
from symphony.linear.slash import SlashIntent, SlashKind
from symphony.orchestrator.poll import Orchestrator, SlashHandlerFailure
from symphony.pipeline.local_review_loop import LoopOutcome, LoopResult


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
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
    # `_push_fn` on the orchestrator is now a DB-token-resolving wrapper
    # around this mock (OAuth in UI 4/7 review fix) — tests that need to
    # assert on push calls directly use this handle instead.
    orch._push_fn_mock = push_fn  # type: ignore[attr-defined]  # noqa: SLF001
    orch._states = {  # noqa: SLF001
        "ENG": {
            "Todo": "state-todo",
            "In Progress": "state-progress",
            "Needs Approval": "state-na",
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


def _intent(kind: SlashKind = SlashKind.APPROVE) -> SlashIntent:
    return SlashIntent(
        kind=kind,
        comment_id="c-command",
        created_at="2026-05-10T01:00:00+00:00",
    )


async def _seed_operator_wait(
    conn: object,
    *,
    issue_id: str = "iss-1",
    run_id: str = "run-1",
    kind: str,
    stage: str = "implement",
    status: str = "failed",
) -> None:
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
        stage=stage,
        status=status,
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )
    await db.operator_waits.upsert(
        conn,  # type: ignore[arg-type]
        issue_id=issue_id,
        run_id=run_id,
        kind=kind,
        linear_team_key="ENG",
        github_repo="org/repo",
        issue_label="",
        created_at="2026-05-10T00:00:00+00:00",
    )


async def _seed_review_state(
    conn: object,
    *,
    issue_id: str = "iss-1",
    pr_number: int = 42,
) -> None:
    await db.review_state.begin_review(
        conn,  # type: ignore[arg-type]
        issue_id,
        pr_number=pr_number,
        pr_url=f"https://github.com/org/repo/pull/{pr_number}",
        github_repo="org/repo",
        issue_label=None,
    )


async def _seed_parked_manual_merge_pr(
    conn: object,
    *,
    issue_id: str = "iss-1",
    github_repo: str = "org/repo",
    pr_number: int = 42,
) -> None:
    await db.issues.upsert(
        conn,  # type: ignore[arg-type]
        id=issue_id,
        identifier="ENG-1",
        title="t",
        team_key="ENG",
    )
    await db.issue_prs.upsert(
        conn,  # type: ignore[arg-type]
        issue_id=issue_id,
        github_repo=github_repo,
        pr_number=pr_number,
        pr_url=f"https://github.com/{github_repo}/pull/{pr_number}",
        created_at="2026-05-10T00:00:00+00:00",
    )
    await db.runs.create(
        conn,  # type: ignore[arg-type]
        id=f"review-{pr_number}",
        issue_id=issue_id,
        stage="review",
        status="completed",
        pid=None,
        started_at="2026-05-10T00:01:00+00:00",
    )
    await db.runs.update_status(
        conn,  # type: ignore[arg-type]
        f"review-{pr_number}",
        "completed",
        ended_at="2026-05-10T00:04:00+00:00",
    )
    assert await db.issue_prs.mark_parked_for_manual_merge(
        conn,  # type: ignore[arg-type]
        issue_id=issue_id,
        github_repo=github_repo,
        pr_number=pr_number,
        parked_at="2026-05-10T00:05:00+00:00",
    )


@pytest.mark.asyncio
async def test_stop_intent_kills_active_runner(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$stop")])
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


@pytest.mark.parametrize(
    ("wait_kind", "handler_name", "intent_kind"),
    [
        (
            db.operator_waits.KIND_IMPLEMENT_FAILED,
            "_handle_implement_failed_slash_intent",
            SlashKind.APPROVE,
        ),
        # `deliver_failed` is no longer handled by the shared implement-failed
        # handler: SYM-111 routes it to the dedicated
        # `_handle_deliver_failed_slash_intent`, which resumes delivery in place
        # rather than moving the issue back to ready. That handler's behaviour
        # is covered by tests/test_implement_e2e.py.
        (
            db.operator_waits.KIND_REVIEW_FAILED,
            "_handle_review_failed_slash_intent",
            SlashKind.RETRY,
        ),
        (
            db.operator_waits.KIND_MERGE,
            "_handle_merge_needs_approval_slash_intent",
            SlashKind.APPROVE,
        ),
    ],
)
@pytest.mark.asyncio
async def test_operator_wait_handlers_lazy_restore_binding_and_dispatch(
    tmp_path: Path,
    wait_kind: str,
    handler_name: str,
    intent_kind: SlashKind,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.lookup_issue = AsyncMock(return_value=_issue())

        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_comment = AsyncMock()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_review_poll = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        if wait_kind == db.operator_waits.KIND_MERGE:
            stage = "merge"
        elif wait_kind == db.operator_waits.KIND_DELIVER_FAILED:
            stage = "deliver"
        else:
            stage = "implement"
        status = "needs_approval" if wait_kind == db.operator_waits.KIND_MERGE else "failed"
        await _seed_operator_wait(
            conn,
            kind=wait_kind,
            stage=stage,
            status=status,
        )
        if wait_kind in (
            db.operator_waits.KIND_REVIEW_FAILED,
            db.operator_waits.KIND_MERGE,
        ):
            await _seed_review_state(conn)

        await getattr(orch, handler_name)("iss-1", "run-1", _intent(intent_kind))

        if wait_kind in (
            db.operator_waits.KIND_IMPLEMENT_FAILED,
            db.operator_waits.KIND_DELIVER_FAILED,
        ):
            linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
            assert await db.operator_waits.get(conn, "iss-1") is None
        elif wait_kind == db.operator_waits.KIND_REVIEW_FAILED:
            orch._gh.pr_comment.assert_awaited_once_with(  # type: ignore[attr-defined]  # noqa: SLF001
                42, "@codex review", repo="org/repo"
            )
            orch._schedule_review_poll.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
            assert await db.operator_waits.get(conn, "iss-1") is None
        else:
            orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
            assert orch._merge_needs_approval_bindings["run-1"] is cfg.repos[0]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.parametrize(
    "handler_name",
    [
        "_handle_implement_failed_slash_intent",
        "_handle_review_failed_slash_intent",
        "_handle_merge_needs_approval_slash_intent",
    ],
)
@pytest.mark.asyncio
async def test_operator_wait_handlers_reject_when_binding_and_wait_missing(
    tmp_path: Path,
    handler_name: str,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")

        await getattr(orch, handler_name)("iss-1", "run-1", _intent())

        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any("$approve" in body and "ignored" in body for body in posted)
        assert any("operator wait" in body for body in posted)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_acceptance_clears_blocked_wait_and_infra_retries(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$retry-acceptance")])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_ACCEPTANCE_BLOCKED,
            stage="acceptance",
        )
        await db.acceptance_state.begin_acceptance(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="abc123",
            mode="code_only",
            preview_url="",
            extracted_criteria="[]",
        )
        await db.acceptance_state.bump_infra_retries(conn, "iss-1")
        await db.acceptance_state.bump_infra_retries(conn, "iss-1")

        orch = _make_orch(cfg, linear, conn)

        await orch._poll_slash_commands()  # noqa: SLF001

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.infra_retries == 0
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert "run-1" not in orch._operator_wait_run_ids  # noqa: SLF001
        assert "iss-1" not in orch._dispatch_run_ids  # noqa: SLF001
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_skip_acceptance_on_blocked_wait_dispatches_merge(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$skip-acceptance")])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_ACCEPTANCE_BLOCKED,
            stage="acceptance",
        )
        await db.acceptance_state.begin_acceptance(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="abc123",
            mode="code_only",
            preview_url="",
            extracted_criteria="[]",
        )
        await db.acceptance_state.bump_infra_retries(conn, "iss-1")
        await db.acceptance_state.bump_infra_retries(conn, "iss-1")

        orch = _make_orch(cfg, linear, conn)
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        _, kwargs = orch._schedule_merge.call_args  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["pr_number"] == 42
        assert kwargs["pr_url"] == "https://github.com/org/repo/pull/42"
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_approve_on_parked_manual_merge_pr_merges_once(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(update={"auto_merge": False, "merge_strategy": "merge"})
        cfg = Config(repos=[binding])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$approve")])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_parked_manual_merge_pr(conn, pr_number=302)

        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_merge = AsyncMock()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._gh.pr_merge.assert_awaited_once_with(  # type: ignore[attr-defined]  # noqa: SLF001
            302,
            strategy="merge",
            auto=False,
            repo="org/repo",
        )
        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
        linear.post_comment.assert_not_awaited()
        pr = await db.issue_prs.get(conn, issue_id="iss-1", github_repo="org/repo")
        assert pr is not None
        assert pr.parked_at == "2026-05-10T00:05:00+00:00"
        assert await db.comment_events.seen(conn, "c1")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_approve_on_parked_manual_merge_pr_reports_merge_error(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding().model_copy(update={"auto_merge": False})])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$approve")])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_parked_manual_merge_pr(conn, pr_number=303)

        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_merge = AsyncMock(  # type: ignore[attr-defined]  # noqa: SLF001
            side_effect=GitHubError("branch protection blocked")
        )

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._gh.pr_merge.assert_awaited_once_with(  # type: ignore[attr-defined]  # noqa: SLF001
            303,
            strategy="squash",
            auto=False,
            repo="org/repo",
        )
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert len(bodies) == 1
        assert "manual merge failed" in bodies[0]
        assert "branch protection blocked" in bodies[0]
        pr = await db.issue_prs.get(conn, issue_id="iss-1", github_repo="org/repo")
        assert pr is not None
        assert pr.parked_at == "2026-05-10T00:05:00+00:00"
        assert await db.comment_events.seen(conn, "c1")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_approve_after_restart_dispatches_from_unread_comment(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$approve")])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            run_id="merge-run",
            kind=db.operator_waits.KIND_MERGE,
            stage="merge",
            status="needs_approval",
        )
        await _seed_review_state(conn, pr_number=166)

        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_merge = AsyncMock()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        linear.comments_since.assert_awaited_once()
        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._gh.pr_merge.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
        _, seen_ids = await db.comment_cursors.get(conn, "iss-1") or ("", [])
        assert seen_ids == ["c1"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_approve_dispatches_when_operator_shares_linear_identity(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        operator_approve = LinearComment.from_node(
            {
                "id": "c-approve",
                "body": "$approve",
                "createdAt": "2026-05-26T13:49:22Z",
                "user": {"name": "Operator", "isMe": True},
                "externalThread": None,
            }
        )
        symphony_rejection = LinearComment.from_node(
            {
                "id": "c-rejection",
                "body": "🚫 `$approve` ignored: no active merge\n\n<!-- symphony:comment -->",
                "createdAt": "2026-05-26T13:49:33Z",
                "user": {"name": "Operator", "isMe": True},
                "externalThread": None,
            }
        )
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[operator_approve, symphony_rejection])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            run_id="merge-run",
            kind=db.operator_waits.KIND_MERGE,
            stage="merge",
            status="needs_approval",
        )
        await _seed_review_state(conn, pr_number=301)

        orch = _make_orch(cfg, linear, conn)
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        assert await db.comment_events.seen(conn, "c-approve")
        assert not await db.comment_events.seen(conn, "c-rejection")
        cursor = await db.comment_cursors.get(conn, "iss-1")
        assert cursor == ("2026-05-26T13:49:33Z", ["c-rejection"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_cap_approve_schedules_merge(tmp_path: Path) -> None:
    """SYM-114: `$approve` on a review-cap park force-advances to merge
    using the parked PR from `review_state`."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$approve")])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            run_id="review-run",
            kind=db.operator_waits.KIND_REVIEW_CAP,
            stage="review",
            status="completed",
        )
        await _seed_review_state(conn, pr_number=166)

        orch = _make_orch(cfg, linear, conn)
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        _, kwargs = orch._schedule_merge.call_args  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["pr_number"] == 166
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_cap_reject_moves_to_blocked_and_clears_wait(tmp_path: Path) -> None:
    """SYM-114: `$reject`/`$stop` on a review-cap park moves the issue to
    blocked and clears the wait instead of leaving it stranded."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$reject")])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            run_id="review-run",
            kind=db.operator_waits.KIND_REVIEW_CAP,
            stage="review",
            status="completed",
        )
        await _seed_review_state(conn, pr_number=166)

        orch = _make_orch(cfg, linear, conn)
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
        linear.move_issue.assert_awaited_once_with("iss-1", "state-blocked")
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert "review-run" not in orch._operator_wait_run_ids  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_cap_retry_rejected_without_redispatch(tmp_path: Path) -> None:
    """SYM-114: `$retry` isn't a supported reply on a review-cap park — it
    must be rejected, not silently re-dispatch a merge."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$retry")])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            run_id="review-run",
            kind=db.operator_waits.KIND_REVIEW_CAP,
            stage="review",
            status="completed",
        )
        await _seed_review_state(conn, pr_number=166)

        orch = _make_orch(cfg, linear, conn)
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        orch._schedule_merge.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
        bodies = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any("$retry" in body and "not supported" in body for body in bodies)
        assert await db.operator_waits.get(conn, "iss-1") is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_skip_acceptance_after_restart_dispatches_merge(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$skip-acceptance")])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            run_id="acceptance-run",
            kind=db.operator_waits.KIND_ACCEPTANCE_REJECTED,
            stage="acceptance",
            status="failed",
        )
        await db.acceptance_state.begin_acceptance(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="abc123",
            mode="code_only",
            preview_url="",
            extracted_criteria="[]",
        )
        await db.acceptance_state.bump_iteration(conn, "iss-1")
        await db.acceptance_state.record_verdict(
            conn,
            "iss-1",
            verdict="reject",
            artifacts_url="",
        )

        orch = _make_orch(cfg, linear, conn)
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        linear.comments_since.assert_awaited_once()
        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        assert await db.operator_waits.get(conn, "iss-1") is None
        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.last_verdict == "pass"
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("$skip-acceptance" in body and "merge" in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_acceptance_after_restart_resets_state_and_dispatches_acceptance(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$retry-acceptance")])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            run_id="acceptance-run",
            kind=db.operator_waits.KIND_ACCEPTANCE_REJECTED,
            stage="acceptance",
            status="failed",
        )
        await db.acceptance_state.begin_acceptance(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="abc123",
            mode="code_only",
            preview_url="",
            extracted_criteria="[]",
        )
        await db.acceptance_state.bump_iteration(conn, "iss-1")
        await db.acceptance_state.record_verdict(
            conn,
            "iss-1",
            verdict="reject",
            artifacts_url="",
        )

        orch = _make_orch(cfg, linear, conn)
        orch._schedule_acceptance = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        linear.comments_since.assert_awaited_once()
        orch._schedule_acceptance.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        assert await db.operator_waits.get(conn, "iss-1") is None
        state = await db.acceptance_state.get(conn, "iss-1")
        assert state.iteration == 0
        assert state.pr_number is None
        assert state.last_verdict == ""
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("$retry-acceptance" in body and "acceptance" in body for body in bodies)
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
                _comment("$stop", cid="c2", created_at="2026-05-10T12:00:00+00:00"),
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
            return_value=[_comment("$stop", cid="c1", created_at=boundary)]
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
            _comment("$stop", cid="c1", created_at=boundary),
            _comment("$stop", cid="c2", created_at=boundary),
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
    """A stale `$stop` posted between runs (after run A ended, before run B
    started) must NOT be replayed against run B."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        # Seed a stored cursor from run A that predates run B's start.
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG")
        await db.comment_cursors.set(conn, "iss-1", "2026-05-10T08:00:00+00:00", ["old"])
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
        linear.comments_since = AsyncMock(return_value=[_comment("$stop", cid="c1")])
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
async def test_stop_kill_failure_does_not_advance_cursor(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$stop", cid="c1")])
        linear.move_issue = AsyncMock()

        orch = _make_orch(cfg, linear, conn)
        orch._runner.kill.side_effect = RuntimeError("kill failed")  # type: ignore[attr-defined]  # noqa: SLF001
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        with pytest.raises(RuntimeError, match="kill failed"):
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
        linear.comments_since = AsyncMock(return_value=[_comment("$stop", is_me=True)])
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
            return_value=[_comment("$stop", external_thread_type="githubPullRequest")]
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


@pytest.mark.asyncio
async def test_self_authored_rejection_comment_does_not_advance_cursor_past_failed_command(
    tmp_path: Path,
) -> None:
    """SYM-32 P1 + P2 (round 2): the `command_rejected` comment posted
    after a failed slash handler is self-authored and arrives back to us.
    The cursor must NOT advance past it while the failed original is still
    pending (otherwise the original is stranded — silent drop), but it MUST
    advance past it once the original recovers (otherwise rejections
    accumulate unboundedly in the `comments_since` window).

    Three ticks: tick 1 only the `$retry`, tick 2 `$retry` + rejection
    while `move_issue` still fails, tick 3 `move_issue` recovers and the
    `$retry` succeeds."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        retry = _comment("$retry", cid="c-retry", created_at="2026-05-10T01:30:00+00:00")
        rejection = _comment(
            "🚫 `$retry` ignored: could not move ...",
            cid="c-rejection",
            created_at="2026-05-10T01:30:30+00:00",
            is_me=True,
        )
        comments_per_tick: list[list[LinearComment]] = [
            [retry],
            [retry, rejection],
            [retry, rejection],
        ]
        tick_idx = {"i": 0}

        async def fake_comments_since(_issue_id: str, _after: datetime) -> list[LinearComment]:
            i = tick_idx["i"]
            tick_idx["i"] += 1
            return comments_per_tick[min(i, len(comments_per_tick) - 1)]

        linear.comments_since = AsyncMock(side_effect=fake_comments_since)
        # Tick 1: 1 fail. Tick 2: 1 more fail. Tick 3: success.
        linear.move_issue = AsyncMock(
            side_effect=[
                LinearError("upstream 503"),
                LinearError("upstream 503"),
                None,
            ]
        )
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            stage="implement",
            status="failed",
        )

        orch = _make_orch(cfg, linear, conn)

        # Tick 1 — only the $retry; handler fails; cursor untouched, wait stays.
        await orch._poll_slash_commands()  # noqa: SLF001
        assert linear.move_issue.await_count == 1
        assert not await db.comment_events.seen(conn, "c-retry")
        assert await db.operator_waits.get(conn, "iss-1") is not None

        # Tick 2 — $retry STILL fails; the rejection is also in the batch but
        # the loop breaks on the $retry failure BEFORE reaching it. Cursor
        # must NOT advance past either comment.
        await orch._poll_slash_commands()  # noqa: SLF001
        cursor = await db.comment_cursors.get(conn, "iss-1")
        if cursor is not None:
            cursor_at, _ = cursor
            assert cursor_at <= "2026-05-10T01:30:00+00:00", (
                f"cursor jumped past failed $retry: {cursor_at}"
            )
        assert not await db.comment_events.seen(conn, "c-rejection")

        # Tick 3 — move_issue recovers. $retry succeeds (marks seen, advances
        # cursor to its timestamp); rejection then iterates safely after it
        # in the same batch, triggering the post-loop cursor catch-up to
        # advance past the rejection too. Without that catch-up, the
        # rejection would be re-fetched on every subsequent tick forever.
        await orch._poll_slash_commands()  # noqa: SLF001
        assert linear.move_issue.await_count == 3
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert await db.comment_events.seen(conn, "c-retry")
        cursor = await db.comment_cursors.get(conn, "iss-1")
        assert cursor is not None
        cursor_at, cursor_ids = cursor
        assert cursor_at == "2026-05-10T01:30:30+00:00", (
            f"cursor must advance past the self-authored rejection; got {cursor_at}"
        )
        assert "c-rejection" in cursor_ids
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_slash_handler_failure_stops_processing_later_comments_in_batch(
    tmp_path: Path,
) -> None:
    """SYM-32 P2: when a comment's handler fails (SlashHandlerFailure), the
    poll loop must NOT keep processing later comments in the same batch.
    Otherwise a later comment would advance the cursor past the failed one,
    stranding the failed command (the very silent-drop the fix is preventing).
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        # Two comments back-to-back: first fails, second would otherwise
        # succeed and advance the cursor past the failure.
        linear.comments_since = AsyncMock(
            return_value=[
                _comment("$retry", cid="c-first", created_at="2026-05-10T01:30:00+00:00"),
                _comment("$retry", cid="c-second", created_at="2026-05-10T01:31:00+00:00"),
            ]
        )
        linear.move_issue = AsyncMock(side_effect=LinearError("upstream 503"))
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            stage="implement",
            status="failed",
        )

        orch = _make_orch(cfg, linear, conn)

        await orch._poll_slash_commands()  # noqa: SLF001

        # First comment fails -> move_issue attempted once, no further moves.
        assert linear.move_issue.await_count == 1
        # Neither comment is marked seen — both must remain replayable next tick.
        assert not await db.comment_events.seen(conn, "c-first")
        assert not await db.comment_events.seen(conn, "c-second")
        # Cursor must not have advanced past the first failed comment.
        cursor = await db.comment_cursors.get(conn, "iss-1")
        if cursor is not None:
            cursor_at, _ = cursor
            assert cursor_at <= "2026-05-10T01:30:00+00:00"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_failed_retry_preserves_wait_when_lookup_fails(
    tmp_path: Path,
) -> None:
    """SYM-32 P1: when `$retry`/`$approve` on a review_failed wait raises
    `SlashHandlerFailure` because `lookup_issue` fails, the operator wait
    must remain intact (in DB and in `_dispatch_run_ids`) so the next poll
    tick can iterate the issue and retry the command. Clearing the wait
    before the lookup would orphan the issue from slash polling."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(side_effect=LinearError("lookup down"))
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_REVIEW_FAILED,
            stage="review",
            status="failed",
        )
        await _seed_review_state(conn)

        orch = _make_orch(cfg, linear, conn)
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001
        orch._operator_wait_run_ids.add("run-1")  # noqa: SLF001
        orch._review_failed_run_bindings["run-1"] = cfg.repos[0]  # noqa: SLF001

        with pytest.raises(SlashHandlerFailure) as excinfo:
            await orch._handle_review_failed_slash_intent(  # noqa: SLF001
                "iss-1", "run-1", _intent(SlashKind.RETRY)
            )
        assert "look up" in excinfo.value.reason

        # Wait must still be present everywhere — operator can retry on next tick.
        assert await db.operator_waits.get(conn, "iss-1") is not None
        assert orch._dispatch_run_ids.get("iss-1") == "run-1"  # noqa: SLF001
        assert "run-1" in orch._operator_wait_run_ids  # noqa: SLF001
        assert "run-1" in orch._review_failed_run_bindings  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_failed_retry_failure_posts_rejection_and_retries_next_tick(
    tmp_path: Path,
) -> None:
    """SYM-32: when `linear.move_issue` raises mid-handler the slash command
    must not be silently dropped — the orchestrator posts a `command_rejected`
    Linear comment, does NOT advance the cursor for that comment, and a
    subsequent poll tick (with `move_issue` recovered) cleanly completes the
    transition and clears the operator wait."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(
            return_value=[_comment("$retry", cid="c-retry", created_at="2026-05-10T01:30:00+00:00")]
        )
        linear.move_issue = AsyncMock(side_effect=[LinearError("upstream 503"), None])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            stage="implement",
            status="failed",
        )

        orch = _make_orch(cfg, linear, conn)

        # First poll: move_issue raises -> SlashHandlerFailure -> rejection posted,
        # operator wait remains, comment NOT marked seen.
        await orch._poll_slash_commands()  # noqa: SLF001

        assert linear.move_issue.await_count == 1
        rejection_bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any(
            "$retry" in body and "ignored" in body and "ready" in body for body in rejection_bodies
        ), f"expected a command_rejected body, got {rejection_bodies!r}"
        assert await db.operator_waits.get(conn, "iss-1") is not None
        assert not await db.comment_events.seen(conn, "c-retry")

        # Second poll: same comment re-served, move_issue now succeeds ->
        # operator wait cleared, comment marked seen, cursor advances.
        await orch._poll_slash_commands()  # noqa: SLF001

        assert linear.move_issue.await_count == 2
        assert linear.move_issue.await_args_list[-1].args == ("iss-1", "state-todo")
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert await db.comment_events.seen(conn, "c-retry")
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Resumed" in body for body in bodies)
    finally:
        await conn.close()


# --- remote_review: false suppresses $retry @codex pings ----------------
#
# Local-only / no-review bindings must never fire the `@codex` bot. A manual
# `$retry` on an active review monitor or after a failed review wait re-arms
# the monitor without posting `@codex review`.


@pytest.mark.asyncio
async def test_active_review_retry_skips_codex_when_remote_review_disabled(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(update={"local_review": True, "remote_review": False})
        cfg = Config(repos=[binding])
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_comment = AsyncMock()  # type: ignore[attr-defined]  # noqa: SLF001

        await db.issues.upsert(
            conn,  # type: ignore[arg-type]
            id="iss-1",
            identifier="ENG-1",
            title="t",
            team_key="ENG",
        )
        await _seed_review_state(conn)

        await orch._handle_active_review_retry_intent(  # noqa: SLF001
            "iss-1", "run-1", _intent(SlashKind.RETRY)
        )

        orch._gh.pr_comment.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
        # The monitor is still re-armed: signature recorded + comment posted.
        state = await db.review_state.get(conn, "iss-1")  # type: ignore[arg-type]
        assert state.last_trigger_signature == "manual_retry:run-1:c-command"
        linear.post_comment.assert_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_failed_retry_skips_codex_when_remote_review_disabled(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(update={"local_review": True, "remote_review": False})
        cfg = Config(repos=[binding])
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.lookup_issue = AsyncMock(return_value=_issue())
        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_comment = AsyncMock()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_review_poll = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        async def approve_local_review(**kwargs: object) -> LoopResult:
            assert kwargs["parent_run_id"] != "run-1"
            await db.runs.create(
                conn,
                id="retry-local-review",
                issue_id="iss-1",
                stage="local_review",
                status="completed",
                pid=None,
                started_at="2026-05-10T01:01:00+00:00",
            )
            return LoopResult(
                outcome=LoopOutcome.APPROVED,
                iterations=1,
                verdicts=(),
            )

        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=approve_local_review
        )

        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_REVIEW_FAILED,
            stage="implement",
            status="failed",
        )
        await _seed_review_state(conn)

        await orch._handle_review_failed_slash_intent(  # noqa: SLF001
            "iss-1", "run-1", _intent(SlashKind.RETRY)
        )

        orch._gh.pr_comment.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._run_local_review_phase.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._push_fn_mock.assert_awaited_once_with(  # type: ignore[attr-defined]  # noqa: SLF001
            Path("/dev/null"), "symphony/eng-1"
        )
        # The monitor restarts only after the local reviewer approves.
        orch._schedule_review_poll.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert any(
            run.id == "retry-local-review"
            and run.stage == "local_review"
            and run.status == "completed"
            for run in history
        )
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_failed_retry_reparks_when_local_only_push_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(update={"local_review": True, "remote_review": False})
        cfg = Config(repos=[binding])
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.lookup_issue = AsyncMock(return_value=_issue())
        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_comment = AsyncMock()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_review_poll = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._push_fn_mock.side_effect = RuntimeError(  # type: ignore[attr-defined]  # noqa: SLF001
            "non-fast-forward"
        )
        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=LoopResult(
                outcome=LoopOutcome.APPROVED,
                iterations=1,
                verdicts=(),
            )
        )

        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_REVIEW_FAILED,
            stage="implement",
            status="failed",
        )
        await _seed_review_state(conn)

        await orch._handle_review_failed_slash_intent(  # noqa: SLF001
            "iss-1", "run-1", _intent(SlashKind.RETRY)
        )

        orch._gh.pr_comment.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._push_fn_mock.assert_awaited_once_with(  # type: ignore[attr-defined]  # noqa: SLF001
            Path("/dev/null"), "symphony/eng-1"
        )
        orch._schedule_review_poll.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_REVIEW_FAILED
        assert wait.run_id != "run-1"
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("local review retry push failed" in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_failed_retry_reparks_when_local_only_review_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(update={"local_review": True, "remote_review": False})
        cfg = Config(repos=[binding])
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.lookup_issue = AsyncMock(return_value=_issue())
        orch = _make_orch(cfg, linear, conn)
        orch._gh.pr_comment = AsyncMock()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_review_poll = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=LoopResult(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=1,
                verdicts=(),
                error="reviewer still found issues",
            )
        )

        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_REVIEW_FAILED,
            stage="implement",
            status="failed",
        )
        await _seed_review_state(conn)

        await orch._handle_review_failed_slash_intent(  # noqa: SLF001
            "iss-1", "run-1", _intent(SlashKind.RETRY)
        )

        orch._gh.pr_comment.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._run_local_review_phase.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._push_fn_mock.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
        orch._schedule_review_poll.assert_not_called()  # type: ignore[attr-defined]  # noqa: SLF001
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_REVIEW_FAILED
        assert wait.run_id != "run-1"
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("local-only review did not approve" in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_web_command_applies_via_drain(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        command_id = orch.enqueue_web_command("iss-1", SlashKind.STOP)
        assert command_id
        await orch._drain_web_commands()  # noqa: SLF001

        # $stop on an active run kills the runner — same path as a Linear command.
        orch._runner.kill.assert_awaited_once_with("run-1")  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_web_command_no_eligible_run_is_dropped(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        # No dispatch / review mapping for the issue → not eligible.
        orch.enqueue_web_command("iss-1", SlashKind.STOP)
        await orch._drain_web_commands()  # noqa: SLF001

        orch._runner.kill.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()
