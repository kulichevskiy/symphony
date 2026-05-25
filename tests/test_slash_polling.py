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
from symphony.linear.client import LinearComment, LinearIssue
from symphony.linear.slash import SlashIntent, SlashKind
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


@pytest.mark.asyncio
async def test_approve_resumes_cost_cap_waiting_run(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$approve")])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        await db.runs.update_status(
            conn,
            "run-1",
            "failed",
            ended_at="2026-05-10T01:00:00+00:00",
        )
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001
        orch._operator_wait_run_ids.add("run-1")  # noqa: SLF001
        orch._cost_cap_run_bindings["run-1"] = cfg.repos[0]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        linear.comments_since.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Resumed" in body for body in bodies)
        assert "iss-1" not in orch._dispatch_run_ids  # noqa: SLF001
        assert "run-1" not in orch._operator_wait_run_ids  # noqa: SLF001
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reject_stops_cost_cap_waiting_run(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$reject")])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        await db.runs.update_status(
            conn,
            "run-1",
            "failed",
            ended_at="2026-05-10T01:00:00+00:00",
        )
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001
        orch._operator_wait_run_ids.add("run-1")  # noqa: SLF001
        orch._cost_cap_run_bindings["run-1"] = cfg.repos[0]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        linear.comments_since.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-blocked")
        assert "iss-1" not in orch._dispatch_run_ids  # noqa: SLF001
        assert "run-1" not in orch._operator_wait_run_ids  # noqa: SLF001
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_approve_resumes_cost_cap_wait_after_restart(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$approve")])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        await db.runs.update_status(
            conn,
            "run-1",
            "failed",
            ended_at="2026-05-10T01:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="run-1",
            kind=db.operator_waits.KIND_COST_CAP,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-05-10T01:00:00+00:00",
        )

        orch = _make_orch(cfg, linear, conn)

        await orch._poll_slash_commands()  # noqa: SLF001

        linear.comments_since.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Resumed" in body for body in bodies)
        assert "iss-1" not in orch._dispatch_run_ids  # noqa: SLF001
        assert "run-1" not in orch._operator_wait_run_ids  # noqa: SLF001
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.parametrize(
    ("wait_kind", "handler_name", "intent_kind"),
    [
        (db.operator_waits.KIND_COST_CAP, "_handle_cost_cap_slash_intent", SlashKind.APPROVE),
        (
            db.operator_waits.KIND_IMPLEMENT_FAILED,
            "_handle_implement_failed_slash_intent",
            SlashKind.APPROVE,
        ),
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

        stage = "merge" if wait_kind == db.operator_waits.KIND_MERGE else "implement"
        status = (
            "needs_approval"
            if wait_kind == db.operator_waits.KIND_MERGE
            else "failed"
        )
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
            db.operator_waits.KIND_COST_CAP,
            db.operator_waits.KIND_IMPLEMENT_FAILED,
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
        "_handle_cost_cap_slash_intent",
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


@pytest.mark.parametrize(
    ("wait_kind", "stage", "status"),
    [
        (db.operator_waits.KIND_COST_CAP, "implement", "failed"),
        (db.operator_waits.KIND_IMPLEMENT_FAILED, "implement", "failed"),
        (db.operator_waits.KIND_REVIEW_FAILED, "review_fix", "failed"),
        (db.operator_waits.KIND_MERGE, "merge", "needs_approval"),
    ],
)
@pytest.mark.asyncio
async def test_operator_wait_reject_missing_blocked_state_keeps_command_unseen(
    tmp_path: Path,
    wait_kind: str,
    stage: str,
    status: str,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$reject")])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        await _seed_operator_wait(
            conn,
            kind=wait_kind,
            stage=stage,
            status=status,
        )
        orch = _make_orch(cfg, linear, conn)
        orch._states["ENG"].pop("Blocked")  # noqa: SLF001

        with pytest.raises(RuntimeError, match="missing blocked state"):
            await orch._poll_slash_commands()  # noqa: SLF001

        assert not await db.comment_events.seen(conn, "c1")
        assert await db.comment_cursors.get(conn, "iss-1") is None
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.run_id == "run-1"
        bodies = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any(
            "`$reject` ignored" in body and "missing blocked state" in body
            for body in bodies
        )
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
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        linear.comments_since.assert_awaited_once()
        orch._schedule_merge.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
        _, seen_ids = await db.comment_cursors.get(conn, "iss-1") or ("", [])
        assert seen_ids == ["c1"]
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
        linear.comments_since = AsyncMock(
            return_value=[_comment("$stop", is_me=True)]
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
                _comment("$stop", external_thread_type="githubPullRequest")
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
