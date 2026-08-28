"""Durable issue-level pipeline controls (SYM-244, slice 1/9).

Covers what the control seam promises: the state and every accepted action
survive a daemon restart, `allowed_actions` derives from mode + outcome alone
(never from the diagnostic reason), and an invalid or duplicate action can
neither double-dispatch nor half-update the row.

The last two tests drive the tracer end to end: a failed implement run parks
with Retry offered, `$retry` records the accepted transition durably before any
side effect, and exactly one fresh implement attempt runs in the same workspace
on top of the checkpoint the failed attempt committed.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config
from symphony.linear.client import LinearError
from symphony.linear.slash import SlashIntent, SlashKind
from symphony.orchestrator.poll import Orchestrator, SlashHandlerFailure
from symphony.pipeline import controls

from .test_implement_e2e import (
    _done_result_line,
    _git,
    _init_git_workspace,
    _issue,
    _no_review_binding,
    _RecordingRunner,
    _scan_and_wait,
    _states,
)

ACTIONS = controls.ControlAction
MODES = controls.PipelineMode
OUTCOMES = controls.AttemptOutcome

ISSUE_ID = "iss-1"


async def _seed_issue(conn: aiosqlite.Connection) -> None:
    await db.issues.upsert(
        conn,
        id=ISSUE_ID,
        identifier="ENG-1",
        title="Add authentication",
        team_key="ENG",
    )


async def _record_implement(
    conn: aiosqlite.Connection,
    *,
    outcome: controls.AttemptOutcome,
    reason: str | None = None,
    at: str = "2026-08-27T10:00:00+00:00",
) -> None:
    await controls.record_stage_outcome(
        conn,
        ISSUE_ID,
        stage="implement",
        outcome=outcome,
        reason=reason,
        run_id="run-1",
        at=at,
    )


@pytest.mark.asyncio
async def test_snapshot_defaults_to_playing_with_no_attempt(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.mode is MODES.PLAYING
        assert snap.outcome is OUTCOMES.PENDING
        assert snap.stage is None
        assert snap.reason is None
        assert snap.allowed_actions == (ACTIONS.PAUSE, ACTIONS.ABORT)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failed_attempt_snapshot_offers_retry(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="agent exited 2")
        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.stage == "implement"
        assert snap.outcome is OUTCOMES.FAILED
        assert snap.reason == "agent exited 2"
        assert snap.run_id == "run-1"
        assert ACTIONS.RETRY in snap.allowed_actions
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_state_and_accepted_action_survive_restart(tmp_path: Path) -> None:
    """No control state lives in memory: reopening the DB shows the same
    mode/outcome and the same accepted-action record."""
    db_path = tmp_path / "s.sqlite"
    conn = await db.connect(db_path)
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="agent exited 2")
        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-retry",
            action_id="c-retry",
            at="2026-08-27T10:05:00+00:00",
        )
        assert result.accepted
    finally:
        await conn.close()

    restarted = await db.connect(db_path)
    try:
        snap = await controls.snapshot(restarted, ISSUE_ID)
        assert snap.mode is MODES.PLAYING
        assert snap.stage == "implement"
        assert snap.outcome is OUTCOMES.PENDING
        assert ACTIONS.RETRY not in snap.allowed_actions
        recorded = await controls.history(restarted, ISSUE_ID)
        assert [(a.action, a.actor, a.from_outcome, a.ts) for a in recorded] == [
            ("retry", "tracker:c-retry", "failed", "2026-08-27T10:05:00+00:00")
        ]
    finally:
        await restarted.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["agent exited 2", "workspace clone failed", None])
async def test_reason_is_data_only_and_never_selects_actions(
    tmp_path: Path, reason: str | None
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason=reason)
        failed = await controls.snapshot(conn, ISSUE_ID)
        assert failed.reason == reason
        assert failed.allowed_actions == controls.allowed_actions(MODES.PLAYING, OUTCOMES.FAILED)

        # The same reason on a still-running attempt widens nothing.
        await _record_implement(
            conn,
            outcome=OUTCOMES.RUNNING,
            reason=reason,
            at="2026-08-27T10:10:00+00:00",
        )
        running = await controls.snapshot(conn, ISSUE_ID)
        assert running.reason == reason
        assert ACTIONS.RETRY not in running.allowed_actions
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_disallowed_action_is_rejected_and_writes_nothing(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.RUNNING)
        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-1",
            action_id="c-1",
            at="2026-08-27T10:01:00+00:00",
        )
        assert not result.accepted
        assert result.rejection
        assert result.snapshot.outcome is OUTCOMES.RUNNING
        assert await controls.history(conn, ISSUE_ID) == []
        assert (await controls.snapshot(conn, ISSUE_ID)).mode is MODES.PLAYING
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_duplicate_action_id_is_applied_exactly_once(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")
        first = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="web:cmd-1",
            action_id="cmd-1",
            at="2026-08-27T10:01:00+00:00",
        )
        second = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="web:cmd-1",
            action_id="cmd-1",
            at="2026-08-27T10:02:00+00:00",
        )
        assert first.accepted
        assert not second.accepted
        assert len(await controls.history(conn, ISSUE_ID)) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_pause_while_running_is_pausing_then_play_resumes(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.RUNNING)
        paused = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.PAUSE,
            actor="web:a-1",
            action_id="a-1",
            at="2026-08-27T10:01:00+00:00",
        )
        # A live attempt cannot stop instantly, so Pause lands in `pausing`.
        assert paused.accepted
        assert paused.snapshot.mode is MODES.PAUSING
        assert paused.snapshot.outcome is OUTCOMES.RUNNING
        assert ACTIONS.PAUSE not in paused.snapshot.allowed_actions

        resumed = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.PLAY,
            actor="web:a-2",
            action_id="a-2",
            at="2026-08-27T10:02:00+00:00",
        )
        assert resumed.accepted
        assert resumed.snapshot.mode is MODES.PLAYING
        assert ACTIONS.PLAY not in resumed.snapshot.allowed_actions
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_pause_and_abort_of_a_failed_attempt_land_paused(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")
        aborted = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.ABORT,
            actor="web:a-1",
            action_id="a-1",
            at="2026-08-27T10:01:00+00:00",
        )
        assert aborted.accepted
        assert aborted.snapshot.mode is MODES.PAUSED
        # Stopping the pipeline does not rewrite what the last attempt did.
        assert aborted.snapshot.outcome is OUTCOMES.FAILED
        assert ACTIONS.PLAY in aborted.snapshot.allowed_actions

        skipped = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.SKIP,
            actor="web:a-2",
            action_id="a-2",
            at="2026-08-27T10:02:00+00:00",
        )
        assert skipped.accepted
        assert skipped.snapshot.outcome is OUTCOMES.SKIPPED
        assert skipped.snapshot.mode is MODES.PLAYING
        assert skipped.snapshot.reason is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_park_from_before_this_module_reads_as_a_failed_attempt(tmp_path: Path) -> None:
    """A wait parked before the control table existed has no control row; the
    snapshot falls back to the durable park so `$retry` stays reachable."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=ISSUE_ID,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-08-27T09:00:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "run-1",
            db.runs.FAILED_STATUS,
            kind="agent_nonzero_exit",
            detail="agent exited 2",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id=ISSUE_ID,
            run_id="run-1",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-08-27T09:10:00+00:00",
        )
        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.stage == "implement"
        assert snap.outcome is OUTCOMES.FAILED
        assert snap.reason == "agent exited 2"
        assert snap.run_id == "run-1"
        assert ACTIONS.RETRY in snap.allowed_actions
    finally:
        await conn.close()


# --- The tracer: implement failure → Retry → one fresh attempt ---------------


def _head_shas(workspace: Path) -> list[str]:
    out = subprocess.run(
        ["git", "log", "--format=%H"],
        cwd=workspace,
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout.split()


def _failing_implement_runner(workspace: Path) -> _RecordingRunner:
    def _commit_then_fail(spec: RunnerSpec) -> None:
        if spec.stage == "implement":
            (workspace / "partial.py").write_text("print('partial')\n")
            _git(workspace, "add", "-A")
            _git(workspace, "commit", "-m", "partial agent work")

    return _RecordingRunner(
        [
            RunnerEvent(kind="started", pid=4242),
            RunnerEvent(kind="stderr", line="boom"),
            RunnerEvent(kind="exit", returncode=2),
        ],
        on_run=_commit_then_fail,
    )


def _succeeding_implement_runner(workspace: Path) -> _RecordingRunner:
    def _commit_retry(spec: RunnerSpec) -> None:
        if spec.stage == "implement":
            _git(workspace, "commit", "--allow-empty", "-m", "retry work")

    return _RecordingRunner(
        [
            RunnerEvent(kind="started", pid=5151),
            RunnerEvent(
                kind="stdout",
                line=_done_result_line("Retried.\n\nSYMPHONY_DONE"),
            ),
            RunnerEvent(kind="exit", returncode=0),
        ],
        on_run=_commit_retry,
    )


def _orchestrator(
    cfg: Config,
    conn: aiosqlite.Connection,
    runner: object,
    workspace_path: Path,
) -> Orchestrator:
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=workspace_path)
    workspace.release = MagicMock()

    gh = MagicMock()
    gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
    gh.pr_comment = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="trunk")

    linear = AsyncMock()
    linear.issues_in_state = AsyncMock(return_value=[_issue()])
    linear.lookup_issue = AsyncMock(return_value=_issue())
    linear.post_comment = AsyncMock(return_value="cmt-1")
    linear.move_issue = AsyncMock()

    orch = Orchestrator(
        cfg,
        linear,
        conn,
        runner=runner,  # type: ignore[arg-type]
        gh=gh,
        workspace=workspace,
        push_fn=AsyncMock(),
    )
    orch._states = {"ENG": _states()}  # noqa: SLF001
    return orch


def _retry_intent(comment_id: str) -> SlashIntent:
    return SlashIntent(
        kind=SlashKind.RETRY,
        comment_id=comment_id,
        created_at="2026-08-27T10:00:00+00:00",
        text="$retry",
    )


def _cfg(tmp_path: Path, binding: object) -> Config:
    return Config(
        repos=[binding],  # type: ignore[list-item]
        log_root=tmp_path / "logs",
        workspace_root=tmp_path / "ws",
        db_path=tmp_path / "s.sqlite",
    )


@pytest.mark.asyncio
async def test_implement_retry_records_transition_then_starts_one_fresh_attempt(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")

        first = _orchestrator(cfg, conn, _failing_implement_runner(workspace_path), workspace_path)
        await _scan_and_wait(first, binding)

        failed_run = (await db.runs.history_for_issue(conn, ISSUE_ID))[0]
        assert failed_run.status == "failed"

        # The failure is visible through the control seam, with Retry offered,
        # and it is the park's own durable row — not the legacy fallback.
        assert await db.pipeline_controls.get(conn, ISSUE_ID) is not None
        parked = await controls.snapshot(conn, ISSUE_ID)
        assert parked.stage == "implement"
        assert parked.outcome is OUTCOMES.FAILED
        assert parked.reason
        assert ACTIONS.RETRY in parked.allowed_actions

        checkpoint = _head_shas(workspace_path)[0]

        await first._handle_implement_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, failed_run.id, _retry_intent("c-retry")
        )

        # The accepted transition is durable, with actor and time.
        actions = await controls.history(conn, ISSUE_ID)
        assert [(a.action, a.from_outcome, a.to_mode) for a in actions] == [
            ("retry", "failed", "playing")
        ]
        assert actions[0].actor
        assert actions[0].ts
        after = await controls.snapshot(conn, ISSUE_ID)
        assert after.outcome is OUTCOMES.PENDING
        assert ACTIONS.RETRY not in after.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is None

        # A re-delivered `$retry` (a restored stale wait, a webhook replay)
        # cannot double-dispatch: Retry is no longer an allowed action.
        await db.operator_waits.upsert(
            conn,
            issue_id=ISSUE_ID,
            run_id=failed_run.id,
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at="2026-08-27T10:01:00+00:00",
        )
        first._implement_failed_run_bindings[failed_run.id] = binding  # noqa: SLF001
        tracker = first.tracker(binding)
        moves = tracker.move_issue.await_count  # type: ignore[attr-defined]
        await first._handle_implement_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, failed_run.id, _retry_intent("c-retry-2")
        )
        assert tracker.move_issue.await_count == moves  # type: ignore[attr-defined]
        assert len(await controls.history(conn, ISSUE_ID)) == 1
        await db.operator_waits.delete(conn, ISSUE_ID, failed_run.id)

        # The fresh attempt runs the implementer exactly once, in the same
        # workspace, on top of the failed attempt's commit.
        second = _orchestrator(
            cfg, conn, _succeeding_implement_runner(workspace_path), workspace_path
        )
        await _scan_and_wait(second, binding)

        specs = second._runner.specs  # type: ignore[attr-defined]  # noqa: SLF001
        assert [s.stage for s in specs] == ["implement"]
        assert [s.workspace_path for s in specs] == [workspace_path]
        assert checkpoint in _head_shas(workspace_path)
        implement_runs = [
            r for r in await db.runs.history_for_issue(conn, ISSUE_ID) if r.stage == "implement"
        ]
        assert len(implement_runs) == 2
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_whose_side_effect_fails_releases_the_transition(
    tmp_path: Path,
) -> None:
    """A recorded transition whose side effect then fails is released, so the
    next poll tick can re-deliver the same `$retry` instead of finding a park
    that no longer offers Retry."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)

        orch = _orchestrator(cfg, conn, _failing_implement_runner(workspace_path), workspace_path)
        await _seed_issue(conn)
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=ISSUE_ID,
            stage="implement",
            status="failed",
            pid=None,
            started_at="2026-08-27T09:00:00+00:00",
        )
        await orch._track_implement_failed_wait(ISSUE_ID, "run-1", binding)  # noqa: SLF001
        tracker = orch.tracker(binding)
        tracker.move_issue.side_effect = LinearError("tracker unreachable")  # type: ignore[attr-defined]

        with pytest.raises(SlashHandlerFailure):
            await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
                ISSUE_ID, "run-1", _retry_intent("c-retry")
            )

        assert await controls.history(conn, ISSUE_ID) == []
        released = await controls.snapshot(conn, ISSUE_ID)
        assert released.outcome is OUTCOMES.FAILED
        assert ACTIONS.RETRY in released.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is not None

        # The re-delivered command lands this time.
        tracker.move_issue.side_effect = None  # type: ignore[attr-defined]
        await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, "run-1", _retry_intent("c-retry")
        )
        assert [a.action for a in await controls.history(conn, ISSUE_ID)] == ["retry"]
        assert await db.operator_waits.get(conn, ISSUE_ID) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_retry_the_control_model_rejects_changes_nothing(
    tmp_path: Path,
) -> None:
    """A `$retry` the control model rejects must leave the tracker and the
    durable wait untouched — no partial application."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)

        orch = _orchestrator(cfg, conn, _failing_implement_runner(workspace_path), workspace_path)
        await _seed_issue(conn)
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=ISSUE_ID,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-08-27T09:00:00+00:00",
        )
        await orch._track_implement_failed_wait(ISSUE_ID, "run-1", binding)  # noqa: SLF001
        # The attempt is still live, so Retry is not an allowed action.
        await _record_implement(conn, outcome=OUTCOMES.RUNNING, at="2026-08-27T09:05:00+00:00")

        await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, "run-1", _retry_intent("c-retry")
        )

        tracker = orch.tracker(binding)
        tracker.move_issue.assert_not_awaited()  # type: ignore[attr-defined]
        assert await db.operator_waits.get(conn, ISSUE_ID) is not None
        assert await controls.history(conn, ISSUE_ID) == []
        posted = [str(c.args[1]) for c in tracker.post_comment.await_args_list]  # type: ignore[attr-defined]
        assert any("$retry" in body for body in posted)
    finally:
        await conn.close()
