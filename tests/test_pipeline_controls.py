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

import asyncio
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

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
async def test_replaying_an_already_applied_action_id_is_rejected_as_a_duplicate(
    tmp_path: Path,
) -> None:
    """Unlike `test_duplicate_action_id_is_applied_exactly_once` above, this
    replays an action that is still *allowed* after it lands — Abort stays
    allowed in every mode/outcome — so the replay reaches `apply`'s duplicate
    `action_id` pre-check instead of being turned away earlier by the
    disallowed-action check on state alone."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")
        first = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.ABORT,
            actor="web:cmd-1",
            action_id="cmd-1",
            at="2026-08-27T10:01:00+00:00",
        )
        assert first.accepted
        assert ACTIONS.ABORT in first.snapshot.allowed_actions

        second = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.ABORT,
            actor="web:cmd-1",
            action_id="cmd-1",
            at="2026-08-27T10:02:00+00:00",
        )
        assert not second.accepted
        assert second.rejection == "abort cmd-1 was already applied"
        assert len(await controls.history(conn, ISSUE_ID)) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_duplicate_action_id_that_slips_past_the_precheck_is_still_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`apply`'s duplicate `action_id` pre-check is what normally turns a
    replayed `action_id` into a clean rejection before any write is
    attempted. This forces that pre-check to miss the existing row — as a
    race the per-issue lock doesn't cover could — so the duplicate instead
    hits `record_action`'s primary-key `IntegrityError`, and asserts that
    `apply`'s `record_action` `IntegrityError` fallback turns that into the
    same clean rejection rather than raising."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")
        first = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.ABORT,
            actor="web:cmd-1",
            action_id="cmd-1",
            at="2026-08-27T10:01:00+00:00",
        )
        assert first.accepted

        real_get_action = db.pipeline_controls.get_action
        calls = 0

        async def missing_precheck_then_real(
            conn_: aiosqlite.Connection, issue_id_: str, action_id_: str
        ) -> object:
            nonlocal calls
            calls += 1
            if calls == 1:
                return None
            return await real_get_action(conn_, issue_id_, action_id_)

        monkeypatch.setattr(db.pipeline_controls, "get_action", missing_precheck_then_real)

        second = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.ABORT,
            actor="web:cmd-1",
            action_id="cmd-1",
            at="2026-08-27T10:02:00+00:00",
        )
        assert not second.accepted
        assert second.rejection == "abort cmd-1 was already applied"
        assert len(await controls.history(conn, ISSUE_ID)) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_non_duplicate_integrity_error_still_propagates(tmp_path: Path) -> None:
    """`apply`'s `record_action` `IntegrityError` fallback only exists to
    reclassify a replayed `action_id`; a genuine constraint violation on the
    same INSERT — here, `record_action`'s foreign key on an issue that was
    never seeded — must not be swallowed as a false "already applied"."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        with pytest.raises(sqlite3.IntegrityError):
            await controls.apply(
                conn,
                "no-such-issue",
                ACTIONS.ABORT,
                actor="web:cmd-1",
                action_id="cmd-1",
                at="2026-08-27T10:01:00+00:00",
            )
        assert await controls.history(conn, "no-such-issue") == []
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
async def test_pausing_settles_to_paused_once_the_live_attempt_finishes(
    tmp_path: Path,
) -> None:
    """`PAUSING` is a transient "stop requested while an attempt was still
    live" state, not an operator intent: once that attempt finishes — here,
    with a failure — `record_stage_outcome` must resolve it to `PAUSED`
    instead of leaving the durable mode stuck on `pausing` forever."""
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
        assert paused.accepted
        assert paused.snapshot.mode is MODES.PAUSING

        await _record_implement(
            conn,
            outcome=OUTCOMES.FAILED,
            reason="boom",
            at="2026-08-27T10:02:00+00:00",
        )
        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.mode is MODES.PAUSED
        assert snap.outcome is OUTCOMES.FAILED
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


def _retry_intent(comment_id: str, *, author: str = "") -> SlashIntent:
    return SlashIntent(
        kind=SlashKind.RETRY,
        comment_id=comment_id,
        created_at="2026-08-27T10:00:00+00:00",
        text="$retry",
        author=author,
    )


def test_control_actor_falls_back_to_web_origin_for_web_button_commands() -> None:
    # A web-button command carries a synthetic `web-`-prefixed comment id and
    # no author; the actor is the origin tag plus the comment id with that
    # prefix stripped, not a doubled `web-` re-encoding of it.
    intent = SlashIntent(
        kind=SlashKind.RETRY,
        comment_id="web-c-retry",
        created_at="2026-08-27T10:00:00+00:00",
        text="$retry",
    )
    assert Orchestrator._control_actor(intent) == "web:c-retry"  # noqa: SLF001


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
        # A tracker comment with no resolvable author: the actor falls back to
        # the synthetic origin:id, not a bare re-encoding of action_id.
        assert actions[0].actor == "tracker:c-retry"
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
async def test_retry_records_the_comment_authors_name_as_actor(tmp_path: Path) -> None:
    """A tracker comment's author is a real person, not just its comment id.
    `_control_actor` must record that name so the durable action row
    identifies who asked, not `action_id` re-encoded."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")

        orch = _orchestrator(cfg, conn, _failing_implement_runner(workspace_path), workspace_path)
        await _scan_and_wait(orch, binding)
        failed_run = (await db.runs.history_for_issue(conn, ISSUE_ID))[0]

        await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, failed_run.id, _retry_intent("c-retry", author="Jane Operator")
        )

        actions = await controls.history(conn, ISSUE_ID)
        assert actions[0].actor == "Jane Operator"
    finally:
        await conn.close()


def _stop_intent(comment_id: str) -> SlashIntent:
    return SlashIntent(
        kind=SlashKind.STOP,
        comment_id=comment_id,
        created_at="2026-08-27T10:00:00+00:00",
        text="$stop",
    )


@pytest.mark.asyncio
async def test_stop_settles_the_control_row_so_it_stops_offering_retry(
    tmp_path: Path,
) -> None:
    """`$stop`/`$reject` on a failed-implement park moves the issue to
    blocked and drops the operator wait, but with no park left behind, the
    durable control row must not keep advertising Retry — that stale-Retry
    shape is exactly what `_track_implement_failed_wait`'s own invariant
    treats as a bug worth compensating for (SYM-244 review)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")

        orch = _orchestrator(cfg, conn, _failing_implement_runner(workspace_path), workspace_path)
        await _scan_and_wait(orch, binding)
        failed_run = (await db.runs.history_for_issue(conn, ISSUE_ID))[0]

        parked = await controls.snapshot(conn, ISSUE_ID)
        assert ACTIONS.RETRY in parked.allowed_actions

        await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, failed_run.id, _stop_intent("c-stop")
        )

        tracker = orch.tracker(binding)
        assert tracker.move_issue.await_args_list[-1] == call(  # type: ignore[attr-defined]
            ISSUE_ID, "state-bl"
        )
        assert await db.operator_waits.get(conn, ISSUE_ID) is None

        settled = await controls.snapshot(conn, ISSUE_ID)
        assert ACTIONS.RETRY not in settled.allowed_actions
        assert ACTIONS.SKIP not in settled.allowed_actions
        assert settled.outcome is OUTCOMES.SKIPPED
    finally:
        await conn.close()


async def _parked_for_stop(
    conn: aiosqlite.Connection, tmp_path: Path
) -> tuple[Orchestrator, object]:
    binding = _no_review_binding(auto_merge=False)
    cfg = _cfg(tmp_path, binding)
    orch = _orchestrator(cfg, conn, AsyncMock(), tmp_path)
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
    return orch, binding


@pytest.mark.asyncio
async def test_stop_clear_recovers_when_the_wait_delete_fails_with_no_foreign_interference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reproduces the reviewer's proof of a stuck park: before the SAVEPOINT
    fix, `record_stage_outcome(commit=False)` and the wait delete shared no
    transactional boundary of their own, so a delete failure left the
    `skipped` control write sitting uncommitted with nothing to undo it —
    a later unrelated `commit=True` DAO call would make it durable while the
    wait stayed open forever (`allowed_actions` never offering Retry again).
    With the SAVEPOINT in place, a delete failure with no other interference
    now rolls back cleanly to the pre-clear, still-retryable park."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        orch, _binding = await _parked_for_stop(conn, tmp_path)
        monkeypatch.setattr(
            db.operator_waits,
            "delete",
            AsyncMock(side_effect=RuntimeError("boom")),
        )

        with pytest.raises(RuntimeError, match="boom"):
            await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
                ISSUE_ID, "run-1", _stop_intent("c-stop")
            )

        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.outcome is OUTCOMES.FAILED
        assert ACTIONS.RETRY in snap.allowed_actions
        wait = await db.operator_waits.get(conn, ISSUE_ID)
        assert wait is not None
        assert wait.run_id == "run-1"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stop_clear_finishes_for_real_when_a_foreign_commit_lands_and_the_delete_then_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same shape as
    `test_track_implement_failed_wait_compensates_when_upsert_fails_after_a_foreign_commit`,
    but for the clear path, and the reviewer's exact proof: an unrelated
    `commit=True` DAO write (e.g. `db.issues.upsert`) lands mid-window,
    destroying this SAVEPOINT, immediately before the wait delete itself
    fails. Unlike the *create* path in `_track_implement_failed_wait` (whose
    correct undo is "restore the previous row, no park existed before"),
    this call is *clearing* a park that did exist — so the always-correct
    convergence here is finishing the clear for real (`skipped`, wait gone),
    not restoring `failed` with the wait already gone underneath it, which
    would just be the stuck shape with the labels swapped."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        orch, _binding = await _parked_for_stop(conn, tmp_path)

        real_delete = db.operator_waits.delete
        calls = 0

        async def _foreign_commit_then_boom_once(
            conn: aiosqlite.Connection, issue_id: str, run_id: str | None = None, **kwargs: object
        ) -> None:
            nonlocal calls
            calls += 1
            if calls > 1:
                # The redo triggered by this call's own compensation must
                # actually succeed for the clear to converge — only the
                # first attempt hits the foreign interference.
                await real_delete(conn, issue_id, run_id, **kwargs)
                return
            await db.issues.upsert(
                conn,
                id=ISSUE_ID,
                identifier="ENG-1",
                title="Add authentication",
                team_key="ENG",
            )
            raise RuntimeError("boom")

        monkeypatch.setattr(db.operator_waits, "delete", _foreign_commit_then_boom_once)

        with pytest.raises(RuntimeError, match="boom"):
            await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
                ISSUE_ID, "run-1", _stop_intent("c-stop")
            )

        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.outcome is OUTCOMES.SKIPPED
        assert ACTIONS.RETRY not in snap.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stop_clear_recovers_from_a_foreign_rollback_in_its_savepoint_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror of
    `test_track_implement_failed_wait_recovers_from_a_foreign_rollback_in_its_savepoint_window`
    for the clear path: a foreign `conn.rollback()` landing inside the clear's
    savepoint window destroys the control-row write and the wait delete
    without an exception of its own. Before this fix's `release_savepoint`
    miss handling, that would return as if the clear had landed, leaving the
    park (and its Retry) intact despite the issue already being moved to
    blocked in the tracker."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        orch, _binding = await _parked_for_stop(conn, tmp_path)

        real_delete = db.operator_waits.delete
        calls = 0

        async def _delete_then_foreign_rollback_once(
            conn: aiosqlite.Connection, issue_id: str, run_id: str | None = None, **kwargs: object
        ) -> None:
            nonlocal calls
            calls += 1
            if calls > 1:
                # The redo triggered by the rollback's `release_savepoint`
                # miss must actually succeed for the clear to converge —
                # only the first attempt hits the foreign rollback.
                await real_delete(conn, issue_id, run_id, **kwargs)
                return
            await real_delete(conn, issue_id, run_id, commit=False)
            await conn.rollback()

        monkeypatch.setattr(db.operator_waits, "delete", _delete_then_foreign_rollback_once)

        await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, "run-1", _stop_intent("c-stop")
        )

        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.outcome is OUTCOMES.SKIPPED
        assert ACTIONS.RETRY not in snap.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is None
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
async def test_retry_whose_side_effect_raises_non_linear_error_is_still_retryable(
    tmp_path: Path,
) -> None:
    """A side effect failure that isn't a `LinearError` — a malformed payload,
    cancellation, plain daemon death — must release the accepted transition
    exactly like a `LinearError` does, instead of leaving a park stuck at a
    pending outcome that no longer offers Retry."""
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
        tracker.move_issue.side_effect = RuntimeError("boom")  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="boom"):
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
            ISSUE_ID, "run-1", _retry_intent("c-retry-fresh")
        )
        assert [a.action for a in await controls.history(conn, ISSUE_ID)] == ["retry"]
        assert await db.operator_waits.get(conn, ISSUE_ID) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_whose_post_comment_fails_after_move_issue_succeeds_is_still_recoverable(
    tmp_path: Path,
) -> None:
    """Every other side-effect-failure test injects the failure into
    `move_issue` itself, which the retry handler already released around —
    but once `move_issue` succeeds, the transition happened and the tail
    (`post_comment` then `_clear_operator_wait`) can still fail. The wait is
    still open and dispatch is still blocked at that point, so a failure
    there must release the transition too, instead of leaving a control row
    stuck `pending` with the park never actually resolved (SYM-244 review)."""
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
        tracker.post_comment.side_effect = RuntimeError("boom")  # type: ignore[attr-defined]

        with pytest.raises(RuntimeError, match="boom"):
            await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
                ISSUE_ID, "run-1", _retry_intent("c-retry")
            )

        assert await controls.history(conn, ISSUE_ID) == []
        released = await controls.snapshot(conn, ISSUE_ID)
        assert released.outcome is OUTCOMES.FAILED
        assert ACTIONS.RETRY in released.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is not None

        # The re-delivered command lands this time.
        tracker.post_comment.side_effect = None  # type: ignore[attr-defined]
        await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, "run-1", _retry_intent("c-retry-fresh")
        )
        assert [a.action for a in await controls.history(conn, ISSUE_ID)] == ["retry"]
        assert await db.operator_waits.get(conn, ISSUE_ID) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_whose_clear_operator_wait_fails_after_move_issue_succeeds_is_still_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror of the `post_comment` case above for the last step of the retry
    tail: `_clear_operator_wait`'s delete raising after `move_issue` (and
    `post_comment`) already succeeded must still release the accepted
    transition instead of leaving the control row durably `pending` with the
    park never actually cleared (SYM-244 review)."""
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

        real_delete = db.operator_waits.delete
        calls = 0

        async def _raise_once(
            conn: aiosqlite.Connection, issue_id: str, run_id: str | None = None, **kwargs: object
        ) -> None:
            nonlocal calls
            calls += 1
            if calls > 1:
                await real_delete(conn, issue_id, run_id, **kwargs)
                return
            raise RuntimeError("boom")

        monkeypatch.setattr(db.operator_waits, "delete", _raise_once)

        with pytest.raises(RuntimeError, match="boom"):
            await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
                ISSUE_ID, "run-1", _retry_intent("c-retry")
            )

        assert await controls.history(conn, ISSUE_ID) == []
        released = await controls.snapshot(conn, ISSUE_ID)
        assert released.outcome is OUTCOMES.FAILED
        assert ACTIONS.RETRY in released.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is not None

        # The re-delivered command lands this time — the binding dict entry
        # was popped by the failed clear above, so this exercises the
        # restore-from-durable-wait fallback rather than the fast path.
        await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, "run-1", _retry_intent("c-retry-fresh")
        )
        assert [a.action for a in await controls.history(conn, ISSUE_ID)] == ["retry"]
        assert await db.operator_waits.get(conn, ISSUE_ID) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_release_does_not_clobber_a_control_row_settled_during_the_side_effect(
    tmp_path: Path,
) -> None:
    """`release` undoes an accepted transition whose side effect then failed
    by restoring the pre-transition snapshot — but if something else (e.g.
    the reconciler's cancel-clear) settles the control row to a newer state
    while that side effect is still in flight, blindly restoring the old
    snapshot would clobber it. Proven here by having `move_issue` itself
    perform that concurrent settle (clearing the wait, recording a terminal
    `skipped` outcome) before raising, mirroring how the reconciler's
    cancel-clear can race a live `move_issue` call (SYM-244 review)."""
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

        async def _cancel_clear_then_fail(*_args: object, **_kwargs: object) -> None:
            await db.operator_waits.delete(conn, ISSUE_ID, "run-1")
            await controls.record_stage_outcome(
                conn,
                ISSUE_ID,
                stage=controls.IMPLEMENT_STAGE,
                outcome=OUTCOMES.SKIPPED,
                reason="tracker issue canceled",
                run_id="run-1",
                at="2026-08-27T10:05:30+00:00",
            )
            raise LinearError("tracker unreachable")

        tracker.move_issue.side_effect = _cancel_clear_then_fail  # type: ignore[attr-defined]

        with pytest.raises(SlashHandlerFailure):
            await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
                ISSUE_ID, "run-1", _retry_intent("c-retry")
            )

        # The action row is gone (the command stays re-deliverable), but the
        # newer, authoritative `skipped` state the concurrent settle wrote
        # must survive untouched — not get clobbered back to `failed`.
        assert await controls.history(conn, ISSUE_ID) == []
        current = await controls.snapshot(conn, ISSUE_ID)
        assert current.outcome is OUTCOMES.SKIPPED
        assert ACTIONS.RETRY not in current.allowed_actions
        assert ACTIONS.SKIP not in current.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_startup_sweep_reconciles_interrupted_retry_to_failed(
    tmp_path: Path,
) -> None:
    """A process death between Retry's commit and the wait being cleared (the
    tracker move landed, but the daemon died before `_clear_operator_wait` ran)
    leaves a control row pending with the implement-failed park still open on
    restart. The startup sweep must reconcile that back to failed so
    Retry/Skip are offered again instead of a permanently stuck park — without
    changing what a still-live process does with the same shape of row (see
    `test_implement_retry_records_transition_then_starts_one_fresh_attempt`'s
    duplicate-rejection check)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
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
        await db.runs.update_status(
            conn,
            "run-1",
            "failed",
            ended_at="2026-08-27T09:05:00+00:00",
            kind="agent_error",
            detail="agent exited 2",
        )
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="agent exited 2")
        # The park predates the retry, same as the real ingress: created once,
        # by the original implement failure.
        await db.operator_waits.upsert(
            conn,
            issue_id=ISSUE_ID,
            run_id="run-1",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="eng-1",
            created_at="2026-08-27T09:05:00+00:00",
        )
        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-retry",
            action_id="c-retry",
            at="2026-08-27T10:05:00+00:00",
        )
        assert result.accepted
        # The side effect and `_clear_operator_wait` never ran: the park is
        # still open, exactly as it would be after a crash right after commit.
        pending = await controls.snapshot(conn, ISSUE_ID)
        assert pending.outcome is OUTCOMES.PENDING

        await controls.reconcile_interrupted_retries(conn, at="2026-08-27T11:00:00+00:00")

        reconciled = await controls.snapshot(conn, ISSUE_ID)
        assert reconciled.outcome is OUTCOMES.FAILED
        assert ACTIONS.RETRY in reconciled.allowed_actions
        assert reconciled.run_id == "run-1"

        # The ingress that died mid-side-effect will re-deliver the very same
        # tracker comment on its next tick. The sweep must have dropped the
        # interrupted retry's own action row so that identical redelivery is
        # accepted, not rejected as a duplicate of a command whose side effect
        # never ran.
        redelivered = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-retry",
            action_id="c-retry",
            at="2026-08-27T11:00:01+00:00",
        )
        assert redelivered.accepted
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_startup_sweep_recovers_from_a_foreign_rollback_inside_its_savepoint_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mirror of `test_apply_recovers_from_a_foreign_rollback_inside_its_savepoint_window`
    for the startup sweep's own reset: a foreign `conn.rollback()` landing
    inside `reconcile_interrupted_retries`'s savepoint window destroys the
    reset back to failed instead of making it durable. Before this fix, the
    sweep would treat the missing `RELEASE SAVEPOINT` as if a foreign commit
    had already landed its rows and return with the control row still
    `pending` and the interrupted retry's action row still on disk — a park
    the sweep can never revisit, since it only repairs rows still in that
    exact shape."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
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
        await db.runs.update_status(
            conn,
            "run-1",
            "failed",
            ended_at="2026-08-27T09:05:00+00:00",
            kind="agent_error",
            detail="agent exited 2",
        )
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="agent exited 2")
        await db.operator_waits.upsert(
            conn,
            issue_id=ISSUE_ID,
            run_id="run-1",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="eng-1",
            created_at="2026-08-27T09:05:00+00:00",
        )
        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-retry",
            action_id="c-retry",
            at="2026-08-27T10:05:00+00:00",
        )
        assert result.accepted
        pending = await controls.snapshot(conn, ISSUE_ID)
        assert pending.outcome is OUTCOMES.PENDING

        real_put = db.pipeline_controls.put

        async def _put_then_foreign_rollback(
            conn: aiosqlite.Connection, **kwargs: object
        ) -> None:
            await real_put(conn, **kwargs)
            await conn.rollback()

        monkeypatch.setattr(db.pipeline_controls, "put", _put_then_foreign_rollback)

        await controls.reconcile_interrupted_retries(conn, at="2026-08-27T11:00:00+00:00")

        reconciled = await controls.snapshot(conn, ISSUE_ID)
        assert reconciled.outcome is OUTCOMES.FAILED
        assert reconciled.run_id == "run-1"
        assert await db.pipeline_controls.get_action(conn, ISSUE_ID, "c-retry") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_orchestrator_run_wires_the_interrupted_retry_sweep_into_startup(
    tmp_path: Path,
) -> None:
    """`reconcile_interrupted_retries` is only exercised above as a bare
    function call; nothing proves `Orchestrator.run()` actually calls it on
    startup, so deleting that wiring would leave the suite green. Drive
    `run()` itself — with no configured repos, so the poll loop it enters has
    nothing to scan — and observe the same interrupted retry get reset to
    failed before the daemon is shut down."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            poll_interval_secs=300,
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=AsyncMock(),
            gh=MagicMock(),
            push_fn=AsyncMock(),
        )

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
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="agent exited 2")
        await db.operator_waits.upsert(
            conn,
            issue_id=ISSUE_ID,
            run_id="run-1",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="eng-1",
            created_at="2026-08-27T09:05:00+00:00",
        )
        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-retry",
            action_id="c-retry",
            at="2026-08-27T10:05:00+00:00",
        )
        assert result.accepted
        pending = await controls.snapshot(conn, ISSUE_ID)
        assert pending.outcome is OUTCOMES.PENDING

        run_task = asyncio.create_task(orch.run())
        try:
            for _ in range(200):
                if (await controls.snapshot(conn, ISSUE_ID)).outcome is OUTCOMES.FAILED:
                    break
                await asyncio.sleep(0.01)
            else:
                pytest.fail("orchestrator.run() never reconciled the interrupted retry")
        finally:
            await orch.shutdown()
            await asyncio.wait_for(run_task, timeout=1)

        reconciled = await controls.snapshot(conn, ISSUE_ID)
        assert reconciled.outcome is OUTCOMES.FAILED
        assert ACTIONS.RETRY in reconciled.allowed_actions
        assert reconciled.run_id == "run-1"

        # The dropped action row (same guarantee as the bare-function test
        # above) proves this went through the real `reconcile_interrupted_retries`
        # codepath, not just a coincidentally-failed snapshot.
        redelivered = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-retry",
            action_id="c-retry",
            at="2026-08-27T11:00:01+00:00",
        )
        assert redelivered.accepted
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


@pytest.mark.asyncio
async def test_concurrent_retries_cannot_both_be_accepted(tmp_path: Path) -> None:
    """Two different action ids racing `apply(RETRY)` for the same issue must
    not both be accepted: a UI Retry click racing a tracker `$retry` comment
    would otherwise both read the same "failed" snapshot, both see Retry
    allowed, and both start a fresh implement attempt."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await _record_implement(conn, outcome=OUTCOMES.FAILED, at="2026-08-27T10:00:00+00:00")

        first, second = await asyncio.gather(
            controls.apply(
                conn,
                ISSUE_ID,
                ACTIONS.RETRY,
                actor="tracker:c-1",
                action_id="c-1",
                at="2026-08-27T10:01:00+00:00",
            ),
            controls.apply(
                conn,
                ISSUE_ID,
                ACTIONS.RETRY,
                actor="web:cmd-1",
                action_id="cmd-1",
                at="2026-08-27T10:01:01+00:00",
            ),
        )

        accepted = [result for result in (first, second) if result.accepted]
        history = await controls.history(conn, ISSUE_ID)
        assert len(accepted) == 1, (
            f"both accepted -> double dispatch; actions={[row.action_id for row in history]}"
        )
        assert len(history) == 1
    finally:
        await conn.close()


def test_lock_and_write_lock_survive_a_fresh_event_loop() -> None:
    """`_lock`/`_write_lock` used to be `asyncio.Lock`s created at import
    time, outside any event loop. `asyncio.Lock` binds to whichever running
    loop first *contends* it (waits on it while it is held), so a second,
    unrelated `asyncio.run()` call that genuinely contends the same lock
    object raised `RuntimeError: ... is bound to a different event loop` —
    proven by two separate `asyncio.run()` calls each racing two tasks
    through `guard_writes` for the same issue id. Both locks must instead be
    created lazily per running loop so a second, later loop never collides
    with a lock bound by an earlier one."""

    async def _contend() -> None:
        async def hold() -> None:
            async with controls.guard_writes(ISSUE_ID):
                await asyncio.sleep(0.01)

        await asyncio.gather(hold(), hold())

    asyncio.run(_contend())
    asyncio.run(_contend())


@pytest.mark.asyncio
async def test_apply_failure_for_one_issue_does_not_corrupt_another_issue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_write_lock` fully serializes `apply`'s SAVEPOINT-to-commit window
    across issues (see its docstring), so two concurrent `apply` calls for
    different issues never interleave on the connection. What this exercises
    is per-issue isolation under that serialization: a failure in one call's
    `put` must roll back only that call's own writes, must leave the other
    issue's action and control rows untouched, and must not raise
    `sqlite3.OperationalError` out of either call."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue_a, issue_b = ISSUE_ID, "iss-2"
        await _seed_issue(conn)
        await db.issues.upsert(
            conn,
            id=issue_b,
            identifier="ENG-2",
            title="Add authorization",
            team_key="ENG",
        )
        await controls.record_stage_outcome(
            conn,
            issue_a,
            stage="implement",
            outcome=OUTCOMES.FAILED,
            reason=None,
            run_id="run-1",
            at="2026-08-27T10:00:00+00:00",
        )
        await controls.record_stage_outcome(
            conn,
            issue_b,
            stage="implement",
            outcome=OUTCOMES.FAILED,
            reason=None,
            run_id="run-2",
            at="2026-08-27T10:00:00+00:00",
        )

        real_put = db.pipeline_controls.put
        failed_once = False

        async def _flaky_put(
            conn: aiosqlite.Connection, *, issue_id: str, **kwargs: object
        ) -> None:
            nonlocal failed_once
            if issue_id == issue_b and not failed_once:
                failed_once = True
                raise RuntimeError("boom")
            await real_put(conn, issue_id=issue_id, **kwargs)

        monkeypatch.setattr(db.pipeline_controls, "put", _flaky_put)

        results = await asyncio.gather(
            controls.apply(
                conn,
                issue_a,
                ACTIONS.RETRY,
                actor="tracker:c-1",
                action_id="c-1",
                at="2026-08-27T10:01:00+00:00",
            ),
            controls.apply(
                conn,
                issue_b,
                ACTIONS.RETRY,
                actor="tracker:c-2",
                action_id="c-2",
                at="2026-08-27T10:01:01+00:00",
            ),
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, BaseException):
                assert isinstance(result, RuntimeError)

        row_a = await db.pipeline_controls.get(conn, issue_a)
        assert row_a is not None and row_a.outcome == str(OUTCOMES.PENDING)
        assert [row.action_id for row in await controls.history(conn, issue_a)] == ["c-1"]

        row_b = await db.pipeline_controls.get(conn, issue_b)
        assert row_b is not None and row_b.outcome == str(OUTCOMES.FAILED)
        assert await controls.history(conn, issue_b) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_apply_cancelled_mid_window_leaves_no_orphan_action_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The poll-loop task can be cancelled at any `await` — including one
    inside `apply`'s SAVEPOINT window, between `record_action` and `put` — on
    daemon shutdown. An `except Exception` catch there would let
    `asyncio.CancelledError` skip the rollback entirely and leave the action
    row behind in the still-open transaction; a later, unrelated foreign
    `conn.commit()` would then make it durable with no matching control-row
    transition, and the redelivered command would be rejected forever as
    "already applied" since `reconcile_interrupted_retries` only repairs a
    `pending` outcome, not this orphan shape. `apply` must catch
    `BaseException` so cancellation rolls back (or, once a foreign commit has
    already flushed part of the write, compensates) exactly like any other
    failure."""
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
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")

        real_put = db.pipeline_controls.put
        calls = 0

        async def _foreign_commit_then_cancel_once(
            conn: aiosqlite.Connection, **kwargs: object
        ) -> None:
            nonlocal calls
            calls += 1
            if calls > 1:
                # The compensating call `apply`'s except block makes once the
                # SAVEPOINT is gone: let it actually restore the row.
                await real_put(conn, **kwargs)
                return
            # An unrelated coroutine's ordinary write, landing mid-window,
            # immediately followed by this task's own cancellation.
            await db.runs.update_status(conn, "run-1", "completed")
            raise asyncio.CancelledError

        monkeypatch.setattr(db.pipeline_controls, "put", _foreign_commit_then_cancel_once)

        with pytest.raises(asyncio.CancelledError):
            await controls.apply(
                conn,
                ISSUE_ID,
                ACTIONS.RETRY,
                actor="tracker:c-1",
                action_id="c-1",
                at="2026-08-27T10:01:00+00:00",
            )

        assert await controls.history(conn, ISSUE_ID) == []
        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None and row.outcome == str(OUTCOMES.FAILED)

        run = (await db.runs.history_for_issue(conn, ISSUE_ID))[0]
        assert run.status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_apply_record_action_cancelled_mid_window_leaves_no_orphan_action_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same bug as `test_apply_cancelled_mid_window_leaves_no_orphan_action_row`,
    but for `record_action`'s own catch, which used to only trap
    `sqlite3.IntegrityError`: a cancellation landing right after
    `record_action`'s INSERT actually reaches SQLite, with an unrelated
    foreign commit already having flushed it durably, used to propagate
    straight past `apply` with nothing to undo it — leaving the action row
    durable with no matching control-row transition, and permanently wedging
    the park since the redelivered command is then rejected as "already
    applied". `record_action`'s except block must catch `BaseException` too
    and, once the SAVEPOINT is gone, delete the now-durable action row
    itself."""
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
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")

        real_record_action = db.pipeline_controls.record_action

        async def _record_action_then_foreign_commit_and_cancel(
            conn: aiosqlite.Connection, **kwargs: object
        ) -> None:
            await real_record_action(conn, **kwargs)
            # An unrelated coroutine's ordinary write, landing mid-window,
            # immediately followed by this task's own cancellation.
            await db.runs.update_status(conn, "run-1", "completed")
            raise asyncio.CancelledError

        monkeypatch.setattr(
            db.pipeline_controls, "record_action", _record_action_then_foreign_commit_and_cancel
        )

        with pytest.raises(asyncio.CancelledError):
            await controls.apply(
                conn,
                ISSUE_ID,
                ACTIONS.RETRY,
                actor="tracker:c-1",
                action_id="c-1",
                at="2026-08-27T10:01:00+00:00",
            )

        assert await db.pipeline_controls.get_action(conn, ISSUE_ID, "c-1") is None
        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None and row.outcome == str(OUTCOMES.FAILED)

        run = (await db.runs.history_for_issue(conn, ISSUE_ID))[0]
        assert run.status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_release_cancelled_mid_window_leaves_no_orphan_action_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same shape as `test_apply_cancelled_mid_window_leaves_no_orphan_action_row`,
    for `release`'s own undo block: a cancellation between `delete_action` and
    the restoring `put`, with an unrelated foreign commit landing first, must
    still finish the undo (compensating explicitly once the SAVEPOINT is
    gone) instead of leaving the action row behind."""
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
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")

        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-1",
            action_id="c-1",
            at="2026-08-27T10:01:00+00:00",
        )
        assert result.accepted

        real_put = db.pipeline_controls.put
        calls = 0

        async def _foreign_commit_then_cancel_once(
            conn: aiosqlite.Connection, **kwargs: object
        ) -> None:
            nonlocal calls
            calls += 1
            if calls > 1:
                # The compensating call `release`'s except block makes once
                # the SAVEPOINT is gone: let it actually restore the row.
                await real_put(conn, **kwargs)
                return
            await db.runs.update_status(conn, "run-1", "completed")
            raise asyncio.CancelledError

        monkeypatch.setattr(db.pipeline_controls, "put", _foreign_commit_then_cancel_once)

        with pytest.raises(asyncio.CancelledError):
            await controls.release(conn, result, at="2026-08-27T10:02:00+00:00")

        assert await controls.history(conn, ISSUE_ID) == []
        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None and row.outcome == str(OUTCOMES.FAILED)

        run = (await db.runs.history_for_issue(conn, ISSUE_ID))[0]
        assert run.status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_track_implement_failed_wait_cancelled_mid_window_rolls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same shape as `test_apply_cancelled_mid_window_leaves_no_orphan_action_row`,
    for the park path: a cancellation between `record_stage_outcome` and
    `operator_waits.upsert` must still roll back the control row it already
    wrote, exactly like `test_track_implement_failed_wait_rolls_back_on_upsert_failure`
    does for an ordinary exception. An `except Exception` catch there would
    let `asyncio.CancelledError` skip the rollback and leave a "failed"
    control row behind with no matching operator wait."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        orch = _orchestrator(cfg, conn, AsyncMock(), tmp_path)
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

        async def _cancel(*args: object, **kwargs: object) -> None:
            raise asyncio.CancelledError

        monkeypatch.setattr(db.operator_waits, "upsert", _cancel)

        with pytest.raises(asyncio.CancelledError):
            await orch._track_implement_failed_wait(ISSUE_ID, "run-1", binding)  # noqa: SLF001

        assert await db.pipeline_controls.get(conn, ISSUE_ID) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_apply_tolerates_a_foreign_commit_inside_its_savepoint_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A wholly unrelated coroutine's ordinary `commit=True` DAO write (e.g.
    the park writer, or a plain `db.runs.update_status`) can land on the one
    shared connection between `apply`'s `record_action` and its own `RELEASE
    SAVEPOINT`. That foreign commit ends the whole transaction and destroys
    `apply`'s still-open savepoint out from under it — which used to surface
    as a bare `sqlite3.OperationalError: no such savepoint`, leaving both the
    action and control rows durably written but the caller with no
    `ActionResult` to run its side effect or `release` behind. `apply` must
    instead return the normal accepted result, with the action and control
    rows landed exactly once and the foreign write itself intact."""
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
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")

        real_record_action = db.pipeline_controls.record_action

        async def _record_action_then_foreign_commit(
            conn: aiosqlite.Connection, **kwargs: object
        ) -> None:
            await real_record_action(conn, **kwargs)
            # An unrelated coroutine's ordinary write, landing mid-window.
            await db.runs.update_status(conn, "run-1", "completed")

        monkeypatch.setattr(
            db.pipeline_controls, "record_action", _record_action_then_foreign_commit
        )

        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-1",
            action_id="c-1",
            at="2026-08-27T10:01:00+00:00",
        )

        assert result.accepted
        assert result.snapshot.outcome is OUTCOMES.PENDING

        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None and row.outcome == str(OUTCOMES.PENDING)
        assert [a.action_id for a in await controls.history(conn, ISSUE_ID)] == ["c-1"]

        run = (await db.runs.history_for_issue(conn, ISSUE_ID))[0]
        assert run.status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_apply_recovers_from_a_foreign_rollback_inside_its_savepoint_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mirror-image bug of the foreign-commit case above: a foreign
    `conn.rollback()` landing mid-window raises the identical "no such
    savepoint" as a foreign commit, but means the opposite — both writes
    destroyed, not durable. Before this fix, `apply` treated any missing
    savepoint as if a foreign commit had already landed its rows and
    returned `accepted=True` with nothing on disk, so the caller would go on
    to dispatch a retry that no action row explained. `apply` must instead
    notice the rows never landed and redo them for real."""
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
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")

        real_put = db.pipeline_controls.put

        async def _put_then_foreign_rollback(
            conn: aiosqlite.Connection, **kwargs: object
        ) -> None:
            await real_put(conn, **kwargs)
            # Unlike the foreign-commit case, this destroys everything
            # written since the transaction began instead of making it
            # durable — including `record_action`'s insert just above it.
            await conn.rollback()

        monkeypatch.setattr(db.pipeline_controls, "put", _put_then_foreign_rollback)

        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-1",
            action_id="c-1",
            at="2026-08-27T10:01:00+00:00",
        )

        assert result.accepted
        assert result.snapshot.outcome is OUTCOMES.PENDING

        action_row = await db.pipeline_controls.get_action(conn, ISSUE_ID, "c-1")
        assert action_row is not None

        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None and row.outcome == str(OUTCOMES.PENDING)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_release_recovers_from_a_foreign_rollback_inside_its_savepoint_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mirror of `test_apply_recovers_from_a_foreign_rollback_inside_its_savepoint_window`
    for `release`'s own undo: a foreign `conn.rollback()` landing mid-window
    destroys the undo instead of making it durable — before this fix,
    `release` would return as if the previous state had been restored while
    the action row it was supposed to delete was still sitting there
    (rolled back right along with the undo's own `delete_action`), leaving
    the park stuck with RETRY no longer offered and the re-delivered command
    rejected as a duplicate."""
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
        await _record_implement(conn, outcome=OUTCOMES.FAILED, reason="boom")

        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="tracker:c-1",
            action_id="c-1",
            at="2026-08-27T10:01:00+00:00",
        )
        assert result.accepted

        real_put = db.pipeline_controls.put

        async def _put_then_foreign_rollback(
            conn: aiosqlite.Connection, **kwargs: object
        ) -> None:
            await real_put(conn, **kwargs)
            await conn.rollback()

        monkeypatch.setattr(db.pipeline_controls, "put", _put_then_foreign_rollback)

        await controls.release(conn, result, at="2026-08-27T10:02:00+00:00")

        assert await db.pipeline_controls.get_action(conn, ISSUE_ID, "c-1") is None
        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None and row.outcome == str(OUTCOMES.FAILED)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_track_implement_failed_wait_rolls_back_on_upsert_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_track_implement_failed_wait` records the control row and the operator
    wait in one transaction (`commit=False`, rolled back together on failure).
    If the wait upsert fails, the control row it just wrote must roll back
    with it rather than being left behind as a "failed" outcome with no wait
    and no action row to explain it."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        orch = _orchestrator(cfg, conn, AsyncMock(), tmp_path)
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

        async def _boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("boom")

        monkeypatch.setattr(db.operator_waits, "upsert", _boom)

        with pytest.raises(RuntimeError, match="boom"):
            await orch._track_implement_failed_wait(ISSUE_ID, "run-1", binding)  # noqa: SLF001

        assert await db.pipeline_controls.get(conn, ISSUE_ID) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_track_implement_failed_wait_compensates_when_upsert_fails_after_a_foreign_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same bug shape as
    `test_apply_record_action_cancelled_mid_window_leaves_no_orphan_action_row`,
    for the park path's own `except BaseException` catch: before this fix, a
    missing `rollback_to_savepoint` (because an unrelated foreign commit —
    e.g. `db.runs.update_status` — already ended the transaction and flushed
    `record_stage_outcome`'s control-row write) was simply discarded, so a
    durably "failed" control row was left behind with no operator wait to
    explain it — contradicting this method's own invariant that a restart can
    never find one without the other. The fix must compensate explicitly:
    restore the control row to what it was before the park (here, no row at
    all) and drop the operator wait."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        orch = _orchestrator(cfg, conn, AsyncMock(), tmp_path)
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

        async def _foreign_commit_then_boom(*args: object, **kwargs: object) -> None:
            # An unrelated coroutine's ordinary write, landing mid-window and
            # ending the whole transaction, immediately followed by the
            # upsert itself failing.
            await db.runs.update_status(conn, "run-1", "completed")
            raise RuntimeError("boom")

        monkeypatch.setattr(db.operator_waits, "upsert", _foreign_commit_then_boom)

        with pytest.raises(RuntimeError, match="boom"):
            await orch._track_implement_failed_wait(ISSUE_ID, "run-1", binding)  # noqa: SLF001

        assert await db.pipeline_controls.get(conn, ISSUE_ID) is None
        assert await db.operator_waits.get(conn, ISSUE_ID) is None

        run = (await db.runs.history_for_issue(conn, ISSUE_ID))[0]
        assert run.status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_track_implement_failed_wait_tolerates_a_foreign_commit_inside_its_savepoint_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same shape as `test_apply_tolerates_a_foreign_commit_inside_its_savepoint_window`,
    but for the park path: an ordinary `commit=True` DAO write from an
    unrelated coroutine (e.g. `db.runs.update_status`) can land on the shared
    connection between `_track_implement_failed_wait`'s own writes and its
    `RELEASE SAVEPOINT`. `guard_writes` only serializes this module's own
    SAVEPOINT-to-commit windows against each other — it does not stop a
    foreign write elsewhere from landing mid-window, ending the whole
    transaction and destroying this SAVEPOINT out from under it. Before this
    fix, that surfaced as a bare `sqlite3.OperationalError: no such
    savepoint`, aborting the rest of park handling even though both the
    control row and the operator wait were already durable."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        orch = _orchestrator(cfg, conn, AsyncMock(), tmp_path)
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

        real_upsert = db.operator_waits.upsert

        async def _upsert_then_foreign_commit(
            conn: aiosqlite.Connection, **kwargs: object
        ) -> None:
            await real_upsert(conn, **kwargs)
            # An unrelated coroutine's ordinary write, landing mid-window.
            await db.runs.update_status(conn, "run-1", "completed")

        monkeypatch.setattr(db.operator_waits, "upsert", _upsert_then_foreign_commit)

        await orch._track_implement_failed_wait(ISSUE_ID, "run-1", binding)  # noqa: SLF001

        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.outcome is OUTCOMES.FAILED
        assert await db.operator_waits.get(conn, ISSUE_ID) is not None

        run = (await db.runs.history_for_issue(conn, ISSUE_ID))[0]
        assert run.status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_track_implement_failed_wait_recovers_from_a_foreign_rollback_in_its_savepoint_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror of `test_apply_recovers_from_a_foreign_rollback_inside_its_savepoint_window`
    for the park path: a foreign `conn.rollback()` landing inside
    `_track_implement_failed_wait`'s savepoint window destroys the control
    row and the operator wait instead of making them durable. Before this
    fix, the missing `RELEASE SAVEPOINT` would be treated the same as a
    foreign commit and the method would return with neither row on disk — a
    restart would find no wait and no reason to offer Retry."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = _cfg(tmp_path, binding)
        orch = _orchestrator(cfg, conn, AsyncMock(), tmp_path)
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

        real_upsert = db.operator_waits.upsert

        async def _upsert_then_foreign_rollback(
            conn: aiosqlite.Connection, **kwargs: object
        ) -> None:
            await real_upsert(conn, **kwargs)
            await conn.rollback()

        monkeypatch.setattr(db.operator_waits, "upsert", _upsert_then_foreign_rollback)

        await orch._track_implement_failed_wait(ISSUE_ID, "run-1", binding)  # noqa: SLF001

        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.outcome is OUTCOMES.FAILED
        assert await db.operator_waits.get(conn, ISSUE_ID) is not None
    finally:
        await conn.close()
