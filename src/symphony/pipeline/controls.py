"""Durable issue-level pipeline controls (SYM-244, slice 1/9).

The external surface is deliberately two calls:

  * `snapshot(conn, issue_id)` — what the pipeline is doing right now: mode
    (`playing | pausing | paused`), the stage it sits on, that stage's latest
    attempt outcome, the diagnostic reason behind it, and `allowed_actions`;
  * `apply(conn, issue_id, action, ...)` — atomically accept or reject one of
    `Play | Pause | Retry | Skip | Abort`.

Two rules hold the design together:

  * **The reason never selects a handler.** `allowed_actions` is a pure
    function of mode and outcome; `reason` rides along as operator-facing data.
    A new failure string can therefore widen what an operator is *told*
    without widening what the daemon will *do*.
  * **Nothing is dispatched that isn't recorded.** `apply` writes the action
    row and the new control row in one transaction and commits *before* the
    caller runs any side effect, so a crash mid-command leaves either "no
    action" or "action recorded" — never a dispatched command with no trace.
    `action_id` (the ingress's own request identity, e.g. a tracker comment id)
    is part of the actions primary key, so a replay is rejected instead of
    dispatched twice.

Only the implement stage records outcomes today — the tracer through an
implement failure. Later slices extend `record_stage_outcome` to the remaining
stages and wire the remaining actions to side effects; this interface does not
change.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from enum import StrEnum

import aiosqlite

from .. import db


class PipelineMode(StrEnum):
    """Operator intent for one issue's pipeline.

    `PAUSING` is the honest middle: the operator asked to stop while an attempt
    was still live, and a live attempt does not stop instantly.
    """

    PLAYING = "playing"
    PAUSING = "pausing"
    PAUSED = "paused"


class AttemptOutcome(StrEnum):
    """How the latest attempt at the current stage ended."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class ControlAction(StrEnum):
    PLAY = "play"
    PAUSE = "pause"
    RETRY = "retry"
    SKIP = "skip"
    ABORT = "abort"


# Stable display/dispatch order; `allowed_actions` filters this tuple so
# callers can rely on the ordering.
_ACTION_ORDER: tuple[ControlAction, ...] = (
    ControlAction.PLAY,
    ControlAction.PAUSE,
    ControlAction.RETRY,
    ControlAction.SKIP,
    ControlAction.ABORT,
)

IMPLEMENT_STAGE = "implement"


@dataclass(frozen=True)
class ControlSnapshot:
    issue_id: str
    mode: PipelineMode
    stage: str | None
    outcome: AttemptOutcome
    # Operator-facing diagnostics for the outcome above. Data only.
    reason: str | None
    run_id: str | None
    allowed_actions: tuple[ControlAction, ...]


@dataclass(frozen=True)
class ActionResult:
    """The result of one `apply`: the state after an accepted action, or the
    unchanged state behind a rejection.

    `previous` and `action_id` are what `release` needs to undo an accepted
    transition whose side effect then failed.
    """

    accepted: bool
    snapshot: ControlSnapshot
    rejection: str | None = None
    previous: ControlSnapshot | None = None
    action_id: str | None = None


def allowed_actions(mode: PipelineMode, outcome: AttemptOutcome) -> tuple[ControlAction, ...]:
    """The actions a pipeline in this (mode, outcome) accepts.

    Derived from those two fields and nothing else — see the module docstring
    on why the diagnostic reason is kept out of it.
    """
    allowed = {ControlAction.ABORT}
    allowed.add(ControlAction.PAUSE if mode is PipelineMode.PLAYING else ControlAction.PLAY)
    if outcome is AttemptOutcome.FAILED:
        # A finished-and-failed attempt is the only state where re-running the
        # stage or stepping over it is meaningful.
        allowed.update({ControlAction.RETRY, ControlAction.SKIP})
    return tuple(action for action in _ACTION_ORDER if action in allowed)


def _snapshot(
    issue_id: str,
    *,
    mode: PipelineMode,
    stage: str | None,
    outcome: AttemptOutcome,
    reason: str | None,
    run_id: str | None,
) -> ControlSnapshot:
    return ControlSnapshot(
        issue_id=issue_id,
        mode=mode,
        stage=stage,
        outcome=outcome,
        reason=reason,
        run_id=run_id,
        allowed_actions=allowed_actions(mode, outcome),
    )


async def snapshot(conn: aiosqlite.Connection, issue_id: str) -> ControlSnapshot:
    """Read the control state for an issue.

    The `pipeline_controls` row wins whenever it exists. When it doesn't, the
    state is derived from the durable park that predates this module — an issue
    parked before the upgrade still has to offer Retry — and otherwise defaults
    to a playing pipeline with no attempt yet.
    """
    row = await db.pipeline_controls.get(conn, issue_id)
    if row is None:
        return await _derived_snapshot(conn, issue_id)
    return _snapshot(
        issue_id,
        mode=PipelineMode(row.mode),
        stage=row.stage,
        outcome=AttemptOutcome(row.outcome),
        reason=row.reason,
        run_id=row.run_id,
    )


async def _derived_snapshot(conn: aiosqlite.Connection, issue_id: str) -> ControlSnapshot:
    """Control state for an issue with no control row: read the durable
    implement park, so a wait opened before this table existed still exposes
    Retry after the upgrade."""
    wait = await db.operator_waits.get(conn, issue_id)
    if wait is None or wait.kind != db.operator_waits.KIND_IMPLEMENT_FAILED:
        return _snapshot(
            issue_id,
            mode=PipelineMode.PLAYING,
            stage=None,
            outcome=AttemptOutcome.PENDING,
            reason=None,
            run_id=None,
        )
    run = await db.runs.get_with_issue(conn, wait.run_id)
    detail = run.run.termination_detail if run is not None else None
    return _snapshot(
        issue_id,
        mode=PipelineMode.PLAYING,
        stage=IMPLEMENT_STAGE,
        outcome=AttemptOutcome.FAILED,
        reason=detail or None,
        run_id=wait.run_id,
    )


async def record_stage_outcome(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    stage: str,
    outcome: AttemptOutcome,
    reason: str | None,
    run_id: str | None,
    at: str,
    commit: bool = True,
) -> ControlSnapshot:
    """Record where the pipeline is and how its latest attempt ended.

    Operator intent (`mode`) is left alone: a paused pipeline reporting a
    failed attempt is still paused. `commit=False` lets the caller fold this
    into the transaction that records the matching park.
    """
    current = await snapshot(conn, issue_id)
    await db.pipeline_controls.put(
        conn,
        issue_id=issue_id,
        mode=str(current.mode),
        stage=stage,
        outcome=str(outcome),
        reason=reason,
        run_id=run_id,
        actor=None,
        updated_at=at,
        commit=commit,
    )
    return _snapshot(
        issue_id,
        mode=current.mode,
        stage=stage,
        outcome=outcome,
        reason=reason,
        run_id=run_id,
    )


async def reconcile_interrupted_retries(conn: aiosqlite.Connection, *, at: str) -> None:
    """Startup sweep for a daemon that died between an accepted Retry's commit
    and its side effect (moving the issue to ready, then clearing the park):
    that leaves a `pipeline_controls` row pending with the implement-failed
    wait still open, and nothing else will ever revisit it on its own. Reset
    any such row back to failed on the way up so Retry/Skip are offered again
    instead of a permanently stuck park.

    The interrupted Retry's own action row is dropped in the same transaction
    as the reset. It was recorded before the side effect that never ran, so
    its `action_id` (typically a tracker comment id) is still sitting in the
    ingress's replay window; leaving the row behind would make `apply` reject
    that identical re-delivery as a duplicate even though the reset just
    advertised Retry as available again.

    Scoped to startup (rather than folded into `snapshot`) so a still-live
    process — where a wait reappearing while an attempt is genuinely pending
    would be a stale/duplicate signal, not an interrupted retry — keeps
    rejecting it.
    """
    for wait in await db.operator_waits.list_all(conn):
        if wait.kind != db.operator_waits.KIND_IMPLEMENT_FAILED:
            continue
        row = await db.pipeline_controls.get(conn, wait.issue_id)
        pending = row is not None and row.outcome == str(AttemptOutcome.PENDING)
        if row is None or row.stage != IMPLEMENT_STAGE or not pending:
            continue
        run = await db.runs.get_with_issue(conn, wait.run_id)
        detail = run.run.termination_detail if run is not None else None
        interrupted_retry = next(
            (
                action
                for action in reversed(await db.pipeline_controls.list_actions(conn, wait.issue_id))
                if action.action == str(ControlAction.RETRY)
                and action.to_outcome == str(AttemptOutcome.PENDING)
            ),
            None,
        )
        if interrupted_retry is not None:
            await db.pipeline_controls.delete_action(
                conn,
                issue_id=wait.issue_id,
                action_id=interrupted_retry.action_id,
                commit=False,
            )
        await record_stage_outcome(
            conn,
            wait.issue_id,
            stage=IMPLEMENT_STAGE,
            outcome=AttemptOutcome.FAILED,
            reason=detail or None,
            run_id=wait.run_id,
            at=at,
            commit=False,
        )
        await conn.commit()


def _next_mode_and_outcome(
    current: ControlSnapshot, action: ControlAction
) -> tuple[PipelineMode, AttemptOutcome]:
    """The (mode, outcome) an accepted action lands in.

    Play/Pause/Abort move operator intent and leave the attempt outcome alone —
    stopping the pipeline does not change what the last attempt did. Retry and
    Skip replace the attempt: Retry clears the way for a fresh one, Skip steps
    over the stage.
    """
    if action is ControlAction.PLAY:
        return PipelineMode.PLAYING, current.outcome
    if action is ControlAction.PAUSE:
        live = current.outcome is AttemptOutcome.RUNNING
        return (PipelineMode.PAUSING if live else PipelineMode.PAUSED), current.outcome
    if action is ControlAction.ABORT:
        return PipelineMode.PAUSED, current.outcome
    if action is ControlAction.RETRY:
        return PipelineMode.PLAYING, AttemptOutcome.PENDING
    return PipelineMode.PLAYING, AttemptOutcome.SKIPPED


async def apply(
    conn: aiosqlite.Connection,
    issue_id: str,
    action: ControlAction,
    *,
    actor: str,
    action_id: str,
    at: str,
) -> ActionResult:
    """Accept or reject one action, atomically.

    A rejected action writes nothing at all. An accepted one commits the action
    record and the new state together, so the caller may run its side effects
    behind a decision that is already durable.
    """
    current = await snapshot(conn, issue_id)
    if action not in current.allowed_actions:
        return ActionResult(
            accepted=False,
            snapshot=current,
            rejection=(
                f"{action.value} is not available while the pipeline is "
                f"{current.mode.value} with a {current.outcome.value} attempt"
            ),
        )
    if await db.pipeline_controls.get_action(conn, issue_id, action_id) is not None:
        return ActionResult(
            accepted=False,
            snapshot=current,
            rejection=f"{action.value} {action_id} was already applied",
        )
    mode, outcome = _next_mode_and_outcome(current, action)
    # An attempt-replacing action drops the previous attempt's diagnostics;
    # an intent-only one keeps them.
    reason = current.reason if outcome is current.outcome else None
    run_id = current.run_id if outcome is current.outcome else None
    try:
        await db.pipeline_controls.record_action(
            conn,
            issue_id=issue_id,
            action_id=action_id,
            action=str(action),
            actor=actor,
            from_mode=str(current.mode),
            to_mode=str(mode),
            from_outcome=str(current.outcome),
            to_outcome=str(outcome),
            stage=current.stage,
            run_id=current.run_id,
            ts=at,
            commit=False,
        )
    except sqlite3.IntegrityError:
        await conn.rollback()
        # Only a racing insert of the same action id is reported as already
        # applied; any other constraint violation (e.g. a foreign-key failure
        # on a missing issue) is a real error and must not be swallowed.
        if await db.pipeline_controls.get_action(conn, issue_id, action_id) is None:
            raise
        return ActionResult(
            accepted=False,
            snapshot=current,
            rejection=f"{action.value} {action_id} was already applied",
        )
    try:
        await db.pipeline_controls.put(
            conn,
            issue_id=issue_id,
            mode=str(mode),
            stage=current.stage,
            outcome=str(outcome),
            reason=reason,
            run_id=run_id,
            actor=actor,
            updated_at=at,
            commit=False,
        )
        await conn.commit()
    except Exception:
        # Never leave half a transition behind for a later unrelated commit to
        # flush: either both rows land or neither does.
        await conn.rollback()
        raise
    return ActionResult(
        accepted=True,
        snapshot=_snapshot(
            issue_id,
            mode=mode,
            stage=current.stage,
            outcome=outcome,
            reason=reason,
            run_id=run_id,
        ),
        previous=current,
        action_id=action_id,
    )


async def release(
    conn: aiosqlite.Connection,
    result: ActionResult,
    *,
    at: str,
) -> None:
    """Undo an accepted transition whose side effect failed.

    Record-then-act means a side effect that dies leaves a transition on file
    that nothing carried out. Releasing it — dropping the action record and
    restoring the previous state in one transaction — is what lets the ingress
    re-deliver the very same command on its next tick instead of facing a park
    that no longer offers the action.
    """
    if not result.accepted or result.previous is None or result.action_id is None:
        return
    previous = result.previous
    try:
        await db.pipeline_controls.delete_action(
            conn,
            issue_id=result.snapshot.issue_id,
            action_id=result.action_id,
            commit=False,
        )
        await db.pipeline_controls.put(
            conn,
            issue_id=previous.issue_id,
            mode=str(previous.mode),
            stage=previous.stage,
            outcome=str(previous.outcome),
            reason=previous.reason,
            run_id=previous.run_id,
            actor=None,
            updated_at=at,
            commit=False,
        )
        await conn.commit()
    except Exception:
        await conn.rollback()
        raise


async def history(
    conn: aiosqlite.Connection, issue_id: str
) -> list[db.pipeline_controls.ControlActionRow]:
    """Accepted actions for an issue, oldest first."""
    return await db.pipeline_controls.list_actions(conn, issue_id)


__all__ = [
    "IMPLEMENT_STAGE",
    "ActionResult",
    "AttemptOutcome",
    "ControlAction",
    "ControlSnapshot",
    "PipelineMode",
    "allowed_actions",
    "apply",
    "history",
    "reconcile_interrupted_retries",
    "record_stage_outcome",
    "release",
    "snapshot",
]
