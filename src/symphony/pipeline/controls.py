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
    dispatched twice. Concurrent `apply`/`release` calls in this module are
    guarded against each other — including ones for a different issue, which
    is why the module also serializes their SAVEPOINT-to-commit windows
    against each other, and `guard_writes` lets a caller elsewhere in the
    daemon join that same serialization for its own SAVEPOINT-to-commit
    window — but nothing stops an unrelated `commit=True` DAO call elsewhere
    on the same shared connection from landing mid-window. Such a call ends
    the whole transaction and destroys the still-open SAVEPOINT out from
    under it, and `RELEASE SAVEPOINT`/`ROLLBACK TO SAVEPOINT` then fail with
    the identical "no such savepoint" whether that foreign call was a
    *commit* (this call's rows already durable) or a *rollback* (this call's
    rows destroyed along with it) — the error text alone cannot tell those
    apart, and treating a missing savepoint as always meaning "a foreign
    commit already landed it" would let a foreign rollback masquerade as a
    successful, durable `apply`/`release`. So a missing `RELEASE SAVEPOINT`
    is followed by a re-read of the rows this call just wrote: if they match
    what was intended, that was a foreign commit and there is nothing left to
    do; otherwise it was a foreign rollback, and the same writes are redone
    and committed for real. A missing `ROLLBACK TO SAVEPOINT` on an error
    path is simpler, since undoing and a foreign commit converge on the same
    end state either way: whatever this call had already written landed
    durably as part of that foreign commit, so it triggers an explicit
    compensating write (deleting the just-inserted action row and restoring
    the previous control row) instead of propagating a bare
    `sqlite3.OperationalError`.

Only the implement stage records outcomes today — the tracer through an
implement failure. Later slices extend `record_stage_outcome` to the remaining
stages and wire the remaining actions to side effects; this interface does not
change.
"""

from __future__ import annotations

import asyncio
import sqlite3
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum

import aiosqlite

from .. import db

# `apply` is a multi-`await` read-modify-write (snapshot -> record_action ->
# put -> commit) with nothing else serializing it: two concurrent applies for
# the same issue (e.g. a web-button Retry racing a tracker-comment Retry) can
# otherwise both read the same "current" snapshot, both see their action
# allowed, and both commit. One lock per issue, owned by this module, closes
# that window regardless of which ingress path called in.
_issue_locks: dict[str, asyncio.Lock] = {}


def _lock(issue_id: str) -> asyncio.Lock:
    return _issue_locks.setdefault(issue_id, asyncio.Lock())


# `apply`/`release` for *different* issues are only serialized by `_lock`
# per-issue, but they all run their SAVEPOINT-to-COMMIT window on the one
# `conn` shared by the whole daemon. A savepoint name alone is not enough to
# keep two such windows from colliding: a foreign call's ROLLBACK TO/RELEASE
# for the same name can target the wrong (innermost) nested savepoint, and
# even with unique names, a foreign call's `conn.commit()` mid-window ends the
# whole transaction and destroys a still-open savepoint out from under it.
# This lock forces every apply/release savepoint window in the daemon — plus
# any other caller's window joined in through `guard_writes` — to run one at
# a time, regardless of issue, so no such interleaving can happen.
_write_lock = asyncio.Lock()


@asynccontextmanager
async def guard_writes(issue_id: str) -> AsyncIterator[None]:
    """Let a caller outside this module run its own SAVEPOINT-to-commit
    window on the shared connection under the same serialization `apply`/
    `release` use for that issue.

    Acquires `_lock(issue_id)` and then `_write_lock`, in that order — the
    same order `apply`/`release` acquire them in, so nesting can never
    deadlock. Use this around any other SAVEPOINT-to-commit block that
    touches this issue's control row (or that otherwise must not land its
    `conn.commit()` inside `apply`/`release`'s own window), rather than
    inventing a second lock.
    """
    async with _lock(issue_id):
        async with _write_lock:
            yield


def _is_missing_savepoint_error(exc: sqlite3.OperationalError) -> bool:
    return "no such savepoint" in str(exc)


async def _apply_landed_durably(
    conn: aiosqlite.Connection,
    issue_id: str,
    action_id: str,
    *,
    mode: PipelineMode,
    outcome: AttemptOutcome,
    run_id: str | None,
) -> bool:
    """Whether `apply`'s action row and control row are actually on disk with
    the values this call wrote — used after a `release_savepoint` miss to
    tell a foreign *commit* (rows durable, nothing to do) apart from a
    foreign *rollback* (rows destroyed) since both raise the identical "no
    such savepoint"."""
    action_row = await db.pipeline_controls.get_action(conn, issue_id, action_id)
    control_row = await db.pipeline_controls.get(conn, issue_id)
    return (
        action_row is not None
        and control_row is not None
        and control_row.mode == str(mode)
        and control_row.outcome == str(outcome)
        and control_row.run_id == run_id
    )


async def _release_landed_durably(
    conn: aiosqlite.Connection,
    issue_id: str,
    action_id: str,
    previous: ControlSnapshot,
) -> bool:
    """Mirror of `_apply_landed_durably` for `release`'s undo: the action row
    must be gone and the control row must equal `previous`."""
    action_row = await db.pipeline_controls.get_action(conn, issue_id, action_id)
    control_row = await db.pipeline_controls.get(conn, issue_id)
    return (
        action_row is None
        and control_row is not None
        and control_row.mode == str(previous.mode)
        and control_row.outcome == str(previous.outcome)
        and control_row.run_id == previous.run_id
    )


async def _sweep_landed_durably(
    conn: aiosqlite.Connection,
    issue_id: str,
    interrupted_retry: db.pipeline_controls.ControlActionRow | None,
    *,
    run_id: str,
) -> bool:
    """Mirror of `_apply_landed_durably` for `reconcile_interrupted_retries`'s
    per-wait reset: the interrupted retry's action row (if any) must be gone
    and the control row must show the reset back to failed."""
    if interrupted_retry is not None:
        action_row = await db.pipeline_controls.get_action(
            conn, issue_id, interrupted_retry.action_id
        )
        if action_row is not None:
            return False
    control_row = await db.pipeline_controls.get(conn, issue_id)
    return (
        control_row is not None
        and control_row.stage == IMPLEMENT_STAGE
        and control_row.outcome == str(AttemptOutcome.FAILED)
        and control_row.run_id == run_id
    )


async def rollback_to_savepoint(conn: aiosqlite.Connection, savepoint: str) -> bool:
    """Undo everything written since `savepoint` was opened.

    Returns `False` when a foreign `commit=True` DAO call elsewhere on the
    shared connection already ended the whole transaction and destroyed this
    savepoint out from under it — in which case whatever this call had
    already written landed durably as part of that foreign commit, and there
    is nothing left here to roll back. The caller must compensate explicitly
    in that case instead of treating this as a successful undo.
    """
    try:
        await conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        await conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        return True
    except sqlite3.OperationalError as exc:
        if not _is_missing_savepoint_error(exc):
            raise
        return False


async def release_savepoint(conn: aiosqlite.Connection, savepoint: str) -> bool:
    """Release `savepoint`, keeping everything written since it opened.

    Returns `False` when a foreign `commit=True` DAO call already ended the
    transaction and released this savepoint out from under it — which can
    equally mean the rows written under it are already durable (a foreign
    *commit*) or that they were just destroyed (a foreign *rollback*): the
    "no such savepoint" text is identical either way. The caller must re-read
    what it wrote and compare against what it intended before treating a
    `False` return as success.
    """
    try:
        await conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        return True
    except sqlite3.OperationalError as exc:
        if not _is_missing_savepoint_error(exc):
            raise
        return False


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
        # Scoped like `apply`/`release`'s own windows: `conn` is the one
        # connection shared by the whole daemon, and `guard_writes` joins
        # this sweep's SAVEPOINT-to-commit window into the same
        # serialization `apply`/`release` use so a concurrent web command for
        # this issue can neither steal this ROLLBACK TO/RELEASE nor commit
        # out from under this still-open savepoint. A foreign write for a
        # *different* issue can still land mid-window and end the whole
        # transaction, hence the same missing-savepoint tolerance and
        # durability re-read `apply`/`release` use below.
        savepoint = f"controls_sweep_{uuid.uuid4().hex}"
        async with guard_writes(wait.issue_id):
            await conn.execute(f"SAVEPOINT {savepoint}")
            try:
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
            except BaseException:
                await rollback_to_savepoint(conn, savepoint)
                raise
            released = await release_savepoint(conn, savepoint)
            await conn.commit()
            if not released and not await _sweep_landed_durably(
                conn, wait.issue_id, interrupted_retry, run_id=wait.run_id
            ):
                # Foreign rollback, not foreign commit: redo the reset for
                # real instead of leaving the pending row (and the
                # interrupted retry's action row) exactly where the sweep
                # found them.
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
                    commit=True,
                )


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
        live = current.outcome is AttemptOutcome.RUNNING
        return (PipelineMode.PAUSING if live else PipelineMode.PAUSED), current.outcome
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

    The whole snapshot-to-commit sequence is serialized per issue (see
    `_lock`), so two concurrent applies for the same issue can never both read
    the same "current" snapshot and both get accepted.
    """
    async with _lock(issue_id):
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
        # A SAVEPOINT scopes the undo to just these two writes: `conn` is one
        # connection shared by the whole daemon, so a bare `conn.rollback()`
        # would discard any other, unrelated work some other coroutine has
        # written to the same not-yet-committed transaction. The name is
        # unique per call and the whole window runs under `_write_lock` (see
        # its docstring) so a concurrent apply/release for a *different* issue
        # can neither steal this ROLLBACK TO/RELEASE nor commit out from under
        # this still-open savepoint.
        savepoint = f"control_apply_{uuid.uuid4().hex}"
        async with _write_lock:
            await conn.execute(f"SAVEPOINT {savepoint}")
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
                # `record_action`'s own INSERT never succeeded, so there is
                # nothing of ours for a foreign commit to have flushed here;
                # a missing-savepoint ROLLBACK just means it beat us to
                # ending the transaction, not that we need to compensate.
                await rollback_to_savepoint(conn, savepoint)
                # Only a racing insert of the same action id is reported as
                # already applied; any other constraint violation (e.g. a
                # foreign-key failure on a missing issue) is a real error and
                # must not be swallowed.
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
            except BaseException:
                # `BaseException`, not `Exception`: a task cancellation lands
                # here too (the poll loop's task is cancelled on shutdown, and
                # every `await` in this block is a cancellation point), and it
                # must undo the same as any other failure — an `Exception`-only
                # catch would let it skip the rollback and leave the action row
                # dangling in the open transaction for a later foreign commit
                # to make durable with no matching control-row transition.
                if not await rollback_to_savepoint(conn, savepoint):
                    # A foreign commit already flushed the action row
                    # `record_action` inserted above before this `put` failed
                    # to land the matching control row: `ROLLBACK TO` has
                    # nothing left to undo. Compensate explicitly — delete
                    # the now-durable action row and restore the control row
                    # to what it was before this call — instead of leaving a
                    # durable action with no matching transition.
                    await db.pipeline_controls.delete_action(
                        conn, issue_id=issue_id, action_id=action_id, commit=False
                    )
                    await db.pipeline_controls.put(
                        conn,
                        issue_id=issue_id,
                        mode=str(current.mode),
                        stage=current.stage,
                        outcome=str(current.outcome),
                        reason=current.reason,
                        run_id=current.run_id,
                        actor=None,
                        updated_at=at,
                        commit=True,
                    )
                raise
            released = await release_savepoint(conn, savepoint)
            # Unconditional and safe either way: a normal `RELEASE` still
            # needs this to finalize the outer transaction to disk, and it is
            # a no-op if a foreign commit already finalized everything. If
            # that foreign commit instead landed between `record_action` and
            # `put` (so `put` opened a fresh implicit transaction of its
            # own), this is what commits *that* transaction — `release_savepoint`
            # having nothing to release does not mean there is nothing left
            # to commit.
            await conn.commit()
            if not released and not await _apply_landed_durably(
                conn, issue_id, action_id, mode=mode, outcome=outcome, run_id=run_id
            ):
                # The missing savepoint was a foreign *rollback*, not a
                # foreign commit: it destroyed both writes above along with
                # itself, so what should have been an accepted, durable
                # transition is currently nothing at all. Redo both writes
                # for real and commit them on their own, rather than handing
                # the caller an `accepted=True` result with nothing on disk.
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
                    commit=True,
                )
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
    async with _lock(result.snapshot.issue_id):
        # Scoped like `apply`'s SAVEPOINT: `conn` is the one connection shared
        # by the whole daemon, so a bare `conn.rollback()` here would discard
        # any other, unrelated work some other coroutine has written to the
        # same not-yet-committed transaction. Unique name plus `_write_lock`
        # for the same reason `apply` needs both — see `_write_lock`'s
        # docstring.
        savepoint = f"control_release_{uuid.uuid4().hex}"
        async with _write_lock:
            await conn.execute(f"SAVEPOINT {savepoint}")
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
            except BaseException:
                # See the matching comment in `apply`: a cancellation must
                # undo this window too, not skip straight past the rollback.
                if not await rollback_to_savepoint(conn, savepoint):
                    # A foreign commit already flushed part of this undo
                    # before the rest failed: finish it explicitly instead of
                    # leaving the control row wherever the failed write left
                    # it. `delete_action` is idempotent and re-`put`ting the
                    # same previous values is a no-op if they already landed.
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
                        commit=True,
                    )
                raise
            # Unconditional and safe either way — see the matching comment in
            # `apply`.
            released = await release_savepoint(conn, savepoint)
            await conn.commit()
            if not released and not await _release_landed_durably(
                conn, result.snapshot.issue_id, result.action_id, previous
            ):
                # Same misclassification as `apply`: the missing savepoint
                # was a foreign rollback that destroyed this undo, not a
                # foreign commit that already landed it. Redo it for real
                # instead of returning as if the previous state were
                # restored.
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
                    commit=True,
                )


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
    "guard_writes",
    "history",
    "reconcile_interrupted_retries",
    "record_stage_outcome",
    "release",
    "release_savepoint",
    "rollback_to_savepoint",
    "snapshot",
]
