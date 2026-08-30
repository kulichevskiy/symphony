"""DAO for `pipeline_controls` and `pipeline_control_actions` (SYM-244).

The control row is the durable state the operator surface reads; the action
rows are the accepted commands that produced it. Both writers take
`commit=False` so `pipeline.controls.apply` can land them in one transaction —
a dispatched action that survives a restart without its state change (or the
reverse) is exactly the split this table pair exists to prevent.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite

from . import state_transitions


@dataclass(frozen=True)
class ControlRow:
    issue_id: str
    mode: str
    stage: str | None
    outcome: str
    reason: str | None
    run_id: str | None
    actor: str | None
    updated_at: str
    # The stage input a Skip approved (PR head SHA where one applies), so the
    # skip can expire when that input changes (SYM-245). NULL on every other row.
    fingerprint: str | None = None


@dataclass(frozen=True)
class ControlActionRow:
    issue_id: str
    action_id: str
    action: str
    actor: str
    from_mode: str
    to_mode: str
    from_outcome: str
    to_outcome: str
    stage: str | None
    run_id: str | None
    ts: str


def _opt(value: object | None) -> str | None:
    return None if value is None else str(value)


def _to_control(row: aiosqlite.Row) -> ControlRow:
    return ControlRow(
        issue_id=str(row["issue_id"]),
        mode=str(row["mode"]),
        stage=_opt(row["stage"]),
        outcome=str(row["outcome"]),
        reason=_opt(row["reason"]),
        run_id=_opt(row["run_id"]),
        actor=_opt(row["actor"]),
        updated_at=str(row["updated_at"]),
        fingerprint=_opt(row["fingerprint"]),
    )


def _to_action(row: aiosqlite.Row) -> ControlActionRow:
    return ControlActionRow(
        issue_id=str(row["issue_id"]),
        action_id=str(row["action_id"]),
        action=str(row["action"]),
        actor=str(row["actor"]),
        from_mode=str(row["from_mode"]),
        to_mode=str(row["to_mode"]),
        from_outcome=str(row["from_outcome"]),
        to_outcome=str(row["to_outcome"]),
        stage=_opt(row["stage"]),
        run_id=_opt(row["run_id"]),
        ts=str(row["ts"]),
    )


async def get(conn: aiosqlite.Connection, issue_id: str) -> ControlRow | None:
    cur = await conn.execute(
        """
        SELECT issue_id, mode, stage, outcome, reason, run_id, actor, updated_at,
               fingerprint
        FROM pipeline_controls
        WHERE issue_id = ?
        """,
        (issue_id,),
    )
    row = await cur.fetchone()
    return None if row is None else _to_control(row)


async def put(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    mode: str,
    stage: str | None,
    outcome: str,
    reason: str | None,
    run_id: str | None,
    actor: str | None,
    updated_at: str,
    fingerprint: str | None = None,
    commit: bool = True,
) -> None:
    old = await get(conn, issue_id)
    await conn.execute(
        """
        INSERT INTO pipeline_controls (
            issue_id, mode, stage, outcome, reason, run_id, actor, updated_at,
            fingerprint
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(issue_id) DO UPDATE SET
            mode = excluded.mode,
            stage = excluded.stage,
            outcome = excluded.outcome,
            reason = excluded.reason,
            run_id = excluded.run_id,
            actor = excluded.actor,
            updated_at = excluded.updated_at,
            fingerprint = excluded.fingerprint
        """,
        (issue_id, mode, stage, outcome, reason, run_id, actor, updated_at, fingerprint),
    )
    for field, new in (("mode", mode), ("outcome", outcome)):
        current = None if old is None else getattr(old, field)
        if current != new:
            await state_transitions.record_transition(
                conn,
                issue_id,
                "pipeline_controls",
                field,
                current,
                new,
                ts=updated_at,
            )
    if commit:
        await conn.commit()


async def delete(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    commit: bool = True,
) -> None:
    """Remove the control row entirely.

    Used by the park path's foreign-commit compensation: when no control row
    existed before the park attempt, converging on "no row" (rather than a
    row holding default values) matches what a plain SAVEPOINT rollback of
    the same INSERT would have left behind.
    """
    await conn.execute("DELETE FROM pipeline_controls WHERE issue_id = ?", (issue_id,))
    if commit:
        await conn.commit()


async def get_action(
    conn: aiosqlite.Connection, issue_id: str, action_id: str
) -> ControlActionRow | None:
    cur = await conn.execute(
        """
        SELECT issue_id, action_id, action, actor, from_mode, to_mode,
               from_outcome, to_outcome, stage, run_id, ts
        FROM pipeline_control_actions
        WHERE issue_id = ? AND action_id = ?
        """,
        (issue_id, action_id),
    )
    row = await cur.fetchone()
    return None if row is None else _to_action(row)


async def record_action(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    action_id: str,
    action: str,
    actor: str,
    from_mode: str,
    to_mode: str,
    from_outcome: str,
    to_outcome: str,
    stage: str | None,
    run_id: str | None,
    ts: str,
    commit: bool = True,
) -> None:
    """Insert an accepted action.

    Raises `sqlite3.IntegrityError` when `(issue_id, action_id)` is already on
    file — the last-line duplicate guard behind `controls.apply`'s own check.
    """
    await conn.execute(
        """
        INSERT INTO pipeline_control_actions (
            issue_id, action_id, action, actor, from_mode, to_mode,
            from_outcome, to_outcome, stage, run_id, ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            issue_id,
            action_id,
            action,
            actor,
            from_mode,
            to_mode,
            from_outcome,
            to_outcome,
            stage,
            run_id,
            ts,
        ),
    )
    if commit:
        await conn.commit()


async def delete_action(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    action_id: str,
    commit: bool = True,
) -> None:
    """Remove an accepted action record.

    Used for a transition whose side effect failed (dropping the record is
    what makes the command re-deliverable), for the startup sweep's reset of
    an interrupted retry, and for `apply`/`release`'s foreign-commit and
    foreign-rollback compensation, which delete a durable action row that has
    no matching control-row transition (or vice versa) before redoing or
    undoing the write for real."""
    await conn.execute(
        "DELETE FROM pipeline_control_actions WHERE issue_id = ? AND action_id = ?",
        (issue_id, action_id),
    )
    if commit:
        await conn.commit()


async def list_actions(conn: aiosqlite.Connection, issue_id: str) -> list[ControlActionRow]:
    cur = await conn.execute(
        """
        SELECT issue_id, action_id, action, actor, from_mode, to_mode,
               from_outcome, to_outcome, stage, run_id, ts
        FROM pipeline_control_actions
        WHERE issue_id = ?
        ORDER BY ts, rowid
        """,
        (issue_id,),
    )
    return [_to_action(row) for row in await cur.fetchall()]


__all__ = [
    "ControlActionRow",
    "ControlRow",
    "delete",
    "delete_action",
    "get",
    "get_action",
    "list_actions",
    "put",
    "record_action",
]
