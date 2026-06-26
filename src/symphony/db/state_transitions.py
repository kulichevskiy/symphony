"""Audit trail for issue state transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import aiosqlite


@dataclass(frozen=True)
class StateTransition:
    id: int
    issue_id: str
    table_name: str
    field: str
    old_value: str | None
    new_value: str | None
    ts: str


def _text(value: object | None) -> str | None:
    return None if value is None else str(value)


def _row_to_transition(row: aiosqlite.Row) -> StateTransition:
    return StateTransition(
        id=int(row["id"]),
        issue_id=str(row["issue_id"]),
        table_name=str(row["table_name"]),
        field=str(row["field"]),
        old_value=row["old_value"],
        new_value=row["new_value"],
        ts=str(row["ts"]),
    )


async def record_transition(
    conn: aiosqlite.Connection,
    issue_id: str,
    table: str,
    field: str,
    old: object | None,
    new: object | None,
    *,
    ts: str | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO state_transitions (
            issue_id, table_name, field, old_value, new_value, ts
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            issue_id,
            table,
            field,
            _text(old),
            _text(new),
            ts or datetime.now(UTC).isoformat(),
        ),
    )


async def list_for_issue(conn: aiosqlite.Connection, issue_id: str) -> list[StateTransition]:
    cur = await conn.execute(
        """
        SELECT id, issue_id, table_name, field, old_value, new_value, ts
        FROM state_transitions
        WHERE issue_id = ?
        ORDER BY ts ASC, id ASC
        """,
        (issue_id,),
    )
    rows = await cur.fetchall()
    return [_row_to_transition(row) for row in rows]


__all__ = ["StateTransition", "list_for_issue", "record_transition"]
