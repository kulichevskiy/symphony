"""DAO for the `operator_waits` table.

Rows represent stopped runs that are still waiting for an operator slash
command, such as cost-cap approval/rejection or a manually stopped review
monitor that can later resume via `$retry` or `$approve`.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite

from . import state_transitions

KIND_COST_CAP = "cost_cap"
KIND_IMPLEMENT_FAILED = "implement_failed"
KIND_REVIEW_FAILED = "review_failed"
KIND_REVIEW_STOPPED = "review_stopped"
KIND_MERGE = "merge"


@dataclass(frozen=True)
class OperatorWait:
    issue_id: str
    run_id: str
    kind: str
    linear_team_key: str
    github_repo: str
    issue_label: str
    created_at: str


async def upsert(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    run_id: str,
    kind: str,
    linear_team_key: str,
    github_repo: str,
    issue_label: str,
    created_at: str,
) -> None:
    old = await get(conn, issue_id)
    await conn.execute(
        """
        INSERT INTO operator_waits (
            issue_id,
            run_id,
            kind,
            linear_team_key,
            github_repo,
            issue_label,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(issue_id) DO UPDATE SET
            run_id = excluded.run_id,
            kind = excluded.kind,
            linear_team_key = excluded.linear_team_key,
            github_repo = excluded.github_repo,
            issue_label = excluded.issue_label,
            created_at = excluded.created_at
        """,
        (
            issue_id,
            run_id,
            kind,
            linear_team_key,
            github_repo,
            issue_label,
            created_at,
        ),
    )
    if old is None:
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "__row__", None, "created"
        )
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "kind", None, kind
        )
    elif old.kind != kind:
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "kind", old.kind, kind
        )
    await conn.commit()


async def list_all(conn: aiosqlite.Connection) -> list[OperatorWait]:
    cur = await conn.execute(
        """
        SELECT
            issue_id,
            run_id,
            kind,
            linear_team_key,
            github_repo,
            issue_label,
            created_at
        FROM operator_waits
        ORDER BY created_at, issue_id
        """
    )
    rows = await cur.fetchall()
    return [
        OperatorWait(
            issue_id=str(row["issue_id"]),
            run_id=str(row["run_id"]),
            kind=str(row["kind"]),
            linear_team_key=str(row["linear_team_key"]),
            github_repo=str(row["github_repo"]),
            issue_label=str(row["issue_label"] or ""),
            created_at=str(row["created_at"]),
        )
        for row in rows
    ]


async def get(conn: aiosqlite.Connection, issue_id: str) -> OperatorWait | None:
    cur = await conn.execute(
        """
        SELECT
            issue_id,
            run_id,
            kind,
            linear_team_key,
            github_repo,
            issue_label,
            created_at
        FROM operator_waits
        WHERE issue_id = ?
        """,
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return OperatorWait(
        issue_id=str(row["issue_id"]),
        run_id=str(row["run_id"]),
        kind=str(row["kind"]),
        linear_team_key=str(row["linear_team_key"]),
        github_repo=str(row["github_repo"]),
        issue_label=str(row["issue_label"] or ""),
        created_at=str(row["created_at"]),
    )


async def delete(
    conn: aiosqlite.Connection,
    issue_id: str,
    run_id: str | None = None,
    *,
    commit: bool = True,
) -> None:
    old = await get(conn, issue_id)
    if run_id is None:
        cur = await conn.execute(
            "DELETE FROM operator_waits WHERE issue_id = ?", (issue_id,)
        )
    else:
        cur = await conn.execute(
            "DELETE FROM operator_waits WHERE issue_id = ? AND run_id = ?",
            (issue_id, run_id),
        )
    if old is not None and (cur.rowcount or 0) > 0:
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "__row__", "removed", None
        )
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "kind", old.kind, None
        )
    if commit:
        await conn.commit()


__all__ = [
    "KIND_COST_CAP",
    "KIND_IMPLEMENT_FAILED",
    "KIND_MERGE",
    "KIND_REVIEW_FAILED",
    "KIND_REVIEW_STOPPED",
    "OperatorWait",
    "delete",
    "get",
    "list_all",
    "upsert",
]
