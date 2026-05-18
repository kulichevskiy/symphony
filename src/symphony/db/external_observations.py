"""DAO for the `external_observations` audit table."""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


@dataclass(frozen=True)
class ExternalObservation:
    id: int
    issue_id: str
    source: str
    observed_at: str
    payload_json: str
    drift_kind: str | None
    action_taken: str


def _row_to_observation(row: aiosqlite.Row) -> ExternalObservation:
    return ExternalObservation(
        id=int(row["id"]),
        issue_id=str(row["issue_id"]),
        source=str(row["source"]),
        observed_at=str(row["observed_at"]),
        payload_json=str(row["payload_json"]),
        drift_kind=(
            str(row["drift_kind"]) if row["drift_kind"] is not None else None
        ),
        action_taken=str(row["action_taken"]),
    )


async def insert(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    source: str,
    observed_at: str,
    payload_json: str,
    drift_kind: str | None,
    action_taken: str,
    commit: bool = True,
) -> None:
    await conn.execute(
        """
        INSERT INTO external_observations (
            issue_id, source, observed_at, payload_json, drift_kind, action_taken
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (issue_id, source, observed_at, payload_json, drift_kind, action_taken),
    )
    if commit:
        await conn.commit()


async def list_recent_for_issue(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    limit: int = 20,
) -> list[ExternalObservation]:
    cur = await conn.execute(
        """
        SELECT id, issue_id, source, observed_at, payload_json, drift_kind, action_taken
        FROM external_observations
        WHERE issue_id = ?
        ORDER BY observed_at DESC, id DESC
        LIMIT ?
        """,
        (issue_id, limit),
    )
    rows = await cur.fetchall()
    return [_row_to_observation(row) for row in rows]


__all__ = ["ExternalObservation", "insert", "list_recent_for_issue"]
