"""DAO for the `issues` table."""

from __future__ import annotations

import aiosqlite


async def upsert(
    conn: aiosqlite.Connection,
    *,
    id: str,
    identifier: str,
    title: str,
    team_key: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO issues (id, identifier, title, team_key)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            identifier = excluded.identifier,
            title      = excluded.title,
            team_key   = excluded.team_key
        """,
        (id, identifier, title, team_key),
    )
    await conn.commit()
