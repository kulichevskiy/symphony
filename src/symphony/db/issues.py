"""DAO for the `issues` table."""

from __future__ import annotations

import aiosqlite

from ..tracker import DEFAULT_PROVIDER, DEFAULT_SITE


async def upsert(
    conn: aiosqlite.Connection,
    *,
    id: str,
    identifier: str,
    title: str,
    team_key: str,
    provider: str = DEFAULT_PROVIDER,
    site: str = DEFAULT_SITE,
) -> None:
    await conn.execute(
        """
        INSERT INTO issues (id, provider, site, identifier, title, team_key)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            identifier = excluded.identifier,
            title      = excluded.title,
            team_key   = excluded.team_key
        WHERE issues.provider = excluded.provider
          AND issues.site = excluded.site
        """,
        (id, provider, site, identifier, title, team_key),
    )
    await conn.commit()
