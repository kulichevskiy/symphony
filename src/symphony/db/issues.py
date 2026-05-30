"""DAO for the `issues` table."""

from __future__ import annotations

import json

import aiosqlite

from ..tracker import DEFAULT_PROVIDER, DEFAULT_SITE


def contextual_id(*, id: str, provider: str, site: str) -> str:
    """Return a stable local id for a tracker-scoped issue identity."""

    return "tracker:" + json.dumps([provider, site, id], separators=(",", ":"))


async def _storage_id_for_upsert(
    conn: aiosqlite.Connection,
    *,
    id: str,
    provider: str,
    site: str,
) -> str:
    scoped_id = contextual_id(id=id, provider=provider, site=site)
    cur = await conn.execute(
        """
        SELECT id
          FROM issues
         WHERE provider = ?
           AND site = ?
           AND tracker_issue_id = ?
         ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END
         LIMIT 1
        """,
        (provider, site, id, id),
    )
    row = await cur.fetchone()
    if row is not None:
        return str(row["id"])

    cur = await conn.execute("SELECT 1 FROM issues WHERE id = ? LIMIT 1", (id,))
    if await cur.fetchone() is not None:
        return scoped_id
    return id


async def upsert(
    conn: aiosqlite.Connection,
    *,
    id: str,
    identifier: str,
    title: str,
    team_key: str,
    provider: str = DEFAULT_PROVIDER,
    site: str = DEFAULT_SITE,
) -> str:
    storage_id = await _storage_id_for_upsert(
        conn,
        id=id,
        provider=provider,
        site=site,
    )
    try:
        await _execute_upsert(
            conn,
            storage_id=storage_id,
            tracker_issue_id=id,
            provider=provider,
            site=site,
            identifier=identifier,
            title=title,
            team_key=team_key,
        )
    except aiosqlite.IntegrityError:
        scoped_id = contextual_id(id=id, provider=provider, site=site)
        if storage_id == scoped_id:
            raise
        storage_id = scoped_id
        await _execute_upsert(
            conn,
            storage_id=storage_id,
            tracker_issue_id=id,
            provider=provider,
            site=site,
            identifier=identifier,
            title=title,
            team_key=team_key,
        )
    await conn.commit()
    return storage_id


async def _execute_upsert(
    conn: aiosqlite.Connection,
    *,
    storage_id: str,
    tracker_issue_id: str,
    provider: str,
    site: str,
    identifier: str,
    title: str,
    team_key: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO issues (
            id, tracker_issue_id, provider, site, identifier, title, team_key
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider, site, tracker_issue_id) DO UPDATE SET
            identifier = excluded.identifier,
            title      = excluded.title,
            team_key   = excluded.team_key
        """,
        (storage_id, tracker_issue_id, provider, site, identifier, title, team_key),
    )
