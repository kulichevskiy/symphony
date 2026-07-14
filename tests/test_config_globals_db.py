"""Config-globals DAO: single-document JSON store with optimistic-lock
`version` (SYM-188); atomicity of the version check under concurrent writers
(SYM-191 review)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from symphony import db


async def _conn(tmp_path: Path):  # type: ignore[no-untyped-def]
    return await db.connect(tmp_path / "state.sqlite")


@pytest.mark.asyncio
async def test_update_roles_round_trip(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        assert await db.config_globals.get(conn) is None
        new_version = await db.config_globals.update_roles(
            conn, roles={"implement": {"agent": "codex"}}, expected_version=0
        )
        assert new_version == 1
        row = await db.config_globals.get(conn)
        assert row is not None
        assert row.roles == {"implement": {"agent": "codex"}}
        assert row.version == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_update_roles_stale_version_rejected(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        await db.config_globals.update_roles(conn, roles={}, expected_version=0)
        with pytest.raises(db.config_globals.StaleVersionError) as exc:
            await db.config_globals.update_roles(conn, roles={}, expected_version=0)
        assert exc.value.current_version == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_update_roles_concurrent_writers_only_one_wins(tmp_path: Path) -> None:
    """Two connections racing `update_roles` with the same `expected_version`
    must not both succeed: the check-and-write is one atomic SQL statement, not
    a separate read followed by a write, so the loser's version check always
    sees the winner's already-committed row rather than the stale value both
    would have read under a read-then-upsert (SYM-191 review)."""
    db_path = tmp_path / "state.sqlite"
    conn_a = await db.connect(db_path)
    conn_b = await db.connect(db_path)
    try:
        results = await asyncio.gather(
            db.config_globals.update_roles(
                conn_a, roles={"implement": {"agent": "codex"}}, expected_version=0
            ),
            db.config_globals.update_roles(
                conn_b, roles={"implement": {"agent": "claude"}}, expected_version=0
            ),
            return_exceptions=True,
        )
        successes = [r for r in results if isinstance(r, int)]
        failures = [r for r in results if isinstance(r, db.config_globals.StaleVersionError)]
        assert successes == [1]
        assert len(failures) == 1
        assert failures[0].current_version == 1
    finally:
        await conn_a.close()
        await conn_b.close()
