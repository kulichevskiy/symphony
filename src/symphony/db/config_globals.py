"""DAO for the single-document `config_globals` table (SYM-188).

Holds the global roles matrix (JSON) and the one-off migration marker
(`migrated_at`, empty until the importer runs), plus a `version` for
optimistic locking. Always row `id = 1`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import aiosqlite


@dataclass(frozen=True)
class ConfigGlobals:
    roles: dict[str, Any] = field(default_factory=dict)
    migrated_at: str = ""
    version: int = 1


async def get(conn: aiosqlite.Connection) -> ConfigGlobals | None:
    """Return the global-config document, or `None` if it was never written."""
    cur = await conn.execute("SELECT roles, migrated_at, version FROM config_globals WHERE id = 1")
    row = await cur.fetchone()
    if row is None:
        return None
    return ConfigGlobals(
        roles=json.loads(row["roles"]),
        migrated_at=str(row["migrated_at"]),
        version=int(row["version"]),
    )


async def set_globals(
    conn: aiosqlite.Connection,
    *,
    roles: dict[str, Any],
    migrated_at: str = "",
    version: int = 1,
    commit: bool = True,
) -> None:
    """Upsert the single global-config row.

    `commit=False` lets a caller fold this write into a larger atomic
    transaction it commits itself.
    """
    await conn.execute(
        """
        INSERT INTO config_globals (id, roles, migrated_at, version)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            roles = excluded.roles,
            migrated_at = excluded.migrated_at,
            version = excluded.version
        """,
        (json.dumps(roles, separators=(",", ":")), migrated_at, version),
    )
    if commit:
        await conn.commit()
