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


class StaleVersionError(Exception):
    """Optimistic-locking conflict on the single global-config row: the stored
    `version` no longer matches the one the caller loaded (a concurrent edit).
    Carries the current version so the API can render it."""

    def __init__(self, current_version: int) -> None:
        self.current_version = current_version
        super().__init__(
            f"config globals version conflict (current={current_version}); reload and retry"
        )


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


async def update_roles(
    conn: aiosqlite.Connection,
    *,
    roles: dict[str, Any],
    expected_version: int,
    commit: bool = True,
) -> int:
    """Replace the global roles matrix under optimistic locking.

    The write only lands when the stored `version` still equals
    `expected_version` (0 when no row exists yet — a fresh, never-migrated DB);
    otherwise a `StaleVersionError` is raised. `migrated_at` is preserved. On
    success `version` is bumped to `expected_version + 1` and returned.
    """
    current = await get(conn)
    current_version = current.version if current is not None else 0
    if current_version != expected_version:
        raise StaleVersionError(current_version)
    new_version = current_version + 1
    await set_globals(
        conn,
        roles=roles,
        migrated_at=current.migrated_at if current is not None else "",
        version=new_version,
        commit=commit,
    )
    return new_version
