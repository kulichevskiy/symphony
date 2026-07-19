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
    # Sparse operator-set operational knob overrides (Config v2 7/9); unset
    # keys fall back to code defaults.
    knobs: dict[str, Any] = field(default_factory=dict)
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
    cur = await conn.execute(
        "SELECT roles, knobs, migrated_at, version FROM config_globals WHERE id = 1"
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return ConfigGlobals(
        roles=json.loads(row["roles"]),
        knobs=json.loads(row["knobs"]),
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
            -- knobs deliberately untouched: set_globals owns roles/migration
            -- state only (Config v2 7/9)
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

    The check-and-write is a single conditional `UPDATE`/`INSERT` statement,
    not a separate `SELECT` followed by a write — SQLite serializes each
    statement at the file-lock level, so this stays atomic across concurrent
    connections/processes racing on the same `expected_version` (unlike a
    read-then-upsert, where two writers could both read the same current
    version before either writes).
    """
    new_version = expected_version + 1
    payload = json.dumps(roles, separators=(",", ":"))
    if expected_version == 0:
        # No row exists yet at any version >= 1 (see schema default), so a
        # fresh DB's first write is an insert guarded against a racing first
        # write rather than an update keyed on a stored version.
        cur = await conn.execute(
            """
            INSERT INTO config_globals (id, roles, migrated_at, version)
            SELECT 1, ?, '', ?
             WHERE NOT EXISTS (SELECT 1 FROM config_globals WHERE id = 1)
            """,
            (payload, new_version),
        )
    else:
        cur = await conn.execute(
            """
            UPDATE config_globals
               SET roles = ?, version = ?
             WHERE id = 1 AND version = ?
            """,
            (payload, new_version, expected_version),
        )
    if cur.rowcount != 1:
        current = await get(conn)
        # The conditional INSERT/UPDATE above already opened a write
        # transaction even though it matched zero rows; leaving it open would
        # hold the connection's write lock until some later, unrelated
        # commit/rollback (SYM-191 review).
        await conn.rollback()
        raise StaleVersionError(current.version if current is not None else 0)
    if commit:
        await conn.commit()
    return new_version


async def update_knobs(
    conn: aiosqlite.Connection,
    *,
    knobs: dict[str, Any],
    expected_version: int,
    commit: bool = True,
) -> int:
    """Replace the operational-knob overrides under the same optimistic
    locking as `update_roles` (one shared `version` for the whole document).
    Returns the bumped version; raises `StaleVersionError` on a conflict."""
    new_version = expected_version + 1
    payload = json.dumps(knobs, separators=(",", ":"))
    if expected_version == 0:
        cur = await conn.execute(
            """
            INSERT INTO config_globals (id, roles, knobs, migrated_at, version)
            SELECT 1, '{}', ?, '', ?
             WHERE NOT EXISTS (SELECT 1 FROM config_globals WHERE id = 1)
            """,
            (payload, new_version),
        )
    else:
        cur = await conn.execute(
            """
            UPDATE config_globals
               SET knobs = ?, version = ?
             WHERE id = 1 AND version = ?
            """,
            (payload, new_version, expected_version),
        )
    if cur.rowcount != 1:
        current = await get(conn)
        await conn.rollback()
        raise StaleVersionError(current.version if current is not None else 0)
    if commit:
        await conn.commit()
    return new_version
