"""DAO for the `config_bindings` table (SYM-188).

One row per repo binding. The `payload` is a *sparse* JSON dict of the
operator-set `RepoBinding` fields only — no defaults materialized, and never a
legacy top-level role field (`agent`, `codex_model`, …). The write path
rejects legacy role fields outright so the DB stays legacy-free by
construction; the roles matrix is the single source of role config.

The natural-key columns (`project_key`, `github_repo`, `issue_label`,
`tracker_provider`, `tracker_site`) are stored alongside the payload and are
byte-compatible with the orchestrator's `_binding_key` tuple (same components,
same order); `issue_label` is normalized to '' so a nullable label can't let
the unlabeled catch-all be configured twice. A unique index over those columns
rejects duplicates at the DB layer.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import aiosqlite

# Kept in lockstep with `symphony.config._LEGACY_ROLE_FIELDS`. Imported lazily
# in the reject check to avoid a config→db import cycle at module load.


@dataclass(frozen=True)
class StoredBinding:
    """One `config_bindings` row, as loaded for the effective-config assembly."""

    id: int
    payload: dict[str, Any]
    version: int
    enabled: bool
    priority: int
    updated_at: str
    updated_by: str
    project_key: str
    github_repo: str
    issue_label: str
    tracker_provider: str
    tracker_site: str


def _reject_legacy_fields(payload: dict[str, Any]) -> None:
    from ..config import _LEGACY_ROLE_FIELDS

    legacy = sorted(_LEGACY_ROLE_FIELDS & payload.keys())
    if legacy:
        raise ValueError(
            f"config binding payload contains legacy role field(s) "
            f"{', '.join(repr(f) for f in legacy)}; role config lives in the "
            f"`roles` matrix only"
        )


async def insert(
    conn: aiosqlite.Connection,
    *,
    payload: dict[str, Any],
    key: tuple[str, str, str, str, str],
    enabled: bool = True,
    priority: int = 0,
    updated_at: str = "",
    updated_by: str = "",
    version: int = 1,
    commit: bool = True,
) -> int:
    """Insert one binding row. Raises `ValueError` on legacy role fields in the
    payload and `sqlite3.IntegrityError` on a duplicate natural key.

    `commit=False` lets a caller batch several inserts (plus other writes)
    into one atomic transaction it commits itself — e.g. a `--replace`
    import, where committing each row individually would leave a partial,
    unrecoverable state on a later failure.
    """
    _reject_legacy_fields(payload)
    project_key, github_repo, issue_label, tracker_provider, tracker_site = key
    cur = await conn.execute(
        """
        INSERT INTO config_bindings (
            payload, version, enabled, priority, updated_at, updated_by,
            project_key, github_repo, issue_label, tracker_provider, tracker_site
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            json.dumps(payload, separators=(",", ":")),
            version,
            1 if enabled else 0,
            priority,
            updated_at,
            updated_by,
            project_key,
            github_repo,
            issue_label,
            tracker_provider,
            tracker_site,
        ),
    )
    if commit:
        await conn.commit()
    return int(cur.lastrowid or 0)


def _row_to_binding(row: aiosqlite.Row) -> StoredBinding:
    return StoredBinding(
        id=int(row["id"]),
        payload=json.loads(row["payload"]),
        version=int(row["version"]),
        enabled=bool(row["enabled"]),
        priority=int(row["priority"]),
        updated_at=str(row["updated_at"]),
        updated_by=str(row["updated_by"]),
        project_key=str(row["project_key"]),
        github_repo=str(row["github_repo"]),
        issue_label=str(row["issue_label"]),
        tracker_provider=str(row["tracker_provider"]),
        tracker_site=str(row["tracker_site"]),
    )


async def list_all(conn: aiosqlite.Connection) -> list[StoredBinding]:
    """All bindings (enabled + disabled) in dispatch-evaluation order:
    `priority` ascending, ties broken by the stable natural-key sort so two
    rows sharing a priority never route differently across reloads."""
    cur = await conn.execute(
        """
        SELECT id, payload, version, enabled, priority, updated_at, updated_by,
               project_key, github_repo, issue_label, tracker_provider, tracker_site
          FROM config_bindings
         ORDER BY priority ASC, project_key ASC, github_repo ASC,
                  issue_label ASC, tracker_provider ASC, tracker_site ASC
        """
    )
    return [_row_to_binding(row) for row in await cur.fetchall()]


async def count(conn: aiosqlite.Connection) -> int:
    cur = await conn.execute("SELECT COUNT(*) FROM config_bindings")
    row = await cur.fetchone()
    return int(row[0]) if row else 0
