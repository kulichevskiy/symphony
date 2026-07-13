"""Config-binding DAO: unique natural key, legacy-field rejection, sparsity,
priority-ordered listing (SYM-188)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from symphony import db


async def _conn(tmp_path: Path):  # type: ignore[no-untyped-def]
    return await db.connect(tmp_path / "state.sqlite")


@pytest.mark.asyncio
async def test_two_unlabeled_bindings_same_scope_rejected(tmp_path: Path) -> None:
    """The label is normalized to '' in the natural key, so two unlabeled
    catch-all bindings on the same project/repo collide (SQLite would treat
    NULL labels as distinct without this)."""
    conn = await _conn(tmp_path)
    key = ("ENG", "org/repo", "", "linear", "default")
    await db.config_bindings.insert(conn, payload={"project_key": "ENG"}, key=key)
    with pytest.raises(sqlite3.IntegrityError):
        await db.config_bindings.insert(conn, payload={"project_key": "ENG"}, key=key)
    await conn.close()


@pytest.mark.asyncio
async def test_labeled_and_unlabeled_coexist(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    await db.config_bindings.insert(
        conn, payload={}, key=("ENG", "org/repo", "", "linear", "default")
    )
    await db.config_bindings.insert(
        conn, payload={}, key=("ENG", "org/repo", "bug", "linear", "default")
    )
    assert await db.config_bindings.count(conn) == 2
    await conn.close()


@pytest.mark.asyncio
async def test_write_path_rejects_legacy_role_fields(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    with pytest.raises(ValueError, match="legacy role field"):
        await db.config_bindings.insert(
            conn,
            payload={"project_key": "ENG", "agent": "codex"},
            key=("ENG", "org/repo", "", "linear", "default"),
        )
    await conn.close()


@pytest.mark.asyncio
async def test_list_all_orders_by_priority_then_natural_key(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    await db.config_bindings.insert(
        conn, payload={"n": "b"}, key=("B", "org/b", "", "linear", "default"), priority=5
    )
    await db.config_bindings.insert(
        conn, payload={"n": "a"}, key=("A", "org/a", "", "linear", "default"), priority=1
    )
    # Same priority as the first — tiebreak by natural key (project A2 < B).
    await db.config_bindings.insert(
        conn, payload={"n": "c"}, key=("A2", "org/c", "", "linear", "default"), priority=5
    )
    rows = await db.config_bindings.list_all(conn)
    assert [r.payload["n"] for r in rows] == ["a", "c", "b"]
    await conn.close()


@pytest.mark.asyncio
async def test_get_returns_row_by_id(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    rid = await db.config_bindings.insert(
        conn, payload={"project_key": "ENG"}, key=("ENG", "org/repo", "", "linear", "default")
    )
    row = await db.config_bindings.get(conn, rid)
    assert row is not None and row.id == rid and row.payload == {"project_key": "ENG"}
    assert await db.config_bindings.get(conn, rid + 999) is None
    await conn.close()


@pytest.mark.asyncio
async def test_update_bumps_version_and_stamps_metadata(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    rid = await db.config_bindings.insert(
        conn, payload={"project_key": "ENG"}, key=("ENG", "org/repo", "", "linear", "default")
    )
    updated = await db.config_bindings.update(
        conn,
        rid,
        payload={"project_key": "ENG", "max_concurrent": 5},
        key=("ENG", "org/repo", "", "linear", "default"),
        enabled=False,
        priority=7,
        expected_version=1,
        updated_at="2026-07-13T00:00:00Z",
        updated_by="alice@example.com",
    )
    assert updated.version == 2
    assert updated.payload["max_concurrent"] == 5
    assert updated.enabled is False and updated.priority == 7
    assert updated.updated_by == "alice@example.com"
    await conn.close()


@pytest.mark.asyncio
async def test_update_stale_version_rejected(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    rid = await db.config_bindings.insert(
        conn, payload={}, key=("ENG", "org/repo", "", "linear", "default")
    )
    with pytest.raises(db.config_bindings.StaleVersionError):
        await db.config_bindings.update(
            conn,
            rid,
            payload={},
            key=("ENG", "org/repo", "", "linear", "default"),
            expected_version=99,
        )
    await conn.close()


@pytest.mark.asyncio
async def test_update_rejects_legacy_fields(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    rid = await db.config_bindings.insert(
        conn, payload={}, key=("ENG", "org/repo", "", "linear", "default")
    )
    with pytest.raises(ValueError, match="legacy role field"):
        await db.config_bindings.update(
            conn,
            rid,
            payload={"agent": "codex"},
            key=("ENG", "org/repo", "", "linear", "default"),
            expected_version=1,
        )
    await conn.close()


@pytest.mark.asyncio
async def test_delete_by_id_with_version(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    rid = await db.config_bindings.insert(
        conn, payload={}, key=("ENG", "org/repo", "", "linear", "default")
    )
    with pytest.raises(db.config_bindings.StaleVersionError):
        await db.config_bindings.delete(conn, rid, expected_version=99)
    await db.config_bindings.delete(conn, rid, expected_version=1)
    assert await db.config_bindings.count(conn) == 0
    await conn.close()


@pytest.mark.asyncio
async def test_payload_round_trips_verbatim(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    payload = {"project_key": "ENG", "roles": {"implement": {"model": "sonnet"}}}
    await db.config_bindings.insert(
        conn, payload=payload, key=("ENG", "org/repo", "", "linear", "default"), priority=3
    )
    rows = await db.config_bindings.list_all(conn)
    assert rows[0].payload == payload
    assert rows[0].priority == 3
    assert rows[0].enabled is True
    await conn.close()
