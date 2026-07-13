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
