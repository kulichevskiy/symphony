"""Tests for the SQLite persistence layer.

These exercise the schema apply, DAO read/write surfaces, and that the
expected indices are created. Each test runs against a tmp SQLite file
so we also incidentally cover persistence-across-reconnect.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db


@pytest.mark.asyncio
async def test_connect_creates_tables_and_persists(tmp_path: Path) -> None:
    p = tmp_path / "state.sqlite"
    conn = await db.connect(p)
    try:
        cur = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = sorted(row[0] for row in await cur.fetchall())
    finally:
        await conn.close()

    for table in ("repos", "issues", "runs", "comment_cursors"):
        assert table in names, f"expected {table} in {names}"

    assert p.exists()

    # Reopen — schema apply must be idempotent and existing data must survive.
    conn2 = await db.connect(p)
    try:
        cur = await conn2.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names2 = sorted(row[0] for row in await cur.fetchall())
        assert names == names2
    finally:
        await conn2.close()


@pytest.mark.asyncio
async def test_indices_present_for_active_and_cost_lookups(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cur = await conn.execute(
            "SELECT name, tbl_name FROM sqlite_master WHERE type='index'"
        )
        rows = await cur.fetchall()
    finally:
        await conn.close()

    runs_idx = [name for (name, tbl) in rows if tbl == "runs"]
    # At least one index supporting active-run lookup (status[/pid]) and one
    # supporting per-issue cost aggregation (issue_id-keyed).
    assert any("status" in n.lower() or "active" in n.lower() for n in runs_idx), runs_idx
    assert any("issue" in n.lower() or "cost" in n.lower() for n in runs_idx), runs_idx


@pytest.mark.asyncio
async def test_issues_upsert_is_idempotent(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="first", team_key="ENG"
        )
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="second", team_key="ENG"
        )
        cur = await conn.execute("SELECT count(*), title FROM issues WHERE id=?", ("iss-1",))
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == "second"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_runs_create_and_has_active(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        assert await db.runs.has_active(conn, "iss-1") is False

        await db.runs.create(
            conn,
            id="r1",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=12345,
            started_at="2026-05-10T00:00:00+00:00",
        )
        assert await db.runs.has_active(conn, "iss-1") is True

        await db.runs.update_status(
            conn, "r1", "completed", ended_at="2026-05-10T00:01:00+00:00"
        )
        assert await db.runs.has_active(conn, "iss-1") is False
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_runs_list_live_with_pid_filters_correctly(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        # Live with pid — should be returned.
        await db.runs.create(
            conn,
            id="alive",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=42,
            started_at="2026-05-10T00:00:00+00:00",
        )
        # Completed — should NOT be returned even though pid is set.
        await db.runs.create(
            conn,
            id="done",
            issue_id="iss-1",
            stage="implement",
            status="completed",
            pid=43,
            started_at="2026-05-09T00:00:00+00:00",
        )
        # Live but pid is null — should NOT be returned.
        await db.runs.create(
            conn,
            id="nopid",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        rows = await db.runs.list_live_with_pid(conn)
        ids = sorted(r.id for r in rows)
        assert ids == ["alive"]
        assert rows[0].pid == 42
        assert rows[0].issue_id == "iss-1"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_runs_cost_aggregation_per_issue(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.issues.upsert(
            conn, id="iss-2", identifier="ENG-2", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="r1",
            issue_id="iss-1",
            stage="implement",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
            cost_usd=1.5,
        )
        await db.runs.create(
            conn,
            id="r2",
            issue_id="iss-1",
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:30:00+00:00",
            cost_usd=0.75,
        )
        await db.runs.create(
            conn,
            id="r3",
            issue_id="iss-2",
            stage="implement",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
            cost_usd=9.0,
        )
        assert await db.runs.cost_for_issue(conn, "iss-1") == pytest.approx(2.25)
        assert await db.runs.cost_for_issue(conn, "iss-2") == pytest.approx(9.0)
        # Issue with no runs aggregates to 0.0.
        await db.issues.upsert(
            conn, id="iss-3", identifier="ENG-3", title="t", team_key="ENG"
        )
        assert await db.runs.cost_for_issue(conn, "iss-3") == pytest.approx(0.0)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_comment_cursor_advance(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        assert await db.comment_cursors.get(conn, "iss-1") is None
        await db.comment_cursors.set(conn, "iss-1", "2026-05-10T00:00:00+00:00")
        assert (
            await db.comment_cursors.get(conn, "iss-1") == "2026-05-10T00:00:00+00:00"
        )
        await db.comment_cursors.set(conn, "iss-1", "2026-05-10T01:00:00+00:00")
        assert (
            await db.comment_cursors.get(conn, "iss-1") == "2026-05-10T01:00:00+00:00"
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_create_if_no_active_is_atomic_dedupe(tmp_path: Path) -> None:
    """`create_if_no_active` must skip the insert when a live (`running`) row
    already exists for the same issue, and must succeed when the previous run
    has terminated. This is what closes the TOCTOU window between the poll
    loop's `has_active` check and the `dispatch` CLI inserting a duplicate.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )

        first = await db.runs.create_if_no_active(
            conn,
            id="run-a",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        assert first is True

        # Second insert while first is still running must be skipped.
        second = await db.runs.create_if_no_active(
            conn,
            id="run-b",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        assert second is False

        # Only one row exists.
        cur = await conn.execute("SELECT COUNT(*) FROM runs WHERE issue_id = ?", ("iss-1",))
        (count,) = await cur.fetchone()  # type: ignore[misc]
        assert count == 1

        # Once the live run terminates, a new run can be created.
        await db.runs.update_status(conn, "run-a", "completed", ended_at="2026-05-10T00:02:00+00:00")
        third = await db.runs.create_if_no_active(
            conn,
            id="run-c",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:03:00+00:00",
        )
        assert third is True
    finally:
        await conn.close()
