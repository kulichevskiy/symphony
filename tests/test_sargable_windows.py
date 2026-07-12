from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.ui.api import _spend_heatmap_query, _started_at_window

# Runs straddling two UTC-day boundaries: last second of 05-15, first and last
# second of 05-16, first second of 05-17. A [05-16, 05-16] day window must keep
# exactly the two 05-16 runs regardless of intra-day time.
_BOUNDARY_RUNS = """
    INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
    VALUES
        ('r-15-late',  'a', 'implement', 'completed', NULL, '2026-05-15T23:59:59Z'),
        ('r-16-early', 'a', 'implement', 'completed', NULL, '2026-05-16T00:00:00Z'),
        ('r-16-late',  'a', 'implement', 'completed', NULL, '2026-05-16T23:59:59Z'),
        ('r-17-early', 'a', 'implement', 'completed', NULL, '2026-05-17T00:00:00Z')
"""


@pytest.mark.asyncio
async def test_started_at_window_is_day_inclusive_at_boundaries(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await db.issues.upsert(conn, id="a", identifier="E-1", title="t", team_key="E")
        await conn.execute(_BOUNDARY_RUNS)
        await conn.commit()

        conds, params = _started_at_window("2026-05-16", "2026-05-16")
        # Sargable rewrite: no substr() on the filtered column.
        assert not any("substr" in c for c in conds)

        sql = f"SELECT id FROM runs r WHERE {' AND '.join(conds)} ORDER BY id"
        rows = await (await conn.execute(sql, params)).fetchall()
        assert [row[0] for row in rows] == ["r-16-early", "r-16-late"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_heatmap_window_where_is_sargable_index_scan(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        sql, params = _spend_heatmap_query("2026-05-16", None, [], [])
        # The WHERE clause (everything after the first WHERE) must not filter on
        # a substr() of the timestamp — that would defeat the index.
        where = sql.split("WHERE", 1)[1]
        assert "substr" not in where

        plan = await (await conn.execute("EXPLAIN QUERY PLAN " + sql, params)).fetchall()
        detail = " ".join(row["detail"] for row in plan)
        assert "idx_runs_started" in detail
        assert "SEARCH" in detail  # index range scan, not a full-table SCAN
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_startup_schema_creates_new_indexes_on_fresh_and_existing_db(
    tmp_path: Path,
) -> None:
    expected = {
        "idx_runs_issue_started",
        "idx_runs_started",
        "idx_comment_events_issue_seen",
    }
    db_path = tmp_path / "state.sqlite"

    # Fresh DB.
    conn = await db.connect(db_path)
    try:
        rows = await (
            await conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
        ).fetchall()
        names = {row[0] for row in rows}
        assert expected <= names
    finally:
        await conn.close()

    # Re-applying schema to the existing DB stays idempotent and keeps them.
    conn = await db.connect(db_path)
    try:
        rows = await (
            await conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
        ).fetchall()
        names = {row[0] for row in rows}
        assert expected <= names
    finally:
        await conn.close()
