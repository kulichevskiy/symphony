"""Tests for the SQLite persistence layer.

These exercise the schema apply, DAO read/write surfaces, and that the
expected indices are created. Each test runs against a tmp SQLite file
so we also incidentally cover persistence-across-reconnect.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiosqlite
import pytest

from symphony import db
from symphony.ui.db import open_read_only_pool


@pytest.mark.asyncio
async def test_connect_creates_tables_and_persists(tmp_path: Path) -> None:
    p = tmp_path / "state.sqlite"
    conn = await db.connect(p)
    try:
        cur = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = sorted(row[0] for row in await cur.fetchall())
    finally:
        await conn.close()

    for table in (
        "repos",
        "issues",
        "runs",
        "issue_prs",
        "comment_cursors",
        "activity_comment_marks",
        "activity_command_marks",
        "operator_waits",
        "state_transitions",
    ):
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
async def test_connect_enables_wal_mode(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        cur = await conn.execute("PRAGMA journal_mode")
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert row is not None
    assert row[0].casefold() == "wal"


@pytest.mark.asyncio
async def test_ui_read_only_pool_rejects_writes(tmp_path: Path) -> None:
    p = tmp_path / "state.sqlite"
    conn = await db.connect(p)
    try:
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="t",
            team_key="ENG",
        )
    finally:
        await conn.close()

    pool = await open_read_only_pool(p)
    try:
        ro_conn = await pool.connection()
        cur = await ro_conn.execute("SELECT title FROM issues WHERE id = ?", ("iss-1",))
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == "t"

        with pytest.raises(aiosqlite.OperationalError, match="readonly"):
            await ro_conn.execute(
                "UPDATE issues SET title = ? WHERE id = ?",
                ("changed", "iss-1"),
            )
    finally:
        await pool.close()


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
async def test_runs_schema_has_termination_columns(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cur = await conn.execute("PRAGMA table_info(runs)")
        cols = {row[1]: row for row in await cur.fetchall()}
    finally:
        await conn.close()

    assert cols["termination_kind"][2] == "TEXT"
    assert cols["termination_kind"][3] == 1
    assert cols["termination_detail"][2] == "TEXT"
    assert cols["termination_detail"][3] == 1
    assert cols["exit_returncode"][2] == "INTEGER"


@pytest.mark.asyncio
async def test_runs_schema_has_usage_columns_and_migrates_existing_rows(
    tmp_path: Path,
) -> None:
    p = tmp_path / "legacy.sqlite"
    legacy = await aiosqlite.connect(p)
    try:
        await legacy.executescript(
            """
            CREATE TABLE issues (
                id               TEXT PRIMARY KEY,
                tracker_issue_id TEXT NOT NULL,
                provider         TEXT NOT NULL DEFAULT 'linear',
                site             TEXT NOT NULL DEFAULT 'default',
                identifier       TEXT NOT NULL,
                title            TEXT NOT NULL,
                team_key         TEXT NOT NULL
            );
            CREATE TABLE runs (
                id          TEXT PRIMARY KEY,
                issue_id    TEXT NOT NULL REFERENCES issues(id),
                stage       TEXT NOT NULL,
                status      TEXT NOT NULL,
                pid         INTEGER,
                started_at  TEXT NOT NULL,
                ended_at    TEXT,
                cost_usd    REAL NOT NULL DEFAULT 0
            );
            INSERT INTO issues (
                id, tracker_issue_id, identifier, title, team_key
            ) VALUES (
                'iss-1', 'ISS-1', 'ISS-1', 't', 'ISS'
            );
            INSERT INTO runs (
                id, issue_id, stage, status, pid, started_at, cost_usd
            ) VALUES (
                'run-1', 'iss-1', 'implement', 'completed', NULL,
                '2026-05-10T00:00:00+00:00', 1.25
            );
            """
        )
        await legacy.commit()
    finally:
        await legacy.close()

    conn = await db.connect(p)
    try:
        cur = await conn.execute("PRAGMA table_info(runs)")
        cols = {row[1]: row for row in await cur.fetchall()}
        for name in (
            "input_tokens",
            "output_tokens",
            "cache_write_tokens",
            "cache_read_tokens",
        ):
            assert cols[name][2] == "INTEGER"
            assert cols[name][3] == 1
            assert cols[name][4] == "0"

        cur = await conn.execute(
            """
            SELECT input_tokens, output_tokens, cache_write_tokens,
                   cache_read_tokens
            FROM runs
            WHERE id = ?
            """,
            ("run-1",),
        )
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert row is not None
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert row["cache_write_tokens"] == 0
    assert row["cache_read_tokens"] == 0


@pytest.mark.asyncio
async def test_runs_add_usage_accumulates_all_buckets(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="r1",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        await db.runs.add_usage(
            conn,
            "r1",
            cost_usd=0.10,
            input_tokens=100,
            output_tokens=20,
            cache_write_tokens=30,
            cache_read_tokens=40,
        )
        await db.runs.add_usage(
            conn,
            "r1",
            cost_usd=0.25,
            input_tokens=5,
            output_tokens=6,
            cache_write_tokens=7,
            cache_read_tokens=8,
        )

        history = await db.runs.history_for_issue(conn, "iss-1")
    finally:
        await conn.close()

    assert len(history) == 1
    assert history[0].cost_usd == pytest.approx(0.35)
    assert history[0].input_tokens == 105
    assert history[0].output_tokens == 26
    assert history[0].cache_write_tokens == 37
    assert history[0].cache_read_tokens == 48


@pytest.mark.asyncio
async def test_update_status_persists_unknown_for_unclassified_terminal_status(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="r1",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        with caplog.at_level(logging.WARNING):
            await db.runs.update_status(
                conn, "r1", "failed", ended_at="2026-05-10T00:01:00+00:00"
            )

        cur = await conn.execute(
            """
            SELECT termination_kind, termination_detail, exit_returncode
            FROM runs WHERE id = ?
            """,
            ("r1",),
        )
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert row is not None
    assert row["termination_kind"] == "unknown"
    assert row["termination_detail"] == ""
    assert row["exit_returncode"] is None
    assert "missing termination kind" in caplog.text


@pytest.mark.asyncio
async def test_update_status_truncates_termination_detail_tail_first(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="r1",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        detail = "\n".join(
            f"line {i:03d} " + ("x" * 120)
            for i in range(200)
        )

        await db.runs.update_status(
            conn,
            "r1",
            "failed",
            ended_at="2026-05-10T00:01:00+00:00",
            kind="push_failed",
            detail=detail,
            returncode=2,
        )

        cur = await conn.execute(
            """
            SELECT termination_kind, termination_detail, exit_returncode
            FROM runs WHERE id = ?
            """,
            ("r1",),
        )
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert row is not None
    stored = row["termination_detail"]
    assert row["termination_kind"] == "push_failed"
    assert row["exit_returncode"] == 2
    assert len(stored.encode("utf-8")) <= 4096
    assert "…[truncated " in stored
    assert "line 199" in stored
    assert "line 000" not in stored


@pytest.mark.asyncio
async def test_update_status_success_leaves_termination_columns_empty(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="r1",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.update_status(
            conn, "r1", "completed", ended_at="2026-05-10T00:01:00+00:00"
        )

        cur = await conn.execute(
            """
            SELECT termination_kind, termination_detail, exit_returncode
            FROM runs WHERE id = ?
            """,
            ("r1",),
        )
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert row is not None
    assert row["termination_kind"] == ""
    assert row["termination_detail"] == ""
    assert row["exit_returncode"] is None


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

        await db.runs.create(
            conn,
            id="review",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:02:00+00:00",
        )
        assert await db.runs.has_active(conn, "iss-1") is True
        assert (
            await db.runs.has_active(conn, "iss-1", ignored_stage="review")
            is False
        )
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
async def test_runs_list_live_review_without_pid_filters_correctly(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        # Live review without pid: should be returned.
        await db.runs.create(
            conn,
            id="nopid",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        # Live implement without pid: should NOT be returned; pidless
        # reconciliation is only for in-process review monitors.
        await db.runs.create(
            conn,
            id="nopid-implement",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:02:00+00:00",
        )
        # Live with pid — should NOT be returned.
        await db.runs.create(
            conn,
            id="alive",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=42,
            started_at="2026-05-10T00:00:00+00:00",
        )
        # Completed — should NOT be returned even though pid is null.
        await db.runs.create(
            conn,
            id="done",
            issue_id="iss-1",
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-09T00:00:00+00:00",
        )
        rows = await db.runs.list_live_review_without_pid(conn)
        ids = sorted(r.id for r in rows)
        assert ids == ["nopid"]
        assert rows[0].pid is None
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
async def test_issue_prs_tracks_merge_candidates(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            binding_key='["ENG","org/repo","backend"]',
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        assert await db.issue_prs.list_merge_candidates(conn) == []

        await db.runs.create(
            conn,
            id="review",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert len(candidates) == 1
        assert candidates[0].pr_number == 42
        assert candidates[0].github_repo == "org/repo"
        assert candidates[0].binding_key == '["ENG","org/repo","backend"]'

        assert await db.issue_prs.mark_parked_for_manual_merge(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            parked_at="2026-05-10T00:02:00+00:00",
        )
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert len(candidates) == 1
        assert candidates[0].parked_at == "2026-05-10T00:02:00+00:00"

        await db.issue_prs.mark_merged(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            merged_at="2026-05-10T00:02:00+00:00",
        )
        assert await db.issue_prs.list_merge_candidates(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_issue_prs_lists_recent_merged_rows_since_cutoff(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    now = datetime(2026, 5, 19, 20, tzinfo=UTC)
    try:
        for issue_id, identifier in [
            ("iss-recent", "ENG-1"),
            ("iss-old", "ENG-2"),
            ("iss-open", "ENG-3"),
        ]:
            await db.issues.upsert(
                conn,
                id=issue_id,
                identifier=identifier,
                title="t",
                team_key="ENG",
            )
            await db.issue_prs.upsert(
                conn,
                issue_id=issue_id,
                github_repo="org/repo",
                binding_key='["ENG","org/repo",""]',
                pr_number=40 + int(identifier.rsplit("-", 1)[1]),
                pr_url=f"https://github.com/org/repo/pull/{identifier}",
                created_at=(now - timedelta(days=2)).isoformat(),
            )

        await db.issue_prs.mark_merged(
            conn,
            issue_id="iss-recent",
            github_repo="org/repo",
            merged_at=(now - timedelta(hours=1)).isoformat(),
        )
        await db.issue_prs.mark_merged(
            conn,
            issue_id="iss-old",
            github_repo="org/repo",
            merged_at=(now - timedelta(hours=25)).isoformat(),
        )

        rows = await db.issue_prs.list_recent_merged(
            conn,
            since=now - timedelta(hours=24),
        )

        assert [row.issue_id for row in rows] == ["iss-recent"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_issue_prs_scopes_candidates_to_current_pr_cycle(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="old-review",
            issue_id="iss-1",
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        await db.runs.create(
            conn,
            id="old-merge",
            issue_id="iss-1",
            stage="merge",
            status="done",
            pid=None,
            started_at="2026-05-10T00:02:00+00:00",
        )
        assert await db.issue_prs.list_merge_candidates(conn) == []

        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=43,
            pr_url="https://github.com/org/repo/pull/43",
            created_at="2026-05-10T01:00:00+00:00",
        )
        assert await db.issue_prs.list_merge_candidates(conn) == []

        await db.runs.create(
            conn,
            id="new-review",
            issue_id="iss-1",
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T01:01:00+00:00",
        )
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert len(candidates) == 1
        assert candidates[0].pr_number == 43

        await db.runs.create(
            conn,
            id="new-merge",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T01:02:00+00:00",
        )
        assert await db.issue_prs.list_merge_candidates(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_orphaned_review_prs_require_latest_review_run_dead(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="t",
            team_key="ENG",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="failed-review",
            issue_id="iss-1",
            stage="review",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        candidates = await db.issue_prs.list_orphaned_review_prs(conn)
        assert [c.pr_number for c in candidates] == [42]

        await db.runs.create(
            conn,
            id="completed-review",
            issue_id="iss-1",
            stage="review",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:02:00+00:00",
        )
        assert await db.issue_prs.list_orphaned_review_prs(conn) == []

        await db.runs.create(
            conn,
            id="latest-interrupted-review",
            issue_id="iss-1",
            stage="review",
            status="interrupted",
            pid=None,
            started_at="2026-05-10T00:03:00+00:00",
        )
        candidates = await db.issue_prs.list_orphaned_review_prs(conn)
        assert [c.pr_number for c in candidates] == [42]

        await db.runs.create(
            conn,
            id="old-merge",
            issue_id="iss-1",
            stage="merge",
            status="completed",
            pid=None,
            started_at="2026-05-09T23:59:00+00:00",
        )
        candidates = await db.issue_prs.list_orphaned_review_prs(conn)
        assert [c.pr_number for c in candidates] == [42]

        await db.runs.create(
            conn,
            id="submitted-merge",
            issue_id="iss-1",
            stage="merge",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:04:00+00:00",
        )
        assert await db.issue_prs.list_orphaned_review_prs(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_latest_for_issue_stage_can_scope_to_current_cycle(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="old-merge",
            issue_id="iss-1",
            stage="merge",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        assert (
            await db.runs.latest_for_issue_stage(
                conn,
                issue_id="iss-1",
                stage="merge",
                started_at_gte="2026-05-10T01:00:00+00:00",
            )
            is None
        )

        await db.runs.create(
            conn,
            id="new-merge",
            issue_id="iss-1",
            stage="merge",
            status="completed",
            pid=None,
            started_at="2026-05-10T01:01:00+00:00",
        )
        latest = await db.runs.latest_for_issue_stage(
            conn,
            issue_id="iss-1",
            stage="merge",
            started_at_gte="2026-05-10T01:00:00+00:00",
        )
        assert latest is not None
        assert latest.id == "new-merge"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_interrupt_stale_merge_needs_approval_only_touches_stale_waits(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        for idx in range(3):
            await db.runs.create(
                conn,
                id=f"stale-merge-{idx}",
                issue_id="iss-1",
                stage="merge",
                status="needs_approval",
                pid=None,
                started_at=f"2026-05-10T00:0{idx + 1}:00+00:00",
            )
        await db.runs.create(
            conn,
            id="done-merge",
            issue_id="iss-1",
            stage="merge",
            status="done",
            pid=None,
            started_at="2026-05-10T00:04:00+00:00",
        )
        await db.runs.create(
            conn,
            id="running-merge",
            issue_id="iss-1",
            stage="merge",
            status="running",
            pid=None,
            started_at="2026-05-10T00:05:00+00:00",
        )
        await db.runs.create(
            conn,
            id="previous-pr-wait",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-09T23:59:00+00:00",
        )

        count = await db.runs.interrupt_stale_merge_needs_approval(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
        )

        assert count == 3
        history = await db.runs.history_for_issue(conn, "iss-1")
        by_id = {run.id: run for run in history}
        for idx in range(3):
            run = by_id[f"stale-merge-{idx}"]
            assert run.status == "interrupted"
            assert run.ended_at is not None
        assert by_id["done-merge"].status == "done"
        assert by_id["done-merge"].ended_at is None
        assert by_id["running-merge"].status == "running"
        assert by_id["running-merge"].ended_at is None
        assert by_id["previous-pr-wait"].status == "needs_approval"
        assert by_id["previous-pr-wait"].ended_at is None
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
        await db.comment_cursors.set(
            conn, "iss-1", "2026-05-10T00:00:00+00:00", ["c1"]
        )
        got = await db.comment_cursors.get(conn, "iss-1")
        assert got == ("2026-05-10T00:00:00+00:00", ["c1"])
        await db.comment_cursors.set(
            conn, "iss-1", "2026-05-10T01:00:00+00:00", ["c2", "c3"]
        )
        got = await db.comment_cursors.get(conn, "iss-1")
        assert got == ("2026-05-10T01:00:00+00:00", ["c2", "c3"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_operator_waits_persist_and_delete(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id="iss-1",
            stage="implement",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="run-1",
            kind=db.operator_waits.KIND_COST_CAP,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="ready",
            created_at="2026-05-10T01:00:00+00:00",
        )

        got = await db.operator_waits.get(conn, "iss-1")
        assert got is not None
        assert got.run_id == "run-1"
        assert got.kind == db.operator_waits.KIND_COST_CAP
        assert got.provider == "linear"
        assert got.issue_label == "ready"
        assert await db.operator_waits.list_all(conn) == [got]
        transitions = await db.state_transitions.list_for_issue(conn, "iss-1")
        assert [(t.field, t.old_value, t.new_value) for t in transitions] == [
            ("__row__", None, "created"),
            ("kind", None, db.operator_waits.KIND_COST_CAP),
        ]

        await db.operator_waits.delete(conn, "iss-1", "run-1")
        assert await db.operator_waits.get(conn, "iss-1") is None
        transitions = await db.state_transitions.list_for_issue(conn, "iss-1")
        assert [(t.field, t.old_value, t.new_value) for t in transitions] == [
            ("__row__", None, "created"),
            ("kind", None, db.operator_waits.KIND_COST_CAP),
            ("__row__", "removed", None),
            ("kind", db.operator_waits.KIND_COST_CAP, None),
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_operator_waits_provider_column_migrates_existing_rows(
    tmp_path: Path,
) -> None:
    p = tmp_path / "legacy.sqlite"
    legacy = await aiosqlite.connect(p)
    try:
        await legacy.executescript(
            """
            CREATE TABLE issues (
                id               TEXT PRIMARY KEY,
                tracker_issue_id TEXT NOT NULL,
                provider         TEXT NOT NULL DEFAULT 'linear',
                site             TEXT NOT NULL DEFAULT 'default',
                identifier       TEXT NOT NULL,
                title            TEXT NOT NULL,
                team_key         TEXT NOT NULL
            );
            CREATE TABLE runs (
                id          TEXT PRIMARY KEY,
                issue_id    TEXT NOT NULL REFERENCES issues(id),
                stage       TEXT NOT NULL,
                status      TEXT NOT NULL,
                pid         INTEGER,
                started_at  TEXT NOT NULL,
                ended_at    TEXT,
                cost_usd    REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE operator_waits (
                issue_id         TEXT PRIMARY KEY REFERENCES issues(id),
                run_id           TEXT NOT NULL REFERENCES runs(id),
                kind             TEXT NOT NULL,
                tracker_provider TEXT NOT NULL DEFAULT 'linear',
                tracker_site     TEXT NOT NULL DEFAULT 'default',
                linear_team_key  TEXT NOT NULL,
                github_repo      TEXT NOT NULL,
                issue_label      TEXT NOT NULL DEFAULT '',
                created_at       TEXT NOT NULL
            );
            INSERT INTO issues (
                id, tracker_issue_id, provider, site, identifier, title, team_key
            ) VALUES (
                'iss-1', 'ISS-1', 'jira', 'https://jira.example.test', 'SYM-1', 't', 'SYM'
            );
            INSERT INTO runs (
                id, issue_id, stage, status, pid, started_at
            ) VALUES (
                'run-1', 'iss-1', 'implement', 'failed', NULL, '2026-05-10T00:00:00+00:00'
            );
            INSERT INTO operator_waits (
                issue_id,
                run_id,
                kind,
                tracker_provider,
                tracker_site,
                linear_team_key,
                github_repo,
                issue_label,
                created_at
            ) VALUES (
                'iss-1',
                'run-1',
                'implement_failed',
                'jira',
                'https://jira.example.test',
                'SYM',
                'org/repo',
                'symphony',
                '2026-05-10T01:00:00+00:00'
            );
            """
        )
        await legacy.commit()
    finally:
        await legacy.close()

    conn = await db.connect(p)
    try:
        cur = await conn.execute("PRAGMA table_info(operator_waits)")
        columns = {str(row["name"]) for row in await cur.fetchall()}
        wait = await db.operator_waits.get(conn, "iss-1")
    finally:
        await conn.close()

    assert "provider" in columns
    assert wait is not None
    assert wait.provider == "jira"
    assert wait.tracker_provider == "jira"
    assert wait.tracker_site == "https://jira.example.test"
    assert wait.linear_team_key == "SYM"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("completed_stage", "wait_kind", "wait_run_stage"),
    [
        ("implement", db.operator_waits.KIND_IMPLEMENT_FAILED, "implement"),
        ("review_fix", db.operator_waits.KIND_REVIEW_FAILED, "review"),
        ("acceptance_fix", db.operator_waits.KIND_ACCEPTANCE_REJECTED, "acceptance"),
    ],
)
async def test_completed_remediation_run_clears_older_matching_wait(
    tmp_path: Path,
    completed_stage: str,
    wait_kind: str,
    wait_run_stage: str,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="wait-run",
            issue_id="iss-1",
            stage=wait_run_stage,
            status="failed",
            pid=None,
            started_at="2026-05-17T09:05:22+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="wait-run",
            kind=wait_kind,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            created_at="2026-05-17T09:16:55+00:00",
        )
        await db.runs.create(
            conn,
            id="remediation-run",
            issue_id="iss-1",
            stage=completed_stage,
            status="running",
            pid=5087,
            started_at="2026-05-17T09:09:55+00:00",
        )

        await db.runs.update_status(
            conn,
            "remediation-run",
            "completed",
            ended_at="2026-05-17T09:18:18+00:00",
        )

        wait = await db.operator_waits.get(conn, "iss-1")
        transitions = await db.state_transitions.list_for_issue(conn, "iss-1")
    finally:
        await conn.close()

    assert wait is None
    assert (
        "operator_waits",
        "kind",
        wait_kind,
        None,
    ) in [
        (t.table_name, t.field, t.old_value, t.new_value)
        for t in transitions
    ]


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
        await db.runs.update_status(
            conn, "run-a", "completed", ended_at="2026-05-10T00:02:00+00:00"
        )
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

        merge = await db.runs.create_if_no_active(
            conn,
            id="run-d",
            issue_id="iss-1",
            stage="merge",
            status="running",
            pid=None,
            started_at="2026-05-10T00:04:00+00:00",
            ignored_stage="review",
        )
        assert merge is True

        blocked = await db.runs.create_if_no_active(
            conn,
            id="run-e",
            issue_id="iss-1",
            stage="merge",
            status="running",
            pid=None,
            started_at="2026-05-10T00:05:00+00:00",
            ignored_stage="review",
        )
        assert blocked is False
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_create_if_not_dispatched_allows_completed_reruns(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="done",
            issue_id="iss-1",
            stage="implement",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        inserted = await db.runs.create_if_not_dispatched(
            conn,
            id="retry",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        assert inserted is True
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_list_recent_keeps_active_runs_outside_limit(tmp_path: Path) -> None:
    """`runs ls` advertises "active + recent runs"; a long-running live run
    must remain visible even when newer terminated runs would otherwise
    crowd it out under `--limit`. This protects incident triage where the
    live run is the most important row to see."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        # An older run that is still live.
        await db.runs.create(
            conn,
            id="run-old-live",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=1,
            started_at="2026-05-01T00:00:00+00:00",
        )
        # Three newer terminated runs; with limit=2 they would push the
        # live run out under a naive ORDER BY/LIMIT.
        for i, ts in enumerate(
            ["2026-05-09T00:00:00+00:00", "2026-05-09T01:00:00+00:00", "2026-05-09T02:00:00+00:00"]
        ):
            await db.runs.create(
                conn,
                id=f"run-done-{i}",
                issue_id="iss-1",
                stage="implement",
                status="completed",
                pid=None,
                started_at=ts,
            )

        rows = await db.runs.list_recent(conn, limit=2)
        ids = [r.run.id for r in rows]
        # All live runs are present regardless of limit.
        assert "run-old-live" in ids
        # The limit applies to terminated rows: only the 2 newest done runs.
        assert "run-done-2" in ids
        assert "run-done-1" in ids
        assert "run-done-0" not in ids
    finally:
        await conn.close()
