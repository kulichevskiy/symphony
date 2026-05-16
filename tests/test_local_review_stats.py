"""`local_review_stats` aggregator + CLI subcommand."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from symphony import db
from symphony.cli import main


async def _seed_run(
    conn,
    *,
    run_id: str,
    issue_id: str,
    stage: str,
    status: str,
    cost_usd: float,
    started_at: str,
    ended_at: str | None,
) -> None:
    await conn.execute(
        """
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at,
                          ended_at, cost_usd)
        VALUES (?, ?, ?, ?, NULL, ?, ?, ?)
        """,
        (run_id, issue_id, stage, status, started_at, ended_at, cost_usd),
    )
    await conn.commit()


def _ts(seconds_offset: int) -> str:
    base = datetime(2026, 5, 10, 12, 0, 0, tzinfo=UTC)
    return (base + timedelta(seconds=seconds_offset)).isoformat()


@pytest.mark.asyncio
async def test_stats_empty_db_returns_all_zeros(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        stats = await db.runs.local_review_stats(conn)
    finally:
        await conn.close()
    assert stats.completed_count == 0
    assert stats.interrupted_count == 0
    assert stats.failed_count == 0
    assert stats.running_count == 0
    assert stats.total_cost_usd == 0.0
    assert stats.avg_cost_usd == 0.0
    assert stats.avg_duration_secs == 0.0
    assert stats.approval_rate == 0.0


@pytest.mark.asyncio
async def test_stats_counts_by_status_and_skips_other_stages(
    tmp_path: Path,
) -> None:
    """Only `stage='local_review'` rows count; implement/review rows
    must not pollute the aggregates."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )

        # 2 approved, 1 skipped, 1 failed, 1 running (local_review).
        await _seed_run(
            conn,
            run_id="lr-1",
            issue_id="iss-1",
            stage="local_review",
            status="completed",
            cost_usd=0.12,
            started_at=_ts(0),
            ended_at=_ts(60),
        )
        await _seed_run(
            conn,
            run_id="lr-2",
            issue_id="iss-1",
            stage="local_review",
            status="completed",
            cost_usd=0.34,
            started_at=_ts(100),
            ended_at=_ts(190),
        )
        await _seed_run(
            conn,
            run_id="lr-3",
            issue_id="iss-1",
            stage="local_review",
            status="interrupted",
            cost_usd=0.05,
            started_at=_ts(200),
            ended_at=_ts(220),
        )
        await _seed_run(
            conn,
            run_id="lr-4",
            issue_id="iss-1",
            stage="local_review",
            status="failed",
            cost_usd=0.50,
            started_at=_ts(300),
            ended_at=_ts(420),
        )
        await _seed_run(
            conn,
            run_id="lr-5",
            issue_id="iss-1",
            stage="local_review",
            status="running",
            cost_usd=0.0,
            started_at=_ts(500),
            ended_at=None,
        )
        # An implement row must NOT contribute to the local-review stats.
        await _seed_run(
            conn,
            run_id="im-1",
            issue_id="iss-1",
            stage="implement",
            status="completed",
            cost_usd=99.99,
            started_at=_ts(0),
            ended_at=_ts(600),
        )

        stats = await db.runs.local_review_stats(conn)
    finally:
        await conn.close()

    assert stats.completed_count == 2
    assert stats.interrupted_count == 1
    assert stats.failed_count == 1
    assert stats.running_count == 1
    # total includes the running row's $0 — that's fine, it's $0.
    assert stats.total_cost_usd == pytest.approx(0.12 + 0.34 + 0.05 + 0.50)
    # avg over rows with ended_at: (0.12 + 0.34 + 0.05 + 0.50) / 4 = 0.2525
    assert stats.avg_cost_usd == pytest.approx(0.2525)
    # avg duration over rows with both ts: (60 + 90 + 20 + 120) / 4 = 72.5
    assert stats.avg_duration_secs == pytest.approx(72.5)
    # approval rate = completed / finished = 2 / 4 = 0.5
    assert stats.approval_rate == pytest.approx(0.5)
    # Implement row's $99.99 must NOT leak in.
    assert stats.total_cost_usd < 10.0


@pytest.mark.asyncio
async def test_stats_approval_rate_only_completed(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        for i in range(3):
            await _seed_run(
                conn,
                run_id=f"lr-{i}",
                issue_id="iss-1",
                stage="local_review",
                status="completed",
                cost_usd=0.1,
                started_at=_ts(i * 100),
                ended_at=_ts(i * 100 + 50),
            )
        stats = await db.runs.local_review_stats(conn)
    finally:
        await conn.close()
    assert stats.approval_rate == 1.0


# --- CLI subcommand -----------------------------------------------------


def test_cli_local_review_stats_empty_db(tmp_path: Path) -> None:
    import asyncio

    db_path = tmp_path / "s.sqlite"

    async def _setup() -> None:
        conn = await db.connect(db_path)
        await conn.close()

    asyncio.run(_setup())

    runner = CliRunner()
    result = runner.invoke(
        main, ["runs", "local-review-stats", "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    assert "completed (APPROVED):    0" in result.output
    assert "approval rate:           0.0%" in result.output
    assert "no finished local-review sessions yet" in result.output


def test_cli_local_review_trace_missing_issue_exits_with_error(
    tmp_path: Path,
) -> None:
    import asyncio

    db_path = tmp_path / "s.sqlite"

    async def _setup() -> None:
        conn = await db.connect(db_path)
        await conn.close()

    asyncio.run(_setup())

    runner = CliRunner()
    result = runner.invoke(
        main, ["runs", "local-review-trace", "ENG-404", "--db", str(db_path)]
    )
    assert result.exit_code == 1
    assert "no issue found" in result.output


def test_cli_local_review_trace_issue_with_no_local_reviews(
    tmp_path: Path,
) -> None:
    import asyncio

    db_path = tmp_path / "s.sqlite"

    async def _setup() -> None:
        conn = await db.connect(db_path)
        try:
            await db.issues.upsert(
                conn, id="iss-1", identifier="ENG-7", title="t", team_key="ENG"
            )
            # Only an implement row — no local-review phase ran.
            await _seed_run(
                conn,
                run_id="im-1",
                issue_id="iss-1",
                stage="implement",
                status="completed",
                cost_usd=0.10,
                started_at=_ts(0),
                ended_at=_ts(60),
            )
        finally:
            await conn.close()

    asyncio.run(_setup())

    runner = CliRunner()
    result = runner.invoke(
        main, ["runs", "local-review-trace", "ENG-7", "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    assert "no local-review runs recorded for ENG-7" in result.output


def test_cli_local_review_trace_lists_newest_first_with_duration(
    tmp_path: Path,
) -> None:
    import asyncio

    db_path = tmp_path / "s.sqlite"

    async def _setup() -> None:
        conn = await db.connect(db_path)
        try:
            await db.issues.upsert(
                conn, id="iss-1", identifier="ENG-9", title="t", team_key="ENG"
            )
            await _seed_run(
                conn,
                run_id="lr-old",
                issue_id="iss-1",
                stage="local_review",
                status="failed",
                cost_usd=0.50,
                started_at=_ts(0),
                ended_at=_ts(120),
            )
            await _seed_run(
                conn,
                run_id="lr-new",
                issue_id="iss-1",
                stage="local_review",
                status="completed",
                cost_usd=0.20,
                started_at=_ts(500),
                ended_at=_ts(560),
            )
            # An interleaved implement row should NOT appear.
            await _seed_run(
                conn,
                run_id="im-1",
                issue_id="iss-1",
                stage="implement",
                status="completed",
                cost_usd=99.0,
                started_at=_ts(200),
                ended_at=_ts(400),
            )
        finally:
            await conn.close()

    asyncio.run(_setup())

    runner = CliRunner()
    result = runner.invoke(
        main, ["runs", "local-review-trace", "ENG-9", "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    assert "ENG-9 (2 total)" in result.output
    # Newest first.
    lr_new_idx = result.output.find("lr-new")
    lr_old_idx = result.output.find("lr-old")
    assert lr_new_idx > 0
    assert lr_old_idx > 0
    assert lr_new_idx < lr_old_idx
    # Implement row's $99 must NOT leak in.
    assert "99.0000" not in result.output
    # Durations rendered.
    assert "60.0s" in result.output  # lr-new
    assert "120.0s" in result.output  # lr-old


def test_cli_local_review_stats_with_seeded_rows(tmp_path: Path) -> None:
    import asyncio

    db_path = tmp_path / "s.sqlite"

    async def _setup() -> None:
        conn = await db.connect(db_path)
        try:
            await db.issues.upsert(
                conn,
                id="iss-1",
                identifier="ENG-1",
                title="t",
                team_key="ENG",
            )
            await _seed_run(
                conn,
                run_id="lr-1",
                issue_id="iss-1",
                stage="local_review",
                status="completed",
                cost_usd=0.20,
                started_at=_ts(0),
                ended_at=_ts(120),
            )
            await _seed_run(
                conn,
                run_id="lr-2",
                issue_id="iss-1",
                stage="local_review",
                status="failed",
                cost_usd=0.80,
                started_at=_ts(200),
                ended_at=_ts(320),
            )
        finally:
            await conn.close()

    asyncio.run(_setup())

    runner = CliRunner()
    result = runner.invoke(
        main, ["runs", "local-review-stats", "--db", str(db_path)]
    )
    assert result.exit_code == 0, result.output
    assert "completed (APPROVED):    1" in result.output
    assert "failed (other):          1" in result.output
    assert "approval rate:           50.0%" in result.output
    assert "total cost:              $1.0000" in result.output
    assert "avg cost per session:    $0.5000" in result.output
    assert "avg duration per session: 120.0s" in result.output
