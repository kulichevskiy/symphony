from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner

from symphony import db
from symphony.cli import main


async def _seed_run(
    db_path: Path,
    *,
    run_id: str,
    issue_id: str | None = None,
    stage: str = "implement",
    status: str = "completed",
    started_at: str = "2026-05-31T00:00:00+00:00",
    tokens: tuple[int, int, int, int] = (0, 0, 0, 0),
) -> None:
    storage_issue_id = issue_id or f"iss-{run_id}"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id=storage_issue_id,
            identifier=f"ENG-{storage_issue_id}",
            title="test",
            team_key="ENG",
        )
        await db.runs.create(
            conn,
            id=run_id,
            issue_id=storage_issue_id,
            stage=stage,
            status="running",
            pid=None,
            started_at=started_at,
        )
        await conn.execute(
            """
            UPDATE runs
               SET status = ?,
                   ended_at = ?,
                   input_tokens = ?,
                   output_tokens = ?,
                   cache_write_tokens = ?,
                   cache_read_tokens = ?
             WHERE id = ?
            """,
            (
                status,
                "2026-05-31T00:01:00+00:00",
                *tokens,
                run_id,
            ),
        )
        await conn.commit()
    finally:
        await conn.close()


def _write_log(path: Path, *events: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        event if isinstance(event, str) else json.dumps(event, separators=(",", ":"))
        for event in events
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _token_rows(db_path: Path) -> dict[str, tuple[int, int, int, int]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT id, input_tokens, output_tokens,
                   cache_write_tokens, cache_read_tokens
            FROM runs
            ORDER BY id
            """
        ).fetchall()
    finally:
        conn.close()
    return {
        str(row["id"]): (
            int(row["input_tokens"]),
            int(row["output_tokens"]),
            int(row["cache_write_tokens"]),
            int(row["cache_read_tokens"]),
        )
        for row in rows
    }


@pytest.mark.asyncio
async def test_runs_backfill_tokens_from_out_logs_and_is_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    issue_id = "iss-shared"
    await _seed_run(
        db_path,
        run_id="implement-parent",
        issue_id=issue_id,
        started_at="2026-05-31T00:00:00+00:00",
        tokens=(999, 999, 999, 999),
    )
    await _seed_run(
        db_path,
        run_id="local-review-row",
        issue_id=issue_id,
        stage="local_review",
        started_at="2026-05-31T00:02:00+00:00",
        tokens=(1, 1, 1, 1),
    )
    await _seed_run(db_path, run_id="missing-log")
    await _seed_run(db_path, run_id="bad-log", tokens=(7, 7, 7, 7))
    await _seed_run(db_path, run_id="codex-run", tokens=(2, 2, 2, 2))

    _write_log(
        log_root / "implement-parent.log",
        "not json",
        {
            "type": "result",
            "total_cost_usd": 0.10,
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_creation_input_tokens": 30,
                "cache_read_input_tokens": 20,
            },
        },
    )
    _write_log(
        log_root / "local_review" / "implement-parent" / "review-0.out.log",
        {
            "type": "result",
            "total_cost_usd": 0.20,
            "usage": {
                "input_tokens": 200,
                "output_tokens": 30,
                "cache_creation_input_tokens": 5,
                "cache_read_input_tokens": 7,
            },
        },
    )
    _write_log(
        log_root / "local_review" / "implement-parent" / "fix-0.out.log",
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 300,
                "output_tokens": 40,
                "cache_write_tokens": 11,
                "cached_input_tokens": 13,
            },
        },
    )
    _write_log(
        log_root / "codex-run.log",
        {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": 700,
                    "output_tokens": 90,
                    "cached_input_tokens": 60,
                }
            },
        },
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 900,
                "output_tokens": 120,
                "cache_write_tokens": 4,
                "cached_input_tokens": 80,
            },
        },
    )
    _write_log(log_root / "bad-log.out.log", "not json", {"type": "system"})

    first = CliRunner().invoke(
        main,
        [
            "runs",
            "backfill-tokens",
            "--db",
            str(db_path),
            "--log-root",
            str(log_root),
        ],
    )
    assert first.exit_code == 0, first.output
    assert "updated: 3" in first.output
    assert "skipped: 2" in first.output

    rows = _token_rows(db_path)
    assert rows["implement-parent"] == (100, 50, 30, 20)
    assert rows["local-review-row"] == (500, 70, 16, 20)
    assert rows["codex-run"] == (900, 120, 4, 80)
    assert rows["bad-log"] == (7, 7, 7, 7)
    assert rows["missing-log"] == (0, 0, 0, 0)

    second = CliRunner().invoke(
        main,
        [
            "runs",
            "backfill-tokens",
            "--db",
            str(db_path),
            "--log-root",
            str(log_root),
        ],
    )
    assert second.exit_code == 0, second.output
    assert "updated: 3" in second.output
    assert "skipped: 2" in second.output
    assert _token_rows(db_path) == rows


@pytest.mark.asyncio
async def test_runs_backfill_tokens_skips_running_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(
        db_path,
        run_id="live-run",
        status="running",
        tokens=(9, 8, 7, 6),
    )
    _write_log(
        log_root / "live-run.out.log",
        {
            "type": "result",
            "total_cost_usd": 0.10,
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_write_tokens": 30,
                "cached_input_tokens": 20,
            },
        },
    )

    result = CliRunner().invoke(
        main,
        [
            "runs",
            "backfill-tokens",
            "--db",
            str(db_path),
            "--log-root",
            str(log_root),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "updated: 0" in result.output
    assert "skipped: 1" in result.output
    assert _token_rows(db_path)["live-run"] == (9, 8, 7, 6)
