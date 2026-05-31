from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from symphony import db
from symphony.db.runs import TERMINATION_DETAIL_MAX_BYTES
from symphony.db.termination_backfill import (
    BACKFILL_PREFIX,
    classify_log_termination,
    run_backfill,
)


async def _seed_run(
    db_path: Path,
    *,
    run_id: str,
    issue_id: str | None = None,
    stage: str = "implement",
    status: str = "failed",
    started_at: str = "2026-05-31T00:00:00+00:00",
    termination_kind: str = "",
    termination_detail: str = "",
    exit_returncode: int | None = None,
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
                   termination_kind = ?,
                   termination_detail = ?,
                   exit_returncode = ?
             WHERE id = ?
            """,
            (
                status,
                "2026-05-31T00:01:00+00:00",
                termination_kind,
                termination_detail,
                exit_returncode,
                run_id,
            ),
        )
        await conn.commit()
    finally:
        await conn.close()


def _write_log(log_root: Path, run_id: str, *events: object) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    lines = [
        event if isinstance(event, str) else json.dumps(event, separators=(",", ":"))
        for event in events
    ]
    (log_root / f"{run_id}.log").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_local_review_log(
    log_root: Path, parent_run_id: str, name: str, *events: object
) -> None:
    log_dir = log_root / "local_review" / parent_run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        event if isinstance(event, str) else json.dumps(event, separators=(",", ":"))
        for event in events
    ]
    (log_dir / name).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _rows(db_path: Path) -> dict[str, sqlite3.Row]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT id, status, termination_kind, termination_detail, exit_returncode
            FROM runs
            ORDER BY id
            """
        ).fetchall()
    finally:
        conn.close()
    return {row["id"]: row for row in rows}


@pytest.mark.asyncio
async def test_backfill_updates_empty_terminal_runs_and_is_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(db_path, run_id="agent-failed", status="failed")
    await _seed_run(db_path, run_id="stalled", status="interrupted")
    await _seed_run(db_path, run_id="monitor", stage="review", status="failed")
    await _seed_run(db_path, run_id="missing", status="failed")
    await _seed_run(db_path, run_id="needs-approval", status="needs_approval")
    await _seed_run(db_path, run_id="success", status="completed")
    await _seed_run(
        db_path,
        run_id="live-captured",
        status="failed",
        termination_kind="spawn_failed",
        termination_detail="live detail",
        exit_returncode=127,
    )

    _write_log(
        log_root,
        "agent-failed",
        {"type": "turn.failed", "error": {"message": "model crashed"}},
    )
    _write_log(log_root, "stalled", {"type": "item.started", "item": {"id": "cmd"}})
    _write_log(log_root, "monitor", "[stderr] monitor stopped")
    _write_log(log_root, "success", {"type": "turn.failed", "error": "ignore"})
    _write_log(log_root, "live-captured", {"type": "turn.failed", "error": "ignore"})

    first = run_backfill(db_path=db_path, log_root=log_root)
    second = run_backfill(db_path=db_path, log_root=log_root)

    assert first.updated == 5
    assert second.updated == 0
    assert dict(first.aggregate) == {
        "agent_nonzero_exit": 1,
        "awaiting_human_merge": 1,
        "monitor_terminated": 1,
        "spawn_failed": 1,
        "stall_timeout": 1,
        "unknown": 1,
    }

    rows = _rows(db_path)
    assert rows["agent-failed"]["termination_kind"] == "agent_nonzero_exit"
    assert "model crashed" in rows["agent-failed"]["termination_detail"]
    assert rows["stalled"]["termination_kind"] == "stall_timeout"
    assert rows["monitor"]["termination_kind"] == "monitor_terminated"
    assert rows["missing"]["termination_kind"] == "unknown"
    assert "missing log" in rows["missing"]["termination_detail"]
    assert rows["needs-approval"]["termination_kind"] == "awaiting_human_merge"
    assert "needs_approval" in rows["needs-approval"]["termination_detail"]
    assert rows["success"]["termination_kind"] == ""
    assert rows["live-captured"]["termination_kind"] == "spawn_failed"
    assert rows["live-captured"]["termination_detail"] == "live detail"
    assert rows["live-captured"]["exit_returncode"] == 127
    for run_id in ("agent-failed", "stalled", "monitor", "missing", "needs-approval"):
        assert rows[run_id]["termination_detail"].startswith(BACKFILL_PREFIX)
        assert rows[run_id]["exit_returncode"] is None


@pytest.mark.asyncio
async def test_backfill_rejects_missing_log_root_before_marking_runs_unknown(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    await _seed_run(db_path, run_id="still-eligible", status="failed")

    with pytest.raises(FileNotFoundError, match="log root not found"):
        run_backfill(db_path=db_path, log_root=tmp_path / "missing-logs")

    rows = _rows(db_path)
    assert rows["still-eligible"]["termination_kind"] == ""
    assert rows["still-eligible"]["termination_detail"] == ""
    assert rows["still-eligible"]["exit_returncode"] is None


@pytest.mark.asyncio
async def test_backfill_rejects_log_root_that_is_not_a_directory(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    log_root.write_text("not a directory", encoding="utf-8")
    await _seed_run(db_path, run_id="still-eligible", status="failed")

    with pytest.raises(NotADirectoryError, match="log root is not a directory"):
        run_backfill(db_path=db_path, log_root=log_root)

    rows = _rows(db_path)
    assert rows["still-eligible"]["termination_kind"] == ""
    assert rows["still-eligible"]["termination_detail"] == ""
    assert rows["still-eligible"]["exit_returncode"] is None


def test_classify_log_termination_completed_tail_on_failed_run_is_agent_exit(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "run.log"
    log_path.write_text(
        json.dumps({"type": "turn.completed", "usage": {"input_tokens": 1}}) + "\n",
        encoding="utf-8",
    )

    result = classify_log_termination(log_path=log_path, stage="implement")

    assert result.kind == "agent_nonzero_exit"
    assert result.detail.startswith(BACKFILL_PREFIX)
    assert "turn.completed" in result.detail


@pytest.mark.asyncio
async def test_backfill_reads_local_review_transcript_directory(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    issue_id = "iss-local-review"
    await _seed_run(
        db_path,
        run_id="implement-parent",
        issue_id=issue_id,
        stage="implement",
        status="completed",
        started_at="2026-05-31T00:00:00+00:00",
    )
    await _seed_run(
        db_path,
        run_id="local-review-row",
        issue_id=issue_id,
        stage="local_review",
        status="failed",
        started_at="2026-05-31T00:01:00+00:00",
    )
    _write_local_review_log(
        log_root,
        "implement-parent",
        "review-0.out.log",
        {"type": "turn.failed", "error": {"message": "reviewer crashed"}},
    )

    result = run_backfill(db_path=db_path, log_root=log_root)

    assert result.updated == 1
    rows = _rows(db_path)
    assert rows["local-review-row"]["termination_kind"] == "agent_nonzero_exit"
    assert "reviewer crashed" in rows["local-review-row"]["termination_detail"]


def test_backfilled_detail_keeps_prefix_and_live_termination_cap(tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    body = "\n".join(f"line {i:03d} " + ("x" * 160) for i in range(200))
    log_path.write_text(body, encoding="utf-8")

    result = classify_log_termination(log_path=log_path, stage="implement")

    assert result.kind == "unknown"
    assert result.detail.startswith(BACKFILL_PREFIX)
    assert len(result.detail.encode("utf-8")) <= TERMINATION_DETAIL_MAX_BYTES
    assert "line 199" in result.detail
    assert "line 000" not in result.detail
