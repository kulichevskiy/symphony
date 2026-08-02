import asyncio
import json
from pathlib import Path

from click.testing import CliRunner

from symphony import db
from symphony.bench.metrics import snapshot_candidate
from symphony.cli import main


async def _seed_run(path: Path) -> None:
    conn = await db.connect(path)
    try:
        await db.issues.upsert(
            conn,
            id="issue-1",
            identifier="BENCH-1",
            title="Ticket",
            team_key="BENCH",
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id="issue-1",
            stage="implement",
            status="completed",
            pid=123,
            started_at="2026-08-01T10:00:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "run-1",
            "completed",
            ended_at="2026-08-01T10:02:00+00:00",
        )
        await db.runs.add_usage(
            conn,
            "run-1",
            cost_usd=0,
            input_tokens=100,
            output_tokens=50,
            cache_write_tokens=20,
            cache_read_tokens=10,
        )
        await db.review_state.begin_review(
            conn,
            "issue-1",
            pr_number=1,
            pr_url="https://github.com/example/repo/pull/1",
            github_repo="example/repo",
            issue_label="BENCH-1",
        )
        await db.review_state.set_codex_review_requested_at(conn, "issue-1", "2026-08-01T10:03:00Z")
        await db.review_state.set_codex_review_requested_at(conn, "issue-1", "2026-08-01T10:04:00Z")
    finally:
        await conn.close()


def test_bench_snapshot_reports_stable_safety_metrics(tmp_path: Path) -> None:
    db_path = tmp_path / "candidate.sqlite"
    asyncio.run(_seed_run(db_path))

    result = CliRunner().invoke(main, ["bench", "snapshot", "--db", str(db_path)])

    assert result.exit_code == 0, result.output
    snapshot = json.loads(result.output.splitlines()[-1])
    assert snapshot == {
        "active_agent_seconds": 120.0,
        "agent_launches": 1,
        "effective_tokens": 176.0,
        "remote_review_rounds": 2,
        "runs_by_status": {"completed": 1},
    }


async def _seed_nested_local_review(path: Path) -> None:
    await _seed_run(path)
    conn = await db.connect(path)
    try:
        await db.runs.create(
            conn,
            id="run-local-review",
            issue_id="issue-1",
            stage="local_review",
            status="completed",
            pid=124,
            started_at="2026-08-01T10:00:30+00:00",
        )
        await db.runs.update_status(
            conn,
            "run-local-review",
            "completed",
            ended_at="2026-08-01T10:01:30+00:00",
        )
    finally:
        await conn.close()


def test_bench_snapshot_counts_local_subprocesses_without_nested_duration(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "candidate.sqlite"
    asyncio.run(_seed_nested_local_review(db_path))
    review_root = tmp_path / "logs/local_review/parent"
    review_root.mkdir(parents=True)
    for name in (
        "review-0-find.out.log",
        "review-0-verify.out.log",
        "fix-0.out.log",
    ):
        (review_root / name).write_text("transcript", encoding="utf-8")

    snapshot = asyncio.run(snapshot_candidate(db_path, tmp_path / "logs"))

    assert snapshot["active_agent_seconds"] == 120.0
    assert snapshot["agent_launches"] == 4
    assert snapshot["local_review_agent_launches"] == 3
    assert snapshot["runs_by_status"] == {"completed": 2}
