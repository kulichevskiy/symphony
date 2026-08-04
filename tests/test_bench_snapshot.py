import asyncio
import json
from pathlib import Path

from click.testing import CliRunner

from symphony import db
from symphony.agent.model_usage import ModelUsage
from symphony.bench.metrics import snapshot_candidate
from symphony.cli import main


async def _seed_run(path: Path, *, provider: str = "codex") -> None:
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
        cache_write_tokens = 20 if provider == "claude" else 0
        await db.runs.add_usage(
            conn,
            "run-1",
            cost_usd=1.25,
            input_tokens=100,
            output_tokens=50,
            cache_write_tokens=cache_write_tokens,
            cache_read_tokens=10,
        )
        await db.run_model_usage.replace_for_run(
            conn,
            "run-1",
            [
                ModelUsage(
                    provider=provider,
                    model="claude-opus-4-8" if provider == "claude" else "gpt-5.6-sol",
                    input_tokens=100,
                    output_tokens=50,
                    cache_write_tokens=cache_write_tokens,
                    cache_read_tokens=10,
                )
            ],
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
        "cache_read_tokens": 10,
        "cache_write_tokens": 0,
        "cost_usd": 1.25,
        "effective_tokens": 151.0,
        "input_tokens": 100,
        "output_tokens": 50,
        "raw_tokens": 150,
        "remote_review_state_transitions": 2,
        "runs_by_status": {"completed": 1},
        "token_metrics_unavailable": False,
    }


def test_bench_snapshot_reports_claude_raw_tokens(tmp_path: Path) -> None:
    db_path = tmp_path / "candidate.sqlite"
    asyncio.run(_seed_run(db_path, provider="claude"))

    snapshot = asyncio.run(snapshot_candidate(db_path))

    assert snapshot["raw_tokens"] == 180
    assert snapshot["effective_tokens"] == 176.0
    assert snapshot["token_metrics_unavailable"] is False


def test_bench_snapshot_rejects_unreconciled_model_usage(tmp_path: Path) -> None:
    db_path = tmp_path / "candidate.sqlite"
    asyncio.run(_seed_run(db_path))

    async def corrupt_usage() -> None:
        conn = await db.connect(db_path)
        try:
            await conn.execute("UPDATE run_model_usage SET input_tokens = input_tokens - 1")
            await conn.commit()
        finally:
            await conn.close()

    asyncio.run(corrupt_usage())

    snapshot = asyncio.run(snapshot_candidate(db_path))

    assert snapshot["raw_tokens"] is None
    assert snapshot["token_metrics_unavailable"] is True


def test_bench_snapshot_rejects_compensating_per_run_usage_errors(tmp_path: Path) -> None:
    db_path = tmp_path / "candidate.sqlite"
    asyncio.run(_seed_run(db_path))

    async def add_and_corrupt_second_run() -> None:
        conn = await db.connect(db_path)
        try:
            await db.runs.create(
                conn,
                id="run-2",
                issue_id="issue-1",
                stage="implement",
                status="completed",
                pid=456,
                started_at="2026-08-01T11:00:00+00:00",
            )
            await db.runs.update_status(
                conn,
                "run-2",
                "completed",
                ended_at="2026-08-01T11:01:00+00:00",
            )
            await db.runs.add_usage(
                conn,
                "run-2",
                cost_usd=0.5,
                input_tokens=100,
                output_tokens=50,
                cache_write_tokens=20,
                cache_read_tokens=10,
            )
            await db.run_model_usage.replace_for_run(
                conn,
                "run-2",
                [
                    ModelUsage(
                        provider="claude",
                        model="claude-opus-4-8",
                        input_tokens=100,
                        output_tokens=50,
                        cache_write_tokens=20,
                        cache_read_tokens=10,
                    )
                ],
            )
            await conn.execute(
                "UPDATE run_model_usage SET input_tokens = input_tokens - 1 WHERE run_id = 'run-1'"
            )
            await conn.execute(
                "UPDATE run_model_usage SET input_tokens = input_tokens + 1 WHERE run_id = 'run-2'"
            )
            await conn.commit()
        finally:
            await conn.close()

    asyncio.run(add_and_corrupt_second_run())

    snapshot = asyncio.run(snapshot_candidate(db_path))

    assert snapshot["raw_tokens"] is None
    assert snapshot["token_metrics_unavailable"] is True


def test_bench_snapshot_defers_reconciliation_while_run_is_active(tmp_path: Path) -> None:
    db_path = tmp_path / "candidate.sqlite"

    async def seed_active_run() -> None:
        conn = await db.connect(db_path)
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
                status="running",
                pid=123,
                started_at="2026-08-01T10:00:00+00:00",
            )
            await db.runs.add_usage(
                conn,
                "run-1",
                cost_usd=0.1,
                input_tokens=100,
                output_tokens=20,
                cache_write_tokens=0,
                cache_read_tokens=10,
            )
        finally:
            await conn.close()

    asyncio.run(seed_active_run())

    snapshot = asyncio.run(snapshot_candidate(db_path))

    assert snapshot["runs_by_status"] == {"running": 1}
    assert snapshot["raw_tokens"] is None
    assert snapshot["token_metrics_unavailable"] is True


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
