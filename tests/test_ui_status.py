from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import aiosqlite
import pytest

from symphony import db
from symphony.ui.status import compute_canonical_status

NOW = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)


async def _connect(tmp_path: Path) -> aiosqlite.Connection:
    return await db.connect(tmp_path / "state.sqlite")


async def _issue(conn: aiosqlite.Connection, issue_id: str) -> None:
    await db.issues.upsert(
        conn,
        id=issue_id,
        identifier=f"ENG-{issue_id}",
        title=f"Issue {issue_id}",
        team_key="ENG",
    )


async def _run(
    conn: aiosqlite.Connection,
    *,
    run_id: str,
    issue_id: str,
    stage: str,
    status: str,
    started_at: str,
    ended_at: str | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
        VALUES (?, ?, ?, ?, NULL, ?, ?, 0)
        """,
        (run_id, issue_id, stage, status, started_at, ended_at),
    )
    await conn.commit()


@pytest.mark.asyncio
async def test_canonical_status_prefers_operator_wait_and_marks_stuck(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "operator")
        await _run(
            conn,
            run_id="run-operator",
            issue_id="operator",
            stage="review",
            status="running",
            started_at="2026-05-17T11:00:00Z",
        )
        await conn.execute(
            """
            INSERT INTO operator_waits (
                issue_id, run_id, kind, linear_team_key, github_repo, issue_label, created_at
            )
            VALUES (
                'operator', 'run-operator', 'review_stopped', 'ENG', 'org/repo',
                'symphony', '2026-05-17T11:30:00Z'
            )
            """
        )
        await conn.commit()

        status = await compute_canonical_status(conn, "operator", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "awaiting_operator",
        "since": "2026-05-17T11:30:00Z",
        "subtitle": "review_stopped",
        "stuck_for": 1800,
    }


@pytest.mark.asyncio
async def test_canonical_status_detects_running_and_running_threshold(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "running")
        await _run(
            conn,
            run_id="run-running",
            issue_id="running",
            stage="implement",
            status="running",
            started_at="2026-05-17T11:00:00Z",
        )

        status = await compute_canonical_status(conn, "running", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "running",
        "since": "2026-05-17T11:00:00Z",
        "subtitle": "implement",
        "stuck_for": 3600,
    }


@pytest.mark.asyncio
async def test_canonical_status_marks_latest_failed_run_always_stuck(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "failed")
        await _run(
            conn,
            run_id="run-failed",
            issue_id="failed",
            stage="implement",
            status="failed",
            started_at="2026-05-17T11:54:00Z",
            ended_at="2026-05-17T11:55:00Z",
        )

        status = await compute_canonical_status(conn, "failed", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "failed",
        "since": "2026-05-17T11:55:00Z",
        "subtitle": "implement",
        "stuck_for": 300,
    }


@pytest.mark.asyncio
async def test_canonical_status_detects_awaiting_review_trigger_from_review_state(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "review")
        await _run(
            conn,
            run_id="run-review",
            issue_id="review",
            stage="review",
            status="completed",
            started_at="2026-05-17T11:00:00Z",
            ended_at="2026-05-17T11:20:00Z",
        )
        await conn.execute(
            """
            INSERT INTO review_state (issue_id, iteration)
            VALUES ('review', 2)
            """
        )
        await conn.execute(
            """
            INSERT INTO comment_events (comment_id, issue_id, seen_at)
            VALUES ('comment-review', 'review', '2026-05-17T11:40:00Z')
            """
        )
        await conn.commit()

        status = await compute_canonical_status(conn, "review", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "awaiting_review_trigger",
        "since": "2026-05-17T11:40:00Z",
        "subtitle": "iteration=2",
        "stuck_for": 1200,
    }


@pytest.mark.asyncio
async def test_canonical_status_detects_open_pr_and_pr_threshold(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "pr")
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            )
            VALUES (
                'pr', 'org/repo', 'ENG|org/repo', 42, 'https://github.com/org/repo/pull/42',
                '2026-05-16T11:00:00Z', NULL
            )
            """
        )
        await conn.commit()

        status = await compute_canonical_status(conn, "pr", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "pr_open",
        "since": "2026-05-16T11:00:00Z",
        "subtitle": "#42",
        "stuck_for": 90000,
    }


@pytest.mark.asyncio
async def test_canonical_status_prefers_open_pr_over_stale_review_state(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "pr-review")
        await conn.execute(
            """
            INSERT INTO review_state (issue_id, iteration)
            VALUES ('pr-review', 2)
            """
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            )
            VALUES (
                'pr-review', 'org/repo', 'ENG|org/repo', 42,
                'https://github.com/org/repo/pull/42', '2026-05-16T11:00:00Z',
                NULL
            )
            """
        )
        await conn.commit()

        status = await compute_canonical_status(conn, "pr-review", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "pr_open",
        "since": "2026-05-16T11:00:00Z",
        "subtitle": "#42",
        "stuck_for": 90000,
    }


@pytest.mark.asyncio
async def test_canonical_status_marks_issue_done_when_all_prs_are_merged(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "done")
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            )
            VALUES (
                'done', 'org/repo', 'ENG|org/repo', 43, 'https://github.com/org/repo/pull/43',
                '2026-05-16T11:00:00Z', '2026-05-17T10:00:00Z'
            )
            """
        )
        await conn.commit()

        status = await compute_canonical_status(conn, "done", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "done",
        "since": "2026-05-17T10:00:00Z",
        "subtitle": None,
        "stuck_for": None,
    }


@pytest.mark.asyncio
async def test_canonical_status_prefers_done_over_stale_review_state(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "done-review")
        await conn.execute(
            """
            INSERT INTO review_state (issue_id, iteration)
            VALUES ('done-review', 2)
            """
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            )
            VALUES (
                'done-review', 'org/repo', 'ENG|org/repo', 43,
                'https://github.com/org/repo/pull/43', '2026-05-16T11:00:00Z',
                '2026-05-17T10:00:00Z'
            )
            """
        )
        await conn.commit()

        status = await compute_canonical_status(conn, "done-review", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "done",
        "since": "2026-05-17T10:00:00Z",
        "subtitle": None,
        "stuck_for": None,
    }


@pytest.mark.asyncio
async def test_canonical_status_defaults_to_idle(tmp_path: Path) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "idle")

        status = await compute_canonical_status(conn, "idle", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "idle",
        "since": None,
        "subtitle": None,
        "stuck_for": None,
    }
