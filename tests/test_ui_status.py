from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiosqlite
import pytest

from symphony import db
from symphony.ui.status import (
    ExternalDriftFlag,
    ExternalStatusSnapshot,
    compute_canonical_status,
)
from symphony.ui.warnings import issue_warnings

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


async def _operator_wait(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    run_id: str,
    kind: str,
    created_at: str,
) -> None:
    await conn.execute(
        """
        INSERT OR IGNORE INTO runs (
            id, issue_id, stage, status, pid, started_at, ended_at, cost_usd
        )
        VALUES (?, ?, 'review', 'completed', NULL, ?, ?, 0)
        """,
        (run_id, issue_id, created_at, created_at),
    )
    await conn.execute(
        """
        INSERT INTO operator_waits (
            issue_id, run_id, kind, linear_team_key, github_repo, issue_label, created_at
        )
        VALUES (?, ?, ?, 'ENG', 'org/repo', 'symphony', ?)
        """,
        (issue_id, run_id, kind, created_at),
    )
    await conn.commit()


@pytest.mark.asyncio
async def test_canonical_status_prefers_operator_wait_and_keeps_kind_subtitle(
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
        await _operator_wait(
            conn,
            issue_id="operator",
            run_id="run-operator",
            kind=db.operator_waits.KIND_REVIEW_STOPPED,
            created_at="2026-05-17T11:30:00Z",
        )

        status = await compute_canonical_status(conn, "operator", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "paused",
        "since": "2026-05-17T11:30:00Z",
        "subtitle": "review_stopped",
        "stuck_for": 1800,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "expected_state"),
    [
        (db.operator_waits.KIND_IMPLEMENT_FAILED, "halted"),
        (db.operator_waits.KIND_DELIVER_FAILED, "halted"),
        (db.operator_waits.KIND_REVIEW_FAILED, "halted"),
        (db.operator_waits.KIND_REVIEW_STOPPED, "paused"),
        (db.operator_waits.KIND_MERGE, "awaiting_merge"),
    ],
)
async def test_canonical_status_splits_operator_wait_kind(
    tmp_path: Path,
    kind: str,
    expected_state: str,
) -> None:
    conn = await _connect(tmp_path)
    try:
        issue_id = f"operator-{kind}"
        await _issue(conn, issue_id)
        await _operator_wait(
            conn,
            issue_id=issue_id,
            run_id=f"run-{kind}",
            kind=kind,
            created_at="2026-05-17T11:59:00Z",
        )

        status = await compute_canonical_status(conn, issue_id, now=NOW)
    finally:
        await conn.close()

    payload = status.to_dict()
    assert payload["state"] == expected_state
    assert payload["subtitle"] == kind


@pytest.mark.asyncio
async def test_canonical_status_ignores_review_failed_wait_after_completed_fix(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "stale-review-wait")
        await _run(
            conn,
            run_id="review-monitor",
            issue_id="stale-review-wait",
            stage="review",
            status="failed",
            started_at="2026-05-17T09:05:22Z",
            ended_at="2026-05-17T09:16:55Z",
        )
        await _run(
            conn,
            run_id="review-fix",
            issue_id="stale-review-wait",
            stage="review_fix",
            status="completed",
            started_at="2026-05-17T09:09:55Z",
            ended_at="2026-05-17T09:18:18Z",
        )
        await _operator_wait(
            conn,
            issue_id="stale-review-wait",
            run_id="review-monitor",
            kind=db.operator_waits.KIND_REVIEW_FAILED,
            created_at="2026-05-17T09:16:55Z",
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at,
                merged_at
            )
            VALUES (
                'stale-review-wait', 'org/repo', 'ENG|org/repo', 200,
                'https://github.com/org/repo/pull/200', '2026-05-17T09:00:00Z',
                NULL
            )
            """
        )
        await conn.commit()

        status = await compute_canonical_status(conn, "stale-review-wait", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "pr_open",
        "since": "2026-05-17T09:00:00Z",
        "subtitle": "#200",
        "stuck_for": None,
    }


@pytest.mark.asyncio
async def test_canonical_status_prefers_running_review_fix_over_review_failed_wait(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "running-review-fix")
        await _run(
            conn,
            run_id="review-monitor",
            issue_id="running-review-fix",
            stage="review",
            status="failed",
            started_at="2026-05-17T09:05:22Z",
            ended_at="2026-05-17T09:16:55Z",
        )
        await _run(
            conn,
            run_id="review-fix",
            issue_id="running-review-fix",
            stage="review_fix",
            status="running",
            started_at="2026-05-17T09:09:55Z",
        )
        await _operator_wait(
            conn,
            issue_id="running-review-fix",
            run_id="review-monitor",
            kind=db.operator_waits.KIND_REVIEW_FAILED,
            created_at="2026-05-17T09:16:55Z",
        )

        status = await compute_canonical_status(conn, "running-review-fix", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "running",
        "since": "2026-05-17T09:09:55Z",
        "subtitle": "review_fix",
        "stuck_for": 10205,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("issue_id", "kind", "created_at", "expected_stuck_for"),
    [
        ("halted-now", db.operator_waits.KIND_IMPLEMENT_FAILED, "2026-05-17T12:00:00Z", 0),
        ("paused-fresh", db.operator_waits.KIND_REVIEW_STOPPED, "2026-05-17T11:46:00Z", None),
        ("paused-stuck", db.operator_waits.KIND_REVIEW_STOPPED, "2026-05-17T11:45:00Z", 900),
        ("merge-fresh", db.operator_waits.KIND_MERGE, "2026-05-17T08:01:00Z", None),
        ("merge-stuck", db.operator_waits.KIND_MERGE, "2026-05-17T08:00:00Z", 14400),
    ],
)
async def test_canonical_status_applies_operator_wait_stuck_thresholds(
    tmp_path: Path,
    issue_id: str,
    kind: str,
    created_at: str,
    expected_stuck_for: int | None,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, issue_id)
        await _operator_wait(
            conn,
            issue_id=issue_id,
            run_id=f"run-{issue_id}",
            kind=kind,
            created_at=created_at,
        )

        status = await compute_canonical_status(conn, issue_id, now=NOW)
    finally:
        await conn.close()

    assert status.stuck_for == expected_stuck_for


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "drift_kind"),
    [
        ("linear.state", "linear_state_done"),
        ("github.state", "merge_zombie"),
        ("github.state", "pr_closed_no_merge"),
        ("github.merged_at", "pr_locally_merged"),
    ],
)
async def test_canonical_status_promotes_external_drift(
    tmp_path: Path,
    field: str,
    drift_kind: str,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "drift")
        await _operator_wait(
            conn,
            issue_id="drift",
            run_id="run-drift",
            kind=db.operator_waits.KIND_MERGE,
            created_at="2026-05-17T11:00:00Z",
        )

        status = await compute_canonical_status(
            conn,
            "drift",
            now=NOW,
            external_snapshot=ExternalStatusSnapshot(
                drift_flags=(
                    ExternalDriftFlag(
                        field=field,
                        source_name=field.partition(".")[0],
                        flagged_at="2026-05-17T09:30:00Z",
                        drift_kind=drift_kind,
                    ),
                )
            ),
        )
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "drift_detected",
        "since": "2026-05-17T09:30:00Z",
        "subtitle": "1 field(s) disagree",
        "stuck_for": 9000,
    }


@pytest.mark.asyncio
async def test_canonical_status_falls_back_when_external_snapshot_has_no_drift(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "clean-snapshot")
        await _operator_wait(
            conn,
            issue_id="clean-snapshot",
            run_id="run-clean-snapshot",
            kind=db.operator_waits.KIND_MERGE,
            created_at="2026-05-17T11:00:00Z",
        )

        status = await compute_canonical_status(
            conn,
            "clean-snapshot",
            now=NOW,
            external_snapshot=ExternalStatusSnapshot(drift_flags=()),
        )
    finally:
        await conn.close()

    assert status.state == "awaiting_merge"


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
async def test_canonical_status_treats_interrupted_run_as_failed(
    tmp_path: Path,
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "interrupted")
        await _run(
            conn,
            run_id="run-interrupted",
            issue_id="interrupted",
            stage="review",
            status="interrupted",
            started_at="2026-05-17T11:40:00Z",
            ended_at="2026-05-17T11:55:00Z",
        )

        status = await compute_canonical_status(conn, "interrupted", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "failed",
        "since": "2026-05-17T11:55:00Z",
        "subtitle": "review (interrupted)",
        "stuck_for": 300,
    }


@pytest.mark.asyncio
async def test_canonical_status_interrupted_run_does_not_mask_open_pr(
    tmp_path: Path,
) -> None:
    # An orphaned `merge` run (host restart killed the PID) must not show as
    # Halted/Failed while the PR is still open and being polled — the merge
    # poll re-drives it, so the issue is really PR_OPEN.
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "orphan-merge")
        await _run(
            conn,
            run_id="run-orphan-merge",
            issue_id="orphan-merge",
            stage="merge",
            status="interrupted",
            started_at="2026-05-17T11:40:00Z",
            ended_at="2026-05-17T11:55:00Z",
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            )
            VALUES (
                'orphan-merge', 'org/repo', 'ENG|org/repo', 42,
                'https://github.com/org/repo/pull/42', '2026-05-16T11:00:00Z', NULL
            )
            """
        )
        await conn.commit()

        status = await compute_canonical_status(conn, "orphan-merge", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict() == {
        "state": "pr_open",
        "since": "2026-05-16T11:00:00Z",
        "subtitle": "#42",
        "stuck_for": 90000,
    }


@pytest.mark.asyncio
async def test_canonical_status_failed_run_still_masks_open_pr(
    tmp_path: Path,
) -> None:
    # A genuine `failed` run (not an orphan) still surfaces as FAILED even with
    # an open PR — only `interrupted` orphans get the PR_OPEN fall-through.
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, "failed-pr")
        await _run(
            conn,
            run_id="run-failed-pr",
            issue_id="failed-pr",
            stage="merge",
            status="failed",
            started_at="2026-05-17T11:40:00Z",
            ended_at="2026-05-17T11:55:00Z",
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            )
            VALUES (
                'failed-pr', 'org/repo', 'ENG|org/repo', 42,
                'https://github.com/org/repo/pull/42', '2026-05-16T11:00:00Z', NULL
            )
            """
        )
        await conn.commit()

        status = await compute_canonical_status(conn, "failed-pr", now=NOW)
    finally:
        await conn.close()

    assert status.to_dict()["state"] == "failed"


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
@pytest.mark.parametrize(
    ("issue_id", "state_seed", "latest_activity_age_secs", "expected"),
    [
        ("fresh-pr", "pr_open", 2 * 60 * 60, []),
        ("stale-pr", "pr_open", 2 * 60 * 60 + 1, ["no_progress"]),
        ("running-issue", "running", 5 * 60 * 60, []),
        ("unknown-age", "pr_open", None, []),
    ],
)
async def test_no_progress_warning_respects_threshold_and_pr_open_state(
    tmp_path: Path,
    issue_id: str,
    state_seed: str,
    latest_activity_age_secs: int | None,
    expected: list[str],
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _issue(conn, issue_id)
        if state_seed == "pr_open":
            await conn.execute(
                """
                INSERT INTO issue_prs (
                    issue_id, github_repo, binding_key, pr_number, pr_url, created_at,
                    merged_at
                )
                VALUES (?, 'org/repo', 'ENG|org/repo', 42,
                        'https://github.com/org/repo/pull/42',
                        '2026-05-17T11:00:00Z', NULL)
                """,
                (issue_id,),
            )
            await conn.commit()
        else:
            await _run(
                conn,
                run_id=f"run-{issue_id}",
                issue_id=issue_id,
                stage="implement",
                status="running",
                started_at="2026-05-17T11:00:00Z",
            )

        status = await compute_canonical_status(conn, issue_id, now=NOW)
    finally:
        await conn.close()

    assert issue_warnings(
        status,
        latest_activity_age_secs=latest_activity_age_secs,
        pr_no_progress_threshold=timedelta(hours=2),
    ) == expected


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
