from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite
import httpx
import pytest

from symphony import db
from symphony.app import create_app
from symphony.ui import api as ui_api
from symphony.webhook import WebhookSettings

from .test_webhook import NOW, SECRET, _body, _Handler, _headers, _payload

UI_NOW = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)


def _token_totals(
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_write_tokens: int = 0,
    cache_read_tokens: int = 0,
) -> dict[str, int]:
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_write_tokens": cache_write_tokens,
        "cache_read_tokens": cache_read_tokens,
    }


class _FakeExternalService:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.cache: dict[str, Any] = {}

    async def get_issue_external(
        self,
        conn: aiosqlite.Connection,
        issue_id: str,
        *,
        refresh: bool = False,
    ) -> dict[str, Any] | None:
        return self.payload


def _dist(tmp_path: Path) -> Path:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text(
        "<!doctype html><html><body>symphony UI v0</body></html>"
    )
    return dist


async def _seed_issue_detail(conn: aiosqlite.Connection) -> None:
    await db.issues.upsert(
        conn,
        id="iss-1",
        identifier="ENG-1",
        title="Fix the thing",
        team_key="ENG",
    )
    await conn.execute(
        """
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
        VALUES
            ('run-1', 'iss-1', 'implement', 'completed', 123, '2026-05-17T10:00:00Z',
             '2026-05-17T10:10:00Z', 1.25),
            ('run-2', 'iss-1', 'review', 'running', NULL, '2026-05-17T10:20:00Z',
             NULL, 0.5)
        """
    )
    await conn.execute(
        """
        UPDATE runs
        SET input_tokens = 100, output_tokens = 20,
            cache_write_tokens = 30, cache_read_tokens = 40
        WHERE id = 'run-1'
        """
    )
    await conn.execute(
        """
        INSERT INTO issue_prs (
            issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
        )
        VALUES (
            'iss-1', 'org/repo', 'ENG|org/repo', 42,
            'https://github.com/org/repo/pull/42', '2026-05-17T10:11:00Z', NULL
        )
        """
    )
    await conn.execute(
        """
        INSERT INTO operator_waits (
            issue_id, run_id, kind, linear_team_key, github_repo, issue_label, created_at
        )
        VALUES (
            'iss-1', 'run-2', 'review_stopped', 'ENG', 'org/repo', 'symphony',
            '2026-05-17T10:30:00Z'
        )
        """
    )
    await conn.execute(
        """
        INSERT INTO review_state (
            issue_id, iteration, last_trigger_signature, ci_fetch_failures, pr_number,
            pr_url, github_repo, issue_label, codex_lgtm_comment_id
        )
        VALUES (
            'iss-1', 3, 'sig', 1, 42, 'https://github.com/org/repo/pull/42',
            'org/repo', 'symphony', 'comment-9'
        )
        """
    )
    await conn.execute(
        """
        INSERT INTO activity_comment_marks (
            run_id, first_unpublished_at, last_event_at, event_count_since_post,
            last_posted_at, last_fingerprint
        )
        VALUES (
            'run-2', '2026-05-17T10:21:00Z', '2026-05-17T10:22:00Z', 7,
            '2026-05-17T10:25:00Z', 'fp'
        )
        """
    )
    await conn.execute(
        """
        INSERT INTO issue_cost_marks (issue_id, warning_posted_at)
        VALUES ('iss-1', '2026-05-17T10:40:00Z')
        """
    )
    for idx in range(55):
        await conn.execute(
            """
            INSERT INTO comment_events (comment_id, issue_id, seen_at)
            VALUES (?, 'iss-1', ?)
            """,
            (f"comment-{idx:02d}", f"2026-05-17T11:{idx:02d}:00Z"),
        )
    await conn.commit()


async def _seed_issue_timeline(conn: aiosqlite.Connection) -> None:
    await db.issues.upsert(
        conn,
        id="iss-timeline",
        identifier="ENG-2",
        title="Watch the thing",
        team_key="ENG",
    )
    await conn.execute(
        """
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
        VALUES (
            'run-timeline', 'iss-timeline', 'implement', 'completed', 456,
            '2026-05-17T10:00:00Z', '2026-05-17T10:05:00Z', 2.5
        )
        """
    )
    await conn.execute(
        """
        INSERT INTO issue_prs (
            issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
        )
        VALUES (
            'iss-timeline', 'org/repo', 'ENG|org/repo', 43,
            'https://github.com/org/repo/pull/43',
            '2026-05-17T10:02:00Z', '2026-05-17T10:06:00Z'
        )
        """
    )
    await conn.execute(
        """
        INSERT INTO comment_events (comment_id, issue_id, seen_at)
        VALUES ('comment-timeline', 'iss-timeline', '2026-05-17T10:01:00Z')
        """
    )
    await conn.execute(
        """
        INSERT INTO activity_comment_marks (
            run_id, first_unpublished_at, last_event_at, event_count_since_post,
            last_posted_at, last_fingerprint
        )
        VALUES (
            'run-timeline', '2026-05-17T10:02:30Z', '2026-05-17T10:03:00Z', 2,
            '2026-05-17T10:03:00Z', 'fp-timeline'
        )
        """
    )
    await conn.execute(
        """
        INSERT INTO issue_cost_marks (issue_id, warning_posted_at)
        VALUES ('iss-timeline', '2026-05-17T10:04:00Z')
        """
    )
    await conn.execute(
        """
        INSERT INTO external_observations (
            issue_id, source, observed_at, payload_json, drift_kind, action_taken
        )
        VALUES
            (
                'iss-timeline', 'github', '2026-05-17T10:04:20Z',
                '{}', 'merge_zombie', 'would_clear'
            ),
            (
                'iss-timeline', 'github', '2026-05-17T10:05:45Z',
                '{}', 'pr_locally_merged', 'cleared'
            )
        """
    )
    await conn.execute(
        """
        INSERT INTO state_transitions (
            issue_id, table_name, field, old_value, new_value, ts
        )
        VALUES
            (
                'iss-timeline', 'review_state', 'iteration', '1', '2',
                '2026-05-17T10:03:30Z'
            ),
            (
                'iss-timeline', 'operator_waits', 'kind', NULL, 'review_stopped',
                '2026-05-17T10:04:30Z'
            ),
            (
                'iss-timeline', 'external_observations', 'external_state_change',
                'linear', 'linear:Done', '2026-05-17T10:04:40Z'
            ),
            (
                'iss-timeline', 'operator_waits', 'kind', 'review_stopped', 'merge',
                '2026-05-17T10:04:45Z'
            ),
            (
                'iss-timeline', 'operator_waits', 'kind', 'merge', NULL,
                '2026-05-17T10:05:30Z'
            )
        """
    )
    await conn.commit()


@pytest.mark.asyncio
async def test_ui_mount_serves_index_and_spa_fallback(tmp_path: Path) -> None:
    app = create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=True,
        ui_dist_dir=_dist(tmp_path),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        root = await client.get("/ui/")
        nested = await client.get("/ui/some/nested/path")

    assert root.status_code == 200
    assert nested.status_code == 200
    assert "symphony UI v0" in root.text
    assert nested.text == root.text


@pytest.mark.asyncio
async def test_ui_disabled_skips_ui_and_api_mounts(tmp_path: Path) -> None:
    app = create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=False,
        ui_dist_dir=_dist(tmp_path),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        ui = await client.get("/ui/")
        api = await client.get("/api/anything")

    assert ui.status_code == 404
    assert api.status_code == 404
    assert "/api/{path:path}" not in {route.path for route in app.routes}


@pytest.mark.asyncio
async def test_api_issues_returns_seeded_issues_sorted(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="issue-known",
            identifier="ADJ-2",
            title="Known tracked issue",
            team_key="ADJ",
        )
        await db.issues.upsert(
            conn,
            id="issue-first",
            identifier="ADJ-1",
            title="Earlier issue",
            team_key="ADJ",
        )
        await db.issues.upsert(
            conn,
            id="issue-ten",
            identifier="ADJ-10",
            title="Later issue",
            team_key="ADJ",
        )
        await db.issues.upsert(
            conn,
            id="issue-web",
            identifier="WEB-1",
            title="Other team issue",
            team_key="WEB",
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_dist_dir=_dist(tmp_path),
            ui_db_path=db_path,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues?scope=all")
    finally:
        await conn.close()

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "issue-first",
            "identifier": "ADJ-1",
            "title": "Earlier issue",
            "team_key": "ADJ",
            **_token_totals(),
            "latest_activity_ts": None,
            "latest_activity_age_secs": None,
            "canonical_status": {
                "state": "idle",
                "since": None,
                "subtitle": None,
                "stuck_for": None,
            },
        },
        {
            "id": "issue-known",
            "identifier": "ADJ-2",
            "title": "Known tracked issue",
            "team_key": "ADJ",
            **_token_totals(),
            "latest_activity_ts": None,
            "latest_activity_age_secs": None,
            "canonical_status": {
                "state": "idle",
                "since": None,
                "subtitle": None,
                "stuck_for": None,
            },
        },
        {
            "id": "issue-ten",
            "identifier": "ADJ-10",
            "title": "Later issue",
            "team_key": "ADJ",
            **_token_totals(),
            "latest_activity_ts": None,
            "latest_activity_age_secs": None,
            "canonical_status": {
                "state": "idle",
                "since": None,
                "subtitle": None,
                "stuck_for": None,
            },
        },
        {
            "id": "issue-web",
            "identifier": "WEB-1",
            "title": "Other team issue",
            "team_key": "WEB",
            **_token_totals(),
            "latest_activity_ts": None,
            "latest_activity_age_secs": None,
            "canonical_status": {
                "state": "idle",
                "since": None,
                "subtitle": None,
                "stuck_for": None,
            },
        },
    ]


@pytest.mark.asyncio
async def test_api_namespace_keeps_placeholder_404(tmp_path: Path) -> None:
    app = create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=True,
        ui_dist_dir=_dist(tmp_path),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/api/anything")

    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}
    assert "/api/{path:path}" in {route.path for route in app.routes}


@pytest.mark.asyncio
async def test_api_issues_all_scope_returns_canonical_statuses_sorted(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="idle",
            identifier="ENG-4",
            title="Idle issue",
            team_key="ENG",
        )
        await db.issues.upsert(
            conn,
            id="running",
            identifier="ENG-2",
            title="Running issue",
            team_key="ENG",
        )
        await db.issues.upsert(
            conn,
            id="awaiting-review",
            identifier="ENG-3",
            title="Awaiting review issue",
            team_key="ENG",
        )
        await db.issues.upsert(
            conn,
            id="stuck-pr",
            identifier="ENG-1",
            title="Stuck PR issue",
            team_key="ENG",
        )
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
            VALUES
                ('run-running', 'running', 'implement', 'running', NULL,
                 '2026-05-17T11:45:00Z', NULL, 0),
                ('run-review', 'awaiting-review', 'review', 'completed', NULL,
                 '2026-05-17T11:45:00Z', '2026-05-17T11:55:00Z', 0)
            """
        )
        await conn.execute(
            """
            INSERT INTO review_state (issue_id, iteration)
            VALUES ('awaiting-review', 1)
            """
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            )
            VALUES (
                'stuck-pr', 'org/repo', 'ENG|org/repo', 44,
                'https://github.com/org/repo/pull/44', '2026-05-16T11:00:00Z', NULL
            )
            """
        )
        await conn.commit()
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues?scope=all")
    finally:
        await conn.close()

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "stuck-pr",
            "identifier": "ENG-1",
            "title": "Stuck PR issue",
            "team_key": "ENG",
            **_token_totals(),
            "latest_activity_ts": "2026-05-16T11:00:00Z",
            "latest_activity_age_secs": 90000,
            "canonical_status": {
                "state": "pr_open",
                "since": "2026-05-16T11:00:00Z",
                "subtitle": "#44",
                "stuck_for": 90000,
            },
            "warnings": ["no_progress"],
        },
        {
            "id": "running",
            "identifier": "ENG-2",
            "title": "Running issue",
            "team_key": "ENG",
            **_token_totals(),
            "latest_activity_ts": "2026-05-17T11:45:00Z",
            "latest_activity_age_secs": 900,
            "canonical_status": {
                "state": "running",
                "since": "2026-05-17T11:45:00Z",
                "subtitle": "implement",
                "stuck_for": None,
            },
        },
        {
            "id": "awaiting-review",
            "identifier": "ENG-3",
            "title": "Awaiting review issue",
            "team_key": "ENG",
            **_token_totals(),
            "latest_activity_ts": "2026-05-17T11:55:00Z",
            "latest_activity_age_secs": 300,
            "canonical_status": {
                "state": "awaiting_review_trigger",
                "since": "2026-05-17T11:55:00Z",
                "subtitle": "iteration=1",
                "stuck_for": None,
            },
        },
        {
            "id": "idle",
            "identifier": "ENG-4",
            "title": "Idle issue",
            "team_key": "ENG",
            **_token_totals(),
            "latest_activity_ts": None,
            "latest_activity_age_secs": None,
            "canonical_status": {
                "state": "idle",
                "since": None,
                "subtitle": None,
                "stuck_for": None,
            },
        },
    ]


@pytest.mark.asyncio
async def test_api_issues_returns_per_issue_token_aggregates(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="with-runs",
            identifier="ENG-1",
            title="Token-heavy issue",
            team_key="ENG",
        )
        await db.issues.upsert(
            conn,
            id="without-runs",
            identifier="ENG-2",
            title="No runs yet",
            team_key="ENG",
        )
        await conn.execute(
            """
            INSERT INTO runs (
                id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
                input_tokens, output_tokens, cache_write_tokens, cache_read_tokens
            )
            VALUES
                ('run-implement', 'with-runs', 'implement', 'completed', NULL,
                 '2026-05-17T11:00:00Z', '2026-05-17T11:05:00Z', 0,
                 100, 20, 30, 40),
                ('run-review', 'with-runs', 'review', 'failed', NULL,
                 '2026-05-17T11:10:00Z', '2026-05-17T11:15:00Z', 0,
                 5, 6, 7, 8),
                ('run-merge', 'with-runs', 'merge', 'running', NULL,
                 '2026-05-17T11:20:00Z', NULL, 0,
                 9, 10, 11, 12)
            """
        )
        await conn.commit()
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues?scope=all")
    finally:
        await conn.close()

    assert response.status_code == 200
    rows = {row["id"]: row for row in response.json()}
    with_runs_totals = _token_totals(
        input_tokens=114,
        output_tokens=36,
        cache_write_tokens=48,
        cache_read_tokens=60,
    )
    without_runs_totals = _token_totals()
    assert {key: rows["with-runs"][key] for key in with_runs_totals} == with_runs_totals
    assert {key: rows["without-runs"][key] for key in without_runs_totals} == without_runs_totals


@pytest.mark.asyncio
async def test_api_issues_includes_latest_activity_from_existing_timestamps(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for issue_id, identifier, title in [
            ("run-running", "ENG-1", "Running run fallback"),
            ("run-ended", "ENG-2", "Completed run"),
            ("comment-activity", "ENG-3", "Comment and activity mark"),
            ("merged-pr", "ENG-4", "Merged PR"),
            ("operator-wait", "ENG-5", "Operator wait"),
            ("idle", "ENG-6", "No activity"),
        ]:
            await db.issues.upsert(
                conn,
                id=issue_id,
                identifier=identifier,
                title=title,
                team_key="ENG",
            )
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
            VALUES
                ('run-running-1', 'run-running', 'implement', 'running', NULL,
                 '2026-05-17T11:58:00Z', NULL, 0),
                ('run-ended-1', 'run-ended', 'implement', 'completed', NULL,
                 '2026-05-17T11:00:00Z', '2026-05-17T11:20:00Z', 0),
                ('run-activity-1', 'comment-activity', 'review', 'completed', NULL,
                 '2026-05-17T11:10:00Z', '2026-05-17T11:15:00Z', 0),
                ('run-wait-1', 'operator-wait', 'review', 'completed', NULL,
                 '2026-05-17T11:00:00Z', '2026-05-17T11:05:00Z', 0)
            """
        )
        await conn.execute(
            """
            INSERT INTO state_transitions (
                issue_id, table_name, field, old_value, new_value, ts
            )
            VALUES (
                'run-ended', 'runs', 'status', 'running', 'completed',
                '2026-05-17T11:19:00Z'
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO comment_events (comment_id, issue_id, seen_at)
            VALUES (
                'comment-activity-1', 'comment-activity', '2026-05-17T11:30:00Z'
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO activity_comment_marks (
                run_id, first_unpublished_at, last_event_at, event_count_since_post,
                last_posted_at, last_fingerprint
            )
            VALUES (
                'run-activity-1', '2026-05-17T11:40:00Z',
                '2026-05-17T11:45:00Z', 2,
                '2026-05-17T11:50:00Z', 'fp'
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            )
            VALUES (
                'merged-pr', 'org/repo', 'ENG|org/repo', 45,
                'https://github.com/org/repo/pull/45',
                '2026-05-17T10:00:00Z', '2026-05-17T11:55:00Z'
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO operator_waits (
                issue_id, run_id, kind, linear_team_key, github_repo, issue_label, created_at
            )
            VALUES (
                'operator-wait', 'run-wait-1', 'review_stopped', 'ENG', 'org/repo',
                'symphony', '2026-05-17T11:52:00Z'
            )
            """
        )
        await conn.commit()
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues?scope=all")
    finally:
        await conn.close()

    assert response.status_code == 200
    rows = {row["id"]: row for row in response.json()}
    assert rows["run-running"]["latest_activity_ts"] == "2026-05-17T11:58:00Z"
    assert rows["run-running"]["latest_activity_age_secs"] == 120
    assert rows["run-ended"]["latest_activity_ts"] == "2026-05-17T11:20:00Z"
    assert rows["run-ended"]["latest_activity_age_secs"] == 2400
    assert rows["comment-activity"]["latest_activity_ts"] == "2026-05-17T11:45:00Z"
    assert rows["comment-activity"]["latest_activity_age_secs"] == 900
    assert rows["merged-pr"]["latest_activity_ts"] == "2026-05-17T11:55:00Z"
    assert rows["merged-pr"]["latest_activity_age_secs"] == 300
    assert rows["operator-wait"]["latest_activity_ts"] == "2026-05-17T11:52:00Z"
    assert rows["operator-wait"]["latest_activity_age_secs"] == 480
    assert rows["idle"]["latest_activity_ts"] is None
    assert rows["idle"]["latest_activity_age_secs"] is None


@pytest.mark.asyncio
async def test_api_issues_default_active_scope_filters_active_sources(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for issue_id, identifier, title in [
            ("active-wait", "ENG-1", "Awaiting operator"),
            ("active-running", "ENG-2", "Running issue"),
            ("active-review", "ENG-3", "Awaiting review"),
            ("active-pr", "ENG-4", "Open PR"),
            ("done", "ENG-5", "Merged PR"),
            ("idle", "ENG-6", "Idle issue"),
        ]:
            await db.issues.upsert(
                conn,
                id=issue_id,
                identifier=identifier,
                title=title,
                team_key="ENG",
            )
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
            VALUES
                ('run-wait', 'active-wait', 'review', 'completed', NULL,
                 '2026-05-17T11:40:00Z', '2026-05-17T11:45:00Z', 0),
                ('run-running', 'active-running', 'implement', 'running', NULL,
                 '2026-05-17T11:50:00Z', NULL, 0),
                ('run-review', 'active-review', 'review', 'completed', NULL,
                 '2026-05-17T11:52:00Z', '2026-05-17T11:56:00Z', 0)
            """
        )
        await conn.execute(
            """
            INSERT INTO operator_waits (
                issue_id, run_id, kind, linear_team_key, github_repo, issue_label, created_at
            )
            VALUES (
                'active-wait', 'run-wait', 'review_stopped', 'ENG', 'org/repo',
                'symphony', '2026-05-17T11:55:00Z'
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
            )
            VALUES
                ('active-pr', 'org/repo', 'ENG|org/repo', 44,
                 'https://github.com/org/repo/pull/44', '2026-05-17T11:57:00Z', NULL),
                ('done', 'org/repo', 'ENG|org/repo', 45,
                 'https://github.com/org/repo/pull/45', '2026-05-17T11:00:00Z',
                 '2026-05-17T11:30:00Z')
            """
        )
        await conn.execute(
            """
            INSERT INTO review_state (issue_id, iteration)
            VALUES
                ('active-review', 1),
                ('done', 2)
            """
        )
        await conn.commit()
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues")
    finally:
        await conn.close()

    assert response.status_code == 200
    rows = response.json()
    assert [row["id"] for row in rows] == [
        "active-wait",
        "active-running",
        "active-review",
        "active-pr",
    ]
    assert rows[0]["canonical_status"] == {
        "state": "paused",
        "since": "2026-05-17T11:55:00Z",
        "subtitle": "review_stopped",
        "stuck_for": None,
    }


@pytest.mark.asyncio
async def test_api_issues_recent_scope_unions_active_with_latest_50_events(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="active-old",
            identifier="ENG-999",
            title="Active old issue",
            team_key="ENG",
        )
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
            VALUES (
                'run-active-old', 'active-old', 'implement', 'running', NULL,
                '2026-05-14T10:00:00Z', NULL, 0
            )
            """
        )
        for idx in range(55):
            issue_id = f"recent-{idx:02d}"
            await db.issues.upsert(
                conn,
                id=issue_id,
                identifier=f"ENG-{idx}",
                title=f"Recent event issue {idx}",
                team_key="ENG",
            )
            await conn.execute(
                """
                INSERT INTO comment_events (comment_id, issue_id, seen_at)
                VALUES (?, ?, ?)
                """,
                (f"comment-recent-{idx:02d}", issue_id, f"2026-05-17T10:{idx:02d}:00Z"),
            )
        await conn.commit()
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues?scope=recent")
    finally:
        await conn.close()

    assert response.status_code == 200
    ids = {row["id"] for row in response.json()}
    assert len(ids) == 51
    assert "active-old" in ids
    assert "recent-54" in ids
    assert "recent-05" in ids
    assert "recent-04" not in ids
    assert "recent-00" not in ids


@pytest.mark.asyncio
async def test_api_issues_q_filters_identifier_and_title_case_insensitively(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="adj-12",
            identifier="ADJ-12",
            title="Implement dashboard filters",
            team_key="ADJ",
        )
        await db.issues.upsert(
            conn,
            id="web-8",
            identifier="WEB-8",
            title="Repair Payments callback",
            team_key="WEB",
        )
        await db.issues.upsert(
            conn,
            id="adj-99",
            identifier="ADJ-99",
            title="Unrelated work",
            team_key="ADJ",
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            by_identifier = await client.get("/api/issues?scope=all&q=adj-12")
            by_title = await client.get("/api/issues?scope=all&q=PAYMENTS")
    finally:
        await conn.close()

    assert by_identifier.status_code == 200
    assert [row["id"] for row in by_identifier.json()] == ["adj-12"]
    assert by_title.status_code == 200
    assert [row["id"] for row in by_title.json()] == ["web-8"]


@pytest.mark.asyncio
async def test_api_issues_uses_one_clock_value_for_all_canonical_statuses(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    clock_calls = 0

    def clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        return UI_NOW

    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="issue-a",
            identifier="ENG-1",
            title="First issue",
            team_key="ENG",
        )
        await db.issues.upsert(
            conn,
            id="issue-b",
            identifier="ENG-2",
            title="Second issue",
            team_key="ENG",
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=clock,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues?scope=all")
    finally:
        await conn.close()

    assert response.status_code == 200
    assert clock_calls == 1


@pytest.mark.asyncio
async def test_api_issues_maps_canonical_status_db_errors_to_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def raise_db_error(*_args: object, **_kwargs: object) -> object:
        raise aiosqlite.OperationalError("database is locked")

    monkeypatch.setattr(ui_api, "compute_canonical_status", raise_db_error)

    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="issue-a",
            identifier="ENG-1",
            title="First issue",
            team_key="ENG",
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues?scope=all")
    finally:
        await conn.close()

    assert response.status_code == 503
    assert response.json() == {"detail": "UI database is not available"}


@pytest.mark.asyncio
async def test_issue_detail_api_returns_nested_issue_payload(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await _seed_issue_detail(conn)
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues/iss-1")
    finally:
        await conn.close()

    assert response.status_code == 200
    assert response.json() == {
        "issue": {
            "id": "iss-1",
            "identifier": "ENG-1",
            "title": "Fix the thing",
            "team_key": "ENG",
        },
        "canonical_status": {
            "state": "paused",
            "since": "2026-05-17T10:30:00Z",
            "subtitle": "review_stopped",
            "stuck_for": 5400,
        },
        "runs": [
            {
                "id": "run-2",
                "stage": "review",
                "status": "running",
                "pid": None,
                "started_at": "2026-05-17T10:20:00Z",
                "ended_at": None,
                "cost_usd": 0.5,
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_write_tokens": 0,
                "cache_read_tokens": 0,
                "termination_kind": "",
                "termination_detail": "",
                "exit_returncode": None,
            },
            {
                "id": "run-1",
                "stage": "implement",
                "status": "completed",
                "pid": 123,
                "started_at": "2026-05-17T10:00:00Z",
                "ended_at": "2026-05-17T10:10:00Z",
                "cost_usd": 1.25,
                "input_tokens": 100,
                "output_tokens": 20,
                "cache_write_tokens": 30,
                "cache_read_tokens": 40,
                "termination_kind": "",
                "termination_detail": "",
                "exit_returncode": None,
            },
        ],
        "issue_prs": [
            {
                "github_repo": "org/repo",
                "binding_key": "ENG|org/repo",
                "pr_number": 42,
                "pr_url": "https://github.com/org/repo/pull/42",
                "created_at": "2026-05-17T10:11:00Z",
                "merged_at": None,
            }
        ],
        "operator_waits": [
            {
                "run_id": "run-2",
                "kind": "review_stopped",
                "linear_team_key": "ENG",
                "github_repo": "org/repo",
                "issue_label": "symphony",
                "created_at": "2026-05-17T10:30:00Z",
            }
        ],
        "review_state": {
            "iteration": 3,
            "last_trigger_signature": "sig",
            "ci_fetch_failures": 1,
            "pr_number": 42,
            "pr_url": "https://github.com/org/repo/pull/42",
            "github_repo": "org/repo",
            "issue_label": "symphony",
            "codex_lgtm_comment_id": "comment-9",
        },
        "comment_events": [
            {
                "comment_id": f"comment-{idx:02d}",
                "seen_at": f"2026-05-17T11:{idx:02d}:00Z",
            }
            for idx in range(54, 4, -1)
        ],
        "activity_comment_marks": [
            {
                "run_id": "run-2",
                "first_unpublished_at": "2026-05-17T10:21:00Z",
                "last_event_at": "2026-05-17T10:22:00Z",
                "event_count_since_post": 7,
                "last_posted_at": "2026-05-17T10:25:00Z",
                "last_fingerprint": "fp",
            }
        ],
        "issue_cost_marks": {"warning_posted_at": "2026-05-17T10:40:00Z"},
    }


@pytest.mark.asyncio
async def test_issue_detail_api_serializes_run_termination_fields(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="iss-termination",
            identifier="ENG-9",
            title="Show failure reason",
            team_key="ENG",
        )
        await conn.execute(
            """
            INSERT INTO runs (
                id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
                termination_kind, termination_detail, exit_returncode
            )
            VALUES (
                'run-failed', 'iss-termination', 'implement', 'failed', NULL,
                '2026-05-17T10:00:00Z', '2026-05-17T10:05:00Z', 0,
                'agent_nonzero_exit', '[backfill] return code 2', 2
            )
            """
        )
        await conn.commit()
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues/iss-termination")
    finally:
        await conn.close()

    assert response.status_code == 200
    assert response.json()["runs"] == [
        {
            "id": "run-failed",
            "stage": "implement",
            "status": "failed",
            "pid": None,
            "started_at": "2026-05-17T10:00:00Z",
            "ended_at": "2026-05-17T10:05:00Z",
            "cost_usd": 0.0,
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_write_tokens": 0,
            "cache_read_tokens": 0,
            "termination_kind": "agent_nonzero_exit",
            "termination_detail": "[backfill] return code 2",
            "exit_returncode": 2,
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_name", "field", "source_value"),
    [
        ("Linear", "linear.state", "Done"),
        ("GitHub", "github.state", "MERGED"),
        ("GitHub", "github.state", "CLOSED"),
        ("GitHub", "github.merged_at", "2026-05-17T09:30:00Z"),
    ],
)
async def test_issue_detail_include_external_promotes_latest_drift(
    tmp_path: Path,
    source_name: str,
    field: str,
    source_value: str,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="iss-drift",
            identifier="VIB-16",
            title="Stale merge wait",
            team_key="VIB",
        )
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
            VALUES ('run-drift', 'iss-drift', 'merge', 'completed', NULL,
                    '2026-05-17T07:00:00Z', '2026-05-17T07:10:00Z', 0)
            """
        )
        await conn.execute(
            """
            INSERT INTO operator_waits (
                issue_id, run_id, kind, linear_team_key, github_repo, issue_label,
                created_at
            )
            VALUES ('iss-drift', 'run-drift', 'merge', 'VIB', 'org/repo',
                    'symphony', '2026-05-17T08:00:00Z')
            """
        )
        await conn.commit()
        external_payload = {
            "fetched_at": "2026-05-17T09:30:00Z",
            "linear": {"state": "Done", "comments": [], "labels": []},
            "github": {"state": "MERGED", "comments": []},
            "drift_flags": [
                {
                    "field": field,
                    "sqlite_value": None,
                    "source_value": source_value,
                    "source_name": source_name,
                    "severity": "drift",
                    "flagged_at": "2026-05-17T08:15:00Z",
                }
            ],
        }
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            ui_external_service=_FakeExternalService(external_payload),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            detail_response = await client.get(
                "/api/issues/iss-drift?include_external=1"
            )
            plain_response = await client.get("/api/issues/iss-drift")
            list_response = await client.get("/api/issues?scope=all")
    finally:
        await conn.close()

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["canonical_status"] == {
        "state": "drift_detected",
        "since": "2026-05-17T08:15:00Z",
        "subtitle": "1 field(s) disagree",
        "stuck_for": 13500,
    }
    assert detail_payload["external_snapshot"]["drift_flags"] == [
        {
            "field": field,
            "sqlite_value": None,
            "source_value": source_value,
            "source_name": source_name,
            "severity": "drift",
            "flagged_at": "2026-05-17T08:15:00Z",
        }
    ]
    assert plain_response.json()["canonical_status"]["state"] == "awaiting_merge"
    assert list_response.json()[0]["canonical_status"]["state"] == "awaiting_merge"


@pytest.mark.asyncio
async def test_issue_detail_include_external_keeps_warning_flags_out_of_status(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="iss-check-warning",
            identifier="VIB-26",
            title="Running with failing checks",
            team_key="VIB",
        )
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
            VALUES ('run-warning', 'iss-check-warning', 'review', 'running', NULL,
                    '2026-05-17T11:40:00Z', NULL, 0)
            """
        )
        await conn.commit()
        external_payload = {
            "fetched_at": "2026-05-17T11:45:00Z",
            "linear": {"state": "In Review", "comments": [], "labels": []},
            "github": {"state": "OPEN", "comments": []},
            "drift_flags": [
                {
                    "field": "github.checks",
                    "sqlite_value": "running",
                    "source_value": "1 failing",
                    "source_name": "GitHub",
                    "severity": "warning",
                    "flagged_at": "2026-05-17T11:40:00Z",
                }
            ],
        }
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            ui_external_service=_FakeExternalService(external_payload),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/api/issues/iss-check-warning?include_external=1"
            )
    finally:
        await conn.close()

    assert response.status_code == 200
    payload = response.json()
    assert payload["canonical_status"] == {
        "state": "running",
        "since": "2026-05-17T11:40:00Z",
        "subtitle": "review",
        "stuck_for": None,
    }
    assert payload["external_snapshot"]["drift_flags"][0]["severity"] == "warning"


@pytest.mark.asyncio
async def test_no_progress_warning_surfaces_on_list_and_detail(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="iss-no-progress",
            identifier="VIB-23",
            title="Open PR stalled",
            team_key="VIB",
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at,
                merged_at
            )
            VALUES ('iss-no-progress', 'org/repo', 'VIB|org/repo', 23,
                    'https://github.com/org/repo/pull/23',
                    '2026-05-17T07:00:00Z', NULL)
            """
        )
        await conn.commit()
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            list_response = await client.get("/api/issues?scope=all")
            detail_response = await client.get("/api/issues/iss-no-progress")
    finally:
        await conn.close()

    assert list_response.status_code == 200
    list_payload = list_response.json()[0]
    assert list_payload["canonical_status"]["state"] == "pr_open"
    assert list_payload["latest_activity_age_secs"] == 18000
    assert list_payload["warnings"] == ["no_progress"]

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["canonical_status"]["state"] == "pr_open"
    assert detail_payload["latest_activity_age_secs"] == 18000
    assert detail_payload["warnings"] == ["no_progress"]


@pytest.mark.asyncio
async def test_zero_no_progress_threshold_is_preserved(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="iss-zero-threshold",
            identifier="VIB-25",
            title="Open PR just moved",
            team_key="VIB",
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (
                issue_id, github_repo, binding_key, pr_number, pr_url, created_at,
                merged_at
            )
            VALUES ('iss-zero-threshold', 'org/repo', 'VIB|org/repo', 25,
                    'https://github.com/org/repo/pull/25',
                    '2026-05-17T11:59:59Z', NULL)
            """
        )
        await conn.commit()
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            ui_pr_no_progress_threshold=timedelta(0),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            list_response = await client.get("/api/issues?scope=all")
            detail_response = await client.get("/api/issues/iss-zero-threshold")
    finally:
        await conn.close()

    assert list_response.status_code == 200
    assert list_response.json()[0]["latest_activity_age_secs"] == 1
    assert list_response.json()[0]["warnings"] == ["no_progress"]
    assert detail_response.status_code == 200
    assert detail_response.json()["latest_activity_age_secs"] == 1
    assert detail_response.json()["warnings"] == ["no_progress"]


@pytest.mark.asyncio
async def test_issue_detail_api_404s_for_unknown_issue(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues/missing")
    finally:
        await conn.close()

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_issue_observations_api_returns_last_twenty_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="iss-observed",
            identifier="ENG-9",
            title="Observed issue",
            team_key="ENG",
        )
        for idx in range(25):
            await db.external_observations.insert(
                conn,
                issue_id="iss-observed",
                source="github" if idx % 2 else "linear",
                observed_at=f"2026-05-17T10:{idx:02d}:00Z",
                payload_json=f'{{"idx":{idx}}}',
                drift_kind="merge_zombie" if idx == 24 else None,
                action_taken="would_clear" if idx == 24 else "observed",
            )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues/iss-observed/observations")
    finally:
        await conn.close()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 20
    assert payload[0] == {
        "id": 25,
        "issue_id": "iss-observed",
        "source": "linear",
        "observed_at": "2026-05-17T10:24:00Z",
        "payload_json": '{"idx":24}',
        "drift_kind": "merge_zombie",
        "action_taken": "would_clear",
    }
    assert payload[-1]["payload_json"] == '{"idx":5}'


@pytest.mark.asyncio
async def test_issue_observations_api_404s_for_unknown_issue(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues/missing/observations")
    finally:
        await conn.close()

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_issue_timeline_api_returns_merged_sorted_events(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await _seed_issue_timeline(conn)
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues/iss-timeline/timeline")
    finally:
        await conn.close()

    assert response.status_code == 200
    assert response.json() == [
        {
            "ts": "2026-05-17T10:00:00Z",
            "kind": "run_started",
            "payload": {"run_id": "run-timeline", "stage": "implement", "pid": 456},
        },
        {
            "ts": "2026-05-17T10:01:00Z",
            "kind": "comment_seen",
            "payload": {"comment_id": "comment-timeline"},
        },
        {
            "ts": "2026-05-17T10:02:00Z",
            "kind": "pr_opened",
            "payload": {
                "github_repo": "org/repo",
                "pr_number": 43,
                "pr_url": "https://github.com/org/repo/pull/43",
            },
        },
        {
            "ts": "2026-05-17T10:03:00Z",
            "kind": "activity_comment_posted",
            "payload": {"run_id": "run-timeline", "fingerprint": "fp-timeline"},
        },
        {
            "ts": "2026-05-17T10:03:30Z",
            "kind": "review_state_changed",
            "payload": {"field": "iteration", "old": "1", "new": "2"},
        },
        {
            "ts": "2026-05-17T10:04:00Z",
            "kind": "cost_warning_posted",
            "payload": {},
        },
        {
            "ts": "2026-05-17T10:04:20Z",
            "kind": "external_observed",
            "payload": {"source": "github", "drift_kind": "merge_zombie"},
        },
        {
            "ts": "2026-05-17T10:04:30Z",
            "kind": "operator_wait_started",
            "payload": {"kind": "review_stopped"},
        },
        {
            "ts": "2026-05-17T10:04:40Z",
            "kind": "external_state_change",
            "payload": {
                "source": "linear",
                "field": "external_state_change",
                "new_value": "linear:Done",
            },
        },
        {
            "ts": "2026-05-17T10:04:45Z",
            "kind": "operator_wait_ended",
            "payload": {"kind": "review_stopped"},
        },
        {
            "ts": "2026-05-17T10:04:45Z",
            "kind": "operator_wait_started",
            "payload": {"kind": "merge"},
        },
        {
            "ts": "2026-05-17T10:05:00Z",
            "kind": "run_ended",
            "payload": {
                "run_id": "run-timeline",
                "stage": "implement",
                "status": "completed",
                "cost_usd": 2.5,
            },
        },
        {
            "ts": "2026-05-17T10:05:30Z",
            "kind": "operator_wait_ended",
            "payload": {"kind": "merge"},
        },
        {
            "ts": "2026-05-17T10:05:45Z",
            "kind": "external_cleared",
            "payload": {
                "source": "github",
                "drift_kind": "pr_locally_merged",
                "fields_changed": ["issue_prs.merged_at"],
            },
        },
        {
            "ts": "2026-05-17T10:06:00Z",
            "kind": "pr_merged",
            "payload": {"github_repo": "org/repo", "pr_number": 43},
        },
    ]


@pytest.mark.asyncio
async def test_issue_timeline_api_404s_for_unknown_issue(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues/missing/timeline")
    finally:
        await conn.close()

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_shared_app_preserves_linear_webhook_route(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            WebhookSettings(secret=SECRET),
            ui_enabled=True,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: NOW,
        )
        body = _body(_payload())

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/linear/webhook",
                content=body,
                headers=_headers(body),
            )
    finally:
        await conn.close()

    assert response.status_code == 200
    assert response.json()["handled"] is True
    assert len(handler.payloads) == 1
