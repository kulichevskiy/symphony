from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite
import httpx
import pytest

from symphony import db
from symphony.agent.model_usage import ModelUsage
from symphony.app import create_app
from symphony.config import Config, RepoBinding
from symphony.linear.slash import SlashKind
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
    (dist / "index.html").write_text("<!doctype html><html><body>symphony UI v0</body></html>")
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
async def test_ui_cache_control_headers(tmp_path: Path) -> None:
    dist = _dist(tmp_path)
    assets = dist / "assets"
    assets.mkdir()
    (assets / "index-abc123.js").write_text("console.log(1)")
    (assets / "index-def456.css").write_text("body{}")

    app = create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=True,
        ui_dist_dir=dist,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        js = await client.get("/ui/assets/index-abc123.js")
        css = await client.get("/ui/assets/index-def456.css")
        root = await client.get("/ui/")
        fallback = await client.get("/ui/some/nested/path")

    immutable = "public, max-age=31536000, immutable"
    assert js.headers["cache-control"] == immutable
    assert css.headers["cache-control"] == immutable
    assert root.headers["cache-control"] == "no-cache"
    assert fallback.headers["cache-control"] == "no-cache"

    # index.html still revalidates via ETag.
    assert "etag" in root.headers
    revalidated = None
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        revalidated = await client.get(
            "/ui/", headers={"if-none-match": root.headers["etag"]}
        )
    assert revalidated.status_code == 304
    assert revalidated.headers["cache-control"] == "no-cache"


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
async def test_api_meta_returns_webhook_url_when_public_url_set(tmp_path: Path) -> None:
    app = create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=True,
        ui_dist_dir=_dist(tmp_path),
        ui_webhook_public_url="https://abc-123.trycloudflare.com/",
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/api/meta")

    assert response.status_code == 200
    # Trailing slash on the public origin is stripped before the path is joined.
    assert response.json() == {
        "tunnel_url": "https://abc-123.trycloudflare.com",
        "linear_webhook_url": "https://abc-123.trycloudflare.com/linear/webhook",
    }


@pytest.mark.asyncio
async def test_api_meta_empty_without_public_url(tmp_path: Path) -> None:
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
        response = await client.get("/api/meta")

    assert response.status_code == 200
    # Defaults excluded → no tunnel advertised in normal/prod runs.
    assert response.json() == {}


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
        # Make each issue active (a running run) so it lists under `active`;
        # this test only asserts the identifier sort within a single status.
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r-first', 'issue-first', 'implement', 'running', NULL,
                 '2026-05-17T10:00:00Z'),
                ('r-known', 'issue-known', 'implement', 'running', NULL,
                 '2026-05-17T10:00:00Z'),
                ('r-ten', 'issue-ten', 'implement', 'running', NULL,
                 '2026-05-17T10:00:00Z'),
                ('r-web', 'issue-web', 'implement', 'running', NULL,
                 '2026-05-17T10:00:00Z')
            """
        )
        await conn.commit()
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
            response = await client.get("/api/issues?scope=active")
    finally:
        await conn.close()

    assert response.status_code == 200
    assert [issue["identifier"] for issue in response.json()] == [
        "ADJ-1",
        "ADJ-2",
        "ADJ-10",
        "WEB-1",
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
async def test_api_issues_active_scope_returns_canonical_statuses_sorted(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
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
            response = await client.get("/api/issues?scope=active")
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
        # `without-runs` has no runs; a review_state row makes it active (so it
        # lists under `active`) without contributing any tokens.
        await conn.execute(
            "INSERT INTO review_state (issue_id, iteration) VALUES ('without-runs', 1)"
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
            response = await client.get("/api/issues?scope=active")
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
async def test_api_issues_filters_by_provider(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn, id="spans", identifier="ENG-1", title="spans both", team_key="ENG"
        )
        await db.issues.upsert(
            conn,
            id="codex-only",
            identifier="ENG-2",
            title="codex only",
            team_key="ENG",
        )
        # Run-level token columns mirror the sum of each run's run_model_usage
        # rows (the schema invariant), so provider=all reflects the run totals.
        await conn.execute(
            """
            INSERT INTO runs (
                id, issue_id, stage, status, pid, started_at, ended_at,
                input_tokens, output_tokens, cache_write_tokens, cache_read_tokens
            )
            VALUES
                ('r1', 'spans', 'implement', 'completed', NULL,
                 '2026-05-17T10:00:00Z', '2026-05-17T10:05:00Z', 100, 20, 30, 40),
                ('r2', 'spans', 'review', 'completed', NULL,
                 '2026-05-17T10:10:00Z', '2026-05-17T10:15:00Z', 10, 2, 0, 3),
                ('r3', 'codex-only', 'implement', 'completed', NULL,
                 '2026-05-17T11:00:00Z', '2026-05-17T11:05:00Z', 50, 5, 5, 5)
            """
        )
        # Both issues are active via review_state (iteration > 0, no PRs).
        await conn.execute(
            "INSERT INTO review_state (issue_id, iteration) VALUES ('spans', 1), ('codex-only', 1)"
        )
        await conn.commit()
        # spans: implement on claude, review on codex. codex-only: codex only.
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("codex", "gpt-5.5", 10, 2, 0, 3)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("codex", "gpt-5.5", 50, 5, 5, 5)]
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
            claude_resp = await client.get("/api/issues?scope=active&provider=claude")
            codex_resp = await client.get("/api/issues?scope=active&provider=codex")
            all_resp = await client.get("/api/issues?scope=active&provider=all")
    finally:
        await conn.close()

    # claude: codex-only issue is dropped (zero claude usage); spans is scoped to
    # its claude tokens only.
    assert claude_resp.status_code == 200
    claude_rows = {r["id"]: r for r in claude_resp.json()}
    assert set(claude_rows) == {"spans"}
    assert {k: claude_rows["spans"][k] for k in _token_totals()} == _token_totals(
        input_tokens=100, output_tokens=20, cache_write_tokens=30, cache_read_tokens=40
    )

    # codex: both issues present, each scoped to its codex tokens.
    assert codex_resp.status_code == 200
    codex_rows = {r["id"]: r for r in codex_resp.json()}
    assert set(codex_rows) == {"spans", "codex-only"}
    assert {k: codex_rows["spans"][k] for k in _token_totals()} == _token_totals(
        input_tokens=10, output_tokens=2, cache_write_tokens=0, cache_read_tokens=3
    )
    assert {k: codex_rows["codex-only"][k] for k in _token_totals()} == _token_totals(
        input_tokens=50, output_tokens=5, cache_write_tokens=5, cache_read_tokens=5
    )

    # provider=all: unchanged behavior — both issues, full per-issue sums.
    assert all_resp.status_code == 200
    all_rows = {r["id"]: r for r in all_resp.json()}
    assert set(all_rows) == {"spans", "codex-only"}
    assert {k: all_rows["spans"][k] for k in _token_totals()} == _token_totals(
        input_tokens=110, output_tokens=22, cache_write_tokens=30, cache_read_tokens=43
    )


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
        # Active markers so every issue lists under `active`. review_state is a
        # neutral marker (no activity timestamp) for the PR-less issues;
        # merged-pr already has a PR, so a stale running run keeps it active
        # without out-ranking its merge timestamp.
        await conn.execute(
            """
            INSERT INTO review_state (issue_id, iteration) VALUES
                ('run-ended', 1), ('comment-activity', 1)
            """
        )
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES ('run-merged-keepalive', 'merged-pr', 'merge', 'running', NULL,
                    '2020-01-01T00:00:00Z')
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
            response = await client.get("/api/issues?scope=active")
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
        # review_state makes each active (so they list under `active`) without
        # adding tokens or activity.
        await conn.execute(
            """
            INSERT INTO review_state (issue_id, iteration) VALUES
                ('adj-12', 1), ('web-8', 1), ('adj-99', 1)
            """
        )
        await conn.commit()
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
            by_identifier = await client.get("/api/issues?scope=active&q=adj-12")
            by_title = await client.get("/api/issues?scope=active&q=PAYMENTS")
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
        await conn.execute(
            "INSERT INTO review_state (issue_id, iteration) VALUES ('issue-a', 1), ('issue-b', 1)"
        )
        await conn.commit()
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
            response = await client.get("/api/issues?scope=active")
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

    monkeypatch.setattr(ui_api, "compute_canonical_statuses", raise_db_error)

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
        await conn.execute("INSERT INTO review_state (issue_id, iteration) VALUES ('issue-a', 1)")
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
            response = await client.get("/api/issues?scope=active")
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
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_write_tokens": 0,
                "cache_read_tokens": 0,
                "termination_kind": "",
                "termination_detail": "",
                "exit_returncode": None,
                "has_log": False,
            },
            {
                "id": "run-1",
                "stage": "implement",
                "status": "completed",
                "pid": 123,
                "started_at": "2026-05-17T10:00:00Z",
                "ended_at": "2026-05-17T10:10:00Z",
                "input_tokens": 100,
                "output_tokens": 20,
                "cache_write_tokens": 30,
                "cache_read_tokens": 40,
                "termination_kind": "",
                "termination_detail": "",
                "exit_returncode": None,
                "has_log": False,
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
        "tokens_by_model": [],
    }


@pytest.mark.asyncio
async def test_issue_detail_api_returns_tokens_by_provider_model(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await _seed_issue_detail(conn)
        # Implement on claude, review on codex — the issue spans two providers.
        await db.run_model_usage.replace_for_run(
            conn, "run-1", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "run-2", [ModelUsage("codex", "gpt-5.5", 10, 2, 0, 3)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            response = await client.get("/api/issues/iss-1")
    finally:
        await conn.close()

    assert response.status_code == 200
    breakdown = response.json()["tokens_by_model"]
    # Sorted by output desc: claude (20) before codex (2).
    assert breakdown == [
        {
            "provider": "claude",
            "model": "claude-opus-4-8",
            "input_tokens": 100,
            "output_tokens": 20,
            "cache_write_tokens": 30,
            "cache_read_tokens": 40,
        },
        {
            "provider": "codex",
            "model": "gpt-5.5",
            "input_tokens": 10,
            "output_tokens": 2,
            "cache_write_tokens": 0,
            "cache_read_tokens": 3,
        },
    ]


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
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_write_tokens": 0,
            "cache_read_tokens": 0,
            "termination_kind": "agent_nonzero_exit",
            "termination_detail": "[backfill] return code 2",
            "exit_returncode": 2,
            "has_log": False,
        }
    ]


@pytest.mark.asyncio
async def test_issue_detail_api_reports_has_log_per_run(tmp_path: Path) -> None:
    """`has_log` is true iff `{log_root}/{run_id}.log` exists — the final-log
    viewer keys purely off it (drained when true, empty state when false)."""
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    log_root.mkdir()
    (log_root / "run-with-log.log").write_text("hi\n", encoding="utf-8")
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id="iss-log",
            identifier="ENG-10",
            title="Log presence",
            team_key="ENG",
        )
        for run_id, started in (
            ("run-with-log", "2026-05-17T10:00:00Z"),
            ("run-without-log", "2026-05-17T09:00:00Z"),
        ):
            await conn.execute(
                """
                INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
                VALUES (?, 'iss-log', 'local_review', 'failed', NULL, ?, ?, 0)
                """,
                (run_id, started, started),
            )
        await conn.commit()
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_log_root=log_root,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues/iss-log")
    finally:
        await conn.close()

    assert response.status_code == 200
    has_log = {r["id"]: r["has_log"] for r in response.json()["runs"]}
    assert has_log == {"run-with-log": True, "run-without-log": False}


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
            detail_response = await client.get("/api/issues/iss-drift?include_external=1")
            plain_response = await client.get("/api/issues/iss-drift")
            list_response = await client.get("/api/issues?scope=active")
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
            response = await client.get("/api/issues/iss-check-warning?include_external=1")
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
            list_response = await client.get("/api/issues?scope=active")
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
            list_response = await client.get("/api/issues?scope=active")
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


async def _client(app: Any):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


@pytest.mark.asyncio
async def test_api_spend_summary_aggregates_per_team_sorted(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (
            ("a", "ENG-1", "ENG"),
            ("b", "ENG-2", "ENG"),
            ("c", "WEB-1", "WEB"),
        ):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at, cost_usd,
                input_tokens, output_tokens, cache_write_tokens, cache_read_tokens)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z',
                 1.25, 100, 20, 30, 40),
                ('r2', 'a', 'review', 'completed', NULL, '2026-05-17T10:20:00Z',
                 0.50, 10, 2, 3, 4),
                ('r3', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z',
                 2.00, 50, 5, 5, 5),
                ('r4', 'c', 'implement', 'completed', NULL, '2026-05-17T12:00:00Z',
                 5.00, 200, 40, 60, 80)
            """
        )
        # per_team/totals now source tokens from run_model_usage; mirror each
        # run's tokens with a single-provider usage row.
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("claude", "claude-opus-4-8", 10, 2, 3, 4)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("claude", "claude-opus-4-8", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r4", [ModelUsage("claude", "claude-opus-4-8", 200, 40, 60, 80)]
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
        async with await _client(app) as client:
            response = await client.get("/api/spend/summary")
    finally:
        await conn.close()

    assert response.status_code == 200
    body = response.json()
    assert body["totals"] == {
        "input_tokens": 360,
        "output_tokens": 67,
        "cache_write_tokens": 98,
        "cache_read_tokens": 129,
        "issues": 3,
    }
    assert "total_tokens" not in body["totals"]
    # Sorted by output tokens desc: WEB (40) before ENG (27).
    assert [t["key"] for t in body["per_team"]] == ["WEB", "ENG"]
    eng = next(t for t in body["per_team"] if t["key"] == "ENG")
    assert eng["output_tokens"] == 20 + 2 + 5
    assert eng["issues"] == 2
    assert "total_tokens" not in eng
    assert "cost_usd" not in body["totals"]
    assert "cost_usd" not in eng
    # Single provider seeded → its breakdown reconciles with the rail totals.
    assert [p["provider"] for p in body["per_provider"]] == ["claude"]
    assert body["per_provider"][0]["output_tokens"] == 67


async def test_api_spend_summary_per_provider_nested_per_model(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (("a", "ENG-1", "ENG"), ("b", "ENG-2", "ENG")):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z'),
                ('r2', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z'),
                ('r3', 'a', 'review', 'completed', NULL, '2026-05-17T12:00:00Z')
            """
        )
        await conn.commit()
        # claude on two issues (one model), codex on one issue (one model).
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("claude", "claude-opus-4-8", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("codex", "gpt-5.5", 10, 2, 0, 3)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            response = await client.get("/api/spend/summary")
    finally:
        await conn.close()

    assert response.status_code == 200
    body = response.json()
    providers = {p["provider"]: p for p in body["per_provider"]}
    assert set(providers) == {"claude", "codex"}

    # claude sums its two runs and counts both issues; sorted first by output.
    assert [p["provider"] for p in body["per_provider"]] == ["claude", "codex"]
    claude = providers["claude"]
    assert claude["input_tokens"] == 150
    assert claude["output_tokens"] == 25
    assert claude["cache_write_tokens"] == 35
    assert claude["cache_read_tokens"] == 45
    assert "total_tokens" not in claude
    assert claude["issues"] == 2
    assert len(claude["per_model"]) == 1
    assert claude["per_model"][0]["model"] == "claude-opus-4-8"
    assert "total_tokens" not in claude["per_model"][0]
    assert claude["per_model"][0]["output_tokens"] == 25
    assert claude["per_model"][0]["issues"] == 2

    codex = providers["codex"]
    assert codex["output_tokens"] == 2
    assert codex["issues"] == 1
    assert codex["per_model"][0]["model"] == "gpt-5.5"


@pytest.mark.asyncio
async def test_api_spend_summary_scoped_by_provider_reconciles(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (
            ("a", "ENG-1", "ENG"),
            ("b", "ENG-2", "ENG"),
            ("c", "WEB-1", "WEB"),
        ):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z'),
                ('r2', 'a', 'review', 'completed', NULL, '2026-05-17T11:00:00Z'),
                ('r3', 'b', 'implement', 'completed', NULL, '2026-05-17T12:00:00Z'),
                ('r4', 'c', 'implement', 'completed', NULL, '2026-05-17T13:00:00Z')
            """
        )
        # Issue a (ENG) is touched by both providers; b (ENG) by codex only;
        # c (WEB) by both. Tokens per (provider, model) sum back to the rail.
        await db.run_model_usage.replace_for_run(
            conn,
            "r1",
            [
                ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40),
                ModelUsage("codex", "gpt-5.5", 10, 2, 0, 3),
            ],
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("claude", "claude-opus-4-8", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("codex", "gpt-5.5", 200, 40, 60, 80)]
        )
        await db.run_model_usage.replace_for_run(
            conn,
            "r4",
            [
                ModelUsage("claude", "claude-opus-4-8", 70, 7, 7, 7),
                ModelUsage("codex", "gpt-5.5", 30, 3, 3, 3),
            ],
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
        async with await _client(app) as client:
            all_resp = await client.get("/api/spend/summary?provider=all")
            claude_resp = await client.get("/api/spend/summary?provider=claude")
            codex_resp = await client.get("/api/spend/summary?provider=codex")
            default_resp = await client.get("/api/spend/summary")
    finally:
        await conn.close()

    assert all_resp.status_code == 200
    assert claude_resp.status_code == 200
    assert codex_resp.status_code == 200
    all_body, claude_body, codex_body = (
        all_resp.json(),
        claude_resp.json(),
        codex_resp.json(),
    )

    token_keys = (
        "input_tokens",
        "output_tokens",
        "cache_write_tokens",
        "cache_read_tokens",
    )

    # Tokens reconcile exactly: all == codex + claude, for totals and per team.
    for key in token_keys:
        assert all_body["totals"][key] == (claude_body["totals"][key] + codex_body["totals"][key])

    def teams(body: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {t["key"]: t for t in body["per_team"]}

    all_teams, claude_teams, codex_teams = (
        teams(all_body),
        teams(claude_body),
        teams(codex_body),
    )
    assert set(all_teams) == {"ENG", "WEB"}
    for team in ("ENG", "WEB"):
        for key in token_keys:
            assert all_teams[team][key] == (claude_teams[team][key] + codex_teams[team][key])

    # Spot-check the scoped values.
    assert all_teams["ENG"]["output_tokens"] == 67
    assert claude_teams["ENG"]["output_tokens"] == 25
    assert codex_teams["ENG"]["output_tokens"] == 42

    # Issue counts are DISTINCT issues touched by the selected provider, so they
    # do NOT sum across providers (issue a is touched by both).
    assert all_teams["ENG"]["issues"] == 2  # {a, b}
    assert claude_teams["ENG"]["issues"] == 1  # {a}
    assert codex_teams["ENG"]["issues"] == 2  # {a, b}
    assert all_body["totals"]["issues"] == 3  # {a, b, c}
    assert claude_body["totals"]["issues"] == 2  # {a, c}
    assert codex_body["totals"]["issues"] == 3  # {a, b, c}

    # No provider param behaves like provider=all.
    assert default_resp.json()["totals"] == all_body["totals"]


@pytest.mark.asyncio
async def test_api_spend_summary_per_stage_reconciles_and_scopes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (
            ("a", "ENG-1", "ENG"),
            ("b", "ENG-2", "ENG"),
            ("c", "WEB-1", "WEB"),
        ):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z'),
                ('r2', 'a', 'review', 'completed', NULL, '2026-05-17T10:20:00Z'),
                ('r3', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z'),
                ('r4', 'c', 'merge', 'completed', NULL, '2026-05-17T12:00:00Z')
            """
        )
        # The review run spent input/cache but zero output → 'review' must still
        # appear as a 0-output row. implement spans two providers.
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("claude", "claude-opus-4-8", 10, 0, 5, 4)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("codex", "gpt-5.5", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r4", [ModelUsage("claude", "claude-opus-4-8", 200, 40, 60, 80)]
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
        async with await _client(app) as client:
            resp = await client.get("/api/spend/summary")
            claude_resp = await client.get("/api/spend/summary?provider=claude")
    finally:
        await conn.close()

    assert resp.status_code == 200
    body = resp.json()
    stages = {s["key"]: s for s in body["per_stage"]}
    # One row per distinct runs.stage in the window — no whitelist.
    assert set(stages) == {"implement", "review", "merge"}
    # review present at zero output rather than hidden.
    assert stages["review"]["output_tokens"] == 0
    assert stages["review"]["input_tokens"] == 10
    assert stages["review"]["issues"] == 1
    # implement sums both providers; all four categories + issue count.
    impl = stages["implement"]
    assert impl["input_tokens"] == 150
    assert impl["output_tokens"] == 25
    assert impl["cache_write_tokens"] == 35
    assert impl["cache_read_tokens"] == 45
    assert impl["issues"] == 2  # {a, b}
    assert stages["merge"]["output_tokens"] == 40

    token_keys = (
        "input_tokens",
        "output_tokens",
        "cache_write_tokens",
        "cache_read_tokens",
    )

    # per_stage reconciles to the same grand total as per_team / per_provider.
    def col_sum(group: str, key: str) -> int:
        return sum(int(r[key]) for r in body[group])

    for key in token_keys:
        assert col_sum("per_stage", key) == col_sum("per_team", key)
        assert col_sum("per_stage", key) == col_sum("per_provider", key)
    assert col_sum("per_stage", "output_tokens") == 65

    # Provider filter scopes per_stage exactly like the other groupings.
    cbody = claude_resp.json()
    cstages = {s["key"]: s for s in cbody["per_stage"]}
    assert cstages["implement"]["output_tokens"] == 20  # claude's r1 only
    assert cstages["review"]["output_tokens"] == 0
    for key in token_keys:
        cstage = sum(int(s[key]) for s in cbody["per_stage"])
        cteam = sum(int(t[key]) for t in cbody["per_team"])
        assert cstage == cteam


def test_series_granularity_daily_for_short_window_weekly_beyond() -> None:
    # <= ~6 weeks (42-day span) buckets daily; one day more flips to weekly.
    assert ui_api._series_granularity("2026-05-01", "2026-05-01") == "day"
    assert ui_api._series_granularity("2026-04-01", "2026-05-13") == "day"  # 42d span
    assert ui_api._series_granularity("2026-04-01", "2026-05-14") == "week"  # 43d span


@pytest.mark.asyncio
async def test_api_spend_stage_series_daily_buckets_short_window(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="a", identifier="ENG-1", title="t", team_key="ENG")
        await db.issues.upsert(conn, id="b", identifier="ENG-2", title="t", team_key="ENG")
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-15T10:00:00Z'),
                ('r2', 'a', 'review', 'completed', NULL, '2026-05-15T10:20:00Z'),
                ('r3', 'b', 'merge', 'completed', NULL, '2026-05-16T11:00:00Z')
            """
        )
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 100, 10, 0, 0)]
        )
        # review spends input but zero output — still a present stage.
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("claude", "claude-opus-4-8", 5, 0, 0, 0)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("codex", "gpt-5.5", 0, 40, 0, 0)]
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
        async with await _client(app) as client:
            resp = await client.get("/api/spend/stage-series?from=2026-05-10&to=2026-05-17")
    finally:
        await conn.close()

    assert resp.status_code == 200
    body = resp.json()
    assert body["bucket"] == "day"
    assert body["start"] == "2026-05-10"
    assert body["end"] == "2026-05-17"
    # zero-output review stage is still listed, like per_stage.
    assert set(body["stages"]) == {"implement", "review", "merge"}
    # Dense daily buckets across the inclusive window.
    starts = [b["start"] for b in body["buckets"]]
    assert starts == [f"2026-05-{d:02d}" for d in range(10, 18)]
    by_start = {b["start"]: b["output_tokens"] for b in body["buckets"]}
    # 05-15 carries implement output; review's 0 output is omitted from the map.
    assert by_start["2026-05-15"] == {"implement": 10}
    assert by_start["2026-05-16"] == {"merge": 40}
    assert by_start["2026-05-11"] == {}


@pytest.mark.asyncio
async def test_api_spend_stage_series_weekly_for_long_window(tmp_path: Path) -> None:
    from datetime import date as _date
    from datetime import timedelta as _td

    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="a", identifier="ENG-1", title="t", team_key="ENG")
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-03-03T10:00:00Z'),
                ('r2', 'a', 'implement', 'completed', NULL, '2026-03-05T10:00:00Z'),
                ('r3', 'a', 'merge', 'completed', NULL, '2026-04-20T10:00:00Z')
            """
        )
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 0, 10, 0, 0)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("claude", "claude-opus-4-8", 0, 7, 0, 0)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("claude", "claude-opus-4-8", 0, 40, 0, 0)]
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
        async with await _client(app) as client:
            resp = await client.get("/api/spend/stage-series?from=2026-03-01&to=2026-05-17")
    finally:
        await conn.close()

    assert resp.status_code == 200
    body = resp.json()
    assert body["bucket"] == "week"
    # Every bucket starts on a Monday.
    for b in body["buckets"]:
        assert _date.fromisoformat(b["start"]).weekday() == 0
    # r1 (03-03 Tue) and r2 (03-05 Thu) share an ISO week → aggregated.
    monday = (_date(2026, 3, 3) - _td(days=_date(2026, 3, 3).weekday())).isoformat()
    by_start = {b["start"]: b["output_tokens"] for b in body["buckets"]}
    assert by_start[monday] == {"implement": 17}
    # r3 lands in a different week's merge segment.
    merge_weeks = [s for s, m in by_start.items() if m.get("merge")]
    assert len(merge_weeks) == 1
    assert by_start[merge_weeks[0]]["merge"] == 40


@pytest.mark.asyncio
async def test_api_spend_stage_series_unfiltered_spans_history_and_scopes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="a", identifier="ENG-1", title="t", team_key="ENG")
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-01-05T10:00:00Z'),
                ('r2', 'a', 'implement', 'completed', NULL, '2026-05-15T10:00:00Z')
            """
        )
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 0, 10, 0, 0)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("codex", "gpt-5.5", 0, 40, 0, 0)]
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
        async with await _client(app) as client:
            allr = await client.get("/api/spend/stage-series")
            claude = await client.get("/api/spend/stage-series?provider=claude")
    finally:
        await conn.close()

    # Unfiltered window spans all observed history → weekly here (>42d span).
    body = allr.json()
    assert body["bucket"] == "week"
    assert body["start"] == "2026-01-05"
    assert body["end"] == "2026-05-15"
    series_out = sum(v for b in body["buckets"] for v in b["output_tokens"].values())
    assert series_out == 50  # 10 + 40
    # Provider filter scopes the series like every other grouping.
    cbody = claude.json()
    cout = sum(v for b in cbody["buckets"] for v in b["output_tokens"].values())
    assert cout == 10


@pytest.mark.asyncio
async def test_api_spend_stage_series_by_team_and_model(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="a", identifier="VIB-1", title="t", team_key="VIB")
        await db.issues.upsert(conn, id="b", identifier="ENG-1", title="t", team_key="ENG")
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-15T10:00:00Z'),
                ('r2', 'b', 'merge', 'completed', NULL, '2026-05-15T11:00:00Z')
            """
        )
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 0, 10, 0, 0)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("codex", "gpt-5.5", 0, 40, 0, 0)]
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
        async with await _client(app) as client:
            team = await client.get("/api/spend/stage-series?by=team&from=2026-05-10&to=2026-05-17")
            model = await client.get(
                "/api/spend/stage-series?by=model&from=2026-05-10&to=2026-05-17"
            )
            bad = await client.get("/api/spend/stage-series?by=bogus")
    finally:
        await conn.close()

    # by=team keys the series on the issue's team_key.
    tbody = team.json()
    assert set(tbody["stages"]) == {"VIB", "ENG"}
    t_by_start = {b["start"]: b["output_tokens"] for b in tbody["buckets"]}
    assert t_by_start["2026-05-15"] == {"VIB": 10, "ENG": 40}
    # by=model keys on provider/model (the per-model rowKey the client builds).
    mbody = model.json()
    assert set(mbody["stages"]) == {"claude/claude-opus-4-8", "codex/gpt-5.5"}
    m_by_start = {b["start"]: b["output_tokens"] for b in mbody["buckets"]}
    assert m_by_start["2026-05-15"] == {
        "claude/claude-opus-4-8": 10,
        "codex/gpt-5.5": 40,
    }
    # An unknown dimension is rejected by the route's pattern.
    assert bad.status_code == 422


@pytest.mark.asyncio
async def test_api_spend_heatmap_buckets_by_day(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="a", identifier="ENG-1", title="t", team_key="ENG")
        await db.issues.upsert(conn, id="b", identifier="ENG-2", title="t", team_key="ENG")
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at, cost_usd,
                input_tokens, output_tokens, cache_write_tokens, cache_read_tokens)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z',
                 1.0, 100, 0, 0, 0),
                ('r2', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z',
                 2.0, 50, 0, 0, 0),
                ('r3', 'a', 'review', 'completed', NULL, '2026-05-16T09:00:00Z',
                 0.5, 10, 0, 0, 0),
                ('r-old', 'a', 'implement', 'completed', NULL, '2024-01-01T00:00:00Z',
                 9.0, 9, 0, 0, 0)
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
        async with await _client(app) as client:
            response = await client.get("/api/spend/heatmap?days=60")
    finally:
        await conn.close()

    assert response.status_code == 200
    body = response.json()
    by_day = {d["date"]: d for d in body["days"]}
    # The 2024 run is outside the 60-day window.
    assert "2024-01-01" not in by_day
    assert by_day["2026-05-17"] == {
        "date": "2026-05-17",
        "input_tokens": 150,
        "output_tokens": 0,
        "cache_write_tokens": 0,
        "cache_read_tokens": 0,
        "issues": 2,
    }
    assert by_day["2026-05-16"] == {
        "date": "2026-05-16",
        "input_tokens": 10,
        "output_tokens": 0,
        "cache_write_tokens": 0,
        "cache_read_tokens": 0,
        "issues": 1,
    }
    assert body["end"] == "2026-05-17"


@pytest.mark.asyncio
async def test_api_spend_heatmap_filters_by_provider(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="a", identifier="ENG-1", title="t", team_key="ENG")
        await db.issues.upsert(conn, id="b", identifier="ENG-2", title="t", team_key="ENG")
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z'),
                ('r2', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z'),
                ('r3', 'a', 'review', 'completed', NULL, '2026-05-16T09:00:00Z')
            """
        )
        await conn.commit()
        # Day 05-17: claude on two issues, codex on one. Day 05-16: codex only.
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("codex", "gpt-5.5", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("codex", "gpt-5.5", 10, 2, 0, 3)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            claude_resp = await client.get("/api/spend/heatmap?days=60&provider=claude")
            codex_resp = await client.get("/api/spend/heatmap?days=60&provider=codex")
    finally:
        await conn.close()

    assert claude_resp.status_code == 200
    claude_days = {d["date"]: d for d in claude_resp.json()["days"]}
    # Only claude's single run on 05-17; 05-16 has no claude rows.
    assert claude_days["2026-05-17"] == {
        "date": "2026-05-17",
        "input_tokens": 100,
        "output_tokens": 20,
        "cache_write_tokens": 30,
        "cache_read_tokens": 40,
        "issues": 1,
    }
    assert "2026-05-16" not in claude_days

    assert codex_resp.status_code == 200
    codex_days = {d["date"]: d for d in codex_resp.json()["days"]}
    assert codex_days["2026-05-17"] == {
        "date": "2026-05-17",
        "input_tokens": 50,
        "output_tokens": 5,
        "cache_write_tokens": 5,
        "cache_read_tokens": 5,
        "issues": 1,
    }
    assert codex_days["2026-05-16"] == {
        "date": "2026-05-16",
        "input_tokens": 10,
        "output_tokens": 2,
        "cache_write_tokens": 0,
        "cache_read_tokens": 3,
        "issues": 1,
    }


@pytest.mark.asyncio
async def test_api_issues_filters_by_teams(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (
            ("v", "VIB-1", "VIB"),
            ("a", "ADJ-1", "ADJ"),
            ("s", "SYM-1", "SYM"),
        ):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at,
                input_tokens, output_tokens, cache_write_tokens, cache_read_tokens)
            VALUES
                ('rv', 'v', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z',
                 100, 20, 30, 40),
                ('ra', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z',
                 50, 5, 5, 5),
                ('rs', 's', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z',
                 10, 2, 0, 3)
            """
        )
        await conn.execute(
            "INSERT INTO review_state (issue_id, iteration) VALUES ('v', 1), ('a', 1), ('s', 1)"
        )
        await conn.commit()
        # VIB on claude, ADJ on codex — lets us prove teams AND provider.
        await db.run_model_usage.replace_for_run(
            conn, "rv", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "ra", [ModelUsage("codex", "gpt-5.5", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "rs", [ModelUsage("claude", "claude-opus-4-8", 10, 2, 0, 3)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            one = await client.get("/api/issues?scope=active&teams=VIB")
            multi = await client.get("/api/issues?scope=active&teams=VIB,ADJ")
            unknown = await client.get("/api/issues?scope=active&teams=NOPE")
            none = await client.get("/api/issues?scope=active")
            combo = await client.get("/api/issues?scope=active&teams=VIB,ADJ&provider=claude")
    finally:
        await conn.close()

    assert one.status_code == 200
    assert {r["id"] for r in one.json()} == {"v"}
    # Multiple teams OR together.
    assert {r["id"] for r in multi.json()} == {"v", "a"}
    # Unknown team → empty, not errored.
    assert unknown.status_code == 200
    assert unknown.json() == []
    # No teams param → all teams.
    assert {r["id"] for r in none.json()} == {"v", "a", "s"}
    # teams AND provider: ADJ has no claude usage, so it drops out.
    assert {r["id"] for r in combo.json()} == {"v"}


@pytest.mark.asyncio
async def test_api_spend_summary_filters_by_teams(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (
            ("a", "VIB-1", "VIB"),
            ("b", "ADJ-1", "ADJ"),
            ("c", "SYM-1", "SYM"),
        ):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z'),
                ('r2', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z'),
                ('r3', 'c', 'implement', 'completed', NULL, '2026-05-17T12:00:00Z')
            """
        )
        await conn.commit()
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("codex", "gpt-5.5", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("claude", "claude-opus-4-8", 10, 2, 0, 3)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            multi = await client.get("/api/spend/summary?teams=VIB,ADJ")
            combo = await client.get("/api/spend/summary?teams=VIB&provider=claude")
    finally:
        await conn.close()

    assert multi.status_code == 200
    body = multi.json()
    # Only the selected teams' rows; SYM excluded. Sorted by output desc.
    assert [t["key"] for t in body["per_team"]] == ["VIB", "ADJ"]
    assert body["totals"]["output_tokens"] == 20 + 5
    assert body["totals"]["issues"] == 2
    # per_provider also scopes to the teams: claude(VIB)=20, codex(ADJ)=5.
    providers = {p["provider"]: p for p in body["per_provider"]}
    assert set(providers) == {"claude", "codex"}
    assert providers["claude"]["output_tokens"] == 20
    assert providers["codex"]["output_tokens"] == 5

    # teams AND provider.
    assert combo.status_code == 200
    cbody = combo.json()
    assert [t["key"] for t in cbody["per_team"]] == ["VIB"]
    assert cbody["totals"]["output_tokens"] == 20
    assert [p["provider"] for p in cbody["per_provider"]] == ["claude"]


@pytest.mark.asyncio
async def test_api_spend_summary_returns_unscoped_teams_from_config(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="a", identifier="VIB-1", title="t", team_key="VIB")
        await conn.execute(
            "INSERT INTO runs (id, issue_id, stage, status, pid, started_at) "
            "VALUES ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z')"
        )
        await conn.commit()
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        states = {"ready": "Ready"}
        config = Config(
            repos=[
                RepoBinding(project_key="VIB", github_repo="o/v", states=states),
                RepoBinding(project_key="ADJ", github_repo="o/a", states=states),
            ]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
            ui_external_config=config,
        )
        async with await _client(app) as client:
            unfiltered = await client.get("/api/spend/summary")
            filtered = await client.get("/api/spend/summary?teams=VIB")
    finally:
        await conn.close()

    assert unfiltered.status_code == 200
    # Teams list sourced from config bindings, sorted, regardless of seeded data.
    assert unfiltered.json()["teams"] == ["ADJ", "VIB"]
    # The list does not change when a filter is applied.
    assert filtered.json()["teams"] == ["ADJ", "VIB"]


@pytest.mark.asyncio
async def test_api_spend_heatmap_filters_by_teams(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (
            ("a", "VIB-1", "VIB"),
            ("b", "ADJ-1", "ADJ"),
            ("c", "SYM-1", "SYM"),
        ):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at,
                input_tokens, output_tokens, cache_write_tokens, cache_read_tokens)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z',
                 100, 20, 30, 40),
                ('r2', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z',
                 50, 5, 5, 5),
                ('r3', 'c', 'implement', 'completed', NULL, '2026-05-16T09:00:00Z',
                 10, 2, 0, 3)
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
        async with await _client(app) as client:
            one = await client.get("/api/spend/heatmap?days=60&teams=VIB")
            multi = await client.get("/api/spend/heatmap?days=60&teams=VIB,ADJ")
    finally:
        await conn.close()

    assert one.status_code == 200
    one_days = {d["date"]: d for d in one.json()["days"]}
    assert one_days["2026-05-17"] == {
        "date": "2026-05-17",
        "input_tokens": 100,
        "output_tokens": 20,
        "cache_write_tokens": 30,
        "cache_read_tokens": 40,
        "issues": 1,
    }
    # SYM's 05-16 row is excluded.
    assert "2026-05-16" not in one_days

    # Multiple teams OR together within the day bucket.
    multi_days = {d["date"]: d for d in multi.json()["days"]}
    assert multi_days["2026-05-17"]["output_tokens"] == 20 + 5
    assert multi_days["2026-05-17"]["issues"] == 2


@pytest.mark.asyncio
async def test_api_issues_filters_by_models(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (
            ("v", "VIB-1", "VIB"),
            ("a", "ADJ-1", "ADJ"),
            ("s", "SYM-1", "SYM"),
        ):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at,
                input_tokens, output_tokens, cache_write_tokens, cache_read_tokens)
            VALUES
                ('rv', 'v', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z',
                 100, 20, 30, 40),
                ('ra', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z',
                 50, 5, 5, 5),
                ('rs', 's', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z',
                 10, 2, 0, 3)
            """
        )
        await conn.execute(
            "INSERT INTO review_state (issue_id, iteration) VALUES ('v', 1), ('a', 1), ('s', 1)"
        )
        await conn.commit()
        await db.run_model_usage.replace_for_run(
            conn, "rv", [ModelUsage("claude", "opus-4.1", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "ra", [ModelUsage("codex", "gpt-5-codex", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "rs", [ModelUsage("claude", "sonnet-4-6", 10, 2, 0, 3)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            one = await client.get("/api/issues?scope=active&models=claude:opus-4.1")
            multi = await client.get(
                "/api/issues?scope=active&models=claude:opus-4.1,codex:gpt-5-codex"
            )
            unknown = await client.get("/api/issues?scope=active&models=claude:nope")
            none = await client.get("/api/issues?scope=active")
            combo = await client.get(
                "/api/issues?scope=active&models=claude:opus-4.1,codex:gpt-5-codex&teams=VIB"
            )
    finally:
        await conn.close()

    assert one.status_code == 200
    assert {r["id"] for r in one.json()} == {"v"}
    # Multiple models OR together (across providers).
    assert {r["id"] for r in multi.json()} == {"v", "a"}
    # Unknown model → empty, not errored.
    assert unknown.status_code == 200
    assert unknown.json() == []
    # No models param → all models.
    assert {r["id"] for r in none.json()} == {"v", "a", "s"}
    # models AND teams: ADJ excluded by team filter even though its model matches.
    assert {r["id"] for r in combo.json()} == {"v"}


@pytest.mark.asyncio
async def test_api_spend_summary_filters_by_models(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (
            ("a", "VIB-1", "VIB"),
            ("b", "ADJ-1", "ADJ"),
            ("c", "SYM-1", "SYM"),
        ):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z'),
                ('r2', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z'),
                ('r3', 'c', 'implement', 'completed', NULL, '2026-05-17T12:00:00Z')
            """
        )
        await conn.commit()
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "opus-4.1", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("codex", "gpt-5-codex", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("claude", "sonnet-4-6", 10, 2, 0, 3)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            multi = await client.get("/api/spend/summary?models=claude:opus-4.1,codex:gpt-5-codex")
            combo = await client.get(
                "/api/spend/summary?models=claude:opus-4.1,claude:sonnet-4-6&provider=claude"
            )
    finally:
        await conn.close()

    assert multi.status_code == 200
    body = multi.json()
    # Only the selected models' rows; SYM's sonnet-4-6 excluded.
    assert {t["key"] for t in body["per_team"]} == {"VIB", "ADJ"}
    assert body["totals"]["output_tokens"] == 20 + 5
    assert body["totals"]["issues"] == 2
    providers = {p["provider"]: p for p in body["per_provider"]}
    assert set(providers) == {"claude", "codex"}
    assert providers["claude"]["output_tokens"] == 20
    assert providers["codex"]["output_tokens"] == 5

    # models AND provider: both selected models are claude; codex unaffected.
    assert combo.status_code == 200
    cbody = combo.json()
    assert {t["key"] for t in cbody["per_team"]} == {"VIB", "SYM"}
    assert cbody["totals"]["output_tokens"] == 20 + 2
    assert [p["provider"] for p in cbody["per_provider"]] == ["claude"]


@pytest.mark.asyncio
async def test_api_spend_summary_returns_unscoped_models(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="a", identifier="VIB-1", title="t", team_key="VIB")
        await db.issues.upsert(conn, id="b", identifier="ADJ-1", title="t", team_key="ADJ")
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z'),
                ('r2', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z')
            """
        )
        await conn.commit()
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "opus-4.1", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("codex", "gpt-5-codex", 50, 5, 5, 5)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            unfiltered = await client.get("/api/spend/summary")
            filtered = await client.get("/api/spend/summary?models=claude:opus-4.1")
    finally:
        await conn.close()

    assert unfiltered.status_code == 200
    # Distinct (provider, model) from run_model_usage, sorted, unscoped.
    expected = [
        {"provider": "claude", "model": "opus-4.1"},
        {"provider": "codex", "model": "gpt-5-codex"},
    ]
    assert unfiltered.json()["models"] == expected
    # The list is stable under an active filter.
    assert filtered.json()["models"] == expected


@pytest.mark.asyncio
async def test_api_spend_heatmap_filters_by_models(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        for iid, ident, team in (
            ("a", "VIB-1", "VIB"),
            ("b", "ADJ-1", "ADJ"),
            ("c", "SYM-1", "SYM"),
        ):
            await db.issues.upsert(conn, id=iid, identifier=ident, title=ident, team_key=team)
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z'),
                ('r2', 'b', 'implement', 'completed', NULL, '2026-05-17T11:00:00Z'),
                ('r3', 'c', 'implement', 'completed', NULL, '2026-05-16T09:00:00Z')
            """
        )
        await conn.commit()
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "opus-4.1", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("codex", "gpt-5-codex", 50, 5, 5, 5)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r3", [ModelUsage("claude", "sonnet-4-6", 10, 2, 0, 3)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            one = await client.get("/api/spend/heatmap?days=60&models=claude:opus-4.1")
            multi = await client.get(
                "/api/spend/heatmap?days=60&models=claude:opus-4.1,codex:gpt-5-codex"
            )
    finally:
        await conn.close()

    assert one.status_code == 200
    one_days = {d["date"]: d for d in one.json()["days"]}
    assert one_days["2026-05-17"] == {
        "date": "2026-05-17",
        "input_tokens": 100,
        "output_tokens": 20,
        "cache_write_tokens": 30,
        "cache_read_tokens": 40,
        "issues": 1,
    }
    # SYM's sonnet-4-6 row on 05-16 is excluded.
    assert "2026-05-16" not in one_days

    # Multiple models OR together within the day bucket.
    multi_days = {d["date"]: d for d in multi.json()["days"]}
    assert multi_days["2026-05-17"]["output_tokens"] == 20 + 5
    assert multi_days["2026-05-17"]["issues"] == 2


@pytest.mark.asyncio
async def test_api_issues_done_scope_window_and_completed_at(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        # Recently-merged (1 day ago) -> done, inside 7d window.
        await db.issues.upsert(
            conn, id="done-recent", identifier="ENG-1", title="recent", team_key="ENG"
        )
        # Merged 16 days ago -> done, outside 7d window.
        await db.issues.upsert(conn, id="done-old", identifier="ENG-2", title="old", team_key="ENG")
        # Running -> not done.
        await db.issues.upsert(
            conn, id="active", identifier="ENG-3", title="active", team_key="ENG"
        )
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
            VALUES
                ('r1', 'done-recent', 'merge', 'completed', NULL,
                 '2026-05-16T09:00:00Z', '2026-05-16T09:30:00Z', 3.0),
                ('r2', 'done-old', 'merge', 'completed', NULL,
                 '2026-05-01T09:00:00Z', '2026-05-01T09:30:00Z', 1.0),
                ('r3', 'active', 'implement', 'running', 999,
                 '2026-05-17T11:00:00Z', NULL, 0.5)
            """
        )
        await conn.execute(
            """
            INSERT INTO issue_prs (issue_id, github_repo, binding_key, pr_number,
                pr_url, created_at, merged_at)
            VALUES
                ('done-recent', 'o/r', 'k', 1, 'u', '2026-05-16T08:00:00Z',
                 '2026-05-16T10:00:00Z'),
                ('done-old', 'o/r', 'k', 2, 'u', '2026-05-01T08:00:00Z',
                 '2026-05-01T10:00:00Z'),
                ('active', 'o/r', 'k', 3, 'u', '2026-05-17T10:00:00Z', NULL)
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
        async with await _client(app) as client:
            wk = await client.get("/api/issues?scope=done&from=2026-05-10&to=2026-05-17")
            mo = await client.get("/api/issues?scope=done&from=2026-04-25&to=2026-05-17")
            allt = await client.get("/api/issues?scope=done")
    finally:
        await conn.close()

    assert wk.status_code == 200
    week = wk.json()
    # done-old (merged 05-01) is below the from bound; only ENG-1 lands in window.
    assert [i["identifier"] for i in week] == ["ENG-1"]
    assert week[0]["completed_at"] == "2026-05-16T10:00:00Z"
    assert "cost_usd" not in week[0]
    assert week[0]["canonical_status"]["state"] == "done"

    # Wider window includes both, newest first.
    assert [i["identifier"] for i in mo.json()] == ["ENG-1", "ENG-2"]
    # No window → all-time done, both kept.
    assert [i["identifier"] for i in allt.json()] == ["ENG-1", "ENG-2"]


@pytest.mark.asyncio
async def test_api_issues_active_scope_filters_by_date_window(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="act-new", identifier="ENG-1", title="new", team_key="ENG")
        await db.issues.upsert(conn, id="act-old", identifier="ENG-2", title="old", team_key="ENG")
        # Both are active (running runs); their last activity is the run start.
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'act-new', 'implement', 'running', 1, '2026-05-16T09:00:00Z'),
                ('r2', 'act-old', 'implement', 'running', 2, '2026-04-01T09:00:00Z')
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
        async with await _client(app) as client:
            win = await client.get("/api/issues?scope=active&from=2026-05-10&to=2026-05-17")
            allt = await client.get("/api/issues?scope=active")
    finally:
        await conn.close()

    assert win.status_code == 200
    # Only the issue whose last activity falls in the window.
    assert [i["identifier"] for i in win.json()] == ["ENG-1"]
    # No window → both active issues.
    assert {i["identifier"] for i in allt.json()} == {"ENG-1", "ENG-2"}


@pytest.mark.asyncio
async def test_api_spend_summary_filters_by_date_window(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="a", identifier="ENG-1", title="t", team_key="ENG")
        await db.issues.upsert(conn, id="b", identifier="WEB-1", title="t", team_key="WEB")
        await conn.execute(
            """
            INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
            VALUES
                ('r1', 'a', 'implement', 'completed', NULL, '2026-05-17T10:00:00Z'),
                ('r2', 'a', 'review', 'completed', NULL, '2026-05-16T09:00:00Z'),
                ('r-old', 'b', 'implement', 'completed', NULL, '2024-01-01T00:00:00Z')
            """
        )
        await conn.commit()
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r2", [ModelUsage("claude", "claude-opus-4-8", 10, 2, 3, 4)]
        )
        await db.run_model_usage.replace_for_run(
            conn, "r-old", [ModelUsage("claude", "claude-opus-4-8", 999, 99, 99, 99)]
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: UI_NOW,
        )
        async with await _client(app) as client:
            windowed = await client.get("/api/spend/summary?from=2026-05-16&to=2026-05-17")
            allt = await client.get("/api/spend/summary")
    finally:
        await conn.close()

    assert windowed.status_code == 200
    body = windowed.json()
    # Only the two May runs (issue a); the 2024 run is outside the window.
    assert body["totals"] == {
        "input_tokens": 110,
        "output_tokens": 22,
        "cache_write_tokens": 33,
        "cache_read_tokens": 44,
        "issues": 1,
    }
    assert [t["key"] for t in body["per_team"]] == ["ENG"]
    assert body["per_provider"][0]["output_tokens"] == 22
    # All-time still sees the 2024 run on the WEB team.
    assert allt.json()["totals"]["output_tokens"] == 22 + 99
    assert {t["key"] for t in allt.json()["per_team"]} == {"ENG", "WEB"}


class _FakeCommandSink:
    def __init__(self) -> None:
        self.calls: list[tuple[str, SlashKind]] = []

    def enqueue_web_command(self, issue_id: str, kind: SlashKind) -> str:
        self.calls.append((issue_id, kind))
        return "cmd-123"


@pytest.mark.asyncio
async def test_api_issue_command_accepts_and_enqueues(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    sink = _FakeCommandSink()
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG")
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            ui_command_sink=sink,
        )
        async with await _client(app) as client:
            ok = await client.post("/api/issues/iss-1/command", json={"command": "approve"})
            bad = await client.post("/api/issues/iss-1/command", json={"command": "nope"})
            missing = await client.post("/api/issues/ghost/command", json={"command": "approve"})
    finally:
        await conn.close()

    assert ok.status_code == 200
    assert ok.json() == {"status": "accepted", "command_id": "cmd-123", "command": "$approve"}
    assert sink.calls == [("iss-1", SlashKind.APPROVE)]
    assert bad.status_code == 400
    assert missing.status_code == 404


class _FakePauseController:
    def __init__(self, paused: bool = False) -> None:
        self.paused = paused

    def is_dispatch_paused(self) -> bool:
        return self.paused

    async def set_dispatch_paused(self, paused: bool) -> None:
        self.paused = paused


@pytest.mark.asyncio
async def test_api_pause_reads_and_toggles(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    pause = _FakePauseController()
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            ui_pause_controller=pause,
        )
        async with await _client(app) as client:
            initial = await client.get("/api/pause")
            paused = await client.post("/api/pause", json={"paused": True})
            after_pause = await client.get("/api/pause")
            resumed = await client.post("/api/pause", json={"paused": False})
    finally:
        await conn.close()

    assert initial.status_code == 200
    assert initial.json() == {"paused": False}
    assert paused.status_code == 200
    assert paused.json() == {"paused": True}
    assert after_pause.json() == {"paused": True}
    assert resumed.json() == {"paused": False}
    assert pause.paused is False


@pytest.mark.asyncio
async def test_api_pause_503_without_controller(tmp_path: Path) -> None:
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
        async with await _client(app) as client:
            get = await client.get("/api/pause")
            post = await client.post("/api/pause", json={"paused": True})
    finally:
        await conn.close()

    assert get.status_code == 503
    assert post.status_code == 503


@pytest.mark.asyncio
async def test_api_issue_command_503_without_sink(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG")
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
        )
        async with await _client(app) as client:
            response = await client.post("/api/issues/iss-1/command", json={"command": "approve"})
    finally:
        await conn.close()

    assert response.status_code == 503
