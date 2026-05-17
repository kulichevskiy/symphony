from __future__ import annotations

from pathlib import Path

import aiosqlite
import httpx
import pytest

from symphony import db
from symphony.app import create_app
from symphony.webhook import WebhookSettings

from .test_webhook import NOW, SECRET, _body, _Handler, _headers, _payload


def _dist(tmp_path: Path) -> Path:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text(
        "<!doctype html><html><body>symphonyd UI v0</body></html>"
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
    assert "symphonyd UI v0" in root.text
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
            response = await client.get("/api/issues")
    finally:
        await conn.close()

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "issue-first",
            "identifier": "ADJ-1",
            "title": "Earlier issue",
            "team_key": "ADJ",
        },
        {
            "id": "issue-known",
            "identifier": "ADJ-2",
            "title": "Known tracked issue",
            "team_key": "ADJ",
        },
        {
            "id": "issue-ten",
            "identifier": "ADJ-10",
            "title": "Later issue",
            "team_key": "ADJ",
        },
        {
            "id": "issue-web",
            "identifier": "WEB-1",
            "title": "Other team issue",
            "team_key": "WEB",
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
        "runs": [
            {
                "id": "run-2",
                "stage": "review",
                "status": "running",
                "pid": None,
                "started_at": "2026-05-17T10:20:00Z",
                "ended_at": None,
                "cost_usd": 0.5,
            },
            {
                "id": "run-1",
                "stage": "implement",
                "status": "completed",
                "pid": 123,
                "started_at": "2026-05-17T10:00:00Z",
                "ended_at": "2026-05-17T10:10:00Z",
                "cost_usd": 1.25,
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
            "ts": "2026-05-17T10:04:00Z",
            "kind": "cost_warning_posted",
            "payload": {},
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
