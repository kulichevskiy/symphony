from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from symphony import db
from symphony.app import create_app
from symphony.config import Config, LinearStates, RepoBinding
from symphony.ui.external import (
    ExternalSnapshotService,
    compute_drift,
    sqlite_external_view,
)

from .test_app import _dist, _Handler

NOW = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)


class _FakeLinear:
    def __init__(self, *responses: dict[str, Any] | Exception) -> None:
        self.calls: list[str] = []
        self._responses = list(responses)

    async def issue_external_snapshot(self, issue_id: str) -> dict[str, Any]:
        self.calls.append(issue_id)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class _FakeGitHub:
    def __init__(self, *responses: dict[str, Any] | Exception) -> None:
        self.calls: list[tuple[int | str, str]] = []
        self._responses = list(responses)

    async def pr_external_snapshot(self, pr: int | str, *, repo: str) -> dict[str, Any]:
        self.calls.append((pr, repo))
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _config(*, done_state: str = "Done") -> Config:
    return Config(
        linear_api_key="x",
        repos=[
            RepoBinding(
                linear_team_key="ENG",
                github_repo="org/repo",
                issue_label="symphony",
                linear_states=LinearStates(ready="Todo", done=done_state),
            )
        ],
    )


async def _connect(tmp_path: Path):
    return await db.connect(tmp_path / "state.sqlite")


async def _seed_external_issue(conn, *, merged_at: str | None = None) -> None:
    await db.issues.upsert(
        conn,
        id="iss-1",
        identifier="ENG-1",
        title="Watch drift",
        team_key="ENG",
    )
    await conn.execute(
        """
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, ended_at, cost_usd)
        VALUES ('run-1', 'iss-1', 'merge', 'running', NULL, '2026-05-17T11:00:00Z', NULL, 0)
        """
    )
    await conn.execute(
        """
        INSERT INTO issue_prs (
            issue_id, github_repo, binding_key, pr_number, pr_url, created_at, merged_at
        )
        VALUES (
            'iss-1', 'org/repo', 'ENG|org/repo', 42, 'https://github.com/org/repo/pull/42',
            '2026-05-17T11:05:00Z', ?
        )
        """,
        (merged_at,),
    )
    await conn.execute(
        """
        INSERT INTO operator_waits (
            issue_id, run_id, kind, linear_team_key, github_repo, issue_label, created_at
        )
        VALUES ('iss-1', 'run-1', 'merge', 'ENG', 'org/repo', 'symphony',
                '2026-05-17T11:10:00Z')
        """
    )
    await conn.commit()


def _linear_payload(state: str = "Done", body: str = "$stop") -> dict[str, Any]:
    return {
        "state": state,
        "updated_at": "2026-05-17T11:20:00Z",
        "labels": ["symphony"],
        "comments": [
            {
                "author": "Ada",
                "ts": "2026-05-17T11:19:00Z",
                "body": body,
                "comment_id": "lc-1",
                "url": "https://linear.app/issue/ENG-1#comment-lc-1",
            }
        ],
    }


def _github_payload(
    state: str = "MERGED",
    *,
    merged_at: str | None = "2026-05-17T11:25:00Z",
    failing: int = 1,
) -> dict[str, Any]:
    return {
        "pr_number": 42,
        "state": state,
        "url": "https://github.com/org/repo/pull/42",
        "mergeable": "MERGEABLE",
        "merge_state_status": "CLEAN",
        "merged_at": merged_at,
        "merged_by": "octo",
        "check_summary": {"passing": 2, "failing": failing, "pending": 0, "total": 3},
        "comments": [
            {
                "author": "codex",
                "ts": "2026-05-17T11:24:00Z",
                "body": "LGTM",
                "comment_id": 99,
                "url": "https://github.com/org/repo/pull/42#discussion_r99",
            }
        ],
    }


@pytest.mark.asyncio
async def test_compute_drift_reports_all_documented_positive_rules(tmp_path: Path) -> None:
    conn = await _connect(tmp_path)
    try:
        await _seed_external_issue(conn)
        sqlite_view = await sqlite_external_view(conn, "iss-1")
    finally:
        await conn.close()

    assert sqlite_view is not None
    flags = compute_drift(
        sqlite_view,
        {"linear": _linear_payload(), "github": _github_payload()},
    )

    assert [flag.field for flag in flags] == [
        "linear.state",
        "github.state",
        "github.merged_at",
        "github.checks",
    ]
    assert flags[-1].severity == "warning"


@pytest.mark.asyncio
async def test_compute_drift_ignores_matching_external_state(tmp_path: Path) -> None:
    conn = await _connect(tmp_path)
    try:
        await _seed_external_issue(conn, merged_at="2026-05-17T11:25:00Z")
        await conn.execute("DELETE FROM operator_waits WHERE issue_id = 'iss-1'")
        await conn.execute("UPDATE runs SET status = 'completed' WHERE issue_id = 'iss-1'")
        await conn.commit()
        sqlite_view = await sqlite_external_view(conn, "iss-1")
    finally:
        await conn.close()

    assert sqlite_view is not None
    flags = compute_drift(
        sqlite_view,
        {
            "linear": _linear_payload(state="In Review"),
            "github": _github_payload(state="OPEN", merged_at=None, failing=0),
        },
    )

    assert flags == []


@pytest.mark.asyncio
async def test_compute_drift_uses_configured_linear_done_state(tmp_path: Path) -> None:
    conn = await _connect(tmp_path)
    try:
        await _seed_external_issue(conn)
        sqlite_view = await sqlite_external_view(conn, "iss-1")
    finally:
        await conn.close()

    assert sqlite_view is not None
    hard_coded_done = compute_drift(
        sqlite_view,
        {"linear": _linear_payload(state="Done")},
        linear_done_state="Completed",
    )
    configured_done = compute_drift(
        sqlite_view,
        {"linear": _linear_payload(state="Completed")},
        linear_done_state="Completed",
    )

    assert [flag.field for flag in hard_coded_done] == []
    assert [flag.field for flag in configured_done] == ["linear.state"]


@pytest.mark.asyncio
async def test_external_snapshot_uses_binding_done_state(tmp_path: Path) -> None:
    conn = await _connect(tmp_path)
    linear = _FakeLinear(_linear_payload(state="Completed"))
    github = _FakeGitHub(_github_payload(state="OPEN", merged_at=None, failing=0))
    service = ExternalSnapshotService(
        _config(done_state="Completed"),
        linear,
        github,
        clock=lambda: NOW,
    )
    try:
        await _seed_external_issue(conn)
        snapshot = await service.get_issue_external(conn, "iss-1")
    finally:
        await conn.close()

    assert snapshot is not None
    assert [flag["field"] for flag in snapshot["drift_flags"]] == ["linear.state"]


@pytest.mark.asyncio
async def test_external_snapshot_resolves_binding_by_issue_label(tmp_path: Path) -> None:
    conn = await _connect(tmp_path)
    linear = _FakeLinear(_linear_payload(state="Completed"))
    github = _FakeGitHub(_github_payload(state="OPEN", merged_at=None, failing=0))
    config = Config(
        linear_api_key="x",
        repos=[
            RepoBinding(
                linear_team_key="ENG",
                github_repo="org/repo",
                issue_label="other",
                linear_states=LinearStates(ready="Todo", done="Done"),
            ),
            RepoBinding(
                linear_team_key="ENG",
                github_repo="org/repo",
                issue_label="symphony",
                linear_states=LinearStates(ready="Todo", done="Completed"),
            ),
        ],
    )
    service = ExternalSnapshotService(config, linear, github, clock=lambda: NOW)
    try:
        await _seed_external_issue(conn)
        snapshot = await service.get_issue_external(conn, "iss-1")
    finally:
        await conn.close()

    assert snapshot is not None
    assert [flag["field"] for flag in snapshot["drift_flags"]] == ["linear.state"]


@pytest.mark.asyncio
async def test_external_snapshot_cache_ttl_and_refresh(tmp_path: Path) -> None:
    current = NOW

    def clock() -> datetime:
        return current

    conn = await _connect(tmp_path)
    linear = _FakeLinear(_linear_payload(), _linear_payload(state="In Review"))
    github = _FakeGitHub(_github_payload(), _github_payload(state="OPEN", merged_at=None))
    service = ExternalSnapshotService(_config(), linear, github, clock=clock)
    try:
        await _seed_external_issue(conn)

        first = await service.get_issue_external(conn, "iss-1")
        current = NOW + timedelta(seconds=10)
        warm = await service.get_issue_external(conn, "iss-1")
        forced = await service.get_issue_external(conn, "iss-1", refresh=True)
        current = NOW + timedelta(seconds=71)
        expired = service.cache.get("iss-1", now=current)
    finally:
        await conn.close()

    assert first is warm
    assert first is not None
    assert forced is not None
    assert expired is None
    assert "iss-1" not in service.cache.payloads
    assert first["fetched_at"] == "2026-05-17T12:00:00Z"
    assert forced["fetched_at"] == "2026-05-17T12:00:10Z"
    assert linear.calls == ["iss-1", "iss-1"]
    assert github.calls == [(42, "org/repo"), (42, "org/repo")]


@pytest.mark.asyncio
async def test_external_snapshot_serves_last_known_good_on_source_error(tmp_path: Path) -> None:
    current = NOW

    def clock() -> datetime:
        return current

    conn = await _connect(tmp_path)
    linear = _FakeLinear(
        _linear_payload(),
        RuntimeError("Linear returned 500"),
        _linear_payload(state="In Review"),
    )
    github = _FakeGitHub(
        _github_payload(),
        _github_payload(state="OPEN", merged_at=None),
        _github_payload(state="OPEN", merged_at=None),
        _github_payload(state="OPEN", merged_at=None),
    )
    service = ExternalSnapshotService(_config(), linear, github, clock=clock)
    try:
        await _seed_external_issue(conn)
        await service.get_issue_external(conn, "iss-1")
        current = NOW + timedelta(seconds=1)
        payload = await service.get_issue_external(conn, "iss-1", refresh=True)
        current = NOW + timedelta(seconds=2)
        backoff_payload = await service.get_issue_external(conn, "iss-1", refresh=True)
        current = NOW + timedelta(seconds=31)
        expired_error_payload = service.cache.get("iss-1", now=current)
        retry_payload = await service.get_issue_external(conn, "iss-1")
    finally:
        await conn.close()

    assert payload is not None
    assert backoff_payload is not None
    assert retry_payload is not None
    assert payload["linear"]["state"] == "Done"
    assert payload["linear"]["stale"] is True
    assert payload["linear"]["error"] == "Linear returned 500"
    assert payload["github"]["state"] == "OPEN"
    assert backoff_payload["linear"]["state"] == "Done"
    assert backoff_payload["linear"]["error"] == "Linear returned 500"
    assert expired_error_payload is None
    assert retry_payload["linear"]["state"] == "In Review"
    assert "error" not in retry_payload["linear"]
    assert ("iss-1", "linear") not in service.cache.source_errors
    assert linear.calls == ["iss-1", "iss-1", "iss-1"]


@pytest.mark.asyncio
async def test_issue_external_endpoint_returns_snapshot_and_drift(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    linear = _FakeLinear(_linear_payload(body="$stop from operator"))
    github = _FakeGitHub(_github_payload())
    service = ExternalSnapshotService(_config(), linear, github, clock=lambda: NOW)
    try:
        await _seed_external_issue(conn)
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_dist_dir=_dist(tmp_path),
            ui_external_service=service,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/api/issues/iss-1/external")
    finally:
        await conn.close()

    assert response.status_code == 200
    payload = response.json()
    assert payload["fetched_at"] == "2026-05-17T12:00:00Z"
    assert payload["linear"]["comments"][0]["body"] == "$stop from operator"
    assert [flag["field"] for flag in payload["drift_flags"]] == [
        "linear.state",
        "github.state",
        "github.merged_at",
        "github.checks",
    ]
