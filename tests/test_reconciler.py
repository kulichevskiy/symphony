from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.github.client import GitHubError
from symphony.github.webhook import GitHubWebhookEvent
from symphony.linear.client import LinearError, LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.orchestrator.reconciler import (
    ACTION_OBSERVED,
    ACTION_WOULD_CLEAR,
    DRIFT_LINEAR_STATE_DONE,
    DRIFT_MERGE_ZOMBIE,
    DRIFT_PR_CLOSED_NO_MERGE,
    DRIFT_PR_LOCALLY_MERGED,
    GithubPrObservation,
    Reconciler,
    classify_github_drift,
    classify_linear_drift,
)

NOW = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)


@dataclass
class _FakeLinear:
    state_name: str = "In Progress"
    error: Exception | None = None
    calls: int = 0

    async def lookup_issue(self, issue_id: str) -> LinearIssue:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return LinearIssue(
            id=issue_id,
            identifier="ENG-1",
            title="Tracked issue",
            description="",
            url="https://linear.app/issue/ENG-1",
            state_id="state",
            state_name=self.state_name,
            state_type="completed" if self.state_name == "Done" else "started",
            team_key="ENG",
            labels=["symphony"],
            updated_at="2026-05-17T11:58:00Z",
        )


class _FakeGitHub:
    def __init__(
        self,
        *,
        view: dict[str, object] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.view = view or {
            "state": "OPEN",
            "mergeable": "MERGEABLE",
            "mergedAt": None,
            "url": "https://github.com/org/repo/pull/42",
        }
        self.error = error
        self.calls: list[tuple[int, str | None]] = []

    async def pr_view(self, pr: int | str, *, repo: str | None = None) -> dict[str, Any]:
        self.calls.append((int(pr), repo))
        if self.error is not None:
            raise self.error
        return dict(self.view)


def _binding(
    *,
    issue_label: str | None = "symphony",
    reconcile_enabled: bool = True,
) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        issue_label=issue_label,
        reconcile_enabled=reconcile_enabled,
        linear_states=LinearStates(ready="Todo", done="Done"),
    )


async def _seed_issue(conn: aiosqlite.Connection, issue_id: str = "iss-1") -> None:
    await db.issues.upsert(
        conn,
        id=issue_id,
        identifier="ENG-1",
        title="Tracked issue",
        team_key="ENG",
    )


async def _seed_run(conn: aiosqlite.Connection, issue_id: str, run_id: str) -> None:
    await db.runs.create(
        conn,
        id=run_id,
        issue_id=issue_id,
        stage="merge",
        status="needs_approval",
        pid=None,
        started_at="2026-05-17T10:00:00Z",
    )


async def _seed_merge_wait(conn: aiosqlite.Connection, issue_id: str = "iss-1") -> None:
    await _seed_run(conn, issue_id, f"run-{issue_id}")
    await db.operator_waits.upsert(
        conn,
        issue_id=issue_id,
        run_id=f"run-{issue_id}",
        kind=db.operator_waits.KIND_MERGE,
        linear_team_key="ENG",
        github_repo="org/repo",
        issue_label="symphony",
        created_at="2026-05-17T10:01:00Z",
    )


async def _seed_pr(
    conn: aiosqlite.Connection,
    issue_id: str = "iss-1",
    *,
    binding_key: str = '["ENG","org/repo","symphony"]',
) -> None:
    await db.issue_prs.upsert(
        conn,
        issue_id=issue_id,
        github_repo="org/repo",
        binding_key=binding_key,
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        created_at="2026-05-17T10:02:00Z",
    )


async def _observation_rows(
    conn: aiosqlite.Connection,
) -> list[tuple[str, str | None, str, str]]:
    cur = await conn.execute(
        """
        SELECT source, drift_kind, action_taken, payload_json
        FROM external_observations
        ORDER BY id
        """
    )
    rows = await cur.fetchall()
    return [
        (
            str(row["source"]),
            str(row["drift_kind"]) if row["drift_kind"] is not None else None,
            str(row["action_taken"]),
            str(row["payload_json"]),
        )
        for row in rows
    ]


def test_classifies_all_drift_kinds() -> None:
    merged_pr = GithubPrObservation(
        github_repo="org/repo",
        pr_number=42,
        state="MERGED",
        mergeable=None,
        merged=True,
        merged_at="2026-05-17T11:00:00Z",
        url="https://github.com/org/repo/pull/42",
    )
    closed_pr = GithubPrObservation(
        github_repo="org/repo",
        pr_number=43,
        state="CLOSED",
        mergeable=None,
        merged=False,
        merged_at=None,
        url="https://github.com/org/repo/pull/43",
    )

    assert (
        classify_linear_drift(
            has_operator_wait=True,
            state_name="Done",
            done_state_names={"Done"},
        )
        == DRIFT_LINEAR_STATE_DONE
    )
    assert classify_github_drift(has_merge_wait=True, prs=[merged_pr]) == DRIFT_MERGE_ZOMBIE
    assert (
        classify_github_drift(has_merge_wait=True, prs=[closed_pr])
        == DRIFT_PR_CLOSED_NO_MERGE
    )
    assert (
        classify_github_drift(has_merge_wait=False, prs=[merged_pr])
        == DRIFT_PR_LOCALLY_MERGED
    )


@pytest.mark.asyncio
async def test_tick_writes_dry_run_observations_for_each_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "1")
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_merge_wait(conn)
        await _seed_pr(conn)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(state_name="Done"),  # type: ignore[arg-type]
            _FakeGitHub(
                view={
                    "state": "MERGED",
                    "mergeable": "UNKNOWN",
                    "mergedAt": "2026-05-17T11:59:00Z",
                    "url": "https://github.com/org/repo/pull/42",
                }
            ),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2

        rows = await _observation_rows(conn)
    finally:
        await conn.close()

    assert [(source, drift, action) for source, drift, action, _ in rows] == [
        ("linear", DRIFT_LINEAR_STATE_DONE, ACTION_WOULD_CLEAR),
        ("github", DRIFT_MERGE_ZOMBIE, ACTION_WOULD_CLEAR),
    ]
    assert json.loads(rows[0][3])["reason"] == "periodic"
    assert json.loads(rows[1][3])["prs"][0]["merged"] is True


@pytest.mark.asyncio
async def test_drift_without_dry_run_keeps_observed_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SYMPHONY_RECONCILE_DRYRUN", raising=False)
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_pr(conn)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            _FakeGitHub(
                view={
                    "state": "MERGED",
                    "mergeable": "UNKNOWN",
                    "mergedAt": "2026-05-17T11:59:00Z",
                    "url": "https://github.com/org/repo/pull/42",
                }
            ),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        rows = await _observation_rows(conn)
    finally:
        await conn.close()

    assert rows[1][1] == DRIFT_PR_LOCALLY_MERGED
    assert rows[1][2] == ACTION_OBSERVED


@pytest.mark.asyncio
async def test_tick_honors_max_per_tick(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        for idx in range(51):
            issue_id = f"iss-{idx:02d}"
            await db.issues.upsert(
                conn,
                id=issue_id,
                identifier=f"ENG-{idx}",
                title=f"Issue {idx}",
                team_key="ENG",
            )
            await db.issue_prs.upsert(
                conn,
                issue_id=issue_id,
                github_repo="org/repo",
                binding_key='["ENG","org/repo","symphony"]',
                pr_number=idx + 1,
                pr_url=f"https://github.com/org/repo/pull/{idx + 1}",
                created_at=f"2026-05-17T10:{idx:02d}:00Z",
            )
        reconciler = Reconciler(
            Config(repos=[_binding()], reconcile_max_per_tick=50),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            _FakeGitHub(),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 100
        cur = await conn.execute(
            "SELECT COUNT(DISTINCT issue_id) AS issue_count, COUNT(*) AS row_count "
            "FROM external_observations"
        )
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert row is not None
    assert row["issue_count"] == 50
    assert row["row_count"] == 100


@pytest.mark.asyncio
async def test_tick_skips_candidate_without_matching_binding(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    fake_linear = _FakeLinear()
    fake_gh = _FakeGitHub()
    try:
        await _seed_issue(conn)
        await _seed_pr(conn)
        reconciler = Reconciler(
            Config(
                repos=[
                    RepoBinding(
                        linear_team_key="ENG",
                        github_repo="org/other",
                        issue_label="symphony",
                        linear_states=LinearStates(ready="Todo"),
                    )
                ]
            ),
            conn,
            fake_linear,  # type: ignore[arg-type]
            fake_gh,  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 0
        cur = await conn.execute("SELECT COUNT(*) AS count FROM external_observations")
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert fake_linear.calls == 0
    assert fake_gh.calls == []
    assert row is not None
    assert row["count"] == 0


@pytest.mark.asyncio
async def test_tick_skips_pr_with_ambiguous_missing_binding_key(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    fake_linear = _FakeLinear()
    fake_gh = _FakeGitHub()
    try:
        await _seed_issue(conn)
        await _seed_pr(conn, binding_key="")
        reconciler = Reconciler(
            Config(
                repos=[
                    _binding(issue_label="backend", reconcile_enabled=False),
                    _binding(issue_label="frontend", reconcile_enabled=True),
                ]
            ),
            conn,
            fake_linear,  # type: ignore[arg-type]
            fake_gh,  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 0
        cur = await conn.execute("SELECT COUNT(*) AS count FROM external_observations")
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert fake_linear.calls == 0
    assert fake_gh.calls == []
    assert row is not None
    assert row["count"] == 0


@pytest.mark.asyncio
async def test_tick_allows_missing_binding_key_when_pr_binding_is_unambiguous(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_pr(conn, binding_key="")
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            _FakeGitHub(),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        rows = await _observation_rows(conn)
    finally:
        await conn.close()

    assert [source for source, _, _, _ in rows] == ["linear", "github"]


@pytest.mark.asyncio
async def test_rate_limit_enters_backoff_and_skips_followup_tick(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    fake_linear = _FakeLinear(error=LinearError("server error 429: slow down"))
    try:
        await _seed_issue(conn)
        await _seed_pr(conn)
        reconciler = Reconciler(
            Config(repos=[_binding()], reconcile_backoff_secs=600),
            conn,
            fake_linear,  # type: ignore[arg-type]
            _FakeGitHub(),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 0
        assert await reconciler.tick() == 0
        cur = await conn.execute("SELECT COUNT(*) AS count FROM external_observations")
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert fake_linear.calls == 1
    assert row is not None
    assert row["count"] == 0


@pytest.mark.asyncio
async def test_github_webhook_event_reconciles_linked_issue(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_pr(conn)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            _FakeGitHub(),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )
        event = GitHubWebhookEvent(
            event_type="pull_request",
            action="closed",
            repo="ORG/REPO",
            delivery_id="delivery-1",
            pr_number=42,
            merged=True,
        )

        assert await reconciler.reconcile_github_event(event) == 2
        rows = await _observation_rows(conn)
    finally:
        await conn.close()

    assert len(rows) == 2
    assert json.loads(rows[0][3])["reason"] == "github_webhook:pull_request.closed"


@pytest.mark.asyncio
async def test_github_5xx_enters_backoff_without_partial_rows(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    fake_gh = _FakeGitHub(error=GitHubError("gh api exited 1: HTTP 503"))
    try:
        await _seed_issue(conn)
        await _seed_pr(conn)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            fake_gh,  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 0
        cur = await conn.execute("SELECT COUNT(*) AS count FROM external_observations")
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert fake_gh.calls == [(42, "org/repo")]
    assert row is not None
    assert row["count"] == 0


@pytest.mark.asyncio
async def test_orchestrator_github_webhook_calls_reconciler(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        linear = AsyncMock()
        runner = MagicMock()
        runner.kill = AsyncMock()
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=Path("/dev/null"))
        workspace.release = MagicMock()
        orch = Orchestrator(
            Config(repos=[_binding()]),
            linear,
            conn,
            runner=runner,
            gh=MagicMock(),
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        spy = MagicMock()
        spy.reconcile_github_event = AsyncMock(return_value=2)
        orch._reconciler = spy  # noqa: SLF001
        event = GitHubWebhookEvent(
            event_type="pull_request",
            action="closed",
            repo="org/repo",
            delivery_id="delivery-1",
            pr_number=42,
        )

        result = await orch.handle_github_webhook(event)
    finally:
        await conn.close()

    assert result.handled is True
    assert result.detail == "observations=2"
    spy.reconcile_github_event.assert_awaited_once_with(event)
