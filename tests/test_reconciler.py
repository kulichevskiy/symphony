from __future__ import annotations

import asyncio
import json
import logging
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
    ACTION_CLEARED,
    ACTION_NOTED,
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
        views_by_repo: dict[str, dict[str, object]] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.view = view or {
            "state": "OPEN",
            "mergeable": "MERGEABLE",
            "mergedAt": None,
            "url": "https://github.com/org/repo/pull/42",
        }
        self.views_by_repo = views_by_repo or {}
        self.error = error
        self.calls: list[tuple[int, str | None]] = []

    async def pr_view(self, pr: int | str, *, repo: str | None = None) -> dict[str, Any]:
        self.calls.append((int(pr), repo))
        if self.error is not None:
            raise self.error
        if repo is not None and repo in self.views_by_repo:
            return dict(self.views_by_repo[repo])
        return dict(self.view)


def _binding(
    *,
    github_repo: str = "org/repo",
    issue_label: str | None = "symphony",
    reconcile_enabled: bool = True,
    done_state: str = "Done",
    auto_merge: bool = True,
) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo=github_repo,
        issue_label=issue_label,
        reconcile_enabled=reconcile_enabled,
        auto_merge=auto_merge,
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval", done=done_state),
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


async def _seed_merge_wait(
    conn: aiosqlite.Connection,
    issue_id: str = "iss-1",
    *,
    issue_label: str = "symphony",
    github_repo: str = "org/repo",
) -> None:
    await _seed_run(conn, issue_id, f"run-{issue_id}")
    await db.operator_waits.upsert(
        conn,
        issue_id=issue_id,
        run_id=f"run-{issue_id}",
        kind=db.operator_waits.KIND_MERGE,
        linear_team_key="ENG",
        github_repo=github_repo,
        issue_label=issue_label,
        created_at="2026-05-17T10:01:00Z",
    )


async def _seed_pr(
    conn: aiosqlite.Connection,
    issue_id: str = "iss-1",
    *,
    github_repo: str = "org/repo",
    binding_key: str | None = None,
    pr_number: int = 42,
) -> None:
    stored_binding_key = (
        binding_key
        if binding_key is not None
        else f'["ENG","{github_repo}","symphony"]'
    )
    await db.issue_prs.upsert(
        conn,
        issue_id=issue_id,
        github_repo=github_repo,
        binding_key=stored_binding_key,
        pr_number=pr_number,
        pr_url=f"https://github.com/{github_repo}/pull/{pr_number}",
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


async def _merged_at(
    conn: aiosqlite.Connection,
    issue_id: str = "iss-1",
    github_repo: str = "org/repo",
) -> str | None:
    cur = await conn.execute(
        "SELECT merged_at FROM issue_prs WHERE issue_id = ? AND github_repo = ?",
        (issue_id, github_repo),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return str(row["merged_at"]) if row["merged_at"] is not None else None


async def _transition_rows(
    conn: aiosqlite.Connection,
    issue_id: str = "iss-1",
) -> list[tuple[str, str, str | None, str | None]]:
    transitions = await db.state_transitions.list_for_issue(conn, issue_id)
    return [
        (
            transition.table_name,
            transition.field,
            transition.old_value,
            transition.new_value,
        )
        for transition in transitions
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
async def test_linear_done_drift_uses_candidate_binding_done_state(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_merge_wait(conn, issue_label="backend")
        reconciler = Reconciler(
            Config(
                repos=[
                    _binding(issue_label="backend", done_state="Shipped"),
                    _binding(issue_label="frontend", done_state="Done"),
                ]
            ),
            conn,
            _FakeLinear(state_name="Done"),  # type: ignore[arg-type]
            _FakeGitHub(),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        rows = await _observation_rows(conn)
    finally:
        await conn.close()

    assert rows[0][0] == "linear"
    assert rows[0][1] is None


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
async def test_active_merge_zombie_clears_wait_and_marks_pr_merged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    conn = await db.connect(tmp_path / "state.sqlite")
    merged_at = "2026-05-17T11:59:00Z"
    try:
        await _seed_issue(conn)
        await _seed_merge_wait(conn)
        await _seed_pr(conn)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            _FakeGitHub(
                view={
                    "state": "MERGED",
                    "mergeable": "UNKNOWN",
                    "mergedAt": merged_at,
                    "url": "https://github.com/org/repo/pull/42",
                }
            ),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        rows = await _observation_rows(conn)
        wait = await db.operator_waits.get(conn, "iss-1")
        stored_merged_at = await _merged_at(conn)
        transitions = await _transition_rows(conn)
    finally:
        await conn.close()

    assert wait is None
    assert stored_merged_at == merged_at
    assert [(source, drift, action) for source, drift, action, _ in rows] == [
        ("linear", None, ACTION_OBSERVED),
        ("github", DRIFT_MERGE_ZOMBIE, ACTION_CLEARED),
    ]
    assert ("operator_waits", "kind", db.operator_waits.KIND_MERGE, None) in transitions
    assert ("issue_prs", "merged_at", None, merged_at) in transitions


@pytest.mark.asyncio
async def test_active_merge_wait_ignores_merged_pr_from_other_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_merge_wait(conn, github_repo="org/repo-a")
        await _seed_pr(conn, github_repo="org/repo-a", pr_number=41)
        await _seed_pr(conn, github_repo="org/repo-b", pr_number=42)
        reconciler = Reconciler(
            Config(
                repos=[
                    _binding(github_repo="org/repo-a"),
                    _binding(github_repo="org/repo-b"),
                ]
            ),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            _FakeGitHub(
                views_by_repo={
                    "org/repo-a": {
                        "state": "OPEN",
                        "mergeable": "MERGEABLE",
                        "mergedAt": None,
                        "url": "https://github.com/org/repo-a/pull/41",
                    },
                    "org/repo-b": {
                        "state": "MERGED",
                        "mergeable": "UNKNOWN",
                        "mergedAt": "2026-05-17T11:59:00Z",
                        "url": "https://github.com/org/repo-b/pull/42",
                    },
                }
            ),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        rows = await _observation_rows(conn)
        wait = await db.operator_waits.get(conn, "iss-1")
        repo_a_merged_at = await _merged_at(conn, github_repo="org/repo-a")
        cur = await conn.execute(
            "SELECT merged_at FROM issue_prs WHERE issue_id = ? AND github_repo = ?",
            ("iss-1", "org/repo-b"),
        )
        repo_b_row = await cur.fetchone()
    finally:
        await conn.close()

    assert wait is not None
    assert repo_a_merged_at is None
    assert repo_b_row is not None
    assert repo_b_row["merged_at"] is None
    assert rows[1][1] is None
    assert rows[1][2] == ACTION_OBSERVED


@pytest.mark.asyncio
async def test_active_pr_locally_merged_marks_pr_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    conn = await db.connect(tmp_path / "state.sqlite")
    merged_at = "2026-05-17T11:59:00Z"
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
                    "mergedAt": merged_at,
                    "url": "https://github.com/org/repo/pull/42",
                }
            ),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        rows = await _observation_rows(conn)
        stored_merged_at = await _merged_at(conn)
        transitions = await _transition_rows(conn)
    finally:
        await conn.close()

    assert stored_merged_at == merged_at
    assert rows[1][1] == DRIFT_PR_LOCALLY_MERGED
    assert rows[1][2] == ACTION_CLEARED
    assert ("issue_prs", "merged_at", None, merged_at) in transitions
    assert not any(row[0] == "operator_waits" and row[2] is not None for row in transitions)


@pytest.mark.asyncio
async def test_auto_merge_false_parked_pr_external_merge_moves_issue_to_done(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    conn = await db.connect(tmp_path / "state.sqlite")
    merged_at = "2026-05-17T11:59:00Z"
    cfg = Config(repos=[_binding(auto_merge=False)])
    try:
        await _seed_issue(conn)
        await _seed_pr(conn)
        parked = await db.issue_prs.mark_parked_for_manual_merge(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            parked_at="2026-05-17T11:00:00Z",
        )
        assert parked

        reconciler = Reconciler(
            cfg,
            conn,
            _FakeLinear(state_name="Needs Approval"),  # type: ignore[arg-type]
            _FakeGitHub(
                view={
                    "state": "MERGED",
                    "mergeable": "UNKNOWN",
                    "mergedAt": merged_at,
                    "url": "https://github.com/org/repo/pull/42",
                }
            ),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        assert await _merged_at(conn) == merged_at

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            return_value=LinearIssue(
                id="iss-1",
                identifier="ENG-1",
                title="Tracked issue",
                description="",
                url="https://linear.app/issue/ENG-1",
                state_id="state-na",
                state_name="Needs Approval",
                state_type="started",
                team_key="ENG",
                labels=["symphony"],
            )
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
            clock=lambda: NOW,
        )
        orch._states = {"ENG": {"Done": "state-done"}}  # noqa: SLF001

        assert await orch._reconcile_merged_issues_linear_state() == 1  # noqa: SLF001
    finally:
        await conn.close()

    linear.move_issue.assert_awaited_once_with("iss-1", "state-done")


@pytest.mark.asyncio
async def test_active_pr_closed_no_merge_clears_wait_without_marking_merged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_merge_wait(conn)
        await _seed_pr(conn)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            _FakeGitHub(
                view={
                    "state": "CLOSED",
                    "mergeable": None,
                    "merged": False,
                    "mergedAt": None,
                    "url": "https://github.com/org/repo/pull/42",
                }
            ),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        rows = await _observation_rows(conn)
        wait = await db.operator_waits.get(conn, "iss-1")
        pr = await db.issue_prs.get(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
        )
        transitions = await _transition_rows(conn)
    finally:
        await conn.close()

    assert wait is None
    assert pr is None
    assert rows[1][1] == DRIFT_PR_CLOSED_NO_MERGE
    assert rows[1][2] == ACTION_CLEARED
    assert ("issue_prs", "__row__", "org/repo#42", None) in transitions


@pytest.mark.asyncio
async def test_active_linear_done_notes_transition_without_clearing_wait(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_merge_wait(conn)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(state_name="Done"),  # type: ignore[arg-type]
            _FakeGitHub(),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        rows = await _observation_rows(conn)
        wait = await db.operator_waits.get(conn, "iss-1")
        transitions = await _transition_rows(conn)
    finally:
        await conn.close()

    assert wait is not None
    assert rows[0][1] == DRIFT_LINEAR_STATE_DONE
    assert rows[0][2] == ACTION_NOTED
    assert (
        "external_observations",
        "external_state_change",
        "linear",
        "linear:Done",
    ) in transitions


@pytest.mark.asyncio
async def test_active_clear_rolls_back_observation_and_wait_delete_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    conn = await db.connect(tmp_path / "state.sqlite")
    merged_at = "2026-05-17T11:59:00Z"

    async def fail_update_merged(*args: object, **kwargs: object) -> bool:
        raise RuntimeError("injected update failure")

    try:
        await _seed_issue(conn)
        await _seed_merge_wait(conn)
        await _seed_pr(conn)
        monkeypatch.setattr(db.issue_prs, "update_merged", fail_update_merged)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            _FakeGitHub(
                view={
                    "state": "MERGED",
                    "mergeable": "UNKNOWN",
                    "mergedAt": merged_at,
                    "url": "https://github.com/org/repo/pull/42",
                }
            ),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        with pytest.raises(RuntimeError, match="injected update failure"):
            await reconciler.reconcile_issue("iss-1", reason="test")
        wait = await db.operator_waits.get(conn, "iss-1")
        stored_merged_at = await _merged_at(conn)
        cur = await conn.execute("SELECT COUNT(*) AS count FROM external_observations")
        observation_count = (await cur.fetchone())["count"]
    finally:
        await conn.close()

    assert wait is not None
    assert stored_merged_at is None
    assert observation_count == 0


@pytest.mark.asyncio
async def test_autoclear_kill_switch_returns_to_observe_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    monkeypatch.setenv("SYMPHONY_RECONCILE_AUTOCLEAR_DISABLED", "1")
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
        stored_merged_at = await _merged_at(conn)
    finally:
        await conn.close()

    assert rows[1][1] == DRIFT_PR_LOCALLY_MERGED
    assert rows[1][2] == ACTION_OBSERVED
    assert stored_merged_at is None


@pytest.mark.asyncio
async def test_active_tick_honors_action_cap_and_logs_overflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        for idx in range(15):
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
            Config(
                repos=[_binding()],
                reconcile_max_per_tick=15,
                reconcile_max_actions_per_tick=10,
            ),
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

        with caplog.at_level(logging.WARNING, logger="symphony.orchestrator.reconciler"):
            assert await reconciler.tick() == 30
        cur = await conn.execute(
            "SELECT COUNT(*) AS count FROM issue_prs WHERE merged_at IS NOT NULL"
        )
        merged_count = (await cur.fetchone())["count"]
        cur = await conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM external_observations
            WHERE action_taken = ?
            """,
            (ACTION_CLEARED,),
        )
        cleared_count = (await cur.fetchone())["count"]
    finally:
        await conn.close()

    assert merged_count == 10
    assert cleared_count == 10
    assert "external reconciler action cap reached" in caplog.text
    assert "deferred_actions=5" in caplog.text


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
async def test_list_candidates_preaggregates_observation_history(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_merge_wait(conn)
        await db.external_observations.insert(
            conn,
            issue_id="iss-1",
            source="linear",
            observed_at="2026-05-17T10:05:00Z",
            payload_json="{}",
            drift_kind=None,
            action_taken=ACTION_OBSERVED,
        )
        await db.external_observations.insert(
            conn,
            issue_id="iss-1",
            source="github",
            observed_at="2026-05-17T10:10:00Z",
            payload_json="{}",
            drift_kind=None,
            action_taken=ACTION_OBSERVED,
        )
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            _FakeGitHub(),  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        candidates = await reconciler._list_candidates()  # noqa: SLF001
    finally:
        await conn.close()

    assert len(candidates) == 1
    assert candidates[0].issue_id == "iss-1"
    assert candidates[0].first_candidate_at == "2026-05-17T10:01:00Z"
    assert candidates[0].last_observed_at == "2026-05-17T10:10:00Z"


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
                        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
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
async def test_github_issue_comment_event_does_not_reconcile(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    fake_linear = _FakeLinear()
    fake_gh = _FakeGitHub()
    try:
        await _seed_issue(conn)
        await _seed_pr(conn)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            fake_linear,  # type: ignore[arg-type]
            fake_gh,  # type: ignore[arg-type]
            clock=lambda: NOW,
        )
        event = GitHubWebhookEvent(
            event_type="issue_comment",
            action="created",
            repo="ORG/REPO",
            delivery_id="delivery-1",
            pr_number=42,
            comment_id=123,
            comment_body="@codex review",
        )

        assert await reconciler.reconcile_github_event(event) == 0
        cur = await conn.execute("SELECT COUNT(*) AS count FROM external_observations")
        row = await cur.fetchone()
    finally:
        await conn.close()

    assert fake_linear.calls == 0
    assert fake_gh.calls == []
    assert row is not None
    assert row["count"] == 0


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
async def test_github_error_with_pr_number_500_does_not_backoff(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    fake_gh = _FakeGitHub(error=GitHubError("gh pr view 500 failed: not found"))
    try:
        await _seed_issue(conn)
        await _seed_pr(conn, pr_number=500)
        reconciler = Reconciler(
            Config(repos=[_binding()]),
            conn,
            _FakeLinear(),  # type: ignore[arg-type]
            fake_gh,  # type: ignore[arg-type]
            clock=lambda: NOW,
        )

        assert await reconciler.tick() == 2
        rows = await _observation_rows(conn)
    finally:
        await conn.close()

    assert fake_gh.calls == [(500, "org/repo")]
    assert rows[1][0] == "github"
    assert json.loads(rows[1][3])["prs"][0]["error"] == "gh pr view 500 failed: not found"


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
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_reconcile(event: GitHubWebhookEvent) -> int:
            started.set()
            await release.wait()
            return 2

        spy.reconcile_github_event = AsyncMock(side_effect=slow_reconcile)
        orch._reconciler = spy  # noqa: SLF001
        event = GitHubWebhookEvent(
            event_type="pull_request",
            action="closed",
            repo="org/repo",
            delivery_id="delivery-1",
            pr_number=42,
        )

        try:
            result = await asyncio.wait_for(orch.handle_github_webhook(event), timeout=1)
            await asyncio.wait_for(started.wait(), timeout=1)
            assert result.handled is True
            assert result.detail == "reconcile scheduled"
            release.set()
            await orch.drain_reconcile_event_tasks()
        finally:
            release.set()
            await orch.drain_reconcile_event_tasks(cancel=True)
    finally:
        await conn.close()

    spy.reconcile_github_event.assert_awaited_once_with(event)


@pytest.mark.asyncio
async def test_orchestrator_github_webhook_closed_unmerged_parked_pr_moves_done_once(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_issue(conn)
        await _seed_pr(conn)
        parked = await db.issue_prs.mark_parked_for_manual_merge(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            parked_at="2026-05-17T11:00:00Z",
        )
        assert parked
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            return_value=LinearIssue(
                id="iss-1",
                identifier="ENG-1",
                title="Tracked issue",
                description="",
                url="https://linear.app/issue/ENG-1",
                state_id="state-na",
                state_name="Needs Approval",
                state_type="started",
                team_key="ENG",
                labels=["symphony"],
            )
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "state": "CLOSED",
                "mergeable": None,
                "merged": False,
                "mergedAt": None,
                "url": "https://github.com/org/repo/pull/42",
            }
        )
        runner = MagicMock()
        runner.kill = AsyncMock()
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=Path("/dev/null"))
        workspace.release = MagicMock()
        orch = Orchestrator(
            Config(repos=[_binding(auto_merge=False)]),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
            clock=lambda: NOW,
        )
        orch._states = {"ENG": {"Done": "state-done"}}  # noqa: SLF001
        event = GitHubWebhookEvent(
            event_type="pull_request",
            action="closed",
            repo="ORG/REPO",
            delivery_id="delivery-1",
            pr_number=42,
            merged=False,
        )

        await orch.handle_github_webhook(event)
        await orch.drain_reconcile_event_tasks()
        await orch.handle_github_webhook(event)
        await orch.drain_reconcile_event_tasks()

        pr = await db.issue_prs.get(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
        )
    finally:
        await conn.close()

    linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
    linear.post_comment.assert_awaited_once_with(
        "iss-1", "🛑 PR closed without merge — marking done"
    )
    assert pr is None
