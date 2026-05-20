"""Orchestrator wiring for the stub Acceptance stage."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import AcceptanceConfig, Config, LinearStates, RepoBinding
from symphony.github.client import CheckRun, PRChecks
from symphony.linear.client import LinearIssue
from symphony.orchestrator import poll as poll_module
from symphony.orchestrator.poll import Orchestrator, _binding_storage_key


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, _run_id: str) -> None:
        return None


def _binding(mode: str = "off") -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        acceptance=AcceptanceConfig(mode=mode),  # type: ignore[arg-type]
        linear_states=LinearStates(
            ready="Todo",
            in_progress="In Progress",
            needs_approval="Needs Approval",
            in_acceptance="In Acceptance",
            blocked="Blocked",
            done="Done",
        ),
    )


def _issue(state_name: str = "Needs Approval", state_id: str = "state-na") -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id=state_id,
        state_name=state_name,
        state_type="started",
        team_key="ENG",
        labels=["feature"],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Needs Approval": "state-na",
        "In Acceptance": "state-acceptance",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


async def _seed_review_candidate(conn, binding: RepoBinding) -> None:  # type: ignore[no-untyped-def]
    await db.issues.upsert(
        conn,
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        team_key="ENG",
    )
    await db.runs.create(
        conn,
        id="implement",
        issue_id="iss-1",
        stage="implement",
        status="completed",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )
    await db.runs.create(
        conn,
        id="review",
        issue_id="iss-1",
        stage="review",
        status="running",
        pid=None,
        started_at="2026-05-10T00:01:00+00:00",
    )
    await db.issue_prs.upsert(
        conn,
        issue_id="iss-1",
        github_repo="org/repo",
        binding_key=_binding_storage_key(binding),
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        created_at="2026-05-10T00:01:00+00:00",
    )


def _github() -> MagicMock:
    gh = MagicMock()
    gh.pr_view = AsyncMock(
        side_effect=[
            {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
            {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
            {
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergedAt": "2026-05-10T00:04:00Z",
            },
        ]
    )
    gh.pr_checks = AsyncMock(
        return_value=PRChecks([CheckRun(name="test", state="SUCCESS", bucket="pass")])
    )
    gh.pr_review_comments = AsyncMock(return_value=[])
    gh.pr_reviews = AsyncMock(
        return_value=[
            {
                "user": {"login": "reviewer"},
                "state": "APPROVED",
                "commit_id": "abc123",
                "submitted_at": "2026-05-10T00:03:00Z",
                "body": "",
            }
        ]
    )
    gh.pr_reactions = AsyncMock(return_value=[])
    gh.pr_issue_comments = AsyncMock(return_value=[])
    gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
    gh.pr_merge = AsyncMock()
    return gh


@pytest.mark.asyncio
async def test_acceptance_mode_runs_stub_between_review_and_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        push_fn = AsyncMock()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=_github(),
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.pr_number == 42
        assert acceptance.pr_url == "https://github.com/org/repo/pull/42"
        assert acceptance.mode == "code_only"
        assert acceptance.last_verdict == "pass"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "merge",
        ]
        assert history[2].status == "completed"
        assert history[3].status == "done"
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "merge"
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-acceptance"),
            call("iss-1", "state-done"),
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_off_preserves_current_review_to_merge_flow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("off")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=_github(),
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review", "merge"]
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert linear.move_issue.await_args_list == [call("iss-1", "state-done")]
        cur = await conn.execute("SELECT COUNT(*) FROM acceptance_state")
        assert (await cur.fetchone())[0] == 0
    finally:
        await conn.close()
