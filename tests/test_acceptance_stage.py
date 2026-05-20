"""Orchestrator wiring for the Acceptance stage."""

from __future__ import annotations

import asyncio
import json
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
from symphony.pipeline.acceptance_classifier import (
    ACCEPTANCE_FOOTER_PASS,
    ACCEPTANCE_FOOTER_REJECT,
)


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent] | list[list[RunnerEvent]]) -> None:
        if events and isinstance(events[0], RunnerEvent):
            self.scripts = [events]  # type: ignore[list-item]
        else:
            self.scripts = events  # type: ignore[assignment]
        self.captured_specs: list[RunnerSpec] = []

    @property
    def captured_spec(self) -> RunnerSpec | None:
        if not self.captured_specs:
            return None
        return self.captured_specs[-1]

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_specs.append(spec)
        index = min(len(self.captured_specs) - 1, len(self.scripts) - 1)
        return self._aiter(self.scripts[index])

    async def _aiter(self, events: list[RunnerEvent]) -> AsyncIterator[RunnerEvent]:
        for ev in events:
            yield ev

    async def kill(self, _run_id: str) -> None:
        return None


def _acceptance_events(
    verdict_footer: str = ACCEPTANCE_FOOTER_PASS,
    *,
    cost: float = 0.0,
) -> list[RunnerEvent]:
    return [
        RunnerEvent(kind="started", pid=2222),
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "result",
                    "subtype": "success",
                    "result": f"Acceptance verdict.\n\n{verdict_footer}",
                    "total_cost_usd": cost,
                    "usage": {"input_tokens": 100, "output_tokens": 20},
                }
            ),
        ),
        RunnerEvent(kind="exit", returncode=0),
    ]


def _merge_events() -> list[RunnerEvent]:
    return [RunnerEvent(kind="exit", returncode=0)]


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
    gh.pr_diff = AsyncMock(
        return_value="diff --git a/auth.py b/auth.py\n+enable_oauth()\n"
    )
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
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
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
        assert acceptance.pr_head_sha == "abc123"
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
        assert [spec.stage for spec in runner.captured_specs] == [
            "acceptance",
            "merge",
        ]
        acceptance_prompt = runner.captured_specs[0].command[-1]
        assert "Need OAuth." in acceptance_prompt
        assert "diff --git a/auth.py b/auth.py" in acceptance_prompt
        assert "mode: code_only" in acceptance_prompt
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-acceptance"),
            call("iss-1", "state-done"),
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_pass_must_match_current_pr_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        await db.acceptance_state.begin_acceptance(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="old123",
            mode="code_only",
            preview_url="",
            extracted_criteria="[]",
        )
        await db.acceptance_state.record_verdict(
            conn,
            "iss-1",
            verdict="pass",
            artifacts_url="",
        )
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
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
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.pr_head_sha == "abc123"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "merge",
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_runs_before_conflict_fix_ready_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        marked = await db.issue_prs.mark_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            head_sha="abc123",
            marked_at="2026-05-10T00:03:00+00:00",
        )
        assert marked is True
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()
        gh.pr_reviews = AsyncMock(return_value=[])

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
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.pr_head_sha == "abc123"
        assert acceptance.last_verdict == "pass"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "merge",
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_reject_posts_parseable_comment_and_does_not_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(_acceptance_events(ACCEPTANCE_FOOTER_REJECT, cost=0.11))
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()

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
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.last_verdict == "reject"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
        ]
        assert history[2].status == "failed"
        assert history[2].cost_usd == pytest.approx(0.11)
        gh.pr_merge.assert_not_awaited()

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Acceptance verdict" in body for body in bodies)
        assert any(ACCEPTANCE_FOOTER_REJECT in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_cost_counts_toward_issue_warning_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        await db.runs.add_cost(conn, "implement", 70.0)
        runner = _FakeRunner([_acceptance_events(cost=10.0), _merge_events()])
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
                cost_cap_per_issue_usd=100.0,
                cost_warning_pct=75,
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
        await orch.drain_dispatch_tasks()

        assert await db.runs.cost_for_issue(conn, "iss-1") == pytest.approx(80.0)
        assert await db.cost_marks.warning_posted_at(conn, "iss-1") is not None
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Cost notice" in body for body in bodies)
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
        runner = _FakeRunner(_merge_events())
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
