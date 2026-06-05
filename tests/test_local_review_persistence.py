"""Local-review `runs` row + cost persistence + status mapping."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import (
    Orchestrator,
    _local_review_status_from_result,
)
from symphony.pipeline.local_review import VERDICT_APPROVED_MARKER
from symphony.pipeline.local_review_loop import LoopOutcome, LoopResult

# --- pure status mapper -----------------------------------------------


def test_status_mapper_completed_for_approved() -> None:
    r = LoopResult(outcome=LoopOutcome.APPROVED, iterations=1, verdicts=())
    assert _local_review_status_from_result(r) == "completed"


def test_status_mapper_failed_for_non_approval_outcomes() -> None:
    for outcome in (
        LoopOutcome.EXHAUSTED,
        LoopOutcome.STUCK_LOOP,
        LoopOutcome.REVIEWER_FAILED,
        LoopOutcome.FIX_RUN_FAILED,
        LoopOutcome.COST_CAP_BREACHED,
    ):
        r = LoopResult(outcome=outcome, iterations=1, verdicts=())
        assert _local_review_status_from_result(r) == "failed", outcome


def test_status_mapper_none_result_is_failed() -> None:
    """Uncaught session exception → no LoopResult; row marked failed."""
    assert _local_review_status_from_result(None) == "failed"


# --- end-to-end: runs row persisted with cost + status ----------------


class _StagedRunner:
    def __init__(self, scripts: dict[str, list[list[RunnerEvent]]]) -> None:
        self._scripts = {k: list(v) for k, v in scripts.items()}
        self.captured: list[RunnerSpec] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured.append(spec)
        bucket = self._scripts.get(spec.stage)
        if not bucket:
            raise AssertionError(f"unexpected stage {spec.stage!r}")
        events = bucket.pop(0)

        async def gen() -> AsyncIterator[RunnerEvent]:
            for ev in events:
                yield ev

        return gen()

    async def kill(self, run_id: str) -> None:
        pass


def _local_binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        review_strategy="local",
        reviewer_agent="codex",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add authentication",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["feature"],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


async def _scan_and_wait(orch: Orchestrator, binding: RepoBinding) -> None:
    tasks = await orch._scan_binding(binding)  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_local_review_run_row_persisted_with_cost_and_completed_status(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_local_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()

        # Implement run reports $0.42; reviewer reports $0.18 via the
        # claude `result` event format.
        implement_result = RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "result",
                    "subtype": "success",
                    "total_cost_usd": 0.42,
                    "usage": {"input_tokens": 100, "output_tokens": 50},
                }
            ),
        )
        # The reviewer is codex; codex emits token-only usage. Use the
        # turn.completed shape; UsageCostEstimator prices the deltas.
        reviewer_msg = RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "i",
                        "type": "agent_message",
                        "text": f"clean\n{VERDICT_APPROVED_MARKER}",
                    },
                }
            ),
        )
        reviewer_usage = RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "turn.completed",
                    "usage": {
                        "input_tokens": 100_000,
                        "output_tokens": 10_000,
                        "cached_input_tokens": 20_000,
                    },
                }
            ),
        )
        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        implement_result,
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
                "local_review": [
                    [
                        reviewer_msg,
                        reviewer_usage,
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # Runs history should contain implement + local_review + review.
        history = await db.runs.history_for_issue(conn, "iss-1")
        stages = sorted({h.stage for h in history})
        assert "implement" in stages
        assert "local_review" in stages
        assert "review" in stages

        # The local_review row finalized with status='completed' and
        # carries the priced cost.
        lr_rows = [h for h in history if h.stage == "local_review"]
        assert len(lr_rows) == 1
        lr = lr_rows[0]
        assert lr.status == "completed"
        # 80k input @ $1.25/M + 20k cached input @ $0.125/M +
        # 10k output @ $10/M = $0.10 + $0.0025 + $0.10 = $0.2025
        assert lr.cost_usd == pytest.approx(0.2025, rel=1e-6)
        assert lr.input_tokens == 100_000
        assert lr.output_tokens == 10_000
        assert lr.cache_write_tokens == 0
        assert lr.cache_read_tokens == 20_000
        assert lr.ended_at is not None

        # cost_for_issue now sums implement + local_review.
        total = await db.runs.cost_for_issue(conn, "iss-1")
        assert total == pytest.approx(0.42 + 0.2025, rel=1e-6)
    finally:
        await conn.close()
