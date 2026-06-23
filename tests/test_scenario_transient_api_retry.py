"""Scenario: a transient provider API error (e.g. a clean 500) on the implement
run is retried with capped exponential backoff (requeue-based — no workspace
slot held during the wait) and either recovers or, once the retry budget is
spent, escalates to the Needs Input lane via the existing operator-wait path.

Covers SYM-141:
  (a) repeated 500s retry to the limit then escalate to an implement-failed
      operator wait (Needs Input), surviving a daemon restart mid-retry; and
  (b) 500-then-success recovers and proceeds to merge.
Both end with `assert_consistent`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.orchestrator import poll as poll_module
from tests.harness import Harness, ManualClock
from tests.harness.sim import PR_MERGED

TEAM = "ENG"
REPO = "org/repo"
READY = "Todo"
IN_PROGRESS = "In Progress"
NEEDS_APPROVAL = "Needs Approval"
DONE = "Done"


def _config(tmp_path: Path) -> Config:
    # local_review/remote_review off → implement → PR → CI → merge, so a
    # transient error surfaces through the implement completion gate.
    return Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        repos=[
            RepoBinding(
                linear_team_key=TEAM,
                github_repo=REPO,
                local_review=False,
                remote_review=False,
                linear_states=LinearStates(
                    ready=READY,
                    in_progress=IN_PROGRESS,
                    code_review=NEEDS_APPROVAL,
                    needs_approval=NEEDS_APPROVAL,
                    done=DONE,
                ),
            )
        ],
    )


@pytest.mark.asyncio
async def test_transient_api_error_retries_then_escalates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Keep the test tight: 2 retries then escalate on the 3rd failure.
    monkeypatch.setattr(poll_module, "AGENT_INFRA_RETRY_LIMIT", 2)
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=clock)
    try:
        issue = harness.sim.seed_issue(
            identifier="ENG-1", team_key=TEAM, state_name=READY, title="flaky api"
        )
        await harness.warmup()
        # Every dispatch hits a clean 500: limit=2 → 2 requeues then escalate.
        for _ in range(3):
            harness.runner.enqueue_transient_api_error(status=500)

        escalated = False
        for i in range(20):
            await harness.step()
            harness.advance(120)  # clear any backoff window before the next poll
            if i == 0:
                # Durability: the retry count + backoff window are read back from
                # the runs table on a fresh daemon, not in-memory.
                await harness.restart()
            wait = await db.operator_waits.get(harness.conn, issue.id)
            if wait is not None and wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED:
                escalated = True
                break
        assert escalated, "issue never escalated to an implement-failed operator wait"

        # Escalated to the human-input lane via the existing path.
        assert harness.sim.issues[issue.id].state_name == NEEDS_APPROVAL
        # No PR was ever opened — every attempt died before publish.
        assert harness.sim.github.pr_for_issue(issue.id) is None
        # The earlier implement runs carry the durable transient-retry marker;
        # the final (escalating) run does not.
        runs = await db.runs.history_for_issue(harness.conn, issue.id)
        retried = [
            r
            for r in runs
            if r.stage == "implement"
            and r.termination_kind == db.runs.TRANSIENT_API_RETRY_KIND
        ]
        assert len(retried) == 2, f"expected 2 retried runs, got {len(retried)}"

        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_transient_api_error_then_success_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(poll_module, "AGENT_INFRA_RETRY_LIMIT", 5)
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=clock)
    try:
        issue = harness.sim.seed_issue(
            identifier="ENG-2", team_key=TEAM, state_name=READY, title="flaky then ok"
        )
        await harness.warmup()
        # First dispatch 500s; the retry falls through to the default success
        # stream → recover and proceed all the way to merge.
        harness.runner.enqueue_transient_api_error(status=500)

        for _ in range(40):
            await harness.step()
            harness.advance(120)
            if harness.sim.issues[issue.id].state_type == "completed":
                break

        sim_pr = harness.sim.github.pr_for_issue(issue.id)
        assert sim_pr is not None, "no PR opened after recovery"
        assert sim_pr.state == PR_MERGED, f"PR not merged: {sim_pr.state}"
        assert harness.sim.issues[issue.id].state_name == DONE
        assert harness.sim.issues[issue.id].state_type == "completed"

        # Exactly one transient-retry implement run preceded the successful one.
        runs = await db.runs.history_for_issue(harness.conn, issue.id)
        retried = [
            r
            for r in runs
            if r.stage == "implement"
            and r.termination_kind == db.runs.TRANSIENT_API_RETRY_KIND
        ]
        assert len(retried) == 1, f"expected 1 retried run, got {len(retried)}"
        # Recovery means no escalation.
        assert await db.operator_waits.get(harness.conn, issue.id) is None

        await harness.assert_consistent()
    finally:
        await harness.close()
