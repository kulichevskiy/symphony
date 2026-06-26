"""Scenario: a transient provider API error (e.g. a clean 500) on the implement
run is retried with capped exponential backoff (requeue-based — no workspace
slot held during the wait) and either recovers or, once the retry budget is
spent, escalates to the Needs Input lane via the existing operator-wait path.

Covers SYM-141:
  (a) repeated 500s retry to the limit then escalate to an implement-failed
      operator wait (Needs Input), surviving a daemon restart mid-retry; and
  (b) 500-then-success recovers and proceeds to merge.
  (c) local_review=True: implement succeeds, reviewer gets a transient 500 →
      re-dispatch short-circuits to the gates (no re-implement), reviewer
      approves on retry, PR merges.
  (d) local_review=True: repeated reviewer 500s exhaust the budget and escalate
      via the existing local-review-infra operator wait.
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


def _local_review_config(tmp_path: Path) -> Config:
    # local_review=True, remote_review=False → implement → in-workspace reviewer
    # → PR → CI → merge. Transient errors in the reviewer turn are retried with
    # backoff; the re-dispatch short-circuits to the gates (branch already ahead).
    return Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        repos=[
            RepoBinding(
                linear_team_key=TEAM,
                github_repo=REPO,
                local_review=True,
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
    # SYM-151: the limit + its readers live on `poll._base`, so patch there.
    monkeypatch.setattr(poll_module._base, "AGENT_INFRA_RETRY_LIMIT", 2)
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
        backoff_checked = False
        for _ in range(20):
            await harness.step()
            if not backoff_checked:
                # After the first requeue the issue is back in Ready but the
                # backoff window is active — a poll inside the window must NOT
                # start a new implement run. Verify before advancing the clock.
                runs_before = [
                    r
                    for r in await db.runs.history_for_issue(harness.conn, issue.id)
                    if r.stage == "implement"
                ]
                await harness.step()  # poll inside backoff window
                runs_after = [
                    r
                    for r in await db.runs.history_for_issue(harness.conn, issue.id)
                    if r.stage == "implement"
                ]
                assert len(runs_after) == len(runs_before), (
                    "backoff gate failed: a new implement run was dispatched "
                    "while still inside the backoff window"
                )
                backoff_checked = True
                # Durability: the retry count + backoff window are read back from
                # the runs table on a fresh daemon, not in-memory.
                await harness.restart()
            harness.advance(120)  # clear any backoff window before the next poll
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
            if r.stage == "implement" and r.termination_kind == db.runs.TRANSIENT_API_RETRY_KIND
        ]
        assert len(retried) == 2, f"expected 2 retried runs, got {len(retried)}"

        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_transient_api_error_then_success_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(poll_module._base, "AGENT_INFRA_RETRY_LIMIT", 5)
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
            if r.stage == "implement" and r.termination_kind == db.runs.TRANSIENT_API_RETRY_KIND
        ]
        assert len(retried) == 1, f"expected 1 retried run, got {len(retried)}"
        # Recovery means no escalation.
        assert await db.operator_waits.get(harness.conn, issue.id) is None

        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_local_review_transient_api_error_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reviewer 500 on local_review=True: re-dispatch short-circuits to the
    pre-push gates (does NOT re-run the implementer), reviewer approves on
    retry, PR merges. Tests the LOCAL_REVIEW_TRANSIENT_RETRY_KIND path and
    the resume_after_local_review branch of the dispatch logic."""
    monkeypatch.setattr(poll_module._base, "AGENT_INFRA_RETRY_LIMIT", 5)
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=_local_review_config(tmp_path), clock=clock)
    try:
        issue = harness.sim.seed_issue(
            identifier="ENG-3", team_key=TEAM, state_name=READY, title="local review flaky"
        )
        await harness.warmup()
        # Implement uses the default stream (HEAD advances, SYMPHONY_DONE).
        # The reviewer inner loop has REVIEWER_FAILURE_RETRIES=1, so each
        # dispatch calls the reviewer up to 2 times before giving up.  Both
        # internal attempts must hit the transient error to exhaust the inner
        # retry and surface REVIEWER_FAILED with api_error.transient=True.
        # stage="local_review" targets the stage-specific queue so the implement
        # stage (which runs first) uses the default stream and is not affected.
        # dispatch 1, attempt 0
        harness.runner.enqueue_transient_api_error(status=500, stage="local_review")
        # dispatch 1, attempt 1
        harness.runner.enqueue_transient_api_error(status=500, stage="local_review")
        harness.runner.enqueue_local_review_approved()  # dispatch 2, attempt 0

        for _ in range(40):
            await harness.step()
            harness.advance(120)
            if harness.sim.issues[issue.id].state_type == "completed":
                break

        sim_pr = harness.sim.github.pr_for_issue(issue.id)
        assert sim_pr is not None, "no PR opened after local-review recovery"
        assert sim_pr.state == PR_MERGED, f"PR not merged: {sim_pr.state}"
        assert harness.sim.issues[issue.id].state_name == DONE

        runs = await db.runs.history_for_issue(harness.conn, issue.id)
        # Exactly one local-review transient retry run (the failed reviewer turn).
        local_review_retried = [
            r
            for r in runs
            if r.stage == "implement"
            and r.termination_kind == db.runs.LOCAL_REVIEW_TRANSIENT_RETRY_KIND
        ]
        assert len(local_review_retried) == 1, (
            f"expected 1 local-review-transient retried run, got {len(local_review_retried)}"
        )
        # The implementer must NOT have been re-run — only 2 implement runs total
        # (the original plus the short-circuit retry that went straight to gates).
        implement_runs = [r for r in runs if r.stage == "implement"]
        assert len(implement_runs) == 2, (
            f"expected 2 implement runs (original + short-circuit retry), got {len(implement_runs)}"
        )
        # No escalation.
        assert await db.operator_waits.get(harness.conn, issue.id) is None

        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_local_review_transient_api_error_escalates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repeated reviewer 500s exhaust the shared retry budget and escalate via
    the existing local-review-infra operator wait (KIND_IMPLEMENT_FAILED),
    exactly as today — no change to the escalation path itself."""
    monkeypatch.setattr(poll_module._base, "AGENT_INFRA_RETRY_LIMIT", 2)
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=_local_review_config(tmp_path), clock=clock)
    try:
        issue = harness.sim.seed_issue(
            identifier="ENG-4", team_key=TEAM, state_name=READY, title="local review always fails"
        )
        await harness.warmup()
        # Implement uses the default stream; limit=2 → 2 requeues then escalate
        # on the 3rd dispatch's local-review 500.  Each dispatch exhausts both
        # internal reviewer retries (REVIEWER_FAILURE_RETRIES=1 → 2 attempts).
        # stage="local_review" ensures implement uses its default stream.
        for _ in range(6):
            harness.runner.enqueue_transient_api_error(status=500, stage="local_review")

        escalated = False
        for _ in range(30):
            await harness.step()
            harness.advance(120)
            wait = await db.operator_waits.get(harness.conn, issue.id)
            if wait is not None and wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED:
                escalated = True
                break
        assert escalated, "issue never escalated after repeated local-review 500s"

        # No PR — every attempt died before push.
        assert harness.sim.github.pr_for_issue(issue.id) is None

        runs = await db.runs.history_for_issue(harness.conn, issue.id)
        local_review_retried = [
            r
            for r in runs
            if r.stage == "implement"
            and r.termination_kind == db.runs.LOCAL_REVIEW_TRANSIENT_RETRY_KIND
        ]
        assert len(local_review_retried) == 2, (
            f"expected 2 local-review-transient retried runs, got {len(local_review_retried)}"
        )
        # No plain TRANSIENT_API_RETRY_KIND runs — the implement itself never failed.
        impl_transient = [
            r
            for r in runs
            if r.stage == "implement" and r.termination_kind == db.runs.TRANSIENT_API_RETRY_KIND
        ]
        assert len(impl_transient) == 0, (
            f"unexpected implement-transient runs (implementer should not have failed): "
            f"{impl_transient}"
        )

        await harness.assert_consistent()
    finally:
        await harness.close()
