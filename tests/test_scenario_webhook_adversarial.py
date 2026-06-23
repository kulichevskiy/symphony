"""Adversarial webhook delivery: dropped / duplicated / out-of-order.

The three real-world webhook failure modes that drive recurring drift bugs,
exercised against the slice-4 rig. Each ends on convergence + assert_consistent.

* Dropped — the merge webhook is enqueued but NEVER delivered. The poll path's
  reconciler tick must self-heal: observe the externally-merged PR, mark it
  merged locally, and re-home the issue into Done.
* Duplicated — the same merge webhook is delivered twice. Handling is
  idempotent: no double-processing, no duplicate dispatch, no duplicate
  issue_prs ownership row.
* Out-of-order — a later poll already observed the merge and converged the
  issue to Done; the stale merge webhook then arrives. Late delivery must not
  corrupt the converged state.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.db.runs import LIVE_STATUSES
from tests.harness import Harness, ManualClock

TEAM = "ENG"
REPO = "org/repo"
READY = "Todo"
CODE_REVIEW = "Needs Approval"
DONE = "Done"


def _config(tmp_path: Path) -> Config:
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
                    in_progress="In Progress",
                    code_review=CODE_REVIEW,
                    done=DONE,
                ),
            )
        ],
    )


async def _seed_parked_pr(harness: Harness):
    """Seed an issue parked in code-review with an open, tracked PR.

    Shared start state for all three scenarios: the issue sits in the
    code-review lane with an open PR recorded in issue_prs (locally unmerged).
    """
    issue = harness.sim.seed_issue(
        identifier="ENG-1",
        team_key=TEAM,
        state_name=CODE_REVIEW,
        title="adversarial webhook",
    )
    await db.issues.upsert(
        harness.conn,
        id=issue.id,
        identifier=issue.identifier,
        title=issue.title,
        team_key=TEAM,
    )
    pr_url = await harness.github.ensure_pr(
        title=issue.title,
        body="",
        head="symphony/eng-1",
        repo=REPO,
        linear_url=issue.url,
    )
    sim_pr = harness.sim.github.pr_for_issue(issue.id)
    assert sim_pr is not None
    await db.issue_prs.upsert(
        harness.conn,
        issue_id=issue.id,
        github_repo=REPO,
        pr_number=sim_pr.number,
        pr_url=pr_url,
        created_at=harness.sim.now_iso(),
    )
    return issue, sim_pr


async def _step_until_done(harness: Harness, issue_id: str) -> None:
    for _ in range(20):
        await harness.step()
        harness.advance(60)
        if harness.sim.issues[issue_id].state_type == "completed":
            break


async def _assert_converged(harness: Harness, issue_id: str) -> None:
    """Issue in Done lane, PR merged in DB, no live runs, no drift."""
    assert harness.sim.issues[issue_id].state_name == DONE
    assert harness.sim.issues[issue_id].state_type == "completed"
    runs = await db.runs.history_for_issue(harness.conn, issue_id)
    live = [r.id for r in runs if r.status in LIVE_STATUSES]
    assert live == [], f"live runs remain: {live}"
    await harness.assert_consistent()


@pytest.mark.asyncio
async def test_dropped_webhook_poll_self_heals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=clock)
    try:
        issue, sim_pr = await _seed_parked_pr(harness)

        # The PR merges out-of-band. merge_pr ENQUEUES the webhook — but the
        # delivery is dropped: we never call deliver_github_webhook().
        harness.sim.merge_pr(sim_pr.number, repo=REPO)
        assert sim_pr.merged
        assert len(harness.sim.github_webhooks) == 1

        # Poll path self-heals: the reconciler tick observes the merged PR,
        # writes merged_at, and the poll loop re-homes the issue to Done.
        await _step_until_done(harness, issue.id)

        # The dropped webhook is still sitting undelivered in the queue.
        assert len(harness.sim.github_webhooks) == 1

        db_pr = await db.issue_prs.get(
            harness.conn, issue_id=issue.id, github_repo=REPO
        )
        assert db_pr is not None and db_pr.merged_at is not None
        await _assert_converged(harness, issue.id)
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_duplicated_webhook_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=clock)
    try:
        issue, sim_pr = await _seed_parked_pr(harness)

        harness.sim.merge_pr(sim_pr.number, repo=REPO)
        # Deliver the SAME merge webhook twice (provider redelivery). The handler
        # bypasses the delivery-dedupe gate, so idempotency must come from the
        # reconcile being a no-op the second time.
        event = harness.sim.github_webhooks[0]
        first = await harness.orch.handle_github_webhook(event)
        assert first.handled
        await harness.drain()
        second = await harness.orch.handle_github_webhook(event)
        assert second.handled
        await harness.drain()

        # No double-processing: exactly one issue_prs ownership row, marked
        # merged once.
        cur = await harness.conn.execute(
            "SELECT COUNT(*) AS n FROM issue_prs WHERE github_repo=? AND pr_number=?",
            (REPO, sim_pr.number),
        )
        row = await cur.fetchone()
        assert row["n"] == 1, "duplicate issue_prs ownership row"

        await _step_until_done(harness, issue.id)

        # No duplicate dispatch: at most one run per stage, none live.
        runs = await db.runs.history_for_issue(harness.conn, issue.id)
        by_stage: dict[str, int] = {}
        for r in runs:
            by_stage[r.stage] = by_stage.get(r.stage, 0) + 1
        assert all(n == 1 for n in by_stage.values()), f"duplicate runs: {by_stage}"

        await _assert_converged(harness, issue.id)
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_out_of_order_webhook_does_not_corrupt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=clock)
    try:
        issue, sim_pr = await _seed_parked_pr(harness)

        # The PR merges out-of-band, enqueueing the webhook.
        harness.sim.merge_pr(sim_pr.number, repo=REPO)
        assert len(harness.sim.github_webhooks) == 1

        # A later poll observes reality FIRST and converges the issue to Done,
        # before the webhook is ever delivered.
        await _step_until_done(harness, issue.id)
        assert harness.sim.issues[issue.id].state_type == "completed"
        await _assert_converged(harness, issue.id)

        # The stale webhook finally arrives, out of order. Late delivery must
        # be a no-op against the already-converged state.
        result = await harness.deliver_github_webhook()
        assert result.handled
        await harness.drain()
        assert harness.sim.github_webhooks == []

        await _assert_converged(harness, issue.id)
    finally:
        await harness.close()
