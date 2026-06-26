"""Flagship drift scenario: PR merged during restart (webhooks + drain).

Bug improvement #3: a PR merged at the exact moment Symphony is restarting must
still converge ("merged without noticing"). Symphony is "down" with an in-flight
merge run and an open, tracked PR. The PR is merged out-of-band — `sim.merge_pr()`
ENQUEUES the GitHub merge webhook but does not deliver it. After `restart()` the
orphaned merge run is retired; then the explicitly-delivered merge webhook +
`drain()` mark the PR merged locally, and the poll loop re-homes the issue into
the Done lane — with no zombie run and no drift.
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


@pytest.mark.asyncio
async def test_pr_merged_during_restart_converges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=clock)
    try:
        # Symphony is mid-merge and about to be restarted: an issue parked in
        # the code-review lane with an open, tracked PR and a running merge run
        # whose pid the Sim reports alive (a healthy worker, pre-restart).
        issue = harness.sim.seed_issue(
            identifier="ENG-1",
            team_key=TEAM,
            state_name=CODE_REVIEW,
            title="merged during restart",
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
        await db.runs.create(
            harness.conn,
            id="merge-1",
            issue_id=issue.id,
            stage="merge",
            status="running",
            pid=20001,
            started_at=harness.sim.now_iso(),
        )
        harness.sim.register_pid(20001)

        # The PR merges out-of-band while Symphony is "down". This ENQUEUES the
        # GitHub merge webhook but delivers nothing — Symphony hasn't noticed.
        harness.sim.merge_pr(sim_pr.number, repo=REPO)
        assert sim_pr.merged
        assert len(harness.sim.github_webhooks) == 1

        # Host restart: the merge run's pid is now dead, so startup reconcile
        # retires the orphan. The PR is still locally unmerged in the DB.
        await harness.restart()
        retired = [
            r for r in await db.runs.history_for_issue(harness.conn, issue.id) if r.id == "merge-1"
        ]
        assert retired and retired[0].status not in LIVE_STATUSES
        db_pr = await db.issue_prs.get(harness.conn, issue_id=issue.id, github_repo=REPO)
        assert db_pr is not None and db_pr.merged_at is None

        # Enable active reconcile so the reconciler writes merged_at to the DB
        # (observe-only mode is the default; active auto-clear requires this).
        monkeypatch.setenv("SYMPHONY_RECONCILE_DRYRUN", "0")

        # Deliver the queued merge webhook, then drain the fire-and-forget
        # reconcile tasks it scheduled. This marks the PR merged in the DB.
        result = await harness.deliver_github_webhook()
        assert result.handled
        await harness.drain()
        assert harness.sim.github_webhooks == []
        db_pr = await db.issue_prs.get(harness.conn, issue_id=issue.id, github_repo=REPO)
        assert db_pr is not None and db_pr.merged_at is not None

        # Step the poll loop: the merged-state reconcile re-homes the issue into
        # the Done lane.
        for _ in range(20):
            await harness.step()
            harness.advance(60)
            if harness.sim.issues[issue.id].state_type == "completed":
                break

        # Converged: issue in the merged/Done lane, no zombie runs.
        assert harness.sim.issues[issue.id].state_name == DONE
        assert harness.sim.issues[issue.id].state_type == "completed"
        runs = await db.runs.history_for_issue(harness.conn, issue.id)
        live = [r.id for r in runs if r.status in LIVE_STATUSES]
        assert live == [], f"live runs remain: {live}"

        await harness.assert_consistent()
    finally:
        await harness.close()
