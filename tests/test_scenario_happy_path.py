"""Happy-path scenario (poll-only): dispatch → PR → checks → merge → Done.

The first end-to-end walk through the rig (tests/harness/), proving the
pipeline walks under pure polling — no restart, no webhooks. The fake runner
emits `started`+PID then a `SYMPHONY_DONE` marker; Symphony opens the PR via
`FakeGitHub.ensure_pr`; the sim-aware push records the branch→head SHA in the
`Sim` and the orchestrator reads it back off `pr_view().headRefOid`; CI is
green in the `Sim`, so the review-bypass merge fires and the issue lands in the
Done lane.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.db.runs import LIVE_STATUSES
from tests.harness import Harness, ManualClock
from tests.harness.sim import PR_MERGED

TEAM = "ENG"
REPO = "org/repo"
READY = "Todo"
DONE = "Done"


def _config(tmp_path: Path) -> Config:
    # remote_review/local_review both off → implement → PR → CI → merge, the
    # no-review happy path the scenario exercises.
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
                    code_review="Needs Approval",
                    done=DONE,
                ),
            )
        ],
    )


@pytest.mark.asyncio
async def test_happy_path_dispatch_to_merge(tmp_path: Path) -> None:
    clock = ManualClock()
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=clock)
    try:
        issue = harness.sim.seed_issue(
            identifier="ENG-1", team_key=TEAM, state_name=READY, title="walk the pipe"
        )

        await harness.warmup()

        # Step the poll loop to quiescence: each step advances the clock past
        # any review/merge cooldown so the next poll acts.
        for _ in range(40):
            await harness.step()
            harness.advance(60)
            sim_issue = harness.sim.issues[issue.id]
            if sim_issue.state_type == "completed":
                break

        # Final state: PR opened in the Sim and merged.
        sim_pr = harness.sim.github.pr_for_issue(issue.id)
        assert sim_pr is not None, "no PR was opened in the Sim"
        assert sim_pr.state == PR_MERGED, f"PR not merged: {sim_pr.state}"
        assert sim_pr.head_sha, "PR head SHA was never recorded by the sim-aware push"
        # The merged head is the synthetic SHA the sim-aware push recorded for
        # the branch — not ensure_pr's fabricated fallback.
        assert harness.sim.branch_head_shas.get((sim_pr.repo, sim_pr.head)) == sim_pr.head_sha, (
            "PR head SHA did not come from the recorded branch→head push"
        )

        # Issue landed in the Done/merged lane.
        assert harness.sim.issues[issue.id].state_name == DONE
        assert harness.sim.issues[issue.id].state_type == "completed"

        # Run statuses are terminal — no live runs linger.
        runs = await db.runs.history_for_issue(harness.conn, issue.id)
        live = [r.id for r in runs if r.status in LIVE_STATUSES]
        assert live == [], f"live runs remain: {live}"

        # The implement run recorded the runner's PID.
        implement = [r for r in runs if r.stage == "implement"]
        assert implement and implement[0].pid is not None

        await harness.assert_consistent()
    finally:
        await harness.close()
