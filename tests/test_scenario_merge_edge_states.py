"""Merge/check edge-state scenarios: drive the poll loop through the real
`gh pr view` states the merge gate must handle, asserting it never merges a PR
GitHub would reject and re-polls (rather than acting) when mergeability is
unknown.

Each scenario walks the proven happy-path pipeline (dispatch → implement → PR),
then — the instant the PR opens, before the merge gate fires — stamps the
`SimPR` with the edge state it models (BEHIND / UNSTABLE / DIRTY / UNKNOWN /
draft, each backed by a recorded `tests/fixtures/contract/github_pr_view_*.json`
golden via the contract tests). Continued stepping then exercises the merge/poll
decision against a realistic input. `assert_consistent` closes every scenario.

The control (`test_scenario_clean_pr_merges`) proves the same harness *does*
merge a CLEAN PR — so a green edge-state assertion means the edge state blocked
the merge, not that the merge gate was never reached.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from tests.harness import Harness, ManualClock
from tests.harness.sim import PR_MERGED, SimCheck, SimPR

TEAM = "ENG"
REPO = "org/repo"
READY = "Todo"
NEEDS_APPROVAL = "Needs Approval"
DONE = "Done"


def _config(tmp_path: Path) -> Config:
    # No-review binding (the happy merge path): implement → PR → CI → merge.
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
                    code_review=NEEDS_APPROVAL,
                    done=DONE,
                ),
            )
        ],
    )


async def _run_pipeline(
    harness: Harness, *, edge: dict[str, object] | None, steps: int = 25
) -> SimPR | None:
    """Dispatch an issue through the pipeline; once the PR opens, stamp `edge`
    onto its `SimPR` (before the merge gate can fire) and keep stepping.

    Returns the issue's `SimPR` (or None if one never opened).
    """
    issue = harness.sim.seed_issue(
        identifier="ENG-1", team_key=TEAM, state_name=READY, title="merge edge state"
    )
    await harness.warmup()

    stamped = False
    for _ in range(steps):
        await harness.step()
        harness.advance(60)
        sim_pr = harness.sim.github.pr_for_issue(issue.id)
        if sim_pr is not None and not stamped and edge is not None:
            # The PR is open but the merge gate has not merged it yet (the merge
            # candidate is polled on a later tick). Stamp the edge state now so
            # the gate sees a realistic non-CLEAN PR.
            assert not sim_pr.merged, "merged before the edge state could be applied"
            for field, value in edge.items():
                setattr(sim_pr, field, value)
            stamped = True
        if harness.sim.issues[issue.id].state_type == "completed":
            break
    return harness.sim.github.pr_for_issue(issue.id)


async def _merge_runs(harness: Harness) -> list[db.runs.Run]:
    runs = await db.runs.history_for_issue(harness.conn, "ENG-1")
    return [r for r in runs if r.stage == "merge"]


@pytest.mark.asyncio
async def test_scenario_clean_pr_merges(tmp_path: Path) -> None:
    """Control: with no edge state the harness merges a CLEAN PR — proving the
    edge-state scenarios below reach (and are blocked at) the merge gate."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=ManualClock())
    try:
        sim_pr = await _run_pipeline(harness, edge=None)
        assert sim_pr is not None and sim_pr.state == PR_MERGED
        assert harness.sim.issues["ENG-1"].state_type == "completed"
        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_scenario_unknown_mergeability_repolls(tmp_path: Path) -> None:
    """UNKNOWN mergeability: the gate re-polls and takes no merge action."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=ManualClock())
    try:
        sim_pr = await _run_pipeline(
            harness,
            edge={
                "mergeable": "UNKNOWN",
                "merge_state_status": "UNKNOWN",
                "checks": [SimCheck(name="ci", conclusion="SUCCESS", required=True)],
            },
        )

        # Never merged, and the gate took NO action: no merge run was created
        # (UNKNOWN is "wait for GitHub to settle", not a trigger).
        assert sim_pr is not None and not sim_pr.merged
        assert await _merge_runs(harness) == []
        assert harness.sim.issues["ENG-1"].state_type != "completed"

        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_scenario_dirty_conflict_does_not_merge(tmp_path: Path) -> None:
    """DIRTY/CONFLICTING: the gate dispatches a rebase fix and never merges."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=ManualClock())
    try:
        sim_pr = await _run_pipeline(
            harness,
            edge={
                "mergeable": "CONFLICTING",
                "merge_state_status": "DIRTY",
                "checks": [SimCheck(name="ci", conclusion="SUCCESS", required=True)],
            },
        )

        # Never merged; a merge-conflict rebase fix run (stage review_fix) was
        # dispatched instead of a merge.
        assert sim_pr is not None and not sim_pr.merged
        runs = await db.runs.history_for_issue(harness.conn, "ENG-1")
        assert any(r.stage == "review_fix" for r in runs), (
            "expected a rebase fix-run for the DIRTY PR"
        )
        assert harness.sim.issues["ENG-1"].state_type != "completed"

        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_scenario_unstable_failing_optional_check_does_not_merge(
    tmp_path: Path,
) -> None:
    """UNSTABLE: a failing optional (non-required) check blocks the merge and
    escalates to operator approval rather than merging."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=ManualClock())
    try:
        sim_pr = await _run_pipeline(
            harness,
            edge={
                "mergeable": "MERGEABLE",
                "merge_state_status": "UNSTABLE",
                "checks": [
                    SimCheck(name="ci", conclusion="SUCCESS", required=True),
                    SimCheck(name="vercel", conclusion="FAILURE", required=False),
                ],
            },
        )

        assert sim_pr is not None and not sim_pr.merged
        merge = await _merge_runs(harness)
        assert merge and any(r.status == "needs_approval" for r in merge), (
            "expected the UNSTABLE PR to be parked in needs_approval, not merged"
        )
        assert harness.sim.issues["ENG-1"].state_type != "completed"

        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_scenario_behind_base_does_not_merge(tmp_path: Path) -> None:
    """BEHIND: a behind-base PR is not mergeable; the gate does not merge it and
    escalates to operator approval."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=ManualClock())
    try:
        sim_pr = await _run_pipeline(
            harness,
            edge={
                "mergeable": "MERGEABLE",
                "merge_state_status": "BEHIND",
                "checks": [SimCheck(name="ci", conclusion="SUCCESS", required=True)],
            },
        )

        assert sim_pr is not None and not sim_pr.merged
        merge = await _merge_runs(harness)
        assert merge and any(r.status == "needs_approval" for r in merge), (
            "expected the BEHIND PR to be parked in needs_approval, not merged"
        )
        assert harness.sim.issues["ENG-1"].state_type != "completed"

        await harness.assert_consistent()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_scenario_draft_pr_does_not_merge(tmp_path: Path) -> None:
    """Draft PR: GitHub refuses to merge a draft; the gate must not merge it."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path), clock=ManualClock())
    try:
        sim_pr = await _run_pipeline(
            harness,
            edge={
                "is_draft": True,
                "checks": [SimCheck(name="ci", conclusion="SUCCESS", required=True)],
            },
        )

        assert sim_pr is not None and not sim_pr.merged
        merge = await _merge_runs(harness)
        assert merge and any(r.status == "needs_approval" for r in merge), (
            "expected the draft PR to be parked in needs_approval, not merged"
        )
        assert harness.sim.issues["ENG-1"].state_type != "completed"

        await harness.assert_consistent()
    finally:
        await harness.close()
