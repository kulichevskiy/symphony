"""`Sim` — the single canonical external reality.

Linear (issues / states / comments) and GitHub (PRs / branches / checks /
merge) live in one store. `sim.linear` / `sim.github` are thin read views onto
that store; the `FakeLinear` / `FakeGitHub` fakes (see `fakes.py`) implement the
orchestrator-facing interfaces against the *same* store.

Symphony's SQLite is a *separate*, possibly-stale view of this reality. For v1
consistency is instantaneous — lag is simulated only via the order in which a
test mutates the Sim vs. when it steps the orchestrator. Per-view staleness can
bolt on later without changing this shape.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from datetime import datetime

from symphony.tracker import Blocker

from .clock import ManualClock

# PR lifecycle in the Sim. `merged` is the terminal success state.
PR_OPEN = "open"
PR_CLOSED = "closed"
PR_MERGED = "merged"


@dataclass
class SimIssue:
    id: str
    identifier: str
    title: str = ""
    description: str = ""
    url: str = ""
    state_id: str = ""
    state_name: str = ""
    state_type: str = ""
    team_key: str = ""
    labels: list[str] = field(default_factory=list)
    blocked_by: list[Blocker] = field(default_factory=list)
    updated_at: str = ""


@dataclass
class SimComment:
    id: str
    issue_id: str
    body: str
    created_at: str
    author_name: str = "operator"
    author_is_me: bool = False
    external_thread_type: str | None = None


@dataclass
class SimPR:
    repo: str
    number: int
    head: str
    issue_id: str = ""
    base: str = "main"
    title: str = ""
    url: str = ""
    state: str = PR_OPEN
    head_sha: str = ""
    checks_passed: bool = True
    auto_merge_enabled: bool = False
    merged_at: str | None = None

    @property
    def merged(self) -> bool:
        return self.state == PR_MERGED


class _LinearView:
    """Thin read view onto the Sim's Linear reality."""

    def __init__(self, sim: Sim) -> None:
        self._sim = sim

    @property
    def issues(self) -> dict[str, SimIssue]:
        return self._sim.issues

    @property
    def comments(self) -> dict[str, list[SimComment]]:
        return self._sim.comments

    @property
    def states(self) -> dict[str, dict[str, str]]:
        return self._sim.states


class _GitHubView:
    """Thin read view onto the Sim's GitHub reality."""

    def __init__(self, sim: Sim) -> None:
        self._sim = sim

    @property
    def prs(self) -> dict[tuple[str, int], SimPR]:
        return self._sim.prs

    def pr_for_issue(self, issue_id: str) -> SimPR | None:
        for pr in self._sim.prs.values():
            if pr.issue_id == issue_id:
                return pr
        return None


class Sim:
    def __init__(self, clock: ManualClock) -> None:
        self._clock = clock
        # Linear reality.
        self.issues: dict[str, SimIssue] = {}
        self.comments: dict[str, list[SimComment]] = {}
        self.states: dict[str, dict[str, str]] = {}
        # team_key → {state_id → state_type}
        self.state_types: dict[str, dict[str, str]] = {}
        self.viewer_teams: list[str] = []
        # GitHub reality, keyed by (repo, number).
        self.prs: dict[tuple[str, int], SimPR] = {}
        # (repo, branch) → latest pushed SHA; populated by _sim_aware_push so
        # ensure_pr (called after the push) can use the real HEAD hash.
        # Keyed by (repo, branch) to avoid cross-repo collisions when two
        # repos share the same branch name.
        self.branch_head_shas: dict[tuple[str, str], str] = {}
        # Pids the Sim considers alive. Only explicitly registered pids are
        # alive; everything else is dead. The reconcile path consults
        # pid_alive() instead of os.kill so the Sim — not a magic dead-PID
        # convention — owns process liveness. Using a live-set (vs a dead-set)
        # means killed pids are never "permanently dead": reusing a pid after a
        # restart cycle simply requires re-registering it.
        self._live_pids: set[int] = set()
        self._pr_counter = itertools.count(1)
        self._comment_counter = itertools.count(1)
        self.linear = _LinearView(self)
        self.github = _GitHubView(self)

    def now(self) -> datetime:
        return self._clock()

    def now_iso(self) -> str:
        return self._clock().isoformat()

    # --- seeding helpers (used by Harness defaults and scenarios) ---

    def seed_team(
        self,
        team_key: str,
        states: dict[str, str],
        types: dict[str, str] | None = None,
    ) -> None:
        """Register a team's workflow states and make it viewer-visible.

        `states` maps state name → state id.
        `types` maps state name → state type (e.g. "unstarted", "started", "completed").
        """
        self.states[team_key] = dict(states)
        if types:
            self.state_types[team_key] = {
                states[name]: t for name, t in types.items() if name in states
            }
        if team_key not in self.viewer_teams:
            self.viewer_teams.append(team_key)

    def seed_issue(
        self,
        *,
        identifier: str,
        team_key: str,
        state_name: str,
        title: str = "",
        labels: list[str] | None = None,
    ) -> SimIssue:
        """Script an issue into a team's lane (e.g. the dispatch/ready lane).

        Resolves `state_name` to its id/type via the team's seeded workflow, so
        the orchestrator's state-driven scan and lane transitions see a faithful
        issue. The synthetic `url` is what `ensure_pr` matches on to link the PR
        back to this issue.
        """
        state_id = self.states.get(team_key, {}).get(state_name, "")
        issue = SimIssue(
            id=identifier,
            identifier=identifier,
            title=title,
            url=f"https://linear.invalid/{identifier}",
            state_id=state_id,
            state_name=state_name,
            state_type=self.state_type_for_id(team_key, state_id) or "",
            team_key=team_key,
            labels=list(labels or []),
            updated_at=self.now_iso(),
        )
        self.issues[issue.id] = issue
        return issue

    def state_name_for_id(self, team_key: str, state_id: str) -> str | None:
        for name, sid in self.states.get(team_key, {}).items():
            if sid == state_id:
                return name
        return None

    def state_type_for_id(self, team_key: str, state_id: str) -> str | None:
        return self.state_types.get(team_key, {}).get(state_id)

    # --- process liveness (the reconcile pid_alive seam) ---

    def register_pid(self, pid: int) -> None:
        """Mark a pid alive (model a healthy worker in a pre-restart scenario)."""
        self._live_pids.add(pid)

    def kill_process(self, pid: int) -> None:
        """Declare a pid dead, modelling a worker lost when the host crashed."""
        self._live_pids.discard(pid)

    def pid_alive(self, pid: int) -> bool:
        return pid in self._live_pids

    def next_pr_number(self) -> int:
        return next(self._pr_counter)

    def next_comment_id(self) -> str:
        return f"sim-comment-{next(self._comment_counter)}"
