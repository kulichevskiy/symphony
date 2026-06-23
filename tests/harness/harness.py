"""`Harness` — the in-memory rig later pipeline-timing scenarios run against.

Owns a single `Sim`, a shared `ManualClock`, the `FakeLinear` / `FakeGitHub`
fakes, and a real `Orchestrator` on a temp SQLite file. Tests drive it
`_tick`-by-`_tick` via `step()`; they never run `Orchestrator.run()`'s sleep
loop.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import aiosqlite

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.orchestrator.poll import Orchestrator
from symphony.orchestrator.reconcile import reconcile

from .clock import ManualClock
from .fakes import FakeGitHub, FakeLinear, FakeRunner
from .invariants import assert_consistent
from .sim import Sim

DEFAULT_TEAM = "ENG"
DEFAULT_REPO = "org/repo"


async def _sim_aware_push(
    workspace_path: Path,
    branch: str,
    sim: "Sim",
    *,
    repo: str | None = None,
    force: bool = False,
    commit_timestamps: "dict[str, str] | None" = None,
) -> None:
    """Push to the fake origin and update the matching SimPR's head_sha."""
    cmd = (
        ["git", "push", "--force-with-lease", "-u", "origin", branch]
        if force
        else ["git", "push", "-u", "origin", branch]
    )
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git push failed: {stderr.decode(errors='replace').strip()}"
        )
    # Read the new HEAD SHA and propagate it to the SimPR so pr_view()
    # returns the updated headRefOid on the next poll.
    sha_proc = await asyncio.create_subprocess_exec(
        "git", "rev-parse", "HEAD",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    sha_out, _ = await sha_proc.communicate()
    head_sha = sha_out.decode().strip()
    if head_sha:
        # Store the pushed SHA keyed by (repo, branch) so ensure_pr can use
        # the real git SHA without collisions across repos sharing a branch name.
        # Fall back to inferring repo from an existing SimPR when the caller
        # didn't supply it (single-repo scenarios before the first ensure_pr).
        pr_repo = repo
        if pr_repo is None:
            for sim_pr in sim.prs.values():
                if sim_pr.head == branch:
                    pr_repo = sim_pr.repo
                    break
        if pr_repo is not None:
            sim.branch_head_shas[(pr_repo, branch)] = head_sha
        # Stamp the commit timestamp at push time so commit_committed_at()
        # never returns a lazily-assigned future clock value.
        if commit_timestamps is not None:
            commit_timestamps.setdefault(head_sha, sim.now_iso())
        for sim_pr in sim.prs.values():
            if sim_pr.head == branch and (pr_repo is None or sim_pr.repo == pr_repo):
                sim_pr.head_sha = head_sha
                break


def _states_from_binding(binding: RepoBinding) -> tuple[dict[str, str], dict[str, str]]:
    """Derive state name → id and state name → type from a binding's TrackerStates."""
    ls = binding.linear_states
    role_types: list[tuple[str, str]] = [
        (ls.ready, "unstarted"),
        (ls.in_progress, "started"),
        (ls.local_code_review, "started"),
        (ls.code_review, "started"),
        (ls.needs_approval, "started"),
        (ls.in_acceptance, "started"),
        (ls.blocked, "started"),
        (ls.done, "completed"),
    ]
    if ls.waiting:
        role_types.append((ls.waiting, "started"))
    states: dict[str, str] = {}
    types: dict[str, str] = {}
    for name, stype in role_types:
        if name and name not in states:
            states[name] = "state-" + name.lower().replace(" ", "-")
            types[name] = stype
    return states, types


def _default_config(tmp_path: Path) -> Config:
    return Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        repos=[
            RepoBinding(
                linear_team_key=DEFAULT_TEAM,
                github_repo=DEFAULT_REPO,
                linear_states=LinearStates(
                    ready="Todo",
                    in_progress="In Progress",
                    code_review="Needs Approval",
                ),
            )
        ],
    )


class Harness:
    def __init__(
        self,
        *,
        config: Config,
        conn: aiosqlite.Connection,
        sim: Sim,
        clock: ManualClock,
        linear: FakeLinear,
        github: FakeGitHub,
        runner: FakeRunner,
        orch: Orchestrator,
    ) -> None:
        self.config = config
        self.conn = conn
        self.sim = sim
        self.clock = clock
        self.linear = linear
        self.github = github
        self.runner = runner
        self.orch = orch

    @classmethod
    async def create(
        cls,
        tmp_path: Path,
        *,
        config: Config | None = None,
        clock: ManualClock | None = None,
    ) -> Harness:
        clock = clock or ManualClock()
        config = config or _default_config(tmp_path)
        sim = Sim(clock)
        # Seed each binding's team workflow so warmup + state validation pass.
        for binding in config.repos:
            states, types = _states_from_binding(binding)
            sim.seed_team(binding.linear_team_key, states, types)

        conn = await db.connect(tmp_path / "symphony.sqlite")
        linear = FakeLinear(sim)
        github = FakeGitHub(sim)
        runner = FakeRunner()

        async def _push_fn(workspace_path: Path, branch: str) -> None:
            await _sim_aware_push(
                workspace_path, branch, sim,
                repo=github._workspace_repos.get(workspace_path),
                commit_timestamps=github._commit_timestamps,
            )

        async def _force_push_fn(workspace_path: Path, branch: str) -> None:
            await _sim_aware_push(
                workspace_path, branch, sim,
                repo=github._workspace_repos.get(workspace_path),
                force=True,
                commit_timestamps=github._commit_timestamps,
            )

        orch = Orchestrator(
            config,
            linear,
            conn,
            runner=runner,
            gh=github,  # type: ignore[arg-type]
            clock=clock,
            push_fn=_push_fn,
            force_push_fn=_force_push_fn,
        )
        return cls(
            config=config,
            conn=conn,
            sim=sim,
            clock=clock,
            linear=linear,
            github=github,
            runner=runner,
            orch=orch,
        )

    def advance(self, secs: float) -> None:
        self.clock.advance(secs)

    async def warmup(self) -> None:
        """Steppable startup work: cache states + run startup reconciles."""
        await reconcile(self.conn, self.linear, bindings=self.config.repos, clock=self.clock)
        await self.orch.warmup()
        await self.orch._restore_operator_waits()  # noqa: SLF001
        await self.orch._reconcile_orphaned_merge_runs(reason="startup")  # noqa: SLF001
        await self.orch._reconcile_auto_recoverable_merge_waits(  # noqa: SLF001
            reason="startup"
        )
        await self._drain()

    async def step(self) -> list[asyncio.Task[None]]:
        """Run one `_tick()` + one reconciler tick to quiescence."""
        await self.orch._drain_web_commands()  # noqa: SLF001
        await self.orch._reconciler.tick()  # noqa: SLF001
        scheduled = await self.orch._tick()  # noqa: SLF001
        if scheduled:
            results = await asyncio.gather(*scheduled, return_exceptions=True)
            for r in results:
                if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                    raise r
        await self._drain()
        return scheduled

    async def _drain(self) -> None:
        await self.orch.drain_reconcile_event_tasks()
        await self.orch.drain_dispatch_tasks()

    async def assert_consistent(self) -> None:
        await assert_consistent(self.sim, self.conn)

    async def close(self) -> None:
        await self.orch.shutdown()
        await self._drain()
        await self.conn.close()
