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
from symphony.github.webhook import GitHubWebhookEvent
from symphony.orchestrator.poll import Orchestrator, WebhookDispatchResult
from symphony.orchestrator.reconcile import reconcile

from .clock import ManualClock
from .fakes import FakeGitHub, FakeLinear, FakeRunner
from .invariants import assert_consistent
from .sim import PR_OPEN, Sim

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
            if (
                sim_pr.head == branch
                and (pr_repo is None or sim_pr.repo == pr_repo)
                and sim_pr.state == PR_OPEN
            ):
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


def _build_runtime(
    config: Config,
    conn: aiosqlite.Connection,
    sim: Sim,
    clock: ManualClock,
    *,
    existing_linear: FakeLinear | None = None,
    existing_github: FakeGitHub | None = None,
    existing_runner: FakeRunner | None = None,
) -> tuple[FakeLinear, FakeGitHub, FakeRunner, Orchestrator]:
    """Construct the fakes + a fresh Orchestrator over the given Sim/clock/conn.

    Shared by `create()` and `restart()` so a restart rebuilds the exact same
    wiring on the reopened connection.  Pass existing fakes to preserve their
    accumulated state across restarts (e.g. FakeGitHub._reviews).
    """
    linear = existing_linear if existing_linear is not None else FakeLinear(sim)
    github = existing_github if existing_github is not None else FakeGitHub(sim)
    runner = existing_runner if existing_runner is not None else FakeRunner()

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
        gh=github,
        clock=clock,
        push_fn=_push_fn,
        force_push_fn=_force_push_fn,
    )
    return linear, github, runner, orch


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
        db_path: Path,
    ) -> None:
        self.config = config
        self.conn = conn
        self.sim = sim
        self.clock = clock
        self.linear = linear
        self.github = github
        self.runner = runner
        self.orch = orch
        self._db_path = db_path

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

        db_path = tmp_path / "symphony.sqlite"
        conn = await db.connect(db_path)
        linear, github, runner, orch = _build_runtime(config, conn, sim, clock)
        return cls(
            config=config,
            conn=conn,
            sim=sim,
            clock=clock,
            linear=linear,
            github=github,
            runner=runner,
            orch=orch,
            db_path=db_path,
        )

    def advance(self, secs: float) -> None:
        self.clock.advance(secs)

    async def warmup(self) -> None:
        """Steppable startup work: cache states + run startup reconciles."""
        await reconcile(
            self.conn,
            self.linear,
            bindings=self.config.repos,
            clock=self.clock,
            pid_alive=self.sim.pid_alive,
        )
        await self.orch.warmup()
        await self.orch._restore_operator_waits()  # noqa: SLF001
        await self.orch._reconcile_orphaned_merge_runs(reason="startup")  # noqa: SLF001
        await self.orch._reconcile_auto_recoverable_merge_waits(  # noqa: SLF001
            reason="startup"
        )
        await self.drain()

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
        await self.drain()
        return scheduled

    async def deliver_github_webhook(self) -> WebhookDispatchResult:
        """Deliver the next queued GitHub webhook to the orchestrator.

        Delivery is explicit so scenarios control ordering. The handler only
        *schedules* fire-and-forget reconcile tasks — call `drain()` to run
        them to quiescence before asserting (the deliver → drain → assert
        rhythm)."""
        if not self.sim.github_webhooks:
            raise AssertionError("no GitHub webhook queued to deliver")
        event: GitHubWebhookEvent = self.sim.github_webhooks.pop(0)
        return await self.orch.handle_github_webhook(event)

    async def deliver_linear_webhook(self) -> WebhookDispatchResult:
        """Deliver the next queued Linear webhook to the orchestrator.

        The Linear counterpart of `deliver_github_webhook`; same explicit
        delivery contract — follow with `drain()` before asserting."""
        if not self.sim.linear_webhooks:
            raise AssertionError("no Linear webhook queued to deliver")
        payload = self.sim.linear_webhooks.pop(0)
        return await self.orch.handle_linear_webhook(payload)

    async def drain(self) -> None:
        """Run the fire-and-forget scheduled background tasks to quiescence:
        the reconcile-event tasks webhook handlers spawn (poll.py
        handle_*_webhook) plus dispatch tasks. Deterministic — no real sleeps."""
        await self.orch.drain_reconcile_event_tasks()
        await self.orch.drain_dispatch_tasks()

    async def restart(self) -> None:
        """Model a host restart: shut the orchestrator down, **close + reopen**
        the DB on the same temp file (a fresh connection — not the live one, so
        WAL checkpoint / dropped caches / uncommitted-write rollback are
        modelled), then build a fresh Orchestrator on the same Sim + clock and
        run startup warmup/reconcile."""
        await self.orch.shutdown()
        await self.orch.drain_reconcile_event_tasks(cancel=True)
        # Crash simulation: skip drain_dispatch_tasks(cancel=True) — that path
        # calls _kill_active_runner (graceful) and triggers _mark_cancelled_dispatch
        # (writes cancelled status to DB). Neither happens in a real crash; reconcile
        # on restart handles orphan cleanup instead. In practice _dispatch_tasks is
        # always empty here because step() drains before returning.
        await self.conn.close()

        self.conn = await db.connect(self._db_path)

        # Mark all pids from still-running rows as dead in the sim: after a
        # restart every subprocess from the previous daemon generation is gone.
        for run in await db.runs.list_live_with_pid(self.conn):
            if run.pid is not None:
                self.sim.kill_process(run.pid)

        # Reuse existing fakes so accumulated GitHub state (_reviews,
        # _pr_comments, _commit_timestamps, _workspace_repos) survives.
        self.linear, self.github, self.runner, self.orch = _build_runtime(
            self.config, self.conn, self.sim, self.clock,
            existing_linear=self.linear,
            existing_github=self.github,
            existing_runner=self.runner,
        )
        await self.warmup()

    async def assert_consistent(self) -> None:
        await assert_consistent(self.sim, self.conn)

    async def close(self) -> None:
        await self.orch.shutdown()
        await self.orch.drain_reconcile_event_tasks(cancel=True)
        await self.orch.drain_dispatch_tasks(cancel=True)
        await self.conn.close()
