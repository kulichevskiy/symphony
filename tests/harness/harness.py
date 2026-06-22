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

from .clock import ManualClock
from .fakes import FakeGitHub, FakeLinear
from .invariants import assert_consistent
from .sim import Sim

DEFAULT_TEAM = "ENG"
DEFAULT_REPO = "org/repo"
# A workflow that covers every lane a binding can reference.
DEFAULT_STATES = {
    "Todo": "state-todo",
    "In Progress": "state-progress",
    "Local Code Review": "state-local-review",
    "Needs Approval": "state-review",
    "Done": "state-done",
}
DEFAULT_STATE_TYPES = {
    "Todo": "unstarted",
    "In Progress": "started",
    "Local Code Review": "started",
    "Needs Approval": "started",
    "Done": "completed",
}


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
        orch: Orchestrator,
    ) -> None:
        self.config = config
        self.conn = conn
        self.sim = sim
        self.clock = clock
        self.linear = linear
        self.github = github
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
            sim.seed_team(binding.linear_team_key, DEFAULT_STATES, DEFAULT_STATE_TYPES)

        conn = await db.connect(tmp_path / "symphony.sqlite")
        linear = FakeLinear(sim)
        github = FakeGitHub(sim)
        orch = Orchestrator(
            config,
            linear,
            conn,
            gh=github,  # type: ignore[arg-type]
            clock=clock,
        )
        return cls(
            config=config,
            conn=conn,
            sim=sim,
            clock=clock,
            linear=linear,
            github=github,
            orch=orch,
        )

    def advance(self, secs: float) -> None:
        self.clock.advance(secs)

    async def warmup(self) -> None:
        """Steppable startup work: cache states + run startup reconciles."""
        await self.orch.warmup()
        await self.orch._restore_operator_waits()  # noqa: SLF001
        await self.orch._reconcile_orphaned_merge_runs(reason="startup")  # noqa: SLF001
        await self.orch._reconcile_auto_recoverable_merge_waits(  # noqa: SLF001
            reason="startup"
        )
        await self._drain()

    async def step(self) -> list[asyncio.Task[None]]:
        """Run one `_tick()` to quiescence and return the dispatched tasks."""
        await self.orch._drain_web_commands()  # noqa: SLF001
        scheduled = await self.orch._tick()  # noqa: SLF001
        if scheduled:
            await asyncio.gather(*scheduled)
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
