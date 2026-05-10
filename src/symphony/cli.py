"""CLI entrypoint.

```
uv run python -m symphony --config examples/config.yaml
uv run symphony preflight                    # validate auth + states
uv run symphony --config ... --once          # one poll cycle then exit (smoke test)
```
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path

import click

from . import db
from .config import Config
from .linear.client import Linear, LinearError
from .orchestrator.poll import Orchestrator
from .orchestrator.reconcile import reconcile


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


@click.group(invoke_without_command=True)
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True),
    default=None,
    help="Path to config YAML.",
)
@click.option("--once", is_flag=True, help="Run one poll tick and exit.")
@click.pass_context
def main(ctx: click.Context, config_path: Path | None, once: bool) -> None:
    """symphonyd — headless Symphony port."""
    _setup_logging()
    if ctx.invoked_subcommand is None:
        if config_path is None:
            click.echo("--config is required when no subcommand is given", err=True)
            sys.exit(2)
        asyncio.run(_run(config_path, once=once))


async def _run(config_path: Path, *, once: bool) -> None:
    cfg = Config.load(config_path)
    if not cfg.linear_api_key:
        click.echo("LINEAR_API_KEY env var is empty; aborting", err=True)
        sys.exit(2)
    async with Linear(cfg.linear_api_key) as linear:
        conn = await db.connect(cfg.db_path)
        try:
            await reconcile(conn, linear)
            orch = Orchestrator(cfg, linear, conn)
            if once:
                await orch.warmup()
                await orch._tick()  # pylint: disable=protected-access
                return
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(orch.shutdown()))
            await orch.run()
        finally:
            await conn.close()


@main.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True),
    required=True,
)
def preflight(config_path: Path) -> None:
    """Validate Linear auth, list visible teams, and confirm configured states exist."""
    _setup_logging()
    asyncio.run(_preflight(config_path))


async def _preflight(config_path: Path) -> None:
    cfg = Config.load(config_path)
    if not cfg.linear_api_key:
        click.echo("LINEAR_API_KEY is empty", err=True)
        sys.exit(2)
    async with Linear(cfg.linear_api_key) as linear:
        try:
            visible = await linear.viewer_team_keys()
        except LinearError as e:
            click.echo(f"linear auth failed: {e}", err=True)
            sys.exit(1)
        click.echo(f"linear teams visible to this key: {visible}")
        ok = True
        for binding in cfg.repos:
            if binding.linear_team_key not in visible:
                click.echo(
                    f"  ✗ {binding.linear_team_key}: not visible — will be skipped at runtime"
                )
                ok = False
                continue
            states = await linear.team_states(binding.linear_team_key)
            ready = binding.linear_states.ready
            if not ready or ready not in states:
                click.echo(
                    f"  ✗ {binding.linear_team_key}: ready state "
                    f"{ready!r} not in team workflow; "
                    f"available: {sorted(states.keys())}"
                )
                ok = False
                continue
            missing = [
                name
                for name in (
                    binding.linear_states.in_progress,
                    binding.linear_states.needs_approval,
                    binding.linear_states.blocked,
                    binding.linear_states.done,
                )
                if name not in states
            ]
            if missing:
                click.echo(
                    f"  ✗ {binding.linear_team_key}: missing states {missing}; "
                    f"available: {sorted(states.keys())}"
                )
                ok = False
            else:
                click.echo(f"  ✓ {binding.linear_team_key} → {binding.github_repo}: states ok")
        sys.exit(0 if ok else 1)


@main.group()
def runs() -> None:
    """Inspect runs in the SQLite store. No orchestrator process required."""


@runs.command("ls")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True),
    required=True,
    help="Path to the symphonyd SQLite file.",
)
@click.option("--limit", type=int, default=50, help="Max rows to show.")
def runs_ls(db_path: Path, limit: int) -> None:
    """List active + recent runs."""
    asyncio.run(_runs_ls(db_path, limit))


async def _runs_ls(db_path: Path, limit: int) -> None:
    conn = await db.connect(db_path)
    try:
        rows = await db.runs.list_recent(conn, limit=limit)
    finally:
        await conn.close()
    if not rows:
        click.echo("(no runs)")
        return
    click.echo("id\tissue\tstage\tstatus\tcost\tstarted_at")
    for r in rows:
        run = r.run
        click.echo(
            f"{run.id}\t{r.identifier}\t{run.stage}\t{run.status}\t"
            f"${run.cost_usd:.2f}\t{run.started_at}"
        )


@runs.command("show")
@click.argument("run_id")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True),
    required=True,
)
def runs_show(run_id: str, db_path: Path) -> None:
    """Show full detail for a single run."""
    asyncio.run(_runs_show(run_id, db_path))


async def _runs_show(run_id: str, db_path: Path) -> None:
    conn = await db.connect(db_path)
    try:
        rwi = await db.runs.get_with_issue(conn, run_id)
        if rwi is None:
            click.echo(f"run not found: {run_id}", err=True)
            sys.exit(1)
        cursor = await db.comment_cursors.get(conn, rwi.run.issue_id)
        history = await db.runs.history_for_issue(conn, rwi.run.issue_id)
    finally:
        await conn.close()
    run = rwi.run
    click.echo(f"id:             {run.id}")
    click.echo(f"issue:          {rwi.identifier} ({run.issue_id})")
    click.echo(f"stage:          {run.stage}")
    click.echo(f"status:         {run.status}")
    click.echo(f"pid:            {run.pid if run.pid is not None else '-'}")
    click.echo(f"started_at:     {run.started_at}")
    click.echo(f"ended_at:       {run.ended_at or '-'}")
    click.echo(f"cost_usd:       {run.cost_usd}")
    click.echo(f"comment cursor: {cursor or '-'}")
    click.echo("stage history:")
    for h in history:
        marker = "*" if h.id == run.id else " "
        click.echo(
            f"  {marker} {h.started_at}  {h.stage:<10}  {h.status:<11}  {h.id}"
        )


@main.command()
@click.argument("linear_id")
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True),
    required=True,
)
def dispatch(linear_id: str, config_path: Path) -> None:
    """Hand-launch a run for a Linear issue, regardless of its state."""
    _setup_logging()
    asyncio.run(_dispatch(linear_id, config_path))


async def _dispatch(linear_id: str, config_path: Path) -> None:
    cfg = Config.load(config_path)
    if not cfg.linear_api_key:
        click.echo("LINEAR_API_KEY is empty", err=True)
        sys.exit(2)
    async with Linear(cfg.linear_api_key) as linear:
        try:
            issue = await linear.lookup_issue(linear_id)
        except LinearError as e:
            click.echo(f"linear lookup failed: {e}", err=True)
            sys.exit(1)
        binding = next(
            (b for b in cfg.repos if b.linear_team_key == issue.team_key), None
        )
        if binding is None:
            configured = sorted({b.linear_team_key for b in cfg.repos})
            click.echo(
                f"no binding configured for team key {issue.team_key!r}; "
                f"configured teams: {configured}",
                err=True,
            )
            sys.exit(1)
        conn = await db.connect(cfg.db_path)
        try:
            orch = Orchestrator(cfg, linear, conn)
            await orch._dispatch_one(binding, issue)  # noqa: SLF001
        finally:
            await conn.close()
        click.echo(f"dispatched {issue.identifier} → {binding.github_repo}")


if __name__ == "__main__":
    main()  # pragma: no cover
