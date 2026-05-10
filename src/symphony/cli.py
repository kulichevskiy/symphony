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

from .config import Config
from .linear.client import Linear, LinearError
from .orchestrator.poll import Orchestrator


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
        orch = Orchestrator(cfg, linear)
        if once:
            await orch.warmup()
            await orch._tick()  # pylint: disable=protected-access
            return
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(orch.shutdown()))
        await orch.run()


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
            missing = [
                name
                for name in (
                    cfg.linear_states.ready,
                    cfg.linear_states.in_progress,
                    cfg.linear_states.needs_approval,
                    cfg.linear_states.blocked,
                    cfg.linear_states.done,
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


if __name__ == "__main__":
    main()  # pragma: no cover
