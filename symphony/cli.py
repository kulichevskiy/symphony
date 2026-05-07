import asyncio
import time
from pathlib import Path
from typing import Annotated

import structlog
import typer

from . import __version__
from .agent import (
    DEFAULT_MAX_TURNS,
    DEFAULT_MODEL,
    DEFAULT_PERMISSION_MODE,
    run_agent,
)
from .config import load_config
from .events import EventLog
from .logging_setup import configure_logging
from .orchestrator import install_shutdown_handler, run_forever
from .runonce import run_once

app = typer.Typer(no_args_is_help=True, add_completion=False)
log = structlog.get_logger()


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"symphony {__version__}")
        raise typer.Exit(0)


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show version and exit",
        ),
    ] = False,
) -> None:
    """Symphony — personal autopilot for GitHub Issues."""
    configure_logging()


@app.command("agent-run")
def agent_run(
    prompt: Annotated[str, typer.Option("--prompt", "-p", help="Prompt for the agent")],
    workdir: Annotated[
        Path, typer.Option("--workdir", "-w", help="Working directory for the agent")
    ],
    model: Annotated[str, typer.Option(help="Claude model")] = DEFAULT_MODEL,
    max_turns: Annotated[int, typer.Option(help="Max turns")] = DEFAULT_MAX_TURNS,
    permission_mode: Annotated[
        str, typer.Option(help="Permission mode")
    ] = DEFAULT_PERMISSION_MODE,
    resume: Annotated[
        str | None, typer.Option(help="Resume an existing session id")
    ] = None,
    settings: Annotated[
        Path | None,
        typer.Option(
            "--settings",
            help="Path to a Claude settings JSON file. Pass an isolated "
            "Symphony-controlled file to keep runs deterministic across hosts.",
        ),
    ] = None,
) -> None:
    """One-shot agent invocation in the given workdir."""
    workdir.mkdir(parents=True, exist_ok=True)

    def on_event(ev: dict) -> None:
        log.info(
            "agent.event",
            kind=ev.get("type"),
            subtype=ev.get("subtype"),
            session_id=ev.get("session_id"),
        )

    log.info("agent.start", workdir=str(workdir), model=model, max_turns=max_turns)
    result = asyncio.run(
        run_agent(
            prompt,
            workdir,
            model=model,
            max_turns=max_turns,
            permission_mode=permission_mode,
            settings_path=settings,
            resume_session=resume,
            on_event=on_event,
        )
    )
    log.info(
        "agent.done",
        session_id=result.session_id,
        exit_code=result.exit_code,
        success=result.success,
        is_error=result.is_error,
        num_turns=result.num_turns,
        duration_ms=result.duration_ms,
        total_cost_usd=result.total_cost_usd,
    )

    if result.final_text:
        typer.echo(result.final_text)

    if not result.success:
        if result.stderr:
            typer.echo(result.stderr, err=True)
        raise typer.Exit(result.exit_code if result.exit_code != 0 else 1)


@app.command("run-once")
def run_once_cmd(
    issue_number: Annotated[int, typer.Argument(help="GitHub issue number to dispatch")],
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help="Path to symphony.toml",
            exists=False,
        ),
    ] = Path("symphony.toml"),
) -> None:
    """End-to-end one-shot for a single issue: agent run → PR open → arm auto-merge."""
    log.info("runonce.start", issue=issue_number, config=str(config))
    result = asyncio.run(run_once(issue_number=issue_number, config_path=config))
    if result.skipped:
        log.error("runonce.skipped", issue=issue_number, reason=result.skip_reason)
        raise typer.Exit(1)
    pr_url = result.pr.url if result.pr is not None else ""
    log.info(
        "runonce.done",
        issue=issue_number,
        pr=pr_url,
        loop=result.loop_outcome.kind.value if result.loop_outcome else None,
        rounds=result.loop_outcome.rounds_used if result.loop_outcome else None,
    )
    if pr_url:
        typer.echo(pr_url)
    # Non-zero exit when the review loop didn't reach APPROVED so the CLI's
    # caller can react (auto-stuck label / agent failure both keep the PR
    # and worktree around — see SYMPHONY.md).
    if result.loop_outcome is not None and result.loop_outcome.kind.value != "approved":
        raise typer.Exit(2)


@app.command("run")
def run_cmd(
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help="Path to symphony.toml",
        ),
    ] = Path("symphony.toml"),
) -> None:
    """Long-running autopilot: poll for `auto`-labeled issues and dispatch."""
    cfg = load_config(config)

    async def _main() -> None:
        loop = asyncio.get_running_loop()
        shutdown = install_shutdown_handler(loop)
        log.info(
            "orchestrator.start",
            label=cfg.github.label,
            cap=cfg.orchestrator.max_concurrent,
            poll_s=cfg.orchestrator.poll_interval_s,
        )
        await run_forever(cfg=cfg, config_path=config, shutdown_event=shutdown)
        log.info("orchestrator.stopped")

    asyncio.run(_main())


@app.command("status")
def status_cmd(
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help="Path to symphony.toml",
        ),
    ] = Path("symphony.toml"),
) -> None:
    """Show in-flight issues and recent terminal runs from the event log."""
    cfg = load_config(config)
    snapshot = EventLog.for_repo(cfg.repo.path).status_snapshot()

    typer.echo("In-flight:")
    if not snapshot.in_flight:
        typer.echo("  none")
    for item in snapshot.in_flight:
        typer.echo(
            "  "
            f"#{item.issue_number} "
            f"round={item.round} "
            f"elapsed={item.elapsed_s}s "
            f"latest_sha={item.latest_sha or '-'} "
            f"last_reviewed_sha={item.last_reviewed_sha or '-'} "
            f"last_review_verdict={item.last_review_verdict or '-'}"
        )

    typer.echo("Terminal runs (24h):")
    if not snapshot.terminal_runs:
        typer.echo("  none")
    for item in snapshot.terminal_runs:
        typer.echo(
            "  "
            f"#{item.issue_number} "
            f"outcome={item.outcome} "
            f"rounds={item.rounds} "
            f"total_elapsed={item.total_elapsed_s}s"
        )


@app.command("logs")
def logs_cmd(
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            help="Path to symphony.toml",
        ),
    ] = Path("symphony.toml"),
    issue: Annotated[
        int | None,
        typer.Option("--issue", help="Only show events for one issue number"),
    ] = None,
    follow: Annotated[
        bool,
        typer.Option("--follow", "-f", help="Keep waiting for new events"),
    ] = False,
    limit: Annotated[int, typer.Option("--limit", help="Initial events to print")] = 100,
) -> None:
    """Print event log rows as JSON lines."""
    cfg = load_config(config)
    event_log = EventLog.for_repo(cfg.repo.path)
    last_id = 0
    for event in event_log.tail_events(issue_number=issue, limit=limit):
        typer.echo(event.to_json_line())
        last_id = event.id

    while follow:
        time.sleep(1.0)
        events = event_log.iter_events(issue_number=issue, after_id=last_id)
        for event in events:
            typer.echo(event.to_json_line())
            last_id = event.id
