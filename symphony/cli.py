import asyncio
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
from .logging_setup import configure_logging
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
    assert result.pr is not None  # for type-checkers; orchestrator guarantees this
    log.info(
        "runonce.done",
        issue=issue_number,
        pr=result.pr.url,
        loop=result.loop_outcome.kind.value if result.loop_outcome else None,
        rounds=result.loop_outcome.rounds_used if result.loop_outcome else None,
    )
    typer.echo(result.pr.url)
    # Non-zero exit when the review loop didn't reach APPROVED so the CLI's
    # caller can react (auto-stuck label / agent failure both keep the PR
    # and worktree around — see SYMPHONY.md).
    if result.loop_outcome is not None and result.loop_outcome.kind.value != "approved":
        raise typer.Exit(2)
