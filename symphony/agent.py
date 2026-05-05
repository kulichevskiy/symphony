import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from .types import AgentResult

CLAUDE_BIN = "claude"
DEFAULT_MODEL = "claude-opus-4-7"
DEFAULT_MAX_TURNS = 50
DEFAULT_PERMISSION_MODE = "bypassPermissions"

Spawner = Callable[..., Awaitable[Any]]


def parse_event_line(line: str) -> dict | None:
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def extract_session_id(events: list[dict]) -> str | None:
    for ev in events:
        sid = ev.get("session_id")
        if sid:
            return sid
    return None


def find_result_event(events: list[dict]) -> dict | None:
    for ev in reversed(events):
        if ev.get("type") == "result":
            return ev
    return None


async def _read_all(stream) -> bytes:
    if stream is None:
        return b""
    return await stream.read()


async def _process_stdout(
    stream,
    events: list[dict],
    on_event: Callable[[dict], None] | None,
) -> None:
    if stream is None:
        return
    async for raw in stream:
        line = raw.decode("utf-8", errors="replace")
        ev = parse_event_line(line)
        if ev is None:
            continue
        events.append(ev)
        if on_event is not None:
            on_event(ev)


def build_argv(
    prompt: str,
    *,
    model: str,
    max_turns: int,
    permission_mode: str,
    settings_path: Path | None = None,
    resume_session: str | None = None,
) -> list[str]:
    argv = [
        CLAUDE_BIN,
        "-p",
        prompt,
        "--output-format",
        "stream-json",
        "--verbose",
        "--model",
        model,
        "--max-turns",
        str(max_turns),
        "--permission-mode",
        permission_mode,
    ]
    if settings_path is not None:
        argv += ["--settings", str(settings_path)]
    if resume_session:
        argv += ["--resume", resume_session]
    return argv


async def run_agent(
    prompt: str,
    workdir: Path,
    *,
    model: str = DEFAULT_MODEL,
    max_turns: int = DEFAULT_MAX_TURNS,
    permission_mode: str = DEFAULT_PERMISSION_MODE,
    settings_path: Path | None = None,
    resume_session: str | None = None,
    on_event: Callable[[dict], None] | None = None,
    spawner: Spawner | None = None,
) -> AgentResult:
    argv = build_argv(
        prompt,
        model=model,
        max_turns=max_turns,
        permission_mode=permission_mode,
        settings_path=settings_path,
        resume_session=resume_session,
    )

    spawn = spawner if spawner is not None else asyncio.create_subprocess_exec
    proc = await spawn(
        *argv,
        cwd=str(workdir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    events: list[dict] = []

    # Drain stderr concurrently with stdout — otherwise a large stderr write
    # while we're still iterating stdout fills the stderr pipe buffer and the
    # child blocks waiting for us to read it, deadlocking the run.
    stderr_task = asyncio.create_task(_read_all(proc.stderr))
    await _process_stdout(proc.stdout, events, on_event)
    stderr_bytes = await stderr_task
    exit_code = await proc.wait()

    session_id = extract_session_id(events)
    result_ev = find_result_event(events)
    is_error = bool(result_ev.get("is_error", False)) if result_ev else (exit_code != 0)
    final_text = result_ev.get("result") if result_ev else None
    duration_ms = result_ev.get("duration_ms") if result_ev else None
    num_turns = result_ev.get("num_turns") if result_ev else None
    total_cost_usd = result_ev.get("total_cost_usd") if result_ev else None

    success = exit_code == 0 and result_ev is not None and not is_error

    return AgentResult(
        session_id=session_id,
        exit_code=exit_code,
        success=success,
        is_error=is_error,
        duration_ms=duration_ms,
        num_turns=num_turns,
        total_cost_usd=total_cost_usd,
        final_text=final_text,
        raw_events=events,
        stderr=stderr_bytes.decode("utf-8", errors="replace"),
    )
