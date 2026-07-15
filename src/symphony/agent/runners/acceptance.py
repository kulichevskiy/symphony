"""Acceptance runner for Claude Code headless mode."""

from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import socket
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field, replace
from pathlib import Path

from symphony.agent.codex_models import DEFAULT_CODEX_MODEL
from symphony.agent.process import parse_event_line
from symphony.agent.runner import Runner, RunnerEvent, RunnerSpec
from symphony.pipeline.acceptance_classifier import (
    ACCEPTANCE_FOOTER_PASS,
    ACCEPTANCE_FOOTER_REJECT,
    ACCEPTANCE_REASON_QUICK_SKIP_TRIVIAL,
    AcceptanceVerdict,
    acceptance_classifier,
)
from symphony.pipeline.cost_guard import UsageCostEstimator, UsageDelta
from symphony.pipeline.local_review_io import (
    CollectedRunnerOutput,
    open_run_log,
    tee_run_log,
)

_DIFF_LIMIT_CHARS = 60_000
_CODE_ONLY_MODE = "code_only"
_DEV_MODE = "dev"
_PREVIEW_MODE = "preview"
_PLAYWRIGHT_MODES = {_DEV_MODE, _PREVIEW_MODE}
_DEV_SERVER_STARTUP_TIMEOUT_SECS = 60.0
_DEV_SERVER_STOP_TIMEOUT_SECS = 5.0
_QUICK_SKIP_DETAILS = "No user-visible behavior described in the ticket or PR diff."
_DOC_EXTENSIONS = {".adoc", ".md", ".mdx", ".rst", ".txt"}
_DOC_FILENAMES = {
    "authors",
    "changelog",
    "codeowners",
    "contributing",
    "license",
    "notice",
    "readme",
    "security",
}
_DEP_FILENAMES = {
    "cargo.lock",
    "cargo.toml",
    "gemfile",
    "gemfile.lock",
    "go.mod",
    "go.sum",
    "package-lock.json",
    "package.json",
    "pnpm-lock.yaml",
    "poetry.lock",
    "pyproject.toml",
    "requirements-dev.txt",
    "requirements.txt",
    "uv.lock",
    "yarn.lock",
}
_DESCRIPTION_TEMPLATE_HEADING_RE = re.compile(
    r"^#+\s*("
    r"what to build|where to verify|acceptance criteria|out of scope|"
    r"description|summary|notes?|implementation|context"
    r")\s*$",
    re.IGNORECASE,
)
_MARKDOWN_HEADING_RE = re.compile(r"^#+\s*")
_MARKDOWN_LIST_RE = re.compile(r"^(?:[-*+]|\d+[.)])\s+")
_MARKDOWN_CHECKBOX_RE = re.compile(r"^\[[ xX]\]\s+")
_EXPLICIT_TRIVIAL_DESCRIPTION_LINE_RE = re.compile(
    r"(?:"
    r"no\s+(?:user[- ]visible\s+)?behaviou?r(?:\s+change)?|"
    r"(?:internal\s+)?refactor(?:\s+[\w./`'\" -]+)?\s+"
    r"(?:only|with\s+no\s+behaviou?r\s+change|no\s+behaviou?r\s+change)"
    r")\.?",
    re.IGNORECASE,
)
_NON_TRIVIAL_DESCRIPTION_RE = re.compile(
    r"\b("
    r"api|endpoint|ui|ux|screen|page|button|form|modal|dialog|"
    r"workflow|flow|user|customer|operator|admin|login|auth|oauth|"
    r"payment|checkout|feature|behaviou?r|visual|visible|browser|"
    r"playwright|preview|server|database|db|persist|state|render|"
    r"click|submit"
    r")\b",
    re.IGNORECASE,
)
_TRIVIAL_DESCRIPTION_LINE_RES = (
    re.compile(
        r"[\w./`'\" -]*(?:typo|spelling|grammar|copyedit|copy edit)"
        r"[\w./`'\" -]*\.?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:update|fix|edit|improve|refresh)?\s*"
        r"(?:docs?|documentation|readme|changelog|license|notice|comments?)"
        r"(?:\s+(?:only|typo|spelling|grammar|copy|copyedit|copy edit|"
        r"links?|wording|text|reference|examples?))*\.?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:bump|update|upgrade)\s+"
        r"(?:[\w./`'\"-]+\s+)?"
        r"(?:dependencies|dependency|deps?|packages?|lockfile|version)"
        r"(?:\s+[\w./`'\" -]+)?\.?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:rename|move)\s+[\w./`'\" -]+\s+(?:to|as)\s+[\w./`'\" -]+\.?",
        re.IGNORECASE,
    ),
)
_DEPENDENCY_DESCRIPTION_RE = re.compile(
    r"\b(dependency|dependencies|dep bump|bump|upgrade|update|lockfile|version)\b",
    re.IGNORECASE,
)
_INTERNAL_ONLY_DESCRIPTION_RE = re.compile(
    r"\b("
    r"internal refactor|refactor only|rename only|no behavior change|"
    r"no behaviour change|no user-visible behavior|no user visible behavior"
    r")\b",
    re.IGNORECASE,
)
_USER_FACING_PATH_PARTS = {
    "api",
    "app",
    "components",
    "frontend",
    "pages",
    "routes",
    "screens",
    "ui",
    "views",
}
_CLAUDE_ACCEPTANCE_PERMISSION_MODE = "default"
_CLAUDE_ACCEPTANCE_DISALLOWED_TOOLS = ",".join(
    (
        "Bash",
        "Read",
        "Edit",
        "Write",
        "MultiEdit",
        "Glob",
        "Grep",
        "LS",
        "NotebookRead",
        "NotebookEdit",
        "WebFetch",
        "WebSearch",
        "TodoWrite",
        "Task",
    )
)
_CLAUDE_DEV_ACCEPTANCE_DISALLOWED_TOOLS = ",".join(
    (
        "Bash",
        "Read",
        "Edit",
        "Write",
        "MultiEdit",
        "Glob",
        "Grep",
        "LS",
        "NotebookRead",
        "NotebookEdit",
        "WebFetch",
        "WebSearch",
        "TodoWrite",
        "Task",
    )
)


async def run_acceptance(
    *,
    runner: Runner,
    run_id: str,
    workspace_path: Path,
    mode: str,
    linear_description: str,
    pr_diff_summary: str,
    taste_guide: str = "",
    criteria: list[str] | None = None,
    stall_secs: float = 300,
    preview_url: str = "",
    dev_command: str | None = None,
    dev_port: int | None = None,
    dev_startup_timeout_secs: float = _DEV_SERVER_STARTUP_TIMEOUT_SECS,
    log_root: Path | None = None,
    agent: str = "claude",
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    effort: str | None = None,
) -> AcceptanceVerdict:
    # `{log_root}/{run_id}.log` is the run's tailable log — the acceptance
    # subprocess tees to it in real time (same file the issue-detail API
    # reports `has_log` for).
    log_path = log_root / f"{run_id}.log" if log_root is not None else None
    if mode == _DEV_MODE:
        return await _run_dev_acceptance(
            runner=runner,
            run_id=run_id,
            workspace_path=workspace_path,
            linear_description=linear_description,
            pr_diff_summary=pr_diff_summary,
            taste_guide=taste_guide,
            criteria=criteria,
            stall_secs=stall_secs,
            preview_url=preview_url,
            dev_command=dev_command,
            dev_port=dev_port,
            dev_startup_timeout_secs=dev_startup_timeout_secs,
            log_path=log_path,
            agent=agent,
            codex_model=codex_model,
            claude_model=claude_model,
            effort=effort,
        )
    if mode == _PREVIEW_MODE:
        return await _run_preview_acceptance(
            runner=runner,
            run_id=run_id,
            workspace_path=workspace_path,
            linear_description=linear_description,
            pr_diff_summary=pr_diff_summary,
            taste_guide=taste_guide,
            criteria=criteria,
            stall_secs=stall_secs,
            preview_url=preview_url,
            log_path=log_path,
            agent=agent,
            codex_model=codex_model,
            claude_model=claude_model,
            effort=effort,
        )
    if mode != _CODE_ONLY_MODE:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=0.0,
            hero_screenshot_url="",
            details=_unsupported_mode_details(mode),
        )

    quick_skip = quick_skip_trivial_acceptance(
        linear_description=linear_description,
        pr_diff_summary=pr_diff_summary,
        criteria=criteria,
    )
    if quick_skip is not None:
        return quick_skip

    prompt = build_acceptance_prompt(
        mode=mode,
        linear_description=linear_description,
        pr_diff_summary=pr_diff_summary,
        taste_guide=taste_guide,
        criteria=criteria,
    )
    spec = RunnerSpec(
        run_id=run_id,
        workspace_path=workspace_path,
        command=build_acceptance_command(
            prompt=prompt,
            agent=agent,
            codex_model=codex_model,
            claude_model=claude_model,
            effort=effort,
        ),
        stall_secs=stall_secs,
        stage="acceptance",
    )
    acceptance_run = await _collect_acceptance_output(
        runner,
        spec,
        wall_clock_secs=stall_secs,
        log_path=log_path,
        agent=agent,
        codex_model=codex_model,
    )
    collected = acceptance_run.output
    if acceptance_run.abort_details:
        usage = _usage_delta_with_cost(acceptance_run.usage, acceptance_run.cost)
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=usage.cost_usd,
            hero_screenshot_url="",
            details=acceptance_run.abort_details,
            usage=usage,
        )
    if not collected.ok_exit:
        parsed = acceptance_classifier(
            transcript=collected.stdout,
            criteria=criteria,
        )
        usage = _usage_delta_with_cost(
            acceptance_run.usage,
            max(parsed.cost, acceptance_run.cost),
        )
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=usage.cost_usd,
            hero_screenshot_url="",
            details=_failed_run_details(
                collected,
                parsed_details=(parsed.details if parsed.kind == "infra_error" else ""),
                time_cap_secs=stall_secs,
            ),
            usage=usage,
        )
    return _with_usage(
        acceptance_classifier(
            transcript=collected.stdout,
            criteria=criteria,
        ),
        acceptance_run.usage,
    )


async def _run_dev_acceptance(
    *,
    runner: Runner,
    run_id: str,
    workspace_path: Path,
    linear_description: str,
    pr_diff_summary: str,
    taste_guide: str,
    criteria: list[str] | None,
    stall_secs: float,
    preview_url: str,
    dev_command: str | None,
    dev_port: int | None,
    dev_startup_timeout_secs: float,
    log_path: Path | None = None,
    agent: str = "claude",
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    effort: str | None = None,
) -> AcceptanceVerdict:
    if not dev_command or dev_port is None:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=0.0,
            hero_screenshot_url="",
            details="dev acceptance requires acceptance.dev_command and acceptance.dev_port.",
            preview_url=preview_url,
        )
    resolved_dev_port = await _resolve_dev_port(dev_port)
    resolved_preview_url = (
        preview_url
        if preview_url and resolved_dev_port == dev_port
        else _localhost_url(resolved_dev_port)
    )

    dev_server = await _start_dev_server(
        command=dev_command,
        workspace_path=workspace_path,
        port=resolved_dev_port,
        preview_url=resolved_preview_url,
        startup_timeout_secs=dev_startup_timeout_secs,
    )
    if dev_server.error_details:
        await _stop_dev_server(dev_server)
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=0.0,
            hero_screenshot_url="",
            details=dev_server.error_details,
            preview_url=resolved_preview_url,
        )

    try:
        return await _run_playwright_acceptance(
            runner=runner,
            run_id=run_id,
            workspace_path=workspace_path,
            mode=_DEV_MODE,
            linear_description=linear_description,
            pr_diff_summary=pr_diff_summary,
            taste_guide=taste_guide,
            criteria=criteria,
            stall_secs=stall_secs,
            preview_url=resolved_preview_url,
            log_path=log_path,
            agent=agent,
            codex_model=codex_model,
            claude_model=claude_model,
            effort=effort,
        )
    finally:
        await _stop_dev_server(dev_server)


async def _run_preview_acceptance(
    *,
    runner: Runner,
    run_id: str,
    workspace_path: Path,
    linear_description: str,
    pr_diff_summary: str,
    taste_guide: str,
    criteria: list[str] | None,
    stall_secs: float,
    preview_url: str,
    log_path: Path | None = None,
    agent: str = "claude",
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    effort: str | None = None,
) -> AcceptanceVerdict:
    if not preview_url:
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=0.0,
            hero_screenshot_url="",
            details="preview acceptance requires a resolved preview URL.",
        )
    return await _run_playwright_acceptance(
        runner=runner,
        run_id=run_id,
        workspace_path=workspace_path,
        mode=_PREVIEW_MODE,
        linear_description=linear_description,
        pr_diff_summary=pr_diff_summary,
        taste_guide=taste_guide,
        criteria=criteria,
        stall_secs=stall_secs,
        preview_url=preview_url,
        log_path=log_path,
        agent=agent,
        codex_model=codex_model,
        claude_model=claude_model,
        effort=effort,
    )


async def _run_playwright_acceptance(
    *,
    runner: Runner,
    run_id: str,
    workspace_path: Path,
    mode: str,
    linear_description: str,
    pr_diff_summary: str,
    taste_guide: str,
    criteria: list[str] | None,
    stall_secs: float,
    preview_url: str,
    log_path: Path | None = None,
    agent: str = "claude",
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    effort: str | None = None,
) -> AcceptanceVerdict:
    artifacts_dir = workspace_path / ".symphony" / "acceptance" / run_id
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    mcp_config_path = _write_playwright_mcp_config(
        workspace_path=workspace_path,
        run_id=run_id,
        output_dir=artifacts_dir,
    )
    prompt = build_acceptance_prompt(
        mode=mode,
        linear_description=linear_description,
        pr_diff_summary=pr_diff_summary,
        taste_guide=taste_guide,
        criteria=criteria,
        preview_url=preview_url,
        artifacts_dir=artifacts_dir,
    )
    spec = RunnerSpec(
        run_id=run_id,
        workspace_path=workspace_path,
        command=build_acceptance_command(
            prompt=prompt,
            mode=mode,
            mcp_config_path=mcp_config_path,
            agent=agent,
            codex_model=codex_model,
            claude_model=claude_model,
            effort=effort,
        ),
        env={
            "SYMPHONY_ACCEPTANCE_PREVIEW_URL": preview_url,
            "SYMPHONY_ACCEPTANCE_ARTIFACT_DIR": str(artifacts_dir),
        },
        stall_secs=stall_secs,
        stage="acceptance",
    )
    acceptance_run = await _collect_acceptance_output(
        runner,
        spec,
        wall_clock_secs=stall_secs,
        log_path=log_path,
        agent=agent,
        codex_model=codex_model,
    )
    collected = acceptance_run.output
    if acceptance_run.abort_details:
        usage = _usage_delta_with_cost(acceptance_run.usage, acceptance_run.cost)
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=usage.cost_usd,
            hero_screenshot_url="",
            details=acceptance_run.abort_details,
            preview_url=preview_url,
            usage=usage,
        )
    if not collected.ok_exit:
        parsed = acceptance_classifier(
            transcript=collected.stdout,
            criteria=criteria,
        )
        usage = _usage_delta_with_cost(
            acceptance_run.usage,
            max(parsed.cost, acceptance_run.cost),
        )
        return AcceptanceVerdict(
            kind="infra_error",
            criteria=list(criteria or []),
            cost=usage.cost_usd,
            hero_screenshot_url="",
            details=_failed_run_details(
                collected,
                parsed_details=(parsed.details if parsed.kind == "infra_error" else ""),
                time_cap_secs=stall_secs,
            ),
            preview_url=preview_url,
            usage=usage,
        )
    verdict = acceptance_classifier(
        transcript=collected.stdout,
        criteria=criteria,
    )
    return _with_usage(
        _validate_dev_artifacts(
            verdict if verdict.preview_url else _with_preview_url(verdict, preview_url),
            criteria=criteria,
        ),
        acceptance_run.usage,
    )


@dataclass(frozen=True)
class _AcceptanceRunOutput:
    output: CollectedRunnerOutput
    abort_details: str = ""
    cost: float = 0.0
    usage: UsageDelta = field(default_factory=UsageDelta)


def _sum_usage_delta(left: UsageDelta, right: UsageDelta) -> UsageDelta:
    return UsageDelta(
        cost_usd=left.cost_usd + right.cost_usd,
        input_tokens=left.input_tokens + right.input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
        cache_write_tokens=left.cache_write_tokens + right.cache_write_tokens,
        cache_read_tokens=left.cache_read_tokens + right.cache_read_tokens,
    )


def _usage_delta_with_cost(usage: UsageDelta, cost: float) -> UsageDelta:
    return UsageDelta(
        cost_usd=cost,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cache_write_tokens=usage.cache_write_tokens,
        cache_read_tokens=usage.cache_read_tokens,
    )


def _with_usage(verdict: AcceptanceVerdict, usage: UsageDelta) -> AcceptanceVerdict:
    return replace(verdict, usage=_usage_delta_with_cost(usage, verdict.cost))


@dataclass
class _DevServer:
    process: asyncio.subprocess.Process | None = None
    stdout_lines: list[str] | None = None
    stderr_lines: list[str] | None = None
    pump_tasks: tuple[asyncio.Task[None], ...] = ()
    error_details: str = ""


async def _collect_acceptance_output(
    runner: Runner,
    spec: RunnerSpec,
    *,
    wall_clock_secs: float,
    log_path: Path | None = None,
    agent: str = "claude",
    codex_model: str = DEFAULT_CODEX_MODEL,
) -> _AcceptanceRunOutput:
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    terminal_kind = "exit"
    returncode: int | None = None
    spawn_error: str | None = None
    stall_timeout = False
    tracked_cost = 0.0
    tracked_usage = UsageDelta()
    # Claude self-prices each turn (`cost_usd` is a running total); Codex
    # never prices itself, so the estimator prices cumulative token deltas
    # against `codex_model` (SYM-192) — same idiom as the general runner.
    cost_estimator = UsageCostEstimator(agent=agent, codex_model=codex_model)
    started_at = time.monotonic()
    iterator: AsyncIterator[RunnerEvent] = runner.run(spec).__aiter__()
    # Tee every line to `{log_root}/{run_id}.log` as it arrives so the
    # acceptance run's log grows in real time; strictly additive to the
    # in-memory collection below.
    logf = open_run_log(log_path)

    async def abort(details: str) -> _AcceptanceRunOutput:
        await runner.kill(spec.run_id)
        await _close_iterator(iterator)
        return _AcceptanceRunOutput(
            output=CollectedRunnerOutput(
                stdout="\n".join(stdout_parts),
                stderr="\n".join(stderr_parts),
                terminal_kind="abort",
                returncode=None,
                spawn_error=None,
                stall_timeout=False,
            ),
            abort_details=details,
            cost=tracked_cost,
            usage=_usage_delta_with_cost(tracked_usage, tracked_cost),
        )

    try:
        while True:
            remaining = wall_clock_secs - (time.monotonic() - started_at)
            if remaining <= 0:
                return await abort(_time_cap_exceeded_details(wall_clock_secs))

            next_event: asyncio.Task[RunnerEvent] = asyncio.create_task(
                _next_runner_event(iterator)
            )
            done, _pending = await asyncio.wait({next_event}, timeout=remaining)
            if not done:
                await runner.kill(spec.run_id)
                next_event.cancel()
                with suppress(asyncio.CancelledError):
                    await next_event
                await _close_iterator(iterator)
                return _AcceptanceRunOutput(
                    output=CollectedRunnerOutput(
                        stdout="\n".join(stdout_parts),
                        stderr="\n".join(stderr_parts),
                        terminal_kind="abort",
                        returncode=None,
                        spawn_error=None,
                        stall_timeout=False,
                    ),
                    abort_details=_time_cap_exceeded_details(wall_clock_secs),
                    cost=tracked_cost,
                    usage=_usage_delta_with_cost(tracked_usage, tracked_cost),
                )

            try:
                event = next_event.result()
            except StopAsyncIteration:
                break

            if event.kind == "stdout" and event.line is not None:
                stdout_parts.append(event.line)
                tee_run_log(logf, event.line)
                usage = parse_event_line(event.line)
                if usage is not None:
                    tracked_usage = _sum_usage_delta(tracked_usage, cost_estimator.delta(usage))
                    tracked_cost = tracked_usage.cost_usd
            elif event.kind == "stderr" and event.line is not None:
                stderr_parts.append(event.line)
                tee_run_log(logf, event.line, stderr=True)
            elif event.kind == "exit":
                terminal_kind = "exit"
                returncode = event.returncode
                break
            elif event.kind == "stall_timeout":
                terminal_kind = "stall_timeout"
                stall_timeout = True
                break
            elif event.kind == "spawn_failed":
                terminal_kind = "spawn_failed"
                spawn_error = event.error
                break

        await _close_iterator(iterator)
        return _AcceptanceRunOutput(
            output=CollectedRunnerOutput(
                stdout="\n".join(stdout_parts),
                stderr="\n".join(stderr_parts),
                terminal_kind=terminal_kind,
                returncode=returncode,
                spawn_error=spawn_error,
                stall_timeout=stall_timeout,
            ),
            cost=tracked_cost,
            usage=_usage_delta_with_cost(tracked_usage, tracked_cost),
        )
    finally:
        if logf is not None:
            logf.close()


async def _close_iterator(iterator: object) -> None:
    aclose = getattr(iterator, "aclose", None)
    if aclose is None:
        return
    with suppress(Exception):  # noqa: BLE001
        await aclose()


async def _next_runner_event(
    iterator: AsyncIterator[RunnerEvent],
) -> RunnerEvent:
    return await iterator.__anext__()


async def _start_dev_server(
    *,
    command: str,
    workspace_path: Path,
    port: int,
    startup_timeout_secs: float,
    preview_url: str | None = None,
) -> _DevServer:
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    try:
        proc = await asyncio.create_subprocess_shell(
            command,
            cwd=workspace_path,
            env=_dev_server_env(port=port, preview_url=preview_url or _localhost_url(port)),
            start_new_session=True,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
    except (OSError, FileNotFoundError) as e:
        return _DevServer(error_details=f"dev server spawn_failed: {type(e).__name__}: {e}")

    tasks = (
        asyncio.create_task(_pump_dev_stream(proc.stdout, stdout_lines)),
        asyncio.create_task(_pump_dev_stream(proc.stderr, stderr_lines)),
    )
    server = _DevServer(
        process=proc,
        stdout_lines=stdout_lines,
        stderr_lines=stderr_lines,
        pump_tasks=tasks,
    )
    deadline = time.monotonic() + startup_timeout_secs
    while time.monotonic() < deadline:
        if proc.returncode is not None:
            server.error_details = _dev_server_exit_details(proc, stdout_lines, stderr_lines)
            return server
        if await _port_reachable("127.0.0.1", port):
            return server
        await asyncio.sleep(0.1)
    server.error_details = (
        "dev server did not become reachable on "
        f"127.0.0.1:{port} within {startup_timeout_secs:.1f}s."
    )
    return server


async def _pump_dev_stream(
    stream: asyncio.StreamReader | None,
    lines: list[str],
    *,
    limit: int = 40,
) -> None:
    if stream is None:
        return
    while True:
        raw = await stream.readline()
        if not raw:
            break
        if len(lines) < limit:
            lines.append(raw.decode(errors="replace").rstrip("\n"))


async def _stop_dev_server(server: _DevServer) -> None:
    proc = server.process
    if proc is None:
        return
    if proc.returncode is None:
        with suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), timeout=_DEV_SERVER_STOP_TIMEOUT_SECS)
        except TimeoutError:
            with suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            with suppress(Exception):
                await proc.wait()
    for task in server.pump_tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*server.pump_tasks, return_exceptions=True)


async def _port_reachable(host: str, port: int) -> bool:
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except OSError:
        return False
    writer.close()
    with suppress(Exception):
        await writer.wait_closed()
    del reader
    return True


async def _resolve_dev_port(preferred_port: int) -> int:
    if not await _port_reachable("127.0.0.1", preferred_port):
        return preferred_port
    return _unused_dev_port()


def _unused_dev_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _localhost_url(port: int) -> str:
    return f"http://127.0.0.1:{port}"


def _dev_server_env(*, port: int, preview_url: str) -> dict[str, str]:
    env = os.environ.copy()
    port_text = str(port)
    env.update(
        {
            "PORT": port_text,
            "SYMPHONY_ACCEPTANCE_DEV_PORT": port_text,
            "SYMPHONY_ACCEPTANCE_PREVIEW_URL": preview_url,
        }
    )
    return env


def _dev_server_exit_details(
    proc: asyncio.subprocess.Process,
    stdout_lines: list[str],
    stderr_lines: list[str],
) -> str:
    lines = stderr_lines or stdout_lines
    tail = "\n".join(lines[-5:]).strip()
    suffix = f"\n{tail}" if tail else ""
    return f"dev server exited before binding rc={proc.returncode}.{suffix}"


def _write_playwright_mcp_config(
    *,
    workspace_path: Path,
    run_id: str,
    output_dir: Path,
) -> Path:
    config_dir = workspace_path / ".symphony" / "acceptance" / run_id
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "playwright-mcp.json"
    config = {
        "mcpServers": {
            "playwright": {
                "command": "npx",
                "args": [
                    "@playwright/mcp@latest",
                    "--headless",
                    "--isolated",
                    "--viewport-size=1280x720",
                    "--output-dir",
                    str(output_dir),
                ],
            }
        }
    }
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return config_path


def _with_preview_url(
    verdict: AcceptanceVerdict,
    preview_url: str,
) -> AcceptanceVerdict:
    return replace(verdict, preview_url=preview_url)


def _validate_dev_artifacts(
    verdict: AcceptanceVerdict,
    *,
    criteria: list[str] | None,
) -> AcceptanceVerdict:
    expected_criteria = list(criteria or [])
    if verdict.kind == "pass":
        hero = [item for item in verdict.screenshots if item.kind == "hero"]
        if len(hero) != 1:
            return AcceptanceVerdict(
                kind="infra_error",
                criteria=list(criteria or []),
                cost=verdict.cost,
                hero_screenshot_url="",
                details="dev acceptance pass must include exactly one hero screenshot.",
                preview_url=verdict.preview_url,
            )
        if expected_criteria and not verdict.criterion_results:
            return AcceptanceVerdict(
                kind="infra_error",
                criteria=expected_criteria,
                cost=verdict.cost,
                hero_screenshot_url="",
                details="dev acceptance pass must include per-criterion results.",
                preview_url=verdict.preview_url,
            )
        reported = {item.criterion.casefold() for item in verdict.criterion_results}
        missing = [
            criterion for criterion in expected_criteria if criterion.casefold() not in reported
        ]
        if missing:
            return AcceptanceVerdict(
                kind="infra_error",
                criteria=expected_criteria,
                cost=verdict.cost,
                hero_screenshot_url="",
                details=(f"dev acceptance pass did not report criteria: {', '.join(missing)}"),
                preview_url=verdict.preview_url,
            )
        failed = [item.criterion for item in verdict.criterion_results if not item.passed]
        if failed:
            return AcceptanceVerdict(
                kind="infra_error",
                criteria=list(criteria or []),
                cost=verdict.cost,
                hero_screenshot_url="",
                details=(f"dev acceptance pass reported failed criteria: {', '.join(failed)}"),
                preview_url=verdict.preview_url,
            )
    if verdict.kind == "reject":
        if criteria and not verdict.criterion_results:
            return AcceptanceVerdict(
                kind="infra_error",
                criteria=list(criteria),
                cost=verdict.cost,
                hero_screenshot_url="",
                details="dev acceptance reject must include per-criterion results.",
                preview_url=verdict.preview_url,
            )
        if criteria and not any(not item.passed for item in verdict.criterion_results):
            return AcceptanceVerdict(
                kind="infra_error",
                criteria=list(criteria),
                cost=verdict.cost,
                hero_screenshot_url="",
                details="dev acceptance reject must identify at least one failed criterion.",
                preview_url=verdict.preview_url,
            )
        missing = [
            item.criterion
            for item in verdict.criterion_results
            if not item.passed and not item.screenshot_path
        ]
        if missing:
            return AcceptanceVerdict(
                kind="infra_error",
                criteria=list(criteria or []),
                cost=verdict.cost,
                hero_screenshot_url="",
                details=(
                    "dev acceptance reject is missing screenshots for failed "
                    f"criteria: {', '.join(missing)}"
                ),
                preview_url=verdict.preview_url,
            )
    return verdict


def build_acceptance_command(
    *,
    prompt: str,
    mode: str = _CODE_ONLY_MODE,
    mcp_config_path: Path | None = None,
    agent: str = "claude",
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    effort: str | None = None,
) -> list[str]:
    """argv for the acceptance-verdict subprocess (the `accept` role, SYM-192).

    `claude_model`/`effort` are the resolved `accept` role's Claude model /
    reasoning effort: set → `--model`/`--effort` flag, unset → no flag (CLI
    default). Both are ignored for codex, which takes `codex_model`/`effort`
    the same way `build_runner_command` does.

    Codex has no `--mcp-config` flag (`mcp_config_path` is Claude-only, used
    for the Playwright MCP server on dev/preview modes); codex's own MCP
    wiring lives in its config.toml, unaffected here — same convention as the
    implement/fix/merge command builders.
    """
    if agent == "codex":
        command = ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "--json"]
        if effort is not None:
            command.extend(["--config", f'model_reasoning_effort="{effort}"'])
        command.extend(["--model", codex_model, prompt])
        return command
    command = [
        "claude",
        "--print",
        "--output-format",
        "stream-json",
        "--verbose",
        "--permission-mode",
        _CLAUDE_ACCEPTANCE_PERMISSION_MODE,
        "--strict-mcp-config",
        "--disallowedTools",
        (
            _CLAUDE_DEV_ACCEPTANCE_DISALLOWED_TOOLS
            if mode in _PLAYWRIGHT_MODES
            else _CLAUDE_ACCEPTANCE_DISALLOWED_TOOLS
        ),
    ]
    if mode in _PLAYWRIGHT_MODES:
        if mcp_config_path is None:
            raise ValueError("visual acceptance requires a Playwright MCP config path")
        command.extend(
            [
                "--mcp-config",
                str(mcp_config_path),
                "--allowedTools",
                "mcp__playwright__*",
            ]
        )
    if claude_model is not None:
        command.extend(["--model", claude_model])
    if effort is not None:
        command.extend(["--effort", effort])
    command.append(prompt)
    return command


def build_acceptance_prompt(
    *,
    mode: str,
    linear_description: str,
    pr_diff_summary: str,
    taste_guide: str = "",
    criteria: list[str] | None = None,
    preview_url: str = "",
    artifacts_dir: Path | None = None,
) -> str:
    if mode in _PLAYWRIGHT_MODES:
        if not preview_url or artifacts_dir is None:
            raise ValueError("visual acceptance requires preview_url and artifacts_dir")
        return _build_dev_acceptance_prompt(
            mode=mode,
            linear_description=linear_description,
            pr_diff_summary=pr_diff_summary,
            taste_guide=taste_guide,
            criteria=criteria,
            preview_url=preview_url,
            artifacts_dir=artifacts_dir,
        )
    if mode != _CODE_ONLY_MODE:
        raise ValueError(_unsupported_mode_details(mode))

    description = linear_description.strip() or "(no Linear description)"
    diff = _truncate_diff(pr_diff_summary.strip() or "(no PR diff available)")
    taste_guide_section = _taste_guide_section(taste_guide)
    criteria_section = _criteria_section(criteria)
    return (
        "You are Symphony's Acceptance-stage agent. Your only job is to "
        "decide whether the PR diff satisfies the Linear ticket description.\n\n"
        "# Mode\n\n"
        f"mode: {mode}\n\n"
        "# Mode-specific instructions for code_only\n\n"
        "## First phase: quick-skip decision\n\n"
        "- Read only the Linear description and PR diff summary first.\n"
        "- Decide trivial / non-trivial before any other acceptance work.\n"
        "- Classify as trivial only when there is no user-visible behavior "
        "worth verifying, such as docs-only typos, dependency bumps, renames, "
        "or internal refactors with no behavior change.\n"
        "- If trivial, skip criteria extraction, Playwright, browser "
        "automation, dev servers, previews, screenshots, tests, and taste "
        "guides. Emit a pass verdict with reason quick_skip_trivial using "
        "this exact footer:\n"
        "<!-- symphony-acceptance-verdict: pass reason=quick_skip_trivial -->\n"
        "- If in doubt, classify as non-trivial and proceed with the full "
        "code_only check below.\n\n"
        "## Full code_only check for non-trivial tickets\n\n"
        "- Compare only the Linear description and the PR diff summary below.\n"
        "- Reject when the diff obviously implements something different from "
        "the requested behavior.\n"
        "- Pass when the diff and description agree at the code level.\n"
        "- If the taste guide below contains hard rules and the PR diff violates "
        "one, reject and cite the specific taste-guide rule.\n"
        "- Do not run Playwright, browser automation, a dev server, or tests.\n"
        "- Do not inspect screenshots or preview URLs.\n"
        "- Do not modify files, commit, push, or merge anything.\n\n"
        f"{taste_guide_section}"
        f"{criteria_section}"
        "# Linear description\n\n"
        f"{description}\n\n"
        "# PR diff summary\n\n"
        "```diff\n"
        f"{diff}\n"
        "```\n\n"
        "# Response format\n\n"
        "Write a short rationale. End your final message with EXACTLY ONE of "
        "these footers on its own line:\n\n"
        f"{ACCEPTANCE_FOOTER_PASS}\n"
        f"{ACCEPTANCE_FOOTER_REJECT}\n"
    )


def _build_dev_acceptance_prompt(
    *,
    mode: str,
    linear_description: str,
    pr_diff_summary: str,
    taste_guide: str,
    criteria: list[str] | None,
    preview_url: str,
    artifacts_dir: Path,
) -> str:
    description = linear_description.strip() or "(no Linear description)"
    diff = _truncate_diff(pr_diff_summary.strip() or "(no PR diff available)")
    taste_guide_section = _taste_guide_section(taste_guide)
    criteria_section = _criteria_section(criteria)
    artifact_dir_text = str(artifacts_dir)
    return (
        "You are Symphony's Acceptance-stage agent. Your only job is to "
        "visually decide whether the live UI satisfies the Linear ticket.\n\n"
        "# Mode\n\n"
        f"mode: {mode}\n\n"
        f"# Mode-specific instructions for {mode}\n\n"
        f"- Open the live app at {preview_url} using the Playwright MCP tools.\n"
        "- Use a single desktop viewport. Do not perform multi-viewport checks.\n"
        "- Verify each extracted acceptance criterion visually against the "
        "live UI.\n"
        "- Capture exactly one hero screenshot when all criteria pass. It must "
        "show the primary view you verified.\n"
        "- If any criterion fails, capture one screenshot for each failed "
        "criterion, showing the failing state.\n"
        f"- Save screenshots as PNG files under `{artifact_dir_text}`.\n"
        "- Do not edit files, commit, push, or merge anything.\n"
        "- End your final response with exactly one verdict footer and include "
        "one JSON artifact block immediately before the footer:\n\n"
        "<!-- symphony-acceptance-artifacts\n"
        "{\n"
        f'  "preview_url": "{preview_url}",\n'
        '  "hero_screenshot": "relative/or/absolute/path.png",\n'
        '  "criteria": [\n'
        '    {"criterion": "criterion text", "passed": true},\n'
        '    {"criterion": "failed criterion text", "passed": false, '
        '"screenshot": "relative/or/absolute/path.png"}\n'
        "  ]\n"
        "}\n"
        "-->\n\n"
        f"{taste_guide_section}"
        f"{criteria_section}"
        "# Linear description\n\n"
        f"{description}\n\n"
        "# PR diff summary\n\n"
        "```diff\n"
        f"{diff}\n"
        "```\n\n"
        "# Response format\n\n"
        "Write a short rationale. End your final message with EXACTLY ONE of "
        "these footers on its own line:\n\n"
        f"{ACCEPTANCE_FOOTER_PASS}\n"
        f"{ACCEPTANCE_FOOTER_REJECT}\n"
    )


def _taste_guide_section(taste_guide: str) -> str:
    content = taste_guide.strip()
    if not content:
        return ""
    return (
        "# Taste guide\n\n"
        "Global guide content appears first. Per-binding guide content appears "
        "after it and may add stricter rules, but it cannot override or silence "
        "global hard rules.\n\n"
        f"{content}\n\n"
    )


def _criteria_section(criteria: list[str] | None) -> str:
    items = [item.strip() for item in criteria or [] if item.strip()]
    if not items:
        return (
            "# Extracted acceptance criteria\n\n"
            "No verifiable criteria were extracted; fall back to matching the "
            "overall Linear description against the PR diff.\n\n"
        )
    body = "# Extracted acceptance criteria\n\n"
    for item in items:
        body += f"- {item}\n"
    return f"{body}\n"


def quick_skip_trivial_acceptance(
    *,
    linear_description: str,
    pr_diff_summary: str,
    criteria: list[str] | None = None,
) -> AcceptanceVerdict | None:
    """Return a pass verdict for obvious no-behavior tickets.

    This deliberately handles only low-risk cases. Ambiguous tickets fall
    through to the full acceptance agent prompt.
    """
    description = linear_description.strip()
    diff = pr_diff_summary.strip()
    if not description or not diff:
        return None
    paths = _changed_paths(diff)
    if not paths or not _is_exclusively_trivial_description(description):
        return None

    lower_description = description.casefold()
    if all(_is_doc_path(path) for path in paths):
        return _quick_skip_verdict(criteria)
    if all(_is_dependency_path(path) for path in paths) and _DEPENDENCY_DESCRIPTION_RE.search(
        lower_description
    ):
        return _quick_skip_verdict(criteria)
    if _INTERNAL_ONLY_DESCRIPTION_RE.search(lower_description) and not any(
        _looks_user_facing_path(path) for path in paths
    ):
        return _quick_skip_verdict(criteria)
    return None


def _is_exclusively_trivial_description(description: str) -> bool:
    lines = _meaningful_description_lines(description)
    return bool(lines) and all(_is_trivial_description_line(line) for line in lines)


def _meaningful_description_lines(description: str) -> list[str]:
    lines: list[str] = []
    in_fence = False
    for raw_line in description.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            lines.append(line)
            continue
        if _DESCRIPTION_TEMPLATE_HEADING_RE.fullmatch(line):
            continue
        line = _MARKDOWN_HEADING_RE.sub("", line).strip()
        line = _MARKDOWN_LIST_RE.sub("", line).strip()
        line = _MARKDOWN_CHECKBOX_RE.sub("", line).strip()
        if line:
            lines.append(line)
    return lines


def _is_trivial_description_line(line: str) -> bool:
    if _EXPLICIT_TRIVIAL_DESCRIPTION_LINE_RE.fullmatch(line):
        return True
    if _NON_TRIVIAL_DESCRIPTION_RE.search(line):
        return False
    return any(pattern.fullmatch(line) for pattern in _TRIVIAL_DESCRIPTION_LINE_RES)


def _quick_skip_verdict(criteria: list[str] | None) -> AcceptanceVerdict:
    return AcceptanceVerdict(
        kind="pass",
        criteria=list(criteria or []),
        cost=0.0,
        hero_screenshot_url="",
        details=_QUICK_SKIP_DETAILS,
        reason=ACCEPTANCE_REASON_QUICK_SKIP_TRIVIAL,
    )


def _changed_paths(diff: str) -> list[str]:
    paths: list[str] = []
    for line in diff.splitlines():
        if not line.startswith("diff --git "):
            continue
        path = _diff_git_new_path(line[len("diff --git ") :])
        if path is None:
            continue
        if path and path != "/dev/null":
            paths.append(path)
    return paths


def _diff_git_new_path(header: str) -> str | None:
    prefix = "a/"
    separator = " b/"
    if not header.startswith(prefix):
        return None

    rest = header[len(prefix) :]
    offset = 0
    while True:
        index = rest.find(separator, offset)
        if index == -1:
            break
        old_path = rest[:index]
        new_path = rest[index + len(separator) :]
        if old_path == new_path:
            return new_path
        offset = index + 1

    index = rest.find(separator)
    if index == -1:
        return None
    new_path = rest[index + len(separator) :]
    if separator in new_path:
        return None
    return new_path


def _is_doc_path(path: str) -> bool:
    path_obj = Path(path)
    parts = {part.casefold() for part in path_obj.parts}
    if {"doc", "docs", "documentation"} & parts:
        return True
    stem = path_obj.stem.casefold()
    suffix = path_obj.suffix.casefold()
    return stem in _DOC_FILENAMES or suffix in _DOC_EXTENSIONS


def _is_dependency_path(path: str) -> bool:
    return Path(path).name.casefold() in _DEP_FILENAMES


def _looks_user_facing_path(path: str) -> bool:
    parts = {part.casefold() for part in Path(path).parts}
    return bool(parts & _USER_FACING_PATH_PARTS)


def _truncate_diff(diff: str) -> str:
    if len(diff) <= _DIFF_LIMIT_CHARS:
        return diff
    return diff[:_DIFF_LIMIT_CHARS] + "\n...[truncated]"


def _unsupported_mode_details(mode: str) -> str:
    return (
        f"Acceptance mode {mode!r} is not supported by the Claude code-only "
        "runner; only 'code_only' can run without dev or preview artifacts."
    )


def _failed_run_details(
    collected: CollectedRunnerOutput,
    *,
    parsed_details: str = "",
    time_cap_secs: float = 0.0,
) -> str:
    if parsed_details:
        return _prefix_cap_details(parsed_details)
    if collected.terminal_kind == "spawn_failed":
        return f"Acceptance runner spawn_failed: {collected.spawn_error or 'unknown'}"
    if collected.stall_timeout:
        return _time_cap_exceeded_details(time_cap_secs)
    return f"Acceptance runner exited rc={collected.returncode}."


def _time_cap_exceeded_details(time_cap_secs: float) -> str:
    minutes = time_cap_secs / 60.0
    return (
        "time_cap_exceeded: acceptance exceeded wall-clock cap "
        f"of {minutes:.2f} minutes ({time_cap_secs:.1f}s)."
    )


def _prefix_cap_details(details: str) -> str:
    lower = details.lower()
    if "time_cap_exceeded" in lower:
        return details
    if "timeout" in lower or "timed out" in lower or "time cap" in lower:
        return f"time_cap_exceeded: {details}"
    return details


__all__ = [
    "build_acceptance_command",
    "build_acceptance_prompt",
    "quick_skip_trivial_acceptance",
    "run_acceptance",
]
