"""Pre-push verify gate: run the binding's `verify_cmd` in the workspace.

The local-review loop only sees what the reviewer reads; nothing in the
pipeline proves the project still *builds*. `verify_cmd` (e.g.
`pnpm build && pnpm test`) closes that gap: it runs after the last
code-mutating stage and before push. Green proceeds; red gets exactly
one implementer fix turn seeded with the tail of the failing output
(same fix-run machinery local review uses), then a re-run. Still red is
the caller's signal to fail closed — no push, no PR.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from ..agent.process import Usage
from ..agent.prompt import review_comment_fix_prompt
from ..agent.runner import Runner, RunnerSpec
from ..config import ResolvedRole
from .local_review_io import collect_runner_output
from .local_review_session import (
    _build_fix_command,
    _safe_run_id,
)

# Cap on how much failing output is fed to the fix turn and posted to
# Linear. The interesting part of a build/test failure is the end.
VERIFY_TAIL_CHARS = 4000

# (workspace_path, verify_cmd, timeout_secs) -> (ok, combined output).
# Injectable so the session is testable without real subprocesses.
VerifyCommandRunner = Callable[[Path, str, int], Awaitable[tuple[bool, str]]]


@dataclass(frozen=True)
class VerifyResult:
    """Outcome of the verify gate. `ok=False` means fail-closed."""

    ok: bool
    fix_attempted: bool = False
    tail: str = ""
    error: str = ""


def output_tail(output: str, max_chars: int = VERIFY_TAIL_CHARS) -> str:
    return output.strip()[-max_chars:]


async def run_verify_command(
    workspace_path: Path, verify_cmd: str, timeout_secs: int
) -> tuple[bool, str]:
    """Run `verify_cmd` through the shell in the workspace.

    Stdout and stderr are interleaved into one stream — build tools split
    diagnostics across both arbitrarily, and the fix turn wants the
    combined tail. A timeout kills the process and counts as red.

    `start_new_session=True` puts the shell in its own process group so a
    timeout can SIGKILL the whole tree (`os.killpg`): `proc.kill()` alone
    only reaps the `/bin/sh -c` shell, leaving the spawned `node`/`pnpm`
    grandchildren orphaned and still chewing through the workspace.
    """
    proc = await asyncio.create_subprocess_shell(
        verify_cmd,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        stdin=asyncio.subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_secs)
    except TimeoutError:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass  # whole group already gone
        await proc.wait()
        return False, f"verify_cmd timed out after {timeout_secs}s"
    return proc.returncode == 0, stdout.decode("utf-8", errors="replace")


def _write_fix_log(fix_log_path: Path | None, text: str) -> None:
    if fix_log_path is None:
        return
    # `/api/runs/{id}/stream` only emits an event once it sees a trailing
    # `\n` (it buffers an unterminated final line forever, waiting for more
    # bytes that never arrive) — without this, the last line a fix turn ever
    # writes never reaches the UI.
    if text and not text.endswith("\n"):
        text += "\n"
    try:
        fix_log_path.parent.mkdir(parents=True, exist_ok=True)  # noqa: ASYNC240
        fix_log_path.write_text(text, encoding="utf-8")  # noqa: ASYNC240
    except OSError:
        pass  # per-model attribution is best-effort


def _fix_log_note(text: str) -> str:
    """Format a placeholder note as a line the stream endpoint can parse.

    `/api/runs/{id}/stream` only renders events `parse_stream_events`
    recovers from agent JSONL; a plain-text placeholder yields no events
    and the LiveFeed shows nothing. Piggyback on codex's `item.completed`
    agent-message shape — unlike claude's `result` kind, it doesn't also
    trip `parse_event_line`'s usage parsing, which would otherwise emit a
    spurious all-zero token tick alongside the message.
    """
    return json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": text}})


def _verify_fix_trigger(verify_cmd: str, tail: str) -> str:
    return (
        f"The verify command `{verify_cmd}` failed in the workspace. "
        "Make it pass.\n\n"
        "Tail of the failing output:\n\n"
        f"```\n{tail}\n```"
    )


async def run_verify_session(
    *,
    runner: Runner,
    workspace_path: Path,
    verify_cmd: str,
    timeout_secs: int,
    parent_run_id: str,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    fixer_role: ResolvedRole,
    stall_secs: int,
    command_secs: int = 1800,
    wall_clock_secs: int = 0,
    command_runner: VerifyCommandRunner = run_verify_command,
    usage_handler: Callable[[Usage], object] | None = None,
    fix_log_path: Path | None = None,
    allow_fixes: bool = True,
    # Per-run credential env for the agent fix turn (e.g. CLAUDE_CONFIG_DIR,
    # Config v2 3/9), resolved lazily: a green verify_cmd never launches an
    # agent, so it must not materialize (or refresh) credentials at all.
    extra_env_provider: Callable[[], Awaitable[dict[str, str]]] | None = None,
) -> VerifyResult:
    """Run the verify gate: verify → (one fix turn → verify again) on red.

    `usage_handler` (e.g. `UsageCostEstimator.delta`) is threaded into the
    fix turn's `collect_runner_output` so its token/cost spend is billed to
    the issue instead of vanishing. `fix_log_path`, when set, receives the
    fix turn's stdout so the caller can attribute per-model usage from it.
    """
    ok, output = await command_runner(workspace_path, verify_cmd, timeout_secs)
    if ok:
        _write_fix_log(
            fix_log_path,
            _fix_log_note("verify_cmd passed on first attempt; no fix turn was run."),
        )
        return VerifyResult(ok=True)

    tail = output_tail(output)
    if not allow_fixes:
        _write_fix_log(
            fix_log_path,
            _fix_log_note("verify_cmd failed; fix turn disabled for publish resume."),
        )
        return VerifyResult(
            ok=False,
            tail=tail,
            error="verify_cmd failed; fix turn disabled for publish resume",
        )

    prompt = review_comment_fix_prompt(
        issue_title=issue_title,
        issue_body=issue_body,
        labels=labels,
        trigger=_verify_fix_trigger(verify_cmd, tail),
    )
    spec = RunnerSpec(
        run_id=_safe_run_id(parent_run_id, "verify-fix"),
        workspace_path=workspace_path,
        env=dict(await extra_env_provider() if extra_env_provider is not None else {}),
        command=_build_fix_command(
            agent=fixer_role.agent,
            codex_model=fixer_role.codex_model_arg(),
            prompt=prompt,
            claude_model=fixer_role.claude_model_arg(),
            effort=fixer_role.effort,
        ),
        stall_secs=stall_secs,
        command_secs=command_secs,
        wall_clock_secs=wall_clock_secs,
        stage="verify_fix",
    )
    # The fix turn tees its output to `fix_log_path` (== `{run_id}.log`) as it
    # arrives, so the verify run's log grows in real time. `_write_fix_log`
    # below only covers the no-output cases, where a parseable note must
    # replace whatever the tee did (or didn't) write.
    collected = await collect_runner_output(
        runner, spec, usage_handler=usage_handler, log_path=fix_log_path
    )
    if not collected.ok_exit:
        detail = (
            f"spawn_failed: {collected.spawn_error or 'unknown'}"
            if collected.terminal_kind == "spawn_failed"
            else "fix-run stalled"
            if collected.stall_timeout
            else f"fix-run exited rc={collected.returncode}"
        )
        # The fix turn can die before emitting any stdout (missing agent
        # binary, CLI/auth errors that land only on stderr) — fall back to a
        # parseable note so the stream endpoint has an event to render
        # instead of an empty log the UI waits on forever.
        if not collected.stdout:
            _write_fix_log(
                fix_log_path,
                _fix_log_note(f"verify-fix turn produced no output ({detail})."),
            )
        return VerifyResult(
            ok=False,
            fix_attempted=True,
            tail=tail,
            error=f"verify_cmd failed and the fix turn did not finish ({detail})",
        )

    ok, output = await command_runner(workspace_path, verify_cmd, timeout_secs)
    if ok:
        return VerifyResult(ok=True, fix_attempted=True)
    return VerifyResult(
        ok=False,
        fix_attempted=True,
        tail=output_tail(output),
        error="verify_cmd still failing after one fix turn",
    )


__all__ = [
    "VerifyCommandRunner",
    "VerifyResult",
    "run_verify_command",
    "run_verify_session",
]
