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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from ..agent.prompt import review_comment_fix_prompt
from ..agent.runner import Runner, RunnerSpec
from .local_review_io import collect_runner_output
from .local_review_session import (
    ImplementerAgent,
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
    """
    proc = await asyncio.create_subprocess_shell(
        verify_cmd,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        stdin=asyncio.subprocess.DEVNULL,
    )
    try:
        stdout, _ = await asyncio.wait_for(
            proc.communicate(), timeout=timeout_secs
        )
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return False, f"verify_cmd timed out after {timeout_secs}s"
    return proc.returncode == 0, stdout.decode("utf-8", errors="replace")


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
    implementer_agent: ImplementerAgent,
    implementer_codex_model: str,
    stall_secs: int,
    command_secs: int = 1800,
    command_runner: VerifyCommandRunner = run_verify_command,
) -> VerifyResult:
    """Run the verify gate: verify → (one fix turn → verify again) on red."""
    ok, output = await command_runner(workspace_path, verify_cmd, timeout_secs)
    if ok:
        return VerifyResult(ok=True)

    tail = output_tail(output)
    prompt = review_comment_fix_prompt(
        issue_title=issue_title,
        issue_body=issue_body,
        labels=labels,
        trigger=_verify_fix_trigger(verify_cmd, tail),
    )
    spec = RunnerSpec(
        run_id=_safe_run_id(parent_run_id, "verify-fix"),
        workspace_path=workspace_path,
        command=_build_fix_command(
            agent=implementer_agent,
            codex_model=implementer_codex_model,
            prompt=prompt,
        ),
        stall_secs=stall_secs,
        command_secs=command_secs,
        stage="verify_fix",
    )
    collected = await collect_runner_output(runner, spec)
    if not collected.ok_exit:
        detail = (
            f"spawn_failed: {collected.spawn_error or 'unknown'}"
            if collected.terminal_kind == "spawn_failed"
            else "fix-run stalled"
            if collected.stall_timeout
            else f"fix-run exited rc={collected.returncode}"
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
