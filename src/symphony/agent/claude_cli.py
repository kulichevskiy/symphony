"""Shared claude CLI argv pieces for mutating (builder) runs.

Lives in `agent/` so both the orchestrator's command builders and the
pipeline's in-session mirror (`local_review_session._build_fix_command`)
can import it without creating a pipeline→orchestrator import cycle.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

# Pre-approved tool surface for mutating claude runs (implement / fix /
# merge / acceptance-fix — every claude spawn that must change the
# workspace). Without an explicit allowlist the spawn depends on whatever
# permission rules live in the operator's ambient ~/.claude — absent in the
# containerized deployment, where a fresh auth volume means every
# Edit/Write/Bash is auto-denied and the run parks blocked. Bare "Bash"
# approves all commands: builder runs execute repo-specific test/build
# commands plus git commit/push. The read-only reviewer keeps its own narrow
# allowlist in pipeline/local_review.py.
CLAUDE_BUILDER_TOOLS: tuple[str, ...] = (
    "Bash",
    "Edit",
    "Write",
    "MultiEdit",
    "NotebookEdit",
    "WebFetch",
    "WebSearch",
)


def claude_builder_allowed_tools(mcp_servers: Mapping[str, Any] | None = None) -> str:
    """Comma-joined `--allowedTools` value for a mutating claude run.

    When the binding grants MCP servers, each gets an `mcp__<name>__*` allow
    rule (the pattern the Playwright acceptance runner already uses):
    `--strict-mcp-config` only makes the server visible — without an allow
    rule its tool calls would still prompt/deny in the fresh-auth headless
    environment. Single comma-joined arg; callers must put the prompt behind
    `--` (the SYM-42 idiom — a variadic --allowedTools would swallow it).
    """
    entries = list(CLAUDE_BUILDER_TOOLS)
    for name in mcp_servers or {}:
        entries.append(f"mcp__{name}__*")
    return ",".join(entries)


# --- PreToolUse deny-hook for builder runs (SYM-224) ----------------------
# Builder spawns dispatch one-shot (`claude --print`, no resume), so an agent
# that defers work behind a self-wakeup or a background task strands the run:
# the commit never lands, HEAD stays put, the issue parks in Needs Input.
# A prompt is advisory; a PreToolUse deny-hook is enforcement. The hook script
# is stdlib-only, ships in the container image under this package (Dockerfile
# `COPY src/ ./src/`), and is invoked by the claude CLI — not imported here.
BUILDER_DENY_HOOK_SCRIPT: Path = (
    Path(__file__).resolve().parent / "hooks" / "deny_builder_background_tasks.py"
)

# Empty = load NO ambient setting sources for the builder run, so our inline
# `--settings` is the only layer: a project `.claude/settings.json` can neither
# add `disableAllHooks: true` (which would silence our deny-hook) nor otherwise
# interfere. Mirrors the read-only reviewer's hermetic delivery.
BUILDER_SETTING_SOURCES = ""


def claude_builder_settings() -> str:
    """Inline `--settings` JSON for a mutating claude run.

    Registers a PreToolUse deny-hook over all tools that blocks the
    background-task machinery a one-shot dispatch cannot honor (background
    Bash, ScheduleWakeup, BashOutput, KillShell, and the cron/monitor
    surface). We do NOT set `disableAllHooks` here — that would silence the
    very hook we're adding (the read-only reviewer keeps its own
    `disableAllHooks` settings).

    The hook `matcher` is a regex matched against `tool_name`, not a glob:
    `"*"` is an invalid "match nothing/one-or-more-of-nothing" regex, so the
    hook would silently never fire. `""` is the documented all-tools matcher.

    Also sets `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1`: without it, a
    foreground Bash call that runs past its timeout is auto-backgrounded by
    the CLI itself (no `run_in_background` on the call for our hook to
    catch), and `BashOutput`/`KillShell` to retrieve/stop it are denied —
    stranding the run. This env var disables that auto-background conversion
    (and `run_in_background`) outright.
    """
    return json.dumps(
        {
            "env": {"CLAUDE_CODE_DISABLE_BACKGROUND_TASKS": "1"},
            "hooks": {
                "PreToolUse": [
                    {
                        "matcher": "",
                        "hooks": [
                            {
                                "type": "command",
                                "command": f"{sys.executable} {BUILDER_DENY_HOOK_SCRIPT}",
                            }
                        ],
                    }
                ]
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )


# --- Control-channel dispatch (SYM-236) -----------------------------------
# The CLI only asks its host for a replacement token when all three of these
# are set, which is why they travel together as one dict rather than as three
# call sites nobody can check against each other:
#
#   * `..._SDK_HAS_OAUTH_REFRESH` is the declaration that the host can answer
#     at all. Without it the CLI dies on a 401 exactly as it does today.
#   * `..._ENTRYPOINT` has to name an embedding surface the CLI arms mid-run
#     recovery for. Its own default entrypoint is not one of them, so leaving
#     this alone silently disarms the whole mechanism (SYM-232 spike).
#   * `..._401_WAIT_MS` is how long a rejected request is held open waiting for
#     the answer. Zero means the CLI gives up before we can reply; this is
#     sized above the dispenser's own ~20s budget so a slow rotation still
#     lands, and it is the budget that docstring refers to.
CLAUDE_CONTROL_CHANNEL_ENTRYPOINT = "local-agent"
CLAUDE_CONTROL_CHANNEL_WAIT_MS = 30_000


def claude_control_channel_env() -> dict[str, str]:
    """Environment that arms the CLI's mid-run auth recovery."""
    return {
        "CLAUDE_CODE_SDK_HAS_OAUTH_REFRESH": "1",
        "CLAUDE_CODE_ENTRYPOINT": CLAUDE_CONTROL_CHANNEL_ENTRYPOINT,
        "CLAUDE_CODE_OAUTH_401_WAIT_MS": str(CLAUDE_CONTROL_CHANNEL_WAIT_MS),
    }


def claude_control_channel_argv(command: Sequence[str], prompt: str) -> list[str] | None:
    """The same claude run with its prompt lifted onto stdin, or None.

    A conversation delivers the prompt as a message, so it must not also ride
    in the argv — the agent would do the work twice. `None` means this argv is
    not one we recognise as carrying exactly this prompt behind `--` (a codex
    run, or a builder that stopped using the SYM-42 idiom), and the caller
    keeps the one-directional shape rather than guessing. That check is what
    makes the split safe to do here instead of at every builder: a drift
    between the two costs a fallback, never a run with no prompt at all.
    """
    if len(command) < 3 or command[0] != "claude":
        return None
    if list(command[-2:]) != ["--", prompt]:
        return None
    return [*command[:-2], "--input-format", "stream-json"]


__all__ = [
    "BUILDER_DENY_HOOK_SCRIPT",
    "BUILDER_SETTING_SOURCES",
    "CLAUDE_BUILDER_TOOLS",
    "CLAUDE_CONTROL_CHANNEL_ENTRYPOINT",
    "claude_builder_allowed_tools",
    "claude_builder_settings",
    "claude_control_channel_argv",
    "claude_control_channel_env",
]
