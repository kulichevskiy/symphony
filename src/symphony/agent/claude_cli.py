"""Shared claude CLI argv pieces for mutating (builder) runs.

Lives in `agent/` so both the orchestrator's command builders and the
pipeline's in-session mirror (`local_review_session._build_fix_command`)
can import it without creating a pipeline→orchestrator import cycle.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
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
    Bash, ScheduleWakeup, BashOutput, KillShell). We do NOT set
    `disableAllHooks` here — that would silence the very hook we're adding
    (the read-only reviewer keeps its own `disableAllHooks` settings).
    """
    return json.dumps(
        {
            "hooks": {
                "PreToolUse": [
                    {
                        "matcher": "*",
                        "hooks": [
                            {
                                "type": "command",
                                "command": f"{sys.executable} {BUILDER_DENY_HOOK_SCRIPT}",
                            }
                        ],
                    }
                ]
            }
        },
        sort_keys=True,
        separators=(",", ":"),
    )


__all__ = [
    "BUILDER_DENY_HOOK_SCRIPT",
    "BUILDER_SETTING_SOURCES",
    "CLAUDE_BUILDER_TOOLS",
    "claude_builder_allowed_tools",
    "claude_builder_settings",
]
