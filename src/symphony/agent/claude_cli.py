"""Shared claude CLI argv pieces for mutating (builder) runs.

Lives in `agent/` so both the orchestrator's command builders and the
pipeline's in-session mirror (`local_review_session._build_fix_command`)
can import it without creating a pipeline→orchestrator import cycle.
"""

from __future__ import annotations

from collections.abc import Mapping
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


__all__ = ["CLAUDE_BUILDER_TOOLS", "claude_builder_allowed_tools"]
