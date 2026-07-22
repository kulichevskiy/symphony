#!/usr/bin/env python3
"""PreToolUse deny-hook for mutating (builder) claude runs (SYM-224).

Builder spawns — implement, review-fix, merge-fix, acceptance-fix — run
one-shot (`claude --print`, no `--resume`/`--continue`). An agent that
defers work behind a self-wakeup or a background task strands the run: the
dispatch never re-invokes, the commit never lands, HEAD stays put, and the
issue parks in Needs Input (the SYM-114 fix-run `39959daf` failure).

This is enforcement, not advice: reads the PreToolUse JSON on stdin and
exits 2 (stderr steers the agent) to block the background-task surface a
one-shot dispatch cannot honor:

  * any `Bash` with `run_in_background == true` — reachable only at the
    parameter level, which tool allowlists can't gate;
  * `ScheduleWakeup`, `BashOutput`, `KillShell` by name — the
    self-continuation + background-poll surface.

Stdlib only: shipped in the container image and invoked by the claude CLI,
not imported by symphony.
"""

from __future__ import annotations

import json
import sys

_BLOCKED_TOOLS = frozenset({"ScheduleWakeup", "BashOutput", "KillShell"})

_STEER = (
    "Run the command in the FOREGROUND (never run_in_background) and make "
    "`git commit` your FINAL action before the completion marker. This run "
    "is one-shot (no resume): deferred/background work never runs, so the "
    "commit would never land and the run would strand."
)


def _decide(payload: dict[str, object]) -> str | None:
    """Return a block message, or None to allow the tool call."""
    tool_name = payload.get("tool_name")
    if tool_name in _BLOCKED_TOOLS:
        return f"{tool_name} is disabled in this run. {_STEER}"
    if tool_name == "Bash":
        tool_input = payload.get("tool_input")
        if isinstance(tool_input, dict) and tool_input.get("run_in_background") is True:
            return f"Background Bash (run_in_background) is disabled in this run. {_STEER}"
    return None


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        # Unparseable input can't be judged; fail open rather than wedge the run.
        return 0
    if not isinstance(payload, dict):
        return 0
    message = _decide(payload)
    if message is None:
        return 0
    print(message, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
