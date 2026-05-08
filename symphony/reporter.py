"""Terminal renderer for orchestrator events."""

from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, TextIO


@dataclass
class TickSnapshot:
    candidates: int
    ready: int
    running: int
    skips: list[tuple[int, str]] = field(default_factory=list)

# Lowest verbosity at which a given event kind is rendered.
# -1 = --quiet, 0 = default, 1 = -v, 2 = -vv.
# `agent.event` is emitted via structlog from `agent` subcommand, not via
# `EventLog`, so it is reachable through the reporter only if the structlog
# bridge re-routes it; listing here keeps the ladder explicit.
_KIND_MIN_VERBOSITY: dict[str, int] = {
    "dispatch": 0,
    "run-terminal": 0,
    "pr-open": 0,
    "merge": 0,
    "paused": 0,
    "resumed": 0,
    "auto-cycle": 0,
    "auto-canceled": 0,
    "auto-stuck": 0,
    "retry-fired": 0,
    "retry-scheduled": 1,
    "push": 1,
    "agent-start": 1,
    "agent-exit": 1,
    "startup-gc": 1,
    "agent.event": 2,
}


class TerminalReporter:
    def __init__(
        self,
        *,
        stream: TextIO = sys.stderr,
        verbosity: int = 0,
        json_mode: bool = False,
        now_fn: Callable[[], float] = time.time,
        heartbeat_interval_s: float = 300.0,
    ) -> None:
        self._stream = stream
        self._verbosity = verbosity
        self._json_mode = json_mode
        self._now = now_fn
        self._heartbeat_interval_s = heartbeat_interval_s
        # Start the timer one full interval in the past so the first
        # maybe_heartbeat call fires immediately. Otherwise the user stares
        # at a silent terminal for 5 minutes after `symphony run` starts —
        # the original problem we set out to fix.
        self._last_activity_at = now_fn() - heartbeat_interval_s

    def event(
        self,
        kind: str,
        *,
        issue_number: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        payload = payload or {}
        # Any incoming event resets the heartbeat timer, even one filtered out
        # by verbosity — internal activity still proves the process is alive.
        now = self._now()
        self._last_activity_at = now
        min_v = _KIND_MIN_VERBOSITY.get(kind, 0)
        if self._verbosity < min_v:
            return
        if self._json_mode:
            self._emit_json(kind, issue_number=issue_number, payload=payload, now=now)
            return
        self._emit_human(kind, issue_number=issue_number, payload=payload, now=now)

    def maybe_heartbeat(self, snapshot: TickSnapshot) -> None:
        # Heartbeat is gated by verbosity at the default tier — `--quiet`
        # (verbosity < 0) suppresses periodic idle output the same way it
        # suppresses default-tier events.
        if self._verbosity < 0:
            return
        now = self._now()
        if now - self._last_activity_at < self._heartbeat_interval_s:
            return
        self._last_activity_at = now
        if self._json_mode:
            obj: dict[str, Any] = {
                "kind": "heartbeat",
                "payload": {
                    "candidates": snapshot.candidates,
                    "ready": snapshot.ready,
                    "running": snapshot.running,
                },
            }
            if self._verbosity >= 1:
                obj["ts"] = now
            self._stream.write(json.dumps(obj, sort_keys=True) + "\n")
            return
        self._stream.write(
            f"{self._ts_prefix(now)}· idle: {snapshot.candidates} candidates, "
            f"{snapshot.ready} ready, {snapshot.running} running\n"
        )

    def _ts_prefix(self, now: float) -> str:
        # Timestamps only at -v / -vv. At default verbosity the lines are sparse
        # enough that wall-clock prefix is just noise.
        if self._verbosity < 1:
            return ""
        return time.strftime("%H:%M:%S", time.localtime(now)) + " "

    def _emit_json(
        self,
        kind: str,
        *,
        issue_number: int | None,
        payload: dict[str, Any],
        now: float,
    ) -> None:
        obj: dict[str, Any] = {"kind": kind, "payload": payload}
        if issue_number is not None:
            obj["issue"] = issue_number
        if self._verbosity >= 1:
            obj["ts"] = now
        self._stream.write(json.dumps(obj, sort_keys=True) + "\n")

    def _emit_human(
        self,
        kind: str,
        *,
        issue_number: int | None,
        payload: dict[str, Any],
        now: float,
    ) -> None:
        ts = self._ts_prefix(now)
        if kind == "dispatch":
            title = payload.get("title", "")
            self._stream.write(f"{ts}→ dispatch #{issue_number} \"{title}\"\n")
        elif kind == "run-terminal":
            outcome = payload.get("outcome", "unknown")
            rounds = payload.get("rounds_used")
            rounds_part = f" ({rounds} rounds)" if rounds is not None else ""
            if outcome == "approved":
                pr_number = payload.get("pr_number")
                pr_part = f" → PR #{pr_number}" if pr_number is not None else ""
                self._stream.write(
                    f"{ts}✓ #{issue_number} approved{pr_part}{rounds_part}\n"
                )
            else:
                self._stream.write(
                    f"{ts}✗ #{issue_number} {outcome}{rounds_part}\n"
                )
        elif kind == "retry-scheduled":
            reason = payload.get("reason", "")
            attempt = payload.get("attempt", "?")
            self._stream.write(
                f"{ts}⏳ retry scheduled #{issue_number} attempt={attempt} reason={reason}\n"
            )
        elif kind == "retry-fired":
            attempt = payload.get("attempt", "?")
            self._stream.write(
                f"{ts}↻ retry-fired #{issue_number} attempt={attempt}\n"
            )
        elif kind == "paused":
            reason = payload.get("reason", "")
            self._stream.write(f"{ts}⚠ paused (reason={reason})\n")
        elif kind == "resumed":
            self._stream.write(f"{ts}▶ resumed\n")
        elif kind == "auto-cycle":
            self._stream.write(f"{ts}⊘ auto-cycle on #{issue_number}\n")
        elif kind == "auto-canceled":
            reason = payload.get("reason", "")
            self._stream.write(f"{ts}⊘ canceled #{issue_number} (reason={reason})\n")
        elif kind == "auto-stuck":
            reason = payload.get("reason", "")
            rounds = payload.get("rounds_used")
            rounds_part = f" after {rounds} rounds" if rounds is not None else ""
            self._stream.write(
                f"{ts}⏸ auto-stuck #{issue_number} (reason={reason}){rounds_part}\n"
            )
        elif kind == "pr-open":
            pr_number = payload.get("number")
            reused = payload.get("reused")
            tag = " (reused)" if reused else ""
            self._stream.write(
                f"{ts}◆ PR #{pr_number} opened for #{issue_number}{tag}\n"
            )
        elif kind == "merge":
            pr_number = payload.get("pr_number")
            self._stream.write(f"{ts}⇩ merged PR #{pr_number} for #{issue_number}\n")
