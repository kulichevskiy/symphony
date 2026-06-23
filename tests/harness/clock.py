"""A manually-advanced clock shared across Orchestrator, Reconciler, and Sim.

Time only moves when a test calls `advance()`. The instance is callable so it
drops straight into the `clock: Callable[[], datetime]` params those components
already accept.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

# A fixed, timezone-aware epoch so tests are reproducible run-to-run.
DEFAULT_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)


class ManualClock:
    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or DEFAULT_START

    def __call__(self) -> datetime:
        return self._now

    def now(self) -> datetime:
        return self._now

    def advance(self, secs: float) -> None:
        self._now += timedelta(seconds=secs)
