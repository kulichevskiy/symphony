"""In-memory orchestrator state.

A single :class:`OrchestratorState` lives on the asyncio event loop thread.
Every mutation (running set, retry queue, ``paused_until``) happens from that
loop, so we don't need locks. Persistence is out of scope here — the SQLite
event log lives in M5 (issue #6); recovery on restart is "look at world state
(open PRs, ``auto/<n>`` branches) and rebuild from observation".
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable

# Backoff schedule for failed-run retries: 10s × 2^(attempt-1), capped at 5
# minutes. Matches SYMPHONY.md and is easy to reason about: attempt 1 = 10s,
# attempt 2 = 20s, attempt 3 = 40s, ... attempt 6 = 320s clamped to 300s,
# stays at 300s thereafter.
BASE_BACKOFF_S = 10.0
MAX_BACKOFF_S = 300.0


def compute_backoff(attempt: int) -> float:
    """Seconds to wait before retrying after the ``attempt``-th failure (1-indexed)."""
    if attempt <= 0:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    return min(BASE_BACKOFF_S * (2 ** (attempt - 1)), MAX_BACKOFF_S)


@dataclass
class RetryEntry:
    """One scheduled retry for a previously-failed issue dispatch."""

    issue_number: int
    attempt: int
    next_retry_at: float  # monotonic timestamp


@dataclass
class OrchestratorState:
    """Runtime state of the long-running orchestrator.

    ``running`` is the set of issue numbers currently in flight (a single
    asyncio task per issue, the run-once + review-loop pipeline). The
    orchestrator must not dispatch the same issue twice concurrently.

    ``retry_queue`` maps issue number → :class:`RetryEntry`. Re-dispatch
    only happens once ``next_retry_at`` is in the past.

    ``paused_until`` is set when an agent run hit a 429 / usage-limit
    response from Anthropic. Dispatch is suspended (but in-flight tasks
    continue to completion) until ``now >= paused_until``. This is
    coarser than per-issue back-off — when the subscription is rate-
    limited, no other dispatch will succeed either.
    """

    running: set[int] = field(default_factory=set)
    retry_queue: dict[int, RetryEntry] = field(default_factory=dict)
    paused_until: float | None = None
    # Live handles for asyncio tasks dispatched by ``run_tick``. ``run_forever``
    # awaits this set after ``shutdown_event`` fires so SIGINT/SIGTERM drains
    # in-flight ``run_once`` invocations rather than cancelling them when the
    # event loop tears down.
    dispatch_tasks: set[asyncio.Task[None]] = field(default_factory=set)

    def is_paused(self, *, now: float) -> bool:
        return self.paused_until is not None and now < self.paused_until

    def is_in_backoff(self, issue_number: int, *, now: float) -> bool:
        entry = self.retry_queue.get(issue_number)
        return entry is not None and now < entry.next_retry_at

    def schedule_retry(
        self,
        issue_number: int,
        *,
        now: float,
        backoff_fn: Callable[[int], float] = compute_backoff,
    ) -> RetryEntry:
        """Bump the attempt counter and arm the next retry timestamp."""
        existing = self.retry_queue.get(issue_number)
        attempt = (existing.attempt + 1) if existing else 1
        entry = RetryEntry(
            issue_number=issue_number,
            attempt=attempt,
            next_retry_at=now + backoff_fn(attempt),
        )
        self.retry_queue[issue_number] = entry
        return entry

    def clear_retry(self, issue_number: int) -> None:
        """Forget any scheduled retry for ``issue_number`` after a successful run."""
        self.retry_queue.pop(issue_number, None)

    def pause(self, *, now: float, duration_s: float) -> None:
        """Suspend dispatch for at least ``duration_s`` seconds."""
        until = now + duration_s
        # Don't shorten an existing pause — multiple rate-limit hits in
        # quick succession should accumulate, not race.
        if self.paused_until is None or until > self.paused_until:
            self.paused_until = until
