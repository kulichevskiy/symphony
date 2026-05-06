"""Long-running orchestrator: dispatches `auto`-labeled issues until shutdown.

The orchestrator is split into pure helpers and an async driver, the same
pattern as :mod:`symphony.reviewer`. Pure helpers (``build_dep_graph``,
``detect_cycles``, ``select_ready``, ``is_rate_limited``) are unit-tested
without the network or the clock; the driver wires them together.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Config
from .github import (
    Issue,
    TrackedIssue,
    label_issue,
    list_open_issues_with_label,
    tracked_issues,
)
from .runonce import RunOnceResult, run_once
from .state import OrchestratorState

log = logging.getLogger(__name__)

# When the agent reports a rate-limit, suspend dispatch for this long. Coarser
# than per-issue back-off because the subscription rate-limit is global —
# re-dispatching a different issue immediately won't help.
RATE_LIMIT_PAUSE_S = 600.0

# Substrings in agent output / events that mean "you got rate-limited". Kept
# permissive — false positives just delay dispatch, false negatives spam the API.
_RATE_LIMIT_INDICATORS = (
    "rate_limit",
    "rate limit",
    "rate-limit",
    "429",
    "usage_limit",
    "usage limit",
    "usage-limit",
)


def is_rate_limited(result_or_text: Any) -> bool:
    """True if ``result``'s stderr / final_text / events mention a rate-limit.

    Accepts either an :class:`AgentResult` (or anything with the same fields)
    or a plain string for unit testing.
    """
    if isinstance(result_or_text, str):
        haystack = result_or_text
    else:
        parts = [
            getattr(result_or_text, "stderr", "") or "",
            getattr(result_or_text, "final_text", "") or "",
        ]
        for ev in getattr(result_or_text, "raw_events", []) or []:
            try:
                parts.append(json.dumps(ev))
            except (TypeError, ValueError):
                pass
        haystack = " ".join(parts)
    haystack = haystack.lower()
    return any(token in haystack for token in _RATE_LIMIT_INDICATORS)


# ---- Dependency graph + readiness ----


def build_dep_graph(
    candidates: list[Issue],
    *,
    fetch_tracked: Callable[[int], list[TrackedIssue]],
) -> dict[int, list[TrackedIssue]]:
    """Map issue number → list of its tracked-issue dependencies.

    ``fetch_tracked`` is injected so tests don't have to monkeypatch the
    GitHub helper.
    """
    return {issue.number: fetch_tracked(issue.number) for issue in candidates}


def is_blocker_satisfied(t: TrackedIssue) -> bool:
    """A blocker is satisfied iff it's closed-as-completed.

    Issues closed as ``NOT_PLANNED`` / ``DUPLICATE`` (or still open) keep
    the dependent blocked — per SYMPHONY.md "completed" is the only signal
    we trust. This is also how :mod:`symphony.runonce` filters satisfied
    deps for the round-1 prompt.
    """
    if t.state != "CLOSED":
        return False
    return t.state_reason in (None, "COMPLETED")


def detect_cycles(graph: dict[int, list[TrackedIssue]]) -> list[set[int]]:
    """Return cycle members in the dep graph as sets of issue numbers.

    Uses iterative DFS with a colour scheme (white/grey/black). When DFS
    discovers a back-edge to a grey node, every node on the active path
    from that grey ancestor to the current node forms a cycle.

    Only nodes present in ``graph`` participate — references to non-auto
    issues that are blockers but aren't candidates themselves can't be
    part of an auto-issue cycle (they're terminal sinks from our view).
    """
    WHITE, GREY, BLACK = 0, 1, 2
    colour: dict[int, int] = {n: WHITE for n in graph}
    cycle_members: set[int] = set()

    def edges(node: int) -> list[int]:
        return [t.number for t in graph.get(node, []) if t.number in graph]

    for start in list(graph):
        if colour[start] != WHITE:
            continue
        # iterative DFS; stack entries are (node, iterator-of-children)
        stack: list[tuple[int, Any]] = [(start, iter(edges(start)))]
        path: list[int] = [start]
        colour[start] = GREY
        while stack:
            node, it = stack[-1]
            try:
                child = next(it)
            except StopIteration:
                colour[node] = BLACK
                stack.pop()
                path.pop()
                continue
            c = colour.get(child, WHITE)
            if c == GREY:
                # Cycle: back-edge from `node` to `child`. Cycle nodes are
                # the path slice from `child`'s position to the end.
                idx = path.index(child)
                cycle_members.update(path[idx:])
            elif c == WHITE:
                colour[child] = GREY
                path.append(child)
                stack.append((child, iter(edges(child))))
    return [cycle_members] if cycle_members else []


@dataclass(frozen=True)
class DispatchSkip:
    """Why an issue is not dispatched on this poll tick."""

    issue_number: int
    reason: str


def select_ready(
    candidates: list[Issue],
    graph: dict[int, list[TrackedIssue]],
    cycles_flat: set[int],
    *,
    state: OrchestratorState,
    has_open_pr: Callable[[int], bool],
    has_local_branch: Callable[[int], bool],
    now: float,
) -> tuple[list[Issue], list[DispatchSkip]]:
    """Pick issues that are eligible to dispatch right now.

    Returns ``(ready, skips)``. ``ready`` is sorted FIFO by ``created_at``.
    """
    ready: list[Issue] = []
    skips: list[DispatchSkip] = []
    for issue in candidates:
        n = issue.number
        if n in state.running:
            skips.append(DispatchSkip(n, "already-running"))
            continue
        if n in cycles_flat:
            skips.append(DispatchSkip(n, "auto-cycle"))
            continue
        if state.is_in_backoff(n, now=now):
            skips.append(DispatchSkip(n, "retry-backoff"))
            continue
        unsatisfied = [
            t for t in graph.get(n, []) if not is_blocker_satisfied(t)
        ]
        if unsatisfied:
            skips.append(DispatchSkip(n, f"blocked-by:{','.join(str(t.number) for t in unsatisfied)}"))
            continue
        if has_open_pr(n):
            skips.append(DispatchSkip(n, "open-pr-exists"))
            continue
        if has_local_branch(n):
            skips.append(DispatchSkip(n, "local-branch-exists"))
            continue
        ready.append(issue)
    ready.sort(key=lambda i: i.created_at or "")
    return ready, skips


# ---- Driver ----


@dataclass
class TickStats:
    """Per-poll-tick summary, mostly for logging/testing."""

    candidates: int
    dispatched: int
    skips: list[DispatchSkip]


async def _dispatch_one(
    issue: Issue,
    *,
    cfg: Config,
    state: OrchestratorState,
    config_path: Path,
    run_once_fn: Callable[..., Awaitable[RunOnceResult]],
    now_fn: Callable[[], float],
    rate_limit_pause_s: float,
) -> None:
    """Run a single issue through ``run_once`` and update state with the result."""
    state.running.add(issue.number)
    try:
        result = await run_once_fn(
            issue_number=issue.number, config_path=config_path
        )
    except Exception as e:  # pragma: no cover — exception path is logged + retried
        log.exception("dispatch crashed for issue #%d", issue.number)
        state.schedule_retry(issue.number, now=now_fn())
        return
    finally:
        state.running.discard(issue.number)

    # Rate-limit detection drives a global pause; per-issue retry is still
    # scheduled so the issue retries after the pause expires.
    rate_limited = False
    if result.skipped and result.skip_reason == "agent-failed":
        rate_limited = is_rate_limited(getattr(result, "agent_result", None) or "")
    elif (
        result.loop_outcome is not None
        and result.loop_outcome.kind.value == "agent_failed"
    ):
        rate_limited = is_rate_limited(
            getattr(result.loop_outcome, "agent_result", None) or ""
        )

    if rate_limited:
        state.pause(now=now_fn(), duration_s=rate_limit_pause_s)
        state.schedule_retry(issue.number, now=now_fn())
        log.warning(
            "rate-limit detected for issue #%d; pausing dispatch for %.0fs",
            issue.number,
            rate_limit_pause_s,
        )
        return

    if result.skipped:
        state.schedule_retry(issue.number, now=now_fn())
        return

    # Loop outcome decides retry vs done.
    outcome = result.loop_outcome
    if outcome is not None and outcome.kind.value == "approved":
        state.clear_retry(issue.number)
        return
    # Anything other than APPROVED keeps the issue in the retry queue so a
    # later tick can re-dispatch (e.g. after the worktree is reset, or for
    # AUTO_STUCK_IDLE which may resolve once Codex catches up).
    state.schedule_retry(issue.number, now=now_fn())


async def run_tick(
    *,
    cfg: Config,
    state: OrchestratorState,
    config_path: Path,
    list_issues: Callable[[], list[Issue]],
    fetch_tracked: Callable[[int], list[TrackedIssue]],
    has_open_pr: Callable[[int], bool],
    has_local_branch: Callable[[int], bool],
    label_fn: Callable[[int, str], None],
    now_fn: Callable[[], float],
    run_once_fn: Callable[..., Awaitable[RunOnceResult]],
    rate_limit_pause_s: float = RATE_LIMIT_PAUSE_S,
) -> TickStats:
    """One iteration of the poll loop. Returns a small stats record."""
    if state.is_paused(now=now_fn()):
        return TickStats(candidates=0, dispatched=0, skips=[])

    candidates = list_issues()
    graph = build_dep_graph(candidates, fetch_tracked=fetch_tracked)
    cycles = detect_cycles(graph)
    cycles_flat: set[int] = set()
    for c in cycles:
        cycles_flat.update(c)
    for n in cycles_flat:
        try:
            label_fn(n, "auto-cycle")
        except Exception:  # pragma: no cover — labeling failure is non-fatal
            log.exception("could not apply auto-cycle label to #%d", n)

    ready, skips = select_ready(
        candidates,
        graph,
        cycles_flat,
        state=state,
        has_open_pr=has_open_pr,
        has_local_branch=has_local_branch,
        now=now_fn(),
    )

    slots = max(0, cfg.orchestrator.max_concurrent - len(state.running))
    to_dispatch = ready[:slots]

    # Spawn each as a background task; the task self-removes from
    # state.running on completion. Handles are tracked on ``state`` so
    # ``run_forever`` can await them after a shutdown signal — without
    # this, returning from ``_main`` causes ``asyncio.run`` to cancel
    # them and abort in-flight work.
    for issue in to_dispatch:
        task = asyncio.create_task(
            _dispatch_one(
                issue,
                cfg=cfg,
                state=state,
                config_path=config_path,
                run_once_fn=run_once_fn,
                now_fn=now_fn,
                rate_limit_pause_s=rate_limit_pause_s,
            ),
            name=f"dispatch-{issue.number}",
        )
        state.dispatch_tasks.add(task)
        task.add_done_callback(state.dispatch_tasks.discard)

    return TickStats(
        candidates=len(candidates), dispatched=len(to_dispatch), skips=skips
    )


async def run_forever(
    *,
    cfg: Config,
    config_path: Path,
    state: OrchestratorState | None = None,
    shutdown_event: asyncio.Event | None = None,
    list_issues_fn: Callable[[], list[Issue]] | None = None,
    fetch_tracked_fn: Callable[[int], list[TrackedIssue]] | None = None,
    has_open_pr_fn: Callable[[int], bool] | None = None,
    has_local_branch_fn: Callable[[int], bool] | None = None,
    label_fn: Callable[[int, str], None] | None = None,
    now_fn: Callable[[], float] | None = None,
    run_once_fn: Callable[..., Awaitable[RunOnceResult]] | None = None,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """The main poll loop. Runs until ``shutdown_event`` is set.

    All side-effecting deps are injectable so tests don't need real GitHub,
    git, or wall-clock time.
    """
    state = state if state is not None else OrchestratorState()
    shutdown_event = shutdown_event or asyncio.Event()
    if now_fn is None:
        import time
        now_fn = time.monotonic

    # Live defaults for the GitHub-side deps. Injected wrappers stay simple.
    if list_issues_fn is None:
        list_issues_fn = lambda: list_open_issues_with_label(  # noqa: E731
            cfg.github.label, repo_path=cfg.repo.path
        )
    if fetch_tracked_fn is None:
        fetch_tracked_fn = lambda n: tracked_issues(n, repo_path=cfg.repo.path)  # noqa: E731
    if label_fn is None:
        label_fn = lambda n, lbl: label_issue(n, lbl, repo_path=cfg.repo.path)  # noqa: E731
    if has_open_pr_fn is None:
        from .github import find_open_pr_for_branch
        from .github import name_with_owner

        owner, _ = name_with_owner(cfg.repo.path)

        def _has_open_pr(n: int) -> bool:
            return (
                find_open_pr_for_branch(
                    f"auto/{n}",
                    repo_path=cfg.repo.path,
                    base_branch=cfg.repo.default_branch,
                    expected_owner=owner,
                )
                is not None
            )

        has_open_pr_fn = _has_open_pr
    if has_local_branch_fn is None:
        from .workspace import _branch_exists

        has_local_branch_fn = lambda n: _branch_exists(  # noqa: E731
            cfg.repo.path, f"auto/{n}"
        )
    if run_once_fn is None:
        run_once_fn = run_once

    while not shutdown_event.is_set():
        try:
            await run_tick(
                cfg=cfg,
                state=state,
                config_path=config_path,
                list_issues=list_issues_fn,
                fetch_tracked=fetch_tracked_fn,
                has_open_pr=has_open_pr_fn,
                has_local_branch=has_local_branch_fn,
                label_fn=label_fn,
                now_fn=now_fn,
                run_once_fn=run_once_fn,
            )
        except Exception:  # pragma: no cover — keep the loop alive
            log.exception("poll tick raised; continuing")

        # Sleep with shutdown awareness — wake immediately on SIGINT-driven
        # shutdown rather than waiting out the full poll interval.
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=cfg.orchestrator.poll_interval_s
            )
        except asyncio.TimeoutError:
            pass

    # Drain in-flight dispatches. The done-callback in ``run_tick`` removes
    # entries from the set as tasks finish, so we snapshot first to avoid
    # mutating the set while iterating. ``return_exceptions=True`` keeps a
    # crashed dispatch from masking the others — ``_dispatch_one`` already
    # logs and reschedules retries internally.
    pending = list(state.dispatch_tasks)
    if pending:
        log.info("draining %d in-flight dispatch task(s)", len(pending))
        await asyncio.gather(*pending, return_exceptions=True)


def install_shutdown_handler(loop: asyncio.AbstractEventLoop) -> asyncio.Event:
    """Install SIGINT/SIGTERM handlers that set a shutdown event.

    The orchestrator's poll loop wakes immediately on the event so the
    pending tick exits without another wait, and in-flight dispatch tasks
    finish their current agent invocation.
    """
    event = asyncio.Event()

    def _trigger() -> None:
        log.info("shutdown signal received; draining in-flight dispatches")
        event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _trigger)
        except (NotImplementedError, RuntimeError):
            # Windows / non-main-thread loops don't support add_signal_handler;
            # fall back to default Python signal handling.
            pass
    return event
