"""Startup reconciliation.

Runs that were live when the host died still show as `running` with the
old PID, or with no PID for in-process review-monitor tasks. We
can't resume that work in a fresh process, so we mark each orphaned row
`interrupted` and post a Linear comment. Live PIDs are left alone — they
belong to runs the orchestrator adopts on the next poll.
"""

from __future__ import annotations

import logging
import os
import signal
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import aiosqlite

from .. import db
from ..linear.client import LinearError
from ..tracker import IssueTracker, TrackerContext, TrackerRegistry

if TYPE_CHECKING:
    from ..config import RepoBinding

log = logging.getLogger(__name__)

_RETRY_BODY = (
    "🔁 **Host restarted — run interrupted**\n\n"
    "The Symphony host was restarted while this run was in flight, so the "
    "agent subprocess or review monitor is gone. Review monitors will resume "
    "automatically when possible; otherwise reply `$retry` to dispatch again.\n"
)

# A local_review orphan has no operator-wait and no active review monitor, so
# `$retry` has no handler for it (poll.py rejects it as "no active retry
# handler"). Re-dispatch is automatic, so the comment must not tell the
# operator to do anything.
_LOCAL_REVIEW_REDISPATCH_BODY = (
    "🔁 **Host restarted — re-dispatched automatically**\n\n"
    "The Symphony host was restarted while this issue was in local code review "
    "(an in-process step with no subprocess to resume). The committed implement "
    "work is intact, so the issue has been moved back to its ready state and "
    "will be re-dispatched automatically on the next poll. No action needed.\n"
)

TrackerResolver = Callable[[TrackerContext], IssueTracker]
TrackerInput = IssueTracker | TrackerRegistry | TrackerResolver


async def _preserve_pidless_review_retry_path(
    conn: aiosqlite.Connection,
    run: db.runs.Run,
    *,
    created_at: str,
) -> None:
    if run.stage != "review":
        return

    state = await db.review_state.get(conn, run.issue_id)
    if not state.github_repo:
        log.warning(
            "could not preserve retry path for pidless review run=%s issue=%s: "
            "missing review_state.github_repo",
            run.id,
            run.issue_id,
        )
        return

    cur = await conn.execute(
        "SELECT provider, site, team_key FROM issues WHERE id = ?",
        (run.issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        log.warning(
            "could not preserve retry path for pidless review run=%s issue=%s: "
            "missing issue row",
            run.id,
            run.issue_id,
        )
        return

    await db.operator_waits.upsert(
        conn,
        issue_id=run.issue_id,
        run_id=run.id,
        kind=db.operator_waits.KIND_REVIEW_FAILED,
        linear_team_key=str(row["team_key"]),
        github_repo=state.github_repo,
        issue_label=state.issue_label,
        created_at=created_at,
        provider=str(row["provider"]),
        tracker_provider=str(row["provider"]),
        tracker_site=str(row["site"]),
    )


def _process_alive(pid: int) -> bool:
    """`os.kill(pid, 0)` is the standard liveness probe: it returns 0 if the
    PID is reachable, raises `ProcessLookupError` (ESRCH) if no such process
    exists, and various other `OSError`s (`EPERM` for foreign-owned PIDs,
    `EINVAL` for bad PID values, plus platform-specific oddities) when it
    can't decide. ESRCH is the only signal that proves death — anything
    else means the process might still be alive. Defaulting unknown-state
    errors to dead would either mark a sibling-owned run `interrupted` (and
    invite `$retry` while a worker is still running) or, worse, crash
    `reconcile()` at startup and prevent the orchestrator from booting."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def _terminate_pid(pid: int) -> bool:
    """SIGTERM→SIGKILL on a duplicate run's process group.

    Returns True when the process was successfully signalled or was already dead
    (safe to mark the row superseded). Returns False when the signal could not be
    sent because the process is alive but unowned (EPERM) or similarly unkillable —
    in that case the caller must NOT mark the row superseded, since the duplicate
    is still running."""
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True  # already dead
    except OSError:
        return False  # EPERM or similar — process is alive and we can't kill it
    with suppress(OSError):  # ProcessLookupError = died after SIGTERM; others: best-effort
        os.killpg(pid, signal.SIGKILL)
    return True


def _tracker_resolver(tracker_or_resolver: TrackerInput) -> TrackerResolver:
    if isinstance(tracker_or_resolver, TrackerRegistry):
        return tracker_or_resolver.resolve
    if hasattr(tracker_or_resolver, "post_comment"):
        tracker = cast(IssueTracker, tracker_or_resolver)
        return lambda _ctx: tracker
    return tracker_or_resolver


async def _tracker_identity_for_issue(
    conn: aiosqlite.Connection,
    issue_id: str,
) -> tuple[str, TrackerContext]:
    cur = await conn.execute(
        "SELECT tracker_issue_id, provider, site, team_key FROM issues WHERE id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return issue_id, TrackerContext()
    provider = str(row["provider"] or "")
    site = str(row["site"] or "")
    if not provider or not site:
        return issue_id, TrackerContext()
    tracker_issue_id = str(row["tracker_issue_id"] or issue_id)
    project_key = str(row["team_key"] or "") if provider == "jira" else ""
    return tracker_issue_id, TrackerContext(
        provider=provider,
        site=site,
        project_key=project_key,
    )


async def _post_reconcile_comment(
    conn: aiosqlite.Connection,
    tracker_for_context: TrackerResolver,
    issue_id: str,
    body: str = _RETRY_BODY,
) -> None:
    tracker_issue_id, ctx = await _tracker_identity_for_issue(conn, issue_id)
    try:
        tracker = tracker_for_context(ctx)
    except KeyError as e:
        log.warning(
            "could not resolve reconcile tracker on %s provider=%s site=%s: %s",
            issue_id,
            ctx.provider,
            ctx.site,
            e,
        )
        return
    try:
        await tracker.post_comment(tracker_issue_id, body)
    except LinearError as e:
        log.warning("could not post reconcile comment on %s: %s", issue_id, e)


def _binding_for_issue(
    bindings: Sequence[RepoBinding],
    *,
    team_key: str,
    ctx: TrackerContext,
) -> RepoBinding | None:
    for binding in bindings:
        if (
            binding.linear_team_key == team_key
            and binding.tracker_provider == ctx.provider
            and binding.tracker_site == ctx.site
        ):
            return binding
    return None


async def _redispatch_orphaned_local_review(
    conn: aiosqlite.Connection,
    tracker_for_context: TrackerResolver,
    bindings: Sequence[RepoBinding],
    run: db.runs.Run,
) -> bool:
    """Move the issue back to its `ready` state so the next poll re-dispatches
    a fresh implement→local_review→push. The committed implement work survives,
    so the re-run is cheap. This is the automated equivalent of the manual
    "move the card back to Todo" recovery.

    Returns True if the issue was moved to `ready`. On any failure returns
    False: the caller then leaves the run live so a later reconcile retries it,
    rather than flipping it `interrupted` and stranding the issue in "Local
    Code Review" with no live run and no working retry handler."""
    if not bindings:
        log.warning(
            "cannot re-dispatch orphaned local_review run=%s issue=%s: "
            "no bindings provided",
            run.id,
            run.issue_id,
        )
        return False

    cur = await conn.execute(
        "SELECT tracker_issue_id, provider, site, team_key FROM issues WHERE id = ?",
        (run.issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        log.warning(
            "cannot re-dispatch orphaned local_review run=%s issue=%s: "
            "missing issue row",
            run.id,
            run.issue_id,
        )
        return False

    provider = str(row["provider"] or "")
    site = str(row["site"] or "")
    team_key = str(row["team_key"] or "")
    project_key = team_key if provider == "jira" else ""
    ctx = (
        TrackerContext(provider=provider, site=site, project_key=project_key)
        if provider and site
        else TrackerContext()
    )
    binding = _binding_for_issue(bindings, team_key=team_key, ctx=ctx)
    if binding is None:
        log.warning(
            "cannot re-dispatch orphaned local_review run=%s issue=%s: "
            "no binding for team=%s provider=%s site=%s",
            run.id,
            run.issue_id,
            team_key,
            ctx.provider,
            ctx.site,
        )
        return False

    try:
        tracker = tracker_for_context(ctx)
    except KeyError as e:
        log.warning(
            "cannot re-dispatch orphaned local_review run=%s: tracker resolve failed: %s",
            run.id,
            e,
        )
        return False

    tracker_issue_id = str(row["tracker_issue_id"] or run.issue_id)
    ready_state = binding.linear_states.ready
    try:
        states = await tracker.team_states(team_key)
        ready_id = states.get(ready_state)
        if ready_id is None:
            log.warning(
                "cannot re-dispatch orphaned local_review run=%s issue=%s: "
                "missing ready state %r for team %s",
                run.id,
                run.issue_id,
                ready_state,
                team_key,
            )
            return False
        await tracker.move_issue(tracker_issue_id, ready_id)
    except LinearError as e:
        log.warning(
            "could not move %s to ready for local_review re-dispatch: %s",
            run.issue_id,
            e,
        )
        return False
    log.info(
        "reconcile: re-dispatched orphaned local_review issue=%s to ready state %r",
        run.issue_id,
        ready_state,
    )
    return True


async def reconcile(
    conn: aiosqlite.Connection,
    tracker_or_resolver: TrackerInput,
    bindings: Sequence[RepoBinding] | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
    pid_alive: Callable[[int], bool] = _process_alive,
    terminate_pid: Callable[[int], bool] = _terminate_pid,
) -> int:
    """Walk live runs; flip orphaned ones to `interrupted`.

    `pid_alive` is the process-liveness probe (default: `os.kill(pid, 0)`).
    Tests inject a Sim-owned probe so liveness is deterministic rather than
    relying on a magic dead-PID convention. `terminate_pid` (default:
    SIGTERM→SIGKILL) kills the younger of any duplicate same-stage live runs;
    it returns True on success/already-dead and False when the process could
    not be killed (EPERM); tests inject a recorder so no real signals fly.

    Returns the number of rows flipped.
    """
    bindings = bindings or ()
    tracker_for_context = _tracker_resolver(tracker_or_resolver)
    rows = await db.runs.list_live_with_pid(conn)
    flipped = 0
    now = (clock() if clock is not None else datetime.now(UTC)).isoformat()  # noqa: clock
    for run in rows:
        if run.pid is None or pid_alive(run.pid):
            continue
        log.info(
            "reconcile: run=%s issue=%s pid=%s is dead — marking interrupted",
            run.id,
            run.issue_id,
            run.pid,
        )
        await db.runs.update_status(
            conn,
            run.id,
            db.runs.INTERRUPTED_STATUS,
            ended_at=now,
            kind="orphaned",
            detail=f"Host restarted; pid {run.pid} is no longer alive",
        )
        await _post_reconcile_comment(conn, tracker_for_context, run.issue_id)
        flipped += 1

    for run in await db.runs.list_live_review_without_pid(conn):
        log.info(
            "reconcile: run=%s issue=%s has no pid — marking interrupted",
            run.id,
            run.issue_id,
        )
        await db.runs.update_status(
            conn,
            run.id,
            db.runs.INTERRUPTED_STATUS,
            ended_at=None,
            kind="orphaned",
            detail="Host restarted; pidless review monitor orphaned",
        )
        # Linked, still-open PRs are resumed by _resurrect_review_runs() on the
        # next poll. Leave ended_at NULL so startup reconcile does not trigger
        # that path's recent-failure cooldown. Historical PR rows that the
        # resurrection query ignores still need the operator-wait retry path.
        if not await db.issue_prs.has_orphaned_review_pr(conn, issue_id=run.issue_id):
            await db.runs.update_status(
                conn,
                run.id,
                db.runs.INTERRUPTED_STATUS,
                ended_at=now,
                kind="orphaned",
                detail="Host restarted; pidless review monitor orphaned",
            )
            await _preserve_pidless_review_retry_path(conn, run, created_at=now)
        await _post_reconcile_comment(conn, tracker_for_context, run.issue_id)
        flipped += 1

    for run in await db.runs.list_live_local_review_without_pid(conn):
        log.info(
            "reconcile: local_review run=%s issue=%s has no pid — "
            "marking interrupted and re-dispatching",
            run.id,
            run.issue_id,
        )
        redispatched = await _redispatch_orphaned_local_review(
            conn, tracker_for_context, bindings, run
        )
        if not redispatched:
            # Re-dispatch failed (flaky move_issue, missing ready state, no
            # binding). Leave the run live so a later reconcile retries on the
            # still-live row — flipping it interrupted now would strand the
            # issue in "Local Code Review" with no live run and no working
            # retry handler.
            continue
        await db.runs.update_status(
            conn,
            run.id,
            db.runs.INTERRUPTED_STATUS,
            ended_at=now,
            kind="orphaned",
            detail="Host restarted; pidless local review monitor orphaned",
        )
        await _post_reconcile_comment(
            conn, tracker_for_context, run.issue_id, _LOCAL_REVIEW_REDISPATCH_BODY
        )
        flipped += 1

    flipped += await _collapse_duplicate_live_runs(conn, now, terminate_pid)
    return flipped


async def _collapse_duplicate_live_runs(
    conn: aiosqlite.Connection,
    now: str,
    terminate_pid: Callable[[int], bool],
) -> int:
    """Belt-and-suspenders behind SYM-152's dispatch-time dedup: if two live
    runs ever share the same `(issue_id, stage)` — a race, a crash, or a manual
    dispatch — keep exactly one survivor and collapse the rest. The duplicate's
    process is terminated if alive and its row marked superseded (not interrupted,
    so it does not shadow the survivor in "latest-run" queries).

    Survivor selection: prefer runs that have a pid (alive) over pidless/stale
    rows; among equal pid-presence keep the oldest. This avoids keeping a stale
    pidless row and terminating the newer run that actually has a live process.

    Runs after the orphan sweeps so it only sees genuinely-live survivors.
    Distinct stages for one issue (e.g. implement + local_review) never group
    together, so they are left untouched."""
    groups: dict[tuple[str, str], list[db.runs.Run]] = {}
    for run in await db.runs.list_live(conn):
        groups.setdefault((run.issue_id, run.stage), []).append(run)

    flipped = 0
    for (issue_id, stage), group in groups.items():
        if len(group) < 2:
            continue
        # Prefer pid-having rows over pidless/stale; among ties keep oldest.
        group_sorted = sorted(group, key=lambda r: (r.pid is None, r.started_at, r.id))
        survivor, *duplicates = group_sorted
        for dup in duplicates:
            log.warning(
                "reconcile: duplicate live run=%s issue=%s stage=%s "
                "(keeping run=%s) — terminating and marking superseded",
                dup.id,
                issue_id,
                stage,
                survivor.id,
            )
            if dup.pid is not None and dup.pid != survivor.pid:
                if not terminate_pid(dup.pid):
                    log.warning(
                        "reconcile: could not terminate duplicate run=%s pid=%s "
                        "(EPERM or similar) — skipping supersede to avoid masking "
                        "a live process",
                        dup.id,
                        dup.pid,
                    )
                    continue
            await db.runs.update_status(
                conn,
                dup.id,
                db.runs.SUPERSEDED_STATUS,
                ended_at=now,
                kind=db.runs.DUPLICATE_STAGE_KIND,
                detail=(
                    f"Duplicate live {stage} run for issue {issue_id}; "
                    f"kept run {survivor.id}"
                ),
            )
            flipped += 1
    return flipped
