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
from collections.abc import Callable
from datetime import UTC, datetime
from typing import cast

import aiosqlite

from .. import db
from ..linear.client import LinearError
from ..tracker import IssueTracker, TrackerContext, TrackerRegistry

log = logging.getLogger(__name__)

_RETRY_BODY = (
    "🔁 **Host restarted — run interrupted**\n\n"
    "The Symphony host was restarted while this run was in flight, so the "
    "agent subprocess or review monitor is gone. Review monitors will resume "
    "automatically when possible; otherwise reply `$retry` to dispatch again.\n"
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
        await tracker.post_comment(tracker_issue_id, _RETRY_BODY)
    except LinearError as e:
        log.warning("could not post reconcile comment on %s: %s", issue_id, e)


async def reconcile(
    conn: aiosqlite.Connection,
    tracker_or_resolver: TrackerInput,
) -> int:
    """Walk live runs; flip orphaned ones to `interrupted`.

    Returns the number of rows flipped.
    """
    tracker_for_context = _tracker_resolver(tracker_or_resolver)
    rows = await db.runs.list_live_with_pid(conn)
    flipped = 0
    now = datetime.now(UTC).isoformat()
    for run in rows:
        if run.pid is None or _process_alive(run.pid):
            continue
        log.info(
            "reconcile: run=%s issue=%s pid=%s is dead — marking interrupted",
            run.id,
            run.issue_id,
            run.pid,
        )
        await db.runs.update_status(
            conn, run.id, db.runs.INTERRUPTED_STATUS, ended_at=now
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
            conn, run.id, db.runs.INTERRUPTED_STATUS, ended_at=None
        )
        # Linked, still-open PRs are resumed by _resurrect_review_runs() on the
        # next poll. Leave ended_at NULL so startup reconcile does not trigger
        # that path's recent-failure cooldown. Historical PR rows that the
        # resurrection query ignores still need the operator-wait retry path.
        if not await db.issue_prs.has_orphaned_review_pr(conn, issue_id=run.issue_id):
            await db.runs.update_status(
                conn, run.id, db.runs.INTERRUPTED_STATUS, ended_at=now
            )
            await _preserve_pidless_review_retry_path(conn, run, created_at=now)
        await _post_reconcile_comment(conn, tracker_for_context, run.issue_id)
        flipped += 1
    return flipped
