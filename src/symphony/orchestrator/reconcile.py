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
from datetime import UTC, datetime

import aiosqlite

from .. import db
from ..linear.client import Linear, LinearError

log = logging.getLogger(__name__)

_RETRY_BODY = (
    "🔁 **Host restarted — run interrupted**\n\n"
    "The Symphony host was restarted while this run was in flight, so the "
    "agent subprocess or review monitor is gone. Review monitors will resume "
    "automatically when possible; otherwise reply `$retry` to dispatch again.\n"
)


async def _preserve_pidless_review_retry_path(
    conn: aiosqlite.Connection,
    run: db.runs.Run,
    *,
    created_at: str,
) -> None:
    if run.stage != "review":
        return
    if await db.issue_prs.has_for_issue(conn, issue_id=run.issue_id):
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
        "SELECT team_key FROM issues WHERE id = ?",
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


async def reconcile(conn: aiosqlite.Connection, linear: Linear) -> int:
    """Walk live runs; flip orphaned ones to `interrupted`.

    Returns the number of rows flipped.
    """
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
        try:
            await linear.post_comment(run.issue_id, _RETRY_BODY)
        except LinearError as e:
            log.warning("could not post reconcile comment on %s: %s", run.issue_id, e)
        flipped += 1

    for run in await db.runs.list_live_review_without_pid(conn):
        log.info(
            "reconcile: run=%s issue=%s has no pid — marking interrupted",
            run.id,
            run.issue_id,
        )
        has_issue_pr = await db.issue_prs.has_for_issue(conn, issue_id=run.issue_id)
        # Linked PRs are resumed by _resurrect_review_runs() on the next poll.
        # Leave ended_at NULL so startup reconcile does not trigger that
        # path's recent-failure cooldown.
        ended_at = None if has_issue_pr else now
        await db.runs.update_status(
            conn, run.id, db.runs.INTERRUPTED_STATUS, ended_at=ended_at
        )
        if not has_issue_pr:
            await _preserve_pidless_review_retry_path(conn, run, created_at=now)
        try:
            await linear.post_comment(run.issue_id, _RETRY_BODY)
        except LinearError as e:
            log.warning("could not post reconcile comment on %s: %s", run.issue_id, e)
        flipped += 1
    return flipped
