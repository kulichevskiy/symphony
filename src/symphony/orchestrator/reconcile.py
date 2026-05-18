"""Startup reconciliation.

Runs that were live when the host died still show as `running` with the
old PID. We can't resume the subprocess (it's gone), so we mark each
dead-PID row `interrupted` and post a Linear comment telling the user to
`$retry`. Live PIDs are left alone — they belong to runs the orchestrator
adopts on the next poll.
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
    "agent subprocess is gone. Reply `$retry` to dispatch again.\n"
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
    """Walk live-with-PID runs; flip dead ones to `interrupted`. Returns
    the number of rows flipped."""
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
    return flipped
