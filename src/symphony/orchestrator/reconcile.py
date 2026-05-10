"""Startup reconciliation.

Runs that were live when the host died still show as `running` with the
old PID. We can't resume the subprocess (it's gone), so we mark each
dead-PID row `interrupted` and post a Linear comment telling the user to
`/retry`. Live PIDs are left alone — they belong to runs the orchestrator
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
    "agent subprocess is gone. Reply `/retry` to dispatch again.\n"
)


def _process_alive(pid: int) -> bool:
    """`os.kill(pid, 0)` raises OSError if the PID is dead or not ours."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
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
        await db.runs.update_status(conn, run.id, "interrupted", ended_at=now)
        try:
            await linear.post_comment(run.issue_id, _RETRY_BODY)
        except LinearError as e:
            log.warning("could not post reconcile comment on %s: %s", run.issue_id, e)
        flipped += 1
    return flipped
