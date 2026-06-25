"""DAO for the `runs` table.

A run row is created when the orchestrator hands an issue to a runner. It
moves through `running` → `completed` | `failed` | `interrupted`. The
startup reconcile walks `running` rows and flips orphaned ones to
`interrupted`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import aiosqlite

from . import operator_waits

log = logging.getLogger(__name__)

# Statuses that mean "this run is supposed to be alive". Kept as a tuple so
# callers (poll dedupe, reconcile) share a single source of truth.
LIVE_STATUSES: tuple[str, ...] = ("running",)
FAILED_STATUS: str = "failed"
INTERRUPTED_STATUS: str = "interrupted"
# Stamped by the startup reconcile on the younger of two duplicate live runs for
# the same (issue_id, stage). Unlike INTERRUPTED_STATUS it is intentionally NOT
# in TERMINAL_NON_SUCCESS_STATUSES — it is a bookkeeping close, not a failure,
# and must not shadow the surviving run in "latest-run" queries.
SUPERSEDED_STATUS: str = "superseded"
NEEDS_APPROVAL_STATUS: str = "needs_approval"
TERMINAL_NON_SUCCESS_STATUSES: frozenset[str] = frozenset(
    {FAILED_STATUS, INTERRUPTED_STATUS, NEEDS_APPROVAL_STATUS}
)
SUCCESS_STATUSES: frozenset[str] = frozenset({"completed", "done"})
# termination_kind stamped on an implement run that passed the completion +
# pre-push gates and entered the agent-free publish stage but failed there
# (push / ensure_pr). Distinguishes a delivery failure — which is safe to
# resume at publish — from an agent-stage failure that left partial work.
PUBLISH_FAILED_KIND: str = "publish_failed"
# termination_kind stamped on an implement run whose agent + completion gate
# succeeded but whose local-review gate produced no verdict (reviewer infra
# failure, not a rejection). Like PUBLISH_FAILED_KIND, the commits are intact
# and trustworthy, so a $retry can resume agent-free (re-run the pre-push
# gates) instead of re-dispatching the implementer — which would find nothing
# to do and fail the "HEAD did not advance" completion contract.
LOCAL_REVIEW_INFRA_FAILED_KIND: str = "local_review_infra_failed"
# termination_kind stamped on an implement run that died on a *transient*
# provider API error (a clean 5xx/429 with no verdict and no HEAD advance —
# SYM-140's typed signal). It means no work happened, so the run is safe to
# re-dispatch: the poll loop requeues the issue after a capped backoff window
# (poll.py _agent_infra_retry_backoff_active) up to AGENT_INFRA_RETRY_LIMIT
# attempts before falling through to the normal infra-failure escalation.
TRANSIENT_API_RETRY_KIND: str = "transient_api_retry"
# Same semantics as TRANSIENT_API_RETRY_KIND but stamped when the transient
# 5xx/429 occurred in the *local-review* turn (not the implement agent itself).
# The implement commits are intact, so the re-dispatch short-circuits to the
# pre-push gates instead of re-running the implementer (mirroring
# LOCAL_REVIEW_INFRA_FAILED_KIND in the resume_after_local_review branch of
# poll.py _run_implement_dispatch). Both kinds share the same retry budget and
# backoff window tracked by _agent_infra_retry_count / _agent_infra_retry_backoff_active.
LOCAL_REVIEW_TRANSIENT_RETRY_KIND: str = "local_review_transient_retry"
# Same semantics as TRANSIENT_API_RETRY_KIND but stamped when the transient
# 5xx/429 occurred in a review-stage fix agent (CI fix, review-comment fix,
# merge-conflict fix, or required-check fix). The implement commits and the PR
# are intact; the re-dispatch short-circuits (branch already ahead) and restarts
# from the pre-push gates → publish → review monitoring. The fix is retried by
# the next review poll cycle that detects the CI/check still failing.
REVIEW_FIX_TRANSIENT_RETRY_KIND: str = "review_fix_transient_retry"
# termination_kind stamped on the younger of two live runs that share the same
# (issue_id, stage). Startup reconcile collapses such duplicates to a single
# survivor (the oldest) — belt-and-suspenders behind SYM-152's dispatch-time
# dedup, for races, crashes, or manual dispatches that slip past it.
DUPLICATE_STAGE_KIND: str = "duplicate_stage"
TERMINATION_DETAIL_MAX_BYTES: int = 4096
TERMINATION_DETAIL_MAX_LINES: int = 80

# Terminal review-monitor statuses that indicate the monitor died or was
# stopped before it could keep polling the linked PR.
REVIEW_RESURRECT_STATUSES: tuple[str, ...] = (FAILED_STATUS, INTERRUPTED_STATUS)

STALE_WAIT_KINDS_CLEARED_BY_COMPLETED_STAGE: dict[str, tuple[str, ...]] = {
    "implement": (operator_waits.KIND_IMPLEMENT_FAILED,),
    "review_fix": (operator_waits.KIND_REVIEW_FAILED,),
    "acceptance_fix": (operator_waits.KIND_ACCEPTANCE_REJECTED,),
}


@dataclass
class Run:
    id: str
    issue_id: str
    stage: str
    status: str
    pid: int | None
    started_at: str
    ended_at: str | None
    cost_usd: float
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0
    termination_kind: str = ""
    termination_detail: str = ""
    exit_returncode: int | None = None


def _row_to_run(row: aiosqlite.Row) -> Run:
    keys = set(row.keys())
    return Run(
        id=row["id"],
        issue_id=row["issue_id"],
        stage=row["stage"],
        status=row["status"],
        pid=row["pid"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        cost_usd=row["cost_usd"],
        input_tokens=(row["input_tokens"] if "input_tokens" in keys else 0),
        output_tokens=(row["output_tokens"] if "output_tokens" in keys else 0),
        cache_write_tokens=(
            row["cache_write_tokens"] if "cache_write_tokens" in keys else 0
        ),
        cache_read_tokens=(
            row["cache_read_tokens"] if "cache_read_tokens" in keys else 0
        ),
        termination_kind=(
            row["termination_kind"] if "termination_kind" in keys else ""
        ),
        termination_detail=(
            row["termination_detail"] if "termination_detail" in keys else ""
        ),
        exit_returncode=(
            row["exit_returncode"] if "exit_returncode" in keys else None
        ),
    )


async def create(
    conn: aiosqlite.Connection,
    *,
    id: str,
    issue_id: str,
    stage: str,
    status: str,
    pid: int | None,
    started_at: str,
    cost_usd: float = 0.0,
    commit: bool = True,
) -> None:
    await conn.execute(
        """
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, cost_usd)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (id, issue_id, stage, status, pid, started_at, cost_usd),
    )
    if commit:
        await conn.commit()


async def create_if_no_active(
    conn: aiosqlite.Connection,
    *,
    id: str,
    issue_id: str,
    stage: str,
    status: str,
    pid: int | None,
    started_at: str,
    cost_usd: float = 0.0,
    ignored_stage: str | None = None,
    ignored_stages: tuple[str, ...] = (),
) -> bool:
    """Atomic dedupe: insert iff no `LIVE_STATUSES` row exists for `issue_id`.

    Closes the TOCTOU window between `has_active` and `create`: a separate
    `has_active` check followed by an unconditional INSERT lets two callers
    (poll loop and `dispatch` CLI, or two manual dispatches) both observe
    "no active run" and both write a `running` row. Doing it in one
    statement makes SQLite's write-side serialization carry the guarantee.

    Returns True if the row was inserted, False if a live run already
    existed and the insert was skipped.

    `ignored_stage` (single) and `ignored_stages` (tuple) are additive.
    """
    all_ignored: tuple[str, ...] = ignored_stages
    if ignored_stage is not None:
        all_ignored = (ignored_stage, *all_ignored)
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    if all_ignored:
        ph = ",".join("?" * len(all_ignored))
        stage_filter = f" AND stage NOT IN ({ph})"
        dedupe_params: tuple[str, ...] = (*LIVE_STATUSES, *all_ignored)
    else:
        stage_filter = ""
        dedupe_params = (*LIVE_STATUSES,)
    cur = await conn.execute(
        f"""
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, cost_usd)
        SELECT ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM runs
            WHERE issue_id = ? AND status IN ({placeholders}){stage_filter}
        )
        """,
        (
            id,
            issue_id,
            stage,
            status,
            pid,
            started_at,
            cost_usd,
            issue_id,
            *dedupe_params,
        ),
    )
    await conn.commit()
    return (cur.rowcount or 0) > 0


async def create_if_not_dispatched(
    conn: aiosqlite.Connection,
    *,
    id: str,
    issue_id: str,
    stage: str,
    status: str,
    pid: int | None,
    started_at: str,
    cost_usd: float = 0.0,
) -> bool:
    """Atomic dispatch dedupe: insert iff no live run exists."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at, cost_usd)
        SELECT ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM runs WHERE issue_id = ? AND status IN ({placeholders})
        )
        """,
        (
            id,
            issue_id,
            stage,
            status,
            pid,
            started_at,
            cost_usd,
            issue_id,
            *LIVE_STATUSES,
        ),
    )
    await conn.commit()
    return (cur.rowcount or 0) > 0


async def update_status(
    conn: aiosqlite.Connection,
    run_id: str,
    status: str,
    *,
    ended_at: str | None = None,
    kind: str | None = None,
    detail: str | None = None,
    returncode: int | None = None,
) -> None:
    cur = await conn.execute(
        """
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens
        FROM runs
        WHERE id = ?
        """,
        (run_id,),
    )
    run = await cur.fetchone()
    if status in TERMINAL_NON_SUCCESS_STATUSES or status == SUPERSEDED_STATUS:
        termination_kind = kind or "unknown"
        if kind is None:
            log.warning(
                "missing termination kind for terminal run status: run_id=%s status=%s",
                run_id,
                status,
            )
        await conn.execute(
            """
            UPDATE runs
               SET status = ?,
                   ended_at = COALESCE(?, ended_at),
                   termination_kind = ?,
                   termination_detail = ?,
                   exit_returncode = ?
             WHERE id = ?
            """,
            (
                status,
                ended_at,
                termination_kind,
                _truncate_termination_detail(detail or ""),
                returncode,
                run_id,
            ),
        )
    elif status in SUCCESS_STATUSES:
        await conn.execute(
            """
            UPDATE runs
               SET status = ?,
                   ended_at = COALESCE(?, ended_at),
                   termination_kind = '',
                   termination_detail = '',
                   exit_returncode = NULL
             WHERE id = ?
            """,
            (status, ended_at, run_id),
        )
    else:
        await conn.execute(
            "UPDATE runs SET status = ?, ended_at = COALESCE(?, ended_at) WHERE id = ?",
            (status, ended_at, run_id),
        )
    if run is not None and status == "completed":
        completed_at = ended_at or run["ended_at"] or run["started_at"]
        await _clear_stale_wait_for_completed_run(conn, run, completed_at)
    await conn.commit()


def _truncate_termination_detail(
    detail: str,
    *,
    max_bytes: int = TERMINATION_DETAIL_MAX_BYTES,
    max_lines: int = TERMINATION_DETAIL_MAX_LINES,
) -> str:
    text = str(detail)
    original_bytes = text.encode("utf-8", errors="replace")
    if (
        len(original_bytes) <= max_bytes
        and len(text.splitlines()) <= max_lines
    ):
        return text

    line_tail = text
    if len(text.splitlines()) > max_lines:
        lines = text.splitlines(keepends=True)
        line_tail = "".join(lines[-max_lines:])

    tail_bytes = line_tail.encode("utf-8", errors="replace")
    truncated_bytes = len(original_bytes) - len(tail_bytes)
    while True:
        marker = f"…[truncated {truncated_bytes} bytes]"
        marker_bytes = ("\n" + marker).encode("utf-8")
        budget = max(0, max_bytes - len(marker_bytes))
        kept_tail_bytes = tail_bytes[-budget:] if budget else b""
        kept_tail = kept_tail_bytes.decode("utf-8", errors="ignore")
        new_truncated_bytes = len(original_bytes) - len(
            kept_tail.encode("utf-8", errors="replace")
        )
        if new_truncated_bytes == truncated_bytes:
            break
        truncated_bytes = new_truncated_bytes

    marker = f"…[truncated {truncated_bytes} bytes]"
    if not kept_tail:
        return marker.encode("utf-8")[:max_bytes].decode(
            "utf-8", errors="ignore"
        )
    return f"{kept_tail}\n{marker}"


def _parse_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _timestamp_after(left: str | None, right: str | None) -> bool:
    left_dt = _parse_timestamp(left)
    right_dt = _parse_timestamp(right)
    if left_dt is None or right_dt is None:
        return False
    return left_dt > right_dt


async def _clear_stale_wait_for_completed_run(
    conn: aiosqlite.Connection,
    run: aiosqlite.Row,
    completed_at: str,
) -> None:
    wait_kinds = STALE_WAIT_KINDS_CLEARED_BY_COMPLETED_STAGE.get(str(run["stage"]))
    if wait_kinds is None:
        return

    wait = await operator_waits.get(conn, str(run["issue_id"]))
    if wait is None or wait.run_id == run["id"] or wait.kind not in wait_kinds:
        return
    if not _timestamp_after(completed_at, wait.created_at):
        return
    await operator_waits.delete(conn, wait.issue_id, wait.run_id, commit=False)


async def interrupt_stale_merge_needs_approval(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
    pr_number: int,
    before: str | None = None,
) -> int:
    """Interrupt stale merge operator waits for the current PR."""
    before_filter = "" if before is None else " AND started_at < ?"
    params: list[object] = [
        datetime.now(UTC).isoformat(),
        issue_id,
        github_repo,
        pr_number,
    ]
    if before is not None:
        params.append(before)
    cur = await conn.execute(
        f"""
        SELECT id
        FROM runs
        WHERE issue_id = ?
          AND stage = 'merge'
          AND status = 'needs_approval'
          AND EXISTS (
              SELECT 1
              FROM issue_prs p
              WHERE p.issue_id = runs.issue_id
                AND p.github_repo = ?
                AND p.pr_number = ?
                AND p.merged_at IS NULL
                AND runs.started_at >= p.created_at
          )
          {before_filter}
        """,
        params[1:],
    )
    rows = await cur.fetchall()
    ended_at = str(params[0])
    interrupted = 0
    for row in rows:
        await update_status(
            conn,
            row["id"],
            INTERRUPTED_STATUS,
            ended_at=ended_at,
            kind="superseded",
            detail=(
                "superseded stale merge needs_approval by newer PR "
                f"{github_repo}#{pr_number}"
            ),
        )
        interrupted += 1
    return interrupted


async def supersede_orphaned_merge_needs_approval(
    conn: aiosqlite.Connection,
    *,
    before: str | None = None,
) -> list[str]:
    """Retire `merge` runs stuck at `needs_approval` with no operator wait.

    A merge `needs_approval` run is created alongside an `operator_wait` of
    kind `merge`. A revival path can clear that wait and dispatch a retry; if a
    host restart then orphans the retry, the original `needs_approval` run
    lingers. `issue_prs.list_merge_candidates` excludes any PR with such a run,
    so the still-open PR drops out of merge polling forever — a zombie.
    Retiring the run re-opens merge candidacy so the normal merge poll
    re-engages.

    Two distinctions keep this from firing on legitimate state:

    * A deliberate `$reject`/`$stop` also clears the wait and leaves the run at
      `needs_approval` — but it never dispatches a retry. So we only retire a
      run that has a *later* merge run which a host restart left `orphaned`
      (the revival attempt). A run with nothing after it (a plain reject), or
      whose later retry was deliberately `$stop`ped (`cancelled`, not
      `orphaned`), is left to keep the PR parked.
    * A merge `needs_approval` run legitimately coexists with the passive
      `review` monitor (created with `ignored_stage="review"`), so the
      in-flight guard ignores `review` runs — otherwise a live monitor would
      keep the zombie from ever being retired.

    `before` (an ISO timestamp) gates on `ended_at < before` to avoid racing a
    freshly-created wait that has not yet committed. Returns the issue ids
    whose runs were superseded.
    """
    before_filter = "" if before is None else " AND r.ended_at < ?"
    params: list[object] = []
    if before is not None:
        params.append(before)
    cur = await conn.execute(
        f"""
        SELECT r.id, r.issue_id
        FROM runs r
        WHERE r.stage = 'merge'
          AND r.status = 'needs_approval'
          AND EXISTS (
              SELECT 1 FROM issue_prs p
              WHERE p.issue_id = r.issue_id
                AND p.merged_at IS NULL
                AND r.started_at >= p.created_at
          )
          AND NOT EXISTS (
              SELECT 1 FROM operator_waits w WHERE w.issue_id = r.issue_id
          )
          AND EXISTS (
              SELECT 1 FROM runs later
              WHERE later.issue_id = r.issue_id
                AND later.stage = 'merge'
                AND later.started_at > r.started_at
                AND later.status = 'interrupted'
                AND later.termination_kind = 'orphaned'
          )
          AND NOT EXISTS (
              SELECT 1 FROM runs r2
              WHERE r2.issue_id = r.issue_id
                AND r2.status = 'running'
                AND r2.stage != 'review'
          )
          {before_filter}
        """,
        params,
    )
    rows = await cur.fetchall()
    ended_at = datetime.now(UTC).isoformat()
    issue_ids: list[str] = []
    for row in rows:
        await update_status(
            conn,
            row["id"],
            INTERRUPTED_STATUS,
            ended_at=ended_at,
            kind="superseded",
            detail=(
                "orphaned merge needs_approval (operator wait cleared) — "
                "re-opening merge candidacy"
            ),
        )
        issue_ids.append(str(row["issue_id"]))
    return issue_ids


async def interrupt_running_merge(conn: aiosqlite.Connection, run_id: str) -> int:
    cur = await conn.execute(
        """
        SELECT id
        FROM runs
        WHERE id = ?
          AND stage = 'merge'
          AND status = 'running'
        """,
        (run_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return 0
    await update_status(
        conn,
        run_id,
        INTERRUPTED_STATUS,
        ended_at=datetime.now(UTC).isoformat(),
        kind="superseded",
        detail="superseded running merge by newer PR/fix result",
    )
    return 1


async def update_pid(conn: aiosqlite.Connection, run_id: str, pid: int | None) -> None:
    await conn.execute("UPDATE runs SET pid = ? WHERE id = ?", (pid, run_id))
    await conn.commit()


async def has_stage_done_announced(
    conn: aiosqlite.Connection, run_id: str
) -> bool:
    cur = await conn.execute(
        """
        SELECT 1
        FROM runs
        WHERE id = ? AND stage_done_announced_at != ''
        LIMIT 1
        """,
        (run_id,),
    )
    row = await cur.fetchone()
    return row is not None


async def mark_stage_done_announced(
    conn: aiosqlite.Connection,
    run_id: str,
    *,
    announced_at: str,
) -> None:
    await conn.execute(
        """
        UPDATE runs
           SET stage_done_announced_at =
               CASE
                   WHEN stage_done_announced_at = '' THEN ?
                   ELSE stage_done_announced_at
               END
         WHERE id = ?
        """,
        (announced_at, run_id),
    )
    await conn.commit()


async def add_usage(
    conn: aiosqlite.Connection,
    run_id: str,
    *,
    cost_usd: float,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_write_tokens: int = 0,
    cache_read_tokens: int = 0,
) -> None:
    await conn.execute(
        """
        UPDATE runs
           SET cost_usd = cost_usd + ?,
               input_tokens = input_tokens + ?,
               output_tokens = output_tokens + ?,
               cache_write_tokens = cache_write_tokens + ?,
               cache_read_tokens = cache_read_tokens + ?
         WHERE id = ?
        """,
        (
            cost_usd,
            input_tokens,
            output_tokens,
            cache_write_tokens,
            cache_read_tokens,
            run_id,
        ),
    )
    await conn.commit()


async def add_cost(
    conn: aiosqlite.Connection, run_id: str, cost_usd: float
) -> None:
    await add_usage(conn, run_id, cost_usd=cost_usd)


async def mark_review_rearm_retry(conn: aiosqlite.Connection, run_id: str) -> None:
    await conn.execute(
        "INSERT OR IGNORE INTO review_rearm_retries (run_id) VALUES (?)",
        (run_id,),
    )
    await conn.commit()


async def clear_review_rearm_retry(conn: aiosqlite.Connection, run_id: str) -> None:
    await conn.execute("DELETE FROM review_rearm_retries WHERE run_id = ?", (run_id,))
    await conn.commit()


async def has_review_rearm_retry(conn: aiosqlite.Connection, run_id: str) -> bool:
    cur = await conn.execute(
        "SELECT 1 FROM review_rearm_retries WHERE run_id = ? LIMIT 1",
        (run_id,),
    )
    row = await cur.fetchone()
    return row is not None


async def has_active(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    ignored_stage: str | None = None,
) -> bool:
    """True if `issue_id` has any run in a live status."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    stage_filter = "" if ignored_stage is None else " AND stage != ?"
    params: tuple[str, ...] = (
        (issue_id, *LIVE_STATUSES)
        if ignored_stage is None
        else (issue_id, *LIVE_STATUSES, ignored_stage)
    )
    cur = await conn.execute(
        f"""
        SELECT 1 FROM runs
        WHERE issue_id = ? AND status IN ({placeholders}){stage_filter}
        LIMIT 1
        """,
        params,
    )
    row = await cur.fetchone()
    return row is not None


async def has_live_stage(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    stage: str,
) -> bool:
    """True if *issue_id* has a live run in *stage*."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"SELECT 1 FROM runs WHERE issue_id = ? AND stage = ? "
        f"AND status IN ({placeholders}) LIMIT 1",
        (issue_id, stage, *LIVE_STATUSES),
    )
    row = await cur.fetchone()
    return row is not None


async def has_running_or_completed(conn: aiosqlite.Connection, issue_id: str) -> bool:
    """Dedupe oracle for the poll loop.

    Historical name retained for compatibility, but dispatch dedupe is
    intentionally live-only: completed runs must not block legitimate reruns.
    """
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"SELECT 1 FROM runs WHERE issue_id = ? AND status IN ({placeholders}) LIMIT 1",
        (issue_id, *LIVE_STATUSES),
    )
    row = await cur.fetchone()
    return row is not None


async def list_live_with_pid(conn: aiosqlite.Connection) -> list[Run]:
    """Live runs with a non-null PID — the input set for startup reconcile."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens,
               termination_kind, termination_detail, exit_returncode
        FROM runs
        WHERE status IN ({placeholders}) AND pid IS NOT NULL
        """,
        LIVE_STATUSES,
    )
    rows = await cur.fetchall()
    return [_row_to_run(r) for r in rows]


async def list_live_review_without_pid(conn: aiosqlite.Connection) -> list[Run]:
    """Live review runs with no PID, owned by in-process review monitors."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens,
               termination_kind, termination_detail, exit_returncode
        FROM runs
        WHERE stage = 'review' AND status IN ({placeholders}) AND pid IS NULL
        """,
        LIVE_STATUSES,
    )
    rows = await cur.fetchall()
    return [_row_to_run(r) for r in rows]


async def list_live_local_review_without_pid(
    conn: aiosqlite.Connection,
) -> list[Run]:
    """Live `local_review` runs with no PID.

    Local review runs in-process (no subprocess pid) at stage `local_review`,
    so it slips past both `list_live_with_pid` (needs a pid) and
    `list_live_review_without_pid` (needs stage `review`). Startup reconcile
    sweeps these separately to recover host-restart orphans."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens,
               termination_kind, termination_detail, exit_returncode
        FROM runs
        WHERE stage = 'local_review' AND status IN ({placeholders})
              AND pid IS NULL
        """,
        LIVE_STATUSES,
    )
    rows = await cur.fetchall()
    return [_row_to_run(r) for r in rows]


async def list_live(conn: aiosqlite.Connection) -> list[Run]:
    """All live runs, oldest first — input set for reconcile duplicate collapse."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens,
               termination_kind, termination_detail, exit_returncode
        FROM runs
        WHERE status IN ({placeholders})
        ORDER BY started_at ASC, id ASC
        """,
        LIVE_STATUSES,
    )
    rows = await cur.fetchall()
    return [_row_to_run(r) for r in rows]


async def list_live_by_stage(
    conn: aiosqlite.Connection, *, stage: str
) -> list[Run]:
    """Live runs for one stage, oldest first."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens,
               termination_kind, termination_detail, exit_returncode
        FROM runs
        WHERE stage = ? AND status IN ({placeholders})
        ORDER BY started_at ASC
        """,
        (stage, *LIVE_STATUSES),
    )
    rows = await cur.fetchall()
    return [_row_to_run(r) for r in rows]


@dataclass
class RunWithIssue:
    run: Run
    identifier: str


def _row_to_run_with_issue(row: aiosqlite.Row) -> RunWithIssue:
    return RunWithIssue(run=_row_to_run(row), identifier=row["identifier"])


async def list_recent(
    conn: aiosqlite.Connection, *, limit: int = 50
) -> list[RunWithIssue]:
    """Inspection-side view: every live run + the most recent terminated
    runs up to `limit`, joined with their issue identifier, ordered by
    `started_at` DESC.

    A naive `ORDER BY started_at DESC LIMIT ?` over all rows hides a
    long-running live run if the newest `limit` terminated runs started
    after it — exactly the wrong thing during incident triage. So we
    always return every `LIVE_STATUSES` row regardless of `limit`, and
    use the limit only for the terminated tail.
    """
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT r.id, r.issue_id, r.stage, r.status, r.pid, r.started_at,
               r.ended_at, r.cost_usd, r.input_tokens, r.output_tokens,
               r.cache_write_tokens, r.cache_read_tokens, r.termination_kind,
               r.termination_detail, r.exit_returncode, i.identifier
        FROM runs r
        JOIN issues i ON i.id = r.issue_id
        WHERE r.status IN ({placeholders})
           OR r.id IN (
                SELECT id FROM runs
                WHERE status NOT IN ({placeholders})
                ORDER BY started_at DESC
                LIMIT ?
           )
        ORDER BY r.started_at DESC
        """,
        (*LIVE_STATUSES, *LIVE_STATUSES, limit),
    )
    rows = await cur.fetchall()
    return [_row_to_run_with_issue(r) for r in rows]


async def get_with_issue(
    conn: aiosqlite.Connection, run_id: str
) -> RunWithIssue | None:
    cur = await conn.execute(
        """
        SELECT r.id, r.issue_id, r.stage, r.status, r.pid, r.started_at,
               r.ended_at, r.cost_usd, r.input_tokens, r.output_tokens,
               r.cache_write_tokens, r.cache_read_tokens, r.termination_kind,
               r.termination_detail, r.exit_returncode, i.identifier
        FROM runs r
        JOIN issues i ON i.id = r.issue_id
        WHERE r.id = ?
        """,
        (run_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_run_with_issue(row)


async def history_for_issue(
    conn: aiosqlite.Connection, issue_id: str
) -> list[Run]:
    """Stage history: all runs for an issue, oldest first."""
    cur = await conn.execute(
        """
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens,
               termination_kind, termination_detail, exit_returncode
        FROM runs
        WHERE issue_id = ?
        ORDER BY started_at ASC
        """,
        (issue_id,),
    )
    rows = await cur.fetchall()
    return [_row_to_run(r) for r in rows]


async def latest_for_issue_stage(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    stage: str,
    started_at_gte: str | None = None,
) -> Run | None:
    started_filter = "" if started_at_gte is None else " AND started_at >= ?"
    params = (
        (issue_id, stage)
        if started_at_gte is None
        else (issue_id, stage, started_at_gte)
    )
    cur = await conn.execute(
        f"""
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens,
               termination_kind, termination_detail, exit_returncode
        FROM runs
        WHERE issue_id = ? AND stage = ?{started_filter}
        ORDER BY started_at DESC
        LIMIT 1
        """,
        params,
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_run(row)


async def latest_live_for_issue_stage(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    stage: str,
) -> Run | None:
    """Most recent live run for an issue/stage, or None."""
    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT id, issue_id, stage, status, pid, started_at, ended_at, cost_usd,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens,
               termination_kind, termination_detail, exit_returncode
        FROM runs
        WHERE issue_id = ? AND stage = ? AND status IN ({placeholders})
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (issue_id, stage, *LIVE_STATUSES),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_run(row)


async def cost_for_issue(conn: aiosqlite.Connection, issue_id: str) -> float:
    cur = await conn.execute(
        "SELECT COALESCE(SUM(cost_usd), 0.0) FROM runs WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return 0.0
    return float(row[0])


# Effective-token weights. Cache writes cost more than fresh tokens; cache
# reads are nearly free. The weighted sum is the family-neutral "effective
# tokens" signal the per-issue soft budget gates on — honest under a flat
# subscription where dollar figures are list-price-notional/estimated.
CACHE_WRITE_WEIGHT: float = 1.25
CACHE_READ_WEIGHT: float = 0.1


def effective_tokens(
    *,
    input_tokens: int,
    output_tokens: int,
    cache_write_tokens: int,
    cache_read_tokens: int,
) -> float:
    """Effective tokens = input + output + cache_write*1.25 + cache_read*0.1."""
    return (
        input_tokens
        + output_tokens
        + cache_write_tokens * CACHE_WRITE_WEIGHT
        + cache_read_tokens * CACHE_READ_WEIGHT
    )


@dataclass
class IssueTokens:
    """Cumulative token usage summed across all of an issue's runs."""

    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0

    @property
    def effective_tokens(self) -> float:
        return effective_tokens(
            input_tokens=self.input_tokens,
            output_tokens=self.output_tokens,
            cache_write_tokens=self.cache_write_tokens,
            cache_read_tokens=self.cache_read_tokens,
        )


async def effective_tokens_by_stage_for_issue(
    conn: aiosqlite.Connection, issue_id: str
) -> dict[str, float]:
    """Per-stage effective-token sum across the issue's runs.

    Used to render the budget-exceeded breakdown. Stages with no recorded
    tokens are omitted. Uses whatever token data is present (~60% of runs
    carry tokens today); the gap errs toward *not* parking.
    """
    cur = await conn.execute(
        """
        SELECT stage,
               COALESCE(SUM(input_tokens), 0),
               COALESCE(SUM(output_tokens), 0),
               COALESCE(SUM(cache_write_tokens), 0),
               COALESCE(SUM(cache_read_tokens), 0)
        FROM runs WHERE issue_id = ?
        GROUP BY stage
        """,
        (issue_id,),
    )
    rows = await cur.fetchall()
    breakdown: dict[str, float] = {}
    for row in rows:
        value = effective_tokens(
            input_tokens=int(row[1]),
            output_tokens=int(row[2]),
            cache_write_tokens=int(row[3]),
            cache_read_tokens=int(row[4]),
        )
        if value > 0:
            breakdown[str(row[0])] = value
    return breakdown


async def tokens_for_issue(
    conn: aiosqlite.Connection, issue_id: str
) -> IssueTokens:
    cur = await conn.execute(
        """
        SELECT COALESCE(SUM(input_tokens), 0),
               COALESCE(SUM(output_tokens), 0),
               COALESCE(SUM(cache_write_tokens), 0),
               COALESCE(SUM(cache_read_tokens), 0)
        FROM runs WHERE issue_id = ?
        """,
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return IssueTokens()
    return IssueTokens(
        input_tokens=int(row[0]),
        output_tokens=int(row[1]),
        cache_write_tokens=int(row[2]),
        cache_read_tokens=int(row[3]),
    )


@dataclass
class LocalReviewStats:
    """Aggregate telemetry over `runs` rows with `stage='local_review'`.

    Surfaced via `symphony runs local-review-stats` so operators can
    answer "is the local-review actually saving time vs. the remote
    @codex bot?" without writing SQL. All values are over rows
    *whose `ended_at` is set* — running rows are not counted in
    averages but ARE counted in `running_count` so the operator can
    see in-flight work.
    """

    completed_count: int  # APPROVED — the local pass converged
    failed_count: int  # everything else (exhaust, stuck, cost-cap, err)
    running_count: int  # status='running' (in-flight when query ran)
    total_cost_usd: float
    avg_cost_usd: float  # over rows with ended_at
    avg_duration_secs: float  # over rows with ended_at AND started_at
    approval_rate: float  # completed / (completed + failed)
    # Raw token sums across all local_review rows. The weighted effective
    # total (the per-issue budget unit) is computed by the caller via the
    # shared `symphony.tokens.effective_tokens` helper.
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cache_write_tokens: int = 0
    total_cache_read_tokens: int = 0


async def local_review_stats(conn: aiosqlite.Connection) -> LocalReviewStats:
    """Pure (read-only) aggregator for the local-review phase.

    The duration calculation parses `started_at`/`ended_at` via
    SQLite's `strftime('%s', ...)` so timezone-aware ISO-8601 strings
    (the format the orchestrator writes) work without round-tripping
    through Python.
    """
    cur = await conn.execute(
        """
        SELECT
            COALESCE(SUM(status = 'completed'), 0) AS completed,
            COALESCE(SUM(status NOT IN ('completed', 'running')), 0) AS failed,
            COALESCE(SUM(status = 'running'), 0) AS running,
            COALESCE(SUM(cost_usd), 0.0) AS total_cost,
            COALESCE(
                AVG(CASE WHEN ended_at IS NOT NULL THEN cost_usd END), 0.0
            ) AS avg_cost,
            COALESCE(
                AVG(
                    CASE
                        WHEN ended_at IS NOT NULL AND started_at IS NOT NULL
                        THEN strftime('%s', ended_at) -
                             strftime('%s', started_at)
                    END
                ),
                0.0
            ) AS avg_duration,
            COALESCE(SUM(input_tokens), 0) AS total_input,
            COALESCE(SUM(output_tokens), 0) AS total_output,
            COALESCE(SUM(cache_write_tokens), 0) AS total_cache_write,
            COALESCE(SUM(cache_read_tokens), 0) AS total_cache_read
        FROM runs
        WHERE stage = 'local_review'
        """
    )
    row = await cur.fetchone()
    if row is None:
        return LocalReviewStats(0, 0, 0, 0.0, 0.0, 0.0, 0.0)
    completed = int(row["completed"])
    failed = int(row["failed"])
    finished = completed + failed
    approval_rate = (completed / finished) if finished > 0 else 0.0
    return LocalReviewStats(
        completed_count=completed,
        failed_count=failed,
        running_count=int(row["running"]),
        total_cost_usd=float(row["total_cost"]),
        avg_cost_usd=float(row["avg_cost"]),
        avg_duration_secs=float(row["avg_duration"]),
        approval_rate=approval_rate,
        total_input_tokens=int(row["total_input"]),
        total_output_tokens=int(row["total_output"]),
        total_cache_write_tokens=int(row["total_cache_write"]),
        total_cache_read_tokens=int(row["total_cache_read"]),
    )
