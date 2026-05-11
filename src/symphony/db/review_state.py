"""DAO for the `review_state` table.

One row per issue. Carries the review iteration counter (capped at 12 →
`needs_approval` per PRD §pipeline) and the most recent trigger
signature so dedup logic survives an orchestrator restart.

Rows are created lazily on first write — `get()` falls back to a zero
state when the row is absent.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


@dataclass(frozen=True)
class ReviewState:
    iteration: int
    last_trigger_signature: str


async def get(conn: aiosqlite.Connection, issue_id: str) -> ReviewState:
    cur = await conn.execute(
        "SELECT iteration, last_trigger_signature FROM review_state WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return ReviewState(iteration=0, last_trigger_signature="")
    return ReviewState(
        iteration=int(row["iteration"]),
        last_trigger_signature=str(row["last_trigger_signature"]),
    )


async def bump_iteration(conn: aiosqlite.Connection, issue_id: str) -> int:
    """Increment the counter atomically and return the new value."""
    await conn.execute(
        """
        INSERT INTO review_state (issue_id, iteration, last_trigger_signature)
        VALUES (?, 1, '')
        ON CONFLICT(issue_id) DO UPDATE SET iteration = iteration + 1
        """,
        (issue_id,),
    )
    await conn.commit()
    cur = await conn.execute(
        "SELECT iteration FROM review_state WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    assert row is not None
    return int(row["iteration"])


async def set_signature(
    conn: aiosqlite.Connection, issue_id: str, signature: str
) -> None:
    await conn.execute(
        """
        INSERT INTO review_state (issue_id, iteration, last_trigger_signature)
        VALUES (?, 0, ?)
        ON CONFLICT(issue_id) DO UPDATE SET last_trigger_signature = excluded.last_trigger_signature
        """,
        (issue_id, signature),
    )
    await conn.commit()


async def reset(conn: aiosqlite.Connection, issue_id: str) -> None:
    """Clear iteration and signature — used when leaving Review (e.g.
    Merge starts, or `/retry` re-enters the stage)."""
    await conn.execute(
        """
        INSERT INTO review_state (issue_id, iteration, last_trigger_signature)
        VALUES (?, 0, '')
        ON CONFLICT(issue_id) DO UPDATE SET
            iteration = 0,
            last_trigger_signature = ''
        """,
        (issue_id,),
    )
    await conn.commit()


__all__ = [
    "ReviewState",
    "bump_iteration",
    "get",
    "reset",
    "set_signature",
]
