"""DAO for the `issue_cost_marks` table.

Backs the once-per-issue cost-warning rule: the orchestrator checks
`warning_posted_at` before posting a fresh `cost_warning` Linear comment
and writes the timestamp after a successful post.
"""

from __future__ import annotations

import aiosqlite


async def warning_posted_at(
    conn: aiosqlite.Connection, issue_id: str
) -> str | None:
    cur = await conn.execute(
        "SELECT warning_posted_at FROM issue_cost_marks WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    posted_at: str | None = row["warning_posted_at"]
    return posted_at


async def mark_warning_posted(
    conn: aiosqlite.Connection, issue_id: str, posted_at: str
) -> None:
    await conn.execute(
        """
        INSERT INTO issue_cost_marks (issue_id, warning_posted_at)
        VALUES (?, ?)
        ON CONFLICT(issue_id) DO UPDATE SET
            warning_posted_at = excluded.warning_posted_at
        """,
        (issue_id, posted_at),
    )
    await conn.commit()
