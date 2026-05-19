"""DAO for the `issue_prs` table."""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite

from . import state_transitions
from .runs import LIVE_STATUSES, REVIEW_RESURRECT_STATUSES


@dataclass(frozen=True)
class IssuePR:
    issue_id: str
    identifier: str
    title: str
    team_key: str
    github_repo: str
    binding_key: str
    pr_number: int
    pr_url: str
    created_at: str
    merged_at: str | None


def _row_to_issue_pr(row: aiosqlite.Row) -> IssuePR:
    return IssuePR(
        issue_id=row["issue_id"],
        identifier=row["identifier"],
        title=row["title"],
        team_key=row["team_key"],
        github_repo=row["github_repo"],
        binding_key=row["binding_key"],
        pr_number=row["pr_number"],
        pr_url=row["pr_url"],
        created_at=row["created_at"],
        merged_at=row["merged_at"],
    )


async def upsert(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
    pr_number: int,
    pr_url: str,
    created_at: str,
    binding_key: str = "",
) -> None:
    await conn.execute(
        """
        INSERT INTO issue_prs (
            issue_id, github_repo, binding_key, pr_number, pr_url, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(issue_id, github_repo) DO UPDATE SET
            binding_key = excluded.binding_key,
            pr_number  = excluded.pr_number,
            pr_url     = excluded.pr_url,
            created_at = excluded.created_at,
            merged_at  = NULL
        """,
        (issue_id, github_repo, binding_key, pr_number, pr_url, created_at),
    )
    await conn.execute(
        """
        DELETE FROM merge_conflict_fix_marks
        WHERE issue_id = ?
          AND github_repo = ?
          AND (pr_number != ? OR pr_created_at != ?)
        """,
        (issue_id, github_repo, pr_number, created_at),
    )
    await conn.commit()


async def mark_merge_conflict_fixed(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
    pr_number: int,
    head_sha: str,
    marked_at: str,
) -> bool:
    """Persist that a conflict fix-run completed for the current PR cycle."""
    if not head_sha:
        return False
    cur = await conn.execute(
        """
        SELECT created_at
        FROM issue_prs
        WHERE issue_id = ?
          AND github_repo = ?
          AND pr_number = ?
          AND merged_at IS NULL
        """,
        (issue_id, github_repo, pr_number),
    )
    row = await cur.fetchone()
    if row is None:
        return False
    pr_created_at = str(row["created_at"])
    await conn.execute(
        """
        INSERT INTO merge_conflict_fix_marks (
            issue_id, github_repo, pr_number, pr_created_at, head_sha, marked_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(issue_id, github_repo) DO UPDATE SET
            pr_number = excluded.pr_number,
            pr_created_at = excluded.pr_created_at,
            head_sha = excluded.head_sha,
            marked_at = excluded.marked_at
        """,
        (issue_id, github_repo, pr_number, pr_created_at, head_sha, marked_at),
    )
    await conn.commit()
    return True


async def has_merge_conflict_fixed(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
    pr_number: int,
    pr_created_at: str,
    head_sha: str,
) -> bool:
    if not head_sha:
        return False
    cur = await conn.execute(
        """
        SELECT 1
        FROM merge_conflict_fix_marks
        WHERE issue_id = ?
          AND github_repo = ?
          AND pr_number = ?
          AND pr_created_at = ?
          AND head_sha = ?
        LIMIT 1
        """,
        (issue_id, github_repo, pr_number, pr_created_at, head_sha),
    )
    row = await cur.fetchone()
    return row is not None


async def clear_merge_conflict_fixed(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
    pr_number: int,
    pr_created_at: str | None = None,
) -> bool:
    pr_cycle_filter = "" if pr_created_at is None else " AND pr_created_at = ?"
    params: tuple[object, ...] = (
        (issue_id, github_repo, pr_number)
        if pr_created_at is None
        else (issue_id, github_repo, pr_number, pr_created_at)
    )
    cur = await conn.execute(
        f"""
        DELETE FROM merge_conflict_fix_marks
        WHERE issue_id = ?
          AND github_repo = ?
          AND pr_number = ?{pr_cycle_filter}
        """,
        params,
    )
    await conn.commit()
    return (cur.rowcount or 0) > 0


async def get(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
) -> IssuePR | None:
    cur = await conn.execute(
        """
        SELECT p.issue_id, i.identifier, i.title, i.team_key, p.github_repo,
               p.binding_key, p.pr_number, p.pr_url, p.created_at, p.merged_at
        FROM issue_prs p
        JOIN issues i ON i.id = p.issue_id
        WHERE p.issue_id = ? AND p.github_repo = ?
        """,
        (issue_id, github_repo),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_issue_pr(row)


async def get_for_issue(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
) -> IssuePR | None:
    cur = await conn.execute(
        """
        SELECT p.issue_id, i.identifier, i.title, i.team_key, p.github_repo,
               p.binding_key, p.pr_number, p.pr_url, p.created_at, p.merged_at
        FROM issue_prs p
        JOIN issues i ON i.id = p.issue_id
        WHERE p.issue_id = ?
        ORDER BY
          p.merged_at IS NOT NULL ASC,
          COALESCE(p.merged_at, p.created_at) DESC,
          p.created_at DESC,
          p.github_repo ASC
        LIMIT 1
        """,
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return _row_to_issue_pr(row)


async def has_for_issue(conn: aiosqlite.Connection, *, issue_id: str) -> bool:
    cur = await conn.execute(
        "SELECT 1 FROM issue_prs WHERE issue_id = ? LIMIT 1",
        (issue_id,),
    )
    row = await cur.fetchone()
    return row is not None


async def has_orphaned_review_pr(conn: aiosqlite.Connection, *, issue_id: str) -> bool:
    """True when review resurrection can pick up an issue's PR."""
    live_placeholders = ",".join("?" * len(LIVE_STATUSES))
    resurrect_placeholders = ",".join("?" * len(REVIEW_RESURRECT_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT 1
        FROM issue_prs p
        WHERE p.issue_id = ?
          AND p.merged_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'review'
                AND r.status IN ({live_placeholders})
          )
          AND (
              SELECT r.status FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'review'
                AND r.started_at >= p.created_at
              ORDER BY r.started_at DESC, r.rowid DESC
              LIMIT 1
          ) IN ({resurrect_placeholders})
          AND NOT EXISTS (
              SELECT 1 FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'merge'
                AND r.status IN ('running', 'completed', 'done', 'needs_approval')
                AND r.started_at >= p.created_at
          )
        LIMIT 1
        """,
        (issue_id, *LIVE_STATUSES, *REVIEW_RESURRECT_STATUSES),
    )
    row = await cur.fetchone()
    return row is not None


async def mark_merged(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
    merged_at: str,
) -> None:
    await update_merged(
        conn,
        issue_id=issue_id,
        github_repo=github_repo,
        pr_number=None,
        merged_at=merged_at,
    )


async def update_merged(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
    pr_number: int | None,
    merged_at: str,
    commit: bool = True,
) -> bool:
    cur = await conn.execute(
        """
        SELECT merged_at
        FROM issue_prs
        WHERE issue_id = ?
          AND github_repo = ?
          AND (? IS NULL OR pr_number = ?)
        """,
        (issue_id, github_repo, pr_number, pr_number),
    )
    row = await cur.fetchone()
    if row is None:
        return False

    old_merged_at = row["merged_at"]
    cur = await conn.execute(
        """
        UPDATE issue_prs
        SET merged_at = ?
        WHERE issue_id = ?
          AND github_repo = ?
          AND (? IS NULL OR pr_number = ?)
        """,
        (merged_at, issue_id, github_repo, pr_number, pr_number),
    )
    updated = (cur.rowcount or 0) > 0
    if updated and old_merged_at != merged_at:
        await state_transitions.record_transition(
            conn,
            issue_id,
            "issue_prs",
            "merged_at",
            old_merged_at,
            merged_at,
        )
    if commit:
        await conn.commit()
    return updated


async def delete(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
    pr_number: int | None = None,
    commit: bool = True,
) -> bool:
    cur = await conn.execute(
        """
        SELECT pr_number
        FROM issue_prs
        WHERE issue_id = ?
          AND github_repo = ?
          AND (? IS NULL OR pr_number = ?)
        """,
        (issue_id, github_repo, pr_number, pr_number),
    )
    row = await cur.fetchone()
    if row is None:
        return False

    cur = await conn.execute(
        """
        DELETE FROM issue_prs
        WHERE issue_id = ?
          AND github_repo = ?
          AND (? IS NULL OR pr_number = ?)
        """,
        (issue_id, github_repo, pr_number, pr_number),
    )
    deleted = (cur.rowcount or 0) > 0
    if deleted:
        await state_transitions.record_transition(
            conn,
            issue_id,
            "issue_prs",
            "__row__",
            f"{github_repo}#{row['pr_number']}",
            None,
        )
    if commit:
        await conn.commit()
    return deleted


async def list_orphaned_review_prs(conn: aiosqlite.Connection) -> list[IssuePR]:
    """PRs whose review run died (last review run is dead, none running).

    Used to auto-resurrect review monitors that crashed mid-flight.
    The cooldown (don't restart if a review run started recently) is enforced
    in the caller.
    """
    live_placeholders = ",".join("?" * len(LIVE_STATUSES))
    resurrect_placeholders = ",".join("?" * len(REVIEW_RESURRECT_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT p.issue_id, i.identifier, i.title, i.team_key, p.github_repo,
               p.binding_key, p.pr_number, p.pr_url, p.created_at, p.merged_at
        FROM issue_prs p
        JOIN issues i ON i.id = p.issue_id
        WHERE p.merged_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'review'
                AND r.status IN ({live_placeholders})
          )
          AND (
              SELECT r.status FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'review'
                AND r.started_at >= p.created_at
              ORDER BY r.started_at DESC, r.rowid DESC
              LIMIT 1
          ) IN ({resurrect_placeholders})
          AND NOT EXISTS (
              SELECT 1 FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'merge'
                AND r.status IN ('running', 'completed', 'done', 'needs_approval')
                AND r.started_at >= p.created_at
        )
        ORDER BY p.created_at ASC
        """,
        (*LIVE_STATUSES, *REVIEW_RESURRECT_STATUSES),
    )
    rows = await cur.fetchall()
    return [_row_to_issue_pr(r) for r in rows]


async def list_merge_candidates(conn: aiosqlite.Connection) -> list[IssuePR]:
    """PRs whose Review handoff completed and whose Merge has not finished.

    The Review stage records a completed handoff row immediately after
    pinging `@codex review`; later ticks keep re-checking the linked PR until
    the review classifier says it is approved and mergeable.
    """
    cur = await conn.execute(
        """
        SELECT p.issue_id, i.identifier, i.title, i.team_key, p.github_repo,
               p.binding_key, p.pr_number, p.pr_url, p.created_at, p.merged_at
        FROM issue_prs p
        JOIN issues i ON i.id = p.issue_id
        WHERE p.merged_at IS NULL
          AND EXISTS (
              SELECT 1 FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'review'
                AND r.status IN ('running', 'completed')
                AND r.started_at >= p.created_at
          )
          AND NOT EXISTS (
              SELECT 1 FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'merge'
                AND r.status IN ('running', 'done', 'needs_approval')
                AND r.started_at >= p.created_at
          )
        ORDER BY p.created_at ASC
        """
    )
    rows = await cur.fetchall()
    return [_row_to_issue_pr(r) for r in rows]


__all__ = [
    "IssuePR",
    "delete",
    "get",
    "get_for_issue",
    "has_for_issue",
    "has_orphaned_review_pr",
    "list_merge_candidates",
    "list_orphaned_review_prs",
    "mark_merged",
    "update_merged",
    "upsert",
]
