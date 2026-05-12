"""DAO for the `issue_prs` table."""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


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
    await conn.commit()


async def mark_merged(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    github_repo: str,
    merged_at: str,
) -> None:
    await conn.execute(
        """
        UPDATE issue_prs
        SET merged_at = ?
        WHERE issue_id = ? AND github_repo = ?
        """,
        (merged_at, issue_id, github_repo),
    )
    await conn.commit()


async def list_orphaned_review_prs(conn: aiosqlite.Connection) -> list[IssuePR]:
    """PRs whose review run died (last review run is failed, none running).

    Used to auto-resurrect review monitors that crashed mid-flight.
    The cooldown (don't restart if a review run started recently) is enforced
    in the caller.
    """
    cur = await conn.execute(
        """
        SELECT p.issue_id, i.identifier, i.title, i.team_key, p.github_repo,
               p.binding_key, p.pr_number, p.pr_url, p.created_at, p.merged_at
        FROM issue_prs p
        JOIN issues i ON i.id = p.issue_id
        WHERE p.merged_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'review'
                AND r.status = 'running'
          )
          AND (
              SELECT r.status FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'review'
                AND r.started_at >= p.created_at
              ORDER BY r.started_at DESC, r.rowid DESC
              LIMIT 1
          ) = 'failed'
          AND NOT EXISTS (
              SELECT 1 FROM runs r
              WHERE r.issue_id = p.issue_id
                AND r.stage = 'merge'
                AND r.status IN ('running', 'completed', 'done', 'needs_approval')
          )
        ORDER BY p.created_at ASC
        """
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
