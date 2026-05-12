"""DAO for the `review_state` table.

One row per issue. Carries the review iteration counter (capped at 12 →
`needs_approval` per PRD §pipeline) and the most recent trigger
signature so dedup logic survives an orchestrator restart.

Rows are created lazily on first write — `get()` falls back to a zero
state when the row is absent. CI fetch failures are stored here too so
flaky `gh pr checks` calls cannot avoid the five-failure tripwire by
restarting the orchestrator between attempts.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


@dataclass(frozen=True)
class ReviewState:
    iteration: int
    last_trigger_signature: str
    ci_fetch_failures: int
    pr_number: int | None
    pr_url: str
    github_repo: str
    issue_label: str
    codex_lgtm_comment_id: str


async def get(conn: aiosqlite.Connection, issue_id: str) -> ReviewState:
    cur = await conn.execute(
        """
        SELECT
            iteration,
            last_trigger_signature,
            ci_fetch_failures,
            pr_number,
            pr_url,
            github_repo,
            issue_label,
            codex_lgtm_comment_id
        FROM review_state
        WHERE issue_id = ?
        """,
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return ReviewState(
            iteration=0,
            last_trigger_signature="",
            ci_fetch_failures=0,
            pr_number=None,
            pr_url="",
            github_repo="",
            issue_label="",
            codex_lgtm_comment_id="",
        )
    return ReviewState(
        iteration=int(row["iteration"]),
        last_trigger_signature=str(row["last_trigger_signature"]),
        ci_fetch_failures=int(row["ci_fetch_failures"]),
        pr_number=int(row["pr_number"]) if row["pr_number"] is not None else None,
        pr_url=str(row["pr_url"]),
        github_repo=str(row["github_repo"]),
        issue_label=str(row["issue_label"]),
        codex_lgtm_comment_id=str(row["codex_lgtm_comment_id"]),
    )


async def begin_review(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    pr_number: int | None,
    pr_url: str,
    github_repo: str,
    issue_label: str | None,
) -> None:
    """Initialize durable state for a fresh Review stage."""
    await conn.execute(
        """
        INSERT INTO review_state (
            issue_id, iteration, last_trigger_signature,
            ci_fetch_failures, pr_number, pr_url, github_repo, issue_label,
            codex_lgtm_comment_id
        )
        VALUES (?, 0, '', 0, ?, ?, ?, ?, '')
        ON CONFLICT(issue_id) DO UPDATE SET
            iteration = 0,
            last_trigger_signature = '',
            ci_fetch_failures = 0,
            pr_number = excluded.pr_number,
            pr_url = excluded.pr_url,
            github_repo = excluded.github_repo,
            issue_label = excluded.issue_label,
            codex_lgtm_comment_id = ''
        """,
        (issue_id, pr_number, pr_url, github_repo, issue_label or ""),
    )
    await conn.commit()


async def bump_iteration(conn: aiosqlite.Connection, issue_id: str) -> int:
    """Increment the counter atomically and return the new value."""
    await conn.execute(
        """
        INSERT INTO review_state (
            issue_id, iteration, last_trigger_signature, ci_fetch_failures
        )
        VALUES (?, 1, '', 0)
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
        INSERT INTO review_state (
            issue_id, iteration, last_trigger_signature, ci_fetch_failures
        )
        VALUES (?, 0, ?, 0)
        ON CONFLICT(issue_id) DO UPDATE SET last_trigger_signature = excluded.last_trigger_signature
        """,
        (issue_id, signature),
    )
    await conn.commit()


async def bump_ci_fetch_failures(conn: aiosqlite.Connection, issue_id: str) -> int:
    """Increment consecutive `gh pr checks` fetch failures and return the count."""
    await conn.execute(
        """
        INSERT INTO review_state (
            issue_id, iteration, last_trigger_signature, ci_fetch_failures
        )
        VALUES (?, 0, '', 1)
        ON CONFLICT(issue_id) DO UPDATE SET
            ci_fetch_failures = ci_fetch_failures + 1
        """,
        (issue_id,),
    )
    await conn.commit()
    cur = await conn.execute(
        "SELECT ci_fetch_failures FROM review_state WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    assert row is not None
    return int(row["ci_fetch_failures"])


async def reset_ci_fetch_failures(
    conn: aiosqlite.Connection, issue_id: str
) -> None:
    await conn.execute(
        """
        INSERT INTO review_state (
            issue_id, iteration, last_trigger_signature, ci_fetch_failures
        )
        VALUES (?, 0, '', 0)
        ON CONFLICT(issue_id) DO UPDATE SET ci_fetch_failures = 0
        """,
        (issue_id,),
    )
    await conn.commit()


async def set_codex_lgtm_comment_id(
    conn: aiosqlite.Connection, issue_id: str, comment_id: str
) -> None:
    """Record the GitHub comment ID of the Codex 'no issues' comment so we
    don't re-post the Linear notification on subsequent polls."""
    await conn.execute(
        """
        INSERT INTO review_state (
            issue_id, iteration, last_trigger_signature, ci_fetch_failures,
            codex_lgtm_comment_id
        )
        VALUES (?, 0, '', 0, ?)
        ON CONFLICT(issue_id) DO UPDATE SET
            codex_lgtm_comment_id = excluded.codex_lgtm_comment_id
        """,
        (issue_id, comment_id),
    )
    await conn.commit()


async def reset(conn: aiosqlite.Connection, issue_id: str) -> None:
    """Clear iteration and signature — used when leaving Review (e.g.
    Merge starts, or `$retry` re-enters the stage)."""
    await conn.execute(
        """
        INSERT INTO review_state (
            issue_id, iteration, last_trigger_signature,
            ci_fetch_failures, pr_number, pr_url, github_repo, issue_label,
            codex_lgtm_comment_id
        )
        VALUES (?, 0, '', 0, NULL, '', '', '', '')
        ON CONFLICT(issue_id) DO UPDATE SET
            iteration = 0,
            last_trigger_signature = '',
            ci_fetch_failures = 0,
            pr_number = NULL,
            pr_url = '',
            github_repo = '',
            issue_label = '',
            codex_lgtm_comment_id = ''
        """,
        (issue_id,),
    )
    await conn.commit()


__all__ = [
    "ReviewState",
    "begin_review",
    "bump_iteration",
    "bump_ci_fetch_failures",
    "get",
    "reset",
    "reset_ci_fetch_failures",
    "set_codex_lgtm_comment_id",
    "set_signature",
]
