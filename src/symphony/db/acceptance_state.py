"""DAO for the `acceptance_state` table.

Acceptance rows are created lazily, like `review_state`. The durable row stores
the current code-only verdict and is already shaped for later criteria
extraction, artifacts, and retry accounting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import aiosqlite

from . import state_transitions

AcceptanceMode = Literal["off", "code_only", "dev", "preview"]
AcceptanceVerdictKind = Literal["pass", "reject", "infra_error"]
_INFRA_RETRY_LIMIT = 2


@dataclass(frozen=True)
class AcceptanceState:
    iteration: int
    pr_number: int | None
    pr_url: str
    pr_head_sha: str
    mode: AcceptanceMode | str
    preview_url: str
    extracted_criteria: str
    last_verdict: AcceptanceVerdictKind | str
    last_artifacts_url: str
    infra_retries: int


_TRACKED_FIELDS = (
    "iteration",
    "pr_number",
    "pr_url",
    "pr_head_sha",
    "mode",
    "preview_url",
    "extracted_criteria",
    "last_verdict",
    "last_artifacts_url",
    "infra_retries",
)


async def _get_existing(
    conn: aiosqlite.Connection, issue_id: str
) -> AcceptanceState | None:
    cur = await conn.execute(
        """
        SELECT
            iteration,
            pr_number,
            pr_url,
            pr_head_sha,
            mode,
            preview_url,
            extracted_criteria,
            last_verdict,
            last_artifacts_url,
            infra_retries
        FROM acceptance_state
        WHERE issue_id = ?
        """,
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return AcceptanceState(
        iteration=int(row["iteration"]),
        pr_number=int(row["pr_number"]) if row["pr_number"] is not None else None,
        pr_url=str(row["pr_url"]),
        pr_head_sha=str(row["pr_head_sha"]),
        mode=str(row["mode"]),
        preview_url=str(row["preview_url"]),
        extracted_criteria=str(row["extracted_criteria"]),
        last_verdict=str(row["last_verdict"]),
        last_artifacts_url=str(row["last_artifacts_url"]),
        infra_retries=int(row["infra_retries"]),
    )


async def _record_transitions(
    conn: aiosqlite.Connection,
    issue_id: str,
    old: AcceptanceState | None,
    new: AcceptanceState,
) -> None:
    if old is None:
        await state_transitions.record_transition(
            conn, issue_id, "acceptance_state", "__row__", None, "created"
        )
        return

    for field in _TRACKED_FIELDS:
        old_value = getattr(old, field)
        new_value = getattr(new, field)
        if old_value != new_value:
            await state_transitions.record_transition(
                conn, issue_id, "acceptance_state", field, old_value, new_value
            )


async def get(conn: aiosqlite.Connection, issue_id: str) -> AcceptanceState:
    existing = await _get_existing(conn, issue_id)
    if existing is not None:
        return existing
    return AcceptanceState(
        iteration=0,
        pr_number=None,
        pr_url="",
        pr_head_sha="",
        mode="off",
        preview_url="",
        extracted_criteria="",
        last_verdict="",
        last_artifacts_url="",
        infra_retries=0,
    )


async def begin_acceptance(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    pr_number: int | None,
    pr_url: str,
    pr_head_sha: str,
    mode: AcceptanceMode,
    preview_url: str,
    extracted_criteria: str,
    reset_iteration: bool = True,
) -> None:
    """Initialize durable state for a fresh Acceptance stage."""
    old = await _get_existing(conn, issue_id)
    iteration = 0 if reset_iteration or old is None else old.iteration
    await conn.execute(
        """
        INSERT INTO acceptance_state (
            issue_id, iteration, pr_number, pr_url, pr_head_sha, mode, preview_url,
            extracted_criteria, last_verdict, last_artifacts_url, infra_retries
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', 0)
        ON CONFLICT(issue_id) DO UPDATE SET
            iteration = excluded.iteration,
            pr_number = excluded.pr_number,
            pr_url = excluded.pr_url,
            pr_head_sha = excluded.pr_head_sha,
            mode = excluded.mode,
            preview_url = excluded.preview_url,
            extracted_criteria = excluded.extracted_criteria,
            last_verdict = '',
            last_artifacts_url = '',
            infra_retries = CASE
                WHEN COALESCE(pr_number, -1) = COALESCE(excluded.pr_number, -1)
                 AND pr_url = excluded.pr_url
                 AND pr_head_sha = excluded.pr_head_sha
                 AND mode = excluded.mode
                THEN infra_retries
                ELSE 0
            END
        """,
        (
            issue_id,
            iteration,
            pr_number,
            pr_url,
            pr_head_sha,
            mode,
            preview_url,
            extracted_criteria,
        ),
    )
    new = await _get_existing(conn, issue_id)
    assert new is not None
    await _record_transitions(conn, issue_id, old, new)
    await conn.commit()


async def record_verdict(
    conn: aiosqlite.Connection,
    issue_id: str,
    *,
    verdict: AcceptanceVerdictKind,
    artifacts_url: str,
    preview_url: str | None = None,
) -> None:
    old = await _get_existing(conn, issue_id)
    await conn.execute(
        """
        INSERT INTO acceptance_state (
            issue_id, iteration, preview_url, last_verdict, last_artifacts_url
        )
        VALUES (?, 0, ?, ?, ?)
        ON CONFLICT(issue_id) DO UPDATE SET
            preview_url = CASE
                WHEN excluded.preview_url != '' THEN excluded.preview_url
                ELSE preview_url
            END,
            last_verdict = excluded.last_verdict,
            last_artifacts_url = excluded.last_artifacts_url
        """,
        (issue_id, preview_url or "", verdict, artifacts_url),
    )
    new = await _get_existing(conn, issue_id)
    assert new is not None
    await _record_transitions(conn, issue_id, old, new)
    await conn.commit()


async def bump_iteration(conn: aiosqlite.Connection, issue_id: str) -> int:
    old = await _get_existing(conn, issue_id)
    await conn.execute(
        """
        INSERT INTO acceptance_state (issue_id, iteration)
        VALUES (?, 1)
        ON CONFLICT(issue_id) DO UPDATE SET iteration = iteration + 1
        """,
        (issue_id,),
    )
    cur = await conn.execute(
        "SELECT iteration FROM acceptance_state WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    assert row is not None
    new = await _get_existing(conn, issue_id)
    assert new is not None
    await _record_transitions(conn, issue_id, old, new)
    await conn.commit()
    return int(row["iteration"])


async def bump_infra_retries(conn: aiosqlite.Connection, issue_id: str) -> int:
    old = await _get_existing(conn, issue_id)
    await conn.execute(
        """
        INSERT INTO acceptance_state (issue_id, infra_retries)
        VALUES (?, 1)
        ON CONFLICT(issue_id) DO UPDATE SET infra_retries = CASE
            WHEN infra_retries >= ? THEN ?
            ELSE infra_retries + 1
        END
        """,
        (issue_id, _INFRA_RETRY_LIMIT, _INFRA_RETRY_LIMIT),
    )
    cur = await conn.execute(
        "SELECT infra_retries FROM acceptance_state WHERE issue_id = ?",
        (issue_id,),
    )
    row = await cur.fetchone()
    assert row is not None
    new = await _get_existing(conn, issue_id)
    assert new is not None
    await _record_transitions(conn, issue_id, old, new)
    await conn.commit()
    return int(row["infra_retries"])


async def reset(conn: aiosqlite.Connection, issue_id: str) -> None:
    old = await _get_existing(conn, issue_id)
    await conn.execute(
        """
        INSERT INTO acceptance_state (
            issue_id, iteration, pr_number, pr_url, pr_head_sha, mode, preview_url,
            extracted_criteria, last_verdict, last_artifacts_url, infra_retries
        )
        VALUES (?, 0, NULL, '', '', 'off', '', '', '', '', 0)
        ON CONFLICT(issue_id) DO UPDATE SET
            iteration = 0,
            pr_number = NULL,
            pr_url = '',
            pr_head_sha = '',
            mode = 'off',
            preview_url = '',
            extracted_criteria = '',
            last_verdict = '',
            last_artifacts_url = '',
            infra_retries = 0
        """,
        (issue_id,),
    )
    new = await _get_existing(conn, issue_id)
    assert new is not None
    await _record_transitions(conn, issue_id, old, new)
    await conn.commit()


__all__ = [
    "AcceptanceMode",
    "AcceptanceState",
    "AcceptanceVerdictKind",
    "begin_acceptance",
    "bump_infra_retries",
    "bump_iteration",
    "get",
    "record_verdict",
    "reset",
]
