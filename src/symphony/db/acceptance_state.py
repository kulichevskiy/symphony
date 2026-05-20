"""DAO for the `acceptance_state` table.

Acceptance rows are created lazily, like `review_state`. The initial runner is
an always-pass stub, but the durable row is already shaped for later verdict
logic, criteria extraction, artifacts, and retry accounting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import aiosqlite

from . import state_transitions

AcceptanceMode = Literal["off", "code_only", "dev", "preview"]
AcceptanceVerdictKind = Literal["pass", "reject", "infra_error"]


@dataclass(frozen=True)
class AcceptanceState:
    iteration: int
    pr_number: int | None
    pr_url: str
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
    mode: AcceptanceMode,
    preview_url: str,
    extracted_criteria: str,
) -> None:
    """Initialize durable state for a fresh Acceptance stage."""
    old = await _get_existing(conn, issue_id)
    await conn.execute(
        """
        INSERT INTO acceptance_state (
            issue_id, iteration, pr_number, pr_url, mode, preview_url,
            extracted_criteria, last_verdict, last_artifacts_url, infra_retries
        )
        VALUES (?, 0, ?, ?, ?, ?, ?, '', '', 0)
        ON CONFLICT(issue_id) DO UPDATE SET
            iteration = 0,
            pr_number = excluded.pr_number,
            pr_url = excluded.pr_url,
            mode = excluded.mode,
            preview_url = excluded.preview_url,
            extracted_criteria = excluded.extracted_criteria,
            last_verdict = '',
            last_artifacts_url = '',
            infra_retries = 0
        """,
        (issue_id, pr_number, pr_url, mode, preview_url, extracted_criteria),
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
) -> None:
    old = await _get_existing(conn, issue_id)
    await conn.execute(
        """
        INSERT INTO acceptance_state (
            issue_id, iteration, last_verdict, last_artifacts_url
        )
        VALUES (?, 0, ?, ?)
        ON CONFLICT(issue_id) DO UPDATE SET
            last_verdict = excluded.last_verdict,
            last_artifacts_url = excluded.last_artifacts_url
        """,
        (issue_id, verdict, artifacts_url),
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
        ON CONFLICT(issue_id) DO UPDATE SET infra_retries = infra_retries + 1
        """,
        (issue_id,),
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
            issue_id, iteration, pr_number, pr_url, mode, preview_url,
            extracted_criteria, last_verdict, last_artifacts_url, infra_retries
        )
        VALUES (?, 0, NULL, '', 'off', '', '', '', '', 0)
        ON CONFLICT(issue_id) DO UPDATE SET
            iteration = 0,
            pr_number = NULL,
            pr_url = '',
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
