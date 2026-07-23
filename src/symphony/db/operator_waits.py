"""DAO for the `operator_waits` table.

Rows represent stopped runs that are still waiting for an operator slash
command, such as a failed implement run or a manually stopped review
monitor that can later resume via `$retry` or `$approve`.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite

from . import state_transitions

KIND_IMPLEMENT_FAILED = "implement_failed"
KIND_IMPLEMENT_BLOCKED = "implement_blocked"
KIND_DELIVER_FAILED = "deliver_failed"
KIND_REVIEW_FAILED = "review_failed"
KIND_REVIEW_STOPPED = "review_stopped"
KIND_MERGE = "merge"
KIND_REVIEW_CAP = "review_cap"
KIND_ACCEPTANCE_BLOCKED = "acceptance_blocked"
KIND_ACCEPTANCE_REJECTED = "acceptance_rejected"
# Soft per-issue token budget tripped at a dispatch boundary. The live agent
# is never killed; the next run simply isn't dispatched. `$approve`/👍 grants
# another budget window and resumes; `$reject` blocks.
KIND_BUDGET_EXCEEDED = "budget_exceeded"


@dataclass(frozen=True)
class OperatorWait:
    issue_id: str
    run_id: str
    kind: str
    provider: str
    tracker_provider: str
    tracker_site: str
    linear_team_key: str
    github_repo: str
    issue_label: str
    created_at: str
    # For `deliver_failed` waits: the real local-review outcome at park time
    # (a `LoopOutcome` value). Lets a `$retry` after a restart preserve the
    # human-approval gate instead of reconstructing a synthetic APPROVED.
    # NULL/None for every other wait kind.
    local_review_outcome: str | None = None


async def upsert(
    conn: aiosqlite.Connection,
    *,
    issue_id: str,
    run_id: str,
    kind: str,
    linear_team_key: str,
    github_repo: str,
    issue_label: str,
    created_at: str,
    provider: str | None = None,
    tracker_provider: str = "linear",
    tracker_site: str = "default",
    local_review_outcome: str | None = None,
    commit: bool = True,
) -> None:
    old = await get(conn, issue_id)
    effective_provider = provider or tracker_provider
    await conn.execute(
        """
        INSERT INTO operator_waits (
            issue_id,
            run_id,
            kind,
            provider,
            tracker_provider,
            tracker_site,
            linear_team_key,
            github_repo,
            issue_label,
            created_at,
            local_review_outcome
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(issue_id) DO UPDATE SET
            run_id = excluded.run_id,
            kind = excluded.kind,
            provider = excluded.provider,
            tracker_provider = excluded.tracker_provider,
            tracker_site = excluded.tracker_site,
            linear_team_key = excluded.linear_team_key,
            github_repo = excluded.github_repo,
            issue_label = excluded.issue_label,
            created_at = excluded.created_at,
            local_review_outcome = excluded.local_review_outcome
        """,
        (
            issue_id,
            run_id,
            kind,
            effective_provider,
            tracker_provider,
            tracker_site,
            linear_team_key,
            github_repo,
            issue_label,
            created_at,
            local_review_outcome,
        ),
    )
    if old is None:
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "__row__", None, "created"
        )
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "kind", None, kind
        )
    elif old.kind != kind:
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "kind", old.kind, kind
        )
    if commit:
        await conn.commit()


async def list_all(conn: aiosqlite.Connection) -> list[OperatorWait]:
    cur = await conn.execute(
        """
        SELECT
            issue_id,
            run_id,
            kind,
            provider,
            tracker_provider,
            tracker_site,
            linear_team_key,
            github_repo,
            issue_label,
            created_at,
            local_review_outcome
        FROM operator_waits
        ORDER BY created_at, issue_id
        """
    )
    rows = await cur.fetchall()
    return [
        OperatorWait(
            issue_id=str(row["issue_id"]),
            run_id=str(row["run_id"]),
            kind=str(row["kind"]),
            provider=str(row["provider"]),
            tracker_provider=str(row["tracker_provider"]),
            tracker_site=str(row["tracker_site"]),
            linear_team_key=str(row["linear_team_key"]),
            github_repo=str(row["github_repo"]),
            issue_label=str(row["issue_label"] or ""),
            created_at=str(row["created_at"]),
            local_review_outcome=(
                str(row["local_review_outcome"])
                if row["local_review_outcome"] is not None
                else None
            ),
        )
        for row in rows
    ]


async def open_identifiers_for_natural_key(
    conn: aiosqlite.Connection,
    natural_key: tuple[str, str, str, str, str],
) -> list[str]:
    """Human issue identifiers of waits parked within a binding's natural-key
    scope `(linear_team_key, github_repo, issue_label, tracker_provider,
    tracker_site)`.

    A drain-guard blocker: resolves through `issues` like
    `issue_prs.open_identifiers_for_binding_key`'s COALESCE, so the blocker
    list renders the same identifier form (`ENG-1`) as `running_runs`/
    `open_prs` instead of the internal issue id (SYM-193 review)."""
    linear_team_key, github_repo, issue_label, tracker_provider, tracker_site = natural_key
    cur = await conn.execute(
        """
        SELECT COALESCE(i.identifier, w.issue_id) AS identifier
          FROM operator_waits w
          LEFT JOIN issues i ON i.id = w.issue_id
         WHERE w.linear_team_key = ?
           AND w.github_repo = ?
           AND w.issue_label = ?
           AND w.tracker_provider = ?
           AND w.tracker_site = ?
         ORDER BY identifier
        """,
        (linear_team_key, github_repo, issue_label, tracker_provider, tracker_site),
    )
    return [str(row["identifier"]) for row in await cur.fetchall()]


async def get(conn: aiosqlite.Connection, issue_id: str) -> OperatorWait | None:
    cur = await conn.execute(
        """
        SELECT
            issue_id,
            run_id,
            kind,
            provider,
            tracker_provider,
            tracker_site,
            linear_team_key,
            github_repo,
            issue_label,
            created_at,
            local_review_outcome
        FROM operator_waits
        WHERE issue_id = ?
        """,
        (issue_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return OperatorWait(
        issue_id=str(row["issue_id"]),
        run_id=str(row["run_id"]),
        kind=str(row["kind"]),
        provider=str(row["provider"]),
        tracker_provider=str(row["tracker_provider"]),
        tracker_site=str(row["tracker_site"]),
        linear_team_key=str(row["linear_team_key"]),
        github_repo=str(row["github_repo"]),
        issue_label=str(row["issue_label"] or ""),
        created_at=str(row["created_at"]),
        local_review_outcome=(
            str(row["local_review_outcome"]) if row["local_review_outcome"] is not None else None
        ),
    )


async def get_by_run_id(conn: aiosqlite.Connection, run_id: str) -> OperatorWait | None:
    cur = await conn.execute(
        """
        SELECT
            issue_id,
            run_id,
            kind,
            provider,
            tracker_provider,
            tracker_site,
            linear_team_key,
            github_repo,
            issue_label,
            created_at,
            local_review_outcome
        FROM operator_waits
        WHERE run_id = ?
        """,
        (run_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return OperatorWait(
        issue_id=str(row["issue_id"]),
        run_id=str(row["run_id"]),
        kind=str(row["kind"]),
        provider=str(row["provider"]),
        tracker_provider=str(row["tracker_provider"]),
        tracker_site=str(row["tracker_site"]),
        linear_team_key=str(row["linear_team_key"]),
        github_repo=str(row["github_repo"]),
        issue_label=str(row["issue_label"] or ""),
        created_at=str(row["created_at"]),
        local_review_outcome=(
            str(row["local_review_outcome"]) if row["local_review_outcome"] is not None else None
        ),
    )


async def delete(
    conn: aiosqlite.Connection,
    issue_id: str,
    run_id: str | None = None,
    *,
    commit: bool = True,
) -> None:
    old = await get(conn, issue_id)
    if run_id is None:
        cur = await conn.execute("DELETE FROM operator_waits WHERE issue_id = ?", (issue_id,))
    else:
        cur = await conn.execute(
            "DELETE FROM operator_waits WHERE issue_id = ? AND run_id = ?",
            (issue_id, run_id),
        )
    if old is not None and (cur.rowcount or 0) > 0:
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "__row__", "removed", None
        )
        await state_transitions.record_transition(
            conn, issue_id, "operator_waits", "kind", old.kind, None
        )
    if commit:
        await conn.commit()


__all__ = [
    "KIND_ACCEPTANCE_BLOCKED",
    "KIND_ACCEPTANCE_REJECTED",
    "KIND_BUDGET_EXCEEDED",
    "KIND_DELIVER_FAILED",
    "KIND_IMPLEMENT_BLOCKED",
    "KIND_IMPLEMENT_FAILED",
    "KIND_MERGE",
    "KIND_REVIEW_CAP",
    "KIND_REVIEW_FAILED",
    "KIND_REVIEW_STOPPED",
    "OperatorWait",
    "delete",
    "get",
    "get_by_run_id",
    "list_all",
    "upsert",
]
