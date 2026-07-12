"""The tracker_queue snapshot: DAO replace semantics and the poll-scan write.

The UI board's Todo/Waiting lanes read this table, so the scan must mirror
exactly what the tracker returned — ready and waiting issues, waiting filtered
to the binding's label — and rewrite a team's rows wholesale each scan.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.tracker import Blocker


async def _rows(conn: aiosqlite.Connection) -> list[dict[str, object]]:
    cur = await conn.execute(
        """
        SELECT team_key, scope, issue_id, identifier, queue, state_name, blocked_by, seen_at
        FROM tracker_queue
        ORDER BY team_key, scope, identifier
        """
    )
    return [dict(r) for r in await cur.fetchall()]


def _row(
    ident: str,
    *,
    queue: str = "ready",
    state_name: str = "Todo",
    blocked_by: str = "",
) -> db.tracker_queue.QueueRow:
    return db.tracker_queue.QueueRow(
        issue_id=f"iss-{ident}",
        identifier=ident,
        title=f"title {ident}",
        queue=queue,
        state_name=state_name,
        blocked_by=blocked_by,
    )


@pytest.mark.asyncio
async def test_replace_scan_rewrites_only_that_team_and_scope(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.tracker_queue.replace_scan(
            conn,
            team_key="ENG",
            scope="org/repo#symphony",
            rows=[_row("ENG-1")],
            seen_at="2026-07-12T09:00:00Z",
        )
        # A second binding on the same team writes under its own scope.
        await db.tracker_queue.replace_scan(
            conn,
            team_key="ENG",
            scope="org/other#",
            rows=[_row("ENG-7")],
            seen_at="2026-07-12T09:00:00Z",
        )
        await db.tracker_queue.replace_scan(
            conn,
            team_key="WEB",
            scope="org/web#",
            rows=[_row("WEB-9", queue="waiting", state_name="Waiting", blocked_by="WEB-8")],
            seen_at="2026-07-12T09:00:00Z",
        )

        # A later scan of the first binding replaces only its own rows.
        await db.tracker_queue.replace_scan(
            conn,
            team_key="ENG",
            scope="org/repo#symphony",
            rows=[_row("ENG-2")],
            seen_at="2026-07-12T09:05:00Z",
        )

        rows = await _rows(conn)
        assert [(r["team_key"], r["scope"], r["identifier"]) for r in rows] == [
            ("ENG", "org/other#", "ENG-7"),
            ("ENG", "org/repo#symphony", "ENG-2"),
            ("WEB", "org/web#", "WEB-9"),
        ]

        # An empty scan clears the scope's lanes.
        await db.tracker_queue.replace_scan(
            conn,
            team_key="ENG",
            scope="org/repo#symphony",
            rows=[],
            seen_at="2026-07-12T09:10:00Z",
        )
        rows = await _rows(conn)
        assert [(r["team_key"], r["identifier"]) for r in rows] == [
            ("ENG", "ENG-7"),
            ("WEB", "WEB-9"),
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_replace_scan_preserves_first_seen_while_queue_unchanged(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.tracker_queue.replace_scan(
            conn,
            team_key="ENG",
            scope="s",
            rows=[_row("ENG-1", queue="waiting", state_name="Waiting", blocked_by="ENG-0")],
            seen_at="2026-07-12T09:00:00Z",
        )
        # Re-scan: still waiting → seen_at keeps the first sighting.
        await db.tracker_queue.replace_scan(
            conn,
            team_key="ENG",
            scope="s",
            rows=[_row("ENG-1", queue="waiting", state_name="Waiting", blocked_by="ENG-0")],
            seen_at="2026-07-12T09:05:00Z",
        )
        rows = await _rows(conn)
        assert rows[0]["seen_at"] == "2026-07-12T09:00:00Z"

        # Queue change (waiting → ready) resets the clock.
        await db.tracker_queue.replace_scan(
            conn,
            team_key="ENG",
            scope="s",
            rows=[_row("ENG-1")],
            seen_at="2026-07-12T09:10:00Z",
        )
        rows = await _rows(conn)
        assert rows[0]["seen_at"] == "2026-07-12T09:10:00Z"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_prune_scopes_drops_unconfigured_bindings(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.tracker_queue.replace_scan(
            conn,
            team_key="ENG",
            scope="org/repo#symphony#linear#default",
            rows=[_row("ENG-1")],
            seen_at="2026-07-12T09:00:00Z",
        )
        # A binding that was since removed from the config.
        await db.tracker_queue.replace_scan(
            conn,
            team_key="OLD",
            scope="org/gone##linear#default",
            rows=[_row("OLD-1")],
            seen_at="2026-07-12T09:00:00Z",
        )

        await db.tracker_queue.prune_scopes(
            conn, keep=[("ENG", "org/repo#symphony#linear#default")]
        )
        rows = await _rows(conn)
        assert [r["identifier"] for r in rows] == ["ENG-1"]

        # No configured bindings at all → the table empties.
        await db.tracker_queue.prune_scopes(conn, keep=[])
        assert await _rows(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_mark_waiting_and_remove_adjust_single_rows(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.tracker_queue.replace_scan(
            conn,
            team_key="ENG",
            scope="s",
            rows=[_row("ENG-1"), _row("ENG-2")],
            seen_at="2026-07-12T09:00:00Z",
        )
        # A same-tick park flips ready → waiting with the blocker list.
        await db.tracker_queue.mark_waiting(
            conn,
            team_key="ENG",
            scope="s",
            issue_id="iss-ENG-1",
            state_name="Waiting",
            blocked_by="ENG-0",
            seen_at="2026-07-12T09:00:30Z",
        )
        # A guard that moves an issue out of the queue lanes drops the row.
        await db.tracker_queue.remove(conn, team_key="ENG", scope="s", issue_id="iss-ENG-2")

        rows = await _rows(conn)
        assert [(r["identifier"], r["queue"], r["blocked_by"]) for r in rows] == [
            ("ENG-1", "waiting", "ENG-0"),
        ]
    finally:
        await conn.close()


def _issue(
    uid: str,
    ident: str,
    *,
    state_name: str = "Todo",
    labels: list[str] | None = None,
    blocked_by: list[Blocker] | None = None,
) -> LinearIssue:
    return LinearIssue(
        id=uid,
        identifier=ident,
        title=f"title {ident}",
        description="",
        url="https://linear.app/x",
        state_id="state-x",
        state_name=state_name,
        state_type="unstarted",
        team_key="ENG",
        labels=labels or [],
        blocked_by=blocked_by or [],
    )


def _open_blocker(ident: str) -> Blocker:
    return Blocker(id=f"uuid-{ident}", identifier=ident, state_type="started", archived=False)


@pytest.mark.asyncio
async def test_scan_persists_ready_and_waiting_queue_snapshot(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="symphony",
            linear_states=LinearStates(
                ready="Todo", code_review="Needs Approval", waiting="Waiting"
            ),
        )
        # Zero capacity: the scan persists the snapshot and skips dispatch.
        cfg = Config(repos=[binding], global_max_concurrent=0)
        linear = AsyncMock()

        async def issues_in_state(
            team_key: str, state_name: str, label: str | None = None
        ) -> list[LinearIssue]:
            if state_name == "Todo":
                return [_issue("iss-1", "ENG-1")]
            return [
                _issue(
                    "iss-2",
                    "ENG-2",
                    state_name="Waiting",
                    labels=["symphony"],
                    blocked_by=[_open_blocker("ENG-1")],
                ),
                # No `symphony` label → excluded from the snapshot.
                _issue(
                    "iss-3",
                    "ENG-3",
                    state_name="Waiting",
                    blocked_by=[_open_blocker("ENG-1")],
                ),
                # No open blockers left: auto-unblocked to Ready during the
                # same scan, so the snapshot records it as ready already.
                _issue("iss-4", "ENG-4", state_name="Waiting", labels=["symphony"]),
                # Raced the concurrent Ready fetch (also in the ready list) —
                # the ready row must win instead of violating the PK.
                _issue(
                    "iss-1",
                    "ENG-1",
                    state_name="Waiting",
                    labels=["symphony"],
                    blocked_by=[_open_blocker("ENG-9")],
                ),
            ]

        linear.issues_in_state = AsyncMock(side_effect=issues_in_state)
        linear.move_issue = AsyncMock()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": {"Todo": "state-todo"}}  # noqa: SLF001

        tasks = await orch._scan_binding(binding)  # noqa: SLF001
        assert tasks == []

        linear.move_issue.assert_awaited_once_with("iss-4", "state-todo")
        rows = await _rows(conn)
        assert [(r["identifier"], r["queue"], r["state_name"], r["blocked_by"]) for r in rows] == [
            ("ENG-1", "ready", "Todo", ""),
            ("ENG-2", "waiting", "Waiting", "ENG-1"),
            ("ENG-4", "ready", "Todo", ""),
        ]
        assert all(r["scope"] == "org/repo#symphony#linear#default" for r in rows)
    finally:
        await conn.close()
