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
        SELECT team_key, issue_id, identifier, queue, state_name, blocked_by
        FROM tracker_queue
        ORDER BY team_key, identifier
        """
    )
    return [dict(r) for r in await cur.fetchall()]


@pytest.mark.asyncio
async def test_replace_team_scan_rewrites_only_that_team(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.tracker_queue.replace_team_scan(
            conn,
            team_key="ENG",
            rows=[
                db.tracker_queue.QueueRow(
                    issue_id="iss-1",
                    identifier="ENG-1",
                    title="one",
                    queue="ready",
                    state_name="Todo",
                )
            ],
            seen_at="2026-07-12T09:00:00Z",
        )
        await db.tracker_queue.replace_team_scan(
            conn,
            team_key="WEB",
            rows=[
                db.tracker_queue.QueueRow(
                    issue_id="iss-9",
                    identifier="WEB-9",
                    title="nine",
                    queue="waiting",
                    state_name="Waiting",
                    blocked_by="WEB-8",
                )
            ],
            seen_at="2026-07-12T09:00:00Z",
        )

        # A later ENG scan with a different set replaces ENG, leaves WEB alone.
        await db.tracker_queue.replace_team_scan(
            conn,
            team_key="ENG",
            rows=[
                db.tracker_queue.QueueRow(
                    issue_id="iss-2",
                    identifier="ENG-2",
                    title="two",
                    queue="ready",
                    state_name="Todo",
                )
            ],
            seen_at="2026-07-12T09:05:00Z",
        )

        rows = await _rows(conn)
        assert [(r["team_key"], r["identifier"], r["queue"]) for r in rows] == [
            ("ENG", "ENG-2", "ready"),
            ("WEB", "WEB-9", "waiting"),
        ]

        # An empty scan clears the team's lanes.
        await db.tracker_queue.replace_team_scan(
            conn, team_key="ENG", rows=[], seen_at="2026-07-12T09:10:00Z"
        )
        rows = await _rows(conn)
        assert [r["team_key"] for r in rows] == ["WEB"]
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
            ]

        linear.issues_in_state = AsyncMock(side_effect=issues_in_state)
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        tasks = await orch._scan_binding(binding)  # noqa: SLF001
        assert tasks == []

        rows = await _rows(conn)
        assert [(r["identifier"], r["queue"], r["state_name"], r["blocked_by"]) for r in rows] == [
            ("ENG-1", "ready", "Todo", ""),
            ("ENG-2", "waiting", "Waiting", "ENG-1"),
        ]
    finally:
        await conn.close()
