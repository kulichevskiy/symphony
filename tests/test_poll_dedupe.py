"""The poll loop dedupes via SQLite, not the old in-memory `_dispatched`
dict. Re-scanning an issue that already has an active run must not
post a second comment."""

from __future__ import annotations

import inspect
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from symphony import db
from symphony.config import Config, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator


def test_orchestrator_no_longer_uses_in_memory_dispatched_dict() -> None:
    src = inspect.getsource(Orchestrator)
    assert "_dispatched" not in src, (
        "the in-memory dedupe ledger must be replaced by a SQLite query"
    )


def _binding() -> RepoBinding:
    return RepoBinding(linear_team_key="ENG", github_repo="org/repo")


def _issue(uid: str = "iss-1", ident: str = "ENG-1") -> LinearIssue:
    return LinearIssue(
        id=uid,
        identifier=ident,
        title="t",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
    )


@pytest.mark.asyncio
async def test_scan_skips_issues_with_active_run(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(cfg, linear, conn)

        # First tick dispatches and records an active run.
        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert linear.post_comment.await_count == 1

        # Second tick must dedupe via the SQLite `runs` table, not a dict.
        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert linear.post_comment.await_count == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_run_row_is_persisted_before_post_comment(tmp_path: Path) -> None:
    """Dedupe correctness: the `runs` row must exist before the API call so
    a crash or DB error after `post_comment` can't leave the issue
    dispatched-but-unrecorded. We assert ordering by checking the DB from
    inside the mocked `post_comment`."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])

        observed: dict[str, bool] = {}

        async def _post(issue_id: str, body: str) -> str:
            observed["had_active_when_posting"] = await db.runs.has_active(
                conn, issue_id
            )
            return "cmt-1"

        linear.post_comment = AsyncMock(side_effect=_post)

        orch = Orchestrator(cfg, linear, conn)
        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001

        assert observed.get("had_active_when_posting") is True
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failed_announce_clears_dedupe_so_next_tick_retries(
    tmp_path: Path,
) -> None:
    """If `post_comment` raises, the run row must be marked non-live so the
    next poll can retry. Otherwise a transient Linear error would jam the
    issue forever behind its own dedupe row."""
    from symphony.linear.client import LinearError

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        # First call fails, second succeeds.
        linear.post_comment = AsyncMock(
            side_effect=[LinearError("boom"), "cmt-1"]
        )

        orch = Orchestrator(cfg, linear, conn)

        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert linear.post_comment.await_count == 1
        # The failed announce row exists but is no longer live, so dedupe
        # lets the next tick retry.
        assert await db.runs.has_active(conn, "iss-1") is False

        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert linear.post_comment.await_count == 2
        assert await db.runs.has_active(conn, "iss-1") is True
    finally:
        await conn.close()
