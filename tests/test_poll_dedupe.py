"""The poll loop dedupes via SQLite, not the old in-memory `_dispatched`
dict. Re-scanning an issue that already has an active run must not
re-dispatch."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator


def test_orchestrator_no_longer_uses_in_memory_dispatched_dict() -> None:
    src = inspect.getsource(Orchestrator)
    assert "_dispatched" not in src, (
        "the in-memory dedupe ledger must be replaced by a SQLite query"
    )


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        pass


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo"),
    )


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


def _make_orch(cfg: Config, linear: AsyncMock, conn: object) -> Orchestrator:
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=Path("/dev/null"))
    workspace.release = MagicMock()
    gh = MagicMock()
    gh.pr_create = AsyncMock(return_value="https://example.invalid/pr/1")
    runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
    push_fn = AsyncMock()
    return Orchestrator(
        cfg,
        linear,
        conn,  # type: ignore[arg-type]
        runner=runner,
        gh=gh,
        workspace=workspace,
        push_fn=push_fn,
    )


@pytest.mark.asyncio
async def test_scan_skips_issues_with_active_run(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = _make_orch(cfg, linear, conn)

        # First tick dispatches and records an active run.
        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        first_call_count = linear.post_comment.await_count
        assert first_call_count >= 1

        # Second tick must dedupe via the SQLite `runs` table — not a dict —
        # so no further comments are posted.
        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert linear.post_comment.await_count == first_call_count
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_run_row_is_persisted_before_post_comment(tmp_path: Path) -> None:
    """Dedupe correctness: the `runs` row must exist before the first
    Linear write so a crash after `post_comment` can't leave the issue
    dispatched-but-unrecorded. Asserted by inspecting the DB from inside
    the mocked `post_comment`."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])

        observed: dict[str, bool] = {}

        async def _post(issue_id: str, body: str) -> str:
            observed.setdefault(
                "had_active_when_first_post",
                await db.runs.has_active(conn, issue_id),
            )
            return "cmt-1"

        linear.post_comment = AsyncMock(side_effect=_post)

        orch = _make_orch(cfg, linear, conn)
        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001

        assert observed.get("had_active_when_first_post") is True
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failed_announce_clears_dedupe_so_next_tick_retries(
    tmp_path: Path,
) -> None:
    """If the ▶ `post_comment` raises, the run row must be marked
    non-live so the next poll can retry. Otherwise a transient Linear
    error would jam the issue forever behind its own dedupe row."""
    from symphony.linear.client import LinearError

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        # First scan's ▶ comment raises; second scan succeeds.
        linear.post_comment = AsyncMock(
            side_effect=[LinearError("boom"), "cmt-1", "cmt-2"]
        )

        orch = _make_orch(cfg, linear, conn)

        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        # The failed announce row exists but is no longer live, so dedupe
        # lets the next tick retry.
        assert await db.runs.has_active(conn, "iss-1") is False

        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        # Second tick re-announces and proceeds (>= 2 total post_comment calls).
        assert linear.post_comment.await_count >= 2
        # The retry produced a successful run, so dedupe now blocks future
        # scans from re-dispatching the same issue.
        assert await db.runs.has_running_or_completed(conn, "iss-1") is True
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failed_state_move_clears_dedupe_so_next_tick_retries(
    tmp_path: Path,
) -> None:
    """If the Linear move to In Progress fails, do not continue to a completed
    run while the issue is still in the ready state."""
    from symphony.linear.client import LinearError

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(side_effect=LinearError("boom"))

        orch = _make_orch(cfg, linear, conn)
        orch._states = {"ENG": {"In Progress": "state-progress"}}  # noqa: SLF001

        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert await db.runs.has_running_or_completed(conn, "iss-1") is False

        await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        assert linear.post_comment.await_count == 2

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.status for run in history] == ["failed", "failed"]
    finally:
        await conn.close()
