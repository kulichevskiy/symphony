from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import Linear
from symphony.orchestrator.poll import Orchestrator
from symphony.orchestrator.reconciler import Reconciler


def _binding(team_key: str = "ENG") -> RepoBinding:
    return RepoBinding(
        linear_team_key=team_key,
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def test_linear_tracker_implements_issue_tracker_protocol() -> None:
    from symphony.tracker import IssueTracker

    linear = Linear("test-key")
    try:
        assert isinstance(linear, IssueTracker)
    finally:
        # Avoid leaking the underlying httpx client from this structural check.
        import asyncio

        asyncio.run(linear.aclose())


def test_orchestrator_and_reconciler_do_not_store_linear_client_attrs() -> None:
    assert "self.linear" not in inspect.getsource(Orchestrator)
    assert "self._linear" not in inspect.getsource(Reconciler)


@pytest.mark.asyncio
async def test_warmup_caches_states_by_provider_and_team(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from symphony import db
    from symphony.tracker import DEFAULT_PROVIDER

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        linear = AsyncMock()
        linear.viewer_team_keys = AsyncMock(return_value=["ENG"])
        linear.team_states = AsyncMock(return_value={"Todo": "state-todo"})
        orch = Orchestrator(
            Config(repos=[_binding()]),
            linear,
            conn,
            gh=MagicMock(),
            workspace=MagicMock(),
        )

        await orch.warmup()

        assert orch._states == {(DEFAULT_PROVIDER, "ENG"): {"Todo": "state-todo"}}  # noqa: SLF001
    finally:
        await conn.close()


def test_poll_no_longer_computes_author_is_me_from_comment_body() -> None:
    source = inspect.getsource(Orchestrator)
    assert "is_symphony_comment" not in source
