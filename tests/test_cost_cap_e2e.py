"""End-to-end cost cap + cost warning across the implement run.

Drives the orchestrator with a fake runner that emits synthetic cost
events. Verifies the once-per-issue warning, the cap-breach park-to-
`needs_approval` flow, and that cost is read from the per-issue total
across runs (not just the current run).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Literal
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.agent.codex_cli import (
    CODEX_APPROVAL_POLICY_CONFIG,
    CODEX_DEFAULT_PERMISSIONS_CONFIG,
)
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearError, LinearIssue
from symphony.orchestrator.poll import Orchestrator


class _CostStreamRunner:
    """Yields one `result` cost event per supplied increment, then exits."""

    def __init__(self, cost_increments: list[float]) -> None:
        self.cost_increments = cost_increments
        self.kill_calls: list[str] = []
        self.events_consumed = 0
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        yield RunnerEvent(kind="started", pid=1234)
        self.events_consumed += 1
        for inc in self.cost_increments:
            yield RunnerEvent(
                kind="stdout",
                line=json.dumps(
                    {
                        "type": "result",
                        "subtype": "success",
                        "total_cost_usd": inc,
                        "usage": {"input_tokens": 1, "output_tokens": 1},
                    }
                ),
            )
            self.events_consumed += 1
        yield RunnerEvent(kind="exit", returncode=0)
        self.events_consumed += 1

    async def kill(self, run_id: str) -> None:
        self.kill_calls.append(run_id)


class _EventRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.kill_calls: list[str] = []
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        self.kill_calls.append(run_id)


def _binding(
    *,
    agent: Literal["claude", "codex"] = "claude",
    codex_model: str = "gpt-5.1-codex",
    cost_cap_usd: float | None = None,
    cost_warning_pct: int | None = None,
) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent=agent,
        codex_model=codex_model,
        branch_prefix="symphony",
        cost_cap_usd=cost_cap_usd,
        cost_warning_pct=cost_warning_pct,
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="t",
        description="d",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=[],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


def _orch(
    cfg: Config, conn: object, runner: _CostStreamRunner, ws_path: Path
) -> tuple[Orchestrator, AsyncMock, MagicMock, MagicMock]:
    linear = AsyncMock()
    linear.issues_in_state = AsyncMock(return_value=[_issue()])
    linear.lookup_issue = AsyncMock(return_value=_issue())
    linear.post_comment = AsyncMock(return_value="cmt-1")
    linear.move_issue = AsyncMock()

    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=ws_path)
    workspace.release = MagicMock()

    gh = MagicMock()
    gh.pr_create = AsyncMock(return_value="https://github.com/org/repo/pull/42")
    gh.pr_comment = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="trunk")

    orch = Orchestrator(
        cfg, linear, conn, runner=runner, gh=gh,
        workspace=workspace, push_fn=AsyncMock(),
    )
    orch._states = {"ENG": _states()}  # noqa: SLF001
    return orch, linear, gh, workspace


@pytest.mark.asyncio
async def test_cap_breach_parks_issue_at_needs_approval(tmp_path: Path) -> None:
    """One synthetic run drives cumulative cost past the warning threshold
    and then past the cap. Expect: warning posted once, runner killed, no
    PR opened, issue moved to needs_approval, stuck-loop-escape comment."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )
        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        # 10 → still below threshold 11.25; 12 → crosses (warning); 16 → cap breach.
        runner = _CostStreamRunner([10.0, 2.0, 4.0])
        orch, linear, gh, _ws = _orch(cfg, conn, runner, ws)

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        moves = [c.args for c in linear.move_issue.await_args_list]
        assert moves[0] == ("iss-1", "state-progress")
        assert ("iss-1", "state-na") in moves

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        # 🚀 start, 💸 cost notice, 🟠 cost-cap pause — exactly one of each.
        assert sum(1 for b in bodies if b.startswith("🚀")) == 1
        assert sum(1 for b in bodies if "Cost notice" in b) == 1
        assert sum(1 for b in bodies if "Cost cap reached" in b) == 1

        gh.pr_create.assert_not_awaited()
        assert runner.kill_calls == [orch._dispatch_run_ids.get("iss-1") or ""] or runner.kill_calls  # noqa: SLF001

        # Cap-breach kept consuming events until the runner's terminal
        # `exit` — otherwise an early aclose() would orphan the runner's
        # pump/watch tasks. The +1 (vs len(increments)) accounts for the
        # post-stdout iteration that runs through to yield `exit`.
        assert runner.events_consumed == len(runner.cost_increments) + 1

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        assert history[0].termination_kind == "cost_cap"
        assert history[0].termination_kind != "unknown"
        assert "cost cap reached" in history[0].termination_detail
        # Cost was persisted on the run before the breach handler ran.
        assert history[0].cost_usd == pytest.approx(16.0)

        # Warning idempotency mark persisted.
        assert await db.cost_marks.warning_posted_at(conn, "iss-1") is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cap_breach_keeps_run_waiting_for_operator_slash(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )
        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        runner = _CostStreamRunner([16.0])
        orch, _linear, _gh, _ws = _orch(cfg, conn, runner, ws)

        await orch._dispatch_with_limits(cfg.repos[0], _issue())  # noqa: SLF001

        run_id = orch._dispatch_run_ids.get("iss-1")  # noqa: SLF001
        assert run_id is not None
        assert run_id in orch._operator_wait_run_ids  # noqa: SLF001
        assert run_id not in orch._active_run_ids  # noqa: SLF001
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.run_id == run_id
        assert wait.kind == db.operator_waits.KIND_COST_CAP
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cap_breach_keeps_counting_late_cost_events(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )
        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        runner = _CostStreamRunner([16.0, 2.0])
        orch, _linear, _gh, _ws = _orch(cfg, conn, runner, ws)

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].cost_usd == pytest.approx(18.0)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cap_uses_per_issue_total_across_runs(tmp_path: Path) -> None:
    """A small new-run increment combined with a prior run total breaches
    the cap — the orchestrator must aggregate cost per-issue, not per-run."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )

        # Pre-existing cost on the issue.
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="prior",
            issue_id="iss-1",
            stage="implement",
            status="completed",
            pid=None,
            started_at="2026-05-09T00:00:00+00:00",
            cost_usd=10.0,
        )

        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        # 6 alone is well under the cap, but 10+6 = 16 → breach.
        runner = _CostStreamRunner([6.0])
        orch, linear, gh, _ws = _orch(cfg, conn, runner, ws)

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        assert runner.captured_spec is not None
        assert "--max-budget-usd" in runner.captured_spec.command
        budget_idx = runner.captured_spec.command.index("--max-budget-usd") + 1
        assert runner.captured_spec.command[budget_idx] == "5.0000"

        moves = [c.args for c in linear.move_issue.await_args_list]
        assert ("iss-1", "state-na") in moves
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Cost cap reached" in b for b in bodies)
        gh.pr_create.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cap_already_reached_skips_runner_and_parks(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )

        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="prior",
            issue_id="iss-1",
            stage="implement",
            status="completed",
            pid=None,
            started_at="2026-05-09T00:00:00+00:00",
            cost_usd=15.0,
        )

        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        runner = _CostStreamRunner([1.0])
        orch, linear, gh, _ws = _orch(cfg, conn, runner, ws)

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        assert runner.captured_spec is None
        moves = [c.args for c in linear.move_issue.await_args_list]
        assert ("iss-1", "state-na") in moves
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Cost cap reached" in b for b in bodies)
        gh.pr_create.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cap_breach_marks_run_failed_when_state_lookup_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )

        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        runner = _CostStreamRunner([16.0])
        orch, linear, gh, _ws = _orch(cfg, conn, runner, ws)
        orch._states_for_binding = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=[_states(), LinearError("linear states unavailable")]
        )

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        assert history[0].ended_at is not None
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Cost cap reached" in b for b in bodies)
        gh.pr_create.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cap_breach_falls_back_to_blocked_when_needs_approval_move_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )

        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        runner = _CostStreamRunner([16.0])
        orch, linear, gh, _ws = _orch(cfg, conn, runner, ws)
        linear.move_issue.side_effect = [
            None,
            LinearError("needs approval temporarily unavailable"),
            None,
        ]

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        moves = [c.args for c in linear.move_issue.await_args_list]
        assert moves[0] == ("iss-1", "state-progress")
        assert ("iss-1", "state-na") in moves
        assert moves[-1] == ("iss-1", "state-bl")
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        gh.pr_create.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cap_breach_does_not_reset_to_ready_when_parking_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )

        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        runner = _CostStreamRunner([16.0])
        orch, linear, gh, _ws = _orch(cfg, conn, runner, ws)
        linear.move_issue.side_effect = [
            None,
            LinearError("needs approval temporarily unavailable"),
            LinearError("blocked temporarily unavailable"),
        ]

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        moves = [c.args for c in linear.move_issue.await_args_list]
        assert moves[0] == ("iss-1", "state-progress")
        assert ("iss-1", "state-na") in moves
        assert ("iss-1", "state-bl") in moves
        assert ("iss-1", "state-todo") not in moves
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        gh.pr_create.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_codex_token_usage_estimates_cost_for_cap(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding(agent="codex")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=0.02,
            cost_warning_pct=75,
        )

        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        runner = _EventRunner(
            [
                RunnerEvent(kind="started", pid=1234),
                RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {
                            "type": "turn.completed",
                            "usage": {
                                "input_tokens": 1_000,
                                "cached_input_tokens": 200,
                                "output_tokens": 2_000,
                            },
                        }
                    ),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )
        orch, linear, gh, _ws = _orch(cfg, conn, runner, ws)

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        assert runner.captured_spec is not None
        command = runner.captured_spec.command
        assert command[:3] == [
            "codex",
            "exec",
            "--json",
        ]
        assert "--sandbox" not in command
        assert "workspace-write" not in command
        configs = [command[i + 1] for i, arg in enumerate(command) if arg == "--config"]
        assert configs == [
            CODEX_DEFAULT_PERMISSIONS_CONFIG,
            CODEX_APPROVAL_POLICY_CONFIG,
        ]
        assert command[command.index("--model") + 1] == "gpt-5.1-codex"
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        assert history[0].cost_usd == pytest.approx(0.021025)
        assert runner.kill_calls
        moves = [c.args for c in linear.move_issue.await_args_list]
        assert ("iss-1", "state-na") in moves
        gh.pr_create.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_warning_does_not_repost_after_persistent_mark(tmp_path: Path) -> None:
    """If the warning was already posted on a prior run, a follow-up run
    that pushes cost further past the threshold (but still below cap)
    must not re-post. The flag in `issue_cost_marks` is the single source
    of truth for once-per-issue."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )

        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="prior",
            issue_id="iss-1",
            stage="implement",
            status="completed",
            pid=None,
            started_at="2026-05-09T00:00:00+00:00",
            cost_usd=12.0,
        )
        await db.cost_marks.mark_warning_posted(
            conn, "iss-1", "2026-05-09T00:00:00+00:00"
        )

        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        # Adds 1.0 → cumulative 13.0, still over threshold but under cap.
        runner = _CostStreamRunner([1.0])
        orch, linear, _gh, _ws = _orch(cfg, conn, runner, ws)

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert not any("Cost notice" in b for b in bodies), bodies
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_warning_retries_after_transient_comment_failure(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )

        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        # 12 crosses the 11.25 threshold, then 13 remains over it without
        # breaching the cap. The failed first warning should retry on 13.
        runner = _CostStreamRunner([12.0, 1.0])
        orch, linear, _gh, _ws = _orch(cfg, conn, runner, ws)
        linear.post_comment.side_effect = [
            "start-cmt",
            LinearError("linear temporarily down"),
            "warning-cmt",
            "done-cmt",
        ]

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert sum(1 for b in bodies if "Cost notice" in b) == 2
        assert await db.cost_marks.warning_posted_at(conn, "iss-1") is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_per_binding_cap_overrides_global(tmp_path: Path) -> None:
    """Binding-level `cost_cap_usd` wins over the global default. With a
    higher per-binding cap, what would breach under the global no longer
    does."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding(cost_cap_usd=30.0)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=15.0,
            cost_warning_pct=75,
        )
        ws = tmp_path / "ws" / "org_srepo" / "eng-1"
        ws.mkdir(parents=True)
        # 16 would breach the global 15, but stays under the per-binding 30.
        runner = _CostStreamRunner([16.0])
        orch, linear, gh, _ws = _orch(cfg, conn, runner, ws)

        await orch._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001

        # PR was opened — the run completed normally under the higher cap.
        gh.pr_create.assert_awaited_once()
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert not any("Cost cap reached" in body for body in bodies)
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [r.stage for r in history] == ["implement", "review"]
        assert history[0].status == "completed"
        assert history[1].status == "running"
    finally:
        await conn.close()


def test_default_cost_cap_is_100_usd() -> None:
    """The acceptance criteria pin the global default to $100."""
    cfg = Config()
    assert cfg.cost_cap_per_issue_usd == 100.0
    assert cfg.cost_warning_pct == 75


def test_per_binding_cost_overrides_loaded_from_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    raw = """
cost_cap_per_issue_usd: 15
cost_warning_pct: 75
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    cost_cap_usd: 25
    cost_warning_pct: 50
    linear_states:
      ready: Todo
      in_progress: In Progress
      code_review: Needs Approval
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(raw)
    cfg = Config.load(p)
    assert cfg.repos[0].cost_cap_usd == 25.0
    assert cfg.repos[0].cost_warning_pct == 50
