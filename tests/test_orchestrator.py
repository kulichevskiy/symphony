"""Tests for symphony.orchestrator — pure helpers + driver."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from symphony.events import EventLog
from symphony.github import Issue, IssueComment, TrackedIssue
from symphony.orchestrator import (
    DispatchSkip,
    build_dep_graph,
    detect_cycles,
    is_blocker_satisfied,
    is_rate_limited,
    run_forever,
    run_tick,
    select_ready,
)
from symphony.reviewer import LoopOutcome, LoopOutcomeKind
from symphony.runonce import RunOnceResult
from symphony.state import OrchestratorState
from symphony.types import AgentResult


def _issue(number: int, *, created_at: str = "2026-01-01T00:00:00Z", labels=("auto",)) -> Issue:
    return Issue(
        number=number,
        title=f"#{number}",
        body="",
        labels=list(labels),
        comments=[],
        created_at=created_at,
    )


def _tracked(number: int, *, state="CLOSED", state_reason="COMPLETED") -> TrackedIssue:
    return TrackedIssue(
        number=number, title=f"#{number}", state=state, state_reason=state_reason, pr_url=None
    )


def _approved_result() -> RunOnceResult:
    return RunOnceResult(
        issue_number=0,
        pr=None,
        skipped=False,
        skip_reason=None,
        worktree=Path("/tmp"),
        loop_outcome=LoopOutcome(
            kind=LoopOutcomeKind.APPROVED,
            rounds_used=0,
            last_session_id="s",
            head_sha="h",
        ),
    )


def _make_cfg(tmp_path: Path):
    return SimpleNamespace(
        repo=SimpleNamespace(path=tmp_path / "repo", default_branch="main"),
        github=SimpleNamespace(label="auto"),
        orchestrator=SimpleNamespace(
            poll_interval_s=60,
            max_concurrent=3,
            review_round_cap=10,
            codex_renudge_after_min=10,
            codex_giveup_after_min=30,
        ),
        agent=SimpleNamespace(model="claude-opus-4-7", max_turns=50),
        paths=SimpleNamespace(
            worktree_root=tmp_path / "wts", prompts_dir=Path(__file__).parent.parent / "prompts"
        ),
    )


# ---- is_rate_limited ----


def test_is_rate_limited_detects_429():
    assert is_rate_limited("oops, status 429 from API")


def test_is_rate_limited_detects_usage_limit():
    assert is_rate_limited("you've hit your usage limit")


def test_is_rate_limited_case_insensitive():
    assert is_rate_limited("RATE_LIMIT exceeded")


def test_is_rate_limited_empty_string():
    assert not is_rate_limited("")


def test_is_rate_limited_unrelated_error():
    assert not is_rate_limited("network unreachable")


def test_is_rate_limited_inspects_agent_result_events():
    result = AgentResult(
        session_id="s",
        exit_code=1,
        success=False,
        is_error=True,
        duration_ms=1,
        num_turns=1,
        total_cost_usd=0.0,
        final_text=None,
        raw_events=[{"type": "result", "subtype": "error_usage_limit", "message": "you hit a usage limit"}],
        stderr="",
    )
    assert is_rate_limited(result)


# ---- is_blocker_satisfied ----


def test_blocker_satisfied_only_when_closed_completed():
    assert is_blocker_satisfied(_tracked(1, state="CLOSED", state_reason="COMPLETED"))
    assert not is_blocker_satisfied(_tracked(1, state="CLOSED", state_reason="NOT_PLANNED"))
    assert not is_blocker_satisfied(_tracked(1, state="CLOSED", state_reason="DUPLICATE"))
    assert not is_blocker_satisfied(_tracked(1, state="OPEN", state_reason=None))


def test_blocker_satisfied_null_state_reason_is_completed_compat():
    """Some closed issues have stateReason=null; treat as completed for compat."""
    assert is_blocker_satisfied(_tracked(1, state="CLOSED", state_reason=None))


# ---- detect_cycles ----


def test_no_cycles_in_dag():
    graph = {
        1: [],
        2: [_tracked(1)],
        3: [_tracked(1), _tracked(2)],
    }
    assert detect_cycles(graph) == []


def test_self_loop_detected():
    graph = {1: [_tracked(1)]}
    cycles = detect_cycles(graph)
    assert cycles == [{1}]


def test_two_node_cycle():
    graph = {1: [_tracked(2)], 2: [_tracked(1)]}
    cycles = detect_cycles(graph)
    assert cycles == [{1, 2}]


def test_three_node_cycle():
    graph = {1: [_tracked(2)], 2: [_tracked(3)], 3: [_tracked(1)]}
    cycles = detect_cycles(graph)
    assert cycles == [{1, 2, 3}]


def test_cycle_does_not_include_external_blocker():
    """A blocker not in the candidate set can't be part of an auto-issue cycle."""
    graph = {
        1: [_tracked(99)],  # 99 is a manual issue, not in graph
        2: [_tracked(1)],
    }
    assert detect_cycles(graph) == []


# ---- select_ready ----


def _ready(
    candidates,
    graph,
    *,
    state=None,
    cycles_flat=None,
    open_prs=None,
    local_branches=None,
    now=0.0,
):
    state = state or OrchestratorState()
    cycles_flat = cycles_flat or set()
    open_prs = open_prs or set()
    local_branches = local_branches or set()
    return select_ready(
        candidates,
        graph,
        cycles_flat,
        state=state,
        has_open_pr=lambda n: n in open_prs,
        has_local_branch=lambda n: n in local_branches,
        now=now,
    )


def test_select_ready_dispatches_unblocked_issues_fifo():
    a = _issue(1, created_at="2026-01-01T00:00:00Z")
    b = _issue(2, created_at="2026-02-01T00:00:00Z")
    c = _issue(3, created_at="2026-03-01T00:00:00Z")
    ready, skips = _ready([c, a, b], {1: [], 2: [], 3: []})
    assert [i.number for i in ready] == [1, 2, 3]
    assert skips == []


def test_select_ready_skips_running_issues():
    state = OrchestratorState(running={1})
    a = _issue(1)
    b = _issue(2)
    ready, skips = _ready([a, b], {1: [], 2: []}, state=state)
    assert [i.number for i in ready] == [2]
    assert skips == [DispatchSkip(1, "already-running")]


def test_select_ready_skips_blocked_issues():
    a = _issue(1)
    b = _issue(2)
    graph = {
        1: [],
        2: [_tracked(99, state="OPEN", state_reason=None)],
    }
    ready, skips = _ready([a, b], graph)
    assert [i.number for i in ready] == [1]
    assert skips[0].issue_number == 2 and skips[0].reason.startswith("blocked-by:")


def test_select_ready_skips_not_planned_blocker():
    """NOT_PLANNED counts as unsatisfied (only COMPLETED satisfies)."""
    a = _issue(1)
    graph = {1: [_tracked(99, state="CLOSED", state_reason="NOT_PLANNED")]}
    ready, skips = _ready([a], graph)
    assert ready == []
    assert skips[0].reason.startswith("blocked-by:99")


def test_select_ready_skips_open_pr():
    a = _issue(1)
    ready, skips = _ready([a], {1: []}, open_prs={1})
    assert ready == []
    assert skips[0].reason == "open-pr-exists"


def test_select_ready_skips_local_branch():
    a = _issue(1)
    ready, skips = _ready([a], {1: []}, local_branches={1})
    assert ready == []
    assert skips[0].reason == "local-branch-exists"


def test_select_ready_skips_cycle_members():
    a = _issue(1)
    b = _issue(2)
    ready, skips = _ready([a, b], {1: [_tracked(2)], 2: [_tracked(1)]}, cycles_flat={1, 2})
    assert ready == []
    assert {s.reason for s in skips} == {"auto-cycle"}


def test_select_ready_skips_in_backoff():
    state = OrchestratorState()
    state.schedule_retry(1, now=0.0)  # retry at 10s
    a = _issue(1)
    ready, skips = _ready([a], {1: []}, state=state, now=5.0)
    assert ready == []
    assert skips[0].reason == "retry-backoff"


def test_select_ready_includes_after_backoff_expires():
    state = OrchestratorState()
    state.schedule_retry(1, now=0.0)
    a = _issue(1)
    ready, _ = _ready([a], {1: []}, state=state, now=15.0)
    assert [i.number for i in ready] == [1]


# ---- run_tick driver ----


@pytest.mark.asyncio
async def test_run_tick_dispatches_up_to_concurrency_cap(tmp_path, monkeypatch):
    cfg = _make_cfg(tmp_path)
    state = OrchestratorState()
    issues = [_issue(i, created_at=f"2026-0{i}-01T00:00:00Z") for i in (1, 2, 3, 4, 5)]

    dispatched: list[int] = []

    async def fake_run_once(*, issue_number, config_path):
        dispatched.append(issue_number)
        await asyncio.sleep(0)  # let other tasks see state.running
        return _approved_result()

    stats = await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: issues,
        fetch_tracked=lambda n: [],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 0.0,
        run_once_fn=fake_run_once,
    )
    assert stats.candidates == 5
    assert stats.dispatched == 3
    # let the spawned tasks run to completion so test cleanup is clean
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert sorted(dispatched) == [1, 2, 3]


@pytest.mark.asyncio
async def test_run_tick_skips_when_paused(tmp_path):
    cfg = _make_cfg(tmp_path)
    state = OrchestratorState(paused_until=1000.0)
    called = []

    async def should_not_be_called(**kw):
        called.append(kw)
        return _approved_result()

    stats = await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: [_issue(1)],
        fetch_tracked=lambda n: [],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 100.0,  # < 1000, paused
        run_once_fn=should_not_be_called,
    )
    assert stats.candidates == 0
    assert stats.dispatched == 0
    assert called == []


@pytest.mark.asyncio
async def test_run_tick_labels_cycle_members(tmp_path):
    cfg = _make_cfg(tmp_path)
    state = OrchestratorState()
    issues = [_issue(1), _issue(2)]
    labels: list[tuple[int, str]] = []

    stats = await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: issues,
        fetch_tracked=lambda n: [_tracked(2 if n == 1 else 1, state="OPEN", state_reason=None)],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: labels.append((n, lbl)),
        now_fn=lambda: 0.0,
        run_once_fn=lambda **kw: _approved_result(),
    )
    assert stats.dispatched == 0
    assert sorted(labels) == [(1, "auto-cycle"), (2, "auto-cycle")]


@pytest.mark.asyncio
async def test_run_tick_clears_retry_on_approval(tmp_path):
    cfg = _make_cfg(tmp_path)
    state = OrchestratorState()
    state.schedule_retry(1, now=0.0)  # pretend a previous failure

    async def fake_run_once(**kw):
        return _approved_result()

    await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: [_issue(1)],
        fetch_tracked=lambda n: [],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 100.0,  # backoff expired (10s)
        run_once_fn=fake_run_once,
    )
    # Drain the dispatch task
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert 1 not in state.retry_queue


@pytest.mark.asyncio
async def test_run_tick_schedules_retry_on_failure(tmp_path):
    cfg = _make_cfg(tmp_path)
    state = OrchestratorState()

    async def fake_run_once(**kw):
        return RunOnceResult(
            issue_number=1,
            pr=None,
            skipped=True,
            skip_reason="empty-diff",
            worktree=tmp_path,
        )

    await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: [_issue(1)],
        fetch_tracked=lambda n: [],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 0.0,
        run_once_fn=fake_run_once,
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert 1 in state.retry_queue
    assert state.retry_queue[1].attempt == 1


@pytest.mark.asyncio
async def test_run_tick_treats_merge_failure_as_terminal(tmp_path):
    cfg = _make_cfg(tmp_path)
    state = OrchestratorState()
    state.schedule_retry(1, now=0.0)

    async def fake_run_once(**kw):
        return RunOnceResult(
            issue_number=1,
            pr=None,
            skipped=False,
            skip_reason=None,
            worktree=tmp_path,
            loop_outcome=LoopOutcome(
                kind=LoopOutcomeKind.MERGE_FAILED,
                rounds_used=0,
                last_session_id="s",
                head_sha="h",
            ),
        )

    await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: [_issue(1)],
        fetch_tracked=lambda n: [],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 100.0,
        run_once_fn=fake_run_once,
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert 1 not in state.retry_queue


@pytest.mark.asyncio
async def test_run_tick_emits_dispatch_and_retry_events(tmp_path):
    cfg = _make_cfg(tmp_path)
    state = OrchestratorState()
    event_log = EventLog.for_repo(tmp_path)

    async def fake_run_once(**kw):
        return RunOnceResult(
            issue_number=1,
            pr=None,
            skipped=True,
            skip_reason="empty-diff",
            worktree=tmp_path,
        )

    await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: [_issue(1)],
        fetch_tracked=lambda n: [],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 0.0,
        run_once_fn=fake_run_once,
        event_log=event_log,
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    kinds = [e.kind for e in event_log.iter_events(issue_number=1)]
    assert kinds == ["dispatch", "retry-scheduled"]
    assert event_log.iter_events(issue_number=1)[1].payload["reason"] == "empty-diff"


@pytest.mark.asyncio
async def test_run_tick_pauses_dispatch_on_agent_rate_limit(tmp_path):
    """A 429/usage-limit failure surfaced via ``RunOnceResult.agent_result``
    must trip ``state.pause(...)`` so the next tick is suspended."""
    cfg = _make_cfg(tmp_path)
    state = OrchestratorState()
    rate_limited_agent = AgentResult(
        session_id="s",
        exit_code=1,
        success=False,
        is_error=True,
        duration_ms=1,
        num_turns=1,
        total_cost_usd=0.0,
        final_text=None,
        raw_events=[{"type": "result", "subtype": "error_usage_limit", "message": "usage limit"}],
        stderr="",
    )

    async def fake_run_once(**kw):
        return RunOnceResult(
            issue_number=1,
            pr=None,
            skipped=True,
            skip_reason="agent-failed",
            worktree=tmp_path,
            agent_result=rate_limited_agent,
        )

    await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: [_issue(1)],
        fetch_tracked=lambda n: [],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 0.0,
        run_once_fn=fake_run_once,
        rate_limit_pause_s=600.0,
    )
    # Drain the dispatch task
    for _ in range(4):
        await asyncio.sleep(0)
    assert state.paused_until == 600.0
    assert 1 in state.retry_queue


@pytest.mark.asyncio
async def test_run_tick_emits_paused_and_resumed_events(tmp_path):
    cfg = _make_cfg(tmp_path)
    state = OrchestratorState()
    event_log = EventLog.for_repo(tmp_path)
    rate_limited_agent = AgentResult(
        session_id="s",
        exit_code=1,
        success=False,
        is_error=True,
        duration_ms=1,
        num_turns=1,
        total_cost_usd=0.0,
        final_text="429",
        raw_events=[],
        stderr="",
    )

    async def fake_run_once(**kw):
        return RunOnceResult(
            issue_number=1,
            pr=None,
            skipped=True,
            skip_reason="agent-failed",
            worktree=tmp_path,
            agent_result=rate_limited_agent,
        )

    await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: [_issue(1)],
        fetch_tracked=lambda n: [],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 0.0,
        run_once_fn=fake_run_once,
        rate_limit_pause_s=600.0,
        event_log=event_log,
    )
    for _ in range(4):
        await asyncio.sleep(0)

    await run_tick(
        cfg=cfg,
        state=state,
        config_path=tmp_path / "symphony.toml",
        list_issues=lambda: [],
        fetch_tracked=lambda n: [],
        has_open_pr=lambda n: False,
        has_local_branch=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 601.0,
        run_once_fn=fake_run_once,
        event_log=event_log,
    )

    kinds = [e.kind for e in event_log.iter_events()]
    assert "paused" in kinds
    assert "resumed" in kinds


@pytest.mark.asyncio
async def test_run_forever_exits_when_shutdown_event_set(tmp_path):
    cfg = _make_cfg(tmp_path)
    cfg.orchestrator.poll_interval_s = 0.01  # speed the test
    state = OrchestratorState()
    shutdown = asyncio.Event()
    ticks = 0

    def list_issues():
        nonlocal ticks
        ticks += 1
        if ticks >= 3:
            shutdown.set()
        return []

    await run_forever(
        cfg=cfg,
        config_path=tmp_path / "symphony.toml",
        state=state,
        shutdown_event=shutdown,
        list_issues_fn=list_issues,
        fetch_tracked_fn=lambda n: [],
        has_open_pr_fn=lambda n: False,
        has_local_branch_fn=lambda n: False,
        label_fn=lambda n, lbl: None,
        now_fn=lambda: 0.0,
        run_once_fn=lambda **kw: _approved_result(),
    )
    assert ticks >= 3


@pytest.mark.asyncio
async def test_run_forever_drains_in_flight_dispatches_on_shutdown(tmp_path):
    """SIGINT during an active dispatch must let the in-flight ``run_once``
    finish, not get cancelled by the event-loop tear-down."""
    cfg = _make_cfg(tmp_path)
    cfg.orchestrator.poll_interval_s = 0.01
    state = OrchestratorState()
    shutdown = asyncio.Event()
    completed: list[int] = []
    started = asyncio.Event()

    async def slow_run_once(*, issue_number, config_path):
        started.set()
        # Block until shutdown fires, then "finish work" after a brief delay
        # so the run_forever loop has already exited its sleep_fn wait when
        # we return. If the task were cancelled by loop tear-down, this
        # append would never execute.
        await shutdown.wait()
        await asyncio.sleep(0.02)
        completed.append(issue_number)
        return _approved_result()

    async def trigger_shutdown_after_first_dispatch():
        await started.wait()
        shutdown.set()

    list_calls = 0

    def list_issues():
        nonlocal list_calls
        list_calls += 1
        # Only return a candidate on the first tick so we don't spawn more
        # tasks after shutdown is requested.
        return [_issue(1)] if list_calls == 1 else []

    await asyncio.gather(
        run_forever(
            cfg=cfg,
            config_path=tmp_path / "symphony.toml",
            state=state,
            shutdown_event=shutdown,
            list_issues_fn=list_issues,
            fetch_tracked_fn=lambda n: [],
            has_open_pr_fn=lambda n: False,
            has_local_branch_fn=lambda n: False,
            label_fn=lambda n, lbl: None,
            now_fn=lambda: 0.0,
            run_once_fn=slow_run_once,
        ),
        trigger_shutdown_after_first_dispatch(),
    )

    assert completed == [1], "shutdown cancelled an in-flight dispatch"
    assert state.dispatch_tasks == set()
