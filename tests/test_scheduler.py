"""Pure dispatch planner: enforces global + per-binding concurrency caps.

The scheduler is called with a list of dispatchable issues (oldest first)
and the count of currently-active runs per binding. It returns the subset
that should actually be dispatched in this tick, respecting both caps.

Pure function — no IO, no DB, no time. Tested as input → output.
"""

from __future__ import annotations

from symphony.pipeline.scheduler import PendingDispatch, plan_dispatch


def test_global_cap_truncates_total_dispatches() -> None:
    pending = [PendingDispatch(issue_id=f"i{n}", binding_key="repoA") for n in range(5)]
    out = plan_dispatch(
        pending=pending,
        active_per_binding={},
        active_total=0,
        global_cap=2,
        binding_caps={"repoA": 99},
    )
    assert [p.issue_id for p in out] == ["i0", "i1"]


def test_per_binding_cap_isolates_repos() -> None:
    pending = [
        PendingDispatch("i1", "repoA"),
        PendingDispatch("i2", "repoA"),
        PendingDispatch("i3", "repoB"),
    ]
    out = plan_dispatch(
        pending=pending,
        active_per_binding={},
        active_total=0,
        global_cap=99,
        binding_caps={"repoA": 1, "repoB": 99},
    )
    assert [p.issue_id for p in out] == ["i1", "i3"]


def test_active_counts_reduce_remaining_capacity() -> None:
    pending = [PendingDispatch(f"i{n}", "repoA") for n in range(3)]
    out = plan_dispatch(
        pending=pending,
        active_per_binding={"repoA": 1},
        active_total=1,
        global_cap=2,
        binding_caps={"repoA": 2},
    )
    assert [p.issue_id for p in out] == ["i0"]


def test_zero_capacity_dispatches_nothing() -> None:
    pending = [PendingDispatch("i1", "repoA")]
    out = plan_dispatch(
        pending=pending,
        active_per_binding={},
        active_total=0,
        global_cap=0,
        binding_caps={"repoA": 99},
    )
    assert out == []


def test_unknown_binding_uses_zero_cap() -> None:
    """If a binding has no entry in `binding_caps`, treat its cap as 0
    rather than unbounded — fail safe, never starve global capacity on a
    typo."""
    pending = [PendingDispatch("i1", "repoUnknown")]
    out = plan_dispatch(
        pending=pending,
        active_per_binding={},
        active_total=0,
        global_cap=99,
        binding_caps={},
    )
    assert out == []


def test_preserves_input_order() -> None:
    pending = [
        PendingDispatch("z", "repoA"),
        PendingDispatch("a", "repoA"),
        PendingDispatch("m", "repoA"),
    ]
    out = plan_dispatch(
        pending=pending,
        active_per_binding={},
        active_total=0,
        global_cap=99,
        binding_caps={"repoA": 99},
    )
    assert [p.issue_id for p in out] == ["z", "a", "m"]
