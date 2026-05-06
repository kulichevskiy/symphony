"""Tests for symphony.state — pure data-structure behaviour."""

from __future__ import annotations

import pytest

from symphony.state import (
    BASE_BACKOFF_S,
    MAX_BACKOFF_S,
    OrchestratorState,
    compute_backoff,
)


# ---- compute_backoff ----


def test_backoff_first_attempt_is_base():
    assert compute_backoff(1) == BASE_BACKOFF_S


def test_backoff_doubles_each_attempt():
    assert compute_backoff(2) == BASE_BACKOFF_S * 2
    assert compute_backoff(3) == BASE_BACKOFF_S * 4
    assert compute_backoff(4) == BASE_BACKOFF_S * 8
    assert compute_backoff(5) == BASE_BACKOFF_S * 16


def test_backoff_caps_at_max():
    # 10 × 2^9 = 5120; clamped to MAX_BACKOFF_S
    assert compute_backoff(10) == MAX_BACKOFF_S
    assert compute_backoff(20) == MAX_BACKOFF_S


def test_backoff_rejects_zero_and_negative():
    with pytest.raises(ValueError):
        compute_backoff(0)
    with pytest.raises(ValueError):
        compute_backoff(-1)


# ---- OrchestratorState ----


def test_initial_state_is_empty():
    s = OrchestratorState()
    assert s.running == set()
    assert s.retry_queue == {}
    assert s.paused_until is None
    assert not s.is_paused(now=0.0)


def test_schedule_retry_first_attempt():
    s = OrchestratorState()
    entry = s.schedule_retry(42, now=100.0)
    assert entry.attempt == 1
    assert entry.next_retry_at == 100.0 + BASE_BACKOFF_S
    assert s.retry_queue[42] == entry


def test_schedule_retry_increments_attempt():
    s = OrchestratorState()
    s.schedule_retry(42, now=0.0)
    e = s.schedule_retry(42, now=100.0)
    assert e.attempt == 2
    assert e.next_retry_at == 100.0 + BASE_BACKOFF_S * 2


def test_is_in_backoff_window():
    s = OrchestratorState()
    s.schedule_retry(42, now=0.0)
    assert s.is_in_backoff(42, now=5.0)  # before 10s window
    assert not s.is_in_backoff(42, now=10.1)  # after 10s window


def test_is_in_backoff_unscheduled_issue_returns_false():
    s = OrchestratorState()
    assert not s.is_in_backoff(99, now=0.0)


def test_clear_retry_removes_entry():
    s = OrchestratorState()
    s.schedule_retry(42, now=0.0)
    s.clear_retry(42)
    assert 42 not in s.retry_queue
    # Idempotent
    s.clear_retry(42)


def test_pause_sets_paused_until():
    s = OrchestratorState()
    s.pause(now=100.0, duration_s=600.0)
    assert s.paused_until == 700.0
    assert s.is_paused(now=600.0)
    assert not s.is_paused(now=701.0)


def test_pause_does_not_shorten_existing_pause():
    """Two rate-limit hits in quick succession must not let the second's
    shorter window override the first's longer one."""
    s = OrchestratorState()
    s.pause(now=100.0, duration_s=600.0)  # paused until 700
    s.pause(now=200.0, duration_s=60.0)   # would expire at 260, must be ignored
    assert s.paused_until == 700.0


def test_pause_extends_when_later_window_is_longer():
    s = OrchestratorState()
    s.pause(now=100.0, duration_s=60.0)   # paused until 160
    s.pause(now=120.0, duration_s=600.0)  # paused until 720, takes effect
    assert s.paused_until == 720.0
