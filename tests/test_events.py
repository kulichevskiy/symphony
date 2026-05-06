"""Tests for the append-only SQLite event log."""

from __future__ import annotations

import sqlite3

from symphony.events import EventLog


def test_event_log_creates_single_events_table(tmp_path):
    log = EventLog.for_repo(tmp_path)
    event = log.emit(
        "dispatch",
        issue_number=42,
        run_id="run-1",
        payload={"title": "Do it"},
        ts=100,
    )

    assert event.id == 1
    assert (tmp_path / ".symphony" / "events.db").is_file()
    with sqlite3.connect(log.db_path) as con:
        tables = [
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        ]
    assert tables == ["events"]


def test_iter_and_tail_events_filter_by_issue(tmp_path):
    log = EventLog.for_repo(tmp_path)
    log.emit("dispatch", issue_number=1, run_id="a", ts=1)
    log.emit("dispatch", issue_number=2, run_id="b", ts=2)
    log.emit("review-verdict", issue_number=1, run_id="a", payload={"verdict": "pending"}, ts=3)

    assert [e.issue_number for e in log.iter_events(issue_number=1)] == [1, 1]
    assert [e.kind for e in log.tail_events(limit=2)] == ["dispatch", "review-verdict"]


def test_replay_review_restores_next_round_inputs(tmp_path):
    log = EventLog.for_repo(tmp_path)
    for round_no in (1, 2, 3):
        log.emit(
            "agent-exit",
            issue_number=4,
            run_id="r",
            payload={"phase": "review", "round": round_no, "success": True},
            ts=round_no,
        )
    log.emit(
        "review-verdict",
        issue_number=4,
        run_id="r",
        payload={"head_sha": "abc123", "verdict": "changes_requested", "round": 3},
        ts=4,
    )

    replay = log.replay_review(4)
    assert replay.rounds_used == 3
    assert replay.last_reviewed_sha == "abc123"
    assert replay.last_review_verdict == "changes_requested"


def test_replay_review_resets_when_new_run_starts(tmp_path):
    log = EventLog.for_repo(tmp_path)
    log.emit(
        "agent-exit",
        issue_number=4,
        run_id="old",
        payload={"phase": "review", "round": 10, "success": True},
        ts=1,
    )
    log.emit(
        "auto-stuck",
        issue_number=4,
        run_id="old",
        payload={"rounds_used": 10, "outcome": "auto_stuck_rounds"},
        ts=2,
    )
    log.emit(
        "agent-start",
        issue_number=4,
        run_id="new",
        payload={"phase": "round1"},
        ts=3,
    )
    log.emit(
        "agent-exit",
        issue_number=4,
        run_id="new",
        payload={"phase": "review", "round": 1, "success": True},
        ts=4,
    )

    replay = log.replay_review(4)
    assert replay.rounds_used == 1


def test_replay_review_does_not_reset_on_retry_dispatch(tmp_path):
    log = EventLog.for_repo(tmp_path)
    log.emit(
        "agent-exit",
        issue_number=5,
        run_id="old",
        payload={"phase": "review", "round": 3, "success": True},
        ts=1,
    )
    log.emit(
        "run-terminal",
        issue_number=5,
        run_id="old",
        payload={"rounds_used": 3, "outcome": "merge_pending"},
        ts=2,
    )
    log.emit("dispatch", issue_number=5, run_id="retry", ts=3)

    replay = log.replay_review(5)
    assert replay.rounds_used == 3


def test_replay_review_resets_rounds_when_fresh_head_changes(tmp_path):
    log = EventLog.for_repo(tmp_path)
    log.emit(
        "review-verdict",
        issue_number=5,
        run_id="old",
        payload={"head_sha": "old-sha", "verdict": "changes_requested", "round": 10},
        ts=1,
    )
    log.emit(
        "run-terminal",
        issue_number=5,
        run_id="old",
        payload={"rounds_used": 10, "outcome": "merge_pending"},
        ts=2,
    )
    log.emit(
        "review-fresh",
        issue_number=5,
        run_id="retry",
        payload={"head_sha": "new-sha", "round": 0},
        ts=3,
    )
    log.emit(
        "agent-exit",
        issue_number=5,
        run_id="retry",
        payload={"phase": "review", "round": 1, "success": True},
        ts=4,
    )

    replay = log.replay_review(5)
    assert replay.rounds_used == 1
    assert replay.last_reviewed_sha == "new-sha"


def test_status_snapshot_includes_active_and_terminal_runs(tmp_path):
    log = EventLog.for_repo(tmp_path)
    log.emit("dispatch", issue_number=6, run_id="r6", ts=100)
    log.emit(
        "review-verdict",
        issue_number=6,
        run_id="r6",
        payload={"head_sha": "sha6", "verdict": "pending", "round": 2},
        ts=120,
    )
    log.emit("dispatch", issue_number=7, run_id="r7", ts=200)
    log.emit(
        "auto-stuck",
        issue_number=7,
        run_id="r7",
        payload={"outcome": "auto_stuck_idle", "rounds_used": 4},
        ts=260,
    )
    log.emit(
        "retry-scheduled",
        issue_number=7,
        run_id="r7",
        payload={"attempt": 1, "reason": "auto_stuck_idle"},
        ts=261,
    )

    snapshot = log.status_snapshot(now_ts=300)
    assert len(snapshot.in_flight) == 1
    active = snapshot.in_flight[0]
    assert active.issue_number == 6
    assert active.round == 2
    assert active.elapsed_s == 200
    assert active.last_reviewed_sha == "sha6"
    assert active.last_review_verdict == "pending"

    assert len(snapshot.terminal_runs) == 1
    terminal = snapshot.terminal_runs[0]
    assert terminal.issue_number == 7
    assert terminal.outcome == "auto_stuck_idle"
    assert terminal.rounds == 4
    assert terminal.total_elapsed_s == 60


def test_status_snapshot_resets_active_state_on_new_dispatch(tmp_path):
    log = EventLog.for_repo(tmp_path)
    log.emit("dispatch", issue_number=8, run_id="old", ts=100)
    log.emit(
        "review-verdict",
        issue_number=8,
        run_id="old",
        payload={"head_sha": "old-sha", "verdict": "changes_requested", "round": 7},
        ts=120,
    )
    log.emit("dispatch", issue_number=8, run_id="new", ts=200)

    snapshot = log.status_snapshot(now_ts=250)
    assert len(snapshot.in_flight) == 1
    active = snapshot.in_flight[0]
    assert active.run_id == "new"
    assert active.elapsed_s == 50
    assert active.round == 0
    assert active.last_reviewed_sha == ""
    assert active.last_review_verdict == ""


def test_status_snapshot_clears_active_state_on_retry_scheduled(tmp_path):
    log = EventLog.for_repo(tmp_path)
    log.emit("dispatch", issue_number=9, run_id="crash", ts=100)
    log.emit(
        "retry-scheduled",
        issue_number=9,
        run_id="crash",
        payload={"attempt": 1, "reason": "exception"},
        ts=130,
    )

    snapshot = log.status_snapshot(now_ts=200)
    assert snapshot.in_flight == []
    assert snapshot.terminal_runs == []


def test_latest_terminal_event_searches_full_issue_history(tmp_path):
    log = EventLog.for_repo(tmp_path)
    terminal = log.emit(
        "run-terminal",
        issue_number=10,
        run_id="old",
        payload={"outcome": "merge_pending", "head_sha": "old-sha"},
        ts=100,
    )
    for index in range(101):
        log.emit(
            "review-verdict",
            issue_number=10,
            run_id=f"noise-{index}",
            payload={"head_sha": f"sha-{index}", "verdict": "pending"},
            ts=101 + index,
        )
    log.emit(
        "run-terminal",
        issue_number=11,
        run_id="other",
        payload={"outcome": "merge_failed"},
        ts=300,
    )

    assert "run-terminal" not in {
        event.kind for event in log.tail_events(issue_number=10, limit=100)
    }
    latest = log.latest_terminal_event(10)
    assert latest is not None
    assert latest.id == terminal.id
    assert latest.payload["outcome"] == "merge_pending"
    assert log.latest_terminal_outcome(10) == "merge_pending"
