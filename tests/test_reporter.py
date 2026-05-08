"""Tests for symphony.reporter — terminal renderer for orchestrator events."""

from __future__ import annotations

import io
import json

from symphony.reporter import TerminalReporter, TickSnapshot


def test_dispatch_event_renders_issue_number_and_title():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)

    reporter.event("dispatch", issue_number=42, payload={"title": "Add foo"})

    output = stream.getvalue()
    assert "#42" in output
    assert "Add foo" in output


def test_retry_scheduled_hidden_at_default_verbosity():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)

    reporter.event(
        "retry-scheduled",
        issue_number=42,
        payload={"attempt": 1, "next_retry_at": 1700000000, "reason": "not-approved"},
    )

    assert stream.getvalue() == ""


def test_retry_scheduled_visible_at_v():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream, verbosity=1)

    reporter.event(
        "retry-scheduled",
        issue_number=42,
        payload={"attempt": 1, "next_retry_at": 1700000000, "reason": "not-approved"},
    )

    output = stream.getvalue()
    assert "#42" in output
    assert "retry" in output.lower()


def test_heartbeat_emitted_after_interval_when_idle():
    stream = io.StringIO()
    clock = [1000.0]
    reporter = TerminalReporter(
        stream=stream,
        now_fn=lambda: clock[0],
        heartbeat_interval_s=300.0,
    )

    snap = TickSnapshot(candidates=5, ready=0, running=2, skips=[])

    # Before interval — no heartbeat.
    clock[0] = 1100.0
    reporter.maybe_heartbeat(snap)
    assert stream.getvalue() == ""

    # After interval — heartbeat.
    clock[0] = 1301.0
    reporter.maybe_heartbeat(snap)
    output = stream.getvalue()
    assert "idle" in output.lower()
    assert "5" in output  # candidates
    assert "2" in output  # running


def test_filtered_event_still_resets_heartbeat_timer():
    stream = io.StringIO()
    clock = [1000.0]
    reporter = TerminalReporter(
        stream=stream,
        verbosity=0,  # agent.event needs -vv, so this filters it out
        now_fn=lambda: clock[0],
        heartbeat_interval_s=300.0,
    )

    snap = TickSnapshot(candidates=0, ready=0, running=1, skips=[])

    # A filtered-out event at t+200 still proves liveness.
    clock[0] = 1200.0
    reporter.event("agent.event", payload={"type": "tool_use"})
    assert stream.getvalue() == ""  # filtered

    # At t+450 (250s since the event) — still inside the interval.
    clock[0] = 1450.0
    reporter.maybe_heartbeat(snap)
    assert stream.getvalue() == ""

    # At t+501 — beyond the interval, heartbeat fires.
    clock[0] = 1501.0
    reporter.maybe_heartbeat(snap)
    assert "idle" in stream.getvalue().lower()


def test_heartbeat_suppressed_in_quiet_mode():
    stream = io.StringIO()
    clock = [1000.0]
    reporter = TerminalReporter(
        stream=stream,
        verbosity=-1,
        now_fn=lambda: clock[0],
        heartbeat_interval_s=300.0,
    )

    snap = TickSnapshot(candidates=5, ready=0, running=2, skips=[])
    clock[0] = 1301.0
    reporter.maybe_heartbeat(snap)

    assert stream.getvalue() == ""


def test_heartbeat_in_json_mode_emits_heartbeat_kind():
    stream = io.StringIO()
    clock = [1000.0]
    reporter = TerminalReporter(
        stream=stream,
        json_mode=True,
        now_fn=lambda: clock[0],
        heartbeat_interval_s=300.0,
    )

    snap = TickSnapshot(candidates=5, ready=0, running=2, skips=[])
    clock[0] = 1301.0
    reporter.maybe_heartbeat(snap)

    obj = json.loads(stream.getvalue().strip())
    assert obj["kind"] == "heartbeat"
    assert obj["payload"]["candidates"] == 5
    assert obj["payload"]["running"] == 2


def test_heartbeat_timer_resets_on_event():
    stream = io.StringIO()
    clock = [1000.0]
    reporter = TerminalReporter(
        stream=stream,
        now_fn=lambda: clock[0],
        heartbeat_interval_s=300.0,
    )

    snap = TickSnapshot(candidates=0, ready=0, running=0, skips=[])

    # Event at t+200 resets timer.
    clock[0] = 1200.0
    reporter.event("dispatch", issue_number=1, payload={"title": "x"})
    stream.truncate(0)
    stream.seek(0)

    # At t+450, only 250s since the event — no heartbeat.
    clock[0] = 1450.0
    reporter.maybe_heartbeat(snap)
    assert stream.getvalue() == ""

    # At t+501, 301s since event — heartbeat fires.
    clock[0] = 1501.0
    reporter.maybe_heartbeat(snap)
    assert "idle" in stream.getvalue().lower()


def test_paused_renders_reason():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)
    reporter.event(
        "paused",
        issue_number=42,
        payload={"reason": "rate-limit", "paused_until": 1700003600},
    )
    output = stream.getvalue()
    assert "paused" in output.lower()
    assert "rate-limit" in output


def test_resumed_renders_simple_marker():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)
    reporter.event("resumed", payload={"paused_until": 1700003600})
    assert "resumed" in stream.getvalue().lower()


def test_auto_cycle_mentions_issue_number():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)
    reporter.event("auto-cycle", issue_number=38, payload={})
    output = stream.getvalue()
    assert "#38" in output
    assert "auto-cycle" in output.lower() or "cycle" in output.lower()


def test_retry_fired_renders_attempt():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)
    reporter.event("retry-fired", issue_number=42, payload={"attempt": 2})
    output = stream.getvalue()
    assert "#42" in output
    assert "2" in output  # attempt
    assert "retry" in output.lower()


def test_merge_renders_pr_number():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)
    reporter.event(
        "merge",
        issue_number=42,
        payload={
            "pr_number": 57,
            "url": "https://github.com/x/y/pull/57",
            "rounds_used": 3,
            "outcome": "approved",
        },
    )
    output = stream.getvalue()
    assert "#42" in output
    assert "PR #57" in output
    assert "merge" in output.lower()


def test_pr_open_renders_pr_number_and_branch():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)
    reporter.event(
        "pr-open",
        issue_number=42,
        payload={
            "number": 57,
            "url": "https://github.com/x/y/pull/57",
            "head": "auto/issue-42",
            "base": "main",
            "reused": False,
        },
    )
    output = stream.getvalue()
    assert "#42" in output
    assert "PR #57" in output


def test_auto_stuck_round_cap_renders_reason_and_rounds():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)
    reporter.event(
        "auto-stuck",
        issue_number=42,
        payload={
            "reason": "round-cap",
            "rounds_used": 10,
            "head_sha": "abc1234",
            "outcome": "auto_stuck_rounds",
        },
    )
    output = stream.getvalue()
    assert "#42" in output
    assert "stuck" in output.lower()
    assert "round-cap" in output
    assert "10" in output


def test_auto_stuck_idle_renders_idle_reason():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)
    reporter.event(
        "auto-stuck",
        issue_number=42,
        payload={
            "reason": "idle",
            "rounds_used": 3,
            "head_sha": "abc1234",
            "outcome": "auto_stuck_idle",
        },
    )
    output = stream.getvalue()
    assert "#42" in output
    assert "stuck" in output.lower()
    assert "idle" in output.lower()


def test_auto_canceled_renders_reason():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)
    reporter.event(
        "auto-canceled",
        issue_number=42,
        payload={"reason": "manual", "rounds_used": 0, "outcome": "auto_canceled"},
    )
    output = stream.getvalue()
    assert "#42" in output
    assert "cancel" in output.lower()


def test_run_terminal_approved_renders_pr_and_rounds():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)

    reporter.event(
        "run-terminal",
        issue_number=42,
        payload={
            "outcome": "approved",
            "rounds_used": 3,
            "pr_number": 57,
            "url": "https://github.com/x/y/pull/57",
        },
    )

    output = stream.getvalue()
    assert "#42" in output
    assert "approved" in output.lower()
    assert "PR #57" in output
    assert "3" in output  # rounds


def test_run_terminal_agent_failed_marks_failure():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream)

    reporter.event(
        "run-terminal",
        issue_number=42,
        payload={"outcome": "agent_failed", "rounds_used": 1},
    )

    output = stream.getvalue()
    assert "#42" in output
    assert "agent_failed" in output.lower() or "failed" in output.lower()


def test_quiet_mode_hides_default_tier_events():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream, verbosity=-1)

    reporter.event("dispatch", issue_number=42, payload={"title": "Add foo"})

    assert stream.getvalue() == ""


def test_json_mode_emits_ndjson_line_per_event():
    stream = io.StringIO()
    reporter = TerminalReporter(stream=stream, json_mode=True)

    reporter.event("dispatch", issue_number=42, payload={"title": "Add foo"})

    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["kind"] == "dispatch"
    assert obj["issue"] == 42
    assert obj["payload"] == {"title": "Add foo"}
