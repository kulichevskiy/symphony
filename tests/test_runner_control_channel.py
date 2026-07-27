"""Control-channel mode: the runner holds a conversation with the agent.

Two seams, deliberately different in what they trust:

* against a **stub child process** (`tests/fixtures/control_channel_agent.py`)
  we stop trusting the binary and check the wire — the prompt really travels on
  stdin, the control frames really round-trip, the process really exits;
* against the **deterministic runner** the harness uses we check the contract —
  the fake must answer, and refuse, the way LocalRunner does, or every
  orchestrator test written on top of it in SYM-236 would be testing a fiction.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path

import pytest

from symphony.agent.control_channel import ControlRequest, Conversation
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.agent.runners.local import LocalRunner
from tests.harness.fakes import FakeRunner

STUB = Path(__file__).parent / "fixtures" / "control_channel_agent.py"
LINGER_SECS = 30


class _Handler:
    """Records what it was asked and answers with a canned payload."""

    def __init__(self, payload: Mapping[str, object] | None, *, boom: bool = False) -> None:
        self.payload = payload
        self.boom = boom
        self.seen: list[ControlRequest] = []

    async def __call__(self, request: ControlRequest) -> Mapping[str, object] | None:
        self.seen.append(request)
        if self.boom:
            raise RuntimeError("handler exploded")
        return self.payload


def _spec(
    tmp_path: Path,
    run_id: str,
    *stub_args: str,
    conversation: Conversation | None = None,
) -> RunnerSpec:
    return RunnerSpec(
        run_id=run_id,
        workspace_path=tmp_path,
        command=[sys.executable, str(STUB), *stub_args],
        stall_secs=20,
        conversation=conversation,
    )


def _talking(prompt: str = "hi", handler: _Handler | None = None) -> Conversation:
    return Conversation(prompt=prompt, handler=handler)


async def _collect(runner: LocalRunner, spec: RunnerSpec, *, within: float) -> list[RunnerEvent]:
    async with asyncio.timeout(within):
        return [ev async for ev in runner.run(spec)]


def _stdout(events: list[RunnerEvent]) -> list[str]:
    return [e.line for e in events if e.kind == "stdout" and e.line]


def _kinds(events: list[RunnerEvent]) -> list[str]:
    return [e.kind for e in events if e.kind != "tick"]


@pytest.mark.asyncio
async def test_prompt_travels_on_stdin_and_the_run_still_ends(tmp_path: Path) -> None:
    # The stub never exits on its own — it waits for the next message the way
    # the real CLI does. Getting an `exit` here means stdin was closed for it.
    runner = LocalRunner()
    spec = _spec(tmp_path, "r-prompt", conversation=_talking("hello agent"))
    events = await _collect(runner, spec, within=15)
    assert '{"type": "assistant", "text": "prompt:hello agent"}' in _stdout(events)
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0
    assert not [e for e in events if e.kind == "stall_timeout"]


@pytest.mark.asyncio
async def test_both_modes_produce_the_same_events_for_the_same_agent_output(
    tmp_path: Path,
) -> None:
    # AC 1: switching a run to a conversation changes how the prompt arrives,
    # and nothing else. Same stub, same output apart from the line that reports
    # the prompt itself — so the event shape either matches or the mode leaks.
    runner = LocalRunner()
    quiet = await _collect(runner, _spec(tmp_path, "r-quiet"), within=15)
    talking = await _collect(
        runner, _spec(tmp_path, "r-talking", conversation=_talking()), within=15
    )
    # `tick` is emitted on an idle poll, so its count is a timing artefact.
    assert _kinds(quiet) == _kinds(talking)
    assert _stdout(quiet)[1:] == _stdout(talking)[1:]
    assert [e.returncode for e in quiet] == [e.returncode for e in talking]


@pytest.mark.asyncio
async def test_control_request_is_answered_through_the_conversations_handler(
    tmp_path: Path,
) -> None:
    runner = LocalRunner()
    handler = _Handler({"accessToken": "tok-1"})
    spec = _spec(tmp_path, "r-answer", "--ask-token", conversation=_talking(handler=handler))
    events = await _collect(runner, spec, within=15)
    assert [r.subtype for r in handler.seen] == ["oauth_token_refresh"]
    assert handler.seen[0].request_id == "req-1"
    assert '{"type": "assistant", "text": "token:tok-1"}' in _stdout(events)
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_a_burst_of_requests_is_answered_one_by_one(tmp_path: Path) -> None:
    # One 401 recovery produced three control requests in ~1.2s in the SYM-232
    # spike. Answering the first and going quiet would leave the rest waiting.
    runner = LocalRunner()
    handler = _Handler({"accessToken": "tok-1"})
    spec = _spec(
        tmp_path,
        "r-burst",
        "--ask-token",
        "--ask-token",
        "--ask-token",
        conversation=_talking(handler=handler),
    )
    events = await _collect(runner, spec, within=15)
    assert [r.request_id for r in handler.seen] == ["req-1", "req-2", "req-3"]
    assert _stdout(events).count('{"type": "assistant", "text": "token:tok-1"}') == 3
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_control_frames_never_reach_the_run_event_stream(tmp_path: Path) -> None:
    # The silent failure this mode has to avoid: protocol traffic read as the
    # agent's own output, corrupting completion markers and cost accounting.
    runner = LocalRunner()
    handler = _Handler({"accessToken": "tok-1"})
    spec = _spec(tmp_path, "r-filter", "--ask-token", conversation=_talking(handler=handler))
    events = await _collect(runner, spec, within=15)
    assert handler.seen, "the exchange must actually have happened"
    for line in _stdout(events):
        assert "control_request" not in line
        assert "control_response" not in line


@pytest.mark.asyncio
async def test_a_refused_request_ends_the_run_instead_of_waiting_out_the_agent(
    tmp_path: Path,
) -> None:
    runner = LocalRunner()
    handler = _Handler(None)
    spec = _spec(
        tmp_path,
        "r-refused",
        "--ask-token",
        "--linger",
        conversation=_talking(handler=handler),
    )
    loop = asyncio.get_running_loop()
    started = loop.time()
    events = await _collect(runner, spec, within=LINGER_SECS - 5)
    assert handler.seen
    assert loop.time() - started < LINGER_SECS / 2
    assert events[-1].kind == "exit"
    assert not [e for e in events if e.kind == "stall_timeout"]


@pytest.mark.asyncio
async def test_a_refused_run_that_ends_itself_keeps_its_own_exit_code(
    tmp_path: Path,
) -> None:
    # Without `--linger` the stub reports the refusal and finishes its turn.
    # The grace period is what lets it get there: SIGTERM straight away would
    # cost the final result frame and read as a plain crash downstream.
    runner = LocalRunner()
    handler = _Handler(None)
    spec = _spec(tmp_path, "r-refused-clean", "--ask-token", conversation=_talking(handler=handler))
    events = await _collect(runner, spec, within=15)
    assert '{"type": "assistant", "text": "token:refused"}' in _stdout(events)
    assert '{"type": "result", "result": "SYMPHONY_DONE"}' in _stdout(events)
    assert events[-1].kind == "exit"
    assert events[-1].returncode == 0


@pytest.mark.asyncio
async def test_a_handler_that_raises_counts_as_a_refusal(tmp_path: Path) -> None:
    runner = LocalRunner()
    handler = _Handler(None, boom=True)
    spec = _spec(
        tmp_path,
        "r-boom",
        "--ask-token",
        "--linger",
        conversation=_talking(handler=handler),
    )
    events = await _collect(runner, spec, within=LINGER_SECS - 5)
    assert handler.seen
    assert events[-1].kind == "exit"


@pytest.mark.asyncio
async def test_a_missing_handler_refuses_rather_than_hanging(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = _spec(tmp_path, "r-nohandler", "--ask-token", "--linger", conversation=_talking())
    events = await _collect(runner, spec, within=LINGER_SECS - 5)
    assert events[-1].kind == "exit"


@pytest.mark.asyncio
async def test_the_one_directional_mode_is_unchanged(tmp_path: Path) -> None:
    # No conversation on the spec: stdin stays /dev/null, so the stub reads EOF
    # at once and reports it. Nothing about the old shape moves in this ticket.
    runner = LocalRunner()
    spec = _spec(tmp_path, "r-old")
    events = await _collect(runner, spec, within=15)
    assert '{"type": "assistant", "text": "prompt:none"}' in _stdout(events)
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_a_frame_with_a_non_string_type_is_just_output(tmp_path: Path) -> None:
    # Classification runs inside the stdout pump, and a set lookup on an
    # unhashable value raises. One malformed provider frame would then kill the
    # pump, losing the rest of the run's output and leaving stdin open: a stall
    # timeout, several minutes later, with no hint of where it came from.
    runner = LocalRunner()
    spec = _spec(tmp_path, "r-malformed", "--malformed", conversation=_talking())
    events = await _collect(runner, spec, within=15)
    assert '{"type": ["control_request"], "text": "not a string type"}' in _stdout(events)
    assert '{"type": "result", "result": "SYMPHONY_DONE"}' in _stdout(events)
    assert events[-1].kind == "exit"


@pytest.mark.asyncio
async def test_a_refused_run_that_ignores_sigterm_is_killed(tmp_path: Path) -> None:
    # The refusal path exists to end a run promptly. An agent that catches
    # SIGTERM would otherwise live to the stall deadline — minutes — which is
    # the exact wait being avoided.
    runner = LocalRunner()
    handler = _Handler(None)
    spec = _spec(
        tmp_path,
        "r-deaf",
        "--ask-token",
        "--linger",
        "--deaf",
        conversation=_talking(handler=handler),
    )
    events = await _collect(runner, spec, within=LINGER_SECS - 5)
    assert handler.seen
    assert events[-1].kind == "exit"
    assert not [e for e in events if e.kind == "stall_timeout"]


@pytest.mark.asyncio
async def test_abandoning_a_run_leaves_no_child_and_no_bookkeeping(tmp_path: Path) -> None:
    # Whatever ends the iteration — shutdown, a cancelled task, a caller that
    # stops reading — must not leave the agent running or the run registered.
    runner = LocalRunner()
    spec = _spec(tmp_path, "r-abandoned", "--ask-token", "--linger", conversation=_talking())
    pid: int | None = None
    stream = runner.run(spec)
    async with asyncio.timeout(15):
        async for ev in stream:
            if ev.kind == "started":
                pid = ev.pid
                break
    await stream.aclose()
    assert pid is not None
    assert runner._active == {}
    with pytest.raises(ProcessLookupError):
        os.killpg(pid, 0)


# --- the contract the harness's deterministic runner has to keep -----------


def _control_request(request_id: str) -> str:
    return json.dumps(
        {
            "type": "control_request",
            "request_id": request_id,
            "request": {"subtype": "oauth_token_refresh"},
        }
    )


_AGENT_STREAM = [
    '{"type": "assistant", "text": "prompt:hi"}',
    _control_request("req-1"),
    '{"type": "assistant", "text": "token:tok-1"}',
    '{"type": "result", "result": "SYMPHONY_DONE"}',
]


def _scripted(events: list[str]) -> FakeRunner:
    fake = FakeRunner()
    fake.enqueue(
        [
            RunnerEvent(kind="started", pid=1),
            *[RunnerEvent(kind="stdout", line=line) for line in events],
            RunnerEvent(kind="exit", returncode=0),
        ]
    )
    return fake


def _fake_spec(tmp_path: Path, run_id: str, conversation: Conversation) -> RunnerSpec:
    return RunnerSpec(
        run_id=run_id,
        workspace_path=tmp_path,
        command=["<fake>"],
        conversation=conversation,
    )


@pytest.mark.asyncio
async def test_fake_runner_matches_local_runner_on_the_control_exchange(
    tmp_path: Path,
) -> None:
    real_handler = _Handler({"accessToken": "tok-1"})
    real = await _collect(
        LocalRunner(),
        _spec(tmp_path, "r-real", "--ask-token", conversation=_talking(handler=real_handler)),
        within=15,
    )

    fake_handler = _Handler({"accessToken": "tok-1"})
    fake = _scripted(_AGENT_STREAM)
    faked = [
        ev async for ev in fake.run(_fake_spec(tmp_path, "r-fake", _talking(handler=fake_handler)))
    ]

    assert _stdout(real) == _stdout(faked)
    assert [r.subtype for r in fake_handler.seen] == [r.subtype for r in real_handler.seen]
    assert [r.request_id for r in fake_handler.seen] == [r.request_id for r in real_handler.seen]
    assert fake.prompts == ["hi"]


@pytest.mark.asyncio
async def test_fake_runner_keeps_a_refused_runs_own_ending(tmp_path: Path) -> None:
    # A refusal does not entitle the fake to invent a kill. The real runner
    # closes stdin and waits, and an agent that ends its own turn keeps its
    # terminal output and its exit code — see
    # `test_a_refused_run_that_ends_itself_keeps_its_own_exit_code`. A fake that
    # synthesised `-15` would let SYM-236 exercise a path production does not
    # take, and would never let it check the parsing of a real refusal result.
    fake = _scripted(_AGENT_STREAM)
    handler = _Handler(None)
    events = [
        ev
        async for ev in fake.run(_fake_spec(tmp_path, "r-fake-refused", _talking(handler=handler)))
    ]
    assert [r.request_id for r in handler.seen] == ["req-1"]
    assert '{"type": "result", "result": "SYMPHONY_DONE"}' in _stdout(events)
    assert events[-1].kind == "exit"
    assert events[-1].returncode == 0


@pytest.mark.asyncio
async def test_fake_runner_stops_asking_the_handler_after_a_refusal(tmp_path: Path) -> None:
    # The real channel closes stdin on a refusal, so nothing that arrives after
    # it reaches the handler — though it is still protocol traffic and still
    # must not surface as agent output.
    fake = _scripted([*_AGENT_STREAM, _control_request("req-2")])
    handler = _Handler(None)
    events = [
        ev async for ev in fake.run(_fake_spec(tmp_path, "r-fake-late", _talking(handler=handler)))
    ]
    assert [r.request_id for r in handler.seen] == ["req-1"]
    for line in _stdout(events):
        assert "control_request" not in line


@pytest.mark.asyncio
async def test_fake_runner_treats_a_raising_handler_as_a_refusal(tmp_path: Path) -> None:
    # A handler that blows up must not blow up the run: the real channel logs
    # it and answers with an error, exactly as it would a plain refusal.
    fake = _scripted([*_AGENT_STREAM, _control_request("req-2")])
    handler = _Handler(None, boom=True)
    events = [
        ev async for ev in fake.run(_fake_spec(tmp_path, "r-fake-boom", _talking(handler=handler)))
    ]
    assert [r.request_id for r in handler.seen] == ["req-1"]
    assert events[-1].kind == "exit"
