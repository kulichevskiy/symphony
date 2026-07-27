"""Control-channel mode: the runner holds a conversation with the agent.

Two seams, deliberately different in what they trust:

* against a **stub child process** (`tests/fixtures/control_channel_agent.py`)
  we stop trusting the binary and check the wire — the prompt really travels on
  stdin, the control frames really round-trip, the process really exits;
* against the **deterministic runner** the harness uses we check the contract —
  the fake must answer a control request the same way, or every orchestrator
  test written on top of it in SYM-236 would be testing a fiction.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from pathlib import Path

import pytest

from symphony.agent.control_channel import ControlRequest
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


def _spec(tmp_path: Path, run_id: str, *stub_args: str, **kwargs: object) -> RunnerSpec:
    import sys

    return RunnerSpec(
        run_id=run_id,
        workspace_path=tmp_path,
        command=[sys.executable, str(STUB), *stub_args],
        stall_secs=20,
        **kwargs,  # type: ignore[arg-type]
    )


async def _collect(runner: LocalRunner, spec: RunnerSpec, *, within: float) -> list[RunnerEvent]:
    async with asyncio.timeout(within):
        return [ev async for ev in runner.run(spec)]


def _stdout(events: list[RunnerEvent]) -> list[str]:
    return [e.line for e in events if e.kind == "stdout" and e.line]


@pytest.mark.asyncio
async def test_prompt_travels_on_stdin_and_the_run_still_ends(tmp_path: Path) -> None:
    # The stub never exits on its own — it waits for the next message the way
    # the real CLI does. Getting an `exit` here means stdin was closed for it.
    runner = LocalRunner()
    spec = _spec(tmp_path, "r-prompt", prompt="hello agent")
    events = await _collect(runner, spec, within=15)
    assert '{"type": "assistant", "text": "prompt:hello agent"}' in _stdout(events)
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0
    assert not [e for e in events if e.kind == "stall_timeout"]


@pytest.mark.asyncio
async def test_control_request_is_answered_through_the_specs_handler(tmp_path: Path) -> None:
    runner = LocalRunner()
    handler = _Handler({"accessToken": "tok-1"})
    spec = _spec(tmp_path, "r-answer", "--ask-token", prompt="hi", control_handler=handler)
    events = await _collect(runner, spec, within=15)
    assert [r.subtype for r in handler.seen] == ["oauth_token_refresh"]
    assert handler.seen[0].request_id == "req-1"
    assert '{"type": "assistant", "text": "token:tok-1"}' in _stdout(events)
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_control_frames_never_reach_the_run_event_stream(tmp_path: Path) -> None:
    # The silent failure this mode has to avoid: protocol traffic read as the
    # agent's own output, corrupting completion markers and cost accounting.
    runner = LocalRunner()
    handler = _Handler({"accessToken": "tok-1"})
    spec = _spec(tmp_path, "r-filter", "--ask-token", prompt="hi", control_handler=handler)
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
        prompt="hi",
        control_handler=handler,
    )
    loop = asyncio.get_running_loop()
    started = loop.time()
    events = await _collect(runner, spec, within=LINGER_SECS - 5)
    assert handler.seen
    assert loop.time() - started < LINGER_SECS / 2
    assert events[-1].kind == "exit"
    assert not [e for e in events if e.kind == "stall_timeout"]


@pytest.mark.asyncio
async def test_a_handler_that_raises_counts_as_a_refusal(tmp_path: Path) -> None:
    runner = LocalRunner()
    handler = _Handler(None, boom=True)
    spec = _spec(
        tmp_path,
        "r-boom",
        "--ask-token",
        "--linger",
        prompt="hi",
        control_handler=handler,
    )
    events = await _collect(runner, spec, within=LINGER_SECS - 5)
    assert handler.seen
    assert events[-1].kind == "exit"


@pytest.mark.asyncio
async def test_a_missing_handler_refuses_rather_than_hanging(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = _spec(tmp_path, "r-nohandler", "--ask-token", "--linger", prompt="hi")
    events = await _collect(runner, spec, within=LINGER_SECS - 5)
    assert events[-1].kind == "exit"


@pytest.mark.asyncio
async def test_the_one_directional_mode_is_unchanged(tmp_path: Path) -> None:
    # No prompt on the spec: stdin stays /dev/null, so the stub reads EOF at
    # once and reports it. Nothing about the old shape moves in this ticket.
    runner = LocalRunner()
    spec = _spec(tmp_path, "r-old")
    events = await _collect(runner, spec, within=15)
    assert '{"type": "assistant", "text": "prompt:none"}' in _stdout(events)
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


# --- the contract the harness's deterministic runner has to keep -----------

_AGENT_STREAM = [
    '{"type": "assistant", "text": "prompt:hi"}',
    json.dumps(
        {
            "type": "control_request",
            "request_id": "req-1",
            "request": {"subtype": "oauth_token_refresh"},
        }
    ),
    '{"type": "assistant", "text": "token:tok-1"}',
    '{"type": "result", "result": "SYMPHONY_DONE"}',
]


@pytest.mark.asyncio
async def test_fake_runner_matches_local_runner_on_the_control_exchange(
    tmp_path: Path,
) -> None:
    real_handler = _Handler({"accessToken": "tok-1"})
    real = await _collect(
        LocalRunner(),
        _spec(tmp_path, "r-real", "--ask-token", prompt="hi", control_handler=real_handler),
        within=15,
    )

    fake_handler = _Handler({"accessToken": "tok-1"})
    fake = FakeRunner()
    fake.enqueue(
        [
            RunnerEvent(kind="started", pid=1),
            *[RunnerEvent(kind="stdout", line=line) for line in _AGENT_STREAM],
            RunnerEvent(kind="exit", returncode=0),
        ]
    )
    fake_spec = RunnerSpec(
        run_id="r-fake",
        workspace_path=tmp_path,
        command=["<fake>"],
        prompt="hi",
        control_handler=fake_handler,
    )
    faked = [ev async for ev in fake.run(fake_spec)]

    assert _stdout(real) == _stdout(faked)
    assert [r.subtype for r in fake_handler.seen] == [r.subtype for r in real_handler.seen]
    assert [r.request_id for r in fake_handler.seen] == [r.request_id for r in real_handler.seen]
    assert fake.prompts == ["hi"]
