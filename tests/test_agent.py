import pytest

from symphony.agent import (
    build_argv,
    extract_session_id,
    find_result_event,
    parse_event_line,
    run_agent,
)


def test_parse_event_line_valid():
    line = '{"type":"system","subtype":"init","session_id":"abc"}'
    assert parse_event_line(line) == {
        "type": "system",
        "subtype": "init",
        "session_id": "abc",
    }


def test_parse_event_line_with_trailing_newline():
    assert parse_event_line('{"a":1}\n') == {"a": 1}


def test_parse_event_line_blank_returns_none():
    assert parse_event_line("") is None
    assert parse_event_line("   \n") is None


def test_parse_event_line_invalid_json_returns_none():
    assert parse_event_line("not json") is None
    assert parse_event_line("{partial") is None


def test_extract_session_id_from_first_event():
    events = [
        {"type": "system", "session_id": "sid-123"},
        {"type": "assistant", "session_id": "sid-123"},
    ]
    assert extract_session_id(events) == "sid-123"


def test_extract_session_id_falls_back_to_later_event():
    events = [
        {"type": "noise"},
        {"type": "assistant", "session_id": "sid-123"},
    ]
    assert extract_session_id(events) == "sid-123"


def test_extract_session_id_none_if_absent():
    assert extract_session_id([{"type": "x"}]) is None
    assert extract_session_id([]) is None


def test_find_result_event_picks_last_result():
    events = [
        {"type": "system"},
        {"type": "result", "subtype": "success", "is_error": False},
    ]
    assert find_result_event(events) == events[-1]


def test_find_result_event_none_if_absent():
    assert find_result_event([{"type": "system"}]) is None


def test_build_argv_minimal():
    argv = build_argv("hi", model="m", max_turns=5, permission_mode="x")
    assert argv[0] == "claude"
    assert "-p" in argv
    assert "hi" in argv
    assert "--output-format" in argv
    assert "stream-json" in argv
    assert "--model" in argv
    assert "m" in argv
    assert "--max-turns" in argv
    assert "5" in argv
    assert "--permission-mode" in argv
    assert "x" in argv


def test_build_argv_resume():
    argv = build_argv(
        "hi", model="m", max_turns=5, permission_mode="x", resume_session="sess-1"
    )
    assert "--resume" in argv
    assert "sess-1" in argv


def test_build_argv_no_resume_omits_flag():
    argv = build_argv("hi", model="m", max_turns=5, permission_mode="x")
    assert "--resume" not in argv


# ---- mocked subprocess tests ----


class _FakeStdout:
    def __init__(self, lines: list[bytes]):
        self._iter = iter(lines)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration as e:
            raise StopAsyncIteration from e


class _FakeStderr:
    def __init__(self, data: bytes = b""):
        self._data = data

    async def read(self):
        return self._data


class _FakeProcess:
    def __init__(self, lines: list[bytes], exit_code: int = 0, stderr: bytes = b""):
        self.stdout = _FakeStdout(lines)
        self.stderr = _FakeStderr(stderr)
        self._exit_code = exit_code

    async def wait(self):
        return self._exit_code


def _make_spawner(proc: _FakeProcess):
    captured: dict = {}

    async def _spawn(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return proc

    _spawn.captured = captured  # type: ignore[attr-defined]
    return _spawn


@pytest.mark.asyncio
async def test_run_agent_happy_path(tmp_path):
    lines = [
        b'{"type":"system","subtype":"init","session_id":"sess-1"}\n',
        b'{"type":"assistant","session_id":"sess-1"}\n',
        b'{"type":"result","subtype":"success","is_error":false,'
        b'"duration_ms":100,"num_turns":1,"total_cost_usd":0.01,'
        b'"result":"4","session_id":"sess-1"}\n',
    ]
    proc = _FakeProcess(lines, exit_code=0)
    spawner = _make_spawner(proc)
    res = await run_agent("hi", tmp_path, spawner=spawner)

    assert res.session_id == "sess-1"
    assert res.exit_code == 0
    assert res.success is True
    assert res.is_error is False
    assert res.final_text == "4"
    assert res.num_turns == 1
    assert res.duration_ms == 100
    assert res.total_cost_usd == 0.01
    assert len(res.raw_events) == 3
    # cwd was set on subprocess
    assert spawner.captured["kwargs"]["cwd"] == str(tmp_path)


@pytest.mark.asyncio
async def test_run_agent_nonzero_exit_is_failure(tmp_path):
    lines = [b'{"type":"system","subtype":"init","session_id":"sess-2"}\n']
    proc = _FakeProcess(lines, exit_code=1, stderr=b"boom")
    res = await run_agent("hi", tmp_path, spawner=_make_spawner(proc))
    assert res.exit_code == 1
    assert res.success is False
    assert res.session_id == "sess-2"
    assert res.stderr == "boom"


@pytest.mark.asyncio
async def test_run_agent_result_is_error_marks_failure(tmp_path):
    lines = [
        b'{"type":"system","subtype":"init","session_id":"sess-3"}\n',
        b'{"type":"result","subtype":"error_max_turns","is_error":true,'
        b'"duration_ms":42,"num_turns":50,"session_id":"sess-3"}\n',
    ]
    proc = _FakeProcess(lines, exit_code=0)
    res = await run_agent("hi", tmp_path, spawner=_make_spawner(proc))
    assert res.is_error is True
    assert res.success is False  # is_error overrides exit code 0


@pytest.mark.asyncio
async def test_run_agent_skips_invalid_lines(tmp_path):
    lines = [
        b'{"type":"system","session_id":"x"}\n',
        b"not json\n",
        b"\n",
        b'{"type":"result","is_error":false,"session_id":"x"}\n',
    ]
    proc = _FakeProcess(lines, exit_code=0)
    res = await run_agent("hi", tmp_path, spawner=_make_spawner(proc))
    assert len(res.raw_events) == 2  # invalid + blank dropped
    assert res.session_id == "x"


@pytest.mark.asyncio
async def test_run_agent_calls_on_event(tmp_path):
    lines = [
        b'{"type":"system","session_id":"x"}\n',
        b'{"type":"result","is_error":false,"session_id":"x"}\n',
    ]
    received: list[dict] = []
    proc = _FakeProcess(lines, exit_code=0)
    await run_agent(
        "hi", tmp_path, spawner=_make_spawner(proc), on_event=received.append
    )
    assert len(received) == 2
    assert received[0]["type"] == "system"
    assert received[1]["type"] == "result"


@pytest.mark.asyncio
async def test_run_agent_detaches_stdin(tmp_path):
    """Regression: subprocess must not inherit parent stdin.

    `claude -p` reads stdin; if Symphony is launched in a pipeline or under
    a supervisor that keeps stdin open, the agent can consume unrelated
    bytes or block on EOF.
    """
    import asyncio as _asyncio

    proc = _FakeProcess(
        [b'{"type":"result","is_error":false,"session_id":"x"}\n'], exit_code=0
    )
    spawner = _make_spawner(proc)
    await run_agent("hi", tmp_path, spawner=spawner)
    assert spawner.captured["kwargs"].get("stdin") == _asyncio.subprocess.DEVNULL


@pytest.mark.asyncio
async def test_run_agent_passes_high_stdout_limit(tmp_path):
    """Regression: subprocess stream limit must be raised above the asyncio
    default (64KiB) so a single oversized `result` line doesn't blow up the
    `async for line in stdout` loop with LimitOverrunError.
    """
    proc = _FakeProcess(
        [b'{"type":"result","is_error":false,"session_id":"x"}\n'], exit_code=0
    )
    spawner = _make_spawner(proc)
    await run_agent("hi", tmp_path, spawner=spawner)
    limit = spawner.captured["kwargs"].get("limit")
    assert limit is not None
    assert limit >= 1024 * 1024  # at least 1 MiB


@pytest.mark.asyncio
async def test_run_agent_drains_stderr_with_many_stdout_events(tmp_path):
    """Regression: stderr must be captured even when stdout is long.

    The implementation drains stderr concurrently to avoid a pipe-buffer
    deadlock when the child writes a lot to stderr while we're still
    iterating stdout. This test ensures both streams are captured together.
    """
    lines = [
        b'{"type":"system","session_id":"x"}\n',
        b'{"type":"assistant","session_id":"x"}\n',
        b'{"type":"assistant","session_id":"x"}\n',
        b'{"type":"assistant","session_id":"x"}\n',
        b'{"type":"result","is_error":false,"session_id":"x"}\n',
    ]
    proc = _FakeProcess(lines, exit_code=0, stderr=b"warn1\nwarn2\n")
    res = await run_agent("hi", tmp_path, spawner=_make_spawner(proc))
    assert res.stderr == "warn1\nwarn2\n"
    assert len(res.raw_events) == 5


@pytest.mark.asyncio
async def test_run_agent_no_result_event_is_failure(tmp_path):
    """Agent exited 0 but never emitted a result event — treat as failure."""
    lines = [b'{"type":"system","session_id":"x"}\n']
    proc = _FakeProcess(lines, exit_code=0)
    res = await run_agent("hi", tmp_path, spawner=_make_spawner(proc))
    assert res.success is False
