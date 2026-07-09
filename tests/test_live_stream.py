from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.app import create_app
from symphony.auth import Auth0Settings
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.ui import live as live_module
from symphony.ui.live import parse_stream_events

from .test_webhook import _Handler

# --- parser: claude stream-json ---------------------------------------------


def test_parse_claude_assistant_text_is_message() -> None:
    line = json.dumps(
        {
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "  Working on it  "}]},
        }
    )
    assert parse_stream_events(line) == [{"kind": "message", "text": "Working on it"}]


def test_parse_claude_tool_use_is_tool_call() -> None:
    line = json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "Bash",
                        "input": {"command": "pytest -q"},
                    }
                ]
            },
        }
    )
    assert parse_stream_events(line) == [
        {"kind": "tool_call", "tool": "Bash", "detail": "pytest -q"}
    ]


def test_parse_claude_tool_use_redacts_secret_in_command() -> None:
    line = json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "Bash",
                        "input": {
                            "command": "curl -H 'Authorization: Bearer sk-live-abc123xyz789' https://x"
                        },
                    }
                ]
            },
        }
    )
    events = parse_stream_events(line)
    assert len(events) == 1
    assert "sk-live-abc123xyz789" not in events[0]["detail"]
    assert "[redacted]" in events[0]["detail"]


def test_parse_claude_edit_tool_is_file_edit() -> None:
    line = json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "name": "Edit",
                        "input": {"file_path": "/ws/src/symphony/app.py"},
                    }
                ]
            },
        }
    )
    assert parse_stream_events(line) == [{"kind": "file_edit", "tool": "Edit", "files": ["app.py"]}]


def test_parse_claude_result_is_tokens_tick() -> None:
    line = json.dumps(
        {
            "type": "result",
            "total_cost_usd": 0.5,
            "usage": {
                "input_tokens": 100,
                "output_tokens": 20,
                "cache_creation_input_tokens": 30,
                "cache_read_input_tokens": 40,
            },
        }
    )
    assert parse_stream_events(line) == [
        {
            "kind": "tokens",
            "cumulative": True,
            "input_tokens": 100,
            "output_tokens": 20,
            "cache_write_tokens": 30,
            "cache_read_tokens": 40,
            "cost_usd": 0.5,
        }
    ]


def test_parse_claude_message_with_text_and_tool_use_yields_both() -> None:
    line = json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "Let me run the tests."},
                    {"type": "tool_use", "name": "Bash", "input": {"command": "ls"}},
                ]
            },
        }
    )
    assert parse_stream_events(line) == [
        {"kind": "message", "text": "Let me run the tests."},
        {"kind": "tool_call", "tool": "Bash", "detail": "ls"},
    ]


# --- parser: codex stream-json ----------------------------------------------


def test_parse_codex_command_started_is_tool_call() -> None:
    line = json.dumps(
        {
            "type": "item.started",
            "item": {"id": "cmd-1", "type": "command_execution", "command": "pytest"},
        }
    )
    assert parse_stream_events(line) == [{"kind": "tool_call", "tool": "shell", "detail": "pytest"}]


def test_parse_codex_command_started_redacts_secret() -> None:
    line = json.dumps(
        {
            "type": "item.started",
            "item": {
                "id": "cmd-1",
                "type": "command_execution",
                "command": "export SUPABASE_ACCESS_TOKEN=sk-live-abc123xyz789",
            },
        }
    )
    events = parse_stream_events(line)
    assert len(events) == 1
    assert "sk-live-abc123xyz789" not in events[0]["detail"]
    assert "[redacted]" in events[0]["detail"]


def test_parse_codex_file_change_is_file_edit() -> None:
    line = json.dumps(
        {
            "type": "item.completed",
            "item": {
                "id": "file-1",
                "type": "file_change",
                "changes": [{"path": "/ws/src/one.py"}, {"path": "/ws/src/two.py"}],
            },
        }
    )
    assert parse_stream_events(line) == [{"kind": "file_edit", "files": ["one.py", "two.py"]}]


def test_parse_codex_token_count_is_tokens_tick() -> None:
    line = json.dumps(
        {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": 7,
                    "output_tokens": 3,
                    "cache_read_tokens": 1,
                }
            },
        }
    )
    assert parse_stream_events(line) == [
        {
            "kind": "tokens",
            "cumulative": True,
            "input_tokens": 7,
            "output_tokens": 3,
            "cache_write_tokens": 0,
            "cache_read_tokens": 1,
            "cost_usd": 0.0,
        }
    ]


def test_parse_non_json_and_stderr_are_ignored() -> None:
    assert parse_stream_events("") == []
    assert parse_stream_events("not json") == []
    assert parse_stream_events("[stderr] some diagnostic") == []


# --- endpoint ---------------------------------------------------------------


def _write_log(log_root: Path, run_id: str, lines: list[str]) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    (log_root / f"{run_id}.log").write_text("".join(f"{line}\n" for line in lines))


async def _seed_run(conn: object, run_id: str, status: str) -> None:
    await db.issues.upsert(  # type: ignore[attr-defined]
        conn,
        id="iss-live",
        identifier="ENG-1",
        title="Live",
        team_key="ENG",
    )
    await conn.execute(  # type: ignore[attr-defined]
        """
        INSERT INTO runs (id, issue_id, stage, status, pid, started_at)
        VALUES (?, 'iss-live', 'implement', ?, NULL, '2026-05-17T10:00:00Z')
        """,
        (run_id, status),
    )
    await conn.commit()  # type: ignore[attr-defined]


def _events(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


@pytest.mark.asyncio
async def test_stream_emits_parsed_events_and_ends_for_completed_run(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    conn = await db.connect(db_path)
    try:
        await _seed_run(conn, "run-live", "completed")
        _write_log(
            log_root,
            "run-live",
            [
                json.dumps(
                    {
                        "type": "assistant",
                        "message": {"content": [{"type": "text", "text": "hi"}]},
                    }
                ),
                json.dumps(
                    {
                        "type": "assistant",
                        "message": {
                            "content": [
                                {"type": "tool_use", "name": "Bash", "input": {"command": "ls"}}
                            ]
                        },
                    }
                ),
                "not json at all",
            ],
        )
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_log_root=log_root,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/runs/run-live/stream")
    finally:
        await conn.close()

    assert resp.status_code == 200
    events = _events(resp.text)
    kinds = [e["kind"] for e in events]
    assert {"kind": "message", "text": "hi"} in events
    assert {"kind": "tool_call", "tool": "Bash", "detail": "ls"} in events
    assert kinds[-1] == "end"


@pytest.mark.asyncio
async def test_stream_offset_skips_already_read_bytes(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    conn = await db.connect(db_path)
    try:
        await _seed_run(conn, "run-off", "completed")
        first = json.dumps(
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "first"}]}}
        )
        second = json.dumps(
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "second"}]}}
        )
        _write_log(log_root, "run-off", [first, second])
        offset = len(f"{first}\n".encode())
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_log_root=log_root,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get(f"/api/runs/run-off/stream?offset={offset}")
    finally:
        await conn.close()

    assert resp.status_code == 200
    events = _events(resp.text)
    texts = [e.get("text") for e in events if e["kind"] == "message"]
    assert texts == ["second"]


@pytest.mark.asyncio
async def test_stream_tails_growing_log_across_polls(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    conn = await db.connect(db_path)
    try:
        await _seed_run(conn, "run-grow", "running")
        log_root.mkdir(parents=True, exist_ok=True)
        log_path = log_root / "run-grow.log"
        log_path.write_text("")
        first = json.dumps(
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "first"}]}}
        )
        second = json.dumps(
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "second"}]}}
        )

        async def _grow_log() -> None:
            await asyncio.sleep(0.05)
            with log_path.open("a") as fh:
                fh.write(first + "\n")
            await asyncio.sleep(0.6)
            with log_path.open("a") as fh:
                fh.write(second + "\n")
            raw = sqlite3.connect(db_path)
            try:
                raw.execute("UPDATE runs SET status = 'completed' WHERE id = 'run-grow'")
                raw.commit()
            finally:
                raw.close()

        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_log_root=log_root,
        )
        grower = asyncio.create_task(_grow_log())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/runs/run-grow/stream")
        await grower
    finally:
        await conn.close()

    assert resp.status_code == 200
    events = _events(resp.text)
    messages = [e["text"] for e in events if e["kind"] == "message"]
    assert messages == ["first", "second"]
    cursor_offsets = [e["offset"] for e in events if e["kind"] == "cursor"]
    assert len(f"{first}\n".encode()) in cursor_offsets
    assert cursor_offsets[-1] == len(f"{first}\n{second}\n".encode())
    assert events[-1]["kind"] == "end"


@pytest.mark.asyncio
async def test_stream_drains_final_line_written_during_terminal_transition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulates the race the reviewer flagged: the run's last line lands on
    disk and the status flips to terminal in the gap between one loop
    iteration's read and its status check. The fix must drain that line
    before signalling `end` instead of dropping it."""
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    conn = await db.connect(db_path)
    try:
        await _seed_run(conn, "run-race", "running")
        first = json.dumps(
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "first"}]}}
        )
        second = json.dumps(
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "second"}]}}
        )
        _write_log(log_root, "run-race", [first])
        log_path = log_root / "run-race.log"

        original_read_from = live_module._read_from
        calls = {"n": 0}

        def _fake_read_from(path: Path, pos: int) -> tuple[bytes, int]:
            calls["n"] += 1
            data, new_pos = original_read_from(path, pos)
            if calls["n"] == 1:
                with path.open("a") as fh:
                    fh.write(second + "\n")
                raw = sqlite3.connect(db_path)
                try:
                    raw.execute("UPDATE runs SET status = 'completed' WHERE id = 'run-race'")
                    raw.commit()
                finally:
                    raw.close()
            return data, new_pos

        monkeypatch.setattr(live_module, "_read_from", _fake_read_from)

        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_log_root=log_root,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/runs/run-race/stream")
    finally:
        await conn.close()

    assert resp.status_code == 200
    events = _events(resp.text)
    messages = [e["text"] for e in events if e["kind"] == "message"]
    assert messages == ["first", "second"]
    assert events[-1]["kind"] == "end"
    assert log_path.read_text() == f"{first}\n{second}\n"


@pytest.mark.asyncio
async def test_stream_endpoint_is_auth_gated(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    conn = await db.connect(db_path)
    try:
        await _seed_run(conn, "run-gated", "completed")
        _write_log(log_root, "run-gated", ["{}"])
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_log_root=log_root,
            auth0_settings=Auth0Settings.from_env(
                domain="t.us.auth0.com",
                client_id="cid",
                allowed_emails="alice@example.com",
            ),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/runs/run-gated/stream")
    finally:
        await conn.close()

    # Route is mounted (not 404) but rejects the missing bearer.
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_stream_unknown_run_is_404(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    conn = await db.connect(db_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_log_root=log_root,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/runs/nope/stream")
    finally:
        await conn.close()

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_stream_orphaned_log_without_runs_row_is_404(tmp_path: Path) -> None:
    """A log file can outlive its `runs` row (DB reset/backfill mismatch, a
    failed insert that left a log behind) — that must still 404 rather than
    stream the orphaned file's contents."""
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    conn = await db.connect(db_path)
    try:
        _write_log(log_root, "run-orphaned", ["{}"])
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_log_root=log_root,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/api/runs/run-orphaned/stream")
    finally:
        await conn.close()

    assert resp.status_code == 404


# --- orchestrator write path: the log must be flushed line-by-line ---------
#
# The endpoint tests above write the run log through their own `open("a")`
# handles, which auto-flush on block exit — they never exercise the
# orchestrator's per-line `logf.flush()` calls that make live tailing
# possible while a run is still in progress. This test drives the write
# path directly and reads the file from an independent handle *before* the
# run finishes, so it would fail if those `flush()` calls were removed.


class _PausingRunner:
    """Yields one stdout and one stderr line, each followed by a pause the
    test controls, so it can observe the log file mid-write."""

    def __init__(self) -> None:
        self.after_stdout = asyncio.Event()
        self.after_stderr = asyncio.Event()
        self.release_stdout = asyncio.Event()
        self.release_stderr = asyncio.Event()

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        yield RunnerEvent(kind="started", pid=1)
        yield RunnerEvent(kind="stdout", line="first line")
        self.after_stdout.set()
        await self.release_stdout.wait()
        yield RunnerEvent(kind="stderr", line="boom")
        self.after_stderr.set()
        await self.release_stderr.wait()
        yield RunnerEvent(kind="exit", returncode=0)

    async def kill(self, run_id: str) -> None:
        pass


@pytest.mark.asyncio
async def test_run_log_is_flushed_line_by_line_while_run_is_live(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    conn = await db.connect(db_path)
    try:
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            linear_states=LinearStates(ready="Todo", code_review="In Review"),
        )
        cfg = Config(
            repos=[binding],
            log_root=log_root,
            workspace_root=tmp_path / "ws",
            db_path=db_path,
        )
        issue = LinearIssue(
            id="iss-1",
            identifier="ENG-1",
            title="Add auth",
            description="Need OAuth.",
            url="https://linear.app/team/issue/ENG-1",
            state_id="state-todo",
            state_name="Todo",
            state_type="unstarted",
            team_key="ENG",
            labels=[],
        )
        runner = _PausingRunner()
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=runner,
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        log_path = log_root / "run-flush.log"

        task = asyncio.create_task(
            orch._run_runner(  # noqa: SLF001
                run_id="run-flush",
                workspace_path=tmp_path / "ws",
                command=["true"],
                stage="implement",
                agent="claude",
                binding=binding,
                issue=issue,
            )
        )
        try:
            await asyncio.wait_for(runner.after_stdout.wait(), timeout=5)
            # The run is still live (log file handle still open) — this only
            # sees the stdout line if the orchestrator flushed after writing it.
            assert log_path.read_text() == "first line\n"
            runner.release_stdout.set()

            await asyncio.wait_for(runner.after_stderr.wait(), timeout=5)
            assert log_path.read_text() == "first line\n[stderr] boom\n"
            runner.release_stderr.set()

            await asyncio.wait_for(task, timeout=5)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    finally:
        await conn.close()
