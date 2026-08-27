"""Live agent-output streaming for the daemon UI.

Two pieces the SPA needs to watch a running agent:

- ``parse_stream_events`` turns one raw per-run log line (``claude`` or
  ``codex`` ``stream-json``) into zero or more small, JSON-serializable
  "readable" events — messages, tool calls, file edits, token ticks — so the
  browser renders a feed instead of a raw JSONL dump.
- ``create_live_stream_router`` mounts ``GET /api/runs/{run_id}/stream`` which
  tails ``{log_root}/{run_id}.log`` and pushes those events as NDJSON while the
  run is live, ending cleanly once the run leaves ``running``.

The endpoint lives under the shared ``/api/*`` auth gate (see ``app.py``); the
frontend consumes it with ``fetch`` + ``ReadableStream`` so the request carries
the Auth0 ``Authorization: Bearer`` header (an ``EventSource`` could not).
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from starlette.responses import StreamingResponse

from ..agent.activity import sanitize_text
from ..agent.process import parse_event_line
from ..db.runs import LIVE_STATUSES
from .db import ReadOnlyDbPool

# Tools whose invocation is really a file edit — surfaced as a distinct event
# so the feed can show "edited app.py" rather than a generic tool call.
_FILE_EDIT_TOOLS = frozenset(
    {"edit", "write", "multiedit", "notebookedit", "str_replace_editor", "apply_patch"}
)

_POLL_INTERVAL_SECS = 0.5


def parse_stream_events(line: str) -> list[dict[str, Any]]:
    """Parse one raw log line into readable events (possibly several, or none).

    Handles both agent formats. Non-JSON lines (including the ``[stderr] ``
    prefixed diagnostics the orchestrator interleaves) yield no events.
    """
    line = line.strip()
    if not line:
        return []
    try:
        obj = json.loads(line)
    except (ValueError, TypeError):
        return []
    if not isinstance(obj, dict):
        return []

    kind = obj.get("type")
    events: list[dict[str, Any]] = []

    # Token ticks: shared parser recognises claude `result` and codex
    # `token_count` / `turn.completed`.
    usage = parse_event_line(line)
    if usage is not None:
        events.append(_tokens_event(usage.__dict__))

    if kind in ("assistant", "user"):
        message = obj.get("message")
        if kind == "assistant" and isinstance(message, dict):
            # Running per-turn total, ahead of the terminal `result` tick above
            # so the feed shows token usage while the run is still live.
            message_usage_event = _message_usage_event(message.get("usage"))
            if message_usage_event is not None:
                events.append(message_usage_event)
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, list):
            for block in content:
                block_event = _claude_block_event(block)
                if block_event is not None:
                    events.append(block_event)
    elif kind == "result":
        # claude's terminal event: the only place carrying the final answer
        # text for `claude --print --output-format stream-json` runs.
        text = obj.get("result")
        if isinstance(text, str) and text.strip():
            events.append({"kind": "message", "text": text.strip()})
    elif kind in ("item.started", "item.completed"):
        item_event = _codex_item_event(obj, str(kind))
        if item_event is not None:
            events.append(item_event)

    return events


def _tokens_event(usage: Mapping[str, Any]) -> dict[str, Any]:
    # Claude `result` and codex `token_count`/`turn.completed` (the events
    # `parse_event_line` recognises) all report the whole-run running total,
    # not a per-turn delta — the client must replace its total with this tick
    # rather than add it.
    return {
        "kind": "tokens",
        "cumulative": True,
        "input_tokens": int(usage.get("input_tokens", 0) or 0),
        "output_tokens": int(usage.get("output_tokens", 0) or 0),
        "cache_write_tokens": int(usage.get("cache_write_tokens", 0) or 0),
        "cache_read_tokens": int(usage.get("cache_read_tokens", 0) or 0),
        "cost_usd": float(usage.get("cost_usd", 0.0) or 0.0),
    }


def _message_usage_event(usage: object) -> dict[str, Any] | None:
    if not isinstance(usage, dict):
        return None
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    if input_tokens is None and output_tokens is None:
        return None
    # Claude's per-assistant-message usage is scoped to that one turn, ahead
    # of the terminal cumulative `result` tick above — the client must add it
    # to the running total rather than replace.
    return {
        "kind": "tokens",
        "cumulative": False,
        "input_tokens": int(input_tokens or 0),
        "output_tokens": int(output_tokens or 0),
        "cache_write_tokens": int(usage.get("cache_creation_input_tokens", 0) or 0),
        "cache_read_tokens": int(usage.get("cache_read_input_tokens", 0) or 0),
        "cost_usd": 0.0,
    }


def _claude_block_event(block: object) -> dict[str, Any] | None:
    if not isinstance(block, dict):
        return None
    btype = block.get("type")
    if btype == "text":
        text = block.get("text")
        if isinstance(text, str) and text.strip():
            return {"kind": "message", "text": text.strip()}
        return None
    if btype == "tool_use":
        name = str(block.get("name") or "tool")
        raw_input = block.get("input")
        tool_input: Mapping[str, Any] = raw_input if isinstance(raw_input, dict) else {}
        if name.casefold() in _FILE_EDIT_TOOLS:
            path = (
                tool_input.get("file_path")
                or tool_input.get("notebook_path")
                or tool_input.get("path")
            )
            files = [os.path.basename(str(path))] if path else []
            return {"kind": "file_edit", "tool": name, "files": files}
        return {"kind": "tool_call", "tool": name, "detail": _tool_detail(tool_input)}
    return None


def _tool_detail(tool_input: Mapping[str, Any]) -> str:
    for key in ("command", "pattern", "query", "file_path", "path", "url", "description"):
        value = tool_input.get(key)
        if isinstance(value, str) and value.strip():
            return sanitize_text(value)
    return ""


def _codex_item_event(obj: Mapping[str, Any], kind: str) -> dict[str, Any] | None:
    raw_item = obj.get("item")
    item: Mapping[str, Any] = raw_item if isinstance(raw_item, dict) else {}
    item_type = item.get("type") or item.get("item_type") or obj.get("item_type")
    if item_type == "command_execution" and kind == "item.started":
        return {"kind": "tool_call", "tool": "shell", "detail": _codex_command(item)}
    if item_type == "file_change" and kind == "item.completed":
        files = _codex_files(item)
        if files:
            return {"kind": "file_edit", "files": files}
        return None
    if item_type in ("agent_message", "assistant_message") and kind == "item.completed":
        text = item.get("text") or item.get("message")
        if isinstance(text, str) and text.strip():
            return {"kind": "message", "text": text.strip()}
    return None


def _codex_command(item: Mapping[str, Any]) -> str:
    value = item.get("command")
    if isinstance(value, str):
        return sanitize_text(value)
    if isinstance(value, list):
        return sanitize_text(" ".join(str(part) for part in value))
    return ""


def _codex_files(item: Mapping[str, Any]) -> list[str]:
    files: list[str] = []

    def add(raw: object) -> None:
        if isinstance(raw, str) and raw:
            base = os.path.basename(raw)
            if base and base not in files:
                files.append(base)

    for key in ("path", "file_path", "file"):
        add(item.get(key))
    changes = item.get("changes")
    if isinstance(changes, list):
        for change in changes:
            if isinstance(change, dict):
                add(change.get("path"))
    return files


def _ndjson(event: Mapping[str, Any]) -> str:
    return json.dumps(event, separators=(",", ":")) + "\n"


def create_live_stream_router(
    pool: ReadOnlyDbPool,
    *,
    log_root: Path,
    poll_interval_secs: float = _POLL_INTERVAL_SECS,
) -> APIRouter:
    """Mount ``GET /api/runs/{run_id}/stream`` tailing the run's live log."""
    router = APIRouter(prefix="/api")

    async def _run_status(run_id: str) -> str | None:
        conn = await pool.connection()
        cur = await conn.execute("SELECT status FROM runs WHERE id = ?", (run_id,))
        row = await cur.fetchone()
        return None if row is None else str(row["status"])

    @router.get("/runs/{run_id}/stream")
    async def stream_run(
        run_id: str,
        offset: int = Query(0, ge=0),
    ) -> StreamingResponse:
        log_path = log_root / f"{run_id}.log"
        status = await _run_status(run_id)
        # No `runs` row → genuinely unknown, even if an orphaned log file
        # exists on disk (DB reset/backfill mismatch, failed insert).
        if status is None:
            raise HTTPException(status_code=404, detail="Run not found")

        async def events() -> AsyncIterator[str]:
            pos = offset
            buffer = b""

            async def drain_once() -> AsyncIterator[str]:
                # Emits a `cursor` after each complete line (not just once per
                # poll) so a reconnect after a mid-batch drop never re-reads a
                # line the client already appended.
                nonlocal pos, buffer
                if not log_path.exists():
                    return
                chunk, new_pos = await asyncio.to_thread(_read_from, log_path, pos)
                buffer += chunk
                pos = new_pos
                while b"\n" in buffer:
                    raw, buffer = buffer.split(b"\n", 1)
                    for event in parse_stream_events(raw.decode(errors="replace")):
                        yield _ndjson(event)
                    # `pos` is the total bytes physically read; `buffer` now
                    # holds only what's left unconsumed (later lines plus a
                    # trailing partial line), so their difference is exactly
                    # this line's resumable boundary.
                    yield _ndjson({"kind": "cursor", "offset": pos - len(buffer)})

            while True:
                async for chunk in drain_once():
                    yield chunk
                current = await _run_status(run_id)
                if current not in LIVE_STATUSES:
                    # The run may have finished writing between the read above
                    # and this status check landing terminal — drain whatever
                    # arrived in that window before signalling `end`.
                    async for chunk in drain_once():
                        yield chunk
                    break
                await asyncio.sleep(poll_interval_secs)
            yield _ndjson({"kind": "end"})

        return StreamingResponse(events(), media_type="application/x-ndjson")

    return router


def _read_from(path: Path, pos: int) -> tuple[bytes, int]:
    with path.open("rb") as handle:
        handle.seek(pos)
        data = handle.read()
        return data, handle.tell()


__all__ = ["create_live_stream_router", "parse_stream_events"]
