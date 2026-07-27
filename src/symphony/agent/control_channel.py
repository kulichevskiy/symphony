"""The runner's half of the agent CLI's control protocol.

A run in control-channel mode is a conversation rather than a monologue. The
prompt reaches the agent as a message on stdin, stdin stays open, and when the
agent needs something only its host can supply — a fresh access token, today —
it asks for it as a `control_request` frame on stdout and waits for a
`control_response` frame to come back on stdin.

`ControlChannel` owns that exchange and nothing else. It knows the wire shapes;
what a request *means*, and what a good answer looks like, live in the handler
the caller supplies. That split is the point of SYM-235: the runner must not
learn what a token is.

Three hazards shape the interface.

**Control frames must never reach the run's event stream.** They share a pipe
with the agent's own output, so a run that forwarded them would corrupt
completion markers, cost accounting and verdict parsing at once — and it would
not look like an auth bug when it happened. `observe()` reports whether it
swallowed the line, and the caller filters on that.

**The agent will not exit while stdin is open.** In stream-json input mode the
CLI waits for the next message indefinitely; only EOF ends it. So the channel
closes stdin as soon as the agent reports the turn finished. That is what turns
"keep stdin open for the run's lifetime" into a run that actually ends.

**A refusal must not cost the agent's whole retry window.** When the handler
declines, the channel answers with an error and closes stdin rather than going
quiet, and raises `refused` so the caller can stop the run instead of waiting
for the agent to time out on its own.

Handlers run as their own tasks rather than inline: one recovery produces
several requests in quick succession (observed in the SYM-232 spike) and a
handler may legitimately take tens of seconds, so blocking the stdout pump on
one would back the agent's output up behind it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)

# Protocol traffic on the agent's stdout. None of it is agent output.
_CONTROL_TYPES = frozenset({"control_request", "control_response", "control_cancel_request"})
# The agent says the turn is over: claude's `result`, codex's `turn.completed`.
_TURN_END_TYPES = frozenset({"result", "turn.completed"})

_REFUSAL_MESSAGE = "the host refused the request"


@dataclass(frozen=True)
class ControlRequest:
    """One question the agent asked its host."""

    request_id: str
    subtype: str
    request: Mapping[str, object]


# Answers with the response payload, or None to refuse. Refusing is a normal
# outcome — an expired connection nobody can rotate is a refusal, not a crash.
ControlHandler = Callable[[ControlRequest], Awaitable[Mapping[str, object] | None]]


def _decode(line: str) -> Mapping[str, Any] | None:
    try:
        event = json.loads(line)
    except (ValueError, TypeError):
        return None
    return event if isinstance(event, dict) else None


def is_control_frame(line: str) -> bool:
    """True when `line` is protocol traffic rather than agent output."""
    event = _decode(line)
    return event is not None and event.get("type") in _CONTROL_TYPES


def read_control_request(line: str) -> ControlRequest | None:
    """The control request `line` carries, or None if it carries none.

    Shared with the harness's deterministic runner so the two cannot drift
    apart on the wire shape.
    """
    event = _decode(line)
    if event is None or event.get("type") != "control_request":
        return None
    raw = event.get("request")
    request: Mapping[str, Any] = raw if isinstance(raw, dict) else {}
    request_id = event.get("request_id") or request.get("request_id")
    subtype = request.get("subtype") or event.get("subtype")
    if not isinstance(request_id, str) or not isinstance(subtype, str):
        log.warning("ignoring a control request with no request_id/subtype: %.200s", line)
        return None
    return ControlRequest(request_id=request_id, subtype=subtype, request=request)


def user_message(text: str) -> Mapping[str, object]:
    """The stream-json frame that carries a prompt to the agent."""
    return {
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
    }


def success_response(request_id: str, payload: Mapping[str, object]) -> Mapping[str, object]:
    return {
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": dict(payload),
        },
    }


def error_response(request_id: str, message: str) -> Mapping[str, object]:
    return {
        "type": "control_response",
        "response": {"subtype": "error", "request_id": request_id, "error": message},
    }


class ControlChannel:
    """The stdin side of one run, and the answers that travel down it.

    Feed every stdout line through `observe()`; it returns True for the lines
    that belong to the protocol and must be withheld from the run's events.
    """

    def __init__(
        self,
        stdin: asyncio.StreamWriter,
        handler: ControlHandler | None,
        *,
        run_id: str,
    ) -> None:
        self._stdin = stdin
        self._handler = handler
        self._run_id = run_id
        self._tasks: set[asyncio.Task[None]] = set()
        self._turn_over = False
        # Raised once a request has been declined. The runner watches it and
        # ends the run rather than letting the agent wait out its retry window.
        self.refused = asyncio.Event()

    async def send_prompt(self, text: str) -> None:
        await self._write(user_message(text))

    def observe(self, line: str) -> bool:
        """Take one stdout line. True when it was protocol traffic.

        Also notices the agent's end-of-turn marker and closes stdin, since an
        agent whose stdin stays open never exits on its own.
        """
        event = _decode(line)
        if event is None:
            return False
        kind = event.get("type")
        if kind not in _CONTROL_TYPES:
            if kind in _TURN_END_TYPES:
                self._end_turn()
            return False
        request = read_control_request(line)
        if request is not None:
            self._spawn(self._answer(request))
        return True

    async def aclose(self) -> None:
        """Abandon any in-flight handler and close stdin. Idempotent."""
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._close_stdin()

    # --- internals ---------------------------------------------------------

    async def _answer(self, request: ControlRequest) -> None:
        payload: Mapping[str, object] | None = None
        if self._handler is None:
            log.warning(
                "run_id=%s asked for %s but the run carries no control handler",
                self._run_id,
                request.subtype,
            )
        else:
            try:
                payload = await self._handler(request)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — a broken handler is a refusal, not a crash
                log.exception(
                    "control handler failed for run_id=%s request=%s",
                    self._run_id,
                    request.subtype,
                )
        if payload is None:
            log.warning("refusing control request %s for run_id=%s", request.subtype, self._run_id)
            await self._write(error_response(request.request_id, _REFUSAL_MESSAGE))
            # Nothing better is coming, so don't leave the agent listening.
            self._end_turn()
            self.refused.set()
            return
        log.info("answered control request %s for run_id=%s", request.subtype, self._run_id)
        await self._write(success_response(request.request_id, payload))

    def _end_turn(self) -> None:
        if self._turn_over:
            return
        self._turn_over = True
        self._spawn(self._drain_then_close())

    async def _drain_then_close(self) -> None:
        with suppress(Exception):
            await self._stdin.drain()
        self._close_stdin()

    def _close_stdin(self) -> None:
        with suppress(Exception):
            if not self._stdin.is_closing():
                self._stdin.close()

    async def _write(self, frame: Mapping[str, object]) -> None:
        if self._stdin.is_closing():
            log.warning(
                "dropping a %s frame for run_id=%s: stdin is closed", frame["type"], self._run_id
            )
            return
        try:
            self._stdin.write((json.dumps(frame) + "\n").encode())
            await self._stdin.drain()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — the child may have died mid-write
            log.warning("could not write to run_id=%s stdin: %s", self._run_id, e)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
