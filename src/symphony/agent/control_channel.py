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
not look like an auth bug when it happened. `intercept()` reports whether it
swallowed the line, and the caller filters on that.

**The agent will not exit while stdin is open.** In stream-json input mode the
CLI waits for the next message indefinitely; only EOF ends it. So the channel
closes stdin as soon as the agent reports the turn finished. That is what turns
"keep stdin open for the run's lifetime" into a run that actually ends.

**A refusal must not cost the agent's whole retry window.** When the handler
declines — which for a token request is the dispenser's own Refusal surfacing
one layer up — the channel answers with an error and closes stdin rather than
going quiet, and raises `refused` so the caller can stop the run instead of
waiting for the agent to time out on its own.

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
# The agent says the turn is over. Claude: `result`. Codex: `turn.completed`,
# or `turn.failed`/`error` when the turn ends badly — an auth or API failure is
# still an ending, and leaving stdin open for one turns a terminal error the
# caller could have read into a silent stall.
_TURN_END_TYPES = frozenset({"result", "turn.completed", "turn.failed", "error"})

_REFUSAL_MESSAGE = "the host refused the request"
# How long a handler already under way gets to land before it is cancelled.
# Sized to the dispenser's own budget: a rotation cut off after the token
# endpoint answered, but before the replacement is stored, kills the shared
# credential for everyone.
_HANDLER_DRAIN_SECS = 25.0


@dataclass(frozen=True)
class ControlRequest:
    """One question the agent asked its host."""

    request_id: str
    subtype: str
    request: Mapping[str, object]


# Answers with the response payload, or None to refuse. Refusing is a normal
# outcome — an expired connection nobody can rotate is a refusal, not a crash.
ControlHandler = Callable[[ControlRequest], Awaitable[Mapping[str, object] | None]]


@dataclass(frozen=True)
class Conversation:
    """What turns a run from a monologue into a conversation.

    One field on the spec rather than two, because the prompt and the handler
    are never useful apart: a prompt on stdin with nobody to answer the
    questions it provokes is exactly the hang this mode exists to remove.
    """

    prompt: str
    handler: ControlHandler | None = None


@dataclass(frozen=True)
class AgentFrame:
    """What one line of the agent's stdout is, to a run holding a channel.

    `control` is the load-bearing bit: control traffic shares the pipe with the
    agent's own output and must be withheld from the run's events. `request` is
    the question to answer, absent on control frames that ask nothing of us.
    `turn_end` says the agent considers the turn finished, which is the cue to
    close stdin. `unanswerable` marks a request the agent is already blocked on
    and that nobody can address — the one case where silence would cost the run
    its whole stall window.
    """

    control: bool = False
    request: ControlRequest | None = None
    turn_end: bool = False
    unanswerable: bool = False


_AGENT_OUTPUT = AgentFrame()


def read_agent_frame(line: str) -> AgentFrame:
    """Classify one stdout line, decoding it exactly once.

    The single place that decides what counts as protocol traffic. Both the
    real channel and the harness's deterministic runner go through it, so the
    two cannot drift apart on the wire shape.
    """
    try:
        event = json.loads(line)
    except (ValueError, TypeError):
        return _AGENT_OUTPUT
    if not isinstance(event, dict):
        return _AGENT_OUTPUT
    kind = event.get("type")
    # A frozenset lookup on an unhashable value raises, and this runs inside
    # the stdout pump: one malformed frame would kill the pump, lose the rest
    # of the run's output and leave stdin open — a stall, not a parse error.
    if not isinstance(kind, str):
        return _AGENT_OUTPUT
    if kind not in _CONTROL_TYPES:
        return AgentFrame(turn_end=kind in _TURN_END_TYPES)
    if kind != "control_request":
        return AgentFrame(control=True)
    raw = event.get("request")
    request: Mapping[str, Any] = raw if isinstance(raw, dict) else {}
    # Shape observed in the SYM-232 spike: the id at the top level, the subtype
    # inside `request`. The id is read from either spot because a run that
    # cannot name the request it is answering simply dies.
    request_id = event.get("request_id") or request.get("request_id")
    if not isinstance(request_id, str):
        # The agent is waiting on a response we have no way to address. Saying
        # nothing costs the run its stall window; the channel ends it instead.
        log.warning("a control request arrived with no usable request_id: %.200s", line)
        return AgentFrame(control=True, unanswerable=True)
    subtype = request.get("subtype")
    if not isinstance(subtype, str):
        # Addressable but unintelligible. Hand it on anyway: the handler will
        # not recognise it, and its refusal is a prompt, correctly addressed
        # error rather than silence.
        log.warning("a control request arrived with no subtype: %.200s", line)
        subtype = ""
    return AgentFrame(
        control=True,
        request=ControlRequest(request_id=request_id, subtype=subtype, request=request),
    )


def _user_message(text: str) -> Mapping[str, object]:
    """The stream-json frame that carries a prompt to the agent."""
    return {
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
    }


def _success_response(request_id: str, payload: Mapping[str, object]) -> Mapping[str, object]:
    return {
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": dict(payload),
        },
    }


def _error_response(request_id: str, message: str) -> Mapping[str, object]:
    return {
        "type": "control_response",
        "response": {"subtype": "error", "request_id": request_id, "error": message},
    }


class ControlChannel:
    """The stdin side of one run, and the answers that travel down it.

    Feed every stdout line through `intercept()`; it returns True for the lines
    that belong to the protocol and must be withheld from the run's events.
    """

    def __init__(
        self,
        stdin: asyncio.StreamWriter,
        conversation: Conversation,
        *,
        run_id: str,
        handler_drain_secs: float = _HANDLER_DRAIN_SECS,
    ) -> None:
        self._stdin = stdin
        self._handler = conversation.handler
        self._run_id = run_id
        self._handler_drain_secs = handler_drain_secs
        self._tasks: set[asyncio.Task[None]] = set()
        # One request at a time. The agent can ask several times in a burst —
        # three in ~1.2s in the SYM-232 spike — and running those handlers side
        # by side means a refusal cannot stop the ones already under way. The
        # dispenser serializes internally anyway, so this costs nothing.
        self._answering = asyncio.Lock()
        self._turn_over = False
        # Raised once a request has been declined. The runner watches it and
        # ends the run rather than letting the agent wait out its retry window.
        self.refused = asyncio.Event()

    async def send_prompt(self, text: str) -> None:
        """Open the conversation. A prompt that does not land ends the run.

        A child that has closed its read end but is still alive would otherwise
        never hear the prompt, never answer, and never finish — the run would
        sit there until the stall watchdog noticed, minutes later."""
        if not await self._write(_user_message(text)):
            await self._refuse(None, "the prompt could not be delivered")

    def intercept(self, line: str) -> bool:
        """Take one stdout line. True when it was protocol traffic.

        Also notices the agent's end-of-turn marker and closes stdin, since an
        agent whose stdin stays open never exits on its own.
        """
        frame = read_agent_frame(line)
        if frame.turn_end:
            self._end_turn()
        if frame.request is None and frame.unanswerable:
            self._spawn(self._refuse(None, "the request could not be understood"))
        elif frame.request is not None:
            if self._turn_over:
                # stdin is shut: whatever the handler produced could not be
                # handed over. That matters because the handler has side
                # effects — the dispenser would burn a single-use refresh token
                # minting an answer with nowhere to go. Still withheld from the
                # event stream; simply not acted on.
                log.warning(
                    "not answering %s for run_id=%s: the channel is already closed",
                    frame.request.subtype,
                    self._run_id,
                )
            else:
                self._spawn(self._answer(frame.request))
        return frame.control

    async def aclose(self) -> None:
        """Let an in-flight handler land, then close stdin. Idempotent.

        Cancelling a handler is not free. The dispenser may already have spent
        the shared connection's single-use refresh token; killing it before it
        can persist the replacement leaves the stored credential unusable for
        every later run, and only an operator reconnect brings it back. So an
        in-flight handler is given its budget to finish, and cancelled only if
        it overruns."""
        tasks = list(self._tasks)
        if tasks:
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    # Shielded: the timeout must cancel the wait, not the work.
                    asyncio.shield(asyncio.gather(*tasks, return_exceptions=True)),
                    timeout=self._handler_drain_secs,
                )
            unfinished = [t for t in tasks if not t.done()]
            if unfinished:
                log.warning(
                    "cancelling %d control handler(s) for run_id=%s after %.0fs",
                    len(unfinished),
                    self._run_id,
                    self._handler_drain_secs,
                )
                for task in unfinished:
                    task.cancel()
                await asyncio.gather(*unfinished, return_exceptions=True)
        self._close_stdin()

    # --- internals ---------------------------------------------------------

    async def _answer(self, request: ControlRequest) -> None:
        async with self._answering:
            if self._turn_over:
                # A sibling request refused while this one waited its turn.
                # Nothing can reach the agent now, and asking the handler
                # anyway would spend a single-use refresh token on an answer
                # with nowhere to go.
                log.warning(
                    "not answering %s for run_id=%s: the channel closed while it queued",
                    request.subtype,
                    self._run_id,
                )
                return
            await self._answer_now(request)

    async def _answer_now(self, request: ControlRequest) -> None:
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
            await self._refuse(request.request_id, _REFUSAL_MESSAGE)
            return
        if not await self._write(_success_response(request.request_id, payload)):
            # The answer exists but could not be handed over — an unserializable
            # payload, a child that died mid-write. The agent is still blocked,
            # so this is a refusal like any other, not something to log and walk
            # away from.
            await self._refuse(request.request_id, "the answer could not be delivered")
            return
        log.info("answered control request %s for run_id=%s", request.subtype, self._run_id)

    async def _refuse(self, request_id: str | None, reason: str) -> None:
        """Decline, tell the agent so, and stop the run waiting on us."""
        log.warning("refusing a control request for run_id=%s: %s", self._run_id, reason)
        if request_id is not None:
            await self._write(_error_response(request_id, reason))
        # Nothing better is coming, so don't leave the agent listening.
        self._end_turn()
        self.refused.set()

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

    async def _write(self, frame: Mapping[str, object]) -> bool:
        """Put one frame on the agent's stdin. False if it did not get there.

        Callers must act on False: a frame the agent never received leaves it
        waiting, and waiting costs the run its whole stall window."""
        if self._stdin.is_closing():
            log.warning(
                "dropping a %s frame for run_id=%s: stdin is closed", frame["type"], self._run_id
            )
            return False
        try:
            payload = (json.dumps(frame) + "\n").encode()
        except (TypeError, ValueError):
            log.exception(
                "could not serialize a %s frame for run_id=%s", frame["type"], self._run_id
            )
            return False
        try:
            self._stdin.write(payload)
            await self._stdin.drain()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — the child may have died mid-write
            log.warning("could not write to run_id=%s stdin: %s", self._run_id, e)
            return False
        return True

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
