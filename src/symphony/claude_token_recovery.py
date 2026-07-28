"""Answering a run whose Claude token was rejected mid-flight (SYM-236).

The dispenser (SYM-234) decides what a complaining run should be given; the
control channel (SYM-235) carries the question and the answer. This is the
piece between them: it turns one `control_request` into one dispenser request,
remembers what the run now holds, and counts what actually happened.

The count is not decoration. There is no config flag behind this mechanism —
the deliberate decision was that the safety net is the existing restart path,
not a switch — so the only way to tell a working recovery from one that has
never been exercised is that a run says how many times it recovered.

A refusal is a normal outcome, not an error: the channel turns it into a
correctly addressed error frame and ends the run, which is exactly what
happens today when a token goes bad mid-run. This ticket removes a cost; it
does not remove a guarantee.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping

from .agent.control_channel import ControlRequest, Decline
from .claude_token_dispenser import ClaudeTokenDispenser, TokenGrant

log = logging.getLogger(__name__)

# The only question this host knows how to answer. The subtype and the field
# the CLI reads the answer out of are both wire shapes observed end-to-end in
# the SYM-232 spike.
OAUTH_TOKEN_REFRESH = "oauth_token_refresh"
_ACCESS_TOKEN_FIELD = "accessToken"

# Records the generation the run now holds. Injected rather than written here
# so the stamp keeps one definition — and one best-effort guard — with the one
# the dispatch path already makes.
Restamp = Callable[[int], Awaitable[None]]


class ClaudeTokenRecovery:
    """One run's side of mid-run token recovery: a `ControlHandler` that hands
    back a fresh access token, or refuses.

    Stateful on purpose. The generation it names when complaining has to be the
    one it currently holds, not the one it was dispatched with: a run that
    recovers, works on, and is rejected again would otherwise name a
    superseded generation and be handed back the same token that just failed.
    """

    def __init__(
        self,
        dispenser: ClaudeTokenDispenser,
        *,
        run_id: str,
        generation: int,
        restamp: Restamp,
    ) -> None:
        self._dispenser = dispenser
        self._run_id = run_id
        self._generation = generation
        self._restamp = restamp
        self._count = 0
        self._refused_retryably = False

    @property
    def refused_retryably(self) -> bool:
        """Whether this run was refused a replacement for a reason that could
        clear on its own — a busy dispenser, an unreachable token endpoint.

        The connection is untouched and still believed good, so the run should
        come back rather than park. That distinction is the dispenser's own
        (`TokenRefusal.permanent`); it is surfaced here because by the time the
        run ends, the refusal is the only thing that explains why."""
        return self._refused_retryably

    def log_tally(self) -> None:
        """Say what recovery did for this run, once it has ended.

        Silent when it did nothing: an armed run that never needed a
        replacement is the common case and would drown the signal. This line is
        the only evidence the mechanism works at all — a run that recovers and
        finishes otherwise looks exactly like a run that was never at risk."""
        if self._count:
            log.info(
                "run_id=%s finished after %d mid-run claude token recovery(ies)",
                self._run_id,
                self._count,
            )

    async def __call__(self, request: ControlRequest) -> Mapping[str, object] | Decline | None:
        if request.subtype != OAUTH_TOKEN_REFRESH:
            # Declined, not refused. The CLI's control vocabulary is far wider
            # than the one question this host advertises, and killing a healthy
            # run over an unrecognised one would be a worse bug than the one
            # this ticket fixes. The agent gets a proper error and decides.
            log.warning(
                "run_id=%s asked its host for %r, which this host does not answer",
                self._run_id,
                request.subtype,
            )
            return Decline("the host does not answer that request")
        served = await self._dispenser.request(self._generation)
        if not isinstance(served, TokenGrant):
            log.warning(
                "run_id=%s could not recover its claude token (%s): %s",
                self._run_id,
                "permanent" if served.permanent else "retryable",
                served.reason,
            )
            self._refused_retryably = not served.permanent
            return None
        self._count += 1
        self._generation = served.generation
        await self._restamp(served.generation)
        log.info(
            "run_id=%s recovered its claude token mid-run: recovery #%d, now on generation %d (%s)",
            self._run_id,
            self._count,
            served.generation,
            "rotated" if served.rotated else "handed out",
        )
        return {_ACCESS_TOKEN_FIELD: served.token}


__all__ = ["OAUTH_TOKEN_REFRESH", "ClaudeTokenRecovery"]
