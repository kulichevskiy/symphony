"""The Claude token dispenser (SYM-234).

One Claude connection backs every run, so rotating its access token
invalidates the token for all of them at once. Left ungoverned that is a
self-sustaining cascade: each run that dies on a rejected token triggers its
own revalidation, each revalidation rotates again, and each rotation kills more
live runs. SYM-229 removed the version of this that expired the shared row;
this removes the version that just keeps minting.

The dispenser decides by comparing generations (SYM-233) rather than secrets. A
run complains by naming the generation it was dispatched with:

  * naming the *current* generation means nobody has rotated since this run
    started — it is the first to notice, so mint once;
  * naming an *older* one means the rotation has already happened — hand back
    what is stored, exchange nothing.

Because requests are serialized, a burst of complaints on one generation
produces exactly one rotation and N-1 hand-outs.

Answering fast is part of the contract. The caller waits ~30s and that budget
is spent concurrently by every failing run, so only failures that can plausibly
clear on their own (5xx, 429, network faults) are retried, inside a budget well
short of the caller's; anything else is refused on the first answer. Silence is
the worst outcome — it costs every waiting run its full timeout before dying.

Abandoning an exchange is not free — the refresh token is one-shot, so a
timeout the server may already have honoured consumes it with nothing to show.
That is why a rejection *following* an abandoned attempt is refused rather than
treated as proof of a dead account: reading it the other way would black out
every run over a network blip.

What this is NOT, yet. Two older paths still rotate the same credential:
`_maybe_refresh_claude_credential` (proactive keep-fresh, SYM-227) and
`_revalidate_claude_after_auth_failure` (the post-mortem re-validate that
requeues a dead run, SYM-229). Both are the restart path SYM-236 explicitly
keeps — "this ticket removes a cost; it does not remove a guarantee" — so
retiring them belongs after the control channel has proven itself, not here.
Until then the guarantee that matters is narrower and enforced: all three share
one lock, so no two exchanges ever race the same one-shot refresh token.

Wiring the dispenser to the runs themselves is SYM-235 (the control channel)
and SYM-236 (the dispatch path). Here it exists, is held by the daemon, and
already owns the read side, so there is one definition of "the current token
and its generation".
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import aiosqlite
import httpx

from . import db
from .claude_login import (
    ClaudeRefreshOutcome,
    claude_access_token,
    claude_expires_at,
    refresh_claude_credential_outcome,
)
from .credentials import CredentialWriteBack
from .crypto import CredentialCipher, CredentialDecryptError, CredentialKeyMissingError

log = logging.getLogger(__name__)

_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# The caller gives up at ~30s. Stay far enough inside it that a refusal still
# reaches the run in time to be acted on, leaving headroom for the daemon's own
# queueing behind the shared refresh lock.
_ROTATION_BUDGET_SECS = 20.0
_RETRY_BACKOFF_SECS = 1.0
# Floor on a single attempt, so a budget already spent still buys one real try
# rather than a guaranteed timeout.
_MIN_ATTEMPT_SECS = 1.0

_PROVIDER = "claude"


@dataclass(frozen=True)
class TokenGrant:
    """A served request: the access token the run should switch to, the
    generation it belongs to (what the run re-stamps itself with, and what it
    will name if it has to complain again), and whether serving it cost a
    rotation."""

    token: str
    generation: int
    rotated: bool


@dataclass(frozen=True)
class TokenRefusal:
    """A request that cannot be served. `permanent` says whether asking again
    could plausibly help: `False` is a flaky token endpoint (the connection is
    untouched and still believed good), `True` is a dead end — the dispenser has
    already done whatever escalation the case warrants."""

    reason: str
    permanent: bool


TokenResponse = TokenGrant | TokenRefusal


@dataclass(frozen=True)
class _StoredConnection:
    credential: str
    generation: int
    status: str


class ClaudeTokenDispenser:
    """Reads the shared Claude access token out for dispatch, and mints a
    replacement for a run whose token was rejected mid-flight.

    Serialization is injectable because the daemon's own refreshes rotate the
    same one-shot refresh token — they must share a lock, or one exchange kills
    the credential the other just minted.
    """

    def __init__(
        self,
        conn: aiosqlite.Connection,
        cipher: CredentialCipher,
        write_back: CredentialWriteBack,
        *,
        lock: asyncio.Lock | None = None,
        now: Callable[[], datetime] | None = None,
        budget_secs: float = _ROTATION_BUDGET_SECS,
        retry_backoff_secs: float = _RETRY_BACKOFF_SECS,
    ) -> None:
        self._conn = conn
        self._cipher = cipher
        self._write_back = write_back
        self._lock = lock or asyncio.Lock()
        self._now = now or (lambda: datetime.now(UTC))
        self._budget_secs = budget_secs
        self._retry_backoff_secs = retry_backoff_secs

    async def snapshot(self) -> tuple[str, int] | None:
        """The stored credential and the generation it was minted under, read
        as one row (SYM-233) — reading them apart can stamp a run with a
        generation newer than the token it holds, which later reads as "this run
        has the freshest token" and provokes a rotation instead of a hand-out.

        `None` when Claude isn't UI-connected or the stored blob no longer
        decrypts; the run then falls back to ambient host auth. An `expired` row
        still yields its credential here, matching every other read path — the
        dispatch gate, not this, is what stops a run on an expired connection.
        """
        stored = await self._stored()
        return None if stored is None else (stored.credential, stored.generation)

    async def request(self, generation: int) -> TokenResponse:
        """Serve a run that found its token rejected, given the generation it
        was dispatched with. Rotates only if that generation is still current.

        The budget starts here, not at the exchange: the lock is shared with the
        daemon's own refreshes, so waiting for it is time the caller is spending
        too. Overrunning would answer after the caller has already given up —
        the silence this is built to avoid."""
        deadline = time.monotonic() + self._budget_secs
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=self._left(deadline))
        except TimeoutError:
            # Almost always benign: whoever holds the lock is minting the very
            # token this run wants, and the requeue picks it up.
            log.warning(
                "claude token request from a run holding generation %d timed out waiting "
                "for the rotation lock",
                generation,
            )
            return TokenRefusal("the claude token dispenser is busy", permanent=False)
        try:
            served = await self._serve(generation, deadline)
        finally:
            self._lock.release()
        if isinstance(served, TokenGrant):
            log.info(
                "claude token dispensed to a run holding generation %d: %s (current generation %d)",
                generation,
                "rotated" if served.rotated else "no rotation",
                served.generation,
            )
        else:
            log.warning(
                "claude token request from a run holding generation %d refused (%s): %s",
                generation,
                "permanent" if served.permanent else "retryable",
                served.reason,
            )
        return served

    def _left(self, deadline: float) -> float:
        """What's left of the budget, floored so an already-spent budget still
        buys one real attempt rather than a guaranteed timeout."""
        return max(deadline - time.monotonic(), _MIN_ATTEMPT_SECS)

    async def _serve(self, generation: int, deadline: float) -> TokenResponse:
        stored = await self._stored()
        if stored is None:
            return TokenRefusal("claude is not connected", permanent=True)
        if stored.status != "connected":
            # The reconnect gate is armed (`_claude_expired_block_reason`): only
            # an operator clears it, so a run still in flight must not rotate
            # the connection back to life behind their back.
            return TokenRefusal("the claude connection is expired", permanent=True)
        if generation != stored.generation:
            return await self._hand_out(stored)
        return await self._rotate(stored, deadline)

    async def _rotate(self, stored: _StoredConnection, deadline: float) -> TokenResponse:
        retried = False
        while True:
            outcome = await self._exchange(stored.credential, deadline)
            if outcome.credential is not None:
                return await self._persist(outcome.credential, expected_prior=stored.credential)
            # A reconnect (or the daemon's own refresh) that landed while our
            # doomed exchange was in flight wins — the connection is fine, the
            # credential we started from is simply no longer the stored one.
            replacement = await self._stored()
            if replacement is not None and replacement.credential != stored.credential:
                return await self._hand_out(replacement)
            if not outcome.transient:
                if retried:
                    # An earlier attempt was abandoned without an answer, so the
                    # server may well have honoured it and rotated — in which
                    # case this rejection means "already spent", not "dead
                    # account". Expiring here would black out the whole fleet on
                    # a network blip. Refuse instead; the next complaint starts
                    # clean and expires the row if it really is dead.
                    return TokenRefusal(
                        "the claude refresh token was rejected after an abandoned attempt",
                        permanent=False,
                    )
                return await self._expire("the claude refresh token was rejected")
            if time.monotonic() + self._retry_backoff_secs >= deadline:
                return TokenRefusal(
                    "the claude token endpoint could not be reached in time", permanent=False
                )
            retried = True
            await asyncio.sleep(self._retry_backoff_secs)

    async def _exchange(self, credential: str, deadline: float) -> ClaudeRefreshOutcome:
        """One token exchange, bounded by what is left of the budget rather than
        by the module's own 30s default — which is exactly the caller's timeout,
        so an exchange that runs to it is indistinguishable from silence."""
        async with httpx.AsyncClient(timeout=self._left(deadline)) as client:
            return await refresh_claude_credential_outcome(credential, client=client)

    async def _persist(self, refreshed: str, *, expected_prior: str) -> TokenResponse:
        """Write the freshly minted credential back under a compare-and-swap on
        the credential the exchange started from, then serve whatever the row
        ends up holding. A reconnect that landed mid-exchange therefore wins
        without a special case: the CAS no-ops and the operator's token is what
        gets handed out."""
        rotated = await self._write_back.write_back(
            _PROVIDER,
            refreshed,
            expires_at=claude_expires_at(refreshed),
            expected_prior=expected_prior,
        )
        stored = await self._stored()
        if stored is None:
            return TokenRefusal(
                "the claude connection was removed while its token was being rotated",
                permanent=True,
            )
        return await self._hand_out(stored, rotated=rotated)

    async def _hand_out(self, stored: _StoredConnection, *, rotated: bool = False) -> TokenResponse:
        """Serve what the row currently holds. A blob with no readable access
        token can't serve anyone, whatever generation was named — expire it so
        the Connections page says so instead of reading `connected` while every
        request quietly fails (the SYM-233 unreadable-blob rule)."""
        token = claude_access_token(stored.credential)
        if token is None:
            return await self._expire("the stored claude credential holds no access token")
        return TokenGrant(token=token, generation=stored.generation, rotated=rotated)

    async def _expire(self, reason: str) -> TokenRefusal:
        """Mark the shared connection expired. This *is* the escalation: it arms
        the reconnect dispatch gate, which parks the affected issues with an
        operator-facing "reconnect it on the Connections page" reason. Reserved
        for failures that cannot clear on their own — a transient one must never
        come through here, or one flaky exchange becomes a fleet-wide outage."""
        await db.oauth_connections.update_status(
            self._conn,
            provider=_PROVIDER,
            status="expired",
            updated_at=self._now().strftime(_ISO_FORMAT),
            updated_by="dispenser",
        )
        return TokenRefusal(reason, permanent=True)

    async def _stored(self) -> _StoredConnection | None:
        status = await db.oauth_connections.get_status(self._conn, _PROVIDER)
        if status is None or status.status not in ("connected", "expired"):
            return None
        try:
            snapshot = await db.oauth_connections.get_credential_and_generation(
                self._conn, _PROVIDER, self._cipher
            )
        except (CredentialDecryptError, CredentialKeyMissingError):
            return None
        if snapshot is None or not snapshot[0]:
            return None
        credential, generation = snapshot
        return _StoredConnection(credential=credential, generation=generation, status=status.status)
