"""SYM-234: the Claude token dispenser — on-demand rotation of the shared
access token, and the decision rule that keeps one rotation from cascading into
a fleet-wide outage.

Every run authenticates against a single Claude connection, so a rotation
invalidates the token for everyone at once. Left to itself, each dying run
revalidates, each revalidation rotates, and each rotation kills more live runs.
The rule that breaks the cycle is a compare-and-swap on the generation: rotate
only when the complainant still names the current one. The first complaint
mints exactly one token; everyone arriving afterwards names a superseded
generation and is simply handed what is already stored.

The dispenser also has to answer *fast* — its caller waits ~30s, and that
budget is spent concurrently by every failing run — so a failure that cannot
clear on its own is refused immediately rather than retried into silence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path

import aiosqlite
import httpx
import pytest
import respx

from symphony import db
from symphony.claude_login import CLAUDE_OAUTH_TOKEN_URL
from symphony.claude_token_dispenser import ClaudeTokenDispenser, TokenGrant, TokenRefusal
from symphony.config import Config, LinearStates, RepoBinding
from symphony.credentials import CredentialWriteBack
from symphony.crypto import CredentialCipher
from tests.harness import Harness

_KEY = "deployment-secret"


def _cred(access: str, *, refresh: str = "rt-1") -> str:
    return json.dumps(
        {
            "claudeAiOauth": {
                "accessToken": access,
                "refreshToken": refresh,
                "expiresAt": 4102444800000,
            }
        }
    )


def _minted(access: str, refresh: str = "rt-2") -> httpx.Response:
    return httpx.Response(
        200, json={"access_token": access, "refresh_token": refresh, "expires_in": 28800}
    )


async def _store(conn: aiosqlite.Connection, credential: str, *, status: str = "connected") -> None:
    await db.oauth_connections.set_connection(
        conn,
        provider="claude",
        credential=credential,
        cipher=CredentialCipher(_KEY),
        status=status,
    )


async def _open(
    tmp_path: Path, **kwargs: float
) -> tuple[aiosqlite.Connection, ClaudeTokenDispenser]:
    """A migrated store plus a dispenser over it. Budget/backoff are overridable
    so the retry tests don't spend the production budget in real seconds."""
    conn = await db.connect(tmp_path / "state.sqlite")
    cipher = CredentialCipher(_KEY)
    return conn, ClaudeTokenDispenser(conn, cipher, CredentialWriteBack(conn, cipher), **kwargs)  # type: ignore[arg-type]


async def _stored_token(conn: aiosqlite.Connection) -> str:
    blob = await db.oauth_connections.get_credential(conn, "claude", CredentialCipher(_KEY))
    assert blob is not None
    return str(json.loads(blob)["claudeAiOauth"]["accessToken"])


@pytest.mark.asyncio
@respx.mock
async def test_a_complaint_naming_the_current_generation_mints_one_token(tmp_path: Path) -> None:
    """The first run to notice its token is dead is the one that pays for the
    rotation: exactly one exchange, and the new token is persisted so every
    later request is served from the store."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=_minted("tok-new"))
    conn, dispenser = await _open(tmp_path)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert served == TokenGrant(token="tok-new", generation=2, rotated=True)
        assert route.call_count == 1
        assert await _stored_token(conn) == "tok-new"
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "connected"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_concurrent_complaints_on_one_generation_share_a_single_rotation(
    tmp_path: Path,
) -> None:
    """The cascade this ticket exists to stop: three runs die on the same token
    at the same moment. One rotation happens, all three are served it."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=_minted("tok-new"))
    conn, dispenser = await _open(tmp_path)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await asyncio.gather(*(dispenser.request(1) for _ in range(3)))

        assert route.call_count == 1
        assert [s.token for s in served] == ["tok-new"] * 3
        assert [s.generation for s in served] == [2] * 3
        # Exactly one of them did the minting; the others rode along.
        assert [s.rotated for s in served].count(True) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_complaint_naming_an_older_generation_is_served_without_rotating(
    tmp_path: Path,
) -> None:
    """A run still holding a superseded token doesn't need a new one minted —
    it needs the one everybody else already has."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=_minted("tok-unwanted"))
    conn, dispenser = await _open(tmp_path)
    try:
        await _store(conn, _cred("tok-v0"))
        await _store(conn, _cred("tok-v1"))

        served = await dispenser.request(1)

        assert served == TokenGrant(token="tok-v1", generation=2, rotated=False)
        assert route.call_count == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_transient_failure_is_retried_inside_the_budget(tmp_path: Path) -> None:
    """A 5xx says nothing about the credential — it's the endpoint having a bad
    moment, and a moment is shorter than the budget."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        side_effect=[
            httpx.Response(503, json={}),
            httpx.Response(429, json={}),
            _minted("tok-new"),
        ]
    )
    conn, dispenser = await _open(tmp_path, budget_secs=30.0, retry_backoff_secs=0.01)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert served == TokenGrant(token="tok-new", generation=2, rotated=True)
        assert route.call_count == 3
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_an_exhausted_budget_refuses_without_expiring_the_connection(
    tmp_path: Path,
) -> None:
    """When the budget runs out the dispenser answers anyway. A flaky endpoint
    is not evidence of a dead account, so the shared row stays connected — the
    fleet-wide outage SYM-229 removed must not come back through this door."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(500, json={}))
    conn, dispenser = await _open(tmp_path, budget_secs=0.0)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert isinstance(served, TokenRefusal)
        assert served.permanent is False
        assert route.call_count == 1
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "connected"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_an_exchange_is_bounded_by_the_budget_not_by_the_callers_timeout(
    tmp_path: Path,
) -> None:
    """The default exchange timeout is 30s — the same 30s the caller waits, so an
    exchange that runs to it is indistinguishable from silence and costs every
    waiting run its full wait. Each attempt is capped by what's left instead."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=_minted("tok-new"))
    conn, dispenser = await _open(tmp_path, budget_secs=5.0)
    try:
        await _store(conn, _cred("tok-v0"))

        await dispenser.request(1)

        read_timeout = route.calls[0].request.extensions["timeout"]["read"]
        assert 0 < read_timeout <= 5.0
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_rejection_following_an_abandoned_attempt_does_not_expire_the_row(
    tmp_path: Path,
) -> None:
    """An attempt abandoned without an answer may still have been honoured — the
    refresh token is one-shot, so the retry can be rejected as *already spent*
    rather than dead. Blacking out the fleet on that reading is exactly the
    SYM-229 outage; refuse instead and let the next complaint start clean."""
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        side_effect=[httpx.Response(503, json={}), httpx.Response(400, json={})]
    )
    conn, dispenser = await _open(tmp_path, budget_secs=30.0, retry_backoff_secs=0.01)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert isinstance(served, TokenRefusal)
        assert served.permanent is False
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "connected"

        # A clean rejection on the next complaint is unambiguous, and expires it.
        respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(400, json={}))
        again = await dispenser.request(1)
        assert isinstance(again, TokenRefusal)
        assert again.permanent is True
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_an_unrecoverable_failure_is_refused_at_once_and_expires_the_row(
    tmp_path: Path,
) -> None:
    """A rejected refresh token cannot clear on its own. Retrying it would spend
    the caller's whole budget to learn nothing, so it is refused on the first
    answer — and the connection is marked expired, which is what arms the
    reconnect gate and calls the operator."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(400, json={"error": "invalid_grant"})
    )
    conn, dispenser = await _open(tmp_path, budget_secs=30.0, retry_backoff_secs=0.01)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert isinstance(served, TokenRefusal)
        assert served.permanent is True
        assert route.call_count == 1
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "expired"
        assert status.updated_by == "dispenser"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_reconnect_landing_mid_rotation_is_not_overwritten(tmp_path: Path) -> None:
    """An operator reconnect that lands while the token exchange is in flight
    wins: the rotation was derived from a credential that is no longer stored,
    so it is dropped and the complainant is served the operator's token."""
    conn, dispenser = await _open(tmp_path)

    async def _reconnect_mid_flight(request: httpx.Request) -> httpx.Response:
        await _store(conn, _cred("tok-operator", refresh="rt-operator"))
        return _minted("tok-rotated")

    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(side_effect=_reconnect_mid_flight)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert served == TokenGrant(token="tok-operator", generation=2, rotated=False)
        assert await _stored_token(conn) == "tok-operator"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_an_expiry_landing_mid_rotation_is_not_cleared_by_the_write_back(
    tmp_path: Path,
) -> None:
    """A liveness Test (or any other path) that expires the row while the
    exchange is in flight leaves the credential untouched, so a
    credential-only CAS would sail through — and `write_back` re-persists as
    `connected`, silently clearing a reconnect gate that was just armed. The
    write refuses inside the statement and the minted token is dropped."""
    conn, dispenser = await _open(tmp_path)

    async def _expire_mid_flight(request: httpx.Request) -> httpx.Response:
        await db.oauth_connections.update_status(conn, provider="claude", status="expired")
        return _minted("tok-rotated")

    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(side_effect=_expire_mid_flight)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert isinstance(served, TokenRefusal)
        assert served.permanent is True
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "expired"
        assert await _stored_token(conn) == "tok-v0"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_the_write_back_cas_is_enforced_inside_the_statement(tmp_path: Path) -> None:
    """The gate check and the write are separate awaits, so a liveness Test can
    land between them. The condition therefore travels into the SQL: the write
    lands only while the row is still the connected generation it was decided
    about."""
    conn = await db.connect(tmp_path / "state.sqlite")
    cipher = CredentialCipher(_KEY)
    write_back = CredentialWriteBack(conn, cipher)
    try:
        await _store(conn, _cred("tok-v0"))
        await db.oauth_connections.update_status(conn, provider="claude", status="expired")

        # An expired row would normally be re-persisted as `connected`; guarded,
        # it is refused instead.
        assert (
            await write_back.write_back("claude", _cred("tok-new"), expect_connected_generation=1)
            is False
        )
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "expired"
        assert await _stored_token(conn) == "tok-v0"

        # A superseded generation is refused too, even while connected.
        await _store(conn, _cred("tok-operator"))
        assert (
            await write_back.write_back("claude", _cred("tok-new"), expect_connected_generation=1)
            is False
        )
        assert await _stored_token(conn) == "tok-operator"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_a_guarded_write_never_recreates_a_disconnected_connection(tmp_path: Path) -> None:
    """A guarded write means "replace the row I decided about", never "create
    one". An upsert would take the INSERT path once a Disconnect removed the row
    — nothing left to conflict with, so the guard on the update never applies —
    and resurrect as `connected` the credential the operator just deleted."""
    conn = await db.connect(tmp_path / "state.sqlite")
    cipher = CredentialCipher(_KEY)
    try:
        await _store(conn, _cred("tok-v0"))
        await db.oauth_connections.delete(conn, "claude")

        wrote = await db.oauth_connections.set_connection(
            conn,
            provider="claude",
            credential=_cred("tok-rotated"),
            cipher=cipher,
            expect_connected_generation=1,
        )

        assert wrote is False
        assert await db.oauth_connections.get_status(conn, "claude") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_disconnect_landing_mid_rotation_is_not_undone(tmp_path: Path) -> None:
    """The same race end to end: the operator disconnects while the exchange is
    in flight, and the rotation must not bring the connection back."""
    conn, dispenser = await _open(tmp_path)

    async def _disconnect_mid_flight(request: httpx.Request) -> httpx.Response:
        await db.oauth_connections.delete(conn, "claude")
        return _minted("tok-rotated")

    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(side_effect=_disconnect_mid_flight)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert isinstance(served, TokenRefusal)
        assert served.permanent is True
        assert await db.oauth_connections.get_status(conn, "claude") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_request_timeout_is_retried_rather_than_read_as_a_dead_account(
    tmp_path: Path,
) -> None:
    """408 is the one 4xx that says nothing about the refresh token — something
    gave up waiting for the request. Reading it as a rejection would expire the
    shared connection and block every run over a slow network."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        side_effect=[httpx.Response(408, json={}), _minted("tok-new")]
    )
    conn, dispenser = await _open(tmp_path, budget_secs=30.0, retry_backoff_secs=0.01)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert served == TokenGrant(token="tok-new", generation=2, rotated=True)
        assert route.call_count == 2
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_slow_exchange_is_abandoned_at_the_deadline(tmp_path: Path) -> None:
    """An httpx timeout applies per operation — connect, write, pool, each read
    — so a slow-but-progressing response outlives all of them and still overruns
    the budget. The absolute deadline is what actually holds; overrunning would
    answer after the caller has given up, which is the silence this exists to
    avoid."""

    async def _never_finishes(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(30)
        return _minted("tok-new")

    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(side_effect=_never_finishes)
    conn, dispenser = await _open(tmp_path, budget_secs=0.2, retry_backoff_secs=0.01)
    try:
        await _store(conn, _cred("tok-v0"))

        started = time.monotonic()
        served = await dispenser.request(1)
        elapsed = time.monotonic() - started

        assert isinstance(served, TokenRefusal)
        # Abandoned, never answered — so the connection must not be expired.
        assert served.permanent is False
        assert elapsed < 10
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "connected"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_expiring_is_guarded_by_the_generation_the_verdict_was_reached_about(
    tmp_path: Path,
) -> None:
    """The escalation write is compare-and-set on the generation. An operator
    reconnect landing between the verdict and the write must not be expired by
    a judgement passed on the credential it replaced — that re-arms the
    fleet-wide gate seconds after the reconnect cleared it."""
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _store(conn, _cred("tok-v0"))

        # The reconnect lands first; the stale verdict then no-ops.
        await _store(conn, _cred("tok-operator"))
        await db.oauth_connections.update_status(
            conn, provider="claude", status="expired", updated_by="dispenser", expected_generation=1
        )
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "connected"

        # A verdict about the generation actually stored still lands.
        await db.oauth_connections.update_status(
            conn, provider="claude", status="expired", updated_by="dispenser", expected_generation=2
        )
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_malformed_success_body_is_refused_rather_than_raised(tmp_path: Path) -> None:
    """A 200 carrying valid-but-unexpected JSON must still produce an answer.
    Raising out of `request` would reach the control-channel caller as silence,
    and it waits out its whole timeout before the run dies."""
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(200, json=[]))
    conn, dispenser = await _open(tmp_path)
    try:
        await _store(conn, _cred("tok-v0"))

        served = await dispenser.request(1)

        assert isinstance(served, TokenRefusal)
        assert served.permanent is True
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_an_expired_or_absent_connection_is_refused_rather_than_resurrected(
    tmp_path: Path,
) -> None:
    """Once the row is expired the reconnect gate is armed and only an operator
    clears it — a still-running run must not rotate it back to life. A provider
    that was never connected has no token to dispense at all. Both are refused
    permanently so the run dies and requeues instead of waiting out its budget."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=_minted("tok-new"))
    conn, dispenser = await _open(tmp_path)
    try:
        never_connected = await dispenser.request(1)
        assert isinstance(never_connected, TokenRefusal)
        assert never_connected.permanent is True

        await _store(conn, _cred("tok-v0"))
        await db.oauth_connections.update_status(conn, provider="claude", status="expired")

        gated = await dispenser.request(1)
        assert isinstance(gated, TokenRefusal)
        assert gated.permanent is True
        assert route.call_count == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_an_unreadable_credential_is_refused_and_expires_the_row(tmp_path: Path) -> None:
    """A stored blob with no access token in it cannot serve anyone, whatever
    generation is named — expire it so the Connections page says so instead of
    reading `connected` while every request quietly fails."""
    conn, dispenser = await _open(tmp_path)
    try:
        await _store(conn, "not json at all")

        served = await dispenser.request(7)

        assert isinstance(served, TokenRefusal)
        assert served.permanent is True
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None and status.status == "expired"
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_every_served_request_names_its_generation_and_rotation_outcome(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Without this we cannot tell a mechanism that works from one that was
    never exercised — there is no config flag to read the answer off."""
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=_minted("tok-new"))
    conn, dispenser = await _open(tmp_path)
    try:
        await _store(conn, _cred("tok-v0"))
        with caplog.at_level(logging.INFO, logger="symphony.claude_token_dispenser"):
            await dispenser.request(1)
            await dispenser.request(1)

        served = [
            r.getMessage() for r in caplog.records if r.name == "symphony.claude_token_dispenser"
        ]
        assert len(served) == 2
        assert "generation 1" in served[0] and "rotated" in served[0]
        assert "generation 1" in served[1] and "no rotation" in served[1]
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_waiting_out_the_budget_on_the_lock_refuses_instead_of_hanging(
    tmp_path: Path,
) -> None:
    """The lock is shared with the daemon's own refreshes, so waiting on it
    spends the caller's budget just as an exchange does. A holder that outlasts
    the budget gets an answer, not silence — the run requeues and picks up
    whatever the holder minted."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=_minted("tok-new"))
    lock = asyncio.Lock()
    conn = await db.connect(tmp_path / "state.sqlite")
    cipher = CredentialCipher(_KEY)
    dispenser = ClaudeTokenDispenser(
        conn, cipher, CredentialWriteBack(conn, cipher), lock=lock, budget_secs=0.05
    )
    try:
        await _store(conn, _cred("tok-v0"))
        async with lock:
            served = await dispenser.request(1)

        assert isinstance(served, TokenRefusal)
        assert served.permanent is False
        assert route.call_count == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_the_daemon_holds_one_dispenser_serialized_with_its_own_refreshes(
    tmp_path: Path,
) -> None:
    """A mid-run rotation must not run beside the daemon's proactive keep-fresh
    refresh — two exchanges against the same one-shot refresh token would kill
    the credential the first just minted. They share one lock."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=_minted("tok-new"))
    harness = await Harness.create(
        tmp_path,
        config=Config(
            workspace_root=tmp_path / "workspaces",
            log_root=tmp_path / "logs",
            symphony_encryption_key=_KEY,
            repos=[
                RepoBinding(
                    linear_team_key="ENG",
                    github_repo="org/repo",
                    linear_states=LinearStates(
                        ready="Todo", in_progress="In Progress", code_review="Needs Approval"
                    ),
                )
            ],
        ),
    )
    try:
        await _store(harness.conn, _cred("tok-v0"))
        dispenser = harness.orch.claude_token_dispenser
        async with harness.orch._claude_refresh_lock:  # noqa: SLF001
            pending = asyncio.create_task(dispenser.request(1))
            await asyncio.sleep(0.05)
            assert route.call_count == 0  # blocked behind the daemon's refresh
        assert (await pending).token == "tok-new"  # type: ignore[union-attr]
        assert route.call_count == 1
    finally:
        await harness.close()
