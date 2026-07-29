"""Daemon wiring: per-run Claude credential materialization + write-back
(Config v2 3/9). The write-back unit is covered in test_credential_write_back;
this pins the orchestrator seam — a connected Claude row is materialized into a
private per-run CLAUDE_CONFIG_DIR, a refreshed credential is re-persisted from
that dir at finalize, and the dir is torn down."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import httpx
import pytest
import respx

from symphony import db
from symphony.claude_login import CLAUDE_OAUTH_TOKEN_URL
from symphony.codex_login import CODEX_REFRESH_TOKEN_URL
from symphony.config import Config, LinearStates, RepoBinding
from symphony.crypto import CredentialCipher
from tests.harness import Harness

ENC_KEY = "deployment-secret"


def _config(tmp_path: Path) -> Config:
    return Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        symphony_encryption_key=ENC_KEY,
        repos=[
            RepoBinding(
                linear_team_key="ENG",
                github_repo="org/repo",
                linear_states=LinearStates(
                    ready="Todo", in_progress="In Progress", code_review="Needs Approval"
                ),
            )
        ],
    )


def _cred(token: str, expires_ms: int = 4102444800000) -> str:
    return json.dumps({"claudeAiOauth": {"accessToken": token, "expiresAt": expires_ms}})


@pytest.mark.asyncio
async def test_materialize_finalize_round_trip_with_refresh(tmp_path: Path) -> None:
    """A connected Claude row materializes into a private per-run dir; a
    mid-run refresh (file rewrite) is written back at finalize; the dir is
    removed. A second run then materializes the refreshed credential — two
    sequential runs, no re-auth."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-v0"), cipher=cipher
        )

        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        config_dir = Path(env["CLAUDE_CONFIG_DIR"])
        cred_file = config_dir / ".credentials.json"
        assert cred_file.read_text(encoding="utf-8") == _cred("tok-v0")

        # The CLI refreshes the token in place mid-run.
        cred_file.write_text(_cred("tok-v1"), encoding="utf-8")
        await harness.orch._finalize_claude_env(env)  # noqa: SLF001
        assert await db.oauth_connections.get_credential(harness.conn, "claude", cipher) == _cred(
            "tok-v1"
        )
        assert config_dir.name not in os.listdir(config_dir.parent)  # torn down

        # Run 2 starts from the refreshed credential.
        env2 = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        dir2 = Path(env2["CLAUDE_CONFIG_DIR"])
        assert dir2 != config_dir
        assert (dir2 / ".credentials.json").read_text(encoding="utf-8") == _cred("tok-v1")
        await harness.orch._finalize_claude_env(env2)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_concurrent_runs_get_separate_dirs(tmp_path: Path) -> None:
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok"), cipher=cipher
        )
        env_a = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        env_b = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        try:
            assert env_a["CLAUDE_CONFIG_DIR"] != env_b["CLAUDE_CONFIG_DIR"]
        finally:
            await harness.orch._finalize_claude_env(env_a)  # noqa: SLF001
            await harness.orch._finalize_claude_env(env_b)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_not_connected_or_non_claude_materializes_nothing(tmp_path: Path) -> None:
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        # Never connected in the UI → no CLAUDE_CONFIG_DIR, ambient auth rules.
        assert await harness.orch._materialize_claude_env("claude") == {}  # noqa: SLF001
        # Non-Claude agents never materialize, connected or not.
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok"), cipher=cipher
        )
        assert await harness.orch._materialize_claude_env("codex") == {}  # noqa: SLF001
        # Finalizing an empty env is a no-op (and must not slurp ambient creds).
        await harness.orch._finalize_claude_env({})  # noqa: SLF001
    finally:
        await harness.close()


def _cred_in(token: str, seconds: float, refresh: str = "rt-1") -> str:
    """Credential whose access token expires `seconds` from now, with a
    refresh token so the central refresh can rotate it."""
    import time as _time

    return json.dumps(
        {
            "claudeAiOauth": {
                "accessToken": token,
                "refreshToken": refresh,
                "expiresAt": int((_time.time() + seconds) * 1000),
                "scopes": ["user:inference"],
            }
        }
    )


def _cred_soon(token: str, refresh: str = "rt-1") -> str:
    """Credential expiring in ~60s — inside any refresh horizon."""
    return _cred_in(token, 60, refresh)


@pytest.mark.asyncio
@respx.mock
async def test_near_expiry_refreshes_exactly_once_under_concurrency(tmp_path: Path) -> None:
    """Config v2 4/9: two concurrent dispatches near expiry → one serialized
    refresh; both runs materialize the refreshed token."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": "tok-new", "refresh_token": "rt-2", "expires_in": 28800},
        )
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok-old"), cipher=cipher
        )
        env_a, env_b = await asyncio.gather(
            harness.orch._materialize_claude_env("claude"),  # noqa: SLF001
            harness.orch._materialize_claude_env("claude"),  # noqa: SLF001
        )
        try:
            assert route.call_count == 1
            for env in (env_a, env_b):
                blob = json.loads(
                    (Path(env["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_text()
                )
                assert blob["claudeAiOauth"]["accessToken"] == "tok-new"
                # SYM-228: the run's copy is access-token-only.
                assert "refreshToken" not in blob["claudeAiOauth"]
                assert blob["claudeAiOauth"]["scopes"] == ["user:inference"]
        finally:
            await harness.orch._finalize_claude_env(env_a)  # noqa: SLF001
            await harness.orch._finalize_claude_env(env_b)  # noqa: SLF001
        stored = await db.oauth_connections.get_credential(harness.conn, "claude", cipher)
        assert json.loads(stored)["claudeAiOauth"]["accessToken"] == "tok-new"
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.updated_by == "write-back"
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_far_expiry_never_refreshes(tmp_path: Path) -> None:
    """A token with runway far beyond the keep-fresh margin is left untouched —
    the daemon does not burn a refresh on an already-fresh credential."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "x"})
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok"), cipher=cipher
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        try:
            assert route.call_count == 0
        finally:
            await harness.orch._finalize_claude_env(env)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_keeps_token_fresh_far_beyond_wall_clock(tmp_path: Path) -> None:
    """SYM-227: the daemon keeps the Claude token proactively fresh with a
    margin far larger than a run's wall clock. A token ~3h out — well beyond
    the run wall clock (so calendar-expiry-mid-run was never the risk), yet
    inside the keep-fresh margin — is re-minted before dispatch, so the run
    receives a near-freshly-minted token and the daemon (not the run's CLI)
    owns refresh-token rotation."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": "tok-fresh", "refresh_token": "rt-2", "expires_in": 28800},
        )
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-stale", 3 * 60 * 60),
            cipher=cipher,
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        try:
            assert route.call_count == 1
            blob = json.loads((Path(env["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_text())
            assert blob["claudeAiOauth"]["accessToken"] == "tok-fresh"
        finally:
            await harness.orch._finalize_claude_env(env)  # noqa: SLF001
        stored = await db.oauth_connections.get_credential(harness.conn, "claude", cipher)
        assert json.loads(stored)["claudeAiOauth"]["accessToken"] == "tok-fresh"
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.updated_by == "write-back"
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_keep_fresh_refresh_failure_fails_open(tmp_path: Path) -> None:
    """SYM-227: a keep-fresh refresh miss on a token that still outlives the run
    must not strand the connection. A ~3h-out token (past the wall clock, so not
    dying mid-run) whose refresh hits a transient failure falls open on the
    still-valid token — dispatch proceeds and the row stays connected."""
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(500, json={}))
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-live", 3 * 60 * 60),
            cipher=cipher,
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        try:
            assert env.get("CLAUDE_CONFIG_DIR")  # dispatch proceeds on the live token
            blob = json.loads((Path(env["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_text())
            assert blob["claudeAiOauth"]["accessToken"] == "tok-live"
        finally:
            await harness.orch._finalize_claude_env(env)  # noqa: SLF001
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "connected"
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_freshly_minted_token_not_re_refreshed(tmp_path: Path) -> None:
    """SYM-227: the keep-fresh margin sits below a fresh token's ~8h TTL, so a
    just-minted token (well beyond the margin) is not re-refreshed — no
    per-dispatch rotation storm that would poison siblings' refresh tokens."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "x"})
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-fresh", 7 * 60 * 60 + 1800),
            cipher=cipher,
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        try:
            assert route.call_count == 0
        finally:
            await harness.orch._finalize_claude_env(env)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_keep_fresh_rotation_deferred_while_runs_hold_the_token(tmp_path: Path) -> None:
    """SYM-230: rotating the shared token invalidates the access-token-only
    copies (SYM-228) already handed to in-flight runs — they cannot self-heal
    and die on "Not logged in" with hours of nominal TTL left. So a keep-fresh
    rotation is deferred while any run still holds the token, and taken as soon
    as the fleet drains."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": "tok-fresh", "refresh_token": "rt-2", "expires_in": 28800},
        )
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        # Run A starts on a freshly minted token — nothing to rotate.
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 8 * 60 * 60),
            cipher=cipher,
        )
        env_a = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert route.call_count == 0

        # Time passes: the same token is now inside the keep-fresh margin (but
        # still outside the dies-mid-run horizon — `_claude_dies_mid_run_horizon_secs`,
        # clamped to 4h regardless of this config's defaults — so run A is not
        # dying of the clock). Run B dispatching must NOT rotate it out from
        # under run A.
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 6 * 60 * 60),
            cipher=cipher,
        )
        env_b = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert route.call_count == 0
        blob = json.loads((Path(env_b["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_text())
        assert blob["claudeAiOauth"]["accessToken"] == "tok-a"

        await harness.orch._finalize_claude_env(env_a)  # noqa: SLF001
        await harness.orch._finalize_claude_env(env_b)  # noqa: SLF001

        # Fleet drained: the deferred rotation is taken on the next dispatch.
        env_c = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        try:
            assert route.call_count == 1
            blob = json.loads((Path(env_c["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_text())
            assert blob["claudeAiOauth"]["accessToken"] == "tok-fresh"
        finally:
            await harness.orch._finalize_claude_env(env_c)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_dying_token_still_rotates_with_runs_in_flight(tmp_path: Path) -> None:
    """SYM-230: the in-flight deferral only covers tokens that outlive a run's
    wall clock. Once the stored token would die mid-run, rotating is strictly
    better than not — the in-flight runs lose it to the clock either way, and
    the dispatching run must not start on a doomed token."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": "tok-fresh", "refresh_token": "rt-2", "expires_in": 28800},
        )
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 8 * 60 * 60),
            cipher=cipher,
        )
        env_a = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok-a"), cipher=cipher
        )
        env_b = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        try:
            assert route.call_count == 1
            blob = json.loads((Path(env_b["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_text())
            assert blob["claudeAiOauth"]["accessToken"] == "tok-fresh"
        finally:
            await harness.orch._finalize_claude_env(env_a)  # noqa: SLF001
            await harness.orch._finalize_claude_env(env_b)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_stale_inflight_registration_ages_out_of_rotation_guard(tmp_path: Path) -> None:
    """SYM-230 review: a materialize whose finalize never runs (the dispatcher
    raised between the two) would otherwise pin the rotation-defer guard
    forever. Once the registration outlives the real max hold time
    (`_claude_max_dir_hold_secs`), it must stop blocking rotation."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": "tok-fresh", "refresh_token": "rt-2", "expires_in": 28800},
        )
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 8 * 60 * 60),
            cipher=cipher,
        )
        # Leaked: materialized but never finalized.
        await harness.orch._materialize_claude_env("claude")  # noqa: SLF001

        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 6 * 60 * 60),
            cipher=cipher,
        )
        env_b = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert route.call_count == 0  # still deferred: the leaked dir is fresh
        await harness.orch._finalize_claude_env(env_b)  # noqa: SLF001

        # Age the leaked registration past the real max hold time.
        harness.advance(harness.orch._claude_max_dir_hold_secs() + 1)  # noqa: SLF001
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 6 * 60 * 60),
            cipher=cipher,
        )
        env_c = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        try:
            assert route.call_count == 1  # the stale registration no longer defers
            blob = json.loads((Path(env_c["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_text())
            assert blob["claudeAiOauth"]["accessToken"] == "tok-fresh"
        finally:
            await harness.orch._finalize_claude_env(env_c)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize("wall_clock_timeout_secs", [7200, 0])
async def test_horizons_stay_below_fresh_token_ttl_under_configured_wall_clock(
    tmp_path: Path, wall_clock_timeout_secs: int
) -> None:
    """SYM-230 review: `wall_clock_timeout_secs * (turns per local-review
    iteration)` can exceed a fresh token's ~8h TTL well before the knob
    reaches its max (24h) — e.g. at 7200s (2h) with the default iteration cap
    alone the raw bound is already 30h, and `0` falls back to a 2h default
    horizon with the same effect. Unclamped, that inverts the whole feature:
    a just-minted token would be re-refreshed on every dispatch, and the
    rotation-defer check would always read "safe to rotate". Pin that, at
    both knob values, a freshly minted token is never re-refreshed at
    dispatch, and a rotation is still deferred while a dir is registered."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": "tok-fresh-2", "refresh_token": "rt-3", "expires_in": 28800},
        )
    )
    config = _config(tmp_path).model_copy(
        update={"wall_clock_timeout_secs": wall_clock_timeout_secs}
    )
    harness = await Harness.create(tmp_path, config=config)
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 8 * 60 * 60),
            cipher=cipher,
        )
        # A just-minted 8h token must never be re-refreshed at dispatch.
        env_a = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert route.call_count == 0

        # Time passes: the token is now within the keep-fresh margin but a
        # second dispatch must not rotate it out from under run A.
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 5 * 60 * 60),
            cipher=cipher,
        )
        env_b = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert route.call_count == 0
        blob = json.loads((Path(env_b["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_text())
        assert blob["claudeAiOauth"]["accessToken"] == "tok-a"

        await harness.orch._finalize_claude_env(env_a)  # noqa: SLF001
        await harness.orch._finalize_claude_env(env_b)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_auth_failure_revalidate_skips_only_on_genuine_supersession(tmp_path: Path) -> None:
    """SYM-230 review: the re-validate skip must compare the FAILING run's own
    recorded access token against the stored credential — not a sibling's.
    Every real caller finalizes the failing run's dir (popping it from
    `_claude_inflight_dirs`) before flagging the auth failure, so an "any
    still-registered sibling holds a different token" check never sees the
    failing run's own token at all. Here the run that fails ("run-a") was
    itself handed "tok-a", its dir is finalized (as every real caller does)
    before the flag, and the stored credential has since moved to "tok-b" (an
    earlier rotation) — the failure is explained by that rotation and the run
    requeues onto "tok-b" without a real refresh."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": "tok-fresh", "refresh_token": "rt-2", "expires_in": 28800},
        )
    )

    class _AuthError:
        message = "Not logged in · Please run /login"
        status = None

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 8 * 60 * 60),
            cipher=cipher,
        )
        env_a = await harness.orch._materialize_claude_env(  # noqa: SLF001
            "claude", run_id="run-a"
        )

        # An earlier rotation supersedes run-a's token before its auth failure
        # is flagged.
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-b", 8 * 60 * 60),
            cipher=cipher,
        )
        # Every real caller finalizes the failing run's dir before flagging —
        # the failing run's recorded token must survive that (SYM-230 review).
        await harness.orch._finalize_claude_env(env_a)  # noqa: SLF001
        requeued = await harness.orch._flag_claude_auth_failure(  # noqa: SLF001
            "claude", _AuthError(), run_id="run-a"
        )
        assert requeued is True
        assert route.call_count == 0
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "connected"
        stored = await db.oauth_connections.get_credential(harness.conn, "claude", cipher)
        assert json.loads(stored)["claudeAiOauth"]["accessToken"] == "tok-b"

        # A different run, whose own token was never recorded, has nothing to
        # attribute to a rotation and must run a real re-validate.
        assert (
            await harness.orch._flag_claude_auth_failure(  # noqa: SLF001
                "claude", _AuthError(), run_id="run-c"
            )
            is True
        )
        assert route.call_count == 1
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_auth_failure_revalidate_ignores_unrelated_sibling_token(tmp_path: Path) -> None:
    """SYM-230 review negative case: a sibling run still holds an unrelated,
    older token ("tok-old"), but the FAILING run ("run-b") was handed the
    token that is still the one currently stored ("tok-current") — nothing
    superseded IT. A guard keyed on "any sibling's token differs from the
    stored one" would wrongly treat the sibling's older token as proof of a
    rotation and mask a dead account. The re-validate must run for real here
    and expire the row."""
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(400, json={}))

    class _AuthError:
        message = "Not logged in · Please run /login"
        status = None

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-old", 8 * 60 * 60),
            cipher=cipher,
        )
        sibling_env = await harness.orch._materialize_claude_env(  # noqa: SLF001
            "claude", run_id="run-sibling"
        )

        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-current", 8 * 60 * 60),
            cipher=cipher,
        )
        env_b = await harness.orch._materialize_claude_env(  # noqa: SLF001
            "claude", run_id="run-b"
        )
        await harness.orch._finalize_claude_env(env_b)  # noqa: SLF001

        requeued = await harness.orch._flag_claude_auth_failure(  # noqa: SLF001
            "claude", _AuthError(), run_id="run-b"
        )
        assert requeued is False
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "expired"

        await harness.orch._finalize_claude_env(sibling_env)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_auth_failure_revalidate_refreshes_when_no_rotation_happened(
    tmp_path: Path,
) -> None:
    """SYM-230 review reproduction: the reported incident had NO rotation
    between the 05:49 mint and the 06:24 "Not logged in" failure — the run
    died holding the exact token still stored in the DB. Skipping the
    re-validate here (as the unconditional "some run still holds runway"
    check used to) would requeue the run onto the identical, already-dead
    token and burn the retry budget while the connection stayed green. With
    no evidence of a supersession (env_a's token still matches the stored
    credential), the guard must fall through to a real re-validate."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200,
            json={"access_token": "tok-fresh", "refresh_token": "rt-2", "expires_in": 28800},
        )
    )

    class _AuthError:
        message = "Not logged in · Please run /login"
        status = None

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok-a", 8 * 60 * 60),
            cipher=cipher,
        )
        env_a = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        requeued = await harness.orch._flag_claude_auth_failure(  # noqa: SLF001
            "claude", _AuthError()
        )
        assert requeued is True
        assert route.call_count == 1  # a real re-validate ran, not a blind skip
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "connected"
        stored = await db.oauth_connections.get_credential(harness.conn, "claude", cipher)
        assert json.loads(stored)["claudeAiOauth"]["accessToken"] == "tok-fresh"
        await harness.orch._finalize_claude_env(env_a)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_unknown_recorded_token_does_not_defer_revalidate_forever(
    tmp_path: Path,
) -> None:
    """SYM-230 review: the supersession skip must only fire when the FAILING
    run's own recorded access token is known (via `run_id`, from
    `_claude_run_access_tokens`) AND provably differs from the stored
    credential. When the failing run's token was never recorded — no
    `run_id` passed, or (as here) the run never materialized a claude dir at
    all, on a credential with no parseable `expiresAt` either — the guard
    must fall through to a real re-validate rather than reading "unknown" as
    "already superseded", or a dead account would never expire the row."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(400, json={}))

    class _AuthError:
        message = "Not logged in · Please run /login"
        status = None

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        no_expiry_blob = json.dumps(
            {"claudeAiOauth": {"accessToken": "tok-a", "refreshToken": "rt-1"}}
        )
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=no_expiry_blob, cipher=cipher
        )
        requeued = await harness.orch._flag_claude_auth_failure(  # noqa: SLF001
            "claude", _AuthError(), run_id="run-unknown"
        )
        assert requeued is False
        assert route.call_count == 1  # a refresh was actually attempted, not skipped
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "expired"
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_refresh_failure_marks_expired_and_blocks_materialization(tmp_path: Path) -> None:
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(400, json={}))
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok-old"), cipher=cipher
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert env == {}  # dispatch must not proceed on a dying token
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "expired"
        assert status.updated_by == "auto-refresh"
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_cas_write_back_skips_when_row_changed_mid_run(tmp_path: Path) -> None:
    """Config v2 5/9: an operator reconnect while a run is in flight wins over
    the run's stale refreshed credential — the finalize write-back CAS no-ops."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-run-start"), cipher=cipher
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        # Mid-run: the CLI refreshes its private copy...
        (Path(env["CLAUDE_CONFIG_DIR"]) / ".credentials.json").write_text(
            _cred("tok-run-refreshed"), encoding="utf-8"
        )
        # ...while the operator reconnects in the UI (row replaced).
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-reconnected"), cipher=cipher
        )
        await harness.orch._finalize_claude_env(env)  # noqa: SLF001
        # The reconnect sticks; the stale run material did not overwrite it.
        assert await db.oauth_connections.get_credential(harness.conn, "claude", cipher) == _cred(
            "tok-reconnected"
        )
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_disconnect_mid_run_is_not_resurrected(tmp_path: Path) -> None:
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok"), cipher=cipher
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        (Path(env["CLAUDE_CONFIG_DIR"]) / ".credentials.json").write_text(
            _cred("tok-refreshed"), encoding="utf-8"
        )
        # Operator disconnects mid-run: the row is deleted.
        await db.oauth_connections.delete(harness.conn, "claude")
        await harness.orch._finalize_claude_env(env)  # noqa: SLF001
        # Disconnect sticks — write_back's no-row guard keeps it deleted.
        assert await db.oauth_connections.get_status(harness.conn, "claude") is None
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_auth_failure_flags_expired_and_gates_dispatch(tmp_path: Path) -> None:
    """SYM-229 blast-radius hardening: a single run's auth failure re-validates
    the shared Claude connection with a daemon refresh instead of expiring it
    outright. When the connection is still refreshable the row stays
    `connected` and the run is requeued (return True) — other dispatch keeps
    running. Only the daemon's OWN refresh genuinely failing flips the row to
    `expired` and arms the reconnect dispatch gate (the SYM-200/201 hot loop
    is still structurally impossible)."""

    class _AuthError:
        message = "Not logged in · Please run /login"
        status = None

    # The re-validate refresh succeeds once (200), then the account is dead
    # (400) — driving the refreshable then not-refreshable paths in order.
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "access_token": "tok-revalidated",
                    "refresh_token": "rt-2",
                    "expires_in": 28800,
                },
            ),
            httpx.Response(400, json={}),
        ]
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        # Refreshable connection (carries a refresh token).
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok"), cipher=cipher
        )
        # Live connection: no block.
        assert await harness.orch._claude_expired_block_reason("claude") is None  # noqa: SLF001

        # A single run's auth failure while the connection can still be
        # refreshed: the daemon re-mints the token, the row stays connected,
        # and the run is requeued (return True) — no fleet-wide expire.
        requeued = await harness.orch._flag_claude_auth_failure(  # noqa: SLF001
            "claude", _AuthError()
        )
        assert requeued is True
        assert route.call_count == 1
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "connected"
        # Other dispatch continues — the gate is not armed.
        assert await harness.orch._claude_expired_block_reason("claude") is None  # noqa: SLF001
        # The re-minted token is persisted.
        stored = await db.oauth_connections.get_credential(harness.conn, "claude", cipher)
        assert json.loads(stored)["claudeAiOauth"]["accessToken"] == "tok-revalidated"

        # Non-auth errors never touch the connection (and never refresh).
        class _Http500:
            message = "API Error: 500 upstream"
            status = 500

        assert (
            await harness.orch._flag_claude_auth_failure("claude", _Http500())  # noqa: SLF001
            is False
        )
        assert route.call_count == 1
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "connected"

        # Only the daemon's OWN refresh genuinely failing expires the row and
        # arms the reconnect gate.
        expired = await harness.orch._flag_claude_auth_failure(  # noqa: SLF001
            "claude", _AuthError()
        )
        assert expired is False
        assert route.call_count == 2
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "expired"
        assert status.updated_by == "auth-failure"

        blocked = await harness.orch._claude_expired_block_reason("claude")  # noqa: SLF001
        assert blocked is not None and "reconnect it" in blocked
        # Non-Claude agents never flip/block.
        assert await harness.orch._claude_expired_block_reason("codex") is None  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_expired_connection_not_revalidated_back_to_connected(tmp_path: Path) -> None:
    """SYM-229 review: once the Claude row is `expired` (the reconnect gate is
    armed), a later stale run's auth failure must NOT silently refresh it back
    to `connected` and unblock the fleet without the operator reconnect. No
    re-validate refresh is attempted; the row stays gated."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200, json={"access_token": "x", "refresh_token": "y", "expires_in": 28800}
        )
    )

    class _AuthError:
        message = "Not logged in"
        status = 401

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok"), cipher=cipher
        )
        await db.oauth_connections.update_status(harness.conn, provider="claude", status="expired")
        result = await harness.orch._flag_claude_auth_failure("claude", _AuthError())  # noqa: SLF001
        assert result is False
        assert route.call_count == 0  # an expired row is never re-validated
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "expired"
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_revalidate_respects_disconnect_mid_flight(tmp_path: Path) -> None:
    """SYM-229 review: if the operator disconnects Claude while the re-validate
    refresh is in flight, the write-back finds no live row. That miss must NOT
    be treated as a harmless CAS race — the run is not requeued (returns False)
    so the intentional disconnect is respected."""
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200, json={"access_token": "tok-new", "refresh_token": "rt-2", "expires_in": 28800}
        )
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok"), cipher=cipher
        )
        # Simulate the operator disconnecting mid-flight: the row is gone by the
        # time the re-validate write-back runs.
        orig_write_back = harness.orch._credential_write_back.write_back  # noqa: SLF001

        async def _disconnecting_write_back(*args: object, **kwargs: object) -> bool:
            await db.oauth_connections.delete(harness.conn, "claude")
            return await orig_write_back(*args, **kwargs)

        harness.orch._credential_write_back.write_back = _disconnecting_write_back  # type: ignore[assignment]  # noqa: SLF001
        verdict = await harness.orch._revalidate_claude_after_auth_failure()  # noqa: SLF001
        assert verdict == "gated"  # neither usable nor "dead" → no expire, no requeue
        assert await db.oauth_connections.get_status(harness.conn, "claude") is None
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_transient_token_endpoint_does_not_expire_connection(tmp_path: Path) -> None:
    """SYM-229 review: a 5xx/unreachable token endpoint says nothing about the
    account, so a run's auth failure during an endpoint flake must NOT expire the
    shared connection (that would block every other run until a reconnect). The
    row stays `connected` and the run is requeued."""
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(500, json={}))

    class _Auth:
        message = "Not logged in"
        status = 401

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok"), cipher=cipher
        )
        assert await harness.orch._flag_claude_auth_failure("claude", _Auth()) is True  # noqa: SLF001
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "connected"
        # A genuinely dead credential (4xx) still expires it.
        respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(400, json={}))
        assert await harness.orch._flag_claude_auth_failure("claude", _Auth()) is False  # noqa: SLF001
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "expired"
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_auth_verdict_is_cached_per_run(tmp_path: Path) -> None:
    """SYM-229 review: one auth failure must cause exactly ONE daemon refresh.
    A verdict recorded for a run (by the runner tail / completion gate) is reused
    by the requeue sites instead of triggering a second refresh that could fail
    transiently and undo an already-proven-safe verdict."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200, json={"access_token": "tok-new", "refresh_token": "rt-2", "expires_in": 28800}
        )
    )

    class _Auth:
        message = "Not logged in"
        status = 401

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok"), cipher=cipher
        )
        # First detection point records the verdict for this run.
        assert (
            await harness.orch._flag_claude_auth_failure(  # noqa: SLF001
                "claude", _Auth(), run_id="run-1"
            )
            is True
        )
        assert route.call_count == 1
        # The requeue site reuses it — no second refresh.
        assert (
            await harness.orch._claude_auth_requeue_signal(  # noqa: SLF001
                "claude", _Auth(), run_id="run-1"
            )
            is True
        )
        assert route.call_count == 1
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_requeue_signal_reads_plaintext_auth_log(tmp_path: Path) -> None:
    """SYM-229 review: the rc=0 fix paths get `api_error` from the JSONL-only
    reader, which skips plaintext/stderr auth lines. The requeue signal falls
    back to scanning the run log so a rc=0 "Not logged in" is re-validated
    (and requeued) instead of parked as a silent no-op."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200, json={"access_token": "tok-new", "refresh_token": "rt-2", "expires_in": 28800}
        )
    )
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok"), cipher=cipher
        )
        log_path = tmp_path / "rc0.log"
        log_path.write_text("Not logged in · Please run /login\n", encoding="utf-8")
        # api_error is None (JSONL reader saw nothing) but the log says otherwise.
        assert (
            await harness.orch._claude_auth_requeue_signal(  # noqa: SLF001
                "claude", None, run_id="run-rc0", log_path=log_path
            )
            is True
        )
        assert route.call_count == 1
        # A log with no auth line stays a no-op (and never refreshes).
        clean = tmp_path / "clean.log"
        clean.write_text("all good\n", encoding="utf-8")
        assert (
            await harness.orch._claude_auth_requeue_signal(  # noqa: SLF001
                "claude", None, run_id="run-clean", log_path=clean
            )
            is False
        )
        assert route.call_count == 1
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_claude_auth_requeue_signal(tmp_path: Path) -> None:
    """SYM-229 review: the signal the review/merge/local-review requeue sites
    read drives the daemon re-validate itself (it must not trust a stale
    `connected` row, since the rc=0 fix paths skip the runner-tail flag). True
    for a Claude auth error on a re-validatable row; False for non-auth errors,
    non-claude agents, no error, and an already-expired (gated) row — and those
    paths never touch the refresh endpoint."""
    route = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200, json={"access_token": "tok-new", "refresh_token": "rt-2", "expires_in": 28800}
        )
    )

    class _Auth:
        message = "Not logged in"
        status = 401

    class _NonAuth:
        message = "API Error: 500 upstream"
        status = 500

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred_soon("tok"), cipher=cipher
        )
        sig = harness.orch._claude_auth_requeue_signal  # noqa: SLF001
        # Claude auth error on a re-validatable row → one daemon refresh → True.
        assert await sig("claude", _Auth()) is True
        assert route.call_count == 1
        # Non-auth error, non-claude agent, and no error short-circuit: no refresh.
        assert await sig("claude", _NonAuth()) is False
        assert await sig("codex", _Auth()) is False
        assert await sig("claude", None) is False
        assert route.call_count == 1
        # An already-expired (gated) row is never re-validated back to connected.
        await db.oauth_connections.update_status(harness.conn, provider="claude", status="expired")
        assert await sig("claude", _Auth()) is False
        assert route.call_count == 1
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_auth_failure_recognizes_codex_refresh_token_phrase(tmp_path: Path) -> None:
    """Regression: a codex `{"type":"turn.failed","error":{"message":"refresh
    token expired"}}` classifies via `classify_stream_api_error` to a
    `StreamApiError(message='refresh token expired', status=None)` — no 401
    status, and none of "not logged in"/"unauthorized"/"authentication" in
    the message. Before this fix `looks_auth` rejected it outright and the
    already-matched classifier tier meant the plaintext/JSON-field fallbacks
    never ran either, so the row stayed `connected` forever on an expired
    codex credential."""

    class _CodexRefreshExpired:
        message = "refresh token expired"
        status = None

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="codex", credential=_codex_cred("tok"), cipher=cipher
        )
        await harness.orch._flag_claude_auth_failure(  # noqa: SLF001
            "codex", _CodexRefreshExpired()
        )
        status = await db.oauth_connections.get_status(harness.conn, "codex")
        assert status is not None and status.status == "expired"
        assert status.updated_by == "auth-failure"
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_auth_failure_without_ui_connection_is_noop(tmp_path: Path) -> None:
    class _AuthError:
        message = "Not logged in"
        status = 401

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        await harness.orch._flag_claude_auth_failure("claude", _AuthError())  # noqa: SLF001
        assert await db.oauth_connections.get_status(harness.conn, "claude") is None
        assert await harness.orch._claude_expired_block_reason("claude") is None  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_materialized_claude_creds_have_no_refresh_token(tmp_path: Path) -> None:
    """SYM-228: a run's materialized credential carries the access token but not
    the one-shot refresh token, so the CLI cannot rotate the shared token and
    concurrent runs (or one issue's own claude processes) can't poison it. The
    daemon keeps the full credential — refresh token included — in the DB and
    owns rotation centrally (SYM-227)."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        # Far-future expiry (30h) so materialize doesn't trip the keep-fresh
        # refresh; a refresh token is present in the stored blob.
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred_in("tok", 30 * 60 * 60, refresh="rt-secret"),
            cipher=cipher,
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        try:
            blob = json.loads((Path(env["CLAUDE_CONFIG_DIR"]) / ".credentials.json").read_text())
            assert blob["claudeAiOauth"]["accessToken"] == "tok"
            assert "refreshToken" not in blob["claudeAiOauth"]
        finally:
            await harness.orch._finalize_claude_env(env)  # noqa: SLF001
        # The DB retains the full credential (incl. the refresh token).
        stored = json.loads(
            await db.oauth_connections.get_credential(harness.conn, "claude", cipher)
        )
        assert stored["claudeAiOauth"]["refreshToken"] == "rt-secret"
    finally:
        await harness.close()


def _codex_cred(token: str = "at") -> str:
    # auth.json shape: tokens.access_token (a JWT is not required for storage).
    return json.dumps({"OPENAI_API_KEY": None, "tokens": {"access_token": token}})


@pytest.mark.asyncio
async def test_codex_materialize_finalize_round_trip(tmp_path: Path) -> None:
    """Config v2 6/9: a connected Codex row materializes into a private
    per-run CODEX_HOME; a mid-run refresh is written back (CAS); teardown."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="codex", credential=_codex_cred("tok-0"), cipher=cipher
        )
        env = await harness.orch._materialize_claude_env("codex")  # noqa: SLF001
        home = Path(env["CODEX_HOME"])
        assert (home / "auth.json").read_text(encoding="utf-8") == _codex_cred("tok-0")
        (home / "auth.json").write_text(_codex_cred("tok-1"), encoding="utf-8")
        await harness.orch._finalize_claude_env(env)  # noqa: SLF001
        assert await db.oauth_connections.get_credential(
            harness.conn, "codex", cipher
        ) == _codex_cred("tok-1")
        assert home.name not in os.listdir(home.parent)
    finally:
        await harness.close()


def _codex_cred_soon(access: str = "at", refresh: str = "rt") -> str:
    # codex expiry = JWT exp; build a minimal JWT with a near-future exp.
    import base64 as _b64
    import time as _time

    exp = int(_time.time() + 60)
    header = _b64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = _b64.urlsafe_b64encode(f'{{"exp":{exp}}}'.encode()).rstrip(b"=").decode()
    jwt = f"{header}.{payload}.sig"
    return json.dumps({"tokens": {"access_token": jwt, "refresh_token": refresh}, "id": access})


@pytest.mark.asyncio
@respx.mock
async def test_codex_central_refresh_serialized_and_written_back(tmp_path: Path) -> None:
    """SYM-217: a near-expiry codex row is refreshed centrally (via the CLI
    seam) exactly once under concurrency, and the refresh is persisted."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="codex", credential=_codex_cred_soon(), cipher=cipher
        )
        route = respx.post(CODEX_REFRESH_TOKEN_URL).mock(
            return_value=httpx.Response(200, json={"access_token": "fresh", "refresh_token": "rt2"})
        )
        env_a, env_b = await asyncio.gather(
            harness.orch._materialize_claude_env("codex"),  # noqa: SLF001
            harness.orch._materialize_claude_env("codex"),  # noqa: SLF001
        )
        try:
            assert route.call_count == 1  # serialized: second dispatch reused the refresh
            for env in (env_a, env_b):
                blob = (Path(env["CODEX_HOME"]) / "auth.json").read_text(encoding="utf-8")
                assert json.loads(blob)["tokens"]["access_token"] == "fresh"
        finally:
            await harness.orch._finalize_claude_env(env_a)  # noqa: SLF001
            await harness.orch._finalize_claude_env(env_b)  # noqa: SLF001
        stored = await db.oauth_connections.get_credential(harness.conn, "codex", cipher)
        assert json.loads(stored)["tokens"]["access_token"] == "fresh"
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_codex_central_refresh_fail_open(tmp_path: Path) -> None:
    """A CLI refresh that can't run leaves the run to refresh in-place — the
    codex dispatch is never blocked (fail-open, unlike claude)."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="codex", credential=_codex_cred_soon(), cipher=cipher
        )

        respx.post(CODEX_REFRESH_TOKEN_URL).mock(return_value=httpx.Response(400, json={}))
        env = await harness.orch._materialize_claude_env("codex")  # noqa: SLF001
        try:
            assert env.get("CODEX_HOME")  # dispatch proceeds
        finally:
            await harness.orch._finalize_claude_env(env)  # noqa: SLF001
    finally:
        await harness.close()
