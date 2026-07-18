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


def _cred_soon(token: str, refresh: str = "rt-1") -> str:
    """Credential expiring in ~60s — inside any refresh horizon."""
    import time as _time

    return json.dumps(
        {
            "claudeAiOauth": {
                "accessToken": token,
                "refreshToken": refresh,
                "expiresAt": int((_time.time() + 60) * 1000),
                "scopes": ["user:inference"],
            }
        }
    )


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
                assert blob["claudeAiOauth"]["refreshToken"] == "rt-2"
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
async def test_auth_failure_flags_expired_and_gates_dispatch(tmp_path: Path) -> None:
    """Config v2 5/9: an auth-failed Claude run flips the row to `expired`;
    the dispatch gate then blocks further Claude runs until reconnect —
    the SYM-200/201 hot loop is structurally impossible."""

    class _AuthError:
        message = "Not logged in · Please run /login"
        status = None

    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok"), cipher=cipher
        )
        # Live connection: no block.
        assert await harness.orch._claude_expired_block_reason("claude") is None  # noqa: SLF001

        await harness.orch._flag_claude_auth_failure("claude", _AuthError())  # noqa: SLF001
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "expired"
        assert status.updated_by == "auth-failure"

        blocked = await harness.orch._claude_expired_block_reason("claude")  # noqa: SLF001
        assert blocked is not None and "reconnect it" in blocked
        # Non-Claude agents and non-auth errors never flip/block.
        assert await harness.orch._claude_expired_block_reason("codex") is None  # noqa: SLF001

        class _Http500:
            message = "API Error: 500 upstream"
            status = 500

        await db.oauth_connections.update_status(
            harness.conn, provider="claude", status="connected"
        )
        await harness.orch._flag_claude_auth_failure("claude", _Http500())  # noqa: SLF001
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None and status.status == "connected"
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
async def test_codex_central_refresh_serialized_and_written_back(tmp_path: Path) -> None:
    """SYM-217: a near-expiry codex row is refreshed centrally (via the CLI
    seam) exactly once under concurrency, and the refresh is persisted."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="codex", credential=_codex_cred_soon(), cipher=cipher
        )
        calls = {"n": 0}
        fresh = json.dumps({"tokens": {"access_token": "fresh", "refresh_token": "rt2"}})

        async def fake_cli_refresh(_credential: str) -> str:
            calls["n"] += 1
            return fresh

        harness.orch._run_codex_cli_refresh = fake_cli_refresh  # type: ignore[assignment]  # noqa: SLF001
        env_a, env_b = await asyncio.gather(
            harness.orch._materialize_claude_env("codex"),  # noqa: SLF001
            harness.orch._materialize_claude_env("codex"),  # noqa: SLF001
        )
        try:
            assert calls["n"] == 1  # serialized: second dispatch reused the refresh
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
async def test_codex_central_refresh_fail_open(tmp_path: Path) -> None:
    """A CLI refresh that can't run leaves the run to refresh in-place — the
    codex dispatch is never blocked (fail-open, unlike claude)."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="codex", credential=_codex_cred_soon(), cipher=cipher
        )

        async def failed_refresh(_credential: str) -> None:
            return None

        harness.orch._run_codex_cli_refresh = failed_refresh  # type: ignore[assignment]  # noqa: SLF001
        env = await harness.orch._materialize_claude_env("codex")  # noqa: SLF001
        try:
            assert env.get("CODEX_HOME")  # dispatch proceeds
        finally:
            await harness.orch._finalize_claude_env(env)  # noqa: SLF001
    finally:
        await harness.close()
