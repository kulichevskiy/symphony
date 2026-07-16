"""Credential resolver + runtime materialization (OAuth in UI 4/7).

The resolver returns a provider's credential DB-first (decrypted from
`oauth_connections`), falling back to the caller-supplied env/volume value when
the provider has no usable DB connection — so migration is per-provider and
zero-downtime. Materialization writes the resolved creds into a private,
torn-down directory as a git credential store and returns the env additions a
run needs (`GH_TOKEN`, git credential helper, Linear bearer).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.credentials import (
    CredentialResolver,
    RunCredentials,
    materialize_credentials,
)
from symphony.crypto import CredentialCipher


async def _conn(tmp_path: Path):  # type: ignore[no-untyped-def]
    return await db.connect(tmp_path / "state.sqlite")


# ---- resolver ----


@pytest.mark.asyncio
async def test_resolve_prefers_db_connection(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    cipher = CredentialCipher("k")
    try:
        await db.oauth_connections.set_connection(
            conn, provider="github", credential="gho_db_token", cipher=cipher
        )
        resolver = CredentialResolver(conn, cipher)
        assert await resolver.resolve("github", fallback="env_token") == "gho_db_token"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_resolve_falls_back_when_no_row(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        resolver = CredentialResolver(conn, CredentialCipher("k"))
        assert await resolver.resolve("github", fallback="env_token") == "env_token"
        assert await resolver.resolve("linear", fallback=None) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_resolve_falls_back_when_not_connected(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    cipher = CredentialCipher("k")
    try:
        await db.oauth_connections.set_connection(
            conn, provider="linear", credential="lin_db", cipher=cipher, status="expired"
        )
        resolver = CredentialResolver(conn, cipher)
        # An expired connection is broken — fall back rather than hand out a
        # token that will 401 mid-run.
        assert await resolver.resolve("linear", fallback="env_key") == "env_key"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_resolve_falls_back_on_decrypt_error(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn, provider="github", credential="gho_db", cipher=CredentialCipher("old-key")
        )
        # Key rotated: the stored ciphertext no longer decrypts. Rather than
        # crash the run, fall back to env/volume so the instance keeps running.
        resolver = CredentialResolver(conn, CredentialCipher("new-key"))
        assert await resolver.resolve("github", fallback="env_token") == "env_token"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_resolve_run_credentials_bundles_both(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    cipher = CredentialCipher("k")
    try:
        await db.oauth_connections.set_connection(
            conn, provider="github", credential="gho_db", cipher=cipher
        )
        resolver = CredentialResolver(conn, cipher)
        creds = await resolver.resolve_run_credentials(
            github_fallback="gh_env", linear_fallback="lin_env"
        )
        assert creds.github_token == "gho_db"  # DB-first
        assert creds.linear_token == "lin_env"  # no row → fallback
    finally:
        await conn.close()


# ---- materialization ----


def test_materialize_writes_git_credential_store_and_env(tmp_path: Path) -> None:
    home = tmp_path / "creds"
    home.mkdir()
    env = materialize_credentials(
        RunCredentials(github_token="gho_x", linear_token="lin_y"), home
    )
    # git credential helper: a global config pointing at a store file that
    # carries the token, so an HTTPS `git push` inside the run authenticates.
    gitconfig = Path(env["GIT_CONFIG_GLOBAL"])
    assert gitconfig.parent == home
    assert "helper = store" in gitconfig.read_text()
    cred_file = home / ".git-credentials"
    assert "gho_x@github.com" in cred_file.read_text()
    # `gh` reads GH_TOKEN / GH_ENTERPRISE_TOKEN.
    assert env["GH_TOKEN"] == "gho_x"
    assert env["GH_ENTERPRISE_TOKEN"] == "gho_x"
    # Linear client sends this as its bearer.
    assert env["LINEAR_API_KEY"] == "lin_y"
    # HOME is never clobbered — the agent CLIs (Claude/Codex) resolve their own
    # auth from HOME and must keep it.
    assert "HOME" not in env


def test_materialize_linear_only_writes_no_git_store(tmp_path: Path) -> None:
    home = tmp_path / "creds"
    home.mkdir()
    env = materialize_credentials(RunCredentials(linear_token="lin_y"), home)
    assert env == {"LINEAR_API_KEY": "lin_y"}
    assert not (home / ".git-credentials").exists()


def test_materialize_empty_creds_returns_empty_env(tmp_path: Path) -> None:
    home = tmp_path / "creds"
    home.mkdir()
    assert materialize_credentials(RunCredentials(), home) == {}
    assert RunCredentials().is_empty
