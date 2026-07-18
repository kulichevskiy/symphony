"""Daemon wiring: per-run Claude credential materialization + write-back
(Config v2 3/9). The write-back unit is covered in test_credential_write_back;
this pins the orchestrator seam — a connected Claude row is materialized into a
private per-run CLAUDE_CONFIG_DIR, a refreshed credential is re-persisted from
that dir at finalize, and the dir is torn down."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from symphony import db
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
        assert not config_dir.exists()  # torn down with the run

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
