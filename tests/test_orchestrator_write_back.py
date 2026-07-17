"""Daemon wiring: a Claude run's refreshed credential is written back to the DB
(OAuth in UI 5/7). The write-back unit is covered in test_credential_write_back;
this pins the orchestrator seam — a connected Claude row is re-persisted from the
credential file after a run, across two sequential runs with no re-auth."""

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
async def test_write_back_persists_refreshed_credential_across_two_runs(tmp_path: Path) -> None:
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cred_path = tmp_path / ".credentials.json"
        harness.orch._claude_credentials_path = cred_path  # noqa: SLF001
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-v0"), cipher=cipher
        )

        # Run 1 refreshes the on-disk token → write-back persists it.
        cred_path.write_text(_cred("tok-v1"), encoding="utf-8")
        await harness.orch._write_back_claude_credentials()  # noqa: SLF001
        assert await db.oauth_connections.get_credential(harness.conn, "claude", cipher) == _cred(
            "tok-v1"
        )

        # Run 2 refreshes again → still no re-auth, DB tracks the latest.
        cred_path.write_text(_cred("tok-v2"), encoding="utf-8")
        await harness.orch._write_back_claude_credentials()  # noqa: SLF001
        assert await db.oauth_connections.get_credential(harness.conn, "claude", cipher) == _cred(
            "tok-v2"
        )
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_write_back_noop_when_claude_not_connected(tmp_path: Path) -> None:
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cred_path = tmp_path / ".credentials.json"
        cred_path.write_text(_cred("ambient"), encoding="utf-8")
        harness.orch._claude_credentials_path = cred_path  # noqa: SLF001

        await harness.orch._write_back_claude_credentials()  # noqa: SLF001

        # Never connected in the UI → ambient file must not be slurped into DB.
        assert await db.oauth_connections.get_status(harness.conn, "claude") is None
    finally:
        await harness.close()
