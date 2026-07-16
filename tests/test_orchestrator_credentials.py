"""Daemon wiring: a run's creds are resolved DB-first with env/volume fallback
(OAuth in UI 4/7). The resolver + materialization units are covered in
test_credentials.py; this pins the orchestrator's `_resolve_run_credentials`
seam — DB connection wins, missing provider falls back per the per-binding
model."""

from __future__ import annotations

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
        linear_api_key="env_linear_key",
        symphony_encryption_key=ENC_KEY,
        repos=[
            RepoBinding(
                linear_team_key="ENG",
                github_repo="org/repo",
                linear_states=LinearStates(
                    ready="Todo",
                    in_progress="In Progress",
                    code_review="Needs Approval",
                ),
            )
        ],
    )


@pytest.mark.asyncio
async def test_run_credentials_prefer_db_and_fall_back(tmp_path: Path) -> None:
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        binding = harness.config.repos[0]
        # No DB rows yet: GitHub has no env fallback here → None (rely on the
        # ambient gh-auth volume); Linear falls back to the configured key.
        creds = await harness.orch._resolve_run_credentials(binding)  # noqa: SLF001
        assert creds.github_token is None
        assert creds.linear_token == "env_linear_key"

        # Connect GitHub via the UI store → the run now drives off the DB token.
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="github",
            credential="gho_db_token",
            cipher=CredentialCipher(ENC_KEY),
        )
        creds = await harness.orch._resolve_run_credentials(binding)  # noqa: SLF001
        assert creds.github_token == "gho_db_token"
        # Linear still has no DB row → still the env fallback (per-provider).
        assert creds.linear_token == "env_linear_key"
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_linear_credential_never_resolved_for_jira_tracked_binding(
    tmp_path: Path,
) -> None:
    """A Jira-tracked binding must never see the Linear DB token, even when a
    connected Linear row exists — the per-binding allowlist model requires
    creds don't leak into a Jira-tracked binding's run."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        binding = harness.config.repos[0].model_copy(
            update={
                "provider": "jira",
                "tracker_provider": "jira",
                "base_url": "https://example.atlassian.net",
            }
        )
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="linear",
            credential="lin_db_secret",
            cipher=CredentialCipher(ENC_KEY),
        )
        creds = await harness.orch._resolve_run_credentials(binding)  # noqa: SLF001
        assert creds.linear_token is None
    finally:
        await harness.close()
