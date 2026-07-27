"""SYM-233: a claude run takes its access token from the environment, and the
daemon records which generation of the shared token that run was handed.

Two things are pinned here. First, the *source*: the CLI's mid-run auth
recovery is armed only when the token arrived through the environment, so a
claude run must no longer be fed a materialized credentials file. Second, the
*stamp*: there is exactly one Claude connection shared by every run, so "which
token does this run hold" is answered by a counter rather than by comparing
secrets — and that answer has to outlive a daemon restart.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.crypto import CredentialCipher
from tests.harness import Harness

ENC_KEY = "deployment-secret"
CLAUDE_TOKEN_ENV = "CLAUDE_CODE_OAUTH_TOKEN"


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


def _cred(token: str, *, refresh: str | None = None, expires_ms: int = 4102444800000) -> str:
    blob: dict[str, object] = {"accessToken": token, "expiresAt": expires_ms}
    if refresh is not None:
        blob["refreshToken"] = refresh
    return json.dumps({"claudeAiOauth": blob})


async def _seed_run(conn, run_id: str) -> None:  # type: ignore[no-untyped-def]
    await db.issues.upsert(conn, id="iss-1", identifier="ENG-1", title="Add auth", team_key="ENG")
    await db.runs.create(
        conn,
        id=run_id,
        issue_id="iss-1",
        stage="implement",
        status="running",
        pid=None,
        started_at="2026-07-27T00:00:00+00:00",
    )


@pytest.mark.asyncio
async def test_claude_run_takes_its_token_from_the_environment(tmp_path: Path) -> None:
    """A connected Claude row materializes as an environment token — no config
    dir, and nothing written to disk for the run to leave behind."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred("tok-v0"),
            cipher=CredentialCipher(ENC_KEY),
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert env == {CLAUDE_TOKEN_ENV: "tok-v0"}
        assert "CLAUDE_CONFIG_DIR" not in env
        # Nothing to tear down, and finalizing must not choke on a token value
        # where it used to find a directory path.
        await harness.orch._finalize_claude_env(env)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_the_refresh_token_never_reaches_the_run(tmp_path: Path) -> None:
    """The daemon owns rotation (SYM-227/228): a run gets the access token and
    nothing else, so it cannot burn the one-shot refresh token."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred("tok-v0", refresh="rt-secret"),
            cipher=CredentialCipher(ENC_KEY),
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert "rt-secret" not in "".join(env.values())
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_an_unparseable_credential_blocks_dispatch_rather_than_leaking_ambient_auth(
    tmp_path: Path,
) -> None:
    """A stored blob with no access token yields no env at all — the run must
    not silently fall through to whatever ambient auth the host has."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential="not json at all",
            cipher=CredentialCipher(ENC_KEY),
        )
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert env == {}
        # The row is expired, so the existing reconnect gate stops the run
        # rather than letting it start on whatever ambient auth the host
        # carries — and the Connections card says so instead of reading
        # `connected` while every dispatch quietly fails.
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None
        assert status.status == "expired"
        blocked = await harness.orch._post_materialize_block_reason("claude", env)  # noqa: SLF001
        assert blocked is not None
        assert "reconnect" in blocked
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_an_unconnected_claude_still_dispatches_on_ambient_auth(tmp_path: Path) -> None:
    """No UI connection at all is not a failure: a deployment that never
    connected Claude keeps running on its ambient host auth, as before."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        env = await harness.orch._materialize_claude_env("claude")  # noqa: SLF001
        assert env == {}
        assert await harness.orch._post_materialize_block_reason("claude", env) is None  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_generation_advances_only_when_the_stored_token_is_replaced(
    tmp_path: Path,
) -> None:
    """The generation counts mintings of the shared token: replacing the
    credential advances it, flipping the row's status does not."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-v0"), cipher=cipher
        )
        first = await db.oauth_connections.get_status(harness.conn, "claude")
        assert first is not None

        await db.oauth_connections.update_status(harness.conn, provider="claude", status="expired")
        gated = await db.oauth_connections.get_status(harness.conn, "claude")
        assert gated is not None
        assert gated.generation == first.generation

        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-v1"), cipher=cipher
        )
        second = await db.oauth_connections.get_status(harness.conn, "claude")
        assert second is not None
        assert second.generation == first.generation + 1
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_generation_keeps_climbing_across_a_disconnect(tmp_path: Path) -> None:
    """Disconnect drops the connection row, but the counter must not restart —
    a run stamped before the disconnect would otherwise collide with a
    re-climbed generation and be mistaken for a holder of the current token."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        for token in ("tok-v0", "tok-v1", "tok-v2"):
            await db.oauth_connections.set_connection(
                harness.conn, provider="claude", credential=_cred(token), cipher=cipher
            )
        before = await db.oauth_connections.get_status(harness.conn, "claude")
        assert before is not None
        assert before.generation == 3

        await db.oauth_connections.delete(harness.conn, "claude")
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-reconnected"), cipher=cipher
        )
        after = await db.oauth_connections.get_status(harness.conn, "claude")
        assert after is not None
        assert after.generation == before.generation + 1
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_the_token_and_its_generation_are_read_as_one_snapshot(tmp_path: Path) -> None:
    """The credential and the generation it was stored under come back from a
    single read, so nothing can land between them and pair a token with a
    generation it doesn't belong to."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-v0"), cipher=cipher
        )
        first = await db.oauth_connections.get_connection_snapshot(harness.conn, "claude", cipher)
        assert first is not None
        assert (first.credential, first.generation, first.status) == (
            _cred("tok-v0"),
            1,
            "connected",
        )

        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-v1"), cipher=cipher
        )
        second = await db.oauth_connections.get_connection_snapshot(harness.conn, "claude", cipher)
        assert second is not None
        assert (second.credential, second.generation) == (_cred("tok-v1"), 2)

        assert (
            await db.oauth_connections.get_connection_snapshot(harness.conn, "codex", cipher)
            is None
        )
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_a_run_records_the_generation_it_was_dispatched_with(tmp_path: Path) -> None:
    """Each run is stamped with the generation it started on, and the stamp
    survives a daemon restart — a later tick has to be able to tell a run
    holding the current token from one holding a superseded one."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        cipher = CredentialCipher(ENC_KEY)
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-v0"), cipher=cipher
        )
        await _seed_run(harness.conn, "run-1")
        await harness.orch._materialize_claude_env("claude", run_id="run-1")  # noqa: SLF001
        current = await db.oauth_connections.get_status(harness.conn, "claude")
        assert current is not None
        assert await db.runs.claude_token_generation(harness.conn, "run-1") == current.generation

        # A rotation after dispatch leaves the earlier run's stamp behind, and
        # a run dispatched after it carries the newer generation.
        await db.oauth_connections.set_connection(
            harness.conn, provider="claude", credential=_cred("tok-v1"), cipher=cipher
        )
        await db.runs.create(
            harness.conn,
            id="run-2",
            issue_id="iss-1",
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-07-27T00:10:00+00:00",
        )
        await harness.orch._materialize_claude_env("claude", run_id="run-2")  # noqa: SLF001

        await harness.restart()
        stamp_1 = await db.runs.claude_token_generation(harness.conn, "run-1")
        stamp_2 = await db.runs.claude_token_generation(harness.conn, "run-2")
        assert stamp_1 == current.generation
        assert stamp_2 == current.generation + 1
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_a_run_dispatched_without_a_connection_carries_no_stamp(tmp_path: Path) -> None:
    """No UI connection means the run authenticates ambiently — there is no
    generation to record, and the absence must be distinguishable from zero."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    try:
        await _seed_run(harness.conn, "run-1")
        assert await harness.orch._materialize_claude_env("claude", run_id="run-1") == {}  # noqa: SLF001
        assert await db.runs.claude_token_generation(harness.conn, "run-1") is None
    finally:
        await harness.close()
