"""Runtime credential write-back (OAuth in UI 5/7).

Claude/Codex refresh their access token at runtime, mutating the credential
file a run reads. After a run the daemon reads it back and, if it changed,
re-encrypts + persists to the DB — so the next run (or a redeploy that lost the
auth volume) still authenticates without a re-auth. Writes are serialized per
provider so two concurrent runs finishing at once can't clobber each other.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from symphony import db
from symphony.credentials import CredentialWriteBack
from symphony.crypto import CredentialCipher

_KEY = "deployment-secret"


async def _connect(tmp_path: Path):
    return await db.connect(tmp_path / "state.sqlite")


async def _connect_claude(conn, credential: str) -> None:
    await db.oauth_connections.set_connection(
        conn,
        provider="claude",
        credential=credential,
        cipher=CredentialCipher(_KEY),
        status="connected",
    )


@pytest.mark.asyncio
async def test_write_back_persists_changed_credential(tmp_path: Path) -> None:
    conn = await _connect(tmp_path)
    try:
        await _connect_claude(conn, "cred-v1")
        cipher = CredentialCipher(_KEY)
        wb = CredentialWriteBack(conn, cipher)

        changed = await wb.write_back("claude", "cred-v2", expires_at="2030-01-01T00:00:00Z")

        assert changed is True
        stored = await db.oauth_connections.get_credential(conn, "claude", cipher)
        assert stored == "cred-v2"
        status = await db.oauth_connections.get_status(conn, "claude")
        assert status is not None
        assert status.status == "connected"
        assert status.expires_at == "2030-01-01T00:00:00Z"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_write_back_noop_when_unchanged(tmp_path: Path) -> None:
    conn = await _connect(tmp_path)
    try:
        await _connect_claude(conn, "cred-v1")
        wb = CredentialWriteBack(conn, CredentialCipher(_KEY))
        assert await wb.write_back("claude", "cred-v1") is False
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_write_back_noop_when_provider_never_connected(tmp_path: Path) -> None:
    """No DB row → the credential is ambient (env/volume only). Write-back must
    not slurp it into the store unsolicited."""
    conn = await _connect(tmp_path)
    try:
        wb = CredentialWriteBack(conn, CredentialCipher(_KEY))
        assert await wb.write_back("claude", "ambient-cred") is False
        assert await db.oauth_connections.get_status(conn, "claude") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_concurrent_write_back_is_serialized_per_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = await _connect(tmp_path)
    try:
        await _connect_claude(conn, "cred-v0")
        wb = CredentialWriteBack(conn, CredentialCipher(_KEY))

        events: list[str] = []
        real_get = db.oauth_connections.get_credential
        real_set = db.oauth_connections.set_connection

        async def traced_get(*args, **kwargs):
            events.append("read")
            await asyncio.sleep(0)  # force a yield so an unlocked flow interleaves
            return await real_get(*args, **kwargs)

        async def traced_set(*args, **kwargs):
            events.append("write")
            await asyncio.sleep(0)
            return await real_set(*args, **kwargs)

        monkeypatch.setattr(db.oauth_connections, "get_credential", traced_get)
        monkeypatch.setattr(db.oauth_connections, "set_connection", traced_set)

        await asyncio.gather(
            wb.write_back("claude", "cred-a"),
            wb.write_back("claude", "cred-b"),
        )

        # Serialized: each call's read+write is a contiguous pair. If the lock
        # were missing, both reads would land before either write.
        assert events in (["read", "write", "read", "write"],), events
    finally:
        await conn.close()
