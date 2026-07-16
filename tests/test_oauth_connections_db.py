"""DAO for the `oauth_connections` table (OAuth in UI 1/7): one row per
provider holding an encrypted credential payload plus status/expiry metadata."""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.crypto import CredentialCipher, CredentialDecryptError


async def _conn(tmp_path: Path):  # type: ignore[no-untyped-def]
    return await db.connect(tmp_path / "state.sqlite")


@pytest.mark.asyncio
async def test_round_trips_encrypted_payload(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    cipher = CredentialCipher("deployment-secret")
    try:
        assert await db.oauth_connections.get_status(conn, "github") is None
        await db.oauth_connections.set_connection(
            conn,
            provider="github",
            credential="gho_secret_token",
            cipher=cipher,
            status="connected",
            expires_at="2026-08-01T00:00:00Z",
            updated_at="2026-07-16T00:00:00Z",
            updated_by="op@x",
        )

        status = await db.oauth_connections.get_status(conn, "github")
        assert status is not None
        assert status.status == "connected"
        assert status.expires_at == "2026-08-01T00:00:00Z"
        assert status.updated_by == "op@x"

        assert await db.oauth_connections.get_credential(conn, "github", cipher) == (
            "gho_secret_token"
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stored_bytes_are_ciphertext_not_plaintext(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    cipher = CredentialCipher("deployment-secret")
    try:
        await db.oauth_connections.set_connection(
            conn, provider="linear", credential="lin_plain_value", cipher=cipher
        )
        cur = await conn.execute(
            "SELECT credential FROM oauth_connections WHERE provider = ?", ("linear",)
        )
        row = await cur.fetchone()
        assert row is not None
        stored = row["credential"]
        assert isinstance(stored, bytes)
        assert stored != b"lin_plain_value"
        assert b"lin_plain_value" not in stored
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_rotated_key_read_is_reauthorize_error(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        await db.oauth_connections.set_connection(
            conn,
            provider="claude",
            credential="payload",
            cipher=CredentialCipher("old-key"),
        )
        with pytest.raises(CredentialDecryptError):
            await db.oauth_connections.get_credential(conn, "claude", CredentialCipher("new-key"))
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_list_statuses_empty_on_fresh_db(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        assert await db.oauth_connections.list_statuses(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_delete_removes_the_row(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    cipher = CredentialCipher("k")
    try:
        await db.oauth_connections.set_connection(
            conn, provider="github", credential="gho_x", cipher=cipher
        )
        await db.oauth_connections.delete(conn, "github")
        assert await db.oauth_connections.get_status(conn, "github") is None
        # Idempotent — deleting a missing row is a no-op, not an error.
        await db.oauth_connections.delete(conn, "github")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_update_status_leaves_credential_intact(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    cipher = CredentialCipher("k")
    try:
        await db.oauth_connections.set_connection(
            conn, provider="github", credential="gho_x", cipher=cipher, status="connected"
        )
        await db.oauth_connections.update_status(conn, provider="github", status="expired")
        status = await db.oauth_connections.get_status(conn, "github")
        assert status is not None and status.status == "expired"
        # The credential is untouched — a re-test could still read it.
        assert await db.oauth_connections.get_credential(conn, "github", cipher) == "gho_x"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_set_replaces_existing_row(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    cipher = CredentialCipher("k")
    try:
        await db.oauth_connections.set_connection(
            conn, provider="codex", credential="a", cipher=cipher, status="connected"
        )
        await db.oauth_connections.set_connection(
            conn, provider="codex", credential="b", cipher=cipher, status="expired"
        )
        status = await db.oauth_connections.get_status(conn, "codex")
        assert status is not None and status.status == "expired"
        assert await db.oauth_connections.get_credential(conn, "codex", cipher) == "b"
        assert len(await db.oauth_connections.list_statuses(conn)) == 1
    finally:
        await conn.close()
