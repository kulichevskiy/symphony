"""DAO for the `oauth_connections` table (OAuth in UI 1/7).

One row per onboarding provider (github/linear/claude/codex). The credential
payload is stored *encrypted* — `set_connection` encrypts on write and
`get_credential` decrypts on read via a `CredentialCipher`. The status view
(`get_status`/`list_statuses`) never touches the credential column, so the
read-only Connections API can report `status`/`expires_at` without a key: the
credential material never leaves the process. A missing row means the provider
has never been connected.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiosqlite

from ..crypto import CredentialCipher

# The providers surfaced on the Connections page, in display order.
PROVIDERS: tuple[str, ...] = ("github", "linear", "claude", "codex")

STATUS_NOT_CONNECTED = "not_connected"


@dataclass(frozen=True)
class ConnectionStatus:
    """Non-secret metadata for one provider's connection. Deliberately omits the
    credential column so it can never be serialized by accident."""

    provider: str
    status: str
    expires_at: str | None
    updated_at: str
    updated_by: str


def _row_to_status(row: aiosqlite.Row) -> ConnectionStatus:
    return ConnectionStatus(
        provider=str(row["provider"]),
        status=str(row["status"]),
        expires_at=None if row["expires_at"] is None else str(row["expires_at"]),
        updated_at=str(row["updated_at"]),
        updated_by=str(row["updated_by"]),
    )


async def get_status(conn: aiosqlite.Connection, provider: str) -> ConnectionStatus | None:
    """The provider's non-secret status row, or `None` if never connected."""
    cur = await conn.execute(
        "SELECT provider, status, expires_at, updated_at, updated_by "
        "FROM oauth_connections WHERE provider = ?",
        (provider,),
    )
    row = await cur.fetchone()
    return _row_to_status(row) if row is not None else None


async def list_statuses(conn: aiosqlite.Connection) -> list[ConnectionStatus]:
    """Every provider's status row (credential column never read)."""
    cur = await conn.execute(
        "SELECT provider, status, expires_at, updated_at, updated_by "
        "FROM oauth_connections ORDER BY provider ASC"
    )
    return [_row_to_status(row) for row in await cur.fetchall()]


async def get_credential(
    conn: aiosqlite.Connection, provider: str, cipher: CredentialCipher
) -> str | None:
    """Decrypt and return the provider's stored credential, or `None` if there
    is no row. Raises `CredentialDecryptError`/`CredentialKeyMissingError` (both
    "must re-authorize") if the key is missing or no longer matches the stored
    ciphertext — never a raw traceback."""
    cur = await conn.execute(
        "SELECT credential FROM oauth_connections WHERE provider = ?", (provider,)
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return cipher.decrypt(bytes(row["credential"]))


async def set_connection(
    conn: aiosqlite.Connection,
    *,
    provider: str,
    credential: str,
    cipher: CredentialCipher,
    status: str = "connected",
    expires_at: str | None = None,
    updated_at: str = "",
    updated_by: str = "",
    commit: bool = True,
) -> None:
    """Encrypt `credential` and upsert the provider's row.

    `commit=False` lets a caller fold this into a larger atomic transaction it
    commits itself.
    """
    encrypted = cipher.encrypt(credential)
    await conn.execute(
        """
        INSERT INTO oauth_connections
            (provider, credential, status, expires_at, updated_at, updated_by)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider) DO UPDATE SET
            credential = excluded.credential,
            status = excluded.status,
            expires_at = excluded.expires_at,
            updated_at = excluded.updated_at,
            updated_by = excluded.updated_by
        """,
        (provider, encrypted, status, expires_at, updated_at, updated_by),
    )
    if commit:
        await conn.commit()


async def update_status(
    conn: aiosqlite.Connection,
    *,
    provider: str,
    status: str,
    updated_at: str = "",
    updated_by: str = "",
    commit: bool = True,
) -> None:
    """Flip a connection's `status` (e.g. `connected`→`expired` after a failed
    liveness `Test`) without touching the encrypted credential column. A no-op
    if the provider has no row."""
    await conn.execute(
        "UPDATE oauth_connections SET status = ?, updated_at = ?, updated_by = ? "
        "WHERE provider = ?",
        (status, updated_at, updated_by, provider),
    )
    if commit:
        await conn.commit()


async def delete(conn: aiosqlite.Connection, provider: str, *, commit: bool = True) -> None:
    """Drop the provider's row entirely — `Disconnect` clears the connection, so
    the encrypted credential is gone, not merely marked disconnected. Idempotent."""
    await conn.execute("DELETE FROM oauth_connections WHERE provider = ?", (provider,))
    if commit:
        await conn.commit()
