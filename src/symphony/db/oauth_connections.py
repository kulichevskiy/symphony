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

from ..crypto import (
    CredentialCipher,
    CredentialDecryptError,
    CredentialKeyMissingError,
    EncryptionKeyLostError,
)

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
    # Which minting of this provider's credential the row currently holds
    # (SYM-233). Advanced by `set_connection` on every credential replacement,
    # so a run stamped with an older value is known to be holding a superseded
    # token without anything having to compare the secrets themselves.
    generation: int = 1


def _row_to_status(row: aiosqlite.Row) -> ConnectionStatus:
    return ConnectionStatus(
        provider=str(row["provider"]),
        status=str(row["status"]),
        expires_at=None if row["expires_at"] is None else str(row["expires_at"]),
        updated_at=str(row["updated_at"]),
        updated_by=str(row["updated_by"]),
        generation=int(row["generation"]),
    )


async def get_status(conn: aiosqlite.Connection, provider: str) -> ConnectionStatus | None:
    """The provider's non-secret status row, or `None` if never connected."""
    cur = await conn.execute(
        "SELECT provider, status, expires_at, updated_at, updated_by, generation "
        "FROM oauth_connections WHERE provider = ?",
        (provider,),
    )
    row = await cur.fetchone()
    return _row_to_status(row) if row is not None else None


async def list_statuses(conn: aiosqlite.Connection) -> list[ConnectionStatus]:
    """Every provider's status row (credential column never read)."""
    cur = await conn.execute(
        "SELECT provider, status, expires_at, updated_at, updated_by, generation "
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


@dataclass(frozen=True)
class ConnectionSnapshot:
    """One row read whole: the decrypted credential plus the status and
    generation it was stored under."""

    credential: str
    generation: int
    status: str


async def get_connection_snapshot(
    conn: aiosqlite.Connection, provider: str, cipher: CredentialCipher
) -> ConnectionSnapshot | None:
    """The provider's credential, generation and status from a single row read;
    `None` if there is no row. Same decrypt-error contract as `get_credential`.

    Reading these apart leaves a window for a write to land between them and
    produce a mixture of two row versions. Both mixtures bite (SYM-233/234): a
    run stamped with a generation newer than the token it holds later reads as
    "holds the newest token" and provokes a rotation instead of a hand-out, and
    a `connected` status paired with a credential a liveness Test has since
    rejected lets a request through an armed gate."""
    cur = await conn.execute(
        "SELECT credential, generation, status FROM oauth_connections WHERE provider = ?",
        (provider,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return ConnectionSnapshot(
        credential=cipher.decrypt(bytes(row["credential"])),
        generation=int(row["generation"]),
        status=str(row["status"]),
    )


async def get_refresh_token(
    conn: aiosqlite.Connection, provider: str, cipher: CredentialCipher
) -> str | None:
    """Decrypt and return the provider's stored refresh token, or `None` if
    there is no row or the provider's token exchange never returned one
    (e.g. GitHub). Same decrypt-error contract as `get_credential`."""
    cur = await conn.execute(
        "SELECT refresh_token FROM oauth_connections WHERE provider = ?", (provider,)
    )
    row = await cur.fetchone()
    if row is None or row["refresh_token"] is None:
        return None
    return cipher.decrypt(bytes(row["refresh_token"]))


async def set_connection(
    conn: aiosqlite.Connection,
    *,
    provider: str,
    credential: str,
    cipher: CredentialCipher,
    refresh_token: str | None = None,
    status: str = "connected",
    expires_at: str | None = None,
    updated_at: str = "",
    updated_by: str = "",
    expect_connected_generation: int | None = None,
    commit: bool = True,
) -> bool:
    """Encrypt `credential` (and `refresh_token`, if the provider's token
    exchange returned one) and upsert the provider's row. Returns whether the
    row was written.

    `commit=False` lets a caller fold this into a larger atomic transaction it
    commits itself.

    `expect_connected_generation` makes the upsert compare-and-set *inside the
    statement* (SYM-234): the write lands only if the row is still `connected`
    and still holds that generation. A caller that checks those separately
    leaves a window in which a liveness Test expires the row between its check
    and its write — and since this function re-persists as `connected`, the
    write would silently clear a reconnect gate that had just been armed. The
    generation counter still advances on a refused write, leaving a gap; gaps
    are harmless because the counter only ever has to be monotonic, never dense.

    Every upsert advances the provider's `generation` (SYM-233) — this is the
    one choke point through which a replacement credential reaches the store,
    so the counter is exactly "how many times has this provider's credential
    been minted". Callers that must not bump it (a status flip, a no-op
    write-back) don't come through here: `write_back` returns early when the
    credential is unchanged, and `update_status` leaves the column alone.

    The counter is kept in `oauth_credential_generations`, which `delete` does
    not touch, so it keeps climbing across a Disconnect → Reconnect instead of
    restarting at 1 and colliding with the stamp of a run still in flight.
    """
    encrypted = cipher.encrypt(credential)
    encrypted_refresh = cipher.encrypt(refresh_token) if refresh_token is not None else None
    cur = await conn.execute(
        """
        INSERT INTO oauth_credential_generations (provider, generation) VALUES (?, 1)
        ON CONFLICT(provider) DO UPDATE SET generation = oauth_credential_generations.generation + 1
        RETURNING generation
        """,
        (provider,),
    )
    row = await cur.fetchone()
    generation = int(row["generation"]) if row is not None else 1
    guard = (
        ""
        if expect_connected_generation is None
        else " WHERE oauth_connections.status = 'connected' AND oauth_connections.generation = ?"
    )
    params: list[object] = [
        provider,
        encrypted,
        encrypted_refresh,
        status,
        expires_at,
        updated_at,
        updated_by,
        generation,
    ]
    if expect_connected_generation is not None:
        params.append(expect_connected_generation)
    cur = await conn.execute(
        f"""
        INSERT INTO oauth_connections
            (provider, credential, refresh_token, status, expires_at, updated_at, updated_by,
             generation)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider) DO UPDATE SET
            credential = excluded.credential,
            refresh_token = excluded.refresh_token,
            status = excluded.status,
            expires_at = excluded.expires_at,
            updated_at = excluded.updated_at,
            updated_by = excluded.updated_by,
            generation = excluded.generation
        {guard}
        """,
        params,
    )
    if commit:
        await conn.commit()
    return cur.rowcount > 0


async def update_status(
    conn: aiosqlite.Connection,
    *,
    provider: str,
    status: str,
    updated_at: str = "",
    updated_by: str = "",
    expected_generation: int | None = None,
    commit: bool = True,
) -> None:
    """Flip a connection's `status` (e.g. `connected`→`expired` after a failed
    liveness `Test`) without touching the encrypted credential column. A no-op
    if the provider has no row.

    `expected_generation` makes the flip compare-and-set (SYM-234): the caller
    passes the generation it decided on, and the write no-ops if the credential
    has been replaced since. An operator reconnect that lands between a caller's
    read and its write must not be expired by a verdict reached about the token
    it replaced — that would re-arm the fleet-wide dispatch gate seconds after
    the reconnect cleared it."""
    if expected_generation is None:
        await conn.execute(
            "UPDATE oauth_connections SET status = ?, updated_at = ?, updated_by = ? "
            "WHERE provider = ?",
            (status, updated_at, updated_by, provider),
        )
    else:
        await conn.execute(
            "UPDATE oauth_connections SET status = ?, updated_at = ?, updated_by = ? "
            "WHERE provider = ? AND generation = ?",
            (status, updated_at, updated_by, provider, expected_generation),
        )
    if commit:
        await conn.commit()


async def assert_cipher_usable(conn: aiosqlite.Connection, cipher: CredentialCipher) -> None:
    """Boot guard (Config v2 2/9): when encrypted credential rows exist, the
    effective key must decrypt them. Raises `EncryptionKeyLostError` (listing
    the affected providers) so a lost/rotated key crashes the boot with an
    instruction instead of surfacing as silent OAuth 503s at runtime. A store
    with no encrypted rows passes regardless of the cipher."""
    cur = await conn.execute(
        "SELECT provider, credential, refresh_token FROM oauth_connections"
        " WHERE length(credential) > 0"
    )
    rows = await cur.fetchall()
    if not rows:
        return
    broken: set[str] = set()
    for row in rows:
        try:
            cipher.decrypt(bytes(row["credential"]))
            # The refresh token matters as much as the access token — a Linear
            # row whose access token decrypts but whose refresh token is corrupt
            # dies at the first in-place refresh (Config v2 2/9 review fix).
            if row["refresh_token"] is not None:
                cipher.decrypt(bytes(row["refresh_token"]))
        except (CredentialDecryptError, CredentialKeyMissingError):
            broken.add(str(row["provider"]))
    if broken:
        raise EncryptionKeyLostError(sorted(broken))


async def delete(conn: aiosqlite.Connection, provider: str, *, commit: bool = True) -> None:
    """Drop the provider's row entirely — `Disconnect` clears the connection, so
    the encrypted credential is gone, not merely marked disconnected. Idempotent.

    Deliberately leaves `oauth_credential_generations` alone: the generation
    must keep climbing across a reconnect, or a run stamped before the
    disconnect could later be mistaken for one holding the current token."""
    await conn.execute("DELETE FROM oauth_connections WHERE provider = ?", (provider,))
    if commit:
        await conn.commit()
