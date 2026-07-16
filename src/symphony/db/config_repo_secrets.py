"""DAO for the `config_repo_secrets` table (SYM-194).

One row per GitHub repo holding that repo's webhook secret, plus a `version`
for optimistic locking and `updated_at/by` metadata. The secret is keyed by
repo — not by binding — because GitHub signature verification is per repo, so
two bindings on one repo can only ever use one secret; per-binding storage
would let the UI save a secret verification never consults.

The value is *write-only*: it is never served in an API response, export, or
log. The DAO stores and reads it; redaction is enforced by the callers (the
config-CRUD serializer, the audit diff, the read-only config view).

`version` guards concurrent edits: two browser tabs editing different bindings
of the same repo would otherwise race on the shared secret without a conflict,
since a binding-row version can't protect a value that lives outside any
binding payload.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import aiosqlite


@dataclass(frozen=True)
class RepoSecret:
    """One `config_repo_secrets` row. An empty `secret` means "unset/cleared"."""

    github_repo: str
    secret: str
    version: int
    updated_at: str
    updated_by: str


class RepoSecretView:
    """In-process view of per-repo webhook secrets, built from the DB at boot
    and hot-swapped by the config write path (SYM-194).

    The webhook verifier resolves its `repo_secrets` from this view on every
    request rather than from a snapshot frozen at app startup, so a secret
    set/replaced/cleared through the UI takes effect without a tick reload or a
    restart — GitHub would otherwise sign with the new secret while Symphony
    kept checking the old one. The view is a same-process object shared by the
    CRUD write path and the verifier; the DB row stays the durable source of
    truth (the view is repopulated from it at boot)."""

    def __init__(self, initial: Mapping[str, str] | None = None) -> None:
        self._secrets: dict[str, str] = {
            repo: secret for repo, secret in (initial or {}).items() if secret
        }

    def set(self, github_repo: str, secret: str) -> None:
        """Set (non-empty) or clear (empty) a repo's live secret."""
        if secret:
            self._secrets[github_repo] = secret
        else:
            self._secrets.pop(github_repo, None)

    def as_map(self) -> dict[str, str]:
        """The current repo→secret map, for building `GitHubWebhookSettings`."""
        return dict(self._secrets)


async def load_view(conn: aiosqlite.Connection) -> RepoSecretView:
    """Build a `RepoSecretView` from the DB's current repo-secret rows."""
    return RepoSecretView({row.github_repo: row.secret for row in await list_all(conn)})


class StaleVersionError(Exception):
    """Optimistic-locking conflict on a repo's webhook secret: the stored
    `version` no longer matches the one the caller loaded (a concurrent edit
    from another binding of the same repo). Carries the current version so the
    API can render it (0 when no row exists yet)."""

    def __init__(self, github_repo: str, current_version: int) -> None:
        self.github_repo = github_repo
        self.current_version = current_version
        super().__init__(
            f"repo webhook secret {github_repo!r} version conflict "
            f"(current={current_version}); reload and retry"
        )


def _row_to_secret(row: aiosqlite.Row) -> RepoSecret:
    return RepoSecret(
        github_repo=str(row["github_repo"]),
        secret=str(row["secret"]),
        version=int(row["version"]),
        updated_at=str(row["updated_at"]),
        updated_by=str(row["updated_by"]),
    )


async def get(conn: aiosqlite.Connection, github_repo: str) -> RepoSecret | None:
    """The repo's secret row, or `None` if the repo has never had one set."""
    cur = await conn.execute(
        "SELECT github_repo, secret, version, updated_at, updated_by "
        "FROM config_repo_secrets WHERE github_repo = ?",
        (github_repo,),
    )
    row = await cur.fetchone()
    return _row_to_secret(row) if row is not None else None


async def list_all(conn: aiosqlite.Connection) -> list[RepoSecret]:
    """Every repo-secret row — the source the in-process verifier view is built
    from at boot."""
    cur = await conn.execute(
        "SELECT github_repo, secret, version, updated_at, updated_by "
        "FROM config_repo_secrets ORDER BY github_repo ASC"
    )
    return [_row_to_secret(row) for row in await cur.fetchall()]


async def set_secret(
    conn: aiosqlite.Connection,
    *,
    github_repo: str,
    secret: str,
    expected_version: int | None,
    updated_at: str = "",
    updated_by: str = "",
    commit: bool = True,
) -> RepoSecret:
    """Set (or clear, with `secret=""`) a repo's webhook secret under optimistic
    locking, bumping `version`.

    `expected_version` is the version the caller loaded: 0 for a repo with no
    row yet, else the stored version. A mismatch raises `StaleVersionError`.
    `None` skips the check entirely (unconditional overwrite) — the back-compat
    path for a save that carries a value but no loaded repo-secret version.

    The check-and-write is a single conditional statement (mirroring
    `config_globals.update_roles`), so two connections racing on the same
    `expected_version` can't both win.

    `commit=False` lets a caller fold this into a larger atomic transaction it
    commits itself (the config write spans binding + repo secret in one tx).
    """
    if expected_version is None:
        current = await get(conn, github_repo)
        new_version = (current.version if current is not None else 0) + 1
        await conn.execute(
            """
            INSERT INTO config_repo_secrets (github_repo, secret, version, updated_at, updated_by)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(github_repo) DO UPDATE SET
                secret = excluded.secret,
                version = excluded.version,
                updated_at = excluded.updated_at,
                updated_by = excluded.updated_by
            """,
            (github_repo, secret, new_version, updated_at, updated_by),
        )
        if commit:
            await conn.commit()
        refreshed = await get(conn, github_repo)
        assert refreshed is not None
        return refreshed

    new_version = expected_version + 1
    if expected_version == 0:
        cur = await conn.execute(
            """
            INSERT INTO config_repo_secrets (github_repo, secret, version, updated_at, updated_by)
            SELECT ?, ?, ?, ?, ?
             WHERE NOT EXISTS (SELECT 1 FROM config_repo_secrets WHERE github_repo = ?)
            """,
            (github_repo, secret, new_version, updated_at, updated_by, github_repo),
        )
    else:
        cur = await conn.execute(
            """
            UPDATE config_repo_secrets
               SET secret = ?, version = ?, updated_at = ?, updated_by = ?
             WHERE github_repo = ? AND version = ?
            """,
            (secret, new_version, updated_at, updated_by, github_repo, expected_version),
        )
    if cur.rowcount != 1:
        current = await get(conn, github_repo)
        # The conditional INSERT/UPDATE opened a write transaction even though it
        # matched zero rows; roll it back before raising so it doesn't hold the
        # connection's write lock until some later unrelated commit (same
        # concern as `config_globals.update_roles`).
        await conn.rollback()
        raise StaleVersionError(github_repo, current.version if current is not None else 0)
    if commit:
        await conn.commit()
    refreshed = await get(conn, github_repo)
    assert refreshed is not None
    return refreshed
