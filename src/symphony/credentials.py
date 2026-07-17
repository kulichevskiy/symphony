"""Runtime credential resolution + materialization (OAuth in UI 4/7).

Two pieces make the UI-stored connections actually drive agent runs:

- `CredentialResolver` returns a provider's credential **DB-first** — decrypted
  from `oauth_connections` — falling back to the caller-supplied env/volume
  value when the provider has no *usable* DB connection. Migration is therefore
  per-provider and zero-downtime: a provider nobody has connected in the UI
  keeps running on `GH_TOKEN` / `LINEAR_API_KEY` exactly as before. A rotated or
  corrupt key (ciphertext no longer decrypts) also falls back rather than
  crashing the run — the connection is dead, but the instance keeps working.

- `materialize_credentials` writes the resolved creds into a private, per-run
  directory as a git credential store and returns the env additions a run
  needs: a git credential helper (so an HTTPS `git push` inside the run
  authenticates), `GH_TOKEN`/`GH_ENTERPRISE_TOKEN` for `gh`, and the Linear
  bearer. The directory is torn down after the run by the runner — it is never
  a persistent volume file, and `HOME` is deliberately left untouched so the
  agent CLIs (Claude/Codex) keep resolving their own auth from it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

from . import db
from .crypto import CredentialCipher, CredentialDecryptError, CredentialKeyMissingError

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunCredentials:
    """Credentials resolved for one run. `None` means "not resolved" — the run
    inherits whatever the ambient env/volume already provides for that provider."""

    github_token: str | None = None
    # Already the full `Authorization` header value (`Bearer <token>` for a
    # DB-resolved OAuth token, unprefixed for an env/volume PAT fallback) —
    # see `CredentialResolver.resolve_linear_auth_header`.
    linear_token: str | None = None

    @property
    def is_empty(self) -> bool:
        return not self.github_token and not self.linear_token


def _is_expired(expires_at: str) -> bool:
    """Whether `expires_at` (the `%Y-%m-%dT%H:%M:%SZ` format `oauth.py` writes)
    is in the past. An unparseable value is treated as not-expired rather than
    forcing a fallback on a format we don't recognize."""
    try:
        deadline = datetime.strptime(expires_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        return False
    return datetime.now(UTC) >= deadline


class CredentialResolver:
    """`provider → credential`, DB connection first, else env/volume fallback."""

    def __init__(self, conn: aiosqlite.Connection, cipher: CredentialCipher) -> None:
        self._conn = conn
        self._cipher = cipher

    async def resolve(self, provider: str, *, fallback: str | None = None) -> str | None:
        """The provider's credential, DB-first.

        Returns the decrypted DB credential only when the provider has a
        `connected` row that decrypts with the current key; otherwise returns
        `fallback`. A decrypt/key error is treated as "no usable DB connection"
        and falls back so a rotated key never takes the instance down mid-run.
        """
        value, _ = await self._resolve_with_source(provider, fallback=fallback)
        return value

    async def resolve_linear_auth_header(self, *, fallback: str | None = None) -> str | None:
        """The `Authorization` header value Linear calls should use, DB-first.

        A DB-connected Linear token is an OAuth access token and must be sent
        as `Bearer <token>` (`linear/client.py`'s own contract, matching the
        working UI probe in `oauth.py`); the env/volume fallback is a personal
        API key and must stay unprefixed. `resolve()` alone can't make this
        call — it returns a bare string with no record of which source it
        came from — so this formats the header directly from the resolved
        source.
        """
        value, from_db = await self._resolve_with_source("linear", fallback=fallback)
        if value and from_db:
            return f"Bearer {value}"
        return value

    async def _resolve_with_source(
        self, provider: str, *, fallback: str | None
    ) -> tuple[str | None, bool]:
        """`(value, from_db)` — `from_db` is True only when `value` came from
        a decrypted `oauth_connections` row, not the fallback."""
        try:
            status = await db.oauth_connections.get_status(self._conn, provider)
        except Exception:  # noqa: BLE001 — a DB read hiccup must not break the run
            log.warning("oauth_connections status read failed for %s; using fallback", provider)
            return fallback, False
        if status is None or status.status != "connected":
            return fallback, False
        if status.expires_at is not None and _is_expired(status.expires_at):
            log.warning(
                "%s connection expired at %s; using fallback", provider, status.expires_at
            )
            return fallback, False
        try:
            credential = await db.oauth_connections.get_credential(
                self._conn, provider, self._cipher
            )
        except (CredentialDecryptError, CredentialKeyMissingError):
            log.warning(
                "stored %s credential could not be decrypted (key missing/rotated); "
                "falling back to env/volume",
                provider,
            )
            return fallback, False
        if credential:
            return credential, True
        return fallback, False

    async def resolve_run_credentials(
        self, *, github_fallback: str | None = None, linear_fallback: str | None = None
    ) -> RunCredentials:
        """Resolve the providers a run materializes (GitHub + Linear) into a bundle."""
        return RunCredentials(
            github_token=await self.resolve("github", fallback=github_fallback),
            linear_token=await self.resolve_linear_auth_header(fallback=linear_fallback),
        )


def github_host_for_repo(repo: str) -> str:
    """The GitHub host a `[HOST/]OWNER/REPO` binding pushes to (see
    `GitHub._api_repo`'s parsing of the same format) — `github.com` when no
    host segment is present."""
    parts = repo.split("/")
    return parts[0] if len(parts) == 3 else "github.com"


def materialize_credentials(
    creds: RunCredentials,
    home_dir: Path,
    *,
    prior_gitconfig: Path | None = None,
    github_host: str = "github.com",
) -> dict[str, str]:
    """Write `creds` into `home_dir` (a private, torn-down run directory) and
    return the env additions the run needs.

    The GitHub token becomes both a git credential store (helper + credentials
    file, referenced via `GIT_CONFIG_GLOBAL` so `HOME` is never clobbered) and
    `GH_TOKEN`/`GH_ENTERPRISE_TOKEN` for `gh`. `GIT_CONFIG_GLOBAL` *replaces*
    (not supplements) the process's normal global gitconfig, so the written
    file `[include]`s `prior_gitconfig` (the pre-existing `GIT_CONFIG_GLOBAL`,
    or `~/.gitconfig` if unset) to keep resolving `user.name`/`user.email` and
    any other global settings — git silently ignores an `[include]` path that
    doesn't exist. The Linear token becomes `LINEAR_API_KEY`: already the exact
    `Authorization` header value the Linear client should send verbatim (see
    `RunCredentials.linear_token`) — `Bearer <token>` for a DB-resolved OAuth
    token, unprefixed for an env/volume PAT fallback. An empty bundle writes
    nothing and returns no env.
    """
    env: dict[str, str] = {}
    if creds.github_token:
        cred_file = home_dir / ".git-credentials"
        # `x-access-token` as the username is the GitHub convention; the token
        # is what authenticates. `store` matches by host on push.
        cred_file.write_text(
            f"https://x-access-token:{creds.github_token}@{github_host}\n", encoding="utf-8"
        )
        cred_file.chmod(0o600)
        include_path = prior_gitconfig or (Path.home() / ".gitconfig")
        gitconfig = home_dir / ".gitconfig"
        gitconfig.write_text(
            f"[include]\n\tpath = {include_path}\n"
            # `helper =` (empty) resets the helper list git accumulated from
            # the included file, so an inherited helper (e.g. a global
            # credential manager) can't answer first and shadow the token
            # below — git tries helpers in file order and stops at the first
            # one that returns a match.
            f"[credential]\n\thelper =\n\thelper = store --file={cred_file}\n",
            encoding="utf-8",
        )
        env["GIT_CONFIG_GLOBAL"] = str(gitconfig)
        # `gh` splits auth by host: GH_TOKEN for github.com / *.ghe.com,
        # GH_ENTERPRISE_TOKEN for GHES. Set both — gh reads the matching one.
        env["GH_TOKEN"] = creds.github_token
        env["GH_ENTERPRISE_TOKEN"] = creds.github_token
    if creds.linear_token:
        env["LINEAR_API_KEY"] = creds.linear_token
    return env
