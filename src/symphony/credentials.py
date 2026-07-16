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
    linear_token: str | None = None

    @property
    def is_empty(self) -> bool:
        return not self.github_token and not self.linear_token


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
        try:
            status = await db.oauth_connections.get_status(self._conn, provider)
        except Exception:  # noqa: BLE001 — a DB read hiccup must not break the run
            log.warning("oauth_connections status read failed for %s; using fallback", provider)
            return fallback
        if status is None or status.status != "connected":
            return fallback
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
            return fallback
        return credential or fallback

    async def resolve_run_credentials(
        self, *, github_fallback: str | None = None, linear_fallback: str | None = None
    ) -> RunCredentials:
        """Resolve the providers a run materializes (GitHub + Linear) into a bundle."""
        return RunCredentials(
            github_token=await self.resolve("github", fallback=github_fallback),
            linear_token=await self.resolve("linear", fallback=linear_fallback),
        )


def materialize_credentials(
    creds: RunCredentials, home_dir: Path, *, prior_gitconfig: Path | None = None
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
    doesn't exist. The Linear token becomes `LINEAR_API_KEY`, the bearer the
    Linear client already sends. An empty bundle writes nothing and returns no
    env.
    """
    env: dict[str, str] = {}
    if creds.github_token:
        cred_file = home_dir / ".git-credentials"
        # `x-access-token` as the username is the GitHub convention; the token
        # is what authenticates. `store` matches by host on push.
        cred_file.write_text(
            f"https://x-access-token:{creds.github_token}@github.com\n", encoding="utf-8"
        )
        cred_file.chmod(0o600)
        include_path = prior_gitconfig or (Path.home() / ".gitconfig")
        gitconfig = home_dir / ".gitconfig"
        gitconfig.write_text(
            f"[include]\n\tpath = {include_path}\n"
            f"[credential]\n\thelper = store --file={cred_file}\n",
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
