"""Codex device-auth login driver (OAuth in UI 6/7).

Codex has no browser-reachable redirect callback we can reach from the operator's
browser (its login server binds container loopback — see `codex-login-docker.sh`
for the SSH-tunnel dance this slice replaces), so the daemon drives the `codex`
CLI *device-auth* login as a subprocess instead:

  * `start` spawns `codex login --device-auth` and scrapes the verification URL +
    user code it prints — the operator opens that URL and enters the code in
    their own browser;
  * unlike Claude's code-paste flow there is nothing to feed back to the daemon:
    the CLI polls the provider itself and exits once the operator completes it,
    so `poll` reports pending → success/failure as the subprocess exits and, on
    success, reads back the `auth.json` the CLI wrote.

The live subprocess handle survives *across* the repeated `poll` requests in the
`PendingLoginRegistry` reused from 5/7 (keyed by an unguessable login-session id
minted on `start`); a daemon restart mid-login just restarts the login.

The credential material is Codex's own `~/.codex/auth.json`: the whole JSON blob
is stored (so a later write-back can restore it byte-for-byte), and
`codex_expires_at` reads the access token's JWT `exp` for the card's `Test`.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import json
import logging
import os
import re
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import httpx

log = logging.getLogger(__name__)

_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# The CLI prints its verification URL and a short user code to stdout/stderr.
# Kept permissive so a minor wording change in the CLI doesn't silently break
# capture: grab the first https URL, and the first code-looking token (an
# uppercase-alnum block, optionally hyphenated — e.g. `ABCD-1234`).
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_URL_RE = re.compile(r"https://\S+", re.IGNORECASE)
# The server-provided user_code is opaque: a hyphenated group (ABCD-1234)
# OR a single block (WDJBMJHT). Prefer the hyphenated form, else a lone block.
_CODE_RE = re.compile(r"\b([A-Z0-9]{4,8}(?:-[A-Z0-9]{4,8})+|[A-Z0-9]{6,10})\b")
# How long to wait for the URL + code to appear after spawning the login.
_START_TIMEOUT_SECS = 60.0

STATUS_PENDING = "pending"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"

# The `codex` device-auth invocation. stdout is piped so the daemon scrapes the
# URL + code; the CLI polls the provider itself, so there is no stdin to feed.
DEFAULT_LOGIN_COMMAND: tuple[str, ...] = ("codex", "login", "--device-auth")


class CodexLoginError(Exception):
    """The login subprocess failed to surface a URL + code. The router renders
    this as a failed connect, never a raw traceback."""


@dataclass(frozen=True)
class CodexDeviceAuth:
    """What the operator needs to complete a device-auth login: the URL to open
    and the short code to type into it."""

    verification_uri: str
    user_code: str


@dataclass(frozen=True)
class CodexPollResult:
    """One `poll` outcome. `credential` is the raw `auth.json` blob, set only on
    `success`."""

    status: str
    credential: str | None = None


class CodexLoginProcess(Protocol):
    """One in-flight `codex` device-auth login. `start` returns the URL + code to
    show the operator; `poll` reports pending/success/failure as the subprocess
    runs and exits; `close` tears down a login that was abandoned."""

    async def start(self) -> CodexDeviceAuth: ...

    async def poll(self) -> CodexPollResult: ...

    async def close(self) -> None: ...


def pin_file_auth_storage(codex_home: Path) -> None:
    """Force codex to persist auth to `auth.json`, not the OS keyring.

    codex's credential storage is `cli_auth_credentials_store` (`file` writes
    `auth.json`; `keyring`/`auto` may use the OS store, leaving `auth.json`
    empty). Pin `file` in the CODEX_HOME config.toml — replacing any existing
    setting — so both the device login and per-run homes always read/write the
    blob the daemon stores (Config v2 6/9 review fix)."""
    import re as _re

    try:
        codex_home.mkdir(parents=True, exist_ok=True)
        config = codex_home / "config.toml"
        existing = config.read_text(encoding="utf-8") if config.exists() else ""
        stripped = _re.sub(r"(?m)^\s*cli_auth_credentials_store\s*=.*$\n?", "", existing)
        if stripped and not stripped.endswith("\n"):
            stripped += "\n"
        config.write_text(stripped + 'cli_auth_credentials_store = "file"\n', encoding="utf-8")
    except OSError:
        log.warning("could not pin codex file auth storage", exc_info=True)


# Back-compat private alias used by SubprocessCodexLogin.
_pin_file_auth_storage = pin_file_auth_storage


def default_codex_credentials_path() -> Path:
    """Where the `codex` CLI writes its OAuth credentials. Honors `CODEX_HOME`
    (codex stores `auth.json` under it when set); otherwise `~/.codex` (`HOME` is
    the deployment's persistent auth volume — see docker-compose.yml)."""
    codex_home = os.environ.get("CODEX_HOME")
    base = Path(codex_home) if codex_home else Path.home() / ".codex"
    return base / "auth.json"


def read_codex_credential(path: Path) -> str | None:
    """The raw contents of Codex's `auth.json`, or `None` if it's missing or
    unreadable. Returned verbatim so a write-back can restore it byte-for-byte.
    An unreadable file (bad permissions, stale dir on the auth volume) is treated
    as absent so best-effort restore/write-back callers get to run their own
    recovery instead of raising before their `try` blocks."""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def _jwt_exp_seconds(token: str) -> int | None:
    """The `exp` (epoch seconds) claim out of a JWT's payload segment, or `None`
    if `token` isn't a parseable JWT with a numeric `exp`."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    segment = parts[1]
    padded = segment + "=" * (-len(segment) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(padded))
    except (ValueError, TypeError, binascii.Error):
        return None
    if not isinstance(claims, dict):
        return None
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)) or isinstance(exp, bool):
        return None
    return int(exp)


def codex_expires_at(raw: str) -> str | None:
    """The stored credential's access-token expiry as an absolute ISO timestamp
    (`%Y-%m-%dT%H:%M:%SZ`), or `None` if the blob has no parseable expiry.

    Codex's `auth.json` holds the access token (a JWT) under
    `tokens.access_token`; the expiry is the JWT's own `exp` claim."""
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    tokens = payload.get("tokens")
    if not isinstance(tokens, dict):
        return None
    access_token = tokens.get("access_token")
    if not isinstance(access_token, str):
        return None
    exp = _jwt_exp_seconds(access_token)
    if exp is None:
        return None
    return datetime.fromtimestamp(exp, tz=UTC).strftime(_ISO_FORMAT)


# codex's own OAuth client + refresh endpoint (from openai/codex
# codex-rs/login/src/auth/manager.rs): the daemon refreshes the stored token
# with the same standard refresh grant the CLI uses, so it's a plain HTTP call
# (testable), not a CLI invocation. Env overrides mirror the CLI's.
CODEX_OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
CODEX_REFRESH_TOKEN_URL = "https://auth.openai.com/oauth/token"
_CODEX_REFRESH_TIMEOUT_SECS = 30.0


async def refresh_codex_credential(
    raw: str, *, client: httpx.AsyncClient | None = None
) -> str | None:
    """Exchange the codex credential blob's refresh token for a fresh access
    token and return the rebuilt `auth.json` blob (unrelated fields preserved),
    or `None` when there's no refresh token or the exchange fails. Never raises.

    Uses codex's documented refresh grant (client_id + POST
    auth.openai.com/oauth/token, grant_type=refresh_token). `access_token` and
    `refresh_token` are updated; `id_token` and everything else are left as-is
    (codex stores id_token as parsed claims — the daemon only needs the access
    token live)."""
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return None
    tokens = payload.get("tokens") if isinstance(payload, dict) else None
    if not isinstance(tokens, dict):
        return None
    refresh_token = tokens.get("refresh_token")
    if not isinstance(refresh_token, str) or not refresh_token:
        return None
    client_id = os.environ.get("CODEX_APP_SERVER_LOGIN_CLIENT_ID", CODEX_OAUTH_CLIENT_ID)
    url = os.environ.get("CODEX_REFRESH_TOKEN_URL_OVERRIDE", CODEX_REFRESH_TOKEN_URL)
    data = {"client_id": client_id, "grant_type": "refresh_token", "refresh_token": refresh_token}
    owns = client is None
    http = client or httpx.AsyncClient(timeout=_CODEX_REFRESH_TIMEOUT_SECS)
    try:
        resp = await http.post(url, json=data)
        if resp.status_code != 200:
            log.warning("codex token refresh failed with HTTP %d", resp.status_code)
            return None
        body = resp.json()
    except (httpx.HTTPError, ValueError):
        log.warning("codex token refresh failed", exc_info=True)
        return None
    finally:
        if owns:
            await http.aclose()
    access_token = body.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        return None
    tokens = dict(tokens)
    tokens["access_token"] = access_token
    new_refresh = body.get("refresh_token")
    if isinstance(new_refresh, str) and new_refresh:
        tokens["refresh_token"] = new_refresh
    payload = dict(payload)
    payload["tokens"] = tokens
    return json.dumps(payload)


def codex_credential_expires_within(raw: str, horizon_secs: float) -> bool:
    """Whether the codex access token expires within `horizon_secs` from now
    (Config v2 4/9 extended to codex, SYM-217): the daemon refreshes centrally
    when expiry falls inside a run's max wall clock. No parseable expiry →
    False."""
    expires_at = codex_expires_at(raw)
    if expires_at is None:
        return False
    deadline = datetime.strptime(expires_at, _ISO_FORMAT).replace(tzinfo=UTC)
    return datetime.now(UTC).timestamp() + horizon_secs >= deadline.timestamp()


def codex_credential_expired(raw: str) -> bool:
    """Whether the stored credential's access token is past its expiry. A blob
    with no parseable expiry is treated as not-expired — the card's `Test`
    reflects "live" rather than flipping a usable connection to expired on a
    format we don't recognize."""
    expires_at = codex_expires_at(raw)
    if expires_at is None:
        return False
    deadline = datetime.strptime(expires_at, _ISO_FORMAT).replace(tzinfo=UTC)
    return datetime.now(UTC) >= deadline


class SubprocessCodexLogin:
    """Drives the real `codex login --device-auth` CLI as a subprocess.

    `start` spawns it and scrapes the verification URL + user code off its
    output, then drains the rest of the pipe in the background so the child never
    blocks writing progress while it polls the provider. `poll` reports pending
    until the subprocess exits, then success (reading back `auth.json`) or
    failure. Faked in tests via `CodexLoginProcess`."""

    def __init__(
        self,
        *,
        command: Sequence[str] = DEFAULT_LOGIN_COMMAND,
        credentials_path: Path | None = None,
        env: dict[str, str] | None = None,
        start_timeout: float = _START_TIMEOUT_SECS,
    ) -> None:
        self._command = tuple(command)
        # Each pending login gets its own private CODEX_HOME so two concurrent
        # device flows (second tab / refresh) never share one auth.json —
        # last-writer-wins there could persist the wrong account (Config v2 6/9
        # review fix). The caller can pin a path for tests.
        if credentials_path is not None:
            self._credentials_path = credentials_path
            self._owns_home = False
        else:
            self._credentials_path = (
                Path(tempfile.mkdtemp(prefix="symphony-codex-login-")) / "auth.json"
            )
            self._owns_home = True
        # Inherit the daemon env (PATH etc. the CLI needs) unless the caller
        # passed an explicit env; then point CODEX_HOME at the private home.
        self._env = dict(env) if env is not None else dict(os.environ)
        self._env["CODEX_HOME"] = str(self._credentials_path.parent)
        self._start_timeout = start_timeout
        self._proc: asyncio.subprocess.Process | None = None
        self._drain_task: asyncio.Task[None] | None = None

    def _ensure_file_auth_storage(self) -> None:
        """Force codex to persist auth to `auth.json`, not the OS keyring.

        codex's `auto` storage prefers a keyring when one is available, which
        would leave the `auth.json` we read back empty. Pin file storage via
        the CODEX_HOME config.toml so the device login always writes the blob
        the daemon stores + materializes (Config v2 6/9 review fix)."""
        _pin_file_auth_storage(self._credentials_path.parent)

    async def start(self) -> CodexDeviceAuth:
        self._ensure_file_auth_storage()
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *self._command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=self._env,
            )
        except OSError as exc:
            raise CodexLoginError("failed to start the Codex login process") from exc
        try:
            device_auth = await asyncio.wait_for(
                self._read_device_auth(), timeout=self._start_timeout
            )
        except CodexLoginError:
            await self.close()
            raise
        except Exception as exc:  # noqa: BLE001 — surface any spawn/read failure cleanly
            await self.close()
            raise CodexLoginError("timed out waiting for the Codex device code") from exc
        # Keep draining stdout to EOF (then reap) so the child never blocks on a
        # full pipe while it polls the provider, and so `poll` can detect exit.
        self._drain_task = asyncio.create_task(self._drain_and_wait())
        return device_auth

    async def _read_device_auth(self) -> CodexDeviceAuth:
        assert self._proc is not None and self._proc.stdout is not None
        url: str | None = None
        code: str | None = None
        while url is None or code is None:
            raw = await self._proc.stdout.readline()
            if not raw:
                raise CodexLoginError("Codex login exited before printing a device code")
            # The real codex CLI wraps the URL and code in ANSI color
            # escapes; strip them so the regexes match the bare values.
            line = _ANSI_RE.sub("", raw.decode(errors="replace"))
            if url is None:
                match = _URL_RE.search(line)
                if match is not None:
                    url = match.group(0)
            if code is None:
                match = _CODE_RE.search(line)
                if match is not None:
                    code = match.group(1)
        return CodexDeviceAuth(verification_uri=url, user_code=code)

    async def _drain_and_wait(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        while await self._proc.stdout.readline():
            pass
        await self._proc.wait()

    async def poll(self) -> CodexPollResult:
        if self._proc is None:
            raise CodexLoginError("login process is not running")
        task = self._drain_task
        if task is None or not task.done():
            return CodexPollResult(status=STATUS_PENDING)
        # The drain task awaited `proc.wait()`, so `returncode` is set now.
        if task.cancelled() or task.exception() is not None or self._proc.returncode != 0:
            return CodexPollResult(status=STATUS_FAILED)
        credential = read_codex_credential(self._credentials_path)
        if not credential:
            return CodexPollResult(status=STATUS_FAILED)
        return CodexPollResult(status=STATUS_SUCCESS, credential=credential)

    async def close(self) -> None:
        task = self._drain_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        proc = self._proc
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
                await proc.wait()
            except ProcessLookupError:
                pass
        if self._owns_home:
            import shutil as _shutil

            _shutil.rmtree(self._credentials_path.parent, ignore_errors=True)
