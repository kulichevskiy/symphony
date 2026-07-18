"""Claude code-paste login driver + pending-login registry (OAuth in UI 5/7).

Claude has no redirect-OAuth we can reach from the operator's browser (no
localhost callback), so the daemon drives the `claude` CLI login as a
subprocess instead:

  * `start` spawns the login process and reads back the OAuth URL it prints —
    the operator authorizes that in their own browser;
  * `submit_code` writes the pasted authorization code to the process's stdin,
    waits for it to finish writing its credentials file, and returns that
    file's raw contents so the caller can encrypt + store it.

The live subprocess handle has to survive *between* those two HTTP requests, so
a `PendingLoginRegistry` (in-memory, single-process — Symphony is one daemon)
holds it keyed by an unguessable login-session id minted on `start`. A daemon
restart mid-login drops the handle; the operator just restarts the login
(acceptable for single-tenant).

The credential material is Claude's own `~/.claude/.credentials.json`: the whole
JSON blob is what we store (so a later write-back can restore it byte-for-byte),
and `claude_expires_at` reads the `expiresAt` out of it for the card's `Test`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import httpx

log = logging.getLogger(__name__)

_SESSION_ENTROPY_BYTES = 32
_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# The CLI prints its consent URL to stdout/stderr; grab the first https URL that
# looks like an OAuth authorize link. Kept permissive so a minor URL change in
# the CLI doesn't silently break capture.
_URL_RE = re.compile(r"https://\S*oauth\S*", re.IGNORECASE)
# How long to wait for the URL to appear / the login to complete after a code.
_START_TIMEOUT_SECS = 60.0
_SUBMIT_TIMEOUT_SECS = 120.0
# How long a pending login may sit unsubmitted before it's treated as
# abandoned (operator closed the tab / refreshed / never pasted a code) and
# its subprocess is killed. Generous — it has to cover opening a browser tab,
# authorizing on claude.ai, and copying the code back.
_PENDING_LOGIN_TTL_SECS = 900.0
# The `claude` login invocation. `auth login` is the scriptable CLI subcommand
# that drives the OAuth flow (`/login` is a REPL-only slash command, not
# invocable as a CLI argument); stdin/stdout are piped so the daemon feeds the
# pasted code and scrapes the URL.
DEFAULT_LOGIN_COMMAND: tuple[str, ...] = ("claude", "auth", "login")


class ClaudeLoginError(Exception):
    """The login subprocess failed to surface a URL or complete on a code. The
    router renders this as a failed connect, never a raw traceback."""


class ClaudeLoginProcess(Protocol):
    """One in-flight `claude` login. `start` returns the OAuth URL to show the
    operator; `submit_code` feeds the pasted code and returns the raw credential
    JSON the CLI wrote; `close` tears down a login that was abandoned."""

    async def start(self) -> str: ...

    async def submit_code(self, code: str) -> str: ...

    async def close(self) -> None: ...


class PendingLoginRegistry:
    """In-memory, single-use registry of live login subprocesses keyed by an
    unguessable session id. Single-process by design (one daemon); a daemon
    restart drops any in-flight login.

    Entries expire after `ttl_secs` (default `_PENDING_LOGIN_TTL_SECS`): if the
    operator closes the tab, refreshes mid-login, or never pastes a code back,
    the subprocess would otherwise run forever. `add` arms an event-loop timer
    that closes the entry once its deadline passes even if no further request
    ever touches the registry (the close-tab case); `add` also opportunistically
    sweeps and `pop` refuses a past-deadline entry, so eviction still holds in a
    sync context with no running loop (unit tests with a faked clock)."""

    def __init__(
        self,
        *,
        id_factory: Callable[[], str] | None = None,
        ttl_secs: float = _PENDING_LOGIN_TTL_SECS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._pending: dict[str, tuple[ClaudeLoginProcess, float]] = {}
        self._timers: dict[str, asyncio.TimerHandle] = {}
        self._id_factory = id_factory or (lambda: secrets.token_urlsafe(_SESSION_ENTROPY_BYTES))
        self._ttl_secs = ttl_secs
        self._clock = clock or time.monotonic

    def add(self, process: ClaudeLoginProcess) -> str:
        self._evict_expired()
        session_id = self._id_factory()
        self._pending[session_id] = (process, self._clock() + self._ttl_secs)
        self._arm_timer(session_id)
        return session_id

    def pop(self, session_id: str) -> ClaudeLoginProcess | None:
        """Return and remove the handle for `session_id` (single-use), or `None`
        if it is unknown, already consumed, or expired."""
        entry = self._pending.pop(session_id, None)
        self._cancel_timer(session_id)
        if entry is None:
            return None
        process, deadline = entry
        if self._clock() >= deadline:
            self._schedule_close(process)
            return None
        return process

    async def discard(self, session_id: str) -> None:
        """Drop and tear down an abandoned/failed login."""
        entry = self._pending.pop(session_id, None)
        self._cancel_timer(session_id)
        if entry is not None:
            await entry[0].close()

    def _evict_expired(self) -> None:
        now = self._clock()
        expired = [sid for sid, (_, deadline) in self._pending.items() if now >= deadline]
        for sid in expired:
            self._expire(sid)

    def _expire(self, session_id: str) -> None:
        """Drop `session_id` if still pending and close its subprocess. Runs both
        from the armed timer (close-tab case) and the opportunistic sweep."""
        self._cancel_timer(session_id)
        entry = self._pending.pop(session_id, None)
        if entry is None:
            return
        log.info("pending Claude login session %s expired unsubmitted; closing it", session_id)
        self._schedule_close(entry[0])

    def _arm_timer(self, session_id: str) -> None:
        """Schedule `_expire(session_id)` on the running loop after the TTL, so an
        abandoned session closes with no later add/pop. No-ops without a running
        loop (sync unit tests) — the sweep/`pop` deadline checks cover that."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._timers[session_id] = loop.call_later(self._ttl_secs, self._expire, session_id)

    def _cancel_timer(self, session_id: str) -> None:
        handle = self._timers.pop(session_id, None)
        if handle is not None:
            handle.cancel()

    @staticmethod
    def _schedule_close(process: ClaudeLoginProcess) -> None:
        """Fire-and-forget `process.close()`. `add`/`pop` are sync so eviction
        can't await; there's always a running loop in production (callers are
        async route handlers), so this only silently no-ops in sync unit tests
        that never advance the clock past a deadline anyway."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(process.close())


def default_claude_credentials_path() -> Path:
    """Where the `claude` CLI writes its OAuth credentials. Honors
    `CLAUDE_CONFIG_DIR` (Claude Code stores `.credentials.json` under it when
    set, per the auth docs); otherwise `~/.claude` (`HOME` is the deployment's
    persistent auth volume — see docker-compose.yml)."""
    config_dir = os.environ.get("CLAUDE_CONFIG_DIR")
    base = Path(config_dir) if config_dir else Path.home() / ".claude"
    return base / ".credentials.json"


def read_claude_credential(path: Path) -> str | None:
    """The raw contents of Claude's credentials file, or `None` if it's missing
    or unreadable. Returned verbatim so a write-back can restore it
    byte-for-byte. An unreadable file (bad permissions, stale dir on the auth
    volume) is treated as absent so best-effort restore/write-back callers get
    to run their own recovery instead of raising before their `try` blocks."""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def claude_expires_at(raw: str) -> str | None:
    """The stored credential's access-token expiry as an absolute ISO timestamp
    (`%Y-%m-%dT%H:%M:%SZ`), or `None` if the blob has no parseable `expiresAt`.

    Claude stores it under `claudeAiOauth.expiresAt` as epoch milliseconds."""
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    oauth = payload.get("claudeAiOauth")
    if not isinstance(oauth, dict):
        return None
    expires_ms = oauth.get("expiresAt")
    if not isinstance(expires_ms, (int, float)) or isinstance(expires_ms, bool):
        return None
    return datetime.fromtimestamp(expires_ms / 1000, tz=UTC).strftime(_ISO_FORMAT)


def claude_credential_expired(raw: str) -> bool:
    """Whether the stored credential's access token is past its `expiresAt`.
    A blob with no parseable expiry is treated as not-expired — the card's
    `Test` reflects "live" rather than flipping a usable connection to expired
    on a format we don't recognize."""
    return claude_credential_expires_within(raw, 0.0)


def claude_credential_expires_within(raw: str, horizon_secs: float) -> bool:
    """Whether the credential's access token expires within `horizon_secs`
    from now (Config v2 4/9: the daemon refreshes proactively when the expiry
    falls inside a run's maximum wall clock, so the CLI never has to rotate the
    one-shot refresh token itself mid-run). No parseable expiry → False."""
    expires_at = claude_expires_at(raw)
    if expires_at is None:
        return False
    deadline = datetime.strptime(expires_at, _ISO_FORMAT).replace(tzinfo=UTC)
    return datetime.now(UTC).timestamp() + horizon_secs >= deadline.timestamp()


# The Claude Code CLI's own public OAuth client — the daemon refreshes the
# UI-stored credential through the standard refresh grant (RFC 6749 §6), the
# same exchange the CLI performs itself. Plan B if this endpoint ever
# resists: force a refresh via an isolated one-shot CLI invocation.
CLAUDE_OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
CLAUDE_OAUTH_TOKEN_URL = "https://console.anthropic.com/v1/oauth/token"
_REFRESH_TIMEOUT_SECS = 30.0


async def refresh_claude_credential(
    raw: str, *, client: httpx.AsyncClient | None = None
) -> str | None:
    """Exchange the credential blob's refresh token for a fresh access token
    and return the rebuilt blob (all unrelated fields preserved verbatim), or
    `None` when the blob has no refresh token or the exchange fails. Never
    raises — the caller treats `None` as "connection is dead, park it"."""
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    oauth = payload.get("claudeAiOauth")
    if not isinstance(oauth, dict):
        return None
    refresh_token = oauth.get("refreshToken")
    if not isinstance(refresh_token, str) or not refresh_token:
        return None
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLAUDE_OAUTH_CLIENT_ID,
    }
    owns_client = client is None
    http = client or httpx.AsyncClient(timeout=_REFRESH_TIMEOUT_SECS)
    try:
        response = await http.post(CLAUDE_OAUTH_TOKEN_URL, json=data)
        if response.status_code != 200:
            log.warning("claude token refresh failed with HTTP %d", response.status_code)
            return None
        token = response.json()
    except (httpx.HTTPError, ValueError):
        log.warning("claude token refresh failed", exc_info=True)
        return None
    finally:
        if owns_client:
            await http.aclose()
    access_token = token.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        return None
    oauth = dict(oauth)
    oauth["accessToken"] = access_token
    # Some providers omit refresh_token on refresh (the old one stays valid);
    # Anthropic rotates it — keep whichever we were given.
    new_refresh = token.get("refresh_token")
    if isinstance(new_refresh, str) and new_refresh:
        oauth["refreshToken"] = new_refresh
    expires_in = token.get("expires_in")
    if isinstance(expires_in, (int, float)) and not isinstance(expires_in, bool):
        oauth["expiresAt"] = int((datetime.now(UTC).timestamp() + expires_in) * 1000)
    payload = dict(payload)
    payload["claudeAiOauth"] = oauth
    return json.dumps(payload)


class SubprocessClaudeLogin:
    """Drives the real `claude` login CLI as a subprocess.

    `start` spawns it and scrapes the OAuth URL off its output; `submit_code`
    writes the pasted code to stdin, waits for the process to exit cleanly, and
    reads back the credentials file it wrote. Faked in tests via
    `ClaudeLoginProcess`."""

    def __init__(
        self,
        *,
        command: Sequence[str] = DEFAULT_LOGIN_COMMAND,
        credentials_path: Path | None = None,
        env: dict[str, str] | None = None,
        start_timeout: float = _START_TIMEOUT_SECS,
        submit_timeout: float = _SUBMIT_TIMEOUT_SECS,
    ) -> None:
        self._command = tuple(command)
        self._credentials_path = credentials_path or default_claude_credentials_path()
        self._env = env
        self._start_timeout = start_timeout
        self._submit_timeout = submit_timeout
        self._proc: asyncio.subprocess.Process | None = None

    async def start(self) -> str:
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *self._command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=self._env,
            )
        except OSError as exc:
            raise ClaudeLoginError("failed to start the Claude login process") from exc
        try:
            return await asyncio.wait_for(self._read_url(), timeout=self._start_timeout)
        except ClaudeLoginError:
            await self.close()
            raise
        except Exception as exc:  # noqa: BLE001 — surface any spawn/read failure cleanly
            await self.close()
            raise ClaudeLoginError("timed out waiting for the Claude login URL") from exc

    async def _read_url(self) -> str:
        assert self._proc is not None and self._proc.stdout is not None
        while True:
            raw = await self._proc.stdout.readline()
            if not raw:
                raise ClaudeLoginError("Claude login exited before printing a URL")
            match = _URL_RE.search(raw.decode(errors="replace"))
            if match is not None:
                return match.group(0)

    async def submit_code(self, code: str) -> str:
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise ClaudeLoginError("login process is not running")
        try:
            # `communicate` drains stdout/stderr concurrently with the write +
            # wait; a bare `proc.wait()` here can deadlock once the pipe fills
            # since the child blocks on write and never reads the code.
            await asyncio.wait_for(
                proc.communicate(input=f"{code}\n".encode()), timeout=self._submit_timeout
            )
        except TimeoutError as exc:
            await self.close()
            raise ClaudeLoginError("timed out completing the Claude login") from exc
        if proc.returncode != 0:
            raise ClaudeLoginError(f"Claude login exited with code {proc.returncode}")
        credential = read_claude_credential(self._credentials_path)
        if not credential:
            raise ClaudeLoginError("Claude login produced no credentials")
        return credential

    async def close(self) -> None:
        proc = self._proc
        if proc is None or proc.returncode is not None:
            return
        try:
            proc.kill()
            await proc.wait()
        except ProcessLookupError:
            pass
