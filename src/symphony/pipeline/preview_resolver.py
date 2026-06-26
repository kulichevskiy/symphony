"""Pattern-based preview URL resolution for Acceptance preview mode.

Supported `preview_url_pattern` placeholders:
  - `{pr_number}`: GitHub PR number
  - `{issue}` / `{issue_identifier}`: Linear issue key, for example `ENG-1`
  - `{issue_id}`: Linear issue UUID
  - `{pr_url}`: GitHub PR URL
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import httpx

from symphony.config import AcceptanceConfig

DEFAULT_PREVIEW_WAIT_TIMEOUT_SECS = 300.0
_INITIAL_BACKOFF_SECS = 1.0
_MAX_BACKOFF_SECS = 15.0
_PROBE_TIMEOUT_SECS = 10.0

PreviewProbe = Callable[[str], Awaitable[bool]]
PreviewSleep = Callable[[float], Awaitable[None]]


class PreviewResolutionError(RuntimeError):
    """Raised when a configured preview URL cannot be resolved live."""

    def __init__(self, message: str, *, url: str = "") -> None:
        super().__init__(message)
        self.url = url


def render_preview_url(
    acceptance: AcceptanceConfig,
    *,
    pr_number: int,
    issue_identifier: str = "",
    issue_id: str = "",
    pr_url: str = "",
) -> str:
    pattern = (acceptance.preview_url_pattern or "").strip()
    if not pattern:
        raise PreviewResolutionError("preview acceptance requires acceptance.preview_url_pattern.")
    try:
        return pattern.format(
            pr_number=pr_number,
            issue=issue_identifier,
            issue_identifier=issue_identifier,
            issue_id=issue_id,
            pr_url=pr_url,
        )
    except KeyError as e:
        raise PreviewResolutionError(
            f"preview_url_pattern contains unknown placeholder {{{e.args[0]}}}."
        ) from e
    except Exception as e:  # noqa: BLE001
        raise PreviewResolutionError(f"could not render preview_url_pattern: {e}") from e


async def resolve_preview_url(
    acceptance: AcceptanceConfig,
    *,
    pr_number: int,
    issue_identifier: str = "",
    issue_id: str = "",
    pr_url: str = "",
    timeout_secs: float | None = None,
    probe: PreviewProbe | None = None,
    sleep: PreviewSleep = asyncio.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> str:
    url = render_preview_url(
        acceptance,
        pr_number=pr_number,
        issue_identifier=issue_identifier,
        issue_id=issue_id,
        pr_url=pr_url,
    )
    wait_timeout_secs = (
        acceptance.preview_wait_timeout_secs if timeout_secs is None else timeout_secs
    )
    if wait_timeout_secs is None:
        wait_timeout_secs = DEFAULT_PREVIEW_WAIT_TIMEOUT_SECS
    wait_timeout_secs = max(float(wait_timeout_secs), 0.0)
    deadline = monotonic() + wait_timeout_secs
    check = probe or _preview_returns_200
    delay = _INITIAL_BACKOFF_SECS

    while True:
        if await check(url):
            return url
        now = monotonic()
        if now >= deadline:
            raise PreviewResolutionError(
                (
                    f"preview URL {url} did not become live with HTTP 200 "
                    f"within {wait_timeout_secs:.1f}s."
                ),
                url=url,
            )
        sleep_for = min(delay, deadline - now)
        if sleep_for > 0:
            await sleep(sleep_for)
        delay = min(delay * 2, _MAX_BACKOFF_SECS)


async def _preview_returns_200(url: str) -> bool:
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=_PROBE_TIMEOUT_SECS,
        ) as client:
            response = await client.get(url)
    except (httpx.HTTPError, httpx.InvalidURL):
        return False
    return response.status_code == 200


__all__ = [
    "DEFAULT_PREVIEW_WAIT_TIMEOUT_SECS",
    "PreviewResolutionError",
    "render_preview_url",
    "resolve_preview_url",
]
