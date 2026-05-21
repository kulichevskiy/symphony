"""Preview URL resolver behavior."""

from __future__ import annotations

import pytest

from symphony.config import AcceptanceConfig
from symphony.pipeline.preview_resolver import (
    PreviewResolutionError,
    resolve_preview_url,
)


@pytest.mark.asyncio
async def test_preview_resolver_substitutes_placeholders_and_waits_for_200() -> None:
    attempts: list[str] = []
    sleeps: list[float] = []

    async def fake_probe(url: str) -> bool:
        attempts.append(url)
        return len(attempts) == 3

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    acceptance = AcceptanceConfig(
        mode="preview",
        preview_url_pattern=(
            "https://vib-{pr_number}-{issue}-{issue_id}.vercel.app?pr={pr_url}"
        ),
        preview_wait_timeout_secs=30.0,
    )

    url = await resolve_preview_url(
        acceptance,
        pr_number=42,
        issue_identifier="ENG-1",
        issue_id="iss-1",
        pr_url="https://github.com/org/repo/pull/42",
        probe=fake_probe,
        sleep=fake_sleep,
    )

    assert url == (
        "https://vib-42-ENG-1-iss-1.vercel.app"
        "?pr=https://github.com/org/repo/pull/42"
    )
    assert attempts == [url, url, url]
    assert sleeps == [1.0, 2.0]


@pytest.mark.asyncio
async def test_preview_resolver_timeout_raises_infra_error() -> None:
    attempts: list[str] = []

    async def fake_probe(url: str) -> bool:
        attempts.append(url)
        return False

    async def fake_sleep(_delay: float) -> None:
        return None

    acceptance = AcceptanceConfig(
        mode="preview",
        preview_url_pattern="https://vib-{pr_number}.vercel.app",
    )

    with pytest.raises(PreviewResolutionError) as excinfo:
        await resolve_preview_url(
            acceptance,
            pr_number=42,
            timeout_secs=0.0,
            probe=fake_probe,
            sleep=fake_sleep,
        )

    assert excinfo.value.url == "https://vib-42.vercel.app"
    assert "did not become live" in str(excinfo.value)
    assert attempts == ["https://vib-42.vercel.app"]


@pytest.mark.asyncio
async def test_preview_resolver_invalid_url_still_raises_resolution_error() -> None:
    acceptance = AcceptanceConfig(
        mode="preview",
        preview_url_pattern="https://vib-{pr_number}:bad.vercel.app",
    )

    with pytest.raises(PreviewResolutionError) as excinfo:
        await resolve_preview_url(
            acceptance,
            pr_number=42,
            timeout_secs=0.0,
        )

    assert excinfo.value.url == "https://vib-42:bad.vercel.app"
    assert "did not become live" in str(excinfo.value)
