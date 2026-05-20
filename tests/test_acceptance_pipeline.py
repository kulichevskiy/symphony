"""Stub Acceptance runner/classifier behavior."""

from __future__ import annotations

import pytest

from symphony.agent.runners.acceptance import run_acceptance
from symphony.pipeline.acceptance_classifier import (
    AcceptanceVerdict,
    acceptance_classifier,
)


def test_acceptance_classifier_stub_passes() -> None:
    verdict = acceptance_classifier(criteria=["tests pass"], cost=0.0)

    assert verdict == AcceptanceVerdict(
        kind="pass",
        criteria=["tests pass"],
        cost=0.0,
        hero_screenshot_url="",
    )


@pytest.mark.asyncio
async def test_acceptance_runner_stub_passes() -> None:
    verdict = await run_acceptance(criteria=["ship it"])

    assert verdict.kind == "pass"
    assert verdict.criteria == ["ship it"]
    assert verdict.cost == 0.0
    assert verdict.hero_screenshot_url == ""
