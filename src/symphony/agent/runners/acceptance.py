"""Always-pass Acceptance runner stub."""

from __future__ import annotations

from symphony.pipeline.acceptance_classifier import (
    AcceptanceVerdict,
    acceptance_classifier,
)


async def run_acceptance(*, criteria: list[str] | None = None) -> AcceptanceVerdict:
    return acceptance_classifier(criteria=criteria)


__all__ = ["run_acceptance"]
