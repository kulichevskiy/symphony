"""Stub Acceptance verdict classifier.

The real classifier lands in later slices. For now it deliberately returns a
hard pass so the stage plumbing can be exercised end to end.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

AcceptanceVerdictKind = Literal["pass", "reject", "infra_error"]


@dataclass(frozen=True)
class AcceptanceVerdict:
    kind: AcceptanceVerdictKind
    criteria: list[str]
    cost: float
    hero_screenshot_url: str


def acceptance_classifier(
    *, criteria: list[str] | None = None, cost: float = 0.0
) -> AcceptanceVerdict:
    return AcceptanceVerdict(
        kind="pass",
        criteria=list(criteria or []),
        cost=cost,
        hero_screenshot_url="",
    )


__all__ = ["AcceptanceVerdict", "AcceptanceVerdictKind", "acceptance_classifier"]
