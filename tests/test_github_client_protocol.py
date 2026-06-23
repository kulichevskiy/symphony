"""The `GitHubClient` Protocol must be satisfied by both the real `GitHub`
client and the test `FakeGitHub`, so the fake↔real boundary is typed."""

from __future__ import annotations

from symphony.github.client import GitHub, GitHubClient

from .harness.clock import ManualClock
from .harness.fakes import FakeGitHub
from .harness.sim import Sim


# mypy verifies structural parity (method signatures); isinstance only guards
# against renames/removals at runtime (it does not check argument types).
def _real_is_client(gh: GitHub) -> GitHubClient:
    return gh


def _fake_is_client(fg: FakeGitHub) -> GitHubClient:
    return fg


def test_real_github_satisfies_protocol() -> None:
    assert isinstance(GitHub(), GitHubClient)


def test_fake_github_satisfies_protocol() -> None:
    assert isinstance(FakeGitHub(Sim(ManualClock())), GitHubClient)
