"""The review-stage `@codex review` gate is driven by `remote_review`.

The orchestrator posts the remote `@codex review` ping iff the binding's
`resolved_remote_review()` is true — orthogonal to the local-review
outcome. `remote_review: false` (local-only and no-review modes) must
never post, dropping the old `local`-strategy remote fallback.
"""

from __future__ import annotations

import warnings

import pytest

from symphony.config import LinearStates, RepoBinding


def _binding(**overrides: object) -> RepoBinding:
    data: dict[str, object] = {
        "github_repo": "acme/widgets",
        "project_key": "WID",
        "states": LinearStates(ready="Todo", code_review="In Review"),
    }
    data.update(overrides)
    return RepoBinding(**data)  # type: ignore[arg-type]


# --- truth table: post iff remote_review --------------------------------


@pytest.mark.parametrize(
    ("local", "remote", "should_post"),
    [
        (False, True, True),  # remote-only (default)
        (True, False, False),  # local-only — @codex never fires
        (True, True, True),  # hybrid: local loop → PR → remote loop
        (False, False, False),  # no review
    ],
)
def test_post_codex_review_follows_remote_review(
    local: bool, remote: bool, should_post: bool
) -> None:
    binding = _binding(local_review=local, remote_review=remote)
    assert binding.resolved_remote_review() is should_post


def test_default_binding_posts_codex_review() -> None:
    """A binding that sets neither field defaults to remote-only."""
    assert _binding().resolved_remote_review() is True


# --- legacy strategy never resurrects the remote fallback ---------------


@pytest.mark.parametrize(
    ("strategy", "should_post"),
    [
        ("remote", True),
        ("hybrid", True),
        ("local", False),  # local-only: the old remote fallback is gone
    ],
)
def test_legacy_strategy_gate(strategy: str, should_post: bool) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        binding = _binding(review_strategy=strategy)
    assert binding.resolved_remote_review() is should_post
