"""Regression coverage for user-facing review configuration docs."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text()


def test_example_config_documents_review_booleans_and_local_lane() -> None:
    text = _read("examples/config.yaml")

    assert "local_review:" in text
    assert "remote_review:" in text
    assert "local_code_review:" in text
    assert "legacy `review_strategy` is deprecated" in text

    for row in (
        "local_review=false remote_review=true",
        "local_review=true  remote_review=false",
        "local_review=true  remote_review=true",
        "local_review=false remote_review=false",
    ):
        assert row in text


def test_example_config_documents_roles_matrix() -> None:
    text = _read("examples/config.yaml")

    # AC3: the `roles:` matrix is documented — global default block, ...
    assert "roles:" in text
    # ... a per-binding override (the per-repo `roles:` block), ...
    assert "model: sonnet" in text
    # ... and at least one legacy -> matrix mapping line.
    assert "agent                              -> roles.{implement,fix,accept}.agent" in text


def test_readme_documents_review_venues_and_role_knobs() -> None:
    text = _read("README.md")

    assert "`local_review`" in text
    assert "`remote_review`" in text
    assert "`local_code_review`" in text
    assert "`agent` is the builder" in text
    assert "`reviewer_agent`" in text
    assert "defaults to the opposite agent family" in text
    assert "remote reviewer is the `@codex` GitHub bot" in text
    assert "`review_strategy`" not in text
