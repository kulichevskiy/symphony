"""CI gate for the review_classifier eval.

Thin pytest wrapper over the harness in `evals/review_classifier/eval.py`.
The eval logic lives in `evals/` so it can also be run as a standalone
report (`uv run python evals/review_classifier/eval.py`); this file just
turns the two headline metrics into gating assertions.

Marked `eval` so the corpus runs can be selected or skipped separately
from the fast unit suite: `pytest -m eval` / `pytest -m "not eval"`.
"""

from __future__ import annotations

import pytest

from evals.review_classifier.eval import run_corpus


@pytest.mark.eval
def test_no_false_approves() -> None:
    """The gate metric: no blocking case may be classified as merge-ok.

    A false-approve is the SYM-28 failure mode — a bad PR auto-merges.
    This must be zero on the curated corpus.
    """
    offenders = [r.case_id for r in run_corpus() if r.is_false_approve]
    assert not offenders, f"false-approve on: {offenders}"


@pytest.mark.eval
def test_curated_cases_match() -> None:
    """Every hand-labelled case must match on both verdict and rule.

    The corpus is curated truth, so a mismatch (even a non-dangerous one,
    e.g. matching the verdict via the wrong rule) is a regression.
    """
    mismatches = [
        f"{r.case_id}: expected {r.expected_kind}/{r.expected_rule}, "
        f"got {r.actual_kind}/{r.actual_rule}"
        for r in run_corpus()
        if not r.passed
    ]
    assert not mismatches, "corpus mismatches:\n" + "\n".join(mismatches)
