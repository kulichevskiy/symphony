import json

import pytest

from symphony.bench.reviewer import (
    cleanup_stale_reviewer_credentials,
    parse_review,
    review_metrics,
)


def test_parse_review_accepts_fenced_json() -> None:
    result = parse_review(
        """```json
        {"findings":[
          {"severity":"Major","title":"Race","evidence":"feedback_inbox/main.py:80","explanation":"Unsafe"},
          {"severity":"Minor","title":"Naming","evidence":"frontend/src/App.tsx:12","explanation":"Unclear"}
        ]}
        ```"""
    )

    assert [finding.severity for finding in result.findings] == ["Major", "Minor"]


def test_review_metrics_keeps_spec_and_standards_separate() -> None:
    spec = parse_review(
        json.dumps(
            {
                "findings": [
                    {
                        "severity": "Critical",
                        "title": "Broken invariant",
                        "evidence": "feedback_inbox/main.py:1",
                        "explanation": "Data loss",
                    }
                ]
            }
        )
    )
    standards = parse_review('{"findings":[]}')

    metrics = review_metrics(spec=spec, standards=standards)

    assert metrics["spec_findings_critical"] == 1
    assert metrics["standards_findings_total"] == 0
    assert metrics["spec_findings"][0]["title"] == "Broken invariant"


@pytest.mark.parametrize("payload", ["not json", '{"findings":[{"severity":"P1"}]}'])
def test_parse_review_rejects_invalid_contract(payload: str) -> None:
    with pytest.raises(RuntimeError):
        parse_review(payload)


def test_cleanup_stale_reviewer_credentials_removes_interrupted_auth(tmp_path) -> None:
    stale = tmp_path / "EXP-1" / "A1" / "bench-codex-stale"
    stale.mkdir(parents=True)
    (stale / "auth.json").write_text("plaintext credential")
    keep = tmp_path / "EXP-1" / "A1" / "candidate-profile.json"
    keep.write_text("{}")

    assert cleanup_stale_reviewer_credentials(tmp_path) == 1
    assert not stale.exists()
    assert keep.exists()
