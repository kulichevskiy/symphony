"""Eval harness for `review_classifier` over a corpus of real PR snapshots.

This is an *assertion eval*: every case in `corpus.jsonl` carries a
hand-labelled expected verdict, and we measure how often the classifier
agrees. Unlike a unit test (one synthetic input → one assertion), the
corpus is grown from production incidents and the headline number is an
*aggregate* metric over the whole set.

The metric that gates CI is the **false-approve rate**: cases where the
classifier said `approved` but the truth is "do not merge". That is the
asymmetric, expensive error — it lets a bad PR auto-merge (the SYM-28
regression). A false *block* (we said changes_requested but the PR was
fine) only wastes a fix-run, so it is reported but does not gate.

Run it directly to see the report:

    uv run python evals/review_classifier/eval.py

Exit code is non-zero if any false-approve is present, so the same entry
point doubles as a CI gate.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from symphony.pipeline.review_classifier import (
    CheckRun,
    Reaction,
    Review,
    ReviewComment,
    ReviewSnapshot,
    review_classifier,
)

CORPUS_PATH = Path(__file__).with_name("corpus.jsonl")

# The verdict kinds that mean "do not merge". Anything not in this set is
# a merge-permitting verdict, and saying it wrongly is the dangerous error.
_BLOCKING_KINDS = {"changes_requested", "pending"}


@dataclass(frozen=True)
class CaseResult:
    case_id: str
    note: str
    expected_kind: str
    expected_rule: str
    actual_kind: str
    actual_rule: str

    @property
    def kind_ok(self) -> bool:
        return self.actual_kind == self.expected_kind

    @property
    def rule_ok(self) -> bool:
        return self.actual_rule == self.expected_rule

    @property
    def passed(self) -> bool:
        # A case passes only if both the verdict and the rule that produced
        # it match. Matching the verdict via the wrong rule is a latent bug.
        return self.kind_ok and self.rule_ok

    @property
    def is_false_approve(self) -> bool:
        # Truth says block, classifier let it through. The expensive error.
        return self.expected_kind in _BLOCKING_KINDS and self.actual_kind not in _BLOCKING_KINDS

    @property
    def is_false_block(self) -> bool:
        # Truth says merge-ok, classifier blocked it. Wasteful, not unsafe.
        return self.expected_kind not in _BLOCKING_KINDS and self.actual_kind in _BLOCKING_KINDS


def load_corpus(path: Path = CORPUS_PATH) -> list[dict]:
    """Parse the JSONL corpus into a list of raw case dicts."""
    cases: list[dict] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        try:
            cases.append(json.loads(line))
        except json.JSONDecodeError as exc:  # pragma: no cover - corpus hygiene
            raise ValueError(f"{path}:{line_no}: invalid JSON — {exc}") from exc
    return cases


def _build_snapshot(raw: dict) -> ReviewSnapshot:
    """Rehydrate the nested JSON snapshot into the classifier's dataclass."""
    return ReviewSnapshot(
        head_sha=raw["head_sha"],
        head_committed_at=raw["head_committed_at"],
        mergeable=raw.get("mergeable"),
        reactions=tuple(Reaction(**r) for r in raw.get("reactions", [])),
        reviews=tuple(Review(**r) for r in raw.get("reviews", [])),
    )


def run_case(case: dict) -> CaseResult:
    """Run one corpus case through the classifier and capture the outcome."""
    comments = [ReviewComment(**c) for c in case.get("comments", [])]
    ci = [CheckRun(**c) for c in case.get("ci", [])]
    snapshot = _build_snapshot(case["snapshot"])
    verdict = review_classifier(comments=comments, ci=ci, snapshot=snapshot)
    expected = case["expected"]
    return CaseResult(
        case_id=case["id"],
        note=case.get("note", ""),
        expected_kind=expected["kind"],
        expected_rule=expected["rule"],
        actual_kind=verdict.kind.value,
        actual_rule=verdict.rule,
    )


def run_corpus(path: Path = CORPUS_PATH) -> list[CaseResult]:
    return [run_case(case) for case in load_corpus(path)]


def render_report(results: list[CaseResult]) -> str:
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    false_approves = [r for r in results if r.is_false_approve]
    false_blocks = [r for r in results if r.is_false_block]

    lines: list[str] = ["", "review_classifier eval", "=" * 60]
    for r in results:
        mark = "PASS" if r.passed else "FAIL"
        lines.append(f"[{mark}] {r.case_id}")
        if not r.passed:
            lines.append(
                f"       expected {r.expected_kind}/{r.expected_rule}"
                f"  got {r.actual_kind}/{r.actual_rule}"
            )
            if r.is_false_approve:
                lines.append("       ⚠ FALSE-APPROVE — a blocking case was let through")
    lines.append("-" * 60)
    lines.append(f"cases            : {total}")
    lines.append(f"passed           : {passed}/{total}")
    lines.append(f"false-approves   : {len(false_approves)}   <-- gate metric")
    lines.append(f"false-blocks     : {len(false_blocks)}   (wasteful, not gated)")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    results = run_corpus()
    print(render_report(results))
    false_approves = sum(1 for r in results if r.is_false_approve)
    failures = sum(1 for r in results if not r.passed)
    if false_approves:
        print(f"GATE FAILED: {false_approves} false-approve(s) on the corpus.")
        return 1
    if failures:
        print(
            f"GATE FAILED: {failures} case(s) mismatched "
            "(no false-approve, but curated truth must match)."
        )
        return 1
    print("GATE PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
