from datetime import UTC, datetime

from symphony.bench.models import Experiment, ExperimentReport, TrialRecord
from symphony.bench.report import render_markdown, render_trial_markdown


def _experiment(**values: object) -> Experiment:
    return Experiment(
        hypothesis="The tested version will finish the complete sample project.",
        design="Run an isolated copy and record completion, quality, time, and cost.",
        **values,
    )


def _trial(candidate: str, repetition: int, tokens: float, passed: int) -> TrialRecord:
    return TrialRecord(
        experiment_id="EXP-1",
        candidate=candidate,  # type: ignore[arg-type]
        repetition=repetition,
        revision="same-sha",
        status="completed",
        started_at=datetime(2026, 8, 1, tzinfo=UTC),
        ended_at=datetime(2026, 8, 1, 1, tzinfo=UTC),
        repository_url=f"https://github.com/kulichevskiy/EXP-1-{candidate}{repetition}",
        issue_urls=[f"https://linear.app/bench/BENCH-{repetition}"],
        metrics={
            "effective_tokens": tokens,
            "raw_tokens": tokens * 2,
            "hidden_checks_passed": passed,
            "hidden_checks_total": 10,
            "spec_findings_total": 0,
            "standards_findings_total": 0,
        },
    )


def test_render_markdown_has_aggregate_and_trial_receipts() -> None:
    report = ExperimentReport(
        experiment=_experiment(
            id="EXP-1",
            status="completed",
            candidate_a="same-sha",
            candidate_b="same-sha",
            repetitions=2,
            created_at=datetime(2026, 8, 1, tzinfo=UTC),
        ),
        trials=[
            _trial("A", 1, 100, 9),
            _trial("B", 1, 120, 8),
            _trial("A", 2, 140, 10),
            _trial("B", 2, 160, 9),
        ],
    )

    rendered = render_markdown(report)

    assert "# Symphony bench EXP-1" in rendered
    assert "| effective_tokens | 120 | 140 | +20 |" in rendered
    assert "| raw_tokens | 240 | 280 | +40 |" in rendered
    assert "EXP-1-A1" in rendered
    assert "same-sha" in rendered


def test_render_markdown_aggregates_only_matched_completed_pairs() -> None:
    unmatched_a = _trial("A", 2, 1000, 1)
    failed_b = _trial("B", 2, 2000, 10).model_copy(
        update={"status": "failed", "error": "trial failed"}
    )
    report = ExperimentReport(
        experiment=_experiment(
            id="EXP-1",
            status="failed",
            candidate_a="same-sha",
            candidate_b="same-sha",
            repetitions=2,
            created_at=datetime(2026, 8, 1, tzinfo=UTC),
        ),
        trials=[
            _trial("A", 1, 100, 9),
            _trial("B", 1, 120, 8),
            unmatched_a,
            failed_b,
        ],
    )

    rendered = render_markdown(report)

    assert "Matched completed repetitions: 1" in rendered
    assert "| effective_tokens | 100 | 120 | +20 |" in rendered
    assert "| effective_tokens | 550 |" not in rendered


def test_render_markdown_single_mode_reports_one_candidate_without_fake_comparison() -> None:
    report = ExperimentReport(
        experiment=_experiment(
            id="EXP-1",
            status="completed",
            candidate_a="same-sha",
            repetitions=1,
            mode="single",
            created_at=datetime(2026, 8, 1, tzinfo=UTC),
        ),
        trials=[_trial("A", 1, 100, 9)],
    )

    rendered = render_markdown(report)

    assert "Mode: single candidate" in rendered
    assert "Candidate B" not in rendered
    assert "B − A" not in rendered
    assert "| effective_tokens | 100 |" in rendered


def test_render_trial_markdown_preserves_unavailable_raw_tokens() -> None:
    trial = _trial("A", 1, 100, 9).model_copy(
        update={
            "status": "failed",
            "metrics": {
                "raw_tokens": None,
                "token_metrics_unavailable": True,
            },
        }
    )
    report = ExperimentReport(
        experiment=_experiment(
            id="EXP-1",
            status="failed",
            candidate_a="same-sha",
            candidate_b="same-sha",
            repetitions=1,
            created_at=datetime(2026, 8, 1, tzinfo=UTC),
        ),
        trials=[trial],
    )

    rendered = render_trial_markdown(report, trial)

    assert "raw_tokens: `null`" in rendered
    assert "token_metrics_unavailable: `True`" in rendered
