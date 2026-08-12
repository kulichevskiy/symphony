from __future__ import annotations

from .models import Experiment, Trial, TrialOutcome


def project_description(subject: Experiment | Trial) -> str:
    return "\n".join(
        (
            "## Hypothesis",
            "",
            subject.hypothesis,
            "",
            "## Experiment design",
            "",
            subject.design,
            "",
            "## Terms",
            "",
            "- **Symphony** is the automated software-development system being tested. It reads "
            "work tickets, changes code, reviews the changes, and runs checks without human help.",
            "- **Sample project** is a fresh copy of Support Queue, the same small test "
            "application used for every run.",
            "- **Version** means one exact Symphony code revision and configuration.",
            "- **Run** means one isolated execution against a fresh copy of the sample project.",
            "- **Hidden checks** are tests that the system cannot see while it is working.",
        )
    )


def launch_update(subject: Experiment | Trial) -> str:
    return "\n".join(
        (
            "## Experiment started",
            "",
            "The experiment has started and is currently on track.",
            "",
            "### Hypothesis",
            "",
            subject.hypothesis,
            "",
            "### Design",
            "",
            subject.design,
            "",
            f"Version A uses revision `{_revision(subject)}`.",
        )
    )


def successful_run_update(trial: Trial, outcome: TrialOutcome) -> str:
    metrics = outcome.metrics
    return "\n".join(
        (
            f"## Version {trial.candidate}, run {trial.repetition} finished successfully",
            "",
            "This isolated run completed all required work and its final checks finished.",
            "",
            f"- Exact revision: `{trial.revision}`",
            f"- Completed tickets: {_metric(metrics, 'completed_tickets')}",
            "- Hidden checks: "
            f"{_metric(metrics, 'hidden_checks_passed')}/"
            f"{_metric(metrics, 'hidden_checks_total')}",
            f"- Duration: {_duration(metrics)}",
            f"- Recorded cost: {_cost(metrics)}",
            f"- Result repository: {outcome.repository_url or 'unavailable'}",
        )
    )


def failed_run_update(trial: Trial, error: str, outcome: TrialOutcome) -> str:
    return "\n".join(
        (
            f"## Version {trial.candidate}, run {trial.repetition} failed",
            "",
            "The experiment is off track. This run did not finish the complete sample project.",
            "",
            f"- Exact revision: `{trial.revision}`",
            f"- Failure: `{error}`",
            "- Completed tickets before the failure: "
            f"{_metric(outcome.metrics, 'completed_tickets')}",
            f"- Duration before the failure: {_duration(outcome.metrics)}",
            f"- Result repository: {outcome.repository_url or 'unavailable'}",
            "",
            "The experiment report preserves the detailed logs and measurements for diagnosis.",
        )
    )


def failed_experiment_update(experiment: Experiment) -> str:
    return "\n".join(
        (
            "## Experiment failed",
            "",
            "The experiment is off track because at least one required run did not finish "
            "successfully. Later successful runs do not change this final result.",
            "",
            f"- Experiment: `{experiment.id}`",
            "- Final result: failed",
            "",
            "See the earlier run updates and the saved experiment report for the failure details.",
        )
    )


def _metric(metrics: dict[str, object], name: str) -> str:
    value = metrics.get(name)
    return str(value) if value is not None else "unavailable"


def _duration(metrics: dict[str, object]) -> str:
    value = metrics.get("wall_seconds")
    if not isinstance(value, (int, float)):
        return "unavailable"
    return f"{value:.1f} seconds"


def _cost(metrics: dict[str, object]) -> str:
    value = metrics.get("cost_usd")
    if not isinstance(value, (int, float)):
        return "unavailable"
    return f"${value:.4f}"


def _revision(subject: Experiment | Trial) -> str:
    return subject.candidate_a if isinstance(subject, Experiment) else subject.revision
