from __future__ import annotations

from collections import defaultdict
from statistics import mean

from .models import ExperimentReport, TrialRecord

_SUMMARY_METRICS = (
    "completed_tickets",
    "hidden_checks_passed",
    "hidden_checks_total",
    "hidden_checks_failed",
    "hidden_checks_errors",
    "regression_checks_passed",
    "regression_checks_total",
    "regression_checks_failed",
    "spec_findings_critical",
    "spec_findings_major",
    "spec_findings_minor",
    "standards_findings_critical",
    "standards_findings_major",
    "standards_findings_minor",
    "effective_tokens",
    "active_agent_seconds",
    "wall_seconds",
    "agent_launches",
    "remote_review_rounds",
    "remote_review_comments",
    "remote_review_p0",
    "remote_review_p1",
    "remote_review_p2",
    "remote_review_p3",
)


def _average(trials: list[TrialRecord], metric: str) -> float | None:
    values = [trial.metrics.get(metric) for trial in trials]
    numeric = [float(value) for value in values if isinstance(value, (int, float))]
    return mean(numeric) if numeric else None


def _number(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "—"
    prefix = "+" if signed and value > 0 else ""
    if value.is_integer():
        return f"{prefix}{int(value)}"
    return f"{prefix}{value:.2f}"


def render_markdown(report: ExperimentReport) -> str:
    experiment = report.experiment
    by_candidate: dict[str, list[TrialRecord]] = defaultdict(list)
    for trial in report.trials:
        by_candidate[trial.candidate].append(trial)

    lines = [
        f"# Symphony bench {experiment.id}",
        "",
        f"Status: **{experiment.status}**  ",
        f"Candidate A: `{experiment.candidate_a}`  ",
        f"Candidate B: `{experiment.candidate_b}`  ",
        f"System A: `{experiment.system_version_a}`  ",
        f"System B: `{experiment.system_version_b}`  ",
        f"Harness: `{experiment.harness_version}`  ",
        f"Repetitions: {experiment.repetitions}; order: A1, B1, A2, B2 …",
        "",
        "## Aggregate means",
        "",
        "| Metric | A | B | B − A |",
        "|---|---:|---:|---:|",
    ]
    for metric in _SUMMARY_METRICS:
        a_value = _average(by_candidate["A"], metric)
        b_value = _average(by_candidate["B"], metric)
        delta = b_value - a_value if a_value is not None and b_value is not None else None
        lines.append(
            f"| {metric} | {_number(a_value)} | {_number(b_value)} | "
            f"{_number(delta, signed=True)} |"
        )

    lines.extend(["", "## Trial receipts", ""])
    for trial in report.trials:
        label = f"{trial.candidate}{trial.repetition}"
        repository = (
            f"[{trial.repository_url}]({trial.repository_url})"
            if trial.repository_url
            else "repository unavailable"
        )
        lines.extend(
            [
                f"### {label} — {trial.status}",
                "",
                f"Revision: `{trial.revision}`  ",
                f"Repository: {repository}  ",
                f"Issues: {', '.join(trial.issue_urls) if trial.issue_urls else 'none'}",
            ]
        )
        if trial.error:
            lines.append(f"Error: `{trial.error}`")
        for lens in ("spec", "standards"):
            raw_findings = trial.metrics.get(f"{lens}_findings")
            if not isinstance(raw_findings, list) or not raw_findings:
                continue
            lines.extend(["", f"{lens.title()} findings:"])
            for finding in raw_findings:
                if not isinstance(finding, dict):
                    continue
                lines.append(
                    f"- **{finding.get('severity', '?')} — {finding.get('title', '?')}** "
                    f"({finding.get('evidence', '?')}): {finding.get('explanation', '')}"
                )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
