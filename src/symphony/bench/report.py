from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
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
    "raw_tokens",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "effective_tokens",
    "cost_usd",
    "active_agent_seconds",
    "wall_seconds",
    "agent_launches",
    "local_review_agent_launches",
    "local_review_rounds",
    "local_review_findings",
    "local_review_critical",
    "local_review_major",
    "local_review_minor",
    "local_review_unclassified",
    "local_review_unparseable_rounds",
    "remote_review_rounds",
    "remote_review_comments",
    "remote_review_unclassified",
    "remote_review_unparseable",
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
    completed_by_candidate: dict[str, dict[int, TrialRecord]] = defaultdict(dict)
    for trial in report.trials:
        if trial.candidate in {"A", "B"} and trial.status == "completed":
            completed_by_candidate[trial.candidate][trial.repetition] = trial
    matched_repetitions = sorted(
        set(completed_by_candidate["A"]) & set(completed_by_candidate["B"])
    )
    by_candidate = {
        candidate: [completed_by_candidate[candidate][item] for item in matched_repetitions]
        for candidate in ("A", "B")
    }

    lines = [
        f"# Symphony bench {experiment.id}",
        "",
        f"Status: **{experiment.status}**  ",
    ]
    if experiment.mode == "single":
        completed_a = [
            trial
            for trial in report.trials
            if trial.candidate == "A" and trial.status == "completed"
        ]
        lines.extend(
            [
                "Mode: single candidate  ",
                f"Candidate A: `{experiment.candidate_a}`  ",
                f"System A: `{experiment.system_version_a}`  ",
                "| Metric | Candidate A |",
                "|---|---:|",
            ]
        )
        for metric in _SUMMARY_METRICS:
            lines.append(f"| {metric} | {_number(_average(completed_a, metric))} |")
    else:
        lines.extend(
            [
                f"Candidate A: `{experiment.candidate_a}`  ",
                f"Candidate B: `{experiment.candidate_b}`  ",
                f"System A: `{experiment.system_version_a}`  ",
                f"System B: `{experiment.system_version_b}`  ",
                "Matched completed repetitions: "
                + (", ".join(str(item) for item in matched_repetitions) or "none"),
                "| Metric | A | B | B − A |",
                "|---|---:|---:|---:|",
            ]
        )
        for metric in _SUMMARY_METRICS:
            a_value = _average(by_candidate["A"], metric)
            b_value = _average(by_candidate["B"], metric)
            delta = b_value - a_value if a_value is not None and b_value is not None else None
            lines.append(
                f"| {metric} | {_number(a_value)} | {_number(b_value)} | "
                f"{_number(delta, signed=True)} |"
            )

    repetition_line = f"Repetitions: {experiment.repetitions}"
    if experiment.mode == "paired":
        repetition_line += "; order: A/B pairs in parallel"
    common = [
        f"Executor toolchain: `{experiment.executor_toolchain_version}`  ",
        f"Harness: `{experiment.harness_version}`  ",
        repetition_line,
        "",
        "## Aggregate means",
        "",
    ]
    table_start = next(index for index, line in enumerate(lines) if line.startswith("| Metric |"))
    lines[table_start:table_start] = common

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


def render_trial_markdown(report: ExperimentReport, record: TrialRecord) -> str:
    label = f"{record.candidate}{record.repetition}"
    lines = [
        f"# Symphony bench {report.experiment.id} — {label}",
        "",
        f"Status: **{record.status}**",
        f"Revision: `{record.revision}`",
        f"System version: `{record.system_version}`",
        f"Started: `{record.started_at.isoformat()}`",
        f"Ended: `{record.ended_at.isoformat() if record.ended_at else 'running'}`",
        f"Repository: {record.repository_url or 'unavailable'}",
        f"Issues: {', '.join(record.issue_urls) if record.issue_urls else 'none'}",
    ]
    if record.error:
        lines.append(f"Error: `{record.error}`")
    lines.extend(["", "## Metrics", ""])
    for key in sorted(record.metrics):
        value = record.metrics[key]
        rendered = (
            json.dumps(value, sort_keys=True)
            if value is None or isinstance(value, (dict, list))
            else str(value)
        )
        lines.append(f"- {key}: `{rendered}`")
    lines.extend(
        [
            "",
            "## Next step",
            "",
            "Next step: continue the experiment if healthy; otherwise diagnose this receipt.",
            "",
        ]
    )
    return "\n".join(lines)


def persist_trial_markdown(root: Path, report: ExperimentReport, record: TrialRecord) -> str:
    body = render_trial_markdown(report, record)
    directory = root / report.experiment.id
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"{record.candidate}{record.repetition}.md"
    temporary = destination.with_suffix(".md.tmp")
    temporary.write_text(body, encoding="utf-8")
    temporary.replace(destination)
    return body


def persist_experiment_markdown(root: Path, report: ExperimentReport) -> str:
    body = render_markdown(report)
    directory = root / report.experiment.id
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / "FINAL.md"
    temporary = destination.with_suffix(".md.tmp")
    temporary.write_text(body, encoding="utf-8")
    temporary.replace(destination)
    return body
