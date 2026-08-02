from pathlib import Path

import pytest

from symphony.bench.github import CommandError
from symphony.bench.grader import (
    FeedbackInboxGrader,
    GraderInfrastructureError,
    HiddenManifest,
    load_hidden_manifest,
    parse_junit_report,
    parse_vitest_report,
    validate_control_result,
)


class GraderCommands:
    def __init__(self) -> None:
        self.hidden_paths: list[Path] = []
        self.hidden_env: dict[str, str] | None = None

    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str:
        del stdin
        if argv[:3] == ["gh", "repo", "clone"]:
            (cwd / "final").mkdir()
            (cwd / "final/frontend/src").mkdir(parents=True)
        if "--junitxml=" in " ".join(argv):
            self.hidden_env = env
            hidden_path = Path(argv[-3])
            self.hidden_paths.append(hidden_path)
            assert hidden_path.name == "bench_hidden_test.py"
            assert hidden_path.exists()  # noqa: ASYNC240 - synchronous fake command
            report = next(Path(item.split("=", 1)[1]) for item in argv if "--junitxml=" in item)
            report.write_text(
                '<testsuite tests="9" failures="0" errors="0" skipped="0" />',
                encoding="utf-8",
            )
        if any(item.startswith("--outputFile=") for item in argv):
            hidden_path = cwd / "src/App.bench.test.tsx"
            self.hidden_paths.append(hidden_path)
            assert hidden_path.exists()
            report = next(
                Path(item.split("=", 1)[1]) for item in argv if item.startswith("--outputFile=")
            )
            report.write_text(
                '{"numTotalTests":7,"numPassedTests":7,"numFailedTests":0,'
                '"numPendingTests":0,"numTodoTests":0}',
                encoding="utf-8",
            )
        return ""


class CrashingGraderCommands(GraderCommands):
    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str:
        result = await super().run(argv, cwd=cwd, env=env, stdin=stdin)
        if "--junitxml=" in " ".join(argv):
            raise CommandError("pytest exited 137: killed")
        return result


def test_parse_junit_report_counts_outcomes(tmp_path: Path) -> None:
    report = tmp_path / "report.xml"
    report.write_text(
        """<?xml version="1.0"?>
        <testsuites tests="7" failures="2" errors="1" skipped="1">
          <testsuite name="hidden" tests="7" failures="2" errors="1" skipped="1" />
        </testsuites>
        """,
        encoding="utf-8",
    )

    assert parse_junit_report(report) == {
        "hidden_checks_total": 7,
        "hidden_checks_passed": 3,
        "hidden_checks_failed": 2,
        "hidden_checks_errors": 1,
        "hidden_checks_skipped": 1,
    }


def test_parse_junit_report_supports_single_suite(tmp_path: Path) -> None:
    report = tmp_path / "report.xml"
    report.write_text(
        '<testsuite tests="2" failures="0" errors="0" skipped="0" />',
        encoding="utf-8",
    )

    assert parse_junit_report(report)["hidden_checks_passed"] == 2


def test_parse_vitest_report_counts_outcomes(tmp_path: Path) -> None:
    report = tmp_path / "vitest.json"
    report.write_text(
        '{"numTotalTests":5,"numPassedTests":3,"numFailedTests":1,'
        '"numPendingTests":1,"numTodoTests":0,"testResults":[]}',
        encoding="utf-8",
    )

    assert parse_vitest_report(report) == {
        "hidden_checks_total": 5,
        "hidden_checks_passed": 3,
        "hidden_checks_failed": 1,
        "hidden_checks_errors": 0,
        "hidden_checks_skipped": 1,
    }


def test_parse_vitest_report_rejects_impossible_accounting(tmp_path: Path) -> None:
    report = tmp_path / "vitest.json"
    report.write_text(
        '{"numTotalTests":5,"numPassedTests":5,"numFailedTests":1,'
        '"numPendingTests":0,"numTodoTests":0}',
        encoding="utf-8",
    )

    with pytest.raises(GraderInfrastructureError, match="accounting"):
        parse_vitest_report(report)


def test_control_validation_accepts_reference_and_expected_negative_control() -> None:
    manifest = HiddenManifest(
        backend_total=4,
        frontend_total=3,
        seed_backend_passed=1,
        seed_frontend_passed=1,
    )
    reference = {
        "backend_hidden_checks_total": 4,
        "backend_hidden_checks_passed": 4,
        "backend_hidden_checks_failed": 0,
        "backend_hidden_checks_errors": 0,
        "backend_hidden_checks_skipped": 0,
        "frontend_hidden_checks_total": 3,
        "frontend_hidden_checks_passed": 3,
        "frontend_hidden_checks_failed": 0,
        "frontend_hidden_checks_errors": 0,
        "frontend_hidden_checks_skipped": 0,
    }
    seed = {
        **reference,
        "backend_hidden_checks_passed": 1,
        "backend_hidden_checks_failed": 3,
        "frontend_hidden_checks_passed": 1,
        "frontend_hidden_checks_failed": 2,
    }

    validate_control_result(reference, manifest, control="reference")
    validate_control_result(seed, manifest, control="seed")


def test_control_validation_rejects_broken_grader_as_infrastructure_failure() -> None:
    manifest = HiddenManifest(
        backend_total=4,
        frontend_total=3,
        seed_backend_passed=1,
        seed_frontend_passed=1,
    )
    broken = {
        "backend_hidden_checks_total": 0,
        "backend_hidden_checks_passed": 0,
        "backend_hidden_checks_failed": 0,
        "backend_hidden_checks_errors": 0,
        "backend_hidden_checks_skipped": 0,
        "frontend_hidden_checks_total": 3,
        "frontend_hidden_checks_passed": 3,
        "frontend_hidden_checks_failed": 0,
        "frontend_hidden_checks_errors": 0,
        "frontend_hidden_checks_skipped": 0,
    }

    with pytest.raises(GraderInfrastructureError, match="expected 4 backend checks, got 0"):
        validate_control_result(broken, manifest, control="reference")


def test_hidden_manifest_cannot_redefine_the_fixed_benchmark_contract(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        '{"backend_total":2,"frontend_total":2,'
        '"seed_backend_passed":1,"seed_frontend_passed":1}',
        encoding="utf-8",
    )

    with pytest.raises(GraderInfrastructureError, match="fixed benchmark contract"):
        load_hidden_manifest(manifest)


def test_parse_junit_report_sums_pytest_suites_when_parent_has_no_counts(
    tmp_path: Path,
) -> None:
    report = tmp_path / "report.xml"
    report.write_text(
        """<testsuites name="pytest tests">
        <testsuite name="hidden-a" tests="3" failures="1" errors="0" skipped="0" />
        <testsuite name="hidden-b" tests="2" failures="0" errors="1" skipped="0" />
        </testsuites>""",
        encoding="utf-8",
    )

    assert parse_junit_report(report) == {
        "hidden_checks_total": 5,
        "hidden_checks_passed": 3,
        "hidden_checks_failed": 1,
        "hidden_checks_errors": 1,
        "hidden_checks_skipped": 0,
    }


@pytest.mark.asyncio
async def test_grader_injects_collectable_hidden_name_then_removes_it(tmp_path: Path) -> None:
    commands = GraderCommands()
    backend_hidden = tmp_path / "backend.py"
    frontend_hidden = tmp_path / "frontend.tsx"
    backend_hidden.write_text("# hidden")
    frontend_hidden.write_text("// hidden")

    metrics = await FeedbackInboxGrader(commands).grade(
        repository_slug="kulichevskiy/trial",
        destination=tmp_path,
        github_token="token",
        backend_hidden_test=backend_hidden,
        frontend_hidden_test=frontend_hidden,
        manifest=HiddenManifest(9, 7, 1, 1),
    )

    assert metrics["backend_hidden_checks_passed"] == 9
    assert metrics["frontend_hidden_checks_passed"] == 7
    assert commands.hidden_env is not None
    assert commands.hidden_env["FEEDBACK_INBOX_DB_PATH"].endswith("hidden-feedback.sqlite")
    assert all(not path.exists() for path in commands.hidden_paths)


@pytest.mark.asyncio
async def test_grader_classifies_crashed_process_as_infrastructure_even_with_report(
    tmp_path: Path,
) -> None:
    backend_hidden = tmp_path / "backend.py"
    frontend_hidden = tmp_path / "frontend.tsx"
    backend_hidden.write_text("# hidden")
    frontend_hidden.write_text("// hidden")

    with pytest.raises(GraderInfrastructureError, match="process failed"):
        await FeedbackInboxGrader(CrashingGraderCommands()).grade(
            repository_slug="kulichevskiy/trial",
            destination=tmp_path,
            github_token="token",
            backend_hidden_test=backend_hidden,
            frontend_hidden_test=frontend_hidden,
            manifest=HiddenManifest(9, 7, 1, 1),
        )
