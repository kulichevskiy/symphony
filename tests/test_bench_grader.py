from pathlib import Path

import pytest

from symphony.bench.grader import EventDeskGrader, parse_junit_report


class GraderCommands:
    def __init__(self) -> None:
        self.hidden_path: Path | None = None
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
        if "--junitxml=" in " ".join(argv):
            self.hidden_env = env
            self.hidden_path = Path(argv[-3])
            assert self.hidden_path.name == "bench_hidden_test.py"
            assert self.hidden_path.exists()
            report = next(Path(item.split("=", 1)[1]) for item in argv if "--junitxml=" in item)
            report.write_text(
                '<testsuite tests="1" failures="0" errors="0" skipped="0" />',
                encoding="utf-8",
            )
        return ""


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

    metrics = await EventDeskGrader(commands).grade(
        repository_slug="kulichevskiy/trial",
        destination=tmp_path,
        github_token="token",
    )

    assert metrics["hidden_checks_passed"] == 1
    assert commands.hidden_path is not None
    assert commands.hidden_env == {
        "EVENTDESK_SESSION_SECRET": "bench-hidden-session-secret",
        "PAYMENT_WEBHOOK_SECRET": "hidden-secret",
    }
    assert not commands.hidden_path.exists()
