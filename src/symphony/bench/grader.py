from __future__ import annotations

import asyncio
import shutil
from importlib.resources import as_file, files
from pathlib import Path
from xml.etree import ElementTree

from .github import CommandError, Commands


def regression_commands() -> dict[str, list[str]]:
    return {
        "backend_tests": ["uv", "run", "--frozen", "--no-sync", "pytest", "-q"],
        "ruff": ["uv", "run", "--frozen", "--no-sync", "ruff", "check", "."],
        "mypy": ["uv", "run", "--frozen", "--no-sync", "mypy", "eventdesk"],
        "frontend_install": ["npm", "ci"],
        "frontend_tests": ["npm", "test", "--", "--run"],
        "frontend_build": ["npm", "run", "build"],
        "frontend_audit": ["npm", "audit", "--audit-level=high"],
    }


def parse_junit_report(path: Path) -> dict[str, int]:
    """Turn pytest's stable JUnit summary into comparable scalar metrics."""
    root = ElementTree.parse(path).getroot()
    if root.tag not in {"testsuite", "testsuites"}:
        raise RuntimeError(f"unexpected JUnit root {root.tag!r}")
    suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
    total = sum(int(suite.attrib.get("tests", 0)) for suite in suites)
    failures = sum(int(suite.attrib.get("failures", 0)) for suite in suites)
    errors = sum(int(suite.attrib.get("errors", 0)) for suite in suites)
    skipped = sum(int(suite.attrib.get("skipped", 0)) for suite in suites)
    return {
        "hidden_checks_total": total,
        "hidden_checks_passed": max(0, total - failures - errors - skipped),
        "hidden_checks_failed": failures,
        "hidden_checks_errors": errors,
        "hidden_checks_skipped": skipped,
    }


class EventDeskGrader:
    """Clone merged main and run tests never copied into the task repository."""

    def __init__(self, commands: Commands) -> None:
        self._commands = commands

    async def grade(
        self,
        *,
        repository_slug: str,
        destination: Path,
        github_token: str,
        hidden_test: Path | None = None,
        checks: dict[str, list[str]] | None = None,
    ) -> dict[str, object]:
        checkout = destination / "final"
        env = {"GH_TOKEN": github_token}
        await self._commands.run(
            ["gh", "repo", "clone", repository_slug, checkout.name],
            cwd=destination,
            env=env,
        )
        await self._commands.run(["uv", "sync", "--locked"], cwd=checkout)
        report = destination / "hidden-junit.xml"
        hidden = hidden_test or files("symphony.bench.assets").joinpath(
            "hidden/test_eventdesk_hidden.py"
        )
        with as_file(hidden) as hidden_path:
            injected_hidden = checkout / "bench_hidden_test.py"
            await asyncio.to_thread(shutil.copyfile, hidden_path, injected_hidden)
            try:
                try:
                    await self._commands.run(
                        [
                            "uv",
                            "run",
                            "--frozen",
                            "--no-sync",
                            "pytest",
                            str(injected_hidden),
                            "-q",
                            f"--junitxml={report}",
                        ],
                        cwd=checkout,
                        env={
                            "EVENTDESK_SESSION_SECRET": "bench-hidden-session-secret",
                            "PAYMENT_WEBHOOK_SECRET": "hidden-secret",
                        },
                    )
                except CommandError:
                    # Product failures are data. Pytest still writes JUnit; only a
                    # missing/invalid report is grader infrastructure failure.
                    if not report.exists():
                        raise
            finally:
                await asyncio.to_thread(injected_hidden.unlink, missing_ok=True)
        metrics: dict[str, object] = {**parse_junit_report(report)}
        results: dict[str, str] = {}
        for name, argv in (checks or regression_commands()).items():
            cwd = checkout / "frontend" if name.startswith("frontend_") else checkout
            try:
                await self._commands.run(argv, cwd=cwd)
                results[name] = "passed"
            except CommandError:
                results[name] = "failed"
        metrics.update(
            regression_checks_total=len(results),
            regression_checks_passed=sum(value == "passed" for value in results.values()),
            regression_checks_failed=sum(value == "failed" for value in results.values()),
            regression_results=results,
        )
        return metrics
