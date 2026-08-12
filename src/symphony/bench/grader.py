from __future__ import annotations

import asyncio
import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from xml.etree import ElementTree

from .github import CommandError, Commands

_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_FAILURE_SUMMARY_LIMIT = 500


class GraderInfrastructureError(RuntimeError):
    """The hidden checks did not execute reliably enough to score a product."""


@dataclass(frozen=True)
class ControlExpectation:
    backend_passed: int
    frontend_passed: int
    backend_failed_test_ids: tuple[str, ...] = ()
    frontend_failed_test_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class HiddenManifest:
    backend_total: int
    frontend_total: int
    seed_backend_passed: int
    seed_frontend_passed: int
    mutations: dict[str, ControlExpectation] = field(default_factory=dict)


_FIXED_SUPPORT_QUEUE_MANIFEST = HiddenManifest(
    backend_total=16,
    frontend_total=13,
    seed_backend_passed=1,
    seed_frontend_passed=1,
    mutations={
        "broken_workflow": ControlExpectation(
            backend_passed=14,
            frontend_passed=13,
            backend_failed_test_ids=(
                "test_failed_update_is_atomic",
                "test_status_transition_matrix",
            ),
        ),
        "broken_accessibility": ControlExpectation(
            backend_passed=16,
            frontend_passed=12,
            frontend_failed_test_ids=(
                "Support Queue hidden contract provides named navigation, live state, "
                "and labeled controls",
            ),
        ),
    },
)


def load_hidden_manifest(path: Path) -> HiddenManifest:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw_mutations = payload["mutations"]
        if not isinstance(raw_mutations, dict):
            raise TypeError("mutations must be an object")
        mutations = {
            str(name): ControlExpectation(
                backend_passed=int(expectation["backend_passed"]),
                frontend_passed=int(expectation["frontend_passed"]),
                backend_failed_test_ids=_test_ids(expectation.get("backend_failed_test_ids", [])),
                frontend_failed_test_ids=_test_ids(expectation.get("frontend_failed_test_ids", [])),
            )
            for name, expectation in raw_mutations.items()
            if isinstance(expectation, dict)
        }
        if len(mutations) != len(raw_mutations):
            raise TypeError("mutation expectation must be an object")
        manifest = HiddenManifest(
            backend_total=int(payload["backend_total"]),
            frontend_total=int(payload["frontend_total"]),
            seed_backend_passed=int(payload["seed_backend_passed"]),
            seed_frontend_passed=int(payload["seed_frontend_passed"]),
            mutations=mutations,
        )
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise GraderInfrastructureError(f"invalid hidden-check manifest: {exc}") from exc
    if (
        manifest.backend_total < 1
        or manifest.frontend_total < 1
        or not 0 <= manifest.seed_backend_passed < manifest.backend_total
        or not 0 <= manifest.seed_frontend_passed < manifest.frontend_total
        or any(
            not 0 <= expected.backend_passed <= manifest.backend_total
            or not 0 <= expected.frontend_passed <= manifest.frontend_total
            or len(expected.backend_failed_test_ids)
            != manifest.backend_total - expected.backend_passed
            or len(expected.frontend_failed_test_ids)
            != manifest.frontend_total - expected.frontend_passed
            for expected in manifest.mutations.values()
        )
    ):
        raise GraderInfrastructureError("invalid hidden-check manifest counters")
    if manifest != _FIXED_SUPPORT_QUEUE_MANIFEST:
        raise GraderInfrastructureError(
            "hidden-check manifest does not match the fixed benchmark contract"
        )
    return manifest


def regression_commands() -> dict[str, list[str]]:
    return {
        "backend_tests": ["uv", "run", "--frozen", "--no-sync", "pytest", "-q"],
        "ruff": ["uv", "run", "--frozen", "--no-sync", "ruff", "check", "."],
        "mypy": ["uv", "run", "--frozen", "--no-sync", "mypy", "support_queue"],
        "frontend_install": ["npm", "ci"],
        "frontend_tests": ["npm", "test", "--", "--run"],
        "frontend_build": ["npm", "run", "build"],
    }


def parse_junit_report(path: Path) -> dict[str, object]:
    """Turn pytest's stable JUnit summary into comparable scalar metrics."""
    root = ElementTree.parse(path).getroot()
    if root.tag not in {"testsuite", "testsuites"}:
        raise RuntimeError(f"unexpected JUnit root {root.tag!r}")
    suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
    total = sum(int(suite.attrib.get("tests", 0)) for suite in suites)
    failures = sum(int(suite.attrib.get("failures", 0)) for suite in suites)
    errors = sum(int(suite.attrib.get("errors", 0)) for suite in suites)
    skipped = sum(int(suite.attrib.get("skipped", 0)) for suite in suites)
    if min(total, failures, errors, skipped) < 0 or failures + errors + skipped > total:
        raise GraderInfrastructureError("invalid JUnit test accounting")
    failure_details = sorted(
        (
            {
                "test_id": testcase.attrib.get("name", ""),
                "message": _failure_summary(failure.attrib.get("message") or failure.text or ""),
            }
            for testcase in root.iter("testcase")
            if (failure := testcase.find("failure")) is not None
        ),
        key=lambda item: item["test_id"],
    )
    return {
        "hidden_checks_total": total,
        "hidden_checks_passed": max(0, total - failures - errors - skipped),
        "hidden_checks_failed": failures,
        "hidden_checks_errors": errors,
        "hidden_checks_skipped": skipped,
        "hidden_failed_test_ids": sorted(
            testcase.attrib.get("name", "")
            for testcase in root.iter("testcase")
            if testcase.find("failure") is not None
        ),
        "hidden_failure_details": failure_details,
    }


def parse_vitest_report(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GraderInfrastructureError(f"invalid Vitest result: {exc}") from exc
    if not isinstance(payload, dict):
        raise GraderInfrastructureError("invalid Vitest result: root must be an object")
    try:
        total = int(payload["numTotalTests"])
        passed = int(payload["numPassedTests"])
        failed = int(payload["numFailedTests"])
        skipped = int(payload.get("numPendingTests", 0)) + int(payload.get("numTodoTests", 0))
    except (KeyError, TypeError, ValueError) as exc:
        raise GraderInfrastructureError(f"invalid Vitest result counters: {exc}") from exc
    if min(total, passed, failed, skipped) < 0 or passed + failed + skipped != total:
        raise GraderInfrastructureError("invalid Vitest test accounting")
    failure_details: list[dict[str, str]] = []
    raw_results = payload.get("testResults", [])
    if not isinstance(raw_results, list):
        raise GraderInfrastructureError("invalid Vitest test results")
    for result in raw_results:
        if not isinstance(result, dict):
            raise GraderInfrastructureError("invalid Vitest test result")
        assertions = result.get("assertionResults", [])
        if not isinstance(assertions, list):
            raise GraderInfrastructureError("invalid Vitest assertion results")
        for assertion in assertions:
            if not isinstance(assertion, dict):
                raise GraderInfrastructureError("invalid Vitest assertion result")
            if assertion.get("status") == "failed":
                name = assertion.get("fullName")
                if not isinstance(name, str) or not name:
                    raise GraderInfrastructureError("failed Vitest assertion has no name")
                messages = assertion.get("failureMessages", [])
                raw_message = messages[0] if isinstance(messages, list) and messages else ""
                failure_details.append(
                    {
                        "test_id": name,
                        "message": _failure_summary(
                            raw_message if isinstance(raw_message, str) else ""
                        ),
                    }
                )
    if len(failure_details) != failed:
        raise GraderInfrastructureError("Vitest failed test identities do not match counters")
    failure_details.sort(key=lambda item: item["test_id"])
    return {
        "hidden_checks_total": total,
        "hidden_checks_passed": passed,
        "hidden_checks_failed": failed,
        "hidden_checks_errors": max(0, total - passed - failed - skipped),
        "hidden_checks_skipped": skipped,
        "hidden_failed_test_ids": [item["test_id"] for item in failure_details],
        "hidden_failure_details": failure_details,
    }


def _failure_summary(raw: str) -> str:
    """Keep the useful assertion without persisting terminal dumps or tracebacks."""
    lines: list[str] = []
    for source in _ANSI_ESCAPE.sub("", raw).splitlines():
        line = " ".join(source.split())
        if not line:
            continue
        if line == "Ignored nodes: comments, script, style" or line.startswith("at "):
            break
        lines.append(line)
        if len(" ".join(lines)) >= _FAILURE_SUMMARY_LIMIT:
            break
    summary = " ".join(lines) or "unknown failure"
    return summary[:_FAILURE_SUMMARY_LIMIT].rstrip()


def validate_control_result(
    metrics: dict[str, object], manifest: HiddenManifest, *, control: str
) -> None:
    if control not in {"reference", "seed"} and control not in manifest.mutations:
        raise ValueError(f"unknown grader control {control!r}")
    for component, expected_total in (
        ("backend", manifest.backend_total),
        ("frontend", manifest.frontend_total),
    ):
        total = _metric_int(metrics, f"{component}_hidden_checks_total")
        if total != expected_total:
            raise GraderInfrastructureError(
                f"{control}: expected {expected_total} {component} checks, got {total}"
            )
        errors = _metric_int(metrics, f"{component}_hidden_checks_errors")
        skipped = _metric_int(metrics, f"{component}_hidden_checks_skipped")
        if errors or skipped:
            raise GraderInfrastructureError(
                f"{control}: {component} checks had {errors} errors and {skipped} skipped"
            )
        passed = _metric_int(metrics, f"{component}_hidden_checks_passed")
        if control == "reference":
            expected_passed = expected_total
        elif control == "seed":
            expected_passed = getattr(manifest, f"seed_{component}_passed")
        else:
            expected_passed = getattr(manifest.mutations[control], f"{component}_passed")
        if passed != expected_passed:
            raise GraderInfrastructureError(
                f"{control}: expected {expected_passed}/{expected_total} {component} passes, "
                f"got {passed}/{total}"
            )
        if control in manifest.mutations:
            expected_failures = getattr(manifest.mutations[control], f"{component}_failed_test_ids")
            actual_failures = _metric_test_ids(metrics, f"{component}_hidden_failed_test_ids")
            if actual_failures != expected_failures:
                raise GraderInfrastructureError(
                    f"{control}: expected {component} failures {list(expected_failures)!r}, "
                    f"got {list(actual_failures)!r}"
                )


class SupportQueueGrader:
    """Clone merged main and run private backend and frontend checks."""

    def __init__(self, commands: Commands) -> None:
        self._commands = commands

    async def grade(
        self,
        *,
        repository_slug: str,
        destination: Path,
        github_token: str,
        backend_hidden_test: Path,
        frontend_hidden_test: Path,
        manifest: HiddenManifest,
        checks: dict[str, list[str]] | None = None,
    ) -> dict[str, object]:
        checkout = destination / "final"
        env = {"GH_TOKEN": github_token}
        await self._commands.run(
            ["gh", "repo", "clone", repository_slug, checkout.name],
            cwd=destination,
            env=env,
        )
        metrics = await self._run_hidden_checks(
            checkout=checkout,
            results_root=destination,
            backend_hidden_test=backend_hidden_test,
            frontend_hidden_test=frontend_hidden_test,
            manifest=manifest,
        )
        results: dict[str, str] = {}
        failure_details: dict[str, str] = {}
        for name, argv in (checks or regression_commands()).items():
            if name == "frontend_install":
                results[name] = "passed"
                continue
            cwd = checkout / "frontend" if name.startswith("frontend_") else checkout
            try:
                await self._commands.run(argv, cwd=cwd)
                results[name] = "passed"
            except CommandError as exc:
                results[name] = "failed"
                failure_details[name] = _failure_summary(str(exc))
        metrics.update(
            regression_checks_total=len(results),
            regression_checks_passed=sum(value == "passed" for value in results.values()),
            regression_checks_failed=sum(value == "failed" for value in results.values()),
            regression_results=results,
            regression_failure_details=failure_details,
        )
        return metrics

    async def validate_controls(
        self,
        *,
        seed_root: Path,
        reference_root: Path,
        mutation_roots: dict[str, Path],
        results_root: Path,
        backend_hidden_test: Path,
        frontend_hidden_test: Path,
        manifest: HiddenManifest,
    ) -> dict[str, object]:
        if set(mutation_roots) != set(manifest.mutations):
            raise GraderInfrastructureError(
                "grader mutation controls do not match the fixed manifest"
            )
        reference = await self._run_hidden_checks(
            checkout=reference_root,
            results_root=results_root / "reference",
            backend_hidden_test=backend_hidden_test,
            frontend_hidden_test=frontend_hidden_test,
            manifest=manifest,
        )
        validate_control_result(reference, manifest, control="reference")
        seed = await self._run_hidden_checks(
            checkout=seed_root,
            results_root=results_root / "seed",
            backend_hidden_test=backend_hidden_test,
            frontend_hidden_test=frontend_hidden_test,
            manifest=manifest,
        )
        validate_control_result(seed, manifest, control="seed")
        controls: dict[str, object] = {"reference": reference, "seed": seed}
        for name, root in mutation_roots.items():
            mutation = await self._run_hidden_checks(
                checkout=root,
                results_root=results_root / name,
                backend_hidden_test=backend_hidden_test,
                frontend_hidden_test=frontend_hidden_test,
                manifest=manifest,
            )
            validate_control_result(mutation, manifest, control=name)
            controls[name] = mutation
        return controls

    async def _run_hidden_checks(
        self,
        *,
        checkout: Path,
        results_root: Path,
        backend_hidden_test: Path,
        frontend_hidden_test: Path,
        manifest: HiddenManifest,
    ) -> dict[str, object]:
        await asyncio.to_thread(results_root.mkdir, parents=True, exist_ok=True)
        try:
            await self._commands.run(["uv", "sync", "--locked"], cwd=checkout)
        except CommandError as exc:
            raise GraderInfrastructureError(f"backend dependency install failed: {exc}") from exc
        backend_report = results_root / "backend-hidden-junit.xml"
        injected_backend = checkout / "bench_hidden_test.py"
        if await asyncio.to_thread(injected_backend.exists):
            raise GraderInfrastructureError(
                f"reserved hidden-check path already exists: {injected_backend.name}"
            )
        await asyncio.to_thread(shutil.copyfile, backend_hidden_test, injected_backend)
        try:
            try:
                await self._commands.run(
                    [
                        "uv",
                        "run",
                        "--frozen",
                        "--no-sync",
                        "pytest",
                        str(injected_backend),
                        "-q",
                        f"--junitxml={backend_report}",
                    ],
                    cwd=checkout,
                    env={"SUPPORT_QUEUE_DB_PATH": str(results_root / "hidden-support.sqlite")},
                )
            except CommandError as exc:
                if not backend_report.exists():
                    raise GraderInfrastructureError(
                        f"backend hidden checks did not produce JUnit: {exc}"
                    ) from exc
                if not _is_test_failure_exit(exc):
                    raise GraderInfrastructureError(
                        f"backend hidden-check process failed: {exc}"
                    ) from exc
        finally:
            await asyncio.to_thread(injected_backend.unlink, missing_ok=True)
        try:
            backend = parse_junit_report(backend_report)
        except (OSError, ElementTree.ParseError, RuntimeError, ValueError) as exc:
            raise GraderInfrastructureError(f"invalid backend JUnit: {exc}") from exc

        frontend_root = checkout / "frontend"
        try:
            await self._commands.run(["npm", "ci"], cwd=frontend_root)
        except CommandError as exc:
            raise GraderInfrastructureError(f"frontend dependency install failed: {exc}") from exc
        frontend_report = results_root / "frontend-hidden.json"
        injected_frontend = frontend_root / "src/App.bench.test.tsx"
        if await asyncio.to_thread(injected_frontend.exists):
            raise GraderInfrastructureError(
                f"reserved hidden-check path already exists: {injected_frontend.name}"
            )
        await asyncio.to_thread(shutil.copyfile, frontend_hidden_test, injected_frontend)
        try:
            try:
                await self._commands.run(
                    [
                        "npm",
                        "test",
                        "--",
                        "--run",
                        "src/App.bench.test.tsx",
                        "--reporter=json",
                        f"--outputFile={frontend_report}",
                    ],
                    cwd=frontend_root,
                )
            except CommandError as exc:
                if not frontend_report.exists():
                    raise GraderInfrastructureError(
                        f"frontend hidden checks did not produce JSON: {exc}"
                    ) from exc
                if not _is_test_failure_exit(exc):
                    raise GraderInfrastructureError(
                        f"frontend hidden-check process failed: {exc}"
                    ) from exc
        finally:
            await asyncio.to_thread(injected_frontend.unlink, missing_ok=True)
        frontend = parse_vitest_report(frontend_report)

        metrics: dict[str, object] = {
            **{f"backend_{key}": value for key, value in backend.items()},
            **{f"frontend_{key}": value for key, value in frontend.items()},
        }
        _validate_hidden_shape(metrics, manifest)
        metrics.update(
            hidden_checks_total=_metric_int(backend, "hidden_checks_total")
            + _metric_int(frontend, "hidden_checks_total"),
            hidden_checks_passed=_metric_int(backend, "hidden_checks_passed")
            + _metric_int(frontend, "hidden_checks_passed"),
            hidden_checks_failed=_metric_int(backend, "hidden_checks_failed")
            + _metric_int(frontend, "hidden_checks_failed"),
            hidden_checks_errors=_metric_int(backend, "hidden_checks_errors")
            + _metric_int(frontend, "hidden_checks_errors"),
            hidden_checks_skipped=_metric_int(backend, "hidden_checks_skipped")
            + _metric_int(frontend, "hidden_checks_skipped"),
        )
        return metrics


def _validate_hidden_shape(metrics: dict[str, object], manifest: HiddenManifest) -> None:
    for component, expected_total in (
        ("backend", manifest.backend_total),
        ("frontend", manifest.frontend_total),
    ):
        actual_total = _metric_int(metrics, f"{component}_hidden_checks_total")
        if actual_total != expected_total:
            raise GraderInfrastructureError(
                f"expected {expected_total} {component} checks, got {actual_total}"
            )
        errors = _metric_int(metrics, f"{component}_hidden_checks_errors")
        skipped = _metric_int(metrics, f"{component}_hidden_checks_skipped")
        if errors or skipped:
            raise GraderInfrastructureError(
                f"{component} hidden checks had {errors} errors and {skipped} skipped"
            )


def _metric_int(metrics: dict[str, object], key: str) -> int:
    value = metrics.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise GraderInfrastructureError(f"hidden-check metric {key!r} is missing or invalid")
    return value


def _test_ids(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise TypeError("failed test ids must be a list of non-empty strings")
    if len(set(value)) != len(value):
        raise TypeError("failed test ids must be unique")
    return tuple(sorted(value))


def _metric_test_ids(metrics: dict[str, object], key: str) -> tuple[str, ...]:
    value = metrics.get(key)
    if not isinstance(value, list):
        raise GraderInfrastructureError(f"hidden-check metric {key!r} is missing or invalid")
    try:
        return _test_ids(value)
    except TypeError as exc:
        raise GraderInfrastructureError(f"hidden-check metric {key!r} is invalid") from exc


def _is_test_failure_exit(error: CommandError) -> bool:
    return re.search(r"\bexited 1:", str(error)) is not None
