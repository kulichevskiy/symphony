from __future__ import annotations

import json
import subprocess
from pathlib import Path

from click.testing import CliRunner

from symphony.bench.cli import bench
from symphony.bench.maintainability import (
    AgentProbeResult,
    MutationOutcome,
    MutationTestResult,
    ProbeOutcome,
    ProbeVerification,
    WaitingOnCustomerVerifier,
    analyze_static,
    discover_mutants,
    parse_claude_probe_result,
    render_maintainability_markdown,
    run_maintenance_probe,
    run_mutation_pack,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_static_report_covers_python_typescript_duplication_and_cycles(tmp_path: Path) -> None:
    repeated = "\n".join(f"    value_{index} = {index}" for index in range(6))
    _write(
        tmp_path / "support_queue" / "a.py",
        f"""from . import b

def decide(first: bool, second: bool) -> int:
    if first and second:
        return 1
{repeated}
    return 0
""",
    )
    _write(
        tmp_path / "support_queue" / "b.py",
        f"""from . import a

def duplicate() -> int:
{repeated}
    return 0
""",
    )
    _write(
        tmp_path / "frontend" / "src" / "App.tsx",
        """export function App({ready}: {ready: boolean}) {
  if (ready) {
    return <main>Ready</main>
  }
  return <main>Waiting</main>
}
""",
    )
    _write(
        tmp_path / "frontend" / "src" / "App.test.tsx",
        "throw new Error('tests are not production source')\n",
    )

    report = analyze_static(tmp_path)

    assert report.source_files == 3
    assert report.function_count == 3
    assert report.complexity_max == 3
    assert report.max_function_lines >= 6
    assert report.duplicated_blocks >= 1
    assert report.import_cycles == (("support_queue.a", "support_queue.b"),)
    assert all("test" not in item.path for item in report.files)


def test_static_report_accepts_a_checkout_reached_through_a_symlink(tmp_path: Path) -> None:
    real = tmp_path / "real"
    _write(real / "frontend" / "src" / "api.ts", "export const health = true\n")
    _write(
        real / "frontend" / "src" / "App.tsx",
        """import {health} from './api'
export function App() { return health ? <main /> : null }
""",
    )
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)

    report = analyze_static(linked)

    assert report.source_files == 2


def test_static_report_counts_duplication_within_one_source_file(tmp_path: Path) -> None:
    block = "\n".join(f"    repeated_{index} = {index}" for index in range(6))
    _write(
        tmp_path / "support_queue" / "main.py",
        f"""def first() -> None:
{block}

def second() -> None:
{block}
""",
    )

    assert analyze_static(tmp_path).duplicated_blocks >= 1


def test_typescript_static_report_covers_methods_arrows_and_index_imports(tmp_path: Path) -> None:
    _write(
        tmp_path / "frontend" / "src" / "App.ts",
        """import './feature'
export const active = value => { return value ? true : false }
""",
    )
    _write(
        tmp_path / "frontend" / "src" / "feature" / "index.ts",
        """import '../App'
export class Queue { resolve(value: boolean) { return value } }
""",
    )

    report = analyze_static(tmp_path)

    assert report.function_count == 2
    assert report.import_cycles == (
        ("frontend.src.App", "frontend.src.feature.index"),
    )


def test_mutants_are_deterministic_and_restricted_to_production_source(tmp_path: Path) -> None:
    _write(
        tmp_path / "support_queue" / "main.py",
        """def allowed(role: str, active: bool) -> bool:
    return role == "admin" and active
""",
    )
    _write(tmp_path / "tests" / "test_main.py", "assert True\n")
    _write(
        tmp_path / "frontend" / "src" / "App.tsx",
        "export const visible = (ready: boolean) => ready ? true : false\n",
    )

    first = discover_mutants(tmp_path, limit=20)
    second = discover_mutants(tmp_path, limit=20)

    assert first == second
    assert {mutant.language for mutant in first} == {"python", "typescript"}
    assert {mutant.operator for mutant in first} >= {
        "boolean_connector",
        "comparison",
        "boolean_literal",
    }
    assert all("test" not in mutant.path for mutant in first)
    assert len({mutant.mutant_id for mutant in first}) == len(first)


def test_typescript_mutants_ignore_strings_and_comments(tmp_path: Path) -> None:
    _write(
        tmp_path / "frontend" / "src" / "App.tsx",
        """// false && true !== false
const label = "true || false === true"
export const visible = (ready: boolean) => ready === true
""",
    )

    mutants = discover_mutants(tmp_path, limit=20)

    assert len(mutants) == 2
    assert {mutant.operator for mutant in mutants} == {"comparison", "boolean_literal"}
    assert all(mutant.line == 3 for mutant in mutants)


def test_markdown_keeps_functional_grade_separate() -> None:
    markdown = render_maintainability_markdown(
        repository="owner/repo",
        static=analyze_static(Path(__file__).parent / "fixtures" / "missing"),
        mutations=(
            MutationOutcome(mutant_id="python:comparison:1", status="killed", duration_seconds=1),
            MutationOutcome(
                mutant_id="typescript:boolean_literal:1",
                status="survived",
                duration_seconds=2,
            ),
        ),
        probe=None,
        errors=("typescript baseline failed",),
    )

    assert "# Maintainability — `owner/repo`" in markdown
    assert "Mutation testing — 1/2 killed" in markdown
    assert "Functional grade is intentionally not included" in markdown
    assert "typescript baseline failed" in markdown
    assert "/29" not in markdown


def test_mutation_pack_records_killed_and_survived_and_restores_source(tmp_path: Path) -> None:
    source_path = tmp_path / "support_queue" / "main.py"
    original = """def allowed(role: str, active: bool) -> bool:
    return role == "admin" and active
"""
    _write(source_path, original)

    class FakeRunner:
        def run(self, language: str, root: Path, *, timeout_seconds: float) -> MutationTestResult:
            assert language == "python"
            assert timeout_seconds == 10
            source = (root / "support_queue" / "main.py").read_text(encoding="utf-8")
            if source == original:
                return MutationTestResult(passed=True, duration_seconds=0.1)
            return MutationTestResult(
                passed="!=" not in source,
                duration_seconds=0.2,
                detail="assertion failed" if "!=" in source else "",
            )

    outcomes = run_mutation_pack(
        tmp_path,
        mutants=discover_mutants(tmp_path, limit=2),
        runner=FakeRunner(),
        timeout_seconds=10,
    )

    assert {outcome.status for outcome in outcomes} == {"killed", "survived"}
    assert source_path.read_text(encoding="utf-8") == original


def test_mutation_pack_rejects_a_failing_baseline(tmp_path: Path) -> None:
    _write(tmp_path / "support_queue" / "main.py", "def health() -> bool:\n    return True\n")

    class FailingRunner:
        def run(self, language: str, root: Path, *, timeout_seconds: float) -> MutationTestResult:
            return MutationTestResult(passed=False, duration_seconds=0.1, detail="baseline failed")

    try:
        run_mutation_pack(
            tmp_path,
            mutants=discover_mutants(tmp_path),
            runner=FailingRunner(),
        )
    except RuntimeError as exc:
        assert "baseline" in str(exc)
    else:
        raise AssertionError("expected a failing baseline to abort mutation testing")


def test_mutation_pack_does_not_count_a_flaky_failure_as_killed(tmp_path: Path) -> None:
    _write(
        tmp_path / "support_queue" / "main.py",
        'def allowed(role: str) -> bool:\n    return role == "admin"\n',
    )
    mutant = next(item for item in discover_mutants(tmp_path) if item.operator == "comparison")

    class FlakyRunner:
        calls = 0

        def run(self, language: str, root: Path, *, timeout_seconds: float) -> MutationTestResult:
            self.calls += 1
            if self.calls <= 2:
                return MutationTestResult(passed=True, duration_seconds=0.1)
            return MutationTestResult(passed=self.calls == 4, duration_seconds=0.1)

    outcomes = run_mutation_pack(tmp_path, mutants=(mutant,), runner=FlakyRunner())

    assert outcomes[0].status == "error"
    assert "non-deterministic" in outcomes[0].detail


def test_maintenance_probe_is_repeated_in_disposable_copies(tmp_path: Path) -> None:
    _write(tmp_path / "support_queue" / "main.py", "STATUS = 'open'\n")
    _write(tmp_path / "frontend" / "src" / "App.tsx", "export const status = 'open'\n")

    class FakeAgent:
        def run(self, root: Path, *, timeout_seconds: float) -> AgentProbeResult:
            assert timeout_seconds == 30
            backend = root / "support_queue" / "main.py"
            backend.write_text(
                backend.read_text(encoding="utf-8") + "WAITING = 'waiting_on_customer'\n",
                encoding="utf-8",
            )
            frontend = root / "frontend" / "src" / "App.tsx"
            frontend.write_text(
                frontend.read_text(encoding="utf-8")
                + "export const waiting = 'waiting_on_customer'\n",
                encoding="utf-8",
            )
            return AgentProbeResult(
                succeeded=True,
                wall_seconds=2,
                raw_tokens=100,
                cost_usd=1.5,
            )

    class FakeVerifier:
        def verify(self, root: Path, *, timeout_seconds: float) -> ProbeVerification:
            assert "waiting_on_customer" in (root / "support_queue" / "main.py").read_text(
                encoding="utf-8"
            )
            return ProbeVerification(passed=True, detail="all checks passed")

    outcome = run_maintenance_probe(
        tmp_path,
        agent=FakeAgent(),
        verifier=FakeVerifier(),
        timeout_seconds=30,
    )

    assert outcome.status == "passed"
    assert outcome.successful_repetitions == 1
    assert outcome.raw_tokens == 100
    assert outcome.cost_usd == 1.5
    assert outcome.changed_files == 2
    assert outcome.changed_lines == 2
    assert "waiting_on_customer" not in (tmp_path / "support_queue" / "main.py").read_text(
        encoding="utf-8"
    )


def test_waiting_status_verifier_requires_product_and_test_coverage(tmp_path: Path) -> None:
    _write(
        tmp_path / "support_queue" / "main.py",
        """waiting_on_customer = 'waiting_on_customer'
TRANSITIONS = {
    ('in_progress', 'waiting_on_customer'),
    ('waiting_on_customer', 'in_progress'),
    ('waiting_on_customer', 'resolved'),
}
""",
    )
    _write(tmp_path / "tests" / "test_waiting.py", "waiting_on_customer\n")
    _write(
        tmp_path / "frontend" / "src" / "App.tsx",
        "const status = 'waiting_on_customer'; const label = 'Awaiting customer reply'\n",
    )
    _write(tmp_path / "frontend" / "src" / "App.test.tsx", "waiting_on_customer\n")

    class PassingCommands:
        calls: list[tuple[str, ...]] = []

        def run(
            self,
            argv: list[str],
            *,
            cwd: Path,
            timeout_seconds: float,
        ) -> MutationTestResult:
            self.calls.append(tuple(argv))
            return MutationTestResult(passed=True, duration_seconds=0.1)

    commands = PassingCommands()
    verification = WaitingOnCustomerVerifier(commands=commands).verify(tmp_path, timeout_seconds=30)

    assert verification.passed
    assert len(commands.calls) == 9
    assert commands.calls[0] == ("uv", "sync", "--locked")
    assert commands.calls[2][-2:] == ("pytest", "tests/test_maintainability_probe.py")
    assert commands.calls[3][-1] == "maintainability-probe.test.tsx"
    assert not (tmp_path / "tests" / "test_maintainability_probe.py").exists()
    assert not (tmp_path / "frontend" / "src" / "maintainability-probe.test.tsx").exists()

    (tmp_path / "frontend" / "src" / "App.test.tsx").unlink()
    missing = WaitingOnCustomerVerifier(commands=commands).verify(tmp_path, timeout_seconds=30)
    assert not missing.passed
    assert "frontend tests" in missing.detail


def test_waiting_status_verifier_rejects_literal_only_implementation(tmp_path: Path) -> None:
    for relative in (
        "support_queue/main.py",
        "tests/test_waiting.py",
        "frontend/src/App.tsx",
        "frontend/src/App.test.tsx",
    ):
        _write(tmp_path / relative, "waiting_on_customer Waiting on customer\n")

    class HiddenFailure:
        calls = 0

        def run(
            self,
            argv: list[str],
            *,
            cwd: Path,
            timeout_seconds: float,
        ) -> MutationTestResult:
            self.calls += 1
            return MutationTestResult(
                self.calls <= 2,
                0.1,
                "invalid transition accepted",
            )

    verification = WaitingOnCustomerVerifier(commands=HiddenFailure()).verify(
        tmp_path,
        timeout_seconds=30,
    )

    assert not verification.passed
    assert "hidden workflow probe" in verification.detail


def test_claude_probe_result_uses_provider_reported_usage() -> None:
    stdout = json.dumps(
        {
            "type": "result",
            "total_cost_usd": 0.5,
            "modelUsage": {
                "claude-opus-5": {
                    "inputTokens": 1,
                    "outputTokens": 2,
                    "cacheCreationInputTokens": 3,
                    "cacheReadInputTokens": 4,
                }
            },
        }
    )

    result = parse_claude_probe_result(stdout, returncode=0, wall_seconds=7)

    assert result.succeeded
    assert result.wall_seconds == 7
    assert result.raw_tokens == 10
    assert result.cost_usd == 0.5


def test_cli_writes_machine_and_human_reports(tmp_path: Path) -> None:
    _write(tmp_path / "support_queue" / "main.py", "def health() -> bool:\n    return True\n")
    output = tmp_path / "out"

    result = CliRunner().invoke(
        bench,
        [
            "maintainability",
            "--repository",
            str(tmp_path),
            "--output",
            str(output),
            "--static-only",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads((output / "MAINTAINABILITY.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["repository"] == str(tmp_path)
    assert payload["static"]["source_files"] == 1


def test_cli_static_refresh_preserves_existing_mutation_and_probe_results(tmp_path: Path) -> None:
    _write(tmp_path / "support_queue" / "main.py", "def health() -> bool:\n    return True\n")
    output = tmp_path / "out"
    from symphony.bench.maintainability import write_reports

    write_reports(
        output=output,
        repository=str(tmp_path),
        static=analyze_static(tmp_path),
        mutations=(MutationOutcome("mutant", "killed", 1),),
        probe=ProbeOutcome("passed", 1, 1, 2, 3, 4, 5, 6),
    )

    result = CliRunner().invoke(
        bench,
        [
            "maintainability",
            "--repository",
            str(tmp_path),
            "--output",
            str(output),
            "--static-only",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads((output / "MAINTAINABILITY.json").read_text(encoding="utf-8"))
    assert payload["mutations"][0]["status"] == "killed"
    assert payload["probe"]["status"] == "passed"
    assert (output / "MAINTAINABILITY.md").exists()


def test_cli_imports_remote_probe_receipt_without_erasing_mutations(tmp_path: Path) -> None:
    _write(tmp_path / "support_queue" / "main.py", "def health() -> bool:\n    return True\n")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Probe",
            "-c",
            "user.email=probe@example.com",
            "commit",
            "-qm",
            "baseline",
        ],
        cwd=tmp_path,
        check=True,
    )
    baseline_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    output = tmp_path / "out"
    from symphony.bench.maintainability import write_reports

    write_reports(
        output=output,
        repository=str(tmp_path),
        static=analyze_static(tmp_path),
        mutations=(MutationOutcome("mutant", "killed", 1),),
    )
    receipt = tmp_path / "receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "repository": str(tmp_path),
                "baseline_sha": baseline_sha,
                "profile": "Claude Opus 5 medium",
                "probe": "waiting_on_customer",
                "probe_outcome": {
                    "status": "passed",
                    "repetitions": 1,
                    "successful_repetitions": 1,
                    "wall_seconds": 2,
                    "raw_tokens": 3,
                    "cost_usd": 4,
                    "changed_files": 5,
                    "changed_lines": 6,
                    "detail": "",
                }
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        bench,
        [
            "maintainability",
            "--repository",
            str(tmp_path),
            "--output",
            str(output),
            "--probe-receipt",
            str(receipt),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads((output / "MAINTAINABILITY.json").read_text(encoding="utf-8"))
    assert payload["probe"]["raw_tokens"] == 3
    assert payload["mutations"][0]["status"] == "killed"
