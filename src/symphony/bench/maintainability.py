from __future__ import annotations

import ast
import difflib
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median
from typing import Literal, Protocol

from ..agent.process import parse_event_line

_SOURCE_SUFFIXES = frozenset({".py", ".ts", ".tsx"})
_TEST_MARKERS = (".test.", ".spec.", "test_", "_test.")
_DUPLICATE_BLOCK_LINES = 6
MAINTENANCE_PROBE_PROMPT = """Implement this exact maintenance change completely in this
Support Queue repository.

Add a new ticket status named `waiting_on_customer` across the backend, persistence contract,
API filtering, workflow rules, frontend controls, accessible status presentation, and automated
tests. The allowed new transitions are `in_progress -> waiting_on_customer`,
`waiting_on_customer -> in_progress`, and `waiting_on_customer -> resolved`. Preserve every
existing transition and behavior. Existing databases must remain readable. Add focused backend
and frontend regression tests containing the exact status name. Run all documented checks.

Work directly in the checkout. Do not commit, push, open a pull request, or change dependencies
unless the implementation genuinely requires it. Finish only when the project tests and build pass.
"""


@dataclass(frozen=True)
class FileMetric:
    path: str
    language: str
    lines: int
    functions: int
    complexity_max: int
    function_lines_max: int


@dataclass(frozen=True)
class StaticMaintainability:
    source_files: int
    source_lines: int
    function_count: int
    complexity_median: float
    complexity_p95: int
    complexity_max: int
    max_function_lines: int
    large_functions: int
    max_file_lines: int
    large_files: int
    duplicated_blocks: int
    import_cycles: tuple[tuple[str, ...], ...]
    files: tuple[FileMetric, ...]


@dataclass(frozen=True)
class Mutant:
    mutant_id: str
    language: Literal["python", "typescript"]
    operator: str
    path: str
    line: int
    offset_start: int
    offset_end: int
    original: str
    replacement: str


@dataclass(frozen=True)
class MutationOutcome:
    mutant_id: str
    status: Literal["killed", "survived", "error", "timeout"]
    duration_seconds: float
    detail: str = ""


@dataclass(frozen=True)
class MutationTestResult:
    passed: bool
    duration_seconds: float
    detail: str = ""
    timed_out: bool = False


class MutationRunner(Protocol):
    def run(
        self,
        language: str,
        root: Path,
        *,
        timeout_seconds: float,
    ) -> MutationTestResult: ...


def _run_subprocess(
    argv: list[str],
    *,
    cwd: Path,
    timeout_seconds: float,
    detail_limit: int,
) -> MutationTestResult:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return MutationTestResult(
            passed=False,
            duration_seconds=time.monotonic() - started,
            detail=str(exc),
            timed_out=True,
        )
    return MutationTestResult(
        passed=completed.returncode == 0,
        duration_seconds=time.monotonic() - started,
        detail=(completed.stderr or completed.stdout)[-detail_limit:].strip(),
    )


class SubprocessMutationRunner:
    def run(
        self,
        language: str,
        root: Path,
        *,
        timeout_seconds: float,
    ) -> MutationTestResult:
        if language == "python":
            argv = ["uv", "run", "--frozen", "--no-sync", "pytest", "-q"]
            cwd = root
        elif language == "typescript":
            argv = ["npm", "test", "--", "--run"]
            cwd = root / "frontend"
        else:
            raise ValueError(f"unsupported mutation language {language!r}")
        return _run_subprocess(
            argv,
            cwd=cwd,
            timeout_seconds=timeout_seconds,
            detail_limit=500,
        )


@dataclass(frozen=True)
class ProbeOutcome:
    status: Literal["passed", "failed"]
    repetitions: int
    successful_repetitions: int
    wall_seconds: float
    raw_tokens: int | None
    cost_usd: float | None
    changed_files: int
    changed_lines: int
    detail: str = ""


@dataclass(frozen=True)
class AgentProbeResult:
    succeeded: bool
    wall_seconds: float
    raw_tokens: int | None
    cost_usd: float | None
    detail: str = ""


@dataclass(frozen=True)
class ProbeVerification:
    passed: bool
    detail: str


class ProbeAgent(Protocol):
    def run(self, root: Path, *, timeout_seconds: float) -> AgentProbeResult: ...


class ProbeVerifier(Protocol):
    def verify(self, root: Path, *, timeout_seconds: float) -> ProbeVerification: ...


class ProbeCommandRunner(Protocol):
    def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> MutationTestResult: ...


class SubprocessProbeCommands:
    def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> MutationTestResult:
        return _run_subprocess(
            argv,
            cwd=cwd,
            timeout_seconds=timeout_seconds,
            detail_limit=1000,
        )


class WaitingOnCustomerVerifier:
    def __init__(self, commands: ProbeCommandRunner | None = None) -> None:
        self._commands = commands or SubprocessProbeCommands()

    @staticmethod
    def _contains_status(paths: list[Path]) -> bool:
        return any(
            "waiting_on_customer" in path.read_text(encoding="utf-8")
            for path in paths
            if path.is_file()
        )

    def verify(self, root: Path, *, timeout_seconds: float) -> ProbeVerification:
        coverage = {
            "backend product": list((root / "support_queue").rglob("*.py")),
            "backend tests": list((root / "tests").rglob("test*.py")),
            "frontend product": [
                path
                for path in (root / "frontend" / "src").rglob("*.ts*")
                if not any(marker in path.name for marker in (".test.", ".spec."))
            ],
            "frontend tests": [
                path
                for path in (root / "frontend" / "src").rglob("*.ts*")
                if any(marker in path.name for marker in (".test.", ".spec."))
            ],
        }
        missing = [name for name, paths in coverage.items() if not self._contains_status(paths)]
        if missing:
            return ProbeVerification(
                passed=False,
                detail=f"waiting_on_customer missing from {', '.join(missing)}",
            )

        preparations = (
            (["uv", "sync", "--locked"], root),
            (["npm", "ci"], root / "frontend"),
        )
        for argv, cwd in preparations:
            prepared = self._commands.run(
                argv,
                cwd=cwd,
                timeout_seconds=timeout_seconds,
            )
            if not prepared.passed:
                return ProbeVerification(
                    passed=False,
                    detail=f"{' '.join(argv)} failed: {prepared.detail}",
                )

        hidden_test = root / "tests" / "test_maintainability_probe.py"
        hidden_test.write_text(BACKEND_PROBE_TEST, encoding="utf-8")
        try:
            hidden = self._commands.run(
                [
                    "uv",
                    "run",
                    "--frozen",
                    "--no-sync",
                    "pytest",
                    hidden_test.relative_to(root).as_posix(),
                ],
                cwd=root,
                timeout_seconds=timeout_seconds,
            )
        finally:
            hidden_test.unlink(missing_ok=True)
        if not hidden.passed:
            return ProbeVerification(
                passed=False,
                detail=f"hidden workflow probe failed: {hidden.detail}",
            )

        frontend_hidden_test = root / "frontend" / "src" / "maintainability-probe.test.tsx"
        frontend_hidden_test.write_text(FRONTEND_PROBE_TEST, encoding="utf-8")
        try:
            frontend_hidden = self._commands.run(
                ["npm", "test", "--", "--run", frontend_hidden_test.name],
                cwd=root / "frontend",
                timeout_seconds=timeout_seconds,
            )
        finally:
            frontend_hidden_test.unlink(missing_ok=True)
        if not frontend_hidden.passed:
            return ProbeVerification(
                passed=False,
                detail=f"hidden accessible-frontend probe failed: {frontend_hidden.detail}",
            )

        checks = (
            (["uv", "run", "--frozen", "--no-sync", "pytest", "-q"], root),
            (["uv", "run", "--frozen", "--no-sync", "ruff", "check", "."], root),
            (
                ["uv", "run", "--frozen", "--no-sync", "mypy", "support_queue"],
                root,
            ),
            (["npm", "test", "--", "--run"], root / "frontend"),
            (["npm", "run", "build"], root / "frontend"),
        )
        for argv, cwd in checks:
            result = self._commands.run(
                argv,
                cwd=cwd,
                timeout_seconds=timeout_seconds,
            )
            if not result.passed:
                return ProbeVerification(
                    passed=False,
                    detail=f"{' '.join(argv)} failed: {result.detail}",
                )
        return ProbeVerification(passed=True, detail="all maintenance-probe checks passed")


BACKEND_PROBE_TEST = '''from fastapi.testclient import TestClient

from support_queue.main import app


def test_waiting_on_customer_workflow_and_filter(monkeypatch, tmp_path):
    monkeypatch.setenv("SUPPORT_QUEUE_DB_PATH", str(tmp_path / "probe.sqlite"))
    headers = {"X-Actor": "probe", "X-Role": "admin"}
    with TestClient(app) as client:
        created = client.post(
            "/tickets",
            json={"title": "Probe", "description": "Maintenance", "priority": "medium"},
            headers=headers,
        )
        assert created.status_code == 201
        ticket = created.json()
        invalid = client.patch(
            f"/tickets/{ticket['id']}",
            json={"version": ticket["version"], "status": "waiting_on_customer"},
            headers=headers,
        )
        assert invalid.status_code == 409
        for status in ("in_progress", "waiting_on_customer"):
            changed = client.patch(
                f"/tickets/{ticket['id']}",
                json={"version": ticket["version"], "status": status},
                headers=headers,
            )
            assert changed.status_code == 200, changed.text
            ticket = changed.json()
        filtered = client.get("/tickets", params={"status": "waiting_on_customer"})
        assert filtered.status_code == 200
        assert [item["id"] for item in filtered.json()] == [ticket["id"]]
        for status in ("in_progress", "waiting_on_customer", "resolved"):
            changed = client.patch(
                f"/tickets/{ticket['id']}",
                json={"version": ticket["version"], "status": status},
                headers=headers,
            )
            assert changed.status_code == 200, changed.text
            ticket = changed.json()
'''

FRONTEND_PROBE_TEST = '''import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { App } from "./App";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

it("renders waiting_on_customer as an accessible status and control option", async () => {
  const ticket = {
    id: 1,
    title: "Maintenance probe",
    description: null,
    priority: "medium",
    status: "waiting_on_customer",
    assignee: null,
    comment_count: 0,
    comments: [],
    version: 1,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  };
  vi.stubGlobal("fetch", vi.fn(async (input) => {
    const payload = String(input).includes("/tickets/1")
      ? ticket
      : [ticket];
    return {
      ok: true,
      status: 200,
      json: async () => payload,
      text: async () => JSON.stringify(payload),
    };
  }));

  render(<App />);

  expect(await screen.findByText(/waiting on customer/i)).toBeInTheDocument();
  expect(screen.getAllByRole("option", { name: /waiting on customer/i })).not.toHaveLength(0);
});
'''


def parse_claude_probe_result(
    stdout: str,
    *,
    returncode: int,
    wall_seconds: float,
    detail: str = "",
) -> AgentProbeResult:
    usage = None
    for line in stdout.splitlines():
        parsed = parse_event_line(line)
        if parsed is not None:
            usage = parsed
    raw_tokens = None
    cost_usd = None
    if usage is not None:
        raw_tokens = (
            usage.input_tokens
            + usage.output_tokens
            + usage.cache_write_tokens
            + usage.cache_read_tokens
        )
        cost_usd = usage.cost_usd
    return AgentProbeResult(
        succeeded=returncode == 0,
        wall_seconds=wall_seconds,
        raw_tokens=raw_tokens,
        cost_usd=cost_usd,
        detail=detail,
    )


class ClaudeOpusMediumProbeAgent:
    def run(self, root: Path, *, timeout_seconds: float) -> AgentProbeResult:
        executable = shutil.which("claude")
        if executable is None:
            return AgentProbeResult(
                succeeded=False,
                wall_seconds=0,
                raw_tokens=None,
                cost_usd=None,
                detail="claude executable unavailable",
            )
        argv = [
            executable,
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-mode",
            "bypassPermissions",
            "--model",
            "claude-opus-5",
            "--effort",
            "medium",
            "--",
            MAINTENANCE_PROBE_PROMPT,
        ]
        started = time.monotonic()
        try:
            completed = subprocess.run(
                argv,
                cwd=root,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return AgentProbeResult(
                succeeded=False,
                wall_seconds=time.monotonic() - started,
                raw_tokens=None,
                cost_usd=None,
                detail=f"probe timed out: {exc}",
            )
        return parse_claude_probe_result(
            completed.stdout,
            returncode=completed.returncode,
            wall_seconds=time.monotonic() - started,
            detail=completed.stderr[-1000:].strip(),
        )


def _production_sources(root: Path) -> tuple[Path, ...]:
    candidates = (
        list((root / "support_queue").rglob("*.py"))
        + list((root / "frontend" / "src").rglob("*.ts"))
        + list((root / "frontend" / "src").rglob("*.tsx"))
    )
    return tuple(
        sorted(
            (
                path
                for path in candidates
                if path.is_file()
                and path.suffix in _SOURCE_SUFFIXES
                and not any(marker in path.name.lower() for marker in _TEST_MARKERS)
                and path.name not in {"test-setup.ts", "vite-env.d.ts"}
            ),
            key=lambda path: path.relative_to(root).as_posix(),
        )
    )


def _python_functions(source: str) -> list[tuple[int, int]]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    functions: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        complexity = 1
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.For, ast.AsyncFor, ast.While, ast.ExceptHandler)):
                complexity += 1
            elif isinstance(child, ast.BoolOp):
                complexity += max(0, len(child.values) - 1)
            elif isinstance(child, ast.Match):
                complexity += len(child.cases)
            elif isinstance(child, ast.comprehension):
                complexity += len(child.ifs)
        functions.append((complexity, max(1, (node.end_lineno or node.lineno) - node.lineno + 1)))
    return functions


_TS_FUNCTION = re.compile(
    r"(?:\bfunction\s+[A-Za-z_$][\w$]*\s*\([^)]*\)|"
    r"\b(?:const|let|var)\s+[A-Za-z_$][\w$]*\s*=\s*(?:async\s*)?"
    r"(?:\([^)]*\)|[A-Za-z_$][\w$]*)\s*=>|"
    r"(?<![\w$])(?!(?:if|for|while|switch|catch)\s*\()[A-Za-z_$][\w$]*\s*\([^)]*\))\s*\{"
)


def _matching_brace(source: str, opening: int) -> int | None:
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(opening, len(source)):
        char = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {"'", '"', "`"}:
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index
    return None


def _typescript_functions(source: str) -> list[tuple[int, int]]:
    functions: list[tuple[int, int]] = []
    for match in _TS_FUNCTION.finditer(source):
        opening = source.find("{", match.start(), match.end())
        closing = _matching_brace(source, opening)
        if closing is None:
            continue
        body = source[opening + 1 : closing]
        decision_count = len(re.findall(r"\b(?:if|for|while|case|catch)\b|&&|\|\||\?\?", body))
        ternaries = len(re.findall(r"(?<!\?)\?(?![?.])", body))
        functions.append(
            (
                1 + decision_count + ternaries,
                source[match.start() : closing + 1].count("\n") + 1,
            )
        )
    return functions


def _percentile_95(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = max(0, (95 * len(ordered) + 99) // 100 - 1)
    return ordered[index]


def _normalized_lines(source: str) -> list[str]:
    result = []
    for raw in source.splitlines():
        line = re.sub(r"\s+", " ", raw.strip())
        if line and not line.startswith(("#", "//")):
            result.append(line)
    return result


def _duplicate_blocks(sources: dict[str, str]) -> int:
    occurrences: dict[tuple[str, ...], list[tuple[str, int]]] = {}
    for path, source in sources.items():
        lines = _normalized_lines(source)
        for index in range(len(lines) - _DUPLICATE_BLOCK_LINES + 1):
            block = tuple(lines[index : index + _DUPLICATE_BLOCK_LINES])
            occurrences.setdefault(block, []).append((path, index))
    return sum(len(locations) > 1 for locations in occurrences.values())


def _module_name(root: Path, path: Path) -> str:
    return ".".join(path.relative_to(root).with_suffix("").parts)


def _import_graph(root: Path, sources: tuple[Path, ...]) -> dict[str, set[str]]:
    modules = {_module_name(root, path): path for path in sources}
    graph: dict[str, set[str]] = {module: set() for module in modules}
    for module, path in modules.items():
        source = path.read_text(encoding="utf-8")
        if path.suffix == ".py":
            try:
                tree = ast.parse(source)
            except SyntaxError:
                continue
            package = module.rsplit(".", 1)[0] if "." in module else ""
            for node in ast.walk(tree):
                targets: list[str] = []
                if isinstance(node, ast.Import):
                    targets = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    if node.level:
                        prefix = package
                        if node.module:
                            targets = [f"{prefix}.{node.module}".strip(".")]
                        else:
                            targets = [f"{prefix}.{alias.name}".strip(".") for alias in node.names]
                    elif node.module:
                        targets = [node.module]
                graph[module].update(target for target in targets if target in modules)
        else:
            for target in re.findall(
                r"(?:\bfrom\s+|\bimport\s+)['\"](\.[^'\"]+)['\"]",
                source,
            ):
                resolved = (path.parent / target).resolve()
                candidates = [resolved] if resolved.suffix in {".ts", ".tsx"} else []
                candidates.extend(
                    [
                        Path(f"{resolved}.ts"),
                        Path(f"{resolved}.tsx"),
                        resolved / "index.ts",
                        resolved / "index.tsx",
                    ]
                )
                for candidate in candidates:
                    if candidate.exists():
                        imported = _module_name(root, candidate)
                        if imported in modules:
                            graph[module].add(imported)
                        break
    return graph


def _cycles(graph: dict[str, set[str]]) -> tuple[tuple[str, ...], ...]:
    found: set[tuple[str, ...]] = set()

    def visit(start: str, current: str, path: list[str]) -> None:
        for target in graph[current]:
            if target == start and len(path) > 1:
                rotations = [tuple(path[index:] + path[:index]) for index in range(len(path))]
                found.add(min(rotations))
            elif target not in path and len(path) < len(graph):
                visit(start, target, [*path, target])

    for module in sorted(graph):
        visit(module, module, [module])
    return tuple(sorted(found))


def analyze_static(root: Path) -> StaticMaintainability:
    root = root.resolve()
    sources = _production_sources(root)
    contents = {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8") for path in sources
    }
    file_metrics: list[FileMetric] = []
    complexities: list[int] = []
    function_lines: list[int] = []
    for path in sources:
        relative = path.relative_to(root).as_posix()
        source = contents[relative]
        language = "python" if path.suffix == ".py" else "typescript"
        functions = (
            _python_functions(source) if language == "python" else _typescript_functions(source)
        )
        complexities.extend(value for value, _ in functions)
        function_lines.extend(lines for _, lines in functions)
        file_metrics.append(
            FileMetric(
                path=relative,
                language=language,
                lines=len(source.splitlines()),
                functions=len(functions),
                complexity_max=max((value for value, _ in functions), default=0),
                function_lines_max=max((lines for _, lines in functions), default=0),
            )
        )
    source_lines = [metric.lines for metric in file_metrics]
    return StaticMaintainability(
        source_files=len(file_metrics),
        source_lines=sum(source_lines),
        function_count=len(complexities),
        complexity_median=float(median(complexities)) if complexities else 0,
        complexity_p95=_percentile_95(complexities),
        complexity_max=max(complexities, default=0),
        max_function_lines=max(function_lines, default=0),
        large_functions=sum(lines >= 60 for lines in function_lines),
        max_file_lines=max(source_lines, default=0),
        large_files=sum(lines >= 400 for lines in source_lines),
        duplicated_blocks=_duplicate_blocks(contents),
        import_cycles=_cycles(_import_graph(root, sources)),
        files=tuple(file_metrics),
    )


def _mutant_id(language: str, operator: str, path: str, line: int, original: str) -> str:
    identity = f"{language}:{operator}:{path}:{line}:{original}"
    digest = hashlib.sha256(identity.encode()).hexdigest()[:8]
    return f"{language}:{operator}:{path}:{line}:{digest}"


def _python_mutants(root: Path, path: Path, source: str) -> list[Mutant]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    lines = source.splitlines(keepends=True)
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line))

    def absolute(line: int, column: int) -> int:
        return offsets[line - 1] + column

    relative = path.relative_to(root).as_posix()
    candidates: list[Mutant] = []

    def add(operator: str, line: int, start: int, end: int, replacement: str) -> None:
        original = source[start:end]
        candidates.append(
            Mutant(
                mutant_id=_mutant_id("python", operator, relative, line, original),
                language="python",
                operator=operator,
                path=relative,
                line=line,
                offset_start=start,
                offset_end=end,
                original=original,
                replacement=replacement,
            )
        )

    comparison_replacements = {ast.Eq: "!=", ast.NotEq: "==", ast.In: "not in", ast.NotIn: "in"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            replacement = comparison_replacements.get(type(node.ops[0]))
            if replacement:
                left_end = absolute(
                    node.left.end_lineno or node.lineno,
                    node.left.end_col_offset or 0,
                )
                right = node.comparators[0]
                right_start = absolute(right.lineno, right.col_offset)
                segment = source[left_end:right_start]
                match = re.search(r"\b(?:not\s+in|in)\b|==|!=", segment)
                if match:
                    add(
                        "comparison",
                        node.lineno,
                        left_end + match.start(),
                        left_end + match.end(),
                        replacement,
                    )
        elif isinstance(node, ast.BoolOp) and len(node.values) >= 2:
            left = node.values[0]
            right = node.values[1]
            start = absolute(left.end_lineno or node.lineno, left.end_col_offset or 0)
            end = absolute(right.lineno, right.col_offset)
            match = re.search(r"\b(?:and|or)\b", source[start:end])
            if match:
                original = match.group()
                add(
                    "boolean_connector",
                    node.lineno,
                    start + match.start(),
                    start + match.end(),
                    "or" if original == "and" else "and",
                )
        elif isinstance(node, ast.Constant) and isinstance(node.value, bool):
            start = absolute(node.lineno, node.col_offset)
            end = absolute(node.end_lineno or node.lineno, node.end_col_offset or node.col_offset)
            add("boolean_literal", node.lineno, start, end, "False" if node.value else "True")
    return candidates


def _typescript_mutants(root: Path, path: Path, source: str) -> list[Mutant]:
    relative = path.relative_to(root).as_posix()
    code = _typescript_code_mask(source)
    patterns = (
        (
            "comparison",
            re.compile(r"===|!==|==|!="),
            {"===": "!==", "!==": "===", "==": "!=", "!=": "=="},
        ),
        ("boolean_connector", re.compile(r"&&|\|\|"), {"&&": "||", "||": "&&"}),
        ("boolean_literal", re.compile(r"\b(?:true|false)\b"), {"true": "false", "false": "true"}),
    )
    mutants: list[Mutant] = []
    for operator, pattern, replacements in patterns:
        for match in pattern.finditer(source):
            if not all(code[match.start() : match.end()]):
                continue
            original = match.group()
            line = source.count("\n", 0, match.start()) + 1
            mutants.append(
                Mutant(
                    mutant_id=_mutant_id("typescript", operator, relative, line, original),
                    language="typescript",
                    operator=operator,
                    path=relative,
                    line=line,
                    offset_start=match.start(),
                    offset_end=match.end(),
                    original=original,
                    replacement=replacements[original],
                )
            )
    return mutants


def _typescript_code_mask(source: str) -> list[bool]:
    """Mark TypeScript outside comments and string/template literals."""
    mask = [True] * len(source)
    index = 0
    state = "code"
    quote = ""
    while index < len(source):
        if state == "code":
            if source.startswith("//", index):
                state = "line_comment"
                mask[index : index + 2] = [False, False]
                index += 2
                continue
            if source.startswith("/*", index):
                state = "block_comment"
                mask[index : index + 2] = [False, False]
                index += 2
                continue
            if source[index] in {"'", '"', "`"}:
                state = "string"
                quote = source[index]
                mask[index] = False
        elif state == "line_comment":
            mask[index] = False
            if source[index] == "\n":
                state = "code"
        elif state == "block_comment":
            mask[index] = False
            if source.startswith("*/", index):
                mask[index : index + 2] = [False, False]
                index += 2
                state = "code"
                continue
        else:
            mask[index] = False
            if source[index] == "\\":
                if index + 1 < len(source):
                    mask[index + 1] = False
                    index += 2
                    continue
            elif source[index] == quote:
                state = "code"
        index += 1
    return mask


def discover_mutants(root: Path, *, limit: int = 24) -> tuple[Mutant, ...]:
    candidates: list[Mutant] = []
    for path in _production_sources(root):
        source = path.read_text(encoding="utf-8")
        if path.suffix == ".py":
            candidates.extend(_python_mutants(root, path, source))
        else:
            candidates.extend(_typescript_mutants(root, path, source))
    candidates.sort(
        key=lambda item: (
            item.language,
            item.operator,
            item.path,
            item.line,
            item.mutant_id,
        )
    )
    if len(candidates) <= limit:
        return tuple(candidates)
    by_operator: dict[tuple[str, str], list[Mutant]] = {}
    for candidate in candidates:
        by_operator.setdefault((candidate.language, candidate.operator), []).append(candidate)
    selected: list[Mutant] = []
    while len(selected) < limit and any(by_operator.values()):
        for key in sorted(by_operator):
            if by_operator[key] and len(selected) < limit:
                selected.append(by_operator[key].pop(0))
    return tuple(selected)


def run_mutation_pack(
    root: Path,
    *,
    mutants: tuple[Mutant, ...],
    runner: MutationRunner,
    timeout_seconds: float = 300,
    baseline_repetitions: int = 2,
) -> tuple[MutationOutcome, ...]:
    if baseline_repetitions < 1:
        raise ValueError("baseline_repetitions must be positive")
    languages = sorted({mutant.language for mutant in mutants})
    for language in languages:
        for repetition in range(1, baseline_repetitions + 1):
            baseline = runner.run(language, root, timeout_seconds=timeout_seconds)
            if not baseline.passed:
                raise RuntimeError(
                    f"{language} mutation baseline failed on repetition "
                    f"{repetition}/{baseline_repetitions}: {baseline.detail}"
                )

    outcomes: list[MutationOutcome] = []
    for mutant in mutants:
        path = root / mutant.path
        original_source = path.read_text(encoding="utf-8")
        actual = original_source[mutant.offset_start : mutant.offset_end]
        if actual != mutant.original:
            outcomes.append(
                MutationOutcome(
                    mutant_id=mutant.mutant_id,
                    status="error",
                    duration_seconds=0,
                    detail=f"source changed: expected {mutant.original!r}, got {actual!r}",
                )
            )
            continue
        mutated = (
            original_source[: mutant.offset_start]
            + mutant.replacement
            + original_source[mutant.offset_end :]
        )
        path.write_text(mutated, encoding="utf-8")
        try:
            result = runner.run(mutant.language, root, timeout_seconds=timeout_seconds)
        except Exception as exc:  # runner failures are evidence, not a reason to leave a mutation
            outcomes.append(
                MutationOutcome(
                    mutant_id=mutant.mutant_id,
                    status="error",
                    duration_seconds=0,
                    detail=str(exc),
                )
            )
        else:
            status: Literal["killed", "survived", "error", "timeout"]
            duration = result.duration_seconds
            detail = result.detail
            if result.timed_out:
                status = "timeout"
            elif result.passed:
                status = "survived"
            else:
                confirmation = runner.run(
                    mutant.language,
                    root,
                    timeout_seconds=timeout_seconds,
                )
                duration += confirmation.duration_seconds
                if confirmation.timed_out:
                    status = "timeout"
                    detail = confirmation.detail
                elif confirmation.passed:
                    status = "error"
                    detail = "non-deterministic test result while confirming killed mutant"
                else:
                    status = "killed"
            outcomes.append(
                MutationOutcome(
                    mutant_id=mutant.mutant_id,
                    status=status,
                    duration_seconds=duration,
                    detail=detail,
                )
            )
        finally:
            path.write_text(original_source, encoding="utf-8")
    return tuple(outcomes)


def _probe_snapshot(root: Path) -> dict[str, str]:
    ignored_parts = frozenset({".git", ".venv", "node_modules", "dist", "__pycache__"})
    snapshot: dict[str, str] = {}
    for path in root.rglob("*"):
        if not path.is_file() or ignored_parts & set(path.relative_to(root).parts):
            continue
        try:
            snapshot[path.relative_to(root).as_posix()] = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
    return snapshot


def _change_surface(before: dict[str, str], after: dict[str, str]) -> tuple[int, int]:
    changed_files = 0
    changed_lines = 0
    for path in sorted(set(before) | set(after)):
        left = before.get(path, "").splitlines()
        right = after.get(path, "").splitlines()
        if left == right:
            continue
        changed_files += 1
        matcher = difflib.SequenceMatcher(a=left, b=right, autojunk=False)
        for tag, left_start, left_end, right_start, right_end in matcher.get_opcodes():
            if tag != "equal":
                changed_lines += (left_end - left_start) + (right_end - right_start)
    return changed_files, changed_lines


def run_maintenance_probe(
    root: Path,
    *,
    agent: ProbeAgent,
    verifier: ProbeVerifier,
    timeout_seconds: float = 7200,
) -> ProbeOutcome:
    with tempfile.TemporaryDirectory(prefix="symphony-maintenance-probe-") as temporary:
        checkout = Path(temporary) / "checkout"
        shutil.copytree(
            root.resolve(),
            checkout,
            ignore=shutil.ignore_patterns(".git", ".venv", "node_modules", "dist"),
        )
        before = _probe_snapshot(checkout)
        agent_result = agent.run(checkout, timeout_seconds=timeout_seconds)
        verification = (
            verifier.verify(checkout, timeout_seconds=timeout_seconds)
            if agent_result.succeeded
            else ProbeVerification(False, f"agent failed: {agent_result.detail}")
        )
        after = _probe_snapshot(checkout)
        changed_files, changed_lines = _change_surface(before, after)

    return ProbeOutcome(
        status="passed" if verification.passed else "failed",
        repetitions=1,
        successful_repetitions=int(verification.passed),
        wall_seconds=agent_result.wall_seconds,
        raw_tokens=agent_result.raw_tokens,
        cost_usd=agent_result.cost_usd,
        changed_files=changed_files,
        changed_lines=changed_lines,
        detail="" if verification.passed else verification.detail,
    )


def report_payload(
    *,
    repository: str,
    static: StaticMaintainability,
    mutations: tuple[MutationOutcome, ...],
    probe: ProbeOutcome | None,
    errors: tuple[str, ...] = (),
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "repository": repository,
        "static": asdict(static),
        "mutations": [asdict(outcome) for outcome in mutations],
        "probe": asdict(probe) if probe else None,
        "errors": list(errors),
    }


def load_existing_results(
    output: Path,
) -> tuple[tuple[MutationOutcome, ...], ProbeOutcome | None, tuple[str, ...]]:
    path = output / "MAINTAINABILITY.json"
    if not path.exists():
        return (), None, ()
    payload = json.loads(path.read_text(encoding="utf-8"))
    mutations = tuple(MutationOutcome(**item) for item in payload.get("mutations", []))
    raw_probe = payload.get("probe")
    probe = ProbeOutcome(**raw_probe) if isinstance(raw_probe, dict) else None
    return mutations, probe, tuple(str(error) for error in payload.get("errors", []))


def load_probe_receipt(
    path: Path,
    *,
    repository: str,
    baseline_sha: str,
) -> ProbeOutcome:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = {
        "schema_version": 1,
        "repository": repository,
        "baseline_sha": baseline_sha,
        "profile": "Claude Opus 5 medium",
        "probe": "waiting_on_customer",
    }
    mismatches = [
        key for key, value in expected.items() if payload.get(key) != value
    ]
    if mismatches:
        raise ValueError(f"probe receipt identity mismatch: {', '.join(mismatches)}")
    raw = payload.get("probe_outcome")
    if not isinstance(raw, dict):
        raise ValueError("probe receipt has no probe_outcome object")
    return ProbeOutcome(**raw)


def render_maintainability_markdown(
    *,
    repository: str,
    static: StaticMaintainability,
    mutations: tuple[MutationOutcome, ...],
    probe: ProbeOutcome | None,
    errors: tuple[str, ...] = (),
) -> str:
    killed = sum(outcome.status == "killed" for outcome in mutations)
    lines = [
        f"# Maintainability — `{repository}`",
        "",
        "> Functional grade is intentionally not included; maintainability is a separate axis.",
        "",
        "## Static diagnostics",
        "",
        f"- Production source: {static.source_files} files, {static.source_lines} lines",
        f"- Functions: {static.function_count}",
        f"- Cyclomatic complexity median/p95/max: "
        f"{static.complexity_median:g}/{static.complexity_p95}/{static.complexity_max}",
        f"- Largest function/file: {static.max_function_lines}/{static.max_file_lines} lines",
        f"- Large functions/files: {static.large_functions}/{static.large_files}",
        f"- Duplicated six-line blocks: {static.duplicated_blocks}",
        f"- Import cycles: {len(static.import_cycles)}",
        "",
        f"## Mutation testing — {killed}/{len(mutations)} killed",
        "",
    ]
    if mutations:
        lines.extend(f"- `{item.mutant_id}`: {item.status}" for item in mutations)
    else:
        lines.append("- Not run.")
    lines.extend(["", "## Maintenance probe", ""])
    if probe is None:
        lines.append("- Not run.")
    else:
        lines.extend(
            [
                f"- Status: {probe.status}",
                f"- Successful repetitions: {probe.successful_repetitions}/{probe.repetitions}",
                f"- Wall time: {probe.wall_seconds:.2f}s",
                "- Raw tokens: "
                f"{probe.raw_tokens if probe.raw_tokens is not None else 'unavailable'}",
                (
                    f"- Cost: ${probe.cost_usd:.4f}"
                    if probe.cost_usd is not None
                    else "- Cost: unavailable"
                ),
                f"- Change surface: {probe.changed_files} files, {probe.changed_lines} lines",
            ]
        )
        if probe.detail:
            lines.append(f"- Detail: {probe.detail}")
    if errors:
        lines.extend(["", "## Errors", "", *(f"- {error}" for error in errors)])
    return "\n".join(lines).rstrip() + "\n"


def write_reports(
    *,
    output: Path,
    repository: str,
    static: StaticMaintainability,
    mutations: tuple[MutationOutcome, ...] = (),
    probe: ProbeOutcome | None = None,
    errors: tuple[str, ...] = (),
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    payload = report_payload(
        repository=repository,
        static=static,
        mutations=mutations,
        probe=probe,
        errors=errors,
    )
    (output / "MAINTAINABILITY.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / "MAINTAINABILITY.md").write_text(
        render_maintainability_markdown(
            repository=repository,
            static=static,
            mutations=mutations,
            probe=probe,
            errors=errors,
        ),
        encoding="utf-8",
    )
