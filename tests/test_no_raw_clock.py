"""CI guard: the orchestrator package must read time through the injected
clock (`self._now()`), never raw `datetime.now(...)` / `time.monotonic(...)`.

Raw wall-clock reads are non-deterministic and defeat the test harness's
`ManualClock`. Two exceptions are allowed:

* `datetime.now(UTC).isoformat()` — persisting a record timestamp (a fact about
  when something happened), not control-flow timing.
* a line explicitly annotated `# noqa: clock` — the sanctioned `_now()`
  wall-clock fallbacks themselves.

This runs under `uv run pytest` in CI (see .github/workflows/ci.yml).
"""

from __future__ import annotations

from pathlib import Path

import symphony.orchestrator as orchestrator_pkg

BANNED = ("datetime.now(", "time.monotonic(")


def _orchestrator_files() -> list[Path]:
    root = Path(orchestrator_pkg.__file__).parent
    return sorted(root.rglob("*.py"))


def test_no_raw_clock_in_orchestrator() -> None:
    violations: list[str] = []
    for path in _orchestrator_files():
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if "# noqa: clock" in line:
                continue
            if ".isoformat()" in line:
                continue
            if any(token in line for token in BANNED):
                violations.append(f"{path.name}:{lineno}: {line.strip()}")
    assert not violations, (
        "raw clock access in orchestrator package — route through self._now():\n"
        + "\n".join(violations)
    )
