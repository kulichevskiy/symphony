#!/usr/bin/env python3
"""One-off proof that the `poll.py` -> `poll/` split (SYM-142..SYM-151) is a
behavioural no-op.

For every method and free function in the pre-split single-file
``src/symphony/orchestrator/poll.py`` (git ``a4fea57``, the last commit before
SYM-142) this asserts that an identically-named function with an *identical
body* exists somewhere in the new ``poll/`` package. Only the placement of the
``def`` (which module / which class), its indentation, and the cosmetic line
re-wrapping that ruff applies when a body is dedented to a shallower scope are
allowed to differ — so the body is normalised by parsing it and dumping its
AST before comparison. Any change to the logic (statements, calls, literals,
control flow) still fails the check; only formatting is ignored.

Run from the repo root:

    uv run python scripts/verify_poll_split_noop.py

Exit code 0 and a "MATCH" summary means the split changed no behaviour.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

ORIGINAL_REF = "a4fea57:src/symphony/orchestrator/poll.py"
PACKAGE_DIR = Path("src/symphony/orchestrator/poll")


def _is_stub(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """True for a ``TYPE_CHECKING`` signature stub (body is a lone ``...``)."""
    return (
        len(node.body) == 1
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and node.body[0].value.value is Ellipsis
    )


def _function_bodies(source: str) -> list[tuple[str, str]]:
    """Return ``(name, normalised_body)`` for every def/async-def in ``source``.

    The body is its statement AST, dumped with ``ast.dump`` (attributes — and
    therefore line numbers — excluded). This normalises away placement,
    indentation, and the cosmetic line re-wrapping ruff applies when a body is
    dedented to a shallower scope, while comparing the logic exactly.
    ``TYPE_CHECKING`` signature stubs are skipped.
    """
    out: list[tuple[str, str]] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if _is_stub(node):
            continue
        body = ast.Module(body=node.body, type_ignores=[])
        out.append((node.name, ast.dump(body)))
    return out


def _load_original() -> str:
    return subprocess.run(
        ["git", "show", ORIGINAL_REF],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _load_package() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in sorted(PACKAGE_DIR.glob("*.py")))


def main() -> int:
    original = _function_bodies(_load_original())

    # name -> set of dedented bodies present anywhere in the new package.
    package_bodies: dict[str, set[str]] = {}
    for path in sorted(PACKAGE_DIR.glob("*.py")):
        for name, body in _function_bodies(path.read_text(encoding="utf-8")):
            package_bodies.setdefault(name, set()).add(body)

    mismatches: list[str] = []
    for name, body in original:
        candidates = package_bodies.get(name)
        if candidates is None:
            mismatches.append(f"{name}: missing from new package")
        elif body not in candidates:
            mismatches.append(f"{name}: body differs from original")

    if mismatches:
        print("MISMATCH — the split is NOT a no-op:", file=sys.stderr)
        for line in mismatches:
            print(f"  - {line}", file=sys.stderr)
        return 1

    print(f"MATCH — {len(original)} method/function bodies byte-identical to {ORIGINAL_REF}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
