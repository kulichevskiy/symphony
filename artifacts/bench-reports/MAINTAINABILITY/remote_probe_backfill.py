from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from remote_probe_runner import BACKEND_PROBE_TEST, FRONTEND_PROBE_TEST

from symphony.bench.executor import RemoteCommands


async def verify(receipt_path: Path) -> dict[str, object]:
    receipt = json.loads(await asyncio.to_thread(receipt_path.read_text, encoding="utf-8"))
    lane = str(receipt["lane"])
    checkout = Path(str(receipt["checkout"]))
    commands = RemoteCommands(
        base_url=os.environ[f"SYMPHONY_BENCH_EXECUTOR_{lane}_URL"],
        token=os.environ["SYMPHONY_BENCH_EXECUTOR_TOKEN"],
        timeout_seconds=1800,
    )
    checks = dict(receipt.get("checks", {}))
    coverage = dict(receipt.get("coverage", {}))
    coverage.pop("frontend_accessible_label", None)
    for key in (
        "frontend accessible label",
        "hidden workflow probe",
        "hidden accessible-frontend probe",
    ):
        checks.pop(key, None)

    backend_test = checkout / "tests" / "test_maintainability_probe.py"
    frontend_test = checkout / "frontend" / "src" / "maintainability-probe.test.tsx"
    writes = (
        (checkout, backend_test.relative_to(checkout), BACKEND_PROBE_TEST),
        (
            checkout / "frontend",
            frontend_test.relative_to(checkout / "frontend"),
            FRONTEND_PROBE_TEST,
        ),
    )
    for cwd, relative, content in writes:
        await commands.run(
            [
                "python",
                "-c",
                "from pathlib import Path; Path("
                + repr(str(relative))
                + ").write_text("
                + repr(content)
                + ")",
            ],
            cwd=cwd,
        )
    try:
        try:
            await commands.run(
                [
                    "uv",
                    "run",
                    "--frozen",
                    "--no-sync",
                    "pytest",
                    str(backend_test.relative_to(checkout)),
                ],
                cwd=checkout,
            )
        except Exception as exc:
            checks["hidden workflow probe"] = f"failed: {str(exc)[-4000:]}"
        else:
            checks["hidden workflow probe"] = "passed"
        try:
            await commands.run(
                ["npm", "test", "--", "--run", frontend_test.name],
                cwd=checkout / "frontend",
            )
        except Exception as exc:
            checks["hidden accessible-frontend probe"] = f"failed: {str(exc)[-4000:]}"
        else:
            checks["hidden accessible-frontend probe"] = "passed"
    finally:
        await commands.run(["rm", "-f", str(backend_test)], cwd=checkout)
        await commands.run(["rm", "-f", str(frontend_test)], cwd=checkout)

    passed = all(value == "passed" for value in checks.values()) and all(coverage.values())
    receipt["checks"] = checks
    receipt["coverage"] = coverage
    receipt["status"] = "passed" if passed else "failed"
    receipt["probe_outcome"] = {
        "status": receipt["status"],
        "repetitions": 1,
        "successful_repetitions": int(passed),
        "wall_seconds": receipt["wall_seconds"],
        "raw_tokens": receipt.get("raw_tokens"),
        "cost_usd": receipt.get("cost_usd"),
        "changed_files": receipt["changed_files"],
        "changed_lines": receipt["changed_lines"],
        "detail": "" if passed else "one or more verification gates failed",
    }
    await asyncio.to_thread(
        receipt_path.write_text,
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("receipt", type=Path)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(verify(args.receipt)), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
