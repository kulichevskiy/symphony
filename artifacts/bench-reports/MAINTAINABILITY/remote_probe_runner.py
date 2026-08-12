from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path

import aiosqlite

from symphony import db
from symphony.agent.process import parse_event_line
from symphony.bench.cli import _BenchSecrets
from symphony.bench.executor import RemoteCommands
from symphony.claude_login import (
    claude_access_token,
    claude_credential_expires_within,
    claude_expires_at,
)
from symphony.claude_token_dispenser import ClaudeTokenDispenser, TokenGrant
from symphony.credentials import CredentialResolver, CredentialWriteBack
from symphony.crypto import CredentialCipher, resolve_encryption_key

PROMPT = """Implement this exact maintenance change completely in this Support Queue repository.

Add a new ticket status named `waiting_on_customer` across the backend, persistence contract,
API filtering, workflow rules, frontend controls, accessible status presentation, and automated
tests. The allowed new transitions are `in_progress -> waiting_on_customer`,
`waiting_on_customer -> in_progress`, and `waiting_on_customer -> resolved`. Preserve every
existing transition and behavior. Existing databases must remain readable. Add focused backend
and frontend regression tests containing the exact status name. Run all documented checks.

Work directly in the checkout. Do not commit, push, open a pull request, or change dependencies
unless the implementation genuinely requires it. Finish only when the project tests and build pass.
"""

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


async def run(repository: str, lane: str) -> dict[str, object]:
    db_path = Path(os.environ["SYMPHONY_BENCH_DB_PATH"])
    key = resolve_encryption_key(_BenchSecrets().encryption_key, db_path.parent)
    cipher = CredentialCipher(key)
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    try:
        github_token = await CredentialResolver(conn, cipher).resolve("github")
        snapshot = await db.oauth_connections.get_connection_snapshot(conn, "claude", cipher)
        if snapshot is None:
            raise RuntimeError("required Claude connection is unavailable")
        claude_token = claude_access_token(snapshot.credential)
        if claude_credential_expires_within(snapshot.credential, 7200):
            response = await ClaudeTokenDispenser(
                conn,
                cipher,
                CredentialWriteBack(conn, cipher),
            ).request(snapshot.generation)
            if not isinstance(response, TokenGrant):
                raise RuntimeError(f"Claude credential refresh refused: {response.reason}")
            claude_token = response.token
        claude_blob = await db.oauth_connections.get_credential(conn, "claude", cipher)
    finally:
        await conn.close()
    if not github_token or not claude_blob:
        raise RuntimeError("required GitHub or Claude connection is unavailable")
    if not claude_token or claude_access_token(claude_blob) != claude_token:
        raise RuntimeError("refreshed Claude token was not durably stored")

    lane = lane.upper()
    base_url = os.environ[f"SYMPHONY_BENCH_EXECUTOR_{lane}_URL"]
    executor_token = os.environ["SYMPHONY_BENCH_EXECUTOR_TOKEN"]
    root = Path(os.environ[f"SYMPHONY_BENCH_ROOT_{lane}"])
    commands = RemoteCommands(base_url=base_url, token=executor_token, timeout_seconds=7200)
    checkout = root / "maintenance-probes" / f"{repository.split('/')[-1]}-{int(time.time())}"
    await commands.run(["mkdir", "-p", str(checkout.parent)], cwd=root)
    await commands.run(
        ["gh", "repo", "clone", repository, checkout.name, "--", "--depth=1"],
        cwd=checkout.parent,
        env={"GH_TOKEN": github_token},
    )
    baseline = await commands.run(["git", "rev-parse", "HEAD"], cwd=checkout)
    started = time.monotonic()
    stdout = await commands.run(
        [
            "claude",
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
            PROMPT,
        ],
        cwd=checkout,
        env={"CLAUDE_CODE_OAUTH_TOKEN": claude_token},
    )
    wall_seconds = time.monotonic() - started
    usage = None
    for line in stdout.splitlines():
        parsed = parse_event_line(line)
        if parsed is not None:
            usage = parsed

    checks = (
        (["uv", "sync", "--locked"], checkout),
        (["uv", "run", "--frozen", "--no-sync", "pytest", "-q"], checkout),
        (["uv", "run", "--frozen", "--no-sync", "ruff", "check", "."], checkout),
        (["uv", "run", "--frozen", "--no-sync", "mypy", "support_queue"], checkout),
        (["npm", "ci"], checkout / "frontend"),
        (["npm", "test", "--", "--run"], checkout / "frontend"),
        (["npm", "run", "build"], checkout / "frontend"),
    )
    check_results: dict[str, str] = {}
    for argv, cwd in checks:
        name = " ".join(argv)
        try:
            await commands.run(argv, cwd=cwd)
        except Exception as exc:
            check_results[name] = f"failed: {str(exc)[-500:]}"
        else:
            check_results[name] = "passed"

    coverage_script = """from pathlib import Path
root=Path('.')
groups={
'backend_product':list((root/'support_queue').rglob('*.py')),
'backend_tests':list((root/'tests').rglob('test*.py')),
'frontend_product':[p for p in (root/'frontend'/'src').rglob('*.ts*')
                    if '.test.' not in p.name and '.spec.' not in p.name],
'frontend_tests':[p for p in (root/'frontend'/'src').rglob('*.ts*')
                  if '.test.' in p.name or '.spec.' in p.name],
}
for name,paths in groups.items():
 print(name, any('waiting_on_customer' in p.read_text() for p in paths if p.is_file()))
"""
    coverage_raw = await commands.run(["python", "-c", coverage_script], cwd=checkout)
    coverage = {
        line.split()[0]: line.split()[1] == "True"
        for line in coverage_raw.splitlines()
        if len(line.split()) == 2
    }
    hidden_test = checkout / "tests" / "test_maintainability_probe.py"
    await commands.run(
        [
            "python",
            "-c",
            "from pathlib import Path; Path('tests/test_maintainability_probe.py').write_text("
            + repr(BACKEND_PROBE_TEST)
            + ")",
        ],
        cwd=checkout,
    )
    try:
        await commands.run(
            [
                "uv",
                "run",
                "--frozen",
                "--no-sync",
                "pytest",
                "tests/test_maintainability_probe.py",
            ],
            cwd=checkout,
        )
    except Exception as exc:
        check_results["hidden workflow probe"] = f"failed: {str(exc)[-500:]}"
    else:
        check_results["hidden workflow probe"] = "passed"
    finally:
        await commands.run(["rm", "-f", str(hidden_test)], cwd=checkout)
    frontend_hidden_test = checkout / "frontend" / "src" / "maintainability-probe.test.tsx"
    await commands.run(
        [
            "python",
            "-c",
            "from pathlib import Path; "
            "Path('src/maintainability-probe.test.tsx').write_text("
            + repr(FRONTEND_PROBE_TEST)
            + ")",
        ],
        cwd=checkout / "frontend",
    )
    try:
        await commands.run(
            ["npm", "test", "--", "--run", frontend_hidden_test.name],
            cwd=checkout / "frontend",
        )
    except Exception as exc:
        check_results["hidden accessible-frontend probe"] = f"failed: {str(exc)[-500:]}"
    else:
        check_results["hidden accessible-frontend probe"] = "passed"
    finally:
        await commands.run(["rm", "-f", str(frontend_hidden_test)], cwd=checkout)
    await commands.run(["git", "add", "-N", "."], cwd=checkout)
    numstat = await commands.run(["git", "diff", "--numstat", "HEAD"], cwd=checkout)
    changed_files = 0
    changed_lines = 0
    for line in numstat.splitlines():
        added, removed, _path = line.split("\t", 2)
        changed_files += 1
        if added.isdigit() and removed.isdigit():
            changed_lines += int(added) + int(removed)
    passed = all(value == "passed" for value in check_results.values()) and all(coverage.values())
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
    probe_outcome = {
        "status": "passed" if passed else "failed",
        "repetitions": 1,
        "successful_repetitions": int(passed),
        "wall_seconds": wall_seconds,
        "raw_tokens": raw_tokens,
        "cost_usd": cost_usd,
        "changed_files": changed_files,
        "changed_lines": changed_lines,
        "detail": "" if passed else "one or more verification gates failed",
    }
    return {
        "schema_version": 1,
        "repository": repository,
        "lane": lane,
        "baseline_sha": baseline.strip(),
        "profile": "Claude Opus 5 medium",
        "probe": "waiting_on_customer",
        "status": "passed" if passed else "failed",
        "probe_outcome": probe_outcome,
        "wall_seconds": wall_seconds,
        "raw_tokens": raw_tokens,
        "cost_usd": cost_usd,
        "changed_files": changed_files,
        "changed_lines": changed_lines,
        "coverage": coverage,
        "checks": check_results,
        "claude_expires_at": claude_expires_at(claude_blob),
        "checkout": str(checkout),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("repository")
    parser.add_argument("lane", choices=("A", "B", "a", "b"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rendered = json.dumps(asyncio.run(run(args.repository, args.lane)), indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
