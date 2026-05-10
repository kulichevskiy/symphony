"""CLI tests for `runs ls`, `runs show`, and `dispatch`.

The first two read directly from a SQLite fixture so the orchestrator
process does not have to be running. `dispatch` resolves the Linear
identifier through a fake Linear client and exercises the same dispatch
path the poll loop uses (run row + announce comment).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from symphony import db
from symphony.cli import main
from symphony.linear.client import LinearIssue


def _populate(p: Path) -> None:
    async def _do() -> None:
        conn = await db.connect(p)
        try:
            await db.issues.upsert(
                conn, id="iss-1", identifier="ENG-1", title="t1", team_key="ENG"
            )
            await db.issues.upsert(
                conn, id="iss-2", identifier="ENG-2", title="t2", team_key="ENG"
            )
            await db.runs.create(
                conn,
                id="run-a",
                issue_id="iss-1",
                stage="implement",
                status="running",
                pid=42,
                started_at="2026-05-10T00:00:00+00:00",
                cost_usd=1.5,
            )
            await db.runs.create(
                conn,
                id="run-b",
                issue_id="iss-2",
                stage="review",
                status="completed",
                pid=None,
                started_at="2026-05-09T00:00:00+00:00",
                cost_usd=0.25,
            )
            await db.comment_cursors.set(conn, "iss-1", "2026-05-10T00:30:00+00:00")
        finally:
            await conn.close()

    asyncio.run(_do())


def test_runs_ls_lists_runs_from_sqlite(tmp_path: Path) -> None:
    p = tmp_path / "state.sqlite"
    _populate(p)
    result = CliRunner().invoke(main, ["runs", "ls", "--db", str(p)])
    assert result.exit_code == 0, result.output
    out = result.output
    # id, issue identifier, stage, status, cost, started-at — per acceptance.
    assert "run-a" in out
    assert "run-b" in out
    assert "ENG-1" in out
    assert "ENG-2" in out
    assert "implement" in out
    assert "review" in out
    assert "running" in out
    assert "completed" in out
    assert "1.5" in out
    assert "2026-05-10T00:00:00+00:00" in out


def test_runs_show_displays_full_detail(tmp_path: Path) -> None:
    p = tmp_path / "state.sqlite"
    _populate(p)
    result = CliRunner().invoke(main, ["runs", "show", "run-a", "--db", str(p)])
    assert result.exit_code == 0, result.output
    out = result.output
    assert "run-a" in out
    assert "ENG-1" in out
    assert "implement" in out
    assert "running" in out
    assert "1.5" in out
    # The comment cursor for the issue is part of the detail surface.
    assert "2026-05-10T00:30:00+00:00" in out


def test_runs_show_unknown_id_errors(tmp_path: Path) -> None:
    p = tmp_path / "state.sqlite"
    _populate(p)
    result = CliRunner().invoke(main, ["runs", "show", "nope", "--db", str(p)])
    assert result.exit_code != 0
    # The error must name the unknown id so the operator knows what was missed.
    assert "nope" in result.output


# --- dispatch --------------------------------------------------------------


class _FakeLinear:
    def __init__(self, issue: LinearIssue | None) -> None:
        self.issue = issue
        self.posted: list[tuple[str, str]] = []

    async def __aenter__(self) -> _FakeLinear:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def lookup_issue(self, ident: str) -> LinearIssue:
        if self.issue is None:
            from symphony.linear.client import LinearError

            raise LinearError(f"not found: {ident}")
        return self.issue

    async def post_comment(self, issue_uuid: str, body: str) -> str:
        self.posted.append((issue_uuid, body))
        return "cmt-1"


def _yaml(team: str, db_path: Path) -> str:
    return f"""
db_path: {db_path}
repos:
  - linear_team_key: {team}
    github_repo: org/api-svc
    linear_states:
      ready: Todo
      in_progress: In Progress
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
"""


def _install_fake_linear(monkeypatch, fake: _FakeLinear) -> None:  # type: ignore[no-untyped-def]
    def _factory(_api_key: str) -> _FakeLinear:
        return fake

    monkeypatch.setattr("symphony.cli.Linear", _factory)


def test_dispatch_creates_run_for_known_team_binding(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    db_path = tmp_path / "state.sqlite"
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(_yaml(team="ENG", db_path=db_path))

    issue = LinearIssue(
        id="iss-1",
        identifier="ENG-42",
        title="hand launch",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
    )
    fake = _FakeLinear(issue)
    _install_fake_linear(monkeypatch, fake)

    result = CliRunner().invoke(
        main, ["dispatch", "ENG-42", "--config", str(cfg_path)]
    )
    assert result.exit_code == 0, result.output

    async def _check() -> None:
        conn = await db.connect(db_path)
        try:
            assert await db.runs.has_active(conn, "iss-1") is True
        finally:
            await conn.close()

    asyncio.run(_check())
    assert len(fake.posted) == 1, "dispatch should announce on Linear"


def test_dispatch_errors_when_no_binding_matches_team_key(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    db_path = tmp_path / "state.sqlite"
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(_yaml(team="ENG", db_path=db_path))

    issue = LinearIssue(
        id="iss-2",
        identifier="WEB-99",
        title="from another team",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="WEB",
    )
    fake = _FakeLinear(issue)
    _install_fake_linear(monkeypatch, fake)

    result = CliRunner().invoke(
        main, ["dispatch", "WEB-99", "--config", str(cfg_path)]
    )
    assert result.exit_code != 0
    # Error must name the unmatched team key so the operator can fix the config.
    assert "WEB" in result.output
    # And no run row should have been written.
    async def _check() -> None:
        conn = await db.connect(db_path)
        try:
            assert await db.runs.has_active(conn, "iss-2") is False
        finally:
            await conn.close()

    asyncio.run(_check())
