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


def _yaml_two_bindings(team: str, db_path: Path) -> str:
    """One Linear team fanned out to two repos via labels — the config
    shape that exposed the dispatch label-matching bug."""
    return f"""
db_path: {db_path}
repos:
  - linear_team_key: {team}
    github_repo: org/api-svc
    issue_label: api
    linear_states:
      ready: Todo
      in_progress: In Progress
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
  - linear_team_key: {team}
    github_repo: org/web-app
    issue_label: web
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


def test_runs_ls_rejects_non_positive_limit(tmp_path: Path) -> None:
    """SQLite treats a negative `LIMIT` as unbounded, so `--limit -1` would
    print the entire `runs` table — the opposite of what `--limit` advertises.
    Click must reject non-positive values up front."""
    p = tmp_path / "state.sqlite"
    _populate(p)
    for bad in ("-1", "0"):
        result = CliRunner().invoke(main, ["runs", "ls", "--db", str(p), "--limit", bad])
        assert result.exit_code != 0, f"--limit {bad} must be rejected"


def test_runs_ls_rejects_directory_for_db_path(tmp_path: Path) -> None:
    """`--db` pointing at a directory must fail at click validation, not later
    when SQLite tries to open it as a database file. This keeps the operator
    error message actionable instead of `unable to open database file`."""
    a_dir = tmp_path / "not_a_db_dir"
    a_dir.mkdir()
    result = CliRunner().invoke(main, ["runs", "ls", "--db", str(a_dir)])
    assert result.exit_code != 0
    # Click's standard message for dir_okay=False mentions "directory".
    assert "directory" in result.output.lower()


def test_dispatch_picks_binding_by_issue_label(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """When one Linear team is fanned out to multiple repos via `issue_label`,
    the CLI must pick the repo whose label is on the issue — selecting by
    `linear_team_key` alone routes the run to the wrong repo and posts the
    start comment in the wrong context."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    db_path = tmp_path / "state.sqlite"
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(_yaml_two_bindings(team="ENG", db_path=db_path))

    issue = LinearIssue(
        id="iss-web",
        identifier="ENG-100",
        title="web work",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["web"],
    )
    fake = _FakeLinear(issue)
    _install_fake_linear(monkeypatch, fake)

    result = CliRunner().invoke(
        main, ["dispatch", "ENG-100", "--config", str(cfg_path)]
    )
    assert result.exit_code == 0, result.output
    # The success message names the repo, so it doubles as a routing assertion.
    assert "org/web-app" in result.output
    assert "org/api-svc" not in result.output


def test_dispatch_errors_when_no_binding_label_matches(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """If every binding for the team is label-scoped and none of the issue's
    labels match, dispatch must refuse — silently picking an arbitrary
    binding would route the run to a repo that isn't supposed to handle it.
    """
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    db_path = tmp_path / "state.sqlite"
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(_yaml_two_bindings(team="ENG", db_path=db_path))

    issue = LinearIssue(
        id="iss-other",
        identifier="ENG-101",
        title="unlabeled",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["mobile"],  # not 'api', not 'web'
    )
    fake = _FakeLinear(issue)
    _install_fake_linear(monkeypatch, fake)

    result = CliRunner().invoke(
        main, ["dispatch", "ENG-101", "--config", str(cfg_path)]
    )
    assert result.exit_code != 0, result.output
    assert "ENG-101" in result.output


def test_dispatch_rejects_directory_for_config_path(tmp_path: Path) -> None:
    """Same fail-fast contract as `--db`: passing a directory must fail at
    click validation, not deeper inside `Config.load()` with `IsADirectoryError`."""
    a_dir = tmp_path / "not_a_config_dir"
    a_dir.mkdir()
    result = CliRunner().invoke(main, ["dispatch", "ENG-1", "--config", str(a_dir)])
    assert result.exit_code != 0
    assert "directory" in result.output.lower()


def test_dispatch_refuses_when_issue_already_has_active_run(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """`dispatch` must respect the same dedupe oracle as the poll loop;
    otherwise an operator retrying it for an issue that's already mid-run
    creates a duplicate `running` row and a duplicate "starting" comment."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    db_path = tmp_path / "state.sqlite"
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(_yaml(team="ENG", db_path=db_path))

    issue = LinearIssue(
        id="iss-existing",
        identifier="ENG-50",
        title="already running",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
    )

    async def _seed_active_run() -> None:
        conn = await db.connect(db_path)
        try:
            await db.issues.upsert(
                conn,
                id=issue.id,
                identifier=issue.identifier,
                title=issue.title,
                team_key=issue.team_key,
            )
            await db.runs.create(
                conn,
                id="run-existing",
                issue_id=issue.id,
                stage="implement",
                status="running",
                pid=999,
                started_at="2026-05-10T00:00:00+00:00",
            )
        finally:
            await conn.close()

    asyncio.run(_seed_active_run())

    fake = _FakeLinear(issue)
    _install_fake_linear(monkeypatch, fake)

    result = CliRunner().invoke(
        main, ["dispatch", "ENG-50", "--config", str(cfg_path)]
    )
    assert result.exit_code != 0, result.output
    assert "ENG-50" in result.output
    # No new comment posted (the existing run already announced).
    assert fake.posted == []

    # And no new run row was created.
    async def _check() -> None:
        conn = await db.connect(db_path)
        try:
            cur = await conn.execute(
                "SELECT COUNT(*) FROM runs WHERE issue_id = ?", (issue.id,)
            )
            (count,) = await cur.fetchone()  # type: ignore[misc]
            assert count == 1
        finally:
            await conn.close()

    asyncio.run(_check())


def test_dispatch_errors_when_announce_comment_fails(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """If the Linear announce comment raises, the run row is flipped to
    `failed` and the CLI must exit non-zero — otherwise an operator running
    `dispatch` during a Linear outage sees a green success message while no
    live run actually started."""
    from symphony.linear.client import LinearError

    monkeypatch.setenv("LINEAR_API_KEY", "x")
    db_path = tmp_path / "state.sqlite"
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(_yaml(team="ENG", db_path=db_path))

    issue = LinearIssue(
        id="iss-3",
        identifier="ENG-77",
        title="hand launch with broken linear",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
    )

    class _BrokenAnnounce(_FakeLinear):
        async def post_comment(self, issue_uuid: str, body: str) -> str:
            raise LinearError("linear is down")

    fake = _BrokenAnnounce(issue)
    _install_fake_linear(monkeypatch, fake)

    result = CliRunner().invoke(
        main, ["dispatch", "ENG-77", "--config", str(cfg_path)]
    )
    assert result.exit_code != 0, result.output
    # The error must be on stderr/output and identify the issue so the
    # operator knows which dispatch did not actually start.
    assert "ENG-77" in result.output

    async def _check() -> None:
        conn = await db.connect(db_path)
        try:
            # Row exists but is `failed`, so dedupe will not block a retry.
            assert await db.runs.has_active(conn, "iss-3") is False
        finally:
            await conn.close()

    asyncio.run(_check())


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
