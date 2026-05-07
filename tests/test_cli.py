import json
from types import SimpleNamespace

from typer.testing import CliRunner

from symphony import __version__
from symphony.cli import app
from symphony.events import EventLog
from symphony.garbage import GcCandidate
from symphony.preflight import PreflightResult
from symphony.types import AgentResult

runner = CliRunner()


def test_version_flag():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_help_lists_agent_run():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "agent-run" in result.output


def test_help_lists_run_once():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "run-once" in result.output


def test_help_lists_run():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    # The long-running orchestrator command, distinct from `run-once`.
    assert "\n run " in result.output or "│ run " in result.output


def test_help_lists_status_and_logs():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "status" in result.output
    assert "logs" in result.output


def test_help_lists_m6_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for name in ("init", "preflight", "cancel", "gc"):
        assert name in result.output


def test_init_writes_starter_files_idempotently(tmp_path):
    result = runner.invoke(app, ["init", "--root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "symphony.toml").is_file()
    assert (tmp_path / "prompts" / "round1.md.j2").is_file()
    assert (tmp_path / "prompts" / "review.md.j2").is_file()
    assert (tmp_path / ".symphony").is_dir()
    assert ".symphony/" in (tmp_path / ".gitignore").read_text()

    (tmp_path / "symphony.toml").write_text("# keep me\n")
    second = runner.invoke(app, ["init", "--root", str(tmp_path)])
    assert second.exit_code == 0, second.output
    assert (tmp_path / "symphony.toml").read_text() == "# keep me\n"


def test_preflight_command_prints_failures_and_exits_nonzero(monkeypatch, tmp_path):
    cfg = SimpleNamespace()
    monkeypatch.setattr("symphony.cli.load_config", lambda p: cfg)
    monkeypatch.setattr(
        "symphony.cli.run_preflight",
        lambda c: [PreflightResult("gh auth", False, "not logged in")],
    )

    result = runner.invoke(app, ["preflight", "--config", str(tmp_path / "x.toml")])

    assert result.exit_code == 1
    assert "FAIL gh auth: not logged in" in result.output


def test_run_refuses_to_start_when_preflight_fails(monkeypatch, tmp_path):
    cfg = SimpleNamespace()
    called = False

    async def fake_run_forever(**kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr("symphony.cli.load_config", lambda p: cfg)
    monkeypatch.setattr(
        "symphony.cli.run_preflight",
        lambda c: [PreflightResult("labels", False, "missing auto")],
    )
    monkeypatch.setattr("symphony.cli.run_forever", fake_run_forever)

    result = runner.invoke(app, ["run", "--config", str(tmp_path / "x.toml")])

    assert result.exit_code == 1
    assert "FAIL labels: missing auto" in result.output
    assert not called


def test_run_starts_after_successful_preflight(monkeypatch, tmp_path):
    cfg = SimpleNamespace(
        github=SimpleNamespace(label="auto"),
        orchestrator=SimpleNamespace(max_concurrent=1, poll_interval_s=60),
    )
    called = False

    async def fake_run_forever(**kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr("symphony.cli.load_config", lambda p: cfg)
    monkeypatch.setattr(
        "symphony.cli.run_preflight",
        lambda c: [PreflightResult("labels", True, "ok")],
    )
    monkeypatch.setattr("symphony.cli.run_forever", fake_run_forever)

    result = runner.invoke(app, ["run", "--config", str(tmp_path / "x.toml")])

    assert result.exit_code == 0, result.output
    assert "OK labels: ok" in result.output
    assert called


def test_cancel_command_requests_cancel(monkeypatch, tmp_path):
    cfg = SimpleNamespace(repo=SimpleNamespace(path=tmp_path))
    captured = {}

    def fake_request_cancel(cfg_arg, issue_number):
        captured["cfg"] = cfg_arg
        captured["issue_number"] = issue_number
        return tmp_path / ".symphony" / "canceled" / "42"

    monkeypatch.setattr("symphony.cli.load_config", lambda p: cfg)
    monkeypatch.setattr("symphony.cli.request_cancel", fake_request_cancel)

    result = runner.invoke(app, ["cancel", "42", "--config", "ignored.toml"])

    assert result.exit_code == 0, result.output
    assert captured == {"cfg": cfg, "issue_number": 42}
    assert "cancel requested for #42" in result.output


def test_gc_lists_candidates_and_defaults_to_no(monkeypatch, tmp_path):
    cfg = SimpleNamespace(repo=SimpleNamespace(path=tmp_path))
    removed = []
    candidate = GcCandidate(42, tmp_path / "repo-42", "auto/42", 20)

    monkeypatch.setattr("symphony.cli.load_config", lambda p: cfg)
    monkeypatch.setattr(
        "symphony.cli.find_gc_candidates",
        lambda cfg_arg, days: [candidate],
    )
    monkeypatch.setattr(
        "symphony.cli.remove_gc_candidate",
        lambda cfg_arg, c: removed.append(c),
    )

    result = runner.invoke(app, ["gc", "--config", "ignored.toml"], input="\n")

    assert result.exit_code == 0, result.output
    assert "#42" in result.output
    assert "Canceled." in result.output
    assert removed == []


def test_gc_removes_after_confirmation(monkeypatch, tmp_path):
    cfg = SimpleNamespace(repo=SimpleNamespace(path=tmp_path))
    removed = []
    candidate = GcCandidate(42, tmp_path / "repo-42", "auto/42", 20)

    monkeypatch.setattr("symphony.cli.load_config", lambda p: cfg)
    monkeypatch.setattr(
        "symphony.cli.find_gc_candidates",
        lambda cfg_arg, days: [candidate],
    )
    monkeypatch.setattr(
        "symphony.cli.remove_gc_candidate",
        lambda cfg_arg, c: removed.append(c),
    )

    result = runner.invoke(app, ["gc", "--config", "ignored.toml"], input="y\n")

    assert result.exit_code == 0, result.output
    assert removed == [candidate]
    assert "removed #42" in result.output


def test_status_reads_event_log(tmp_path, monkeypatch):
    cfg = SimpleNamespace(repo=SimpleNamespace(path=tmp_path))
    monkeypatch.setattr("symphony.cli.load_config", lambda p: cfg)
    log = EventLog.for_repo(tmp_path)
    log.emit("dispatch", issue_number=42, run_id="r", ts=100)
    log.emit(
        "review-verdict",
        issue_number=42,
        run_id="r",
        payload={"head_sha": "abc", "verdict": "pending", "round": 3},
        ts=110,
    )

    result = runner.invoke(app, ["status", "--config", "ignored.toml"])

    assert result.exit_code == 0, result.output
    assert "#42" in result.output
    assert "round=3" in result.output
    assert "last_review_verdict=pending" in result.output


def test_logs_outputs_json_lines_and_filters_issue(tmp_path, monkeypatch):
    cfg = SimpleNamespace(repo=SimpleNamespace(path=tmp_path))
    monkeypatch.setattr("symphony.cli.load_config", lambda p: cfg)
    log = EventLog.for_repo(tmp_path)
    log.emit("dispatch", issue_number=1, run_id="a", ts=1)
    log.emit("dispatch", issue_number=2, run_id="b", ts=2)

    result = runner.invoke(app, ["logs", "--config", "ignored.toml", "--issue", "2"])

    assert result.exit_code == 0, result.output
    rows = [json.loads(line) for line in result.output.splitlines()]
    assert [row["issue_number"] for row in rows] == [2]


def test_run_once_invokes_orchestrator(tmp_path, monkeypatch):
    from symphony.github import PR
    from symphony.reviewer import LoopOutcome, LoopOutcomeKind
    from symphony.runonce import RunOnceResult

    captured: dict = {}

    async def fake_run_once(*, issue_number, config_path):
        captured["issue_number"] = issue_number
        captured["config_path"] = config_path
        return RunOnceResult(
            issue_number=issue_number,
            pr=PR(number=99, url="https://x/pr/99"),
            skipped=False,
            skip_reason=None,
            worktree=tmp_path,
            loop_outcome=LoopOutcome(
                kind=LoopOutcomeKind.APPROVED,
                rounds_used=0,
                last_session_id="sess-A",
                head_sha="abc",
            ),
        )

    monkeypatch.setattr("symphony.cli.run_once", fake_run_once)
    cfg = tmp_path / "symphony.toml"
    cfg.write_text("# stub\n")
    result = runner.invoke(app, ["run-once", "3", "--config", str(cfg)])
    assert result.exit_code == 0, result.output
    assert "https://x/pr/99" in result.output
    assert captured["issue_number"] == 3
    assert captured["config_path"] == cfg


def test_run_once_exits_non_zero_when_auto_stuck(tmp_path, monkeypatch):
    from symphony.github import PR
    from symphony.reviewer import LoopOutcome, LoopOutcomeKind
    from symphony.runonce import RunOnceResult

    async def fake_run_once(*, issue_number, config_path):
        return RunOnceResult(
            issue_number=issue_number,
            pr=PR(number=99, url="https://x/pr/99"),
            skipped=False,
            skip_reason=None,
            worktree=tmp_path,
            loop_outcome=LoopOutcome(
                kind=LoopOutcomeKind.AUTO_STUCK_ROUNDS,
                rounds_used=10,
                last_session_id="sess-A",
                head_sha="abc",
            ),
        )

    monkeypatch.setattr("symphony.cli.run_once", fake_run_once)
    cfg = tmp_path / "symphony.toml"
    cfg.write_text("# stub\n")
    result = runner.invoke(app, ["run-once", "3", "--config", str(cfg)])
    # PR URL still printed; exit code surfaces non-approval terminal state.
    assert "https://x/pr/99" in result.output
    assert result.exit_code == 2


def test_run_once_skipped_exits_nonzero(tmp_path, monkeypatch):
    from symphony.runonce import RunOnceResult

    async def fake_run_once(*, issue_number, config_path):
        return RunOnceResult(
            issue_number=issue_number,
            pr=None,
            skipped=True,
            skip_reason="empty-diff",
            worktree=tmp_path,
        )

    monkeypatch.setattr("symphony.cli.run_once", fake_run_once)
    cfg = tmp_path / "symphony.toml"
    cfg.write_text("# stub\n")
    result = runner.invoke(app, ["run-once", "3", "--config", str(cfg)])
    assert result.exit_code != 0


def test_agent_run_invokes_runner(tmp_path, monkeypatch):
    captured: dict = {}

    async def fake_run_agent(prompt, workdir, **kwargs):
        captured["prompt"] = prompt
        captured["workdir"] = workdir
        captured["kwargs"] = kwargs
        if cb := kwargs.get("on_event"):
            cb({"type": "system", "session_id": "sess-x"})
        return AgentResult(
            session_id="sess-x",
            exit_code=0,
            success=True,
            is_error=False,
            duration_ms=10,
            num_turns=1,
            total_cost_usd=0.0,
            final_text="ok",
            raw_events=[],
            stderr="",
        )

    monkeypatch.setattr("symphony.cli.run_agent", fake_run_agent)
    result = runner.invoke(
        app,
        ["agent-run", "--prompt", "hi there", "--workdir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output
    assert captured["prompt"] == "hi there"
    assert captured["workdir"] == tmp_path


def test_agent_run_failure_propagates_exit_code(tmp_path, monkeypatch):
    async def fake_run_agent(prompt, workdir, **kwargs):
        return AgentResult(
            session_id="sess-x",
            exit_code=2,
            success=False,
            is_error=True,
            duration_ms=10,
            num_turns=1,
            total_cost_usd=0.0,
            final_text=None,
            raw_events=[],
            stderr="bad",
        )

    monkeypatch.setattr("symphony.cli.run_agent", fake_run_agent)
    result = runner.invoke(
        app, ["agent-run", "--prompt", "x", "--workdir", str(tmp_path)]
    )
    assert result.exit_code == 2


def test_agent_run_passes_settings_path(tmp_path, monkeypatch):
    captured: dict = {}

    async def fake_run_agent(prompt, workdir, **kwargs):
        captured["settings_path"] = kwargs.get("settings_path")
        return AgentResult(
            session_id="s",
            exit_code=0,
            success=True,
            is_error=False,
            duration_ms=1,
            num_turns=1,
            total_cost_usd=0.0,
            final_text="ok",
            raw_events=[],
            stderr="",
        )

    monkeypatch.setattr("symphony.cli.run_agent", fake_run_agent)
    settings_file = tmp_path / "settings.json"
    settings_file.write_text("{}")
    result = runner.invoke(
        app,
        [
            "agent-run",
            "--prompt",
            "x",
            "--workdir",
            str(tmp_path),
            "--settings",
            str(settings_file),
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["settings_path"] == settings_file


def test_agent_run_settings_default_none(tmp_path, monkeypatch):
    captured: dict = {}

    async def fake_run_agent(prompt, workdir, **kwargs):
        captured["settings_path"] = kwargs.get("settings_path")
        return AgentResult(
            session_id="s",
            exit_code=0,
            success=True,
            is_error=False,
            duration_ms=1,
            num_turns=1,
            total_cost_usd=0.0,
            final_text="ok",
            raw_events=[],
            stderr="",
        )

    monkeypatch.setattr("symphony.cli.run_agent", fake_run_agent)
    result = runner.invoke(
        app, ["agent-run", "--prompt", "x", "--workdir", str(tmp_path)]
    )
    assert result.exit_code == 0
    assert captured["settings_path"] is None


def test_agent_run_success_clean_exit_zero(tmp_path, monkeypatch):
    async def fake_run_agent(prompt, workdir, **kwargs):
        return AgentResult(
            session_id="s",
            exit_code=0,
            success=True,
            is_error=False,
            duration_ms=1,
            num_turns=1,
            total_cost_usd=0.0,
            final_text="done",
            raw_events=[],
            stderr="",
        )

    monkeypatch.setattr("symphony.cli.run_agent", fake_run_agent)
    result = runner.invoke(
        app, ["agent-run", "--prompt", "x", "--workdir", str(tmp_path)]
    )
    assert result.exit_code == 0
