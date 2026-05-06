from typer.testing import CliRunner

from symphony import __version__
from symphony.cli import app
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
