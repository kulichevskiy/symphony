"""`symphony local-review-dry-run` CLI subcommand."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

from click.testing import CliRunner

from symphony import cli as cli_module
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.cli import main
from symphony.pipeline.local_review import (
    VERDICT_APPROVED_MARKER,
    VERDICT_CHANGES_REQUESTED_MARKER,
)


class _CapturedSpec:
    """Holds the spec passed to the runner so tests can assert argv."""

    spec: RunnerSpec | None = None


def _fake_runner_factory(captured: _CapturedSpec, final_text: str):
    """Build a Runner that records the spec and returns one agent_message
    event followed by exit. Also populates `-o <file>` if codex argv."""

    class _FakeRunner:
        def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
            captured.spec = spec
            # Mirror real codex: write the final agent message to the
            # `-o <file>` path before yielding stdout, so the reviewer
            # path that prefers the file picks it up.
            if "-o" in spec.command:
                idx = spec.command.index("-o")
                Path(spec.command[idx + 1]).write_text(
                    final_text, encoding="utf-8"
                )

            async def gen() -> AsyncIterator[RunnerEvent]:
                yield RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {
                            "type": "item.completed",
                            "item": {
                                "id": "i",
                                "type": "agent_message",
                                "text": final_text,
                            },
                        }
                    ),
                )
                yield RunnerEvent(kind="exit", returncode=0)

            return gen()

        async def kill(self, run_id: str) -> None:
            pass

    return lambda: _FakeRunner()


def test_dry_run_approved_prints_verdict_and_no_findings(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    workspace = tmp_path / "ws"
    workspace.mkdir()
    captured = _CapturedSpec()
    monkeypatch.setattr(
        cli_module,
        "_DRY_RUN_RUNNER_FACTORY",
        _fake_runner_factory(
            captured, f"looks clean\n{VERDICT_APPROVED_MARKER}"
        ),
    )

    result = CliRunner().invoke(
        main,
        [
            "local-review-dry-run",
            "--workspace",
            str(workspace),
            "--base",
            "main",
            "--reviewer",
            "codex",
            "--title",
            "Test issue",
            "--body",
            "Add a feature",
            "--label",
            "feature",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "verdict: approved" in result.output
    assert "terminal: exit" in result.output
    # The argv that hit the runner is the production argv from
    # `build_local_review_command(agent='codex')`.
    assert captured.spec is not None
    assert captured.spec.command[:2] == ["codex", "exec"]
    assert "--sandbox" in captured.spec.command
    assert "read-only" in captured.spec.command


def test_dry_run_changes_requested_prints_findings(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    workspace = tmp_path / "ws"
    workspace.mkdir()
    captured = _CapturedSpec()
    findings_body = (
        "## Findings\n"
        "- `add.py:6` - missing zero check.\n\n"
        f"{VERDICT_CHANGES_REQUESTED_MARKER}"
    )
    monkeypatch.setattr(
        cli_module,
        "_DRY_RUN_RUNNER_FACTORY",
        _fake_runner_factory(captured, findings_body),
    )

    result = CliRunner().invoke(
        main,
        [
            "local-review-dry-run",
            "--workspace",
            str(workspace),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "verdict: changes_requested" in result.output
    assert "add.py:6" in result.output
    assert "missing zero check" in result.output


def test_dry_run_claude_reviewer_uses_print_argv(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    workspace = tmp_path / "ws"
    workspace.mkdir()
    captured = _CapturedSpec()
    monkeypatch.setattr(
        cli_module,
        "_DRY_RUN_RUNNER_FACTORY",
        _fake_runner_factory(captured, f"ok\n{VERDICT_APPROVED_MARKER}"),
    )

    result = CliRunner().invoke(
        main,
        [
            "local-review-dry-run",
            "--workspace",
            str(workspace),
            "--reviewer",
            "claude",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured.spec is not None
    assert captured.spec.command[0] == "claude"
    assert "--print" in captured.spec.command
    # No `-o` for claude (only codex writes `--output-last-message`).
    assert "-o" not in captured.spec.command


def test_dry_run_codex_without_model_strips_model_flag(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """No `--reviewer-model` → strip `--model` from argv so codex uses
    the operator's account default. Critical: real ChatGPT accounts
    reject the default model literal otherwise (iter 5 lesson)."""
    workspace = tmp_path / "ws"
    workspace.mkdir()
    captured = _CapturedSpec()
    monkeypatch.setattr(
        cli_module,
        "_DRY_RUN_RUNNER_FACTORY",
        _fake_runner_factory(captured, f"ok\n{VERDICT_APPROVED_MARKER}"),
    )

    result = CliRunner().invoke(
        main,
        [
            "local-review-dry-run",
            "--workspace",
            str(workspace),
            "--reviewer",
            "codex",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured.spec is not None
    assert "--model" not in captured.spec.command


def test_dry_run_codex_with_model_keeps_flag(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    workspace = tmp_path / "ws"
    workspace.mkdir()
    captured = _CapturedSpec()
    monkeypatch.setattr(
        cli_module,
        "_DRY_RUN_RUNNER_FACTORY",
        _fake_runner_factory(captured, f"ok\n{VERDICT_APPROVED_MARKER}"),
    )

    result = CliRunner().invoke(
        main,
        [
            "local-review-dry-run",
            "--workspace",
            str(workspace),
            "--reviewer",
            "codex",
            "--reviewer-model",
            "gpt-5.1-codex",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured.spec is not None
    assert "--model" in captured.spec.command
    assert (
        captured.spec.command[captured.spec.command.index("--model") + 1]
        == "gpt-5.1-codex"
    )


def test_dry_run_threads_issue_context_into_prompt(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    workspace = tmp_path / "ws"
    workspace.mkdir()
    captured = _CapturedSpec()
    monkeypatch.setattr(
        cli_module,
        "_DRY_RUN_RUNNER_FACTORY",
        _fake_runner_factory(captured, f"ok\n{VERDICT_APPROVED_MARKER}"),
    )

    result = CliRunner().invoke(
        main,
        [
            "local-review-dry-run",
            "--workspace",
            str(workspace),
            "--title",
            "Add OAuth login",
            "--body",
            "Users should sign in via Google.",
            "--label",
            "feature",
            "--label",
            "auth",
        ],
    )
    assert result.exit_code == 0, result.output
    prompt = captured.spec.command[-1]  # type: ignore[union-attr]
    assert "Add OAuth login" in prompt
    assert "Users should sign in via Google." in prompt
    assert "feature" in prompt
    assert "auth" in prompt


def test_dry_run_unparseable_message_surfaces_raw_text(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    workspace = tmp_path / "ws"
    workspace.mkdir()
    captured = _CapturedSpec()
    monkeypatch.setattr(
        cli_module,
        "_DRY_RUN_RUNNER_FACTORY",
        _fake_runner_factory(
            captured, "I have thoughts but forgot the marker."
        ),
    )

    result = CliRunner().invoke(
        main,
        ["local-review-dry-run", "--workspace", str(workspace)],
    )
    assert result.exit_code == 0, result.output
    assert "verdict: unparseable" in result.output
    # Raw message is printed so the operator can see what went wrong.
    assert "forgot the marker" in result.output
