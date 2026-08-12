"""Codex CLI argv tests."""

from __future__ import annotations

from pathlib import Path

from symphony.agent.codex_cli import build_codex_workspace_write_command


def test_build_codex_workspace_write_command_bypasses_nested_sandbox(tmp_path: Path) -> None:
    argv = build_codex_workspace_write_command(
        prompt="fix this",
        codex_model="gpt-5.1-codex",
        workspace_path=tmp_path,
    )

    # No nested OS sandbox (bwrap can't init in our container); the container is
    # the boundary. The bypass flag supersedes the permissions/approval knobs.
    assert argv[:4] == ["codex", "exec", "--json", "--dangerously-bypass-approvals-and-sandbox"]
    assert "--sandbox" not in argv
    assert "workspace-write" not in argv
    # Unset effort → no --config at all (Codex CLI default effort).
    assert "--config" not in argv
    assert argv[argv.index("--model") + 1] == "gpt-5.1-codex"
    assert argv[argv.index("--cd") + 1] == str(tmp_path)
    assert argv[-1] == "fix this"


def test_build_codex_workspace_write_command_carries_effort(tmp_path: Path) -> None:
    argv = build_codex_workspace_write_command(
        prompt="fix this",
        codex_model="gpt-5.1-codex",
        effort="high",
        workspace_path=tmp_path,
    )

    assert "--dangerously-bypass-approvals-and-sandbox" in argv
    # Effort is the only remaining --config knob.
    configs = [argv[i + 1] for i, arg in enumerate(argv) if arg == "--config"]
    assert configs == ['model_reasoning_effort="high"']
    assert argv[argv.index("--model") + 1] == "gpt-5.1-codex"
    assert argv[-1] == "fix this"


def test_build_codex_workspace_write_command_escapes_effort_as_toml_string(
    tmp_path: Path,
) -> None:
    argv = build_codex_workspace_write_command(
        prompt="fix this",
        codex_model="gpt-future",
        effort='high"\nmodel="injected',
        workspace_path=tmp_path,
    )

    assert argv[argv.index("--config") + 1] == (
        'model_reasoning_effort="high\\"\\nmodel=\\"injected"'
    )
