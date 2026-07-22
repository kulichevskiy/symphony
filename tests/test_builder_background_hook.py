"""SYM-224: hard-block background-task machinery in builder claude runs.

Mutating (builder) claude spawns — implement, review-fix, merge-fix,
acceptance-fix — run one-shot (`claude --print`, no resume). An agent that
defers work behind a self-wakeup or a background task strands the run: the
commit never lands, HEAD stays put, the issue parks in Needs Input.

Enforcement is a PreToolUse deny-hook (deterministic), not a prompt. These
tests cover the hook script's decisions and that every builder command
builder wires it in via --settings/--setting-sources, while the read-only
reviewer/verifier path keeps its disableAllHooks behavior.
"""

from __future__ import annotations

import json
import subprocess
import sys

from symphony.agent.claude_cli import (
    BUILDER_DENY_HOOK_SCRIPT,
    BUILDER_SETTING_SOURCES,
    claude_builder_settings,
)
from symphony.agent.runners.acceptance import build_acceptance_command
from symphony.orchestrator.poll import build_runner_command
from symphony.pipeline.local_review_session import _build_fix_command


def _run_hook(payload: dict) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(BUILDER_DENY_HOOK_SCRIPT)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
    )


# --- hook decisions --------------------------------------------------------


def test_hook_blocks_background_bash() -> None:
    result = _run_hook(
        {"tool_name": "Bash", "tool_input": {"command": "pytest", "run_in_background": True}}
    )
    assert result.returncode == 2
    assert "foreground" in result.stderr.lower()


def test_hook_blocks_scheduled_wakeup() -> None:
    result = _run_hook({"tool_name": "ScheduleWakeup", "tool_input": {"delaySeconds": 120}})
    assert result.returncode == 2
    assert "commit" in result.stderr.lower()


def test_hook_blocks_bash_output_and_kill_shell() -> None:
    for tool in ("BashOutput", "KillShell"):
        result = _run_hook({"tool_name": tool, "tool_input": {}})
        assert result.returncode == 2, tool


def test_hook_allows_foreground_bash() -> None:
    result = _run_hook({"tool_name": "Bash", "tool_input": {"command": "git commit -m x"}})
    assert result.returncode == 0
    assert result.stderr == ""


def test_hook_allows_bash_with_explicit_foreground_flag() -> None:
    result = _run_hook(
        {"tool_name": "Bash", "tool_input": {"command": "pytest", "run_in_background": False}}
    )
    assert result.returncode == 0


def test_hook_allows_edit_and_write() -> None:
    for tool in ("Edit", "Write", "Read"):
        result = _run_hook({"tool_name": tool, "tool_input": {"file_path": "a.py"}})
        assert result.returncode == 0, tool


def test_hook_ignores_malformed_stdin() -> None:
    result = subprocess.run(
        [sys.executable, str(BUILDER_DENY_HOOK_SCRIPT)],
        input="not json",
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


# --- settings payload ------------------------------------------------------


def test_builder_settings_registers_pretooluse_hook() -> None:
    settings = json.loads(claude_builder_settings())
    entries = settings["hooks"]["PreToolUse"]
    commands = [h["command"] for entry in entries for h in entry["hooks"]]
    assert any(str(BUILDER_DENY_HOOK_SCRIPT) in c for c in commands)


def test_builder_settings_does_not_disable_all_hooks() -> None:
    settings = json.loads(claude_builder_settings())
    assert settings.get("disableAllHooks") is not True


# --- wiring into every builder command builder -----------------------------


def _assert_hook_wired(argv: list[str]) -> None:
    assert "--settings" in argv
    settings = json.loads(argv[argv.index("--settings") + 1])
    entries = settings["hooks"]["PreToolUse"]
    commands = [h["command"] for entry in entries for h in entry["hooks"]]
    assert any(str(BUILDER_DENY_HOOK_SCRIPT) in c for c in commands)
    assert "--setting-sources" in argv
    assert argv[argv.index("--setting-sources") + 1] == BUILDER_SETTING_SOURCES


def test_build_runner_command_wires_deny_hook() -> None:
    _assert_hook_wired(build_runner_command("claude", "do it"))


def test_build_fix_command_wires_deny_hook() -> None:
    _assert_hook_wired(_build_fix_command(agent="claude", codex_model="gpt-5.5", prompt="fix it"))


def test_build_acceptance_command_wires_deny_hook() -> None:
    _assert_hook_wired(build_acceptance_command(prompt="verdict please"))


def test_codex_builder_command_has_no_claude_settings() -> None:
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as d:
        argv = build_runner_command("codex", "do it", workspace_path=Path(d))
    assert "--settings" not in argv


# --- reviewer/verifier path unaffected -------------------------------------


def test_reviewer_command_keeps_disable_all_hooks() -> None:
    from symphony.pipeline.local_review import build_local_review_command

    argv = build_local_review_command(
        agent="claude", prompt="review", base_branch="main", pass_two=False
    )
    settings = json.loads(argv[argv.index("--settings") + 1])
    assert settings["disableAllHooks"] is True
