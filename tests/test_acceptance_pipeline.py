"""Acceptance runner/classifier behavior."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.agent.runners.acceptance import (
    build_acceptance_command,
    build_acceptance_prompt,
    run_acceptance,
)
from symphony.pipeline.acceptance_classifier import (
    ACCEPTANCE_FOOTER_INFRA_ERROR,
    ACCEPTANCE_FOOTER_PASS,
    ACCEPTANCE_FOOTER_REJECT,
    AcceptanceVerdict,
    acceptance_classifier,
)


class _ScriptedRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, _run_id: str) -> None:
        return None


def _claude_result(text: str, *, cost: float = 0.0) -> str:
    return json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "result": text,
            "total_cost_usd": cost,
            "usage": {"input_tokens": 100, "output_tokens": 20},
        }
    )


def test_acceptance_classifier_parses_pass_footer_and_cost() -> None:
    transcript = _claude_result(
        f"Diff satisfies the Linear issue.\n\n{ACCEPTANCE_FOOTER_PASS}",
        cost=0.42,
    )

    verdict = acceptance_classifier(
        transcript=transcript,
        criteria=["description matches diff"],
    )

    assert verdict == AcceptanceVerdict(
        kind="pass",
        criteria=["description matches diff"],
        cost=0.42,
        hero_screenshot_url="",
        details="Diff satisfies the Linear issue.",
    )


def test_acceptance_classifier_ignores_raw_prompt_footer_examples() -> None:
    transcript = "\n".join(
        [
            json.dumps({"type": "system", "subtype": "init"}),
            "Write a short rationale. End your final message with EXACTLY ONE "
            "of these footers:",
            ACCEPTANCE_FOOTER_PASS,
            ACCEPTANCE_FOOTER_REJECT,
            json.dumps(
                {
                    "type": "result",
                    "subtype": "success",
                    "total_cost_usd": 0.17,
                }
            ),
        ]
    )

    verdict = acceptance_classifier(
        transcript=transcript,
        criteria=["description matches diff"],
    )

    assert verdict == AcceptanceVerdict(
        kind="infra_error",
        criteria=["description matches diff"],
        cost=0.17,
        hero_screenshot_url="",
        details="Acceptance agent did not emit a final message.",
    )


@pytest.mark.asyncio
async def test_acceptance_runner_invokes_claude_headless_for_code_only(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(
        [
            RunnerEvent(kind="started", pid=1234),
            RunnerEvent(
                kind="stdout",
                line=_claude_result(
                    "The patch implements the requested icon.\n\n"
                    f"{ACCEPTANCE_FOOTER_PASS}",
                    cost=0.12,
                ),
            ),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="code_only",
        linear_description="Add a settings icon to the toolbar.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
        criteria=["toolbar has settings icon"],
        stall_secs=15,
        max_budget_usd=3.25,
    )

    assert verdict.kind == "pass"
    assert verdict.criteria == ["toolbar has settings icon"]
    assert verdict.cost == pytest.approx(0.12)
    assert verdict.hero_screenshot_url == ""

    assert runner.captured_spec is not None
    assert runner.captured_spec.stage == "acceptance"
    assert runner.captured_spec.run_id == "acceptance-1"
    assert runner.captured_spec.workspace_path == tmp_path
    assert runner.captured_spec.command[:5] == [
        "claude",
        "--print",
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    assert "--permission-mode" in runner.captured_spec.command
    permission_mode_idx = runner.captured_spec.command.index("--permission-mode") + 1
    assert runner.captured_spec.command[permission_mode_idx] == "default"
    assert "--disallowedTools" in runner.captured_spec.command
    disallowed_tools_idx = runner.captured_spec.command.index("--disallowedTools") + 1
    disallowed_tools = runner.captured_spec.command[disallowed_tools_idx].split(",")
    assert {"Bash", "Read", "Edit"}.issubset(disallowed_tools)
    assert "--max-budget-usd" in runner.captured_spec.command
    budget_idx = runner.captured_spec.command.index("--max-budget-usd") + 1
    assert runner.captured_spec.command[budget_idx] == "3.2500"
    prompt = runner.captured_spec.command[-1]
    assert "Add a settings icon to the toolbar." in prompt
    assert "diff --git a/ui.py b/ui.py" in prompt
    assert "mode: code_only" in prompt
    assert "Do not run Playwright" in prompt
    assert "Do not inspect screenshots" in prompt


def test_acceptance_command_disallows_claude_tools_without_budget() -> None:
    command = build_acceptance_command(prompt="judge this")

    assert "--permission-mode" in command
    assert command[command.index("--permission-mode") + 1] == "default"
    assert "--disallowedTools" in command
    disallowed_tools = command[command.index("--disallowedTools") + 1].split(",")
    assert {"Bash", "Read", "Edit", "Write", "MultiEdit"}.issubset(
        disallowed_tools
    )
    assert "--max-budget-usd" not in command
    assert command[-1] == "judge this"


@pytest.mark.asyncio
async def test_acceptance_runner_rejects_non_code_only_mode_without_prompt_runner(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(
        [
            RunnerEvent(kind="started", pid=1234),
            RunnerEvent(
                kind="stdout",
                line=_claude_result(
                    f"Should not run.\n\n{ACCEPTANCE_FOOTER_INFRA_ERROR}",
                    cost=0.12,
                ),
            ),
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="dev",
        linear_description="Run the dev acceptance flow.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+run_dev_check()",
        criteria=["dev acceptance works"],
        stall_secs=15,
        max_budget_usd=3.25,
    )

    assert verdict.kind == "infra_error"
    assert verdict.criteria == ["dev acceptance works"]
    assert verdict.cost == 0.0
    assert "Acceptance mode 'dev' is not supported" in verdict.details
    assert runner.captured_spec is None


def test_acceptance_prompt_rejects_non_code_only_mode() -> None:
    with pytest.raises(ValueError, match="Acceptance mode 'preview' is not supported"):
        build_acceptance_prompt(
            mode="preview",
            linear_description="Open the preview.",
            pr_diff_summary="diff --git a/app.py b/app.py",
        )


def test_acceptance_prompt_includes_taste_guide_when_configured() -> None:
    prompt = build_acceptance_prompt(
        mode="code_only",
        linear_description="Add a polished VIB icon.",
        pr_diff_summary="diff --git a/ui.tsx b/ui.tsx\n+<span>Icon</span>",
        taste_guide=(
            "## Principles\n\nGlobal taste.\n\n"
            "## Hard rules (acceptance must reject if violated)\n\n"
            "- Do not render icon names as text.\n\n"
            "## Hard rules (acceptance must reject if violated)\n\n"
            "- Binding-specific icon rule.\n"
        ),
    )

    assert "# Taste guide" in prompt
    assert prompt.index("Global taste.") < prompt.index("Binding-specific icon rule.")
    assert "- Do not render icon names as text." in prompt
    assert "reject and cite the specific taste-guide rule" in prompt
    assert "cannot override or silence global hard rules" in prompt


def test_acceptance_prompt_works_without_taste_guide_section() -> None:
    prompt = build_acceptance_prompt(
        mode="code_only",
        linear_description="Add a settings icon.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+add_icon()",
    )

    assert "# Linear description" in prompt
    assert "# Taste guide" not in prompt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("terminal", "expected_details"),
    [
        (RunnerEvent(kind="exit", returncode=2), "exited rc=2"),
        (RunnerEvent(kind="stall_timeout"), "stalled"),
        (
            RunnerEvent(kind="spawn_failed", error="FileNotFoundError: claude"),
            "spawn_failed: FileNotFoundError: claude",
        ),
    ],
)
async def test_acceptance_runner_fails_when_claude_does_not_complete_successfully(
    tmp_path: Path,
    terminal: RunnerEvent,
    expected_details: str,
) -> None:
    runner = _ScriptedRunner(
        [
            RunnerEvent(
                kind="stdout",
                line=_claude_result(
                    f"Looks good.\n\n{ACCEPTANCE_FOOTER_PASS}",
                    cost=0.33,
                ),
            ),
            terminal,
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="code_only",
        linear_description="Add a settings icon to the toolbar.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
        criteria=["toolbar has settings icon"],
        stall_secs=15,
        max_budget_usd=3.25,
    )

    assert verdict.kind == "infra_error"
    assert verdict.criteria == ["toolbar has settings icon"]
    assert verdict.cost == pytest.approx(0.33)
    assert expected_details in verdict.details


def test_acceptance_classifier_parses_reject_footer() -> None:
    transcript = _claude_result(
        "The ticket asks for an icon, but the diff only adds text.\n\n"
        f"{ACCEPTANCE_FOOTER_REJECT}",
        cost=0.08,
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict.kind == "reject"
    assert verdict.cost == pytest.approx(0.08)
