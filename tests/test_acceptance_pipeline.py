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
    quick_skip_trivial_acceptance,
    run_acceptance,
)
from symphony.pipeline.acceptance_classifier import (
    ACCEPTANCE_FOOTER_INFRA_ERROR,
    ACCEPTANCE_FOOTER_PASS,
    ACCEPTANCE_FOOTER_REJECT,
    AcceptanceVerdict,
    acceptance_classifier,
    extract_acceptance_criteria,
    format_acceptance_verdict_comment,
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


def test_acceptance_classifier_treats_tool_failures_as_infra_error() -> None:
    transcript = "\n".join(
        [
            json.dumps(
                {
                    "type": "user",
                    "message": {
                        "content": [
                            {
                                "type": "tool_result",
                                "is_error": True,
                                "content": "Playwright timeout opening preview",
                            }
                        ]
                    },
                }
            ),
            _claude_result(
                "Could not inspect the app.\n\n"
                f"{ACCEPTANCE_FOOTER_REJECT}",
                cost=0.09,
            ),
        ]
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict.kind == "infra_error"
    assert verdict.cost == pytest.approx(0.09)
    assert "Playwright timeout" in verdict.details


def test_acceptance_classifier_allows_recovered_tool_failure_pass() -> None:
    transcript = "\n".join(
        [
            json.dumps(
                {
                    "type": "user",
                    "message": {
                        "content": [
                            {
                                "type": "tool_result",
                                "is_error": True,
                                "content": "Playwright timeout opening preview",
                            }
                        ]
                    },
                }
            ),
            _claude_result(
                "Retried the preview and the implementation is correct.\n\n"
                f"{ACCEPTANCE_FOOTER_PASS}",
                cost=0.09,
            ),
        ]
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict.kind == "pass"
    assert verdict.cost == pytest.approx(0.09)
    assert "Retried the preview" in verdict.details


def test_acceptance_classifier_preserves_product_reject_after_noninfra_tool_error() -> None:
    transcript = "\n".join(
        [
            json.dumps(
                {
                    "type": "user",
                    "message": {
                        "content": [
                            {
                                "type": "tool_result",
                                "is_error": True,
                                "content": "pytest failed: expected 200 got 500",
                            }
                        ]
                    },
                }
            ),
            _claude_result(
                "The implementation still returns 500.\n\n"
                f"{ACCEPTANCE_FOOTER_REJECT}",
                cost=0.09,
            ),
        ]
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict.kind == "reject"
    assert "returns 500" in verdict.details


@pytest.mark.parametrize(
    ("subtype", "message"),
    [
        ("error_max_budget", "cost cap exceeded"),
        ("error_timeout", "time cap exceeded"),
    ],
)
def test_acceptance_classifier_treats_runner_cap_signals_as_infra_error(
    subtype: str,
    message: str,
) -> None:
    transcript = json.dumps(
        {
            "type": "result",
            "subtype": subtype,
            "result": message,
            "total_cost_usd": 0.12,
        }
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict.kind == "infra_error"
    assert verdict.cost == pytest.approx(0.12)
    assert message in verdict.details


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


def test_acceptance_classifier_parses_quick_skip_reason() -> None:
    transcript = _claude_result(
        "Acceptance skipped - no user-visible behavior described.\n\n"
        "<!-- symphony-acceptance-verdict: pass reason=quick_skip_trivial -->",
        cost=0.02,
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict == AcceptanceVerdict(
        kind="pass",
        criteria=[],
        cost=0.02,
        hero_screenshot_url="",
        details="Acceptance skipped - no user-visible behavior described.",
        reason="quick_skip_trivial",
    )


def test_quick_skip_comment_has_distinct_prefix() -> None:
    verdict = AcceptanceVerdict(
        kind="pass",
        criteria=["README typo is fixed"],
        cost=0.0,
        hero_screenshot_url="",
        details="No user-visible behavior described.",
        reason="quick_skip_trivial",
    )

    body = format_acceptance_verdict_comment(
        verdict=verdict,
        pr_url="https://github.com/org/repo/pull/42",
    )

    assert body.startswith("**Acceptance: skipped - trivial change.**")
    assert "**Acceptance verdict:** `pass`" in body
    assert "Reason: `quick_skip_trivial`" in body
    assert (
        "- **README typo is fixed**: not checked because acceptance was skipped as trivial."
        in body
    )
    assert "included in the overall acceptance review" not in body
    assert "symphony-acceptance-verdict: pass reason=quick_skip_trivial" in body


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


def test_acceptance_criteria_extraction_ignores_offsection_checkboxes() -> None:
    description = (
        "Ship OAuth.\n\n"
        "## Tasks\n\n"
        "- [ ] Coordinate release timing.\n\n"
        "## Acceptance criteria\n\n"
        "- [ ] OAuth login is implemented.\n"
        "- Existing sessions still load.\n\n"
        "## Out of scope\n\n"
        "- [ ] Password reset changes.\n\n"
        "## Checklist\n\n"
        "- [ ] Migration is idempotent."
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "OAuth login is implemented",
            "predicate": "OAuth login is implemented.",
        },
        {
            "name": "Existing sessions still load",
            "predicate": "Existing sessions still load.",
        },
        {
            "name": "Migration is idempotent",
            "predicate": "Migration is idempotent.",
        },
    ]


def test_acceptance_criteria_extraction_keeps_nested_heading_items() -> None:
    description = (
        "Ship OAuth.\n\n"
        "## Acceptance criteria\n\n"
        "### Backend criteria\n\n"
        "- [ ] OAuth login is implemented.\n\n"
        "### Regression coverage\n\n"
        "- Existing sessions still load.\n\n"
        "## Out of scope\n\n"
        "- [ ] Password reset changes."
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "OAuth login is implemented",
            "predicate": "OAuth login is implemented.",
        },
        {
            "name": "Existing sessions still load",
            "predicate": "Existing sessions still load.",
        },
    ]


def test_acceptance_criteria_extraction_skips_nested_noncriteria_sections() -> None:
    description = (
        "Ship OAuth.\n\n"
        "## Acceptance criteria\n\n"
        "### Backend criteria\n\n"
        "- [ ] OAuth login is implemented.\n\n"
        "### Out of scope\n\n"
        "- [ ] Password reset changes.\n\n"
        "#### Notes\n\n"
        "- [ ] Release timing is tracked elsewhere.\n\n"
        "### Regression coverage\n\n"
        "- Existing sessions still load.\n\n"
        "## Where to verify\n\n"
        "- [ ] Staging login flow."
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "OAuth login is implemented",
            "predicate": "OAuth login is implemented.",
        },
        {
            "name": "Existing sessions still load",
            "predicate": "Existing sessions still load.",
        },
    ]


def test_acceptance_criteria_extraction_ignores_negated_criteria_heading() -> None:
    description = (
        "Ship OAuth.\n\n"
        "## Acceptance criteria\n\n"
        "- [ ] OAuth login is implemented.\n\n"
        "## Non-criteria follow-up\n\n"
        "- [ ] Coordinate release timing."
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "OAuth login is implemented",
            "predicate": "OAuth login is implemented.",
        },
    ]


def test_acceptance_verdict_comment_uses_neutral_per_criterion_breakdown() -> None:
    body = format_acceptance_verdict_comment(
        verdict=AcceptanceVerdict(
            kind="reject",
            criteria=["OAuth login is implemented", "Existing sessions still load"],
            cost=0.12,
            hero_screenshot_url="",
            details="Diff only adds docs.",
        ),
        pr_url="https://github.example/pr/1",
    )

    assert "**Acceptance verdict:** `reject`" in body
    assert (
        "- **OAuth login is implemented**: included in the overall acceptance review."
        in body
    )
    assert (
        "- **Existing sessions still load**: included in the overall acceptance review."
        in body
    )
    assert "- **OAuth login is implemented**: `reject`" not in body
    assert "- **Existing sessions still load**: `reject`" not in body


def test_acceptance_verdict_comment_marks_infra_error_criteria_unchecked() -> None:
    body = format_acceptance_verdict_comment(
        verdict=AcceptanceVerdict(
            kind="infra_error",
            criteria=["OAuth login is implemented"],
            cost=0.12,
            hero_screenshot_url="",
            details="Acceptance agent did not emit a verdict footer.",
        ),
        pr_url="https://github.example/pr/1",
    )

    assert "**Acceptance verdict:** `infra_error`" in body
    assert (
        "- **OAuth login is implemented**: not checked because the acceptance run "
        "failed before review completed."
    ) in body
    assert "included in the overall acceptance review" not in body


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


@pytest.mark.asyncio
async def test_acceptance_runner_quick_skips_trivial_readme_typo_without_claude(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(
        [
            RunnerEvent(kind="started", pid=1234),
            RunnerEvent(
                kind="stdout",
                line=_claude_result(
                    "Should not run.\n\n"
                    f"{ACCEPTANCE_FOOTER_REJECT}",
                    cost=0.12,
                ),
            ),
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="code_only",
        linear_description="Fix a typo in README.md.",
        pr_diff_summary=(
            "diff --git a/README.md b/README.md\n"
            "-This pacakge runs Symphony.\n"
            "+This package runs Symphony.\n"
        ),
        criteria=[],
        stall_secs=15,
        max_budget_usd=3.25,
    )

    assert verdict == AcceptanceVerdict(
        kind="pass",
        criteria=[],
        cost=0.0,
        hero_screenshot_url="",
        details="No user-visible behavior described in the ticket or PR diff.",
        reason="quick_skip_trivial",
    )
    assert runner.captured_spec is None


def test_acceptance_quick_skip_requires_only_trivial_ticket_text() -> None:
    verdict = quick_skip_trivial_acceptance(
        linear_description="Fix a README typo and add the OAuth settings screen.",
        pr_diff_summary=(
            "diff --git a/README.md b/README.md\n"
            "-This pacakge runs Symphony.\n"
            "+This package runs Symphony.\n"
        ),
    )

    assert verdict is None


def test_acceptance_quick_skip_preserves_diff_paths_with_spaces() -> None:
    verdict = quick_skip_trivial_acceptance(
        linear_description="Internal refactor only.",
        pr_diff_summary=(
            "diff --git a/ui/login page.tsx b/ui/login page.tsx\n"
            "-const label = 'Sign in';\n"
            "+const label = 'Sign in';\n"
        ),
    )

    assert verdict is None


def test_acceptance_quick_skip_preserves_diff_paths_with_b_directory() -> None:
    verdict = quick_skip_trivial_acceptance(
        linear_description="Internal refactor only.",
        pr_diff_summary=(
            "diff --git a/app/foo b/bar.py b/app/foo b/bar.py\n"
            "-old_helper()\n"
            "+new_helper()\n"
        ),
    )

    assert verdict is None


def test_acceptance_prompt_includes_first_phase_quick_skip_contract() -> None:
    prompt = build_acceptance_prompt(
        mode="code_only",
        linear_description="Add a settings icon to the toolbar.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
    )

    assert "First phase: quick-skip decision" in prompt
    assert "trivial / non-trivial" in prompt
    assert "quick_skip_trivial" in prompt
    assert "If in doubt, classify as non-trivial" in prompt


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
