"""Acceptance runner/classifier behavior."""

from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.agent.runners import acceptance as acceptance_module
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
    AcceptanceCriterionResult,
    AcceptanceScreenshot,
    AcceptanceVerdict,
    acceptance_classifier,
    extract_acceptance_criteria,
    format_acceptance_verdict_comment,
)


class _ScriptedRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.captured_spec: RunnerSpec | None = None
        self.killed_run_ids: list[str] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        self.killed_run_ids.append(run_id)


class _SlowRunner(_ScriptedRunner):
    def __init__(self, *, delay_secs: float, events: list[RunnerEvent]) -> None:
        super().__init__(events)
        self.delay_secs = delay_secs

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        yield RunnerEvent(kind="started", pid=1234)
        await asyncio.sleep(self.delay_secs)
        for ev in self.events:
            yield ev


class _CloseTrackingIterator:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.index = 0
        self.closed = False

    def __aiter__(self) -> _CloseTrackingIterator:
        return self

    async def __anext__(self) -> RunnerEvent:
        if self.index >= len(self.events):
            raise StopAsyncIteration
        event = self.events[self.index]
        self.index += 1
        return event

    async def aclose(self) -> None:
        self.closed = True


class _CloseTrackingRunner(_ScriptedRunner):
    def __init__(self, events: list[RunnerEvent]) -> None:
        super().__init__(events)
        self.iterator = _CloseTrackingIterator(events)

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self.iterator


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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _dev_artifact_result(
    *,
    verdict_footer: str = ACCEPTANCE_FOOTER_PASS,
    preview_url: str,
    hero_path: str | None = None,
    criteria: list[dict[str, object]] | None = None,
) -> str:
    payload: dict[str, object] = {"preview_url": preview_url}
    if hero_path is not None:
        payload["hero_screenshot"] = hero_path
    if criteria is not None:
        payload["criteria"] = criteria
    return _claude_result(
        "Visual acceptance completed.\n\n"
        "<!-- symphony-acceptance-artifacts\n"
        f"{json.dumps(payload)}\n"
        "-->\n\n"
        f"{verdict_footer}",
        cost=0.12,
    )


def test_acceptance_classifier_requires_boolean_artifact_passed() -> None:
    failed_screenshot = ".symphony/acceptance/acceptance-1/failed.png"
    transcript = _dev_artifact_result(
        preview_url="http://127.0.0.1:3000",
        hero_path=".symphony/acceptance/acceptance-1/hero.png",
        criteria=[
            {
                "criterion": "toolbar has settings icon",
                "passed": "false",
                "screenshot": failed_screenshot,
            }
        ],
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict.criterion_results == ()
    assert not any(item.path == failed_screenshot for item in verdict.screenshots)


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
                f"Could not inspect the app.\n\n{ACCEPTANCE_FOOTER_REJECT}",
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
                f"The implementation still returns 500.\n\n{ACCEPTANCE_FOOTER_REJECT}",
                cost=0.09,
            ),
        ]
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict.kind == "reject"
    assert "returns 500" in verdict.details


def test_acceptance_classifier_treats_runner_cap_signals_as_infra_error() -> None:
    transcript = json.dumps(
        {
            "type": "result",
            "subtype": "error_timeout",
            "result": "time cap exceeded",
            "total_cost_usd": 0.12,
        }
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict.kind == "infra_error"
    assert verdict.cost == pytest.approx(0.12)
    assert "time cap exceeded" in verdict.details


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
        "- **README typo is fixed**: not checked because acceptance was skipped as trivial." in body
    )
    assert "included in the overall acceptance review" not in body
    assert "symphony-acceptance-verdict: pass reason=quick_skip_trivial" in body


def test_acceptance_classifier_ignores_raw_prompt_footer_examples() -> None:
    transcript = "\n".join(
        [
            json.dumps({"type": "system", "subtype": "init"}),
            "Write a short rationale. End your final message with EXACTLY ONE of these footers:",
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


def test_acceptance_criteria_extraction_folds_nested_list_items() -> None:
    description = (
        "Ship acceptance checks.\n\n"
        "## Acceptance criteria\n\n"
        "- [ ] Criteria extraction is published before checking:\n"
        "  - [ ] Comment is posted before the verdict.\n"
        "  - Extracted JSON is stored in acceptance_state.\n"
        "- [ ] Verdict references criteria by name.\n"
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "Criteria extraction is published before checking",
            "predicate": (
                "Criteria extraction is published before checking: Comment is "
                "posted before the verdict. Extracted JSON is stored in "
                "acceptance_state."
            ),
        },
        {
            "name": "Verdict references criteria by name",
            "predicate": "Verdict references criteria by name.",
        },
    ]


def test_acceptance_criteria_extraction_folds_lazy_continuation_lines() -> None:
    description = (
        "Ship acceptance checks.\n\n"
        "## Acceptance criteria\n\n"
        "- [ ] Criteria extraction is published before checking\n"
        "before the verdict comment is posted.\n\n"
        "Operator notes stay out of the criterion.\n"
        "- [ ] Verdict references criteria by name.\n"
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "Criteria extraction is published before checking",
            "predicate": (
                "Criteria extraction is published before checking before the "
                "verdict comment is posted."
            ),
        },
        {
            "name": "Verdict references criteria by name",
            "predicate": "Verdict references criteria by name.",
        },
    ]


def test_acceptance_criteria_extraction_preserves_leading_hyphen_text() -> None:
    description = (
        "Ship validation.\n\n## Acceptance criteria\n\n- [ ] `-1` remains a valid input.\n"
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "-1 remains a valid input",
            "predicate": "-1 remains a valid input.",
        },
    ]


def test_acceptance_criteria_extraction_accepts_setext_headings() -> None:
    description = (
        "Ship acceptance checks.\n\n"
        "Acceptance criteria\n"
        "---\n\n"
        "- [ ] Criteria are published first.\n\n"
        "Out of scope\n"
        "---\n\n"
        "- [ ] Per-criterion screenshots are included.\n"
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "Criteria are published first",
            "predicate": "Criteria are published first.",
        },
    ]


def test_acceptance_criteria_extraction_accepts_heading_suffix_text() -> None:
    description = (
        "Ship acceptance checks.\n\n"
        "## Acceptance criteria (must pass)\n\n"
        "- [ ] Criteria are published first.\n\n"
        "## Checklist for release\n\n"
        "- [ ] Verdict references criteria by name.\n"
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "Criteria are published first",
            "predicate": "Criteria are published first.",
        },
        {
            "name": "Verdict references criteria by name",
            "predicate": "Verdict references criteria by name.",
        },
    ]


def test_acceptance_criteria_extraction_ignores_indented_code_headings() -> None:
    description = (
        "Ship OAuth.\n\n"
        "    ## Acceptance criteria\n\n"
        "    - [ ] This checklist is a code sample.\n\n"
        "## Acceptance criteria\n\n"
        "- [ ] OAuth login is implemented.\n"
    )

    assert extract_acceptance_criteria(description) == [
        {
            "name": "OAuth login is implemented",
            "predicate": "OAuth login is implemented.",
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
    assert "- **OAuth login is implemented**: included in the overall acceptance review." in body
    assert "- **Existing sessions still load**: included in the overall acceptance review." in body
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


def test_acceptance_verdict_comment_embeds_dev_mode_screenshots() -> None:
    body = format_acceptance_verdict_comment(
        verdict=AcceptanceVerdict(
            kind="reject",
            criteria=["OAuth login is implemented", "Existing sessions still load"],
            cost=0.12,
            hero_screenshot_url="",
            details="OAuth button is missing.",
            screenshots=(
                AcceptanceScreenshot(
                    kind="criterion",
                    label="OAuth login is implemented",
                    path=".symphony/acceptance/oauth.png",
                    url="https://uploads.linear.app/oauth.png",
                ),
            ),
            criterion_results=(
                AcceptanceCriterionResult(
                    criterion="OAuth login is implemented",
                    passed=False,
                    screenshot_path=".symphony/acceptance/oauth.png",
                    screenshot_url="https://uploads.linear.app/oauth.png",
                ),
                AcceptanceCriterionResult(
                    criterion="Existing sessions still load",
                    passed=True,
                ),
            ),
        ),
        pr_url="https://github.example/pr/1",
    )

    assert "- ❌ **OAuth login is implemented**" in body
    assert "![OAuth login is implemented](https://uploads.linear.app/oauth.png)" in body
    assert "- ✅ **Existing sessions still load**" in body


def test_acceptance_verdict_comment_marks_missing_criterion_results_unreported() -> None:
    body = format_acceptance_verdict_comment(
        verdict=AcceptanceVerdict(
            kind="pass",
            criteria=["OAuth login is implemented", "Existing sessions still load"],
            cost=0.12,
            hero_screenshot_url="",
            details="OAuth button works.",
            criterion_results=(
                AcceptanceCriterionResult(
                    criterion="OAuth login is implemented",
                    passed=True,
                ),
            ),
        ),
        pr_url="https://github.example/pr/1",
    )

    assert "- ✅ **OAuth login is implemented**: verified." in body
    assert "- **Existing sessions still load**: not reported by the acceptance agent." in body
    assert "- ✅ **Existing sessions still load**" not in body


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
                    f"The patch implements the requested icon.\n\n{ACCEPTANCE_FOOTER_PASS}",
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
    assert "--max-budget-usd" not in runner.captured_spec.command
    prompt = runner.captured_spec.command[-1]
    assert "Add a settings icon to the toolbar." in prompt
    assert "diff --git a/ui.py b/ui.py" in prompt
    assert "mode: code_only" in prompt
    assert "Do not run Playwright" in prompt
    assert "Do not inspect screenshots" in prompt


@pytest.mark.asyncio
async def test_dev_acceptance_launches_dev_server_and_enables_playwright_mcp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _free_port()
    preview_url = f"http://127.0.0.1:{port}"
    screenshot = ".symphony/acceptance/acceptance-1/hero.png"
    stopped_servers: list[object] = []

    async def fake_port_reachable(_host: str, _port: int) -> bool:
        return False

    async def fake_start_dev_server(**_kwargs: object) -> object:
        return acceptance_module._DevServer()  # noqa: SLF001

    async def fake_stop_dev_server(server: object) -> None:
        stopped_servers.append(server)

    monkeypatch.setattr(acceptance_module, "_port_reachable", fake_port_reachable)
    monkeypatch.setattr(acceptance_module, "_start_dev_server", fake_start_dev_server)
    monkeypatch.setattr(acceptance_module, "_stop_dev_server", fake_stop_dev_server)
    runner = _ScriptedRunner(
        [
            RunnerEvent(kind="started", pid=1234),
            RunnerEvent(
                kind="stdout",
                line=_dev_artifact_result(
                    preview_url=preview_url,
                    hero_path=screenshot,
                    criteria=[
                        {
                            "criterion": "toolbar has settings icon",
                            "passed": True,
                        }
                    ],
                ),
            ),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="dev",
        linear_description="Add a settings icon to the toolbar.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
        criteria=["toolbar has settings icon"],
        stall_secs=5,
        preview_url=preview_url,
        dev_command="npm run dev",
        dev_port=port,
        dev_startup_timeout_secs=5,
    )

    assert verdict.kind == "pass"
    assert verdict.preview_url == preview_url
    assert verdict.screenshots == (
        AcceptanceScreenshot(kind="hero", label="Primary verified view", path=screenshot),
    )
    assert runner.captured_spec is not None
    command = runner.captured_spec.command
    assert "--mcp-config" in command
    assert "--strict-mcp-config" in command
    mcp_config = Path(command[command.index("--mcp-config") + 1])
    mcp_config_text = await asyncio.to_thread(mcp_config.read_text, encoding="utf-8")
    assert "@playwright/mcp@latest" in mcp_config_text
    assert "--headless" in mcp_config_text
    assert "--isolated" in mcp_config_text
    assert "--output-dir" in mcp_config_text
    prompt = command[-1]
    assert preview_url in prompt
    assert "Capture exactly one hero screenshot" in prompt
    assert "symphony-acceptance-artifacts" in prompt
    assert len(stopped_servers) == 1


@pytest.mark.asyncio
async def test_dev_acceptance_stops_dev_server_when_setup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _free_port()
    preview_url = f"http://127.0.0.1:{port}"
    stopped_servers: list[object] = []

    async def fake_port_reachable(_host: str, _port: int) -> bool:
        return False

    async def fake_start_dev_server(**_kwargs: object) -> object:
        return acceptance_module._DevServer()  # noqa: SLF001

    async def fake_stop_dev_server(server: object) -> None:
        stopped_servers.append(server)

    def fail_write_mcp_config(**_kwargs: object) -> Path:
        raise RuntimeError("mcp config boom")

    monkeypatch.setattr(acceptance_module, "_port_reachable", fake_port_reachable)
    monkeypatch.setattr(acceptance_module, "_start_dev_server", fake_start_dev_server)
    monkeypatch.setattr(acceptance_module, "_stop_dev_server", fake_stop_dev_server)
    monkeypatch.setattr(
        acceptance_module,
        "_write_playwright_mcp_config",
        fail_write_mcp_config,
    )
    runner = _ScriptedRunner([])

    with pytest.raises(RuntimeError, match="mcp config boom"):
        await run_acceptance(
            runner=runner,
            run_id="acceptance-1",
            workspace_path=tmp_path,
            mode="dev",
            linear_description="Add a settings icon to the toolbar.",
            pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
            criteria=["toolbar has settings icon"],
            stall_secs=5,
            preview_url=preview_url,
            dev_command="npm run dev",
            dev_port=port,
            dev_startup_timeout_secs=5,
        )

    assert len(stopped_servers) == 1
    assert runner.captured_spec is None


@pytest.mark.asyncio
async def test_dev_acceptance_pass_requires_reported_criteria(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _free_port()
    preview_url = f"http://127.0.0.1:{port}"
    screenshot = ".symphony/acceptance/acceptance-1/hero.png"
    stopped_servers: list[object] = []

    async def fake_port_reachable(_host: str, _port: int) -> bool:
        return False

    async def fake_start_dev_server(**_kwargs: object) -> object:
        return acceptance_module._DevServer()  # noqa: SLF001

    async def fake_stop_dev_server(server: object) -> None:
        stopped_servers.append(server)

    monkeypatch.setattr(acceptance_module, "_port_reachable", fake_port_reachable)
    monkeypatch.setattr(acceptance_module, "_start_dev_server", fake_start_dev_server)
    monkeypatch.setattr(acceptance_module, "_stop_dev_server", fake_stop_dev_server)
    runner = _ScriptedRunner(
        [
            RunnerEvent(kind="started", pid=1234),
            RunnerEvent(
                kind="stdout",
                line=_dev_artifact_result(
                    preview_url=preview_url,
                    hero_path=screenshot,
                ),
            ),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="dev",
        linear_description="Add a settings icon to the toolbar.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
        criteria=["toolbar has settings icon"],
        stall_secs=5,
        preview_url=preview_url,
        dev_command="npm run dev",
        dev_port=port,
        dev_startup_timeout_secs=5,
    )

    assert verdict.kind == "infra_error"
    assert verdict.criteria == ["toolbar has settings icon"]
    assert "dev acceptance pass must include per-criterion results" in verdict.details
    assert len(stopped_servers) == 1


@pytest.mark.asyncio
async def test_dev_acceptance_pass_with_failed_criterion_is_infra_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _free_port()
    preview_url = f"http://127.0.0.1:{port}"
    hero = ".symphony/acceptance/acceptance-1/hero.png"
    failed = ".symphony/acceptance/acceptance-1/settings-missing.png"
    stopped_servers: list[object] = []

    async def fake_port_reachable(_host: str, _port: int) -> bool:
        return True

    async def fake_start_dev_server(**_kwargs: object) -> object:
        return acceptance_module._DevServer()  # noqa: SLF001

    async def fake_stop_dev_server(server: object) -> None:
        stopped_servers.append(server)

    monkeypatch.setattr(acceptance_module, "_port_reachable", fake_port_reachable)
    monkeypatch.setattr(acceptance_module, "_start_dev_server", fake_start_dev_server)
    monkeypatch.setattr(acceptance_module, "_stop_dev_server", fake_stop_dev_server)
    runner = _ScriptedRunner(
        [
            RunnerEvent(kind="started", pid=1234),
            RunnerEvent(
                kind="stdout",
                line=_dev_artifact_result(
                    preview_url=preview_url,
                    hero_path=hero,
                    criteria=[
                        {
                            "criterion": "toolbar has settings icon",
                            "passed": False,
                            "screenshot": failed,
                        }
                    ],
                ),
            ),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="dev",
        linear_description="Add a settings icon to the toolbar.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
        criteria=["toolbar has settings icon"],
        stall_secs=5,
        preview_url=preview_url,
        dev_command="npm run dev",
        dev_port=port,
        dev_startup_timeout_secs=5,
    )

    assert verdict.kind == "infra_error"
    assert verdict.criteria == ["toolbar has settings icon"]
    assert "dev acceptance pass reported failed criteria" in verdict.details
    assert "toolbar has settings icon" in verdict.details
    assert verdict.preview_url == preview_url
    assert len(stopped_servers) == 1


@pytest.mark.asyncio
async def test_dev_acceptance_uses_fallback_port_when_configured_port_is_busy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    occupied_port = _free_port()
    fallback_port = _free_port()
    while fallback_port == occupied_port:
        fallback_port = _free_port()
    occupied_url = f"http://127.0.0.1:{occupied_port}"
    fallback_url = f"http://127.0.0.1:{fallback_port}"
    screenshot = ".symphony/acceptance/acceptance-1/hero.png"
    start_kwargs: dict[str, object] = {}
    stopped_servers: list[object] = []

    async def fake_port_reachable(_host: str, checked_port: int) -> bool:
        return checked_port == occupied_port

    async def fake_start_dev_server(**kwargs: object) -> object:
        start_kwargs.update(kwargs)
        return acceptance_module._DevServer()  # noqa: SLF001

    async def fake_stop_dev_server(server: object) -> None:
        stopped_servers.append(server)

    monkeypatch.setattr(acceptance_module, "_port_reachable", fake_port_reachable)
    monkeypatch.setattr(acceptance_module, "_unused_dev_port", lambda: fallback_port)
    monkeypatch.setattr(acceptance_module, "_start_dev_server", fake_start_dev_server)
    monkeypatch.setattr(acceptance_module, "_stop_dev_server", fake_stop_dev_server)
    runner = _ScriptedRunner(
        [
            RunnerEvent(kind="started", pid=1234),
            RunnerEvent(
                kind="stdout",
                line=_dev_artifact_result(
                    preview_url=fallback_url,
                    hero_path=screenshot,
                    criteria=[
                        {
                            "criterion": "toolbar has settings icon",
                            "passed": True,
                        }
                    ],
                ),
            ),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="dev",
        linear_description="Add a settings icon to the toolbar.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
        criteria=["toolbar has settings icon"],
        stall_secs=5,
        preview_url=occupied_url,
        dev_command="npm run dev",
        dev_port=occupied_port,
        dev_startup_timeout_secs=5,
    )

    assert verdict.kind == "pass"
    assert verdict.preview_url == fallback_url
    assert start_kwargs["port"] == fallback_port
    assert start_kwargs["preview_url"] == fallback_url
    assert runner.captured_spec is not None
    assert fallback_url in runner.captured_spec.command[-1]
    assert occupied_url not in runner.captured_spec.command[-1]
    assert runner.captured_spec.env["SYMPHONY_ACCEPTANCE_PREVIEW_URL"] == fallback_url
    assert len(stopped_servers) == 1


@pytest.mark.asyncio
async def test_dev_acceptance_startup_timeout_returns_infra_error_without_claude(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = _free_port()
    stopped_servers: list[object] = []

    async def fake_port_reachable(_host: str, _port: int) -> bool:
        return False

    async def fake_start_dev_server(**_kwargs: object) -> object:
        return acceptance_module._DevServer(  # noqa: SLF001
            error_details=(f"dev server did not become reachable on 127.0.0.1:{port} within 0.1s.")
        )

    async def fake_stop_dev_server(server: object) -> None:
        stopped_servers.append(server)

    monkeypatch.setattr(acceptance_module, "_port_reachable", fake_port_reachable)
    monkeypatch.setattr(acceptance_module, "_start_dev_server", fake_start_dev_server)
    monkeypatch.setattr(acceptance_module, "_stop_dev_server", fake_stop_dev_server)
    runner = _ScriptedRunner(
        [
            RunnerEvent(kind="stdout", line=_claude_result("Should not run.")),
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="dev",
        linear_description="Open the dev UI.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
        criteria=["toolbar has settings icon"],
        stall_secs=5,
        preview_url=f"http://127.0.0.1:{port}",
        dev_command="npm run dev",
        dev_port=port,
        dev_startup_timeout_secs=0.05,
    )

    assert verdict.kind == "infra_error"
    assert verdict.criteria == ["toolbar has settings icon"]
    assert "did not become reachable" in verdict.details
    assert runner.captured_spec is None
    assert len(stopped_servers) == 1


@pytest.mark.asyncio
async def test_preview_acceptance_uses_playwright_without_dev_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    preview_url = "https://vib-42.vercel.app"
    screenshot = ".symphony/acceptance/acceptance-1/hero.png"

    async def fail_start_dev_server(**_kwargs: object) -> object:
        raise AssertionError("preview acceptance must not start a dev server")

    monkeypatch.setattr(acceptance_module, "_start_dev_server", fail_start_dev_server)
    runner = _ScriptedRunner(
        [
            RunnerEvent(kind="started", pid=1234),
            RunnerEvent(
                kind="stdout",
                line=_dev_artifact_result(
                    preview_url=preview_url,
                    hero_path=screenshot,
                    criteria=[
                        {
                            "criterion": "toolbar has settings icon",
                            "passed": True,
                        }
                    ],
                ),
            ),
            RunnerEvent(kind="exit", returncode=0),
        ]
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="preview",
        linear_description="Add a settings icon to the toolbar.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
        criteria=["toolbar has settings icon"],
        stall_secs=5,
        preview_url=preview_url,
        dev_command="npm run dev",
        dev_port=3000,
        dev_startup_timeout_secs=5,
    )

    assert verdict.kind == "pass"
    assert verdict.preview_url == preview_url
    assert verdict.screenshots == (
        AcceptanceScreenshot(kind="hero", label="Primary verified view", path=screenshot),
    )
    assert runner.captured_spec is not None
    assert runner.captured_spec.env["SYMPHONY_ACCEPTANCE_PREVIEW_URL"] == preview_url
    command = runner.captured_spec.command
    assert "--mcp-config" in command
    assert "--strict-mcp-config" in command
    assert "mode: preview" in command[-1]
    assert preview_url in command[-1]


@pytest.mark.asyncio
async def test_acceptance_runner_closes_event_stream_after_terminal_event(
    tmp_path: Path,
) -> None:
    runner = _CloseTrackingRunner(
        [
            RunnerEvent(
                kind="stdout",
                line=_claude_result(
                    f"The patch implements the requested icon.\n\n{ACCEPTANCE_FOOTER_PASS}",
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
    )

    assert verdict.kind == "pass"
    assert runner.iterator.closed is True


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
                    f"Should not run.\n\n{ACCEPTANCE_FOOTER_REJECT}",
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
            "diff --git a/app/foo b/bar.py b/app/foo b/bar.py\n-old_helper()\n+new_helper()\n"
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


@pytest.mark.asyncio
async def test_acceptance_runner_aborts_with_infra_error_when_time_cap_reached(
    tmp_path: Path,
) -> None:
    runner = _SlowRunner(
        delay_secs=0.05,
        events=[
            RunnerEvent(
                kind="stdout",
                line=_claude_result(
                    f"Looks good.\n\n{ACCEPTANCE_FOOTER_PASS}",
                    cost=0.02,
                ),
            ),
            RunnerEvent(kind="exit", returncode=0),
        ],
    )

    verdict = await run_acceptance(
        runner=runner,
        run_id="acceptance-1",
        workspace_path=tmp_path,
        mode="code_only",
        linear_description="Add a settings icon to the toolbar.",
        pr_diff_summary="diff --git a/ui.py b/ui.py\n+ add_icon('settings')",
        criteria=["toolbar has settings icon"],
        stall_secs=0.01,
    )

    assert verdict.kind == "infra_error"
    assert verdict.criteria == ["toolbar has settings icon"]
    assert verdict.cost == 0.0
    assert "time_cap_exceeded" in verdict.details
    assert runner.killed_run_ids == ["acceptance-1"]


def test_acceptance_command_disallows_claude_tools_without_budget() -> None:
    command = build_acceptance_command(prompt="judge this")

    assert "--permission-mode" in command
    assert command[command.index("--permission-mode") + 1] == "default"
    assert "--disallowedTools" in command
    disallowed_tools = command[command.index("--disallowedTools") + 1].split(",")
    assert {"Bash", "Read", "Edit", "Write", "MultiEdit"}.issubset(disallowed_tools)
    assert "--strict-mcp-config" in command
    assert "--mcp-config" not in command
    assert "--max-budget-usd" not in command
    assert command[-1] == "judge this"


@pytest.mark.asyncio
async def test_dev_acceptance_requires_dev_command_and_port_without_prompt_runner(
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
    )

    assert verdict.kind == "infra_error"
    assert verdict.criteria == ["dev acceptance works"]
    assert verdict.cost == 0.0
    assert "requires acceptance.dev_command and acceptance.dev_port" in verdict.details
    assert runner.captured_spec is None


def test_preview_acceptance_prompt_requires_visual_inputs() -> None:
    with pytest.raises(ValueError, match="requires preview_url and artifacts_dir"):
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
        (RunnerEvent(kind="stall_timeout"), "time_cap_exceeded"),
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
    )

    assert verdict.kind == "infra_error"
    assert verdict.criteria == ["toolbar has settings icon"]
    assert verdict.cost == pytest.approx(0.33)
    assert expected_details in verdict.details


def test_acceptance_classifier_parses_reject_footer() -> None:
    transcript = _claude_result(
        f"The ticket asks for an icon, but the diff only adds text.\n\n{ACCEPTANCE_FOOTER_REJECT}",
        cost=0.08,
    )

    verdict = acceptance_classifier(transcript=transcript)

    assert verdict.kind == "reject"
    assert verdict.cost == pytest.approx(0.08)
