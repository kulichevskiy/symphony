"""Cost accounting for the local-review loop."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from symphony.agent.process import Usage
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import ResolvedRole
from symphony.pipeline.cost_guard import UsageCostEstimator
from symphony.pipeline.local_review import (
    VERDICT_APPROVED_MARKER,
    VERDICT_CHANGES_REQUESTED_MARKER,
    LocalVerdict,
)
from symphony.pipeline.local_review_io import collect_runner_output
from symphony.pipeline.local_review_loop import (
    FixerOutput,
    LoopOutcome,
    ReviewerOutput,
    run_local_review_loop,
)
from symphony.pipeline.local_review_session import run_local_review_session

# --- UsageCostEstimator: now public ----------------------------------


def test_usage_cost_estimator_claude_uses_reported_cost() -> None:
    est = UsageCostEstimator(agent="claude", codex_model="gpt-5.1-codex")
    delta1 = est.delta(
        Usage(
            cost_usd=0.10,
            input_tokens=100,
            output_tokens=20,
            cache_write_tokens=40,
            cache_read_tokens=60,
        )
    )
    delta2 = est.delta(
        Usage(
            cost_usd=0.25,
            input_tokens=300,
            output_tokens=80,
            cache_write_tokens=10,
            cache_read_tokens=20,
        )
    )
    assert delta1.cost_usd == pytest.approx(0.10)
    assert delta1.input_tokens == 100
    assert delta1.output_tokens == 20
    assert delta1.cache_write_tokens == 40
    assert delta1.cache_read_tokens == 60
    assert delta2.cost_usd == pytest.approx(0.25)
    assert delta2.input_tokens == 300
    assert delta2.output_tokens == 80
    assert delta2.cache_write_tokens == 10
    assert delta2.cache_read_tokens == 20
    assert est.total_cost_usd == pytest.approx(0.35)


def test_usage_cost_estimator_codex_charges_token_deltas_only() -> None:
    """Codex emits cumulative tokens per turn; the estimator must
    charge only the *new* tokens on each call so spanning multiple
    iterations doesn't double-bill."""
    est = UsageCostEstimator(agent="codex", codex_model="gpt-5.1-codex")
    delta1 = est.delta(
        Usage(
            cost_usd=0.0,
            input_tokens=1000,
            output_tokens=200,
            cache_read_tokens=100,
        )
    )
    delta2 = est.delta(
        Usage(
            cost_usd=0.0,
            input_tokens=1500,  # +500 new input
            output_tokens=350,  # +150 new output
            cache_read_tokens=250,  # +150 new cached input
        )
    )
    assert delta1.cost_usd > 0
    assert delta1.input_tokens == 1000
    assert delta1.output_tokens == 200
    assert delta1.cache_write_tokens == 0
    assert delta1.cache_read_tokens == 100
    assert delta2.cost_usd > 0
    assert delta2.input_tokens == 500
    assert delta2.output_tokens == 150
    assert delta2.cache_write_tokens == 0
    assert delta2.cache_read_tokens == 150
    # Second call must price the delta, not the cumulative.
    full_run = UsageCostEstimator(agent="codex", codex_model="gpt-5.1-codex")
    delta_full = full_run.delta(
        Usage(
            cost_usd=0.0,
            input_tokens=1500,
            output_tokens=350,
            cache_read_tokens=250,
        )
    )
    assert est.total_cost_usd == pytest.approx(delta_full.cost_usd)


# --- collect_runner_output usage_handler ------------------------------


def _events(*lines: str) -> list[RunnerEvent]:
    out: list[RunnerEvent] = []
    for ln in lines:
        out.append(RunnerEvent(kind="stdout", line=ln))
    out.append(RunnerEvent(kind="exit", returncode=0))
    return out


class _ScriptedRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self._events = events

    def run(self, _spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        async def gen() -> AsyncIterator[RunnerEvent]:
            for ev in self._events:
                yield ev

        return gen()

    async def kill(self, run_id: str) -> None:
        pass


@pytest.mark.asyncio
async def test_collect_runner_output_invokes_usage_handler_on_result_event(
    tmp_path: Path,
) -> None:
    result_event = json.dumps(
        {
            "type": "result",
            "total_cost_usd": 0.42,
            "usage": {"input_tokens": 100, "output_tokens": 50},
        }
    )
    runner = _ScriptedRunner(_events(result_event))
    spec = RunnerSpec(run_id="r1", workspace_path=tmp_path, command=["x"], stall_secs=10)

    captured: list[Usage] = []
    out = await collect_runner_output(runner, spec, usage_handler=captured.append)
    assert out.ok_exit
    assert len(captured) == 1
    assert captured[0].cost_usd == pytest.approx(0.42)
    assert captured[0].input_tokens == 100


@pytest.mark.asyncio
async def test_collect_runner_output_skips_non_usage_lines(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(_events("not-json", json.dumps({"type": "system"})))
    spec = RunnerSpec(run_id="r1", workspace_path=tmp_path, command=["x"], stall_secs=10)
    captured: list[Usage] = []
    await collect_runner_output(runner, spec, usage_handler=captured.append)
    assert captured == []


# --- loop-level cost accumulation -------------------------------------


@dataclass
class _ReviewerScript:
    messages: list[str]
    costs: list[float]
    head_shas: list[str] = field(default_factory=list)
    message_by_call: bool = False
    calls: list[int] = field(default_factory=list)

    async def __call__(self, i: int) -> ReviewerOutput:
        self.calls.append(i)
        message_index = len(self.calls) - 1 if self.message_by_call else i
        stdout = json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "id": "i",
                    "type": "agent_message",
                    "text": self.messages[message_index],
                },
            }
        )
        return ReviewerOutput(
            stdout=stdout,
            head_sha=(self.head_shas[message_index] if self.head_shas else f"sha{i}"),
            cost_usd=self.costs[message_index],
        )


@dataclass
class _FixerScript:
    costs: list[float]
    received: list[LocalVerdict] = field(default_factory=list)

    async def __call__(self, i: int, verdict: LocalVerdict) -> FixerOutput:
        self.received.append(verdict)
        return FixerOutput(ok=True, cost_usd=self.costs[i])


@pytest.mark.asyncio
async def test_loop_total_cost_sums_reviewer_and_fixer() -> None:
    reviewer = _ReviewerScript(
        messages=[
            f"## Findings\n- [Major] bug\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            f"good\n{VERDICT_APPROVED_MARKER}",
        ],
        costs=[0.10, 0.05],
        head_shas=["s1", "s2"],
    )
    fixer = _FixerScript(costs=[0.20])
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert result.total_cost_usd == pytest.approx(0.10 + 0.20 + 0.05)


@pytest.mark.asyncio
async def test_loop_total_cost_includes_retried_reviewer() -> None:
    reviewer = _ReviewerScript(
        messages=[
            "No verdict marker.",
            f"good\n{VERDICT_APPROVED_MARKER}",
        ],
        costs=[0.10, 0.05],
        head_shas=["s1", "s1"],
        message_by_call=True,
    )
    fixer = _FixerScript(costs=[])
    result = await run_local_review_loop(
        reviewer_agent="codex",
        reviewer=reviewer,
        fixer=fixer,
        cap=5,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert reviewer.calls == [0, 0]
    assert result.total_cost_usd == pytest.approx(0.15)


# --- session-level cost propagation ----------------------------------


def _codex_event_stream_with_cost(
    final_text: str, *, input_tokens: int, output_tokens: int
) -> list[RunnerEvent]:
    final = json.dumps(
        {
            "type": "item.completed",
            "item": {"id": "i", "type": "agent_message", "text": final_text},
        }
    )
    usage_line = json.dumps(
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cached_input_tokens": 0,
            },
        }
    )
    return [
        RunnerEvent(kind="stdout", line=final),
        RunnerEvent(kind="stdout", line=usage_line),
        RunnerEvent(kind="exit", returncode=0),
    ]


class _StagedRunner:
    def __init__(self, scripts: list[list[RunnerEvent]]) -> None:
        self._scripts = scripts
        self.specs: list[RunnerSpec] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.specs.append(spec)
        events = self._scripts[len(self.specs) - 1]

        async def gen() -> AsyncIterator[RunnerEvent]:
            for ev in events:
                yield ev

        return gen()

    async def kill(self, run_id: str) -> None:
        pass


class _HybridRunner:
    def __init__(self, *, required_fixes: int = 1) -> None:
        self.specs: list[RunnerSpec] = []
        self.head = "implemented-head"
        self.spec_round = 0
        self.fix_round = 0
        self.required_fixes = required_fixes

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.specs.append(spec)

        async def gen() -> AsyncIterator[RunnerEvent]:
            if spec.stage == "local_review_fix":
                self.fix_round += 1
                self.head = f"fixed-head-{self.fix_round}"
                yield RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {
                            "type": "item.completed",
                            "item": {
                                "id": "fix",
                                "type": "agent_message",
                                "text": "Fixed and committed.",
                            },
                        }
                    ),
                )
            elif spec.run_id.endswith("-spec"):
                self.spec_round += 1
                text = (
                    "## Findings\n"
                    f"- [Major] api.py:{self.spec_round} remaining defect. Fix it.\n"
                    f"{VERDICT_CHANGES_REQUESTED_MARKER}"
                    if self.spec_round <= self.required_fixes
                    else f"Spec and standards pass.\n{VERDICT_APPROVED_MARKER}"
                )
                yield RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {
                            "type": "item.completed",
                            "item": {"id": "spec", "type": "agent_message", "text": text},
                        }
                    ),
                )
            else:
                yield RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {
                            "type": "item.completed",
                            "item": {
                                "id": "bug",
                                "type": "agent_message",
                                "text": "No findings.",
                            },
                        }
                    ),
                )
            yield RunnerEvent(kind="exit", returncode=0)

        return gen()

    async def kill(self, run_id: str) -> None:
        pass


class _ClaudeHybridRunner:
    def __init__(self) -> None:
        self.specs: list[RunnerSpec] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.specs.append(spec)

        async def gen() -> AsyncIterator[RunnerEvent]:
            axis = "Spec and Standards" if spec.run_id.endswith("-spec") else "Bug"
            yield RunnerEvent(
                kind="stdout",
                line=json.dumps(
                    {
                        "type": "result",
                        "result": f"{axis} review passed.\n{VERDICT_APPROVED_MARKER}",
                    }
                ),
            )
            yield RunnerEvent(kind="exit", returncode=0)

        return gen()

    async def kill(self, run_id: str) -> None:
        pass


@pytest.mark.asyncio
async def test_session_total_cost_reflects_codex_token_pricing(
    tmp_path: Path,
) -> None:
    """Real codex sessions emit token counts only; the session must
    price them via the estimator and surface the total."""
    runner = _StagedRunner(
        [
            _codex_event_stream_with_cost(
                f"good\n{VERDICT_APPROVED_MARKER}",
                input_tokens=1_000_000,
                output_tokens=500_000,
            )
        ]
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="r1",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.APPROVED
    # Pricing sanity: 1M input @ $1.25 + 0.5M output @ $10 = $1.25 + $5 = $6.25
    assert result.total_cost_usd == pytest.approx(6.25, rel=1e-6)


@pytest.mark.asyncio
async def test_hybrid_session_runs_two_axes_and_stops_after_first_clean_closure(
    tmp_path: Path,
) -> None:
    runner = _HybridRunner()

    async def head_sha(_: Path) -> str:
        return runner.head

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="r1",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.6-sol"),
        verifier_role=ResolvedRole(agent="codex", model="gpt-5.6-sol"),
        fixer_role=ResolvedRole(agent="codex", model="gpt-5.6-sol"),
        cap=9,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        review_mode="hybrid",
    )

    assert result.outcome == LoopOutcome.APPROVED
    fix_specs = [spec for spec in runner.specs if spec.stage == "local_review_fix"]
    assert len(fix_specs) == 1
    bug_specs = [spec for spec in runner.specs if spec.run_id.endswith("-bug")]
    assert len(bug_specs) == 2
    assert bug_specs[0].command[bug_specs[0].command.index("--base") + 1] == "main"
    assert bug_specs[1].command[bug_specs[1].command.index("--base") + 1] == "implemented-head"
    assert "remaining defect" not in " ".join(bug_specs[1].command).lower()
    assert bug_specs[1].command[-2] == "-o"


@pytest.mark.asyncio
async def test_hybrid_session_runs_both_axes_with_claude_roles(tmp_path: Path) -> None:
    runner = _ClaudeHybridRunner()

    async def head_sha(_: Path) -> str:
        return "implemented-head"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="r-claude-hybrid",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="claude", model="claude-opus-5"),
        verifier_role=ResolvedRole(agent="claude", model="claude-opus-5"),
        fixer_role=ResolvedRole(agent="claude", model="claude-opus-5"),
        cap=3,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        review_mode="hybrid",
    )

    assert result.outcome == LoopOutcome.APPROVED
    review_specs = [spec for spec in runner.specs if spec.stage == "local_review"]
    assert [spec.run_id.rsplit("-", 1)[-1] for spec in review_specs] == ["spec", "bug"]
    assert all(spec.command[0] == "claude" for spec in review_specs)
    prompts = [spec.command[-1] for spec in review_specs]
    assert "Spec and Standards reviewer" in prompts[0]
    assert "functional bugs" in prompts[1]


@pytest.mark.asyncio
async def test_hybrid_session_uses_configured_fix_cap_until_approved(tmp_path: Path) -> None:
    runner = _HybridRunner(required_fixes=2)

    async def head_sha(_: Path) -> str:
        return runner.head

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="r-multi-fix",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.6-sol"),
        verifier_role=ResolvedRole(agent="codex", model="gpt-5.6-sol"),
        fixer_role=ResolvedRole(agent="codex", model="gpt-5.6-sol"),
        cap=3,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        review_mode="hybrid",
    )

    assert result.outcome == LoopOutcome.APPROVED
    assert len([spec for spec in runner.specs if spec.stage == "local_review_fix"]) == 2
    assert len([spec for spec in runner.specs if spec.run_id.endswith("-bug")]) == 3


@pytest.mark.asyncio
async def test_session_counts_each_codex_subprocess_from_zero(tmp_path: Path) -> None:
    runner = _StagedRunner(
        [
            _codex_event_stream_with_cost(
                "No verdict marker.",
                input_tokens=1_000,
                output_tokens=200,
            ),
            _codex_event_stream_with_cost(
                f"good\n{VERDICT_APPROVED_MARKER}",
                input_tokens=500,
                output_tokens=100,
            ),
        ]
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="r1",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert len(runner.specs) == 2
    assert result.input_tokens == 1_500
    assert result.output_tokens == 300
    assert result.total_cost_usd == pytest.approx(0.004875, rel=1e-6)
