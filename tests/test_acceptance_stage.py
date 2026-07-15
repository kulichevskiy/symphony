"""Orchestrator wiring for the Acceptance stage."""

from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import httpx
import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import AcceptanceConfig, Config, LinearStates, RepoBinding, RoleConfig
from symphony.github.client import CheckRun, GitHubError, PRChecks
from symphony.linear.client import LinearError, LinearIssue
from symphony.orchestrator import poll as poll_module
from symphony.orchestrator.poll import Orchestrator, _binding_storage_key
from symphony.orchestrator.poll import _acceptance as acceptance_module
from symphony.pipeline.acceptance_classifier import (
    ACCEPTANCE_FOOTER_INFRA_ERROR,
    ACCEPTANCE_FOOTER_PASS,
    ACCEPTANCE_FOOTER_REJECT,
    AcceptanceCriterionResult,
    AcceptanceScreenshot,
    AcceptanceVerdict,
)


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent] | list[list[RunnerEvent]]) -> None:
        if events and isinstance(events[0], RunnerEvent):
            self.scripts = [events]  # type: ignore[list-item]
        else:
            self.scripts = events  # type: ignore[assignment]
        self.captured_specs: list[RunnerSpec] = []

    @property
    def captured_spec(self) -> RunnerSpec | None:
        if not self.captured_specs:
            return None
        return self.captured_specs[-1]

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_specs.append(spec)
        index = min(len(self.captured_specs) - 1, len(self.scripts) - 1)
        return self._aiter(self.scripts[index])

    async def _aiter(self, events: list[RunnerEvent]) -> AsyncIterator[RunnerEvent]:
        for ev in events:
            yield ev

    async def kill(self, _run_id: str) -> None:
        return None


def _acceptance_events(
    verdict_footer: str = ACCEPTANCE_FOOTER_PASS,
    *,
    cost: float = 0.0,
    usage: dict[str, int] | None = None,
) -> list[RunnerEvent]:
    return [
        RunnerEvent(kind="started", pid=2222),
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "result",
                    "subtype": "success",
                    "result": f"Acceptance verdict.\n\n{verdict_footer}",
                    "total_cost_usd": cost,
                    "usage": usage
                    or {
                        "input_tokens": 100,
                        "cache_creation_input_tokens": 30,
                        "cache_read_input_tokens": 40,
                        "output_tokens": 20,
                    },
                }
            ),
        ),
        RunnerEvent(kind="exit", returncode=0),
    ]


def _codex_acceptance_events(verdict_footer: str = ACCEPTANCE_FOOTER_PASS) -> list[RunnerEvent]:
    return [
        RunnerEvent(kind="started", pid=2222),
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "turn.completed",
                    "usage": {
                        "input_tokens": 900,
                        "output_tokens": 120,
                        "cached_input_tokens": 80,
                    },
                }
            ),
        ),
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "type": "agent_message",
                        "text": f"Acceptance verdict.\n\n{verdict_footer}",
                    },
                }
            ),
        ),
        RunnerEvent(kind="exit", returncode=0),
    ]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _malformed_acceptance_events() -> list[RunnerEvent]:
    return [
        RunnerEvent(kind="started", pid=2222),
        RunnerEvent(kind="stdout", line="not-json"),
        RunnerEvent(kind="exit", returncode=0),
    ]


def _merge_events() -> list[RunnerEvent]:
    return [RunnerEvent(kind="exit", returncode=0)]


def _fix_events() -> list[RunnerEvent]:
    return [
        RunnerEvent(kind="started", pid=3333),
        RunnerEvent(kind="exit", returncode=0),
    ]


def _binding(
    mode: str = "off",
    *,
    acceptance_time_cap_minutes: float = 15.0,
) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        acceptance=AcceptanceConfig(  # type: ignore[arg-type]
            mode=mode,
            time_cap_minutes=acceptance_time_cap_minutes,
        ),
        linear_states=LinearStates(
            ready="Todo",
            code_review="Needs Approval",
            in_progress="In Progress",
            needs_approval="Needs Approval",
            in_acceptance="In Acceptance",
            blocked="Blocked",
            done="Done",
        ),
    )


def _issue(
    state_name: str = "Needs Approval",
    state_id: str = "state-na",
    *,
    title: str = "Add auth",
    description: str = "Need OAuth.",
) -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title=title,
        description=description,
        url="https://linear.app/team/issue/ENG-1",
        state_id=state_id,
        state_name=state_name,
        state_type="started",
        team_key="ENG",
        labels=["feature"],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Needs Approval": "state-na",
        "In Acceptance": "state-acceptance",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


async def _seed_review_candidate(conn, binding: RepoBinding) -> None:  # type: ignore[no-untyped-def]
    await db.issues.upsert(
        conn,
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        team_key="ENG",
    )
    await db.runs.create(
        conn,
        id="implement",
        issue_id="iss-1",
        stage="implement",
        status="completed",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )
    await db.runs.create(
        conn,
        id="review",
        issue_id="iss-1",
        stage="review",
        status="running",
        pid=None,
        started_at="2026-05-10T00:01:00+00:00",
    )
    await db.issue_prs.upsert(
        conn,
        issue_id="iss-1",
        github_repo="org/repo",
        binding_key=_binding_storage_key(binding),
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        created_at="2026-05-10T00:01:00+00:00",
    )


def _github() -> MagicMock:
    gh = MagicMock()
    # Green CI rollup matches `pr_checks` below: SYM-108 gates the no_signal
    # conflict-fix merge on the head's checks being green.
    _green_rollup = [{"__typename": "StatusContext", "context": "ci", "state": "SUCCESS"}]
    gh.pr_view = AsyncMock(
        side_effect=[
            {
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergedAt": None,
                "statusCheckRollup": _green_rollup,
            },
            {
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergedAt": None,
                "statusCheckRollup": _green_rollup,
            },
            {
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergedAt": "2026-05-10T00:04:00Z",
                "statusCheckRollup": _green_rollup,
            },
        ]
    )
    gh.pr_checks = AsyncMock(
        return_value=PRChecks([CheckRun(name="test", state="SUCCESS", bucket="pass")])
    )
    gh.pr_review_comments = AsyncMock(return_value=[])
    gh.pr_reviews = AsyncMock(
        return_value=[
            {
                "user": {"login": "reviewer"},
                "state": "APPROVED",
                "commit_id": "abc123",
                "submitted_at": "2026-05-10T00:03:00Z",
                "body": "",
            }
        ]
    )
    gh.pr_reactions = AsyncMock(return_value=[])
    gh.pr_issue_comments = AsyncMock(return_value=[])
    gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
    gh.pr_diff = AsyncMock(return_value="diff --git a/auth.py b/auth.py\n+enable_oauth()\n")
    gh.pr_merge = AsyncMock()
    return gh


@pytest.mark.asyncio
async def test_acceptance_mode_runs_code_only_runner_between_review_and_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    loaded_taste_guide_path: dict[str, str | None] = {}

    def fake_load_taste_guide(*, binding_taste_guide: str | None) -> str:
        loaded_taste_guide_path["path"] = binding_taste_guide
        return (
            "## Principles\n\nGlobal taste.\n\n"
            "## Hard rules (acceptance must reject if violated)\n\n"
            "- Reject icon names rendered as text.\n\n"
            "## Hard rules (acceptance must reject if violated)\n\n"
            "- Per-binding hard rule.\n"
        )

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    monkeypatch.setattr(acceptance_module, "load_taste_guide", fake_load_taste_guide)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        binding = binding.model_copy(
            update={
                "acceptance": binding.acceptance.model_copy(
                    update={"taste_guide": "./docs/sample-ux.md"}
                )
            }
        )
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        push_fn = AsyncMock()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=_github(),
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.pr_number == 42
        assert acceptance.pr_url == "https://github.com/org/repo/pull/42"
        assert acceptance.pr_head_sha == "abc123"
        assert acceptance.mode == "code_only"
        assert acceptance.last_verdict == "pass"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "merge",
        ]
        assert history[2].status == "completed"
        assert history[2].input_tokens == 100
        assert history[2].output_tokens == 20
        assert history[2].cache_write_tokens == 30
        assert history[2].cache_read_tokens == 40
        assert history[3].status == "done"
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert [spec.stage for spec in runner.captured_specs] == [
            "acceptance",
            "merge",
        ]
        acceptance_prompt = runner.captured_specs[0].command[-1]
        assert "Need OAuth." in acceptance_prompt
        assert "diff --git a/auth.py b/auth.py" in acceptance_prompt
        assert "mode: code_only" in acceptance_prompt
        assert loaded_taste_guide_path["path"] == "./docs/sample-ux.md"
        assert acceptance_prompt.index("Global taste.") < acceptance_prompt.index(
            "Per-binding hard rule."
        )
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-acceptance"),
            call("iss-1", "state-done"),
        ]
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert not any("degraded to code-only" in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_pass_honors_needs_human_approval_label_added_mid_flow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        issue_with_override = _issue()
        issue_with_override.labels = ["feature", "needs-human-approval"]
        linear.lookup_issue = AsyncMock(
            side_effect=[
                _issue(),
                _issue(),
                issue_with_override,
            ]
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.last_verdict == "pass"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "merge",
        ]
        assert history[2].status == "completed"
        assert history[3].status == "needs_approval"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_MERGE
        assert wait.run_id == history[3].id
        assert [spec.stage for spec in runner.captured_specs] == ["acceptance"]
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-acceptance"),
            call("iss-1", "state-na"),
        ]
        gh.pr_merge.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_verdict_run_uses_resolved_accept_role(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`roles.accept` must drive the acceptance-verdict subprocess itself,
    not just the post-rejection fix run (SYM-192 review)."""

    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        # No legacy top-level `agent=` on the binding: it would conflict with
        # the global `roles["accept"]` matrix cell set below.
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            branch_prefix="symphony",
            acceptance=AcceptanceConfig(mode="code_only", time_cap_minutes=15.0),  # type: ignore[arg-type]
            linear_states=LinearStates(
                ready="Todo",
                code_review="Needs Approval",
                in_progress="In Progress",
                needs_approval="Needs Approval",
                in_acceptance="In Acceptance",
                blocked="Blocked",
                done="Done",
            ),
        )
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner([_codex_acceptance_events(), _merge_events()])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(
            Config(
                repos=[binding],
                roles={
                    "accept": RoleConfig(agent="codex", model="gpt-5.1-codex", effort="high"),
                },
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=_github(),
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        assert [spec.stage for spec in runner.captured_specs] == ["acceptance", "merge"]
        acceptance_spec = runner.captured_specs[0]
        acceptance_command = acceptance_spec.command
        assert acceptance_command[0] == "codex"
        assert acceptance_command[acceptance_command.index("--model") + 1] == "gpt-5.1-codex"
        assert (
            acceptance_command[acceptance_command.index("--config") + 1]
            == 'model_reasoning_effort="high"'
        )
        # The accept role's resolved Codex model must attribute the run's
        # per-model usage row, not go unrecorded (SYM-192 review).
        usage_rows = await db.run_model_usage.list_for_run(conn, acceptance_spec.run_id)
        assert [(row.provider, row.model) for row in usage_rows] == [("codex", "gpt-5.1-codex")]
        assert (usage_rows[0].input_tokens, usage_rows[0].output_tokens) == (900, 120)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_publishes_extracted_criteria_before_checking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            return_value=_issue(
                description=(
                    "Ship OAuth.\n\n"
                    "## Acceptance criteria\n\n"
                    "- [ ] OAuth login is implemented:\n"
                    "  - GitHub OAuth is supported.\n"
                    "- [ ] Existing sessions still load.\n\n"
                    "## Out of scope\n\n"
                    "- Password reset changes."
                )
            )
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        expected_criteria = [
            {
                "name": "OAuth login is implemented",
                "predicate": "OAuth login is implemented: GitHub OAuth is supported.",
            },
            {
                "name": "Existing sessions still load",
                "predicate": "Existing sessions still load.",
            },
        ]
        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert json.loads(acceptance.extracted_criteria) == expected_criteria

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        criteria_index = next(
            i
            for i, body in enumerate(bodies)
            if "### Symphony extracted acceptance criteria" in body
        )
        verdict_index = next(
            i for i, body in enumerate(bodies) if "**Acceptance verdict:**" in body
        )
        assert criteria_index < verdict_index
        assert "OAuth login is implemented" in bodies[criteria_index]
        assert "GitHub OAuth is supported" in bodies[criteria_index]
        assert "Existing sessions still load" in bodies[criteria_index]
        assert gh.pr_diff.await_count == 1
        assert runner.captured_specs[0].stage == "acceptance"
        acceptance_prompt = runner.captured_specs[0].command[-1]
        assert "- OAuth login is implemented: GitHub OAuth is supported." in acceptance_prompt
        assert "OAuth login is implemented" in bodies[verdict_index]
        assert (
            "- **OAuth login is implemented: GitHub OAuth is supported.**"
            not in bodies[verdict_index]
        )
        assert "Existing sessions still load" in bodies[verdict_index]
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["dev", "preview"])
async def test_acceptance_degrades_missing_where_to_verify_to_code_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    mode: str,
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    caplog.set_level("INFO", logger="symphony.orchestrator.poll")
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(mode)
        binding.acceptance.preview_url_pattern = "https://preview.example/{issue}"
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue(description="Need OAuth."))
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.mode == mode
        assert acceptance.preview_url == ""
        assert acceptance.last_verdict == "pass"

        assert [spec.stage for spec in runner.captured_specs] == [
            "acceptance",
            "merge",
        ]
        acceptance_prompt = runner.captured_specs[0].command[-1]
        assert "mode: code_only" in acceptance_prompt
        assert "mode: dev" not in acceptance_prompt
        assert "mode: preview" not in acceptance_prompt
        gh.pr_diff.assert_awaited_once()
        assert runner.captured_specs[0].workspace_path == tmp_path / "ws" / "org" / "eng-1"

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        degrade_note = (
            "Acceptance: degraded to code-only — no `Where to verify` in ticket description"
        )
        assert any(degrade_note in body for body in bodies)
        assert degrade_note in caplog.text
        gh.pr_merge.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_runner_uses_acceptance_time_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(
            "code_only",
            acceptance_time_cap_minutes=7,
        )
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
                stall_timeout_secs=30,
            ),
            linear,
            conn,
            runner=runner,
            gh=_github(),
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance_spec = runner.captured_specs[0]
        assert acceptance_spec.stall_secs == 7 * 60
        assert "--max-budget-usd" not in acceptance_spec.command
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_quick_skip_posts_distinct_pass_and_still_merges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(_merge_events())
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            return_value=_issue(
                title="Fix README typo",
                description="Fix a typo in README.md.",
            )
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()
        gh.pr_diff = AsyncMock(
            return_value=(
                "diff --git a/README.md b/README.md\n"
                "-This pacakge runs Symphony.\n"
                "+This package runs Symphony.\n"
            )
        )

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.last_verdict == "pass"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "merge",
        ]
        assert history[2].status == "completed"
        assert history[2].cost_usd == 0.0
        assert [spec.stage for spec in runner.captured_specs] == ["merge"]
        gh.pr_merge.assert_awaited_once()

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any(body.startswith("**Acceptance: skipped - trivial change.**") for body in bodies)
        assert any("Reason: `quick_skip_trivial`" in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_pass_must_match_current_pr_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        await db.acceptance_state.begin_acceptance(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="old123",
            mode="code_only",
            preview_url="",
            extracted_criteria="[]",
        )
        await db.acceptance_state.record_verdict(
            conn,
            "iss-1",
            verdict="pass",
            artifacts_url="",
        )
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=_github(),
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.pr_head_sha == "abc123"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "merge",
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_runs_before_conflict_fix_ready_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        marked = await db.issue_prs.mark_merge_conflict_fixed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            head_sha="abc123",
            marked_at="2026-05-10T00:03:00+00:00",
        )
        assert marked is True
        runner = _FakeRunner([_acceptance_events(), _merge_events()])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()
        gh.pr_reviews = AsyncMock(return_value=[])

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.pr_head_sha == "abc123"
        assert acceptance.last_verdict == "pass"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "merge",
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_reject_dispatches_fix_then_rechecks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    async def no_fetch(_workspace_path: Path, _branch: str) -> None:
        return None

    async def remote_sha(_workspace_path: Path, _ref: str) -> str:
        return "abc123"

    async def fixed_head_sha(_workspace_path: Path) -> str:
        return "def456"

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    monkeypatch.setattr(acceptance_module, "_git_fetch_branch", no_fetch)
    monkeypatch.setattr(acceptance_module, "_workspace_ref_sha", remote_sha)
    monkeypatch.setattr(acceptance_module, "_workspace_head_sha", fixed_head_sha)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(
            [
                _acceptance_events(ACCEPTANCE_FOOTER_REJECT, cost=0.11),
                _fix_events(),
                _acceptance_events(ACCEPTANCE_FOOTER_PASS, cost=0.13),
                _merge_events(),
            ]
        )
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()
        push_fn = AsyncMock()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.iteration == 1
        assert acceptance.pr_head_sha == "def456"
        assert acceptance.last_verdict == "pass"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "acceptance_fix",
            "acceptance",
            "merge",
        ]
        assert history[2].status == "failed"
        assert history[2].cost_usd == pytest.approx(0.11)
        assert history[3].status == "completed"
        assert history[4].status == "completed"
        assert history[5].status == "done"
        assert push_fn.await_count >= 1
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert [spec.stage for spec in runner.captured_specs] == [
            "acceptance",
            "acceptance_fix",
            "acceptance",
            "merge",
        ]
        fix_prompt = runner.captured_specs[1].command[-1]
        assert "product/UX mismatch" in fix_prompt
        assert "Acceptance verdict." in fix_prompt
        gh.pr_merge.assert_awaited_once()

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Acceptance verdict" in body for body in bodies)
        assert any(ACCEPTANCE_FOOTER_REJECT in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_reject_after_fix_opens_operator_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    async def no_fetch(_workspace_path: Path, _branch: str) -> None:
        return None

    async def remote_sha(_workspace_path: Path, _ref: str) -> str:
        return "abc123"

    async def fixed_head_sha(_workspace_path: Path) -> str:
        return "def456"

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    monkeypatch.setattr(acceptance_module, "_git_fetch_branch", no_fetch)
    monkeypatch.setattr(acceptance_module, "_workspace_ref_sha", remote_sha)
    monkeypatch.setattr(acceptance_module, "_workspace_head_sha", fixed_head_sha)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(
            [
                _acceptance_events(ACCEPTANCE_FOOTER_REJECT, cost=0.11),
                _fix_events(),
                _acceptance_events(ACCEPTANCE_FOOTER_REJECT, cost=0.07),
            ]
        )
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        push_fn = AsyncMock()
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.iteration == 1
        assert acceptance.last_verdict == "reject"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "acceptance_fix",
            "acceptance",
        ]
        assert history[3].status == "completed"
        assert history[4].status == "failed"

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_ACCEPTANCE_REJECTED
        assert wait.run_id == history[4].id
        gh.pr_merge.assert_not_awaited()

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("$skip-acceptance" in body for body in bodies)
        assert any("$retry-acceptance" in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_fix_without_new_commit_opens_operator_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    async def no_fetch(_workspace_path: Path, _branch: str) -> None:
        return None

    async def remote_sha(_workspace_path: Path, _ref: str) -> str:
        return "abc123"

    async def unchanged_head_sha(_workspace_path: Path) -> str:
        return "abc123"

    async def clean_status(_workspace_path: Path) -> str:
        return ""

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    monkeypatch.setattr(acceptance_module, "_git_fetch_branch", no_fetch)
    monkeypatch.setattr(acceptance_module, "_workspace_ref_sha", remote_sha)
    monkeypatch.setattr(acceptance_module, "_workspace_head_sha", unchanged_head_sha)
    monkeypatch.setattr(acceptance_module, "_git_status_short", clean_status)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(
            [
                _acceptance_events(ACCEPTANCE_FOOTER_REJECT, cost=0.11),
                _fix_events(),
            ]
        )
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        push_fn = AsyncMock()
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.iteration == 1
        assert acceptance.last_verdict == "reject"

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "acceptance_fix",
        ]
        assert history[2].status == "failed"
        assert history[3].status == "failed"
        assert push_fn.await_count == 0

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_ACCEPTANCE_REJECTED
        assert wait.run_id == history[2].id
        assert await orch._poll_merge_candidates() == []  # noqa: SLF001

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("$skip-acceptance" in body for body in bodies)
        assert any("$retry-acceptance" in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_diff_fetch_failure_records_infra_error_without_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(_acceptance_events())
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()
        gh.pr_diff = AsyncMock(side_effect=GitHubError("rate limited"))

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.last_verdict == "infra_error"
        assert acceptance.infra_retries == 1

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
        ]
        assert history[2].status == "failed"
        assert runner.captured_specs == []
        workspace.acquire.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Could not fetch PR diff" in body for body in bodies)
        assert any(ACCEPTANCE_FOOTER_INFRA_ERROR in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_infra_error_retries_with_backoff_then_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    async def expire_latest_acceptance_backoff() -> None:
        latest = await db.runs.latest_for_issue_stage(
            conn,
            issue_id="iss-1",
            stage="acceptance",
        )
        assert latest is not None
        await db.runs.update_status(
            conn,
            latest.id,
            latest.status,
            ended_at="2026-05-10T00:00:00+00:00",
        )

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("code_only")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(
            [
                _malformed_acceptance_events(),
                _malformed_acceptance_events(),
                _malformed_acceptance_events(),
            ]
        )
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergedAt": None,
            }
        )

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.last_verdict == "infra_error"
        assert acceptance.infra_retries == 1
        assert await db.operator_waits.get(conn, "iss-1") is None

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()
        assert len(runner.captured_specs) == 1

        await expire_latest_acceptance_backoff()
        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.infra_retries == 2
        assert await db.operator_waits.get(conn, "iss-1") is None

        await expire_latest_acceptance_backoff()
        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.infra_retries == 2
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_ACCEPTANCE_BLOCKED
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "acceptance",
            "acceptance",
        ]
        assert all(run.status == "failed" for run in history[2:])
        blocked_comment = linear.post_comment.await_args_list[-1].args[1]
        assert "`org/repo#42`" in blocked_comment
        assert "`org/repo#0`" not in blocked_comment
        gh.pr_merge.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_preview_acceptance_resolves_url_runs_visual_flow_and_skips_dev_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    async def fake_resolve_preview_url(**kwargs: object) -> str:
        assert kwargs["acceptance"].preview_url_pattern == ("https://vib-{pr_number}.vercel.app")
        assert kwargs["pr_number"] == 42
        return preview_url

    async def fake_run_acceptance(**kwargs: object) -> AcceptanceVerdict:
        assert kwargs["mode"] == "preview"
        assert kwargs["preview_url"] == preview_url
        return AcceptanceVerdict(
            kind="pass",
            criteria=[
                "OAuth login is implemented: GitHub OAuth is supported.",
            ],
            cost=0.13,
            hero_screenshot_url="",
            details="Preview acceptance passed.",
            preview_url=preview_url,
            screenshots=(
                AcceptanceScreenshot(
                    kind="hero",
                    label="Primary verified view",
                    path=".symphony/acceptance/run/hero.png",
                ),
            ),
            criterion_results=(
                AcceptanceCriterionResult(
                    criterion="OAuth login is implemented: GitHub OAuth is supported.",
                    passed=True,
                ),
            ),
        )

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    monkeypatch.setattr(acceptance_module, "resolve_preview_url", fake_resolve_preview_url)
    monkeypatch.setattr(acceptance_module, "run_acceptance", fake_run_acceptance)
    preview_url = "https://vib-42.vercel.app"
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("preview")
        binding.acceptance.preview_url_pattern = "https://vib-{pr_number}.vercel.app"
        binding.acceptance.dev_command = "npm run dev"
        binding.acceptance.dev_port = _free_port()
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(_merge_events())
        workspace_path = tmp_path / "ws" / "org" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            return_value=_issue(
                description=(
                    "Need OAuth.\n\n"
                    "## Where to verify\n\n"
                    "- Open the login screen.\n\n"
                    "## Acceptance criteria\n\n"
                    "- [ ] OAuth login is implemented:\n"
                    "  - GitHub OAuth is supported.\n"
                )
            )
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(side_effect=["criteria-cmt", "verdict-cmt", "merge-cmt"])
        linear.upload_issue_attachment = AsyncMock(
            return_value="https://uploads.linear.app/hero.png"
        )
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.mode == "preview"
        assert acceptance.preview_url == preview_url
        assert acceptance.last_verdict == "pass"
        assert acceptance.infra_retries == 0

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == [
            "implement",
            "review",
            "acceptance",
            "merge",
        ]
        assert history[2].status == "completed"
        assert history[3].status == "done"
        assert [spec.stage for spec in runner.captured_specs] == ["merge"]
        gh.pr_diff.assert_awaited_once()
        linear.upload_issue_attachment.assert_awaited_once()
        gh.pr_merge.assert_awaited_once()

        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any(preview_url in body for body in bodies)
        assert any(
            "![Primary verified view](https://uploads.linear.app/hero.png)" in body
            for body in bodies
        )
        assert not any("pass-through acceptance behavior" in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_dev_acceptance_sets_preview_url_uploads_screenshot_and_records_comment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    async def fake_run_acceptance(**kwargs: object) -> AcceptanceVerdict:
        assert kwargs["mode"] == "dev"
        assert kwargs["preview_url"] == preview_url
        assert kwargs["dev_command"] == "npm run dev"
        assert kwargs["dev_port"] == port
        return AcceptanceVerdict(
            kind="pass",
            criteria=[
                "OAuth login is implemented: GitHub OAuth is supported.",
            ],
            cost=0.13,
            hero_screenshot_url="",
            details="Visual acceptance passed.",
            preview_url=preview_url,
            screenshots=(
                AcceptanceScreenshot(
                    kind="hero",
                    label="Primary verified view",
                    path=".symphony/acceptance/run/hero.png",
                ),
            ),
            criterion_results=(
                AcceptanceCriterionResult(
                    criterion="OAuth login is implemented: GitHub OAuth is supported.",
                    passed=True,
                ),
            ),
        )

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    monkeypatch.setattr(acceptance_module, "run_acceptance", fake_run_acceptance)
    port = _free_port()
    preview_url = f"http://127.0.0.1:{port}"
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("dev")
        binding.acceptance.dev_port = port
        binding.acceptance.dev_command = "npm run dev"
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(_merge_events())
        workspace_path = tmp_path / "ws" / "org" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            return_value=_issue(
                description=(
                    "Need OAuth.\n\n"
                    "## Where to verify\n\n"
                    "- Open the login screen.\n\n"
                    "## Acceptance criteria\n\n"
                    "- [ ] OAuth login is implemented:\n"
                    "  - GitHub OAuth is supported.\n"
                )
            )
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(side_effect=["criteria-cmt", "verdict-cmt"])
        linear.upload_issue_attachment = AsyncMock(
            return_value="https://uploads.linear.app/hero.png"
        )
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.mode == "dev"
        assert acceptance.preview_url == preview_url
        assert acceptance.last_verdict == "pass"
        assert acceptance.last_artifacts_url == (
            "https://linear.app/team/issue/ENG-1#comment-verdict-cmt"
        )
        linear.upload_issue_attachment.assert_awaited_once()
        upload_call = linear.upload_issue_attachment.await_args
        assert upload_call.kwargs["issue_uuid"] == "iss-1"
        assert upload_call.kwargs["title"] == "Acceptance screenshot: Primary verified view"
        assert upload_call.kwargs["path"] == workspace_path / ".symphony/acceptance/run/hero.png"

        verdict_comment = linear.post_comment.await_args_list[1].args[1]
        assert preview_url in verdict_comment
        assert "![Primary verified view](https://uploads.linear.app/hero.png)" in verdict_comment
        assert "pass-through acceptance behavior" not in verdict_comment
        assert runner.captured_specs[0].stage == "merge"
        gh.pr_diff.assert_awaited_once()
        gh.pr_merge.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_dev_acceptance_records_resolved_preview_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    async def fake_run_acceptance(**kwargs: object) -> AcceptanceVerdict:
        assert kwargs["mode"] == "dev"
        assert kwargs["preview_url"] == configured_url
        return AcceptanceVerdict(
            kind="pass",
            criteria=[
                "OAuth login is implemented: GitHub OAuth is supported.",
            ],
            cost=0.13,
            hero_screenshot_url="",
            details="Visual acceptance passed.",
            preview_url=resolved_url,
            screenshots=(
                AcceptanceScreenshot(
                    kind="hero",
                    label="Primary verified view",
                    path=".symphony/acceptance/run/hero.png",
                ),
            ),
            criterion_results=(
                AcceptanceCriterionResult(
                    criterion="OAuth login is implemented: GitHub OAuth is supported.",
                    passed=True,
                ),
            ),
        )

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    monkeypatch.setattr(acceptance_module, "run_acceptance", fake_run_acceptance)
    configured_port = _free_port()
    resolved_port = _free_port()
    while resolved_port == configured_port:
        resolved_port = _free_port()
    configured_url = f"http://127.0.0.1:{configured_port}"
    resolved_url = f"http://127.0.0.1:{resolved_port}"
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("dev")
        binding.acceptance.dev_port = configured_port
        binding.acceptance.dev_command = "npm run dev"
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(_merge_events())
        workspace_path = tmp_path / "ws" / "org" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            return_value=_issue(
                description=(
                    "Need OAuth.\n\n"
                    "## Where to verify\n\n"
                    "- Open the login screen.\n\n"
                    "## Acceptance criteria\n\n"
                    "- [ ] OAuth login is implemented:\n"
                    "  - GitHub OAuth is supported.\n"
                )
            )
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(side_effect=["criteria-cmt", "verdict-cmt"])
        linear.upload_issue_attachment = AsyncMock(
            return_value="https://uploads.linear.app/hero.png"
        )
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.preview_url == resolved_url
        assert acceptance.last_verdict == "pass"
        verdict_comment = linear.post_comment.await_args_list[1].args[1]
        assert resolved_url in verdict_comment
        assert configured_url not in verdict_comment
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_dev_acceptance_invalid_screenshot_path_records_infra_error(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        workspace_path = tmp_path / "ws" / "org" / "eng-1"
        workspace_path.mkdir(parents=True)
        linear = AsyncMock()
        linear.upload_issue_attachment = AsyncMock()
        orch = Orchestrator(
            Config(
                repos=[],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=_FakeRunner([]),
            gh=_github(),
            push_fn=AsyncMock(),
        )

        verdict = AcceptanceVerdict(
            kind="pass",
            criteria=[],
            cost=0.0,
            hero_screenshot_url="",
            screenshots=(
                AcceptanceScreenshot(
                    kind="hero",
                    label="Primary verified view",
                    path="../outside.png",
                ),
            ),
        )

        result = await orch._upload_acceptance_screenshots(  # noqa: SLF001
            issue=_issue(),
            workspace_path=workspace_path,
            verdict=verdict,
        )

        assert result.kind == "infra_error"
        assert "acceptance screenshot upload failed" in result.details
        assert "escapes workspace" in result.details
        assert result.screenshots == ()
        linear.upload_issue_attachment.assert_not_awaited()
    finally:
        await conn.close()


def test_acceptance_artifact_path_reports_symlink_loop_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace_path = tmp_path / "ws" / "org" / "eng-1"
    workspace_path.mkdir(parents=True)
    raw_path = ".symphony/acceptance/run/loop/hero.png"
    loop_path = workspace_path / raw_path
    original_resolve = Path.resolve

    def fake_resolve(self: Path, *args: object, **kwargs: object) -> Path:
        if self == loop_path:
            raise RuntimeError("Symlink loop from test")
        return original_resolve(self, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", fake_resolve)

    with pytest.raises(OSError, match="cannot be resolved"):
        poll_module._acceptance_artifact_path(workspace_path, raw_path)  # noqa: SLF001


@pytest.mark.asyncio
async def test_dev_acceptance_http_upload_failure_records_infra_error(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        workspace_path = tmp_path / "ws" / "org" / "eng-1"
        screenshot_path = workspace_path / ".symphony/acceptance/run/hero.png"
        screenshot_path.parent.mkdir(parents=True)
        screenshot_path.write_bytes(b"fake png")
        linear = AsyncMock()
        linear.upload_issue_attachment = AsyncMock(
            side_effect=httpx.ConnectError("signed upload failed")
        )
        orch = Orchestrator(
            Config(
                repos=[],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=_FakeRunner([]),
            gh=_github(),
            push_fn=AsyncMock(),
        )

        verdict = AcceptanceVerdict(
            kind="pass",
            criteria=[],
            cost=0.0,
            hero_screenshot_url="",
            screenshots=(
                AcceptanceScreenshot(
                    kind="hero",
                    label="Primary verified view",
                    path=".symphony/acceptance/run/hero.png",
                ),
            ),
        )

        result = await orch._upload_acceptance_screenshots(  # noqa: SLF001
            issue=_issue(),
            workspace_path=workspace_path,
            verdict=verdict,
        )

        assert result.kind == "infra_error"
        assert "acceptance screenshot upload failed" in result.details
        assert "signed upload failed" in result.details
        assert result.screenshots == ()
        linear.upload_issue_attachment.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_dev_acceptance_records_hero_upload_when_verdict_comment_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    async def fake_run_acceptance(**kwargs: object) -> AcceptanceVerdict:
        assert kwargs["mode"] == "dev"
        return AcceptanceVerdict(
            kind="pass",
            criteria=[
                "OAuth login is implemented: GitHub OAuth is supported.",
            ],
            cost=0.13,
            hero_screenshot_url="",
            details="Visual acceptance passed.",
            preview_url=preview_url,
            screenshots=(
                AcceptanceScreenshot(
                    kind="hero",
                    label="Primary verified view",
                    path=".symphony/acceptance/run/hero.png",
                ),
            ),
            criterion_results=(
                AcceptanceCriterionResult(
                    criterion="OAuth login is implemented: GitHub OAuth is supported.",
                    passed=True,
                ),
            ),
        )

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    monkeypatch.setattr(acceptance_module, "run_acceptance", fake_run_acceptance)
    port = _free_port()
    preview_url = f"http://127.0.0.1:{port}"
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("dev")
        binding.acceptance.dev_port = port
        binding.acceptance.dev_command = "npm run dev"
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(_merge_events())
        workspace_path = tmp_path / "ws" / "org" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            return_value=_issue(
                description=(
                    "Need OAuth.\n\n"
                    "## Where to verify\n\n"
                    "- Open the login screen.\n\n"
                    "## Acceptance criteria\n\n"
                    "- [ ] OAuth login is implemented:\n"
                    "  - GitHub OAuth is supported.\n"
                )
            )
        )
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(side_effect=["criteria-cmt", LinearError("comment failed")])
        linear.upload_issue_attachment = AsyncMock(
            return_value="https://uploads.linear.app/hero.png"
        )
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        await orch.drain_dispatch_tasks()

        acceptance = await db.acceptance_state.get(conn, "iss-1")
        assert acceptance.last_verdict == "pass"
        assert acceptance.last_artifacts_url == "https://uploads.linear.app/hero.png"
        linear.upload_issue_attachment.assert_awaited_once()
        gh.pr_merge.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_off_preserves_current_review_to_merge_flow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("off")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(_merge_events())
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=_github(),
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review", "merge"]
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert linear.move_issue.await_args_list == [call("iss-1", "state-done")]
        cur = await conn.execute("SELECT COUNT(*) FROM acceptance_state")
        assert (await cur.fetchone())[0] == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_off_honors_needs_human_approval_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sync(_workspace_path: Path, _branch: str) -> None:
        return None

    monkeypatch.setattr(poll_module, "_sync_workspace_to_remote", no_sync)
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding("off")
        await _seed_review_candidate(conn, binding)
        runner = _FakeRunner(_merge_events())
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        issue_with_override = _issue()
        issue_with_override.labels = ["feature", "needs-human-approval"]
        linear.lookup_issue = AsyncMock(return_value=issue_with_override)
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = _github()

        orch = Orchestrator(
            Config(
                repos=[binding],
                log_root=tmp_path / "logs",
                workspace_root=tmp_path / "ws",
                db_path=tmp_path / "s.sqlite",
            ),
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review", "merge"]
        assert history[2].status == "needs_approval"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_MERGE
        assert wait.run_id == history[2].id
        assert runner.captured_specs == []
        assert linear.move_issue.await_args_list == [call("iss-1", "state-na")]
        gh.pr_merge.assert_not_awaited()
    finally:
        await conn.close()
