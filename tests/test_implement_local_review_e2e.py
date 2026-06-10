"""End-to-end dispatch with `review_strategy=local`.

A binding configured for local review must (a) run the in-workspace
reviewer after the implementer succeeds and (b) skip the `@codex review`
PR ping when the local pass approves. The remote review monitor row is
still created for approved local-only PRs. When local-only review does
not converge, the PR is parked in Needs Approval without a remote bot ping.
Reviewer/fix-run/cost-cap infrastructure failures block the issue like a
failed implement run.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.pipeline.local_review import (
    VERDICT_APPROVED_MARKER,
    LocalVerdict,
    LocalVerdictKind,
)
from symphony.pipeline.local_review_loop import LoopOutcome, LoopResult
from symphony.pipeline.review_classifier import Verdict, VerdictKind

from ._workspace_helpers import advance_head


def _events_exit_zero(events: list[RunnerEvent]) -> bool:
    return any(ev.kind == "exit" and ev.returncode == 0 for ev in events)


class _StagedRunner:
    """Returns scripted events keyed by `RunnerSpec.stage`.

    `scripts` maps stage → list of events. Each call to `run(spec)`
    consumes one bucket; if a stage is invoked more times than scripts
    were provided we raise so a test isn't silently green.
    """

    def __init__(self, scripts: dict[str, list[list[RunnerEvent]]]) -> None:
        self._scripts = {k: list(v) for k, v in scripts.items()}
        self.captured: list[RunnerSpec] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured.append(spec)
        bucket = self._scripts.get(spec.stage)
        if not bucket:
            raise AssertionError(
                f"unexpected stage {spec.stage!r}; remaining={self._scripts}"
            )
        events = bucket.pop(0)
        # A successful implement run commits its work; the completion gate
        # requires HEAD to advance over the branch base.
        if spec.stage == "implement" and _events_exit_zero(events):
            advance_head(spec.workspace_path)

        async def gen() -> AsyncIterator[RunnerEvent]:
            for ev in events:
                yield ev

        return gen()

    async def kill(self, run_id: str) -> None:
        pass


def _local_binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        review_strategy="local",
        # Force reviewer = codex (default already, but be explicit).
        reviewer_agent="codex",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _hybrid_binding(*, local_cap: int | None = None) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        local_review=True,
        remote_review=True,
        reviewer_agent="codex",
        local_review_iteration_cap=local_cap,
        linear_states=LinearStates(
            ready="Todo",
            local_code_review="Local Code Review",
            code_review="In Review",
            needs_approval="Needs Approval",
        ),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add authentication",
        description="Need OAuth login for the dashboard.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["feature"],
    )


def _issue_in_review() -> LinearIssue:
    issue = _issue()
    issue.state_id = "state-review"
    issue.state_name = "In Review"
    issue.state_type = "started"
    return issue


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Local Code Review": "state-local-review",
        "In Review": "state-review",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


async def _scan_and_wait(orch: Orchestrator, binding: RepoBinding) -> None:
    tasks = await orch._scan_binding(binding)  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)


def _codex_agent_message(text: str) -> RunnerEvent:
    return RunnerEvent(
        kind="stdout",
        line=json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "i", "type": "agent_message", "text": text},
            }
        ),
    )


@pytest.mark.asyncio
async def test_hybrid_strategy_runs_local_then_remote_then_merge(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _hybrid_binding(local_cap=2)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            local_review_iteration_cap=9,
            review_iteration_cap=4,
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(
            side_effect=[_issue(), _issue_in_review(), _issue_in_review()]
        )
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")
        gh.pr_view = AsyncMock(
            side_effect=[
                {
                    "headRefOid": "head-sha",
                    "mergeable": "MERGEABLE",
                    "mergeStateStatus": "CLEAN",
                    "state": "OPEN",
                    "mergedAt": None,
                },
                {
                    "headRefOid": "head-sha",
                    "mergeable": "MERGEABLE",
                    "mergeStateStatus": "CLEAN",
                    "state": "OPEN",
                    "mergedAt": None,
                },
                {
                    "headRefOid": "head-sha",
                    "mergeable": "MERGEABLE",
                    "mergeStateStatus": "CLEAN",
                    "state": "MERGED",
                    "mergedAt": "2026-06-05T10:00:00Z",
                },
            ]
        )
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        RunnerEvent(
                            kind="stdout",
                            line=json.dumps(
                                {
                                    "type": "result",
                                    "subtype": "success",
                                    "total_cost_usd": 0.01,
                                    "usage": {
                                        "input_tokens": 1,
                                        "output_tokens": 1,
                                    },
                                }
                            ),
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
                "local_review": [
                    [
                        _codex_agent_message(f"ok\n{VERDICT_APPROVED_MARKER}"),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
                "merge": [[RunnerEvent(kind="exit", returncode=0)]],
            }
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._review_verdict_for_pr = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="test_approved")
        )

        await _scan_and_wait(orch, binding)
        merge_tasks = await orch._poll_merge_candidates()  # noqa: SLF001
        if merge_tasks:
            await asyncio.gather(*merge_tasks)

        assert [spec.stage for spec in runner.captured] == [
            "implement",
            "local_review",
            "merge",
        ]
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-local-review"),
            call("iss-1", "state-review"),
            call("iss-1", "state-done"),
        ]
        assert any(
            "cap=2" in c.args[1] for c in linear.post_comment.await_args_list
        )
        gh.pr_comment.assert_any_await(42, "@codex review", repo="org/repo")
        gh.pr_merge.assert_awaited_once_with(
            42,
            strategy=binding.merge_strategy,
            auto=binding.allow_auto_merge,
            repo="org/repo",
        )
        assert push_fn.await_count == 2

        history = await db.runs.history_for_issue(conn, "iss-1")
        statuses = {run.stage: run.status for run in history}
        assert statuses["implement"] == "completed"
        assert statuses["local_review"] == "completed"
        assert statuses["review"] == "completed"
        assert statuses["merge"] == "done"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_hybrid_strategy_local_non_convergence_skips_remote_review(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _hybrid_binding()
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")
        push_fn = AsyncMock()

        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        RunnerEvent(
                            kind="stdout",
                            line=json.dumps(
                                {
                                    "type": "result",
                                    "subtype": "success",
                                    "total_cost_usd": 0.01,
                                    "usage": {
                                        "input_tokens": 1,
                                        "output_tokens": 1,
                                    },
                                }
                            ),
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=LoopResult(
                outcome=LoopOutcome.EXHAUSTED,
                iterations=2,
                verdicts=(
                    LocalVerdict(
                        kind=LocalVerdictKind.CHANGES_REQUESTED,
                        findings="src/auth.py:12 missing token validation",
                    ),
                ),
                error="local review exhausted",
            )
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        gh.pr_create.assert_awaited_once()
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        codex_calls = [
            c
            for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body"))
            == "@codex review"
        ]
        assert codex_calls == []
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-na" in move_targets
        assert "state-review" not in move_targets

        history = await db.runs.history_for_issue(conn, "iss-1")
        review_rows = [h for h in history if h.stage == "review"]
        assert len(review_rows) == 1
        assert review_rows[0].status == "needs_approval"
        assert "src/auth.py:12 missing token validation" in (
            review_rows[0].termination_detail
        )
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.run_id == review_rows[0].id
        assert wait.kind == db.operator_waits.KIND_REVIEW_FAILED
        assert review_rows[0].id in orch._operator_wait_run_ids  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_strategy_approved_skips_codex_review(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_local_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()

        implement_result = RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "result",
                    "subtype": "success",
                    "total_cost_usd": 0.42,
                    "usage": {"input_tokens": 100, "output_tokens": 50},
                }
            ),
        )
        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        implement_result,
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
                "local_review": [
                    [
                        _codex_agent_message(
                            f"Looks clean to me.\n{VERDICT_APPROVED_MARKER}"
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # Two stages dispatched: implement (claude) and local_review (codex).
        stages = [s.stage for s in runner.captured]
        assert stages == ["implement", "local_review"]
        assert runner.captured[1].command[:2] == ["codex", "exec"]
        assert "--sandbox" in runner.captured[1].command
        assert (
            runner.captured[1]
            .command[runner.captured[1].command.index("--sandbox") + 1]
            == "read-only"
        )

        # PR was still opened.
        gh.pr_create.assert_awaited_once()

        # `@codex review` was NOT posted — that's the whole point of local mode
        # with an APPROVED outcome.
        for call_obj in gh.pr_comment.await_args_list:
            args = call_obj.args
            kwargs = call_obj.kwargs
            posted_body = args[1] if len(args) >= 2 else kwargs.get("body")
            assert posted_body != "@codex review", (
                f"expected @codex review to be suppressed in local mode "
                f"after APPROVED verdict, but got: {call_obj}"
            )

        # The in-workspace reviewer gets its own pre-PR Linear lane. Local-only
        # mode must not require or move into the remote PR-review lane.
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-local-review" in move_targets
        assert "state-na" not in move_targets

        # Review monitor row was created so post-merge logic still works.
        history = await db.runs.history_for_issue(conn, "iss-1")
        stages_seen = sorted({h.stage for h in history})
        assert "review" in stages_seen
        assert "implement" in stages_seen
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_strategy_approved_posts_pr_summary(tmp_path: Path) -> None:
    """When local-review APPROVES, post a summary PR comment so humans
    visiting the PR on GitHub see the verdict trail."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_local_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")
        push_fn = AsyncMock()

        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        RunnerEvent(
                            kind="stdout",
                            line=json.dumps(
                                {
                                    "type": "result",
                                    "subtype": "success",
                                    "total_cost_usd": 0.42,
                                    "usage": {"input_tokens": 1, "output_tokens": 1},
                                }
                            ),
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
                "local_review": [
                    [
                        _codex_agent_message(
                            f"clean\n{VERDICT_APPROVED_MARKER}"
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh,
            workspace=workspace, push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # Find the PR comment that's the local-review summary.
        summary_calls = [
            c
            for c in gh.pr_comment.await_args_list
            if "local reviewer" in str(c).lower()
        ]
        assert len(summary_calls) == 1, (
            f"expected one local-review summary PR comment, got: "
            f"{gh.pr_comment.await_args_list}"
        )
        call = summary_calls[0]
        # Body is the second positional arg.
        body = call.args[1] if len(call.args) >= 2 else call.kwargs.get("body")
        assert "approved this pr" in body.lower()
        assert "codex" in body  # reviewer_agent = codex (opposite of claude impl)
        assert "iterations: 1" in body
        assert "$0." in body
        assert "`local`" in body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_strategy_disabled_pr_summary_does_not_post(
    tmp_path: Path,
) -> None:
    """`post_local_review_pr_summary: false` keeps the PR thread quiet."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_local_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            post_local_review_pr_summary=False,
        )
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")
        push_fn = AsyncMock()

        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        RunnerEvent(
                            kind="stdout",
                            line=json.dumps(
                                {
                                    "type": "result",
                                    "subtype": "success",
                                    "total_cost_usd": 0.01,
                                    "usage": {"input_tokens": 1, "output_tokens": 1},
                                }
                            ),
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
                "local_review": [
                    [
                        _codex_agent_message(
                            f"ok\n{VERDICT_APPROVED_MARKER}"
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh,
            workspace=workspace, push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        for call in gh.pr_comment.await_args_list:
            body = call.args[1] if len(call.args) >= 2 else call.kwargs.get("body")
            assert "local reviewer" not in body.lower(), (
                f"expected no local-review PR summary, but got: {body}"
            )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_binding_pr_summary_override_off_beats_global_on(
    tmp_path: Path,
) -> None:
    """Per-binding `post_local_review_pr_summary: false` must beat
    global `True`. Mirrors the cost-cap override pattern."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            agent="claude",
            review_strategy="local",
            reviewer_agent="codex",
            post_local_review_pr_summary=False,  # override
            linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            post_local_review_pr_summary=True,  # global ON
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")
        push_fn = AsyncMock()

        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        RunnerEvent(
                            kind="stdout",
                            line=json.dumps(
                                {
                                    "type": "result",
                                    "subtype": "success",
                                    "total_cost_usd": 0.01,
                                    "usage": {"input_tokens": 1, "output_tokens": 1},
                                }
                            ),
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
                "local_review": [
                    [
                        _codex_agent_message(f"ok\n{VERDICT_APPROVED_MARKER}"),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh,
            workspace=workspace, push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # Global says yes, binding says no — binding wins.
        for call in gh.pr_comment.await_args_list:
            body = call.args[1] if len(call.args) >= 2 else call.kwargs.get("body")
            assert "local reviewer" not in body.lower(), (
                f"binding override should have suppressed summary; got: {body}"
            )
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [LoopOutcome.EXHAUSTED, LoopOutcome.STUCK_LOOP])
async def test_local_strategy_non_convergence_parks_pr_in_needs_approval(
    tmp_path: Path,
    outcome: LoopOutcome,
) -> None:
    """EXHAUSTED/STUCK still pushes a PR but parks it for human approval.

    The last unresolved local-review findings must be present on Linear, and
    local-only mode must not fall back to `@codex review`.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_local_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")
        push_fn = AsyncMock()

        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        RunnerEvent(
                            kind="stdout",
                            line=json.dumps(
                                {
                                    "type": "result",
                                    "subtype": "success",
                                    "total_cost_usd": 0.01,
                                    "usage": {"input_tokens": 1, "output_tokens": 1},
                                }
                            ),
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh,
            workspace=workspace, push_fn=push_fn,
        )
        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=LoopResult(
                outcome=outcome,
                iterations=1,
                verdicts=(
                    LocalVerdict(
                        kind=LocalVerdictKind.CHANGES_REQUESTED,
                        findings="src/auth.py:12 missing token validation",
                    ),
                ),
                error=f"{outcome.value} test outcome",
            )
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])
        orch._run_local_review_phase.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001

        # Neither the @codex ping nor the local-review summary should fire.
        codex_calls = [
            c for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body"))
            == "@codex review"
        ]
        summary_calls = [
            c for c in gh.pr_comment.await_args_list
            if "local reviewer" in str(c).lower()
        ]
        assert codex_calls == []
        assert len(summary_calls) == 0
        gh.pr_create.assert_awaited_once()
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-na" in move_targets
        assert "state-bl" not in move_targets
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("src/auth.py:12 missing token validation" in body for body in posted)

        history = await db.runs.history_for_issue(conn, "iss-1")
        review_rows = [h for h in history if h.stage == "review"]
        assert len(review_rows) == 1
        assert review_rows[0].status == "needs_approval"
        assert "src/auth.py:12 missing token validation" in review_rows[0].termination_detail
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is None
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [
        LoopOutcome.REVIEWER_FAILED,
        LoopOutcome.FIX_RUN_FAILED,
        LoopOutcome.FIX_RUN_BLOCKED,
    ],
)
async def test_local_strategy_infra_failures_block_without_pr(
    tmp_path: Path,
    outcome: LoopOutcome,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_local_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")
        push_fn = AsyncMock()

        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        RunnerEvent(
                            kind="stdout",
                            line=json.dumps(
                                {
                                    "type": "result",
                                    "subtype": "success",
                                    "total_cost_usd": 0.01,
                                    "usage": {"input_tokens": 1, "output_tokens": 1},
                                }
                            ),
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=LoopResult(
                outcome=outcome,
                iterations=1,
                verdicts=(),
                error=f"{outcome.value} test outcome",
            )
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        orch._run_local_review_phase.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        push_fn.assert_not_awaited()
        gh.pr_create.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-bl" in move_targets
        assert "state-na" not in move_targets

        history = await db.runs.history_for_issue(conn, "iss-1")
        implement_rows = [h for h in history if h.stage == "implement"]
        assert len(implement_rows) == 1
        assert implement_rows[0].status == "failed"
        assert outcome.value in implement_rows[0].termination_detail
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_review_phase_exception_does_not_break_pipeline(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """The outer `try/except Exception` in `_run_local_review_phase`
    must swallow arbitrary errors and let the orchestrator proceed.

    Simulate an unexpected fault inside `run_local_review_session`
    (any non-LinearError / non-GitHubError exception). The phase
    should return `None`. With `remote_review: false` (local-only mode), that
    is an infrastructure failure: no PR is opened and the issue is blocked.
    """
    import symphony.orchestrator.poll as poll_mod

    async def _exploding_session(**_: object) -> None:
        raise RuntimeError("local-review session blew up")

    monkeypatch.setattr(
        poll_mod, "run_local_review_session", _exploding_session
    )

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_local_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")
        push_fn = AsyncMock()

        # Only the implement stage will spawn the runner; the local
        # review path explodes before the runner is touched.
        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        RunnerEvent(
                            kind="stdout",
                            line=json.dumps(
                                {
                                    "type": "result",
                                    "subtype": "success",
                                    "total_cost_usd": 0.01,
                                    "usage": {"input_tokens": 1, "output_tokens": 1},
                                }
                            ),
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh,
            workspace=workspace, push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        gh.pr_create.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-bl" in move_targets

        history = await db.runs.history_for_issue(conn, "iss-1")
        implement_rows = [h for h in history if h.stage == "implement"]
        assert len(implement_rows) == 1
        assert implement_rows[0].status == "failed"
        assert "local-review session failed" in implement_rows[0].termination_detail
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_strategy_does_not_post_codex_when_reviewer_fails(
    tmp_path: Path,
) -> None:
    """`remote_review: false` never pings `@codex`, even when the local
    reviewer subprocess fails to spawn. The old remote fallback that
    legacy `local` strategy used to fire here is gone — the issue blocks
    without opening a PR."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_local_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_create = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()

        runner = _StagedRunner(
            {
                "implement": [
                    [
                        RunnerEvent(kind="started", pid=4242),
                        RunnerEvent(
                            kind="stdout",
                            line=json.dumps(
                                {
                                    "type": "result",
                                    "subtype": "success",
                                    "total_cost_usd": 0.01,
                                    "usage": {"input_tokens": 1, "output_tokens": 1},
                                }
                            ),
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
                "local_review": [
                    [RunnerEvent(kind="spawn_failed", error="codex missing")],
                ],
            }
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # Reviewer crashed, but remote_review is off → no @codex ping or PR.
        codex_pings = [
            c
            for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body"))
            == "@codex review"
        ]
        assert codex_pings == [], (
            "local-only mode must not post @codex when the reviewer fails"
        )
        gh.pr_create.assert_not_awaited()
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-bl" in move_targets
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()
