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
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.github.client import GitHubError
from symphony.linear.client import LinearIssue
from symphony.linear.slash import SlashIntent, SlashKind
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
            raise AssertionError(f"unexpected stage {spec.stage!r}; remaining={self._scripts}")
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


def _init_git_workspace_with_base(workspace_path: Path) -> None:
    advance_head(workspace_path)
    subprocess.run(
        ["git", "branch", "trunk"],
        cwd=workspace_path,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.run(
        ["git", "checkout", "-q", "-b", "symphony/eng-1"],
        cwd=workspace_path,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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
        assert any("cap=2" in c.args[1] for c in linear.post_comment.await_args_list)
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
async def test_deliver_failed_retry_preserves_local_review_needs_approval_after_restart(
    tmp_path: Path,
) -> None:
    """A deliver_failed retry after restart must preserve non-approval.

    Without persisting the local-review outcome on the wait, reconstruction
    turns the missing in-memory verdict into APPROVED and silently bypasses the
    human-approval gate after the PR opens.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _local_binding()
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
        _init_git_workspace_with_base(workspace_path)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(side_effect=GitHubError("gh pr create: HTTP 401"))
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

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_DELIVER_FAILED
        assert wait.local_review_outcome == LoopOutcome.EXHAUSTED.value
        run_id = wait.run_id

        # Simulate a daemon restart: only DB state remains.
        orch._pending_deliveries.clear()  # noqa: SLF001
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        linear.move_issue.reset_mock()

        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            run_id,
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T01:00:00+00:00",
            ),
        )

        gh.ensure_pr.assert_awaited_once()
        assert workspace.acquire.await_count == 2
        assert workspace.release.call_count == 2
        assert len([s for s in runner.captured if s.stage == "implement"]) == 1
        codex_calls = [
            c
            for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body")) == "@codex review"
        ]
        assert codex_calls == []
        assert await db.operator_waits.get(conn, "iss-1") is None
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-na" in move_targets

        history = await db.runs.history_for_issue(conn, "iss-1")
        review_rows = [h for h in history if h.stage == "review"]
        assert len(review_rows) == 1
        assert review_rows[0].status == "needs_approval"
        assert review_rows[0].termination_detail == "local-review ended with exhausted"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_deliver_failed_retry_adopts_live_review_run_without_duplicate_handoff(
    tmp_path: Path,
) -> None:
    """If handoff fails after the Review run starts, `$retry` adopts it.

    The resumed delivery must not repost `@codex review`, but it must reassert
    the Linear Review lane because `deliver_failed` parking moved the issue to
    Needs Approval. A successful resume must repair the Implement run status
    and preserve the PR row's original review-cycle timestamp.
    """
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
        _init_git_workspace_with_base(workspace_path)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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
            return_value=LoopResult(outcome=LoopOutcome.APPROVED, iterations=1, verdicts=())
        )
        orch._post_local_review_pr_summary = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=[RuntimeError("summary write failed"), None]
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_DELIVER_FAILED
        run_id = wait.run_id

        codex_calls = [
            c
            for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body")) == "@codex review"
        ]
        assert len(codex_calls) == 1
        assert [s.stage for s in runner.captured].count("implement") == 1

        history = await db.runs.history_for_issue(conn, "iss-1")
        implement = next(run for run in history if run.stage == "implement")
        review_rows = [run for run in history if run.stage == "review"]
        assert implement.status == "failed"
        assert len(review_rows) == 1
        assert review_rows[0].status == "running"
        issue_pr = await db.issue_prs.get_for_issue(conn, issue_id="iss-1")
        assert issue_pr is not None
        original_pr_created_at = issue_pr.created_at
        await db.review_state.set_signature(conn, "iss-1", "codex_inline:stale")
        await db.review_state.bump_iteration(conn, "iss-1")
        await db.review_state.bump_ci_fetch_failures(conn, "iss-1")
        await db.review_state.set_codex_lgtm_comment_id(conn, "iss-1", "comment-42")

        parked_issue = _issue()
        parked_issue.state_id = "state-na"
        parked_issue.state_name = "Needs Approval"
        parked_issue.state_type = "started"
        linear.lookup_issue.return_value = parked_issue

        tasks = await orch._poll_review_runs()  # noqa: SLF001
        assert tasks == []
        history = await db.runs.history_for_issue(conn, "iss-1")
        review_rows = [run for run in history if run.stage == "review"]
        assert len(review_rows) == 1
        assert review_rows[0].status == "running"

        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            run_id,
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T01:00:00+00:00",
            ),
        )

        codex_calls = [
            c
            for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body")) == "@codex review"
        ]
        assert len(codex_calls) == 1
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert move_targets.count("state-review") == 2
        assert move_targets[-1] == "state-review"
        assert gh.ensure_pr.await_count == 2
        assert push_fn.await_count == 2
        assert orch._post_local_review_pr_summary.await_count == 2  # type: ignore[attr-defined]  # noqa: SLF001
        assert [s.stage for s in runner.captured].count("implement") == 1
        assert await db.operator_waits.get(conn, "iss-1") is None
        issue_pr = await db.issue_prs.get_for_issue(conn, issue_id="iss-1")
        assert issue_pr is not None
        assert issue_pr.created_at == original_pr_created_at
        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 1
        assert state.last_trigger_signature == "codex_inline:stale"
        assert state.ci_fetch_failures == 1
        assert state.codex_lgtm_comment_id == "comment-42"
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert len(candidates) == 1
        assert candidates[0].pr_number == 42

        history = await db.runs.history_for_issue(conn, "iss-1")
        implement = next(run for run in history if run.stage == "implement")
        review_rows = [run for run in history if run.stage == "review"]
        assert implement.status == "completed"
        assert implement.termination_kind == ""
        assert implement.termination_detail == ""
        assert len(review_rows) == 1
        assert review_rows[0].status == "running"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_deliver_failed_reject_interrupts_live_review_monitor(
    tmp_path: Path,
) -> None:
    """Rejecting a failed delivery handoff must not leave Review running."""
    conn = await db.connect(tmp_path / "s.sqlite")
    review_task: asyncio.Task[bool] | None = None
    try:
        binding = _hybrid_binding()
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        run_id = "implement-run"
        review_run_id = "review-run"

        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Add authentication",
            team_key="ENG",
        )
        await db.runs.create(
            conn,
            id=run_id,
            issue_id="iss-1",
            stage="implement",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id=review_run_id,
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id=run_id,
            kind=db.operator_waits.KIND_DELIVER_FAILED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at="2026-05-10T00:02:00+00:00",
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )

        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=MagicMock())
        orch._states = {"ENG": _states()}  # noqa: SLF001

        review_task = asyncio.create_task(asyncio.Event().wait())
        orch._review_poll_run_ids.add(review_run_id)  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = review_run_id  # noqa: SLF001
        orch._review_poll_run_tasks[review_run_id] = review_task  # noqa: SLF001

        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            run_id,
            SlashIntent(
                kind=SlashKind.REJECT,
                comment_id="c-reject",
                created_at="2026-05-10T00:03:00+00:00",
            ),
        )
        await asyncio.gather(review_task, return_exceptions=True)

        assert await db.operator_waits.get(conn, "iss-1") is None
        assert review_run_id not in orch._review_poll_run_ids  # noqa: SLF001
        assert "iss-1" not in orch._review_poll_issue_ids  # noqa: SLF001
        assert review_run_id not in orch._review_poll_run_tasks  # noqa: SLF001
        assert review_task.cancelled()

        history = await db.runs.history_for_issue(conn, "iss-1")
        review_rows = [run for run in history if run.stage == "review"]
        assert len(review_rows) == 1
        assert review_rows[0].status == "interrupted"
        assert review_rows[0].termination_kind == "cancelled"
    finally:
        if review_task is not None and not review_task.done():
            review_task.cancel()
            await asyncio.gather(review_task, return_exceptions=True)
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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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

        gh.ensure_pr.assert_awaited_once()
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        codex_calls = [
            c
            for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body")) == "@codex review"
        ]
        assert codex_calls == []
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-na" in move_targets
        assert "state-review" not in move_targets

        history = await db.runs.history_for_issue(conn, "iss-1")
        review_rows = [h for h in history if h.stage == "review"]
        assert len(review_rows) == 1
        assert review_rows[0].status == "needs_approval"
        assert "src/auth.py:12 missing token validation" in (review_rows[0].termination_detail)
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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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
                        _codex_agent_message(f"Looks clean to me.\n{VERDICT_APPROVED_MARKER}"),
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
        # codex's nested OS sandbox is bypassed (container is the boundary).
        assert "--dangerously-bypass-approvals-and-sandbox" in runner.captured[1].command
        assert "--sandbox" not in runner.captured[1].command

        # PR was still opened.
        gh.ensure_pr.assert_awaited_once()

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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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
                        _codex_agent_message(f"clean\n{VERDICT_APPROVED_MARKER}"),
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

        # Find the PR comment that's the local-review summary.
        summary_calls = [
            c for c in gh.pr_comment.await_args_list if "local reviewer" in str(c).lower()
        ]
        assert len(summary_calls) == 1, (
            f"expected one local-review summary PR comment, got: {gh.pr_comment.await_args_list}"
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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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
            c
            for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body")) == "@codex review"
        ]
        summary_calls = [
            c for c in gh.pr_comment.await_args_list if "local reviewer" in str(c).lower()
        ]
        assert codex_calls == []
        assert len(summary_calls) == 0
        gh.ensure_pr.assert_awaited_once()
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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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
        gh.ensure_pr.assert_not_awaited()
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
    # SYM-150: the local-review phase moved to `poll._lifecycle`, which is where
    # `run_local_review_session` is now looked up.
    from symphony.orchestrator.poll import _lifecycle as lifecycle_mod

    async def _exploding_session(**_: object) -> None:
        raise RuntimeError("local-review session blew up")

    monkeypatch.setattr(lifecycle_mod, "run_local_review_session", _exploding_session)

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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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

        gh.ensure_pr.assert_not_awaited()
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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
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
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body")) == "@codex review"
        ]
        assert codex_pings == [], "local-only mode must not post @codex when the reviewer fails"
        gh.ensure_pr.assert_not_awaited()
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-bl" in move_targets
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_review_deliver_failed_resumes_after_restart(
    tmp_path: Path,
) -> None:
    """A `local_review` binding whose `pr_create` fails after the completion
    gate parks `deliver_failed`. After a daemon restart drops the in-memory
    delivery stash, `$retry` must reconstruct the context as already-approved
    and resume delivery to an open PR + review stage — never dead-end in
    `_fail_review_run` ("local-only review did not approve"). Regression for
    the reconstructed-as-`None` gate bug on a local-review binding.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _local_binding()  # local_review True, remote_review False
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
        _init_git_workspace_with_base(workspace_path)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(side_effect=GitHubError("gh pr create: HTTP 401"))
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
                                    "usage": {
                                        "input_tokens": 7,
                                        "output_tokens": 11,
                                        "cache_creation_input_tokens": 13,
                                        "cache_read_input_tokens": 17,
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
        # Local review approves on the first (pre-PR) pass.
        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=LoopResult(outcome=LoopOutcome.APPROVED, iterations=1, verdicts=())
        )
        # Spy on the dead-end path the reconstructed-as-None bug took.
        orch._fail_review_run = AsyncMock(wraps=orch._fail_review_run)  # type: ignore[method-assign]  # noqa: SLF001
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        # pr_create raised after the completion gate → parked deliver_failed,
        # agent ran exactly once, PR not yet open.
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_DELIVER_FAILED
        run_id = wait.run_id
        push_fn.assert_awaited_once()
        assert len([s for s in runner.captured if s.stage == "implement"]) == 1

        # --- Daemon restart: the in-memory delivery stash is gone, so the
        # resume must reconstruct the context. ---
        orch._pending_deliveries.clear()  # noqa: SLF001
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")

        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            run_id,
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T01:00:00+00:00",
            ),
        )

        # Resumed delivery opened the PR and started the Review stage; it never
        # dead-ended in `_fail_review_run`, and the agent was not re-invoked.
        gh.ensure_pr.assert_awaited_once()
        orch._fail_review_run.assert_not_awaited()  # type: ignore[attr-defined]
        assert len([s for s in runner.captured if s.stage == "implement"]) == 1
        assert await db.operator_waits.get(conn, "iss-1") is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert any(h.stage == "review" for h in history), (
            "expected the resumed delivery to start the Review stage"
        )
        # The reconstructed synthetic-APPROVED result must not post a degenerate
        # "iterations: 0" local-review PR summary.
        summary_calls = [
            c for c in gh.pr_comment.await_args_list if "local reviewer" in str(c).lower()
        ]
        assert summary_calls == []
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        stage_done_posts = [body for body in posted if "**Implement → Review**" in body]
        assert len(stage_done_posts) == 1
        assert "Tokens: in 7 · out 11 · cache w 13 / r 17" in stage_done_posts[0]
    finally:
        await conn.close()
