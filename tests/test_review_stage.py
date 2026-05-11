"""Orchestrator-level wiring for the Review stage.

Acceptance criteria covered here:
- On Implement success, orchestrator posts `@codex review` on the PR and
  records a completed `runs` row with `stage='review'`.
- Fix-runs spawn the agent CLI configured on the binding (claude or
  codex), NOT the Codex GitHub bot. The bot is only consulted via the
  PR-comment side-channel.
- The iteration cap (12) routes the issue to `needs_approval` and posts
  the stuck-loop-escape comment.
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
from symphony.github.client import CheckRun, GitHub, GitHubError, PRChecks
from symphony.linear.client import LinearComment, LinearIssue
from symphony.orchestrator.poll import (
    Orchestrator,
    build_fix_runner_command,
    pr_number_from_url,
)


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        pass


class _BlockingRunner:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.captured_spec: RunnerSpec | None = None
        self.killed_run_ids: list[str] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        self.started.set()
        yield RunnerEvent(kind="started", pid=999)
        await self.release.wait()
        yield RunnerEvent(kind="exit", returncode=0)

    async def kill(self, run_id: str) -> None:
        self.killed_run_ids.append(run_id)
        self.release.set()


def _binding(
    *,
    agent: str = "claude",
    codex_model: str = "gpt-5.1-codex",
    issue_label: str | None = None,
) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent=agent,  # type: ignore[arg-type]
        codex_model=codex_model,
        issue_label=issue_label,
        branch_prefix="symphony",
        linear_states=LinearStates(ready="Todo"),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["feature"],
    )


def _issue_in_progress(*, labels: list[str] | None = None) -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-progress",
        state_name="In Progress",
        state_type="started",
        team_key="ENG",
        labels=labels if labels is not None else ["feature"],
    )


def _issue_done() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-done",
        state_name="Done",
        state_type="completed",
        team_key="ENG",
        labels=["feature"],
    )


def _comment(body: str, *, cid: str = "c1") -> LinearComment:
    return LinearComment(
        id=cid,
        body=body,
        created_at="2026-05-11T12:00:00+00:00",
        author_name="user",
        author_is_me=False,
        external_thread_type=None,
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


# --- Codex-bot ping --------------------------------------------------------


@pytest.mark.asyncio
async def test_implement_success_posts_codex_review_and_records_review_handoff(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
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

        result_line = json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "total_cost_usd": 0.10,
            }
        )
        events = [
            RunnerEvent(kind="started", pid=4242),
            RunnerEvent(kind="stdout", line=result_line),
            RunnerEvent(kind="exit", returncode=0),
        ]
        runner = _FakeRunner(events)

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        # The orchestrator posts `@codex review` on the PR with the right repo.
        gh.pr_comment.assert_awaited()
        codex_pings = [
            c for c in gh.pr_comment.await_args_list
            if c.kwargs.get("body") == "@codex review"
            or (len(c.args) >= 2 and c.args[1] == "@codex review")
        ]
        assert codex_pings, (
            "expected at least one `gh pr_comment` with body '@codex review' "
            f"but got {gh.pr_comment.await_args_list!r}"
        )

        # A review-stage monitor row exists for this issue. It remains live
        # so later ticks can poll CI and review signals.
        history = await db.runs.history_for_issue(conn, "iss-1")
        stages = [r.stage for r in history]
        assert "review" in stages
        review_runs = [r for r in history if r.stage == "review"]
        assert review_runs[0].status == "running"
        assert await db.runs.has_running_or_completed(conn, "iss-1") is True
    finally:
        await conn.close()


# --- CI red-check fix-runs -------------------------------------------------


async def _seed_active_review(
    conn,
    *,
    run_id: str = "review-run",
    failures: int = 0,
    signature: str = "",
    issue_label: str | None = None,
) -> None:  # type: ignore[no-untyped-def]
    await db.issues.upsert(
        conn,
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        team_key="ENG",
    )
    await db.review_state.begin_review(
        conn,
        "iss-1",
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        github_repo="org/repo",
        issue_label=issue_label,
    )
    for _ in range(failures):
        await db.review_state.bump_ci_fetch_failures(conn, "iss-1")
    if signature:
        await db.review_state.set_signature(conn, "iss-1", signature)
    await db.runs.create(
        conn,
        id=run_id,
        issue_id="iss-1",
        stage="review",
        status="running",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )


async def _poll_review_and_wait(orch: Orchestrator) -> list[asyncio.Task[None]]:
    tasks = await orch._poll_review_runs()  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)
    return tasks


@pytest.mark.asyncio
async def test_red_ci_dispatches_fix_run_with_log_tail_and_retriggers_review(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding(agent="codex", codex_model="gpt-5.1-codex-max")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [
                    CheckRun(
                        name="lint",
                        state="FAILURE",
                        bucket="fail",
                        link="https://github.com/org/repo/actions/runs/1/jobs/2",
                    )
                ]
            )
        )
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="ruff found a lint failure")
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=999),
                RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {
                            "type": "token_count",
                            "info": {
                                "total_token_usage": {
                                    "input_tokens": 1_000,
                                    "cached_input_tokens": 100,
                                    "output_tokens": 500,
                                }
                            },
                        }
                    ),
                ),
                RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {
                            "type": "turn.completed",
                            "usage": {
                                "input_tokens": 1_800,
                                "cached_input_tokens": 200,
                                "output_tokens": 900,
                            },
                        }
                    ),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )

        push_fn = AsyncMock()
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

        await _poll_review_and_wait(orch)

        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "review"
        assert runner.captured_spec.command[:7] == [
            "codex",
            "exec",
            "--json",
            "--sandbox",
            "workspace-write",
            "--model",
            "gpt-5.1-codex-max",
        ]
        prompt = runner.captured_spec.command[-1]
        assert prompt.startswith("# Failing check log tail")
        assert "ruff found a lint failure" in prompt
        assert "Failing required CI checks: lint" in prompt

        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.pr_comment.assert_awaited_with(42, "@codex review", repo="org/repo")

        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 1
        assert state.last_trigger_signature == "ci:head-sha:lint"
        assert state.ci_fetch_failures == 0
        history = await db.runs.history_for_issue(conn, "iss-1")
        monitor = next(r for r in history if r.id == "review-run")
        fix_runs = [r for r in history if r.stage == "review_fix"]
        assert len(fix_runs) == 1
        assert monitor.status == "running"
        assert monitor.pid is None
        assert monitor.cost_usd == pytest.approx(0.0)
        assert fix_runs[0].status == "completed"
        assert fix_runs[0].pid == 999
        assert fix_runs[0].cost_usd == pytest.approx(0.011025)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_red_ci_defers_signature_until_fix_run_succeeds(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="lint", state="FAILURE", bucket="fail", link=None)]
            )
        )
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()

        runner = _BlockingRunner()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        tasks: list[asyncio.Task[None]] = []
        try:
            tasks = await orch._poll_review_runs()  # noqa: SLF001
            await asyncio.wait_for(runner.started.wait(), timeout=1)

            state = await db.review_state.get(conn, "iss-1")
            assert state.iteration == 0
            assert state.last_trigger_signature == ""

            runner.release.set()
            await asyncio.gather(*tasks)

            state = await db.review_state.get(conn, "iss-1")
            assert state.iteration == 1
            assert state.last_trigger_signature == "ci:head-sha:lint"
        finally:
            runner.release.set()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_red_ci_workspace_failure_does_not_consume_iteration(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(side_effect=RuntimeError("workspace busy"))

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="lint", state="FAILURE", bucket="fail", link=None)]
            )
        )
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="lint failed")

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        await _poll_review_and_wait(orch)

        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 0
        assert state.last_trigger_signature == ""
        assert runner.captured_spec is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        monitor = next(r for r in history if r.id == "review-run")
        assert monitor.status == "failed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_red_ci_dedup_skips_identical_back_to_back_fix_run(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, signature="ci:head-sha:lint")
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="lint", state="FAILURE", bucket="fail", link=None)]
            )
        )
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock()

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        await _poll_review_and_wait(orch)

        assert runner.captured_spec is None
        gh.check_log_tail.assert_not_awaited()
        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_red_ci_head_lookup_failure_does_not_dedup_to_unknown_head(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, signature="ci:unknown-head:lint")
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [
                    CheckRun(
                        name="lint",
                        state="FAILURE",
                        bucket="fail",
                        link="https://github.com/org/repo/actions/runs/1/jobs/2",
                    )
                ]
            )
        )
        gh.head_sha = AsyncMock(side_effect=GitHubError("head lookup failed"))
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        await _poll_review_and_wait(orch)

        assert runner.captured_spec is not None
        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 1
        assert state.last_trigger_signature.startswith("ci:unknown-head-")
        assert state.last_trigger_signature != "ci:unknown-head:lint"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_pr_checks_success_resets_persisted_fetch_failure_counter(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, failures=4)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.head_sha = AsyncMock(return_value="head-sha")

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        await _poll_review_and_wait(orch)

        state = await db.review_state.get(conn, "iss-1")
        assert state.ci_fetch_failures == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_five_consecutive_pr_checks_failures_fail_review_run(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, failures=4)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh_shim = tmp_path / "gh"
        gh_shim.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "sys.stderr.write('network down\\n')\n"
            "sys.exit(1)\n"
        )
        gh_shim.chmod(0o755)

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=GitHub(gh_path=str(gh_shim)),
        )

        await _poll_review_and_wait(orch)

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[0].status == "failed"
        posted = linear.post_comment.await_args.args[1]
        assert "gh pr checks failed 5 consecutive times" in posted
        state = await db.review_state.get(conn, "iss-1")
        assert state.ci_fetch_failures == 5
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_active_review_uses_persisted_binding_when_label_removed(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, issue_label="feature")
        cfg = Config(
            repos=[_binding(issue_label="feature")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress(labels=[]))

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="lint", state="FAILURE", bucket="fail", link=None)]
            )
        )
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        await _poll_review_and_wait(orch)

        gh.pr_checks.assert_awaited_once_with(42, repo="org/repo")
        assert runner.captured_spec is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_active_review_does_not_rebind_stored_pr_to_different_repo(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, issue_label="feature")
        cfg = Config(
            repos=[
                _binding().model_copy(update={"github_repo": "org/other-repo"}),
            ],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        gh.pr_checks = AsyncMock()

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        tasks = await _poll_review_and_wait(orch)

        assert tasks == []
        gh.pr_checks.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[0].status == "failed"
        posted = linear.post_comment.await_args.args[1]
        assert "no longer matches any configured repository binding" in posted
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_active_review_closes_when_issue_leaves_in_progress(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_done())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        gh.pr_checks = AsyncMock()

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        tasks = await _poll_review_and_wait(orch)

        assert tasks == []
        gh.pr_checks.assert_not_awaited()
        linear.post_comment.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[0].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_fix_run_does_not_block_poll_tick(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.issues_in_state = AsyncMock(return_value=[])

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="lint", state="FAILURE", bucket="fail", link=None)]
            )
        )
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()

        runner = _BlockingRunner()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks: list[asyncio.Task[None]] = []
        try:
            tasks = await asyncio.wait_for(orch._tick(), timeout=1)  # noqa: SLF001
            await asyncio.wait_for(runner.started.wait(), timeout=1)

            linear.issues_in_state.assert_awaited_once_with("ENG", "Todo", None)
            assert len(tasks) == 1
            assert not tasks[0].done()
        finally:
            runner.release.set()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_respects_global_dispatch_limit(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding()],
            global_max_concurrent=1,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.head_sha = AsyncMock(return_value="head-sha")

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        await orch._global_dispatch_sem.acquire()  # noqa: SLF001
        tasks: list[asyncio.Task[None]] = []
        try:
            tasks = await orch._poll_review_runs()  # noqa: SLF001
            await asyncio.sleep(0)

            assert len(tasks) == 1
            assert not tasks[0].done()
            gh.pr_checks.assert_not_awaited()
        finally:
            orch._global_dispatch_sem.release()  # noqa: SLF001
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        gh.pr_checks.assert_awaited_once_with(42, repo="org/repo")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_queued_review_poll_revalidates_issue_state_before_ci(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding()],
            global_max_concurrent=1,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(
            side_effect=[_issue_in_progress(), _issue_done()]
        )

        gh = MagicMock()
        gh.pr_checks = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        await orch._global_dispatch_sem.acquire()  # noqa: SLF001
        tasks: list[asyncio.Task[None]] = []
        try:
            tasks = await orch._poll_review_runs()  # noqa: SLF001
            await asyncio.sleep(0)

            assert len(tasks) == 1
            assert not tasks[0].done()
        finally:
            orch._global_dispatch_sem.release()  # noqa: SLF001

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        gh.pr_checks.assert_not_awaited()
        assert linear.lookup_issue.await_args_list == [call("iss-1"), call("iss-1")]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[0].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("global_cap", "binding_cap"),
    [
        (0, 2),
        (1, 0),
    ],
)
async def test_review_poll_respects_zero_capacity_config(
    tmp_path: Path,
    global_cap: int,
    binding_cap: int,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        binding = _binding().model_copy(update={"max_concurrent": binding_cap})
        cfg = Config(
            repos=[binding],
            global_max_concurrent=global_cap,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())

        gh = MagicMock()
        gh.pr_checks = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        tasks = await _poll_review_and_wait(orch)

        assert tasks == []
        gh.pr_checks.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[0].status == "running"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stop_intent_kills_active_review_fix_run(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.comments_since = AsyncMock(return_value=[_comment("/stop")])

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="lint", state="FAILURE", bucket="fail", link=None)]
            )
        )
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()

        runner = _BlockingRunner()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        tasks = await orch._poll_review_runs()  # noqa: SLF001
        try:
            await asyncio.wait_for(runner.started.wait(), timeout=1)
            fix_run_id = orch._dispatch_run_ids["iss-1"]  # noqa: SLF001

            await orch._poll_slash_commands()  # noqa: SLF001

            assert runner.killed_run_ids == [fix_run_id]
            assert runner.release.is_set()
            monitor = (await db.runs.history_for_issue(conn, "iss-1"))[0]
            assert monitor.id == "review-run"
            assert monitor.pid is None
        finally:
            runner.release.set()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await conn.close()


# --- Fix-runs go through the binding agent ---------------------------------


def test_build_fix_runner_command_uses_claude_when_binding_is_claude() -> None:
    argv = build_fix_runner_command("claude", "fix this")
    assert argv[0] == "claude"
    assert "fix this" in argv


def test_build_fix_runner_command_uses_codex_when_binding_is_codex() -> None:
    argv = build_fix_runner_command(
        "codex",
        "fix this",
        codex_model="gpt-5.1-codex-max",
    )
    assert argv[:7] == [
        "codex",
        "exec",
        "--json",
        "--sandbox",
        "workspace-write",
        "--model",
        "gpt-5.1-codex-max",
    ]
    assert "fix this" in argv


def test_build_fix_runner_command_passes_configured_codex_model() -> None:
    argv = build_fix_runner_command(
        "codex",
        "fix this",
        codex_model="gpt-5.1-codex-max",
    )
    assert argv[:7] == [
        "codex",
        "exec",
        "--json",
        "--sandbox",
        "workspace-write",
        "--model",
        "gpt-5.1-codex-max",
    ]
    assert argv[-1] == "fix this"


# --- PR URL parser ---------------------------------------------------------


def test_pr_number_from_url_parses_github_url() -> None:
    assert pr_number_from_url("https://github.com/org/repo/pull/42") == 42
    assert pr_number_from_url("https://github.com/org/repo/pull/1234\n") == 1234


def test_pr_number_from_url_returns_none_for_garbage() -> None:
    assert pr_number_from_url("") is None
    assert pr_number_from_url("not a url") is None
