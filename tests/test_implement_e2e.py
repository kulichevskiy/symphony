"""End-to-end Implement dispatch with mocked external surfaces.

Drag a Linear issue into `ready` → orchestrator clones the workspace,
posts 🚀 comment, moves the issue to `in_progress`, spawns the agent
runner with the implement prompt, parses cost/tokens from streaming
JSON, persists run state to SQLite, opens a PR with the right title /
body, posts a stage-transition comment, and **halts** at "In Progress".

Review and Merge are out of scope for this slice.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.github.client import GitHubError
from symphony.linear.client import LinearComment, LinearError, LinearIssue
from symphony.linear.slash import SlashIntent, SlashKind
from symphony.orchestrator.poll import Orchestrator, _ImplementHandoff
from symphony.pipeline.local_review_loop import LoopOutcome, LoopResult

from ._workspace_helpers import advance_head


class _FakeRunner:
    def __init__(
        self,
        events: list[RunnerEvent],
        *,
        commit_on_implement: bool = False,
        dirty_on_implement: bool = False,
    ) -> None:
        self.events = events
        self.commit_on_implement = commit_on_implement
        self.dirty_on_implement = dirty_on_implement
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        # Simulate the agent committing its work so the completion gate sees
        # HEAD advance over the branch base.
        if self.commit_on_implement and spec.stage == "implement":
            advance_head(spec.workspace_path)
        # Simulate the agent editing files while investigating but NOT
        # committing — leaves an uncommitted (dirty) tree, no HEAD advance.
        if self.dirty_on_implement and spec.stage == "implement":
            (spec.workspace_path / "scratch.txt").write_text("investigated\n")
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        pass


class _ExplodingRunner:
    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        if False:
            yield RunnerEvent(kind="exit", returncode=0)
        raise RuntimeError("agent stream exploded")

    async def kill(self, run_id: str) -> None:
        pass


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _no_review_binding(*, auto_merge: bool) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        local_review=False,
        remote_review=False,
        auto_merge=auto_merge,
        linear_states=LinearStates(
            ready="Todo",
            local_code_review="",
            code_review="",
            needs_approval="Needs Approval",
        ),
    )


def _issue(
    *,
    state_id: str = "state-todo",
    state_name: str = "Todo",
) -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add authentication",
        description="Need OAuth login for the dashboard.",
        url="https://linear.app/team/issue/ENG-1",
        state_id=state_id,
        state_name=state_name,
        state_type="unstarted",
        team_key="ENG",
        labels=["feature", "backend"],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
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


@pytest.mark.asyncio
async def test_implement_dispatch_full_flow(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        log_root = tmp_path / "logs"
        cfg = Config(
            repos=[_binding()],
            log_root=log_root,
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

        result_line = json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "result": "Implemented OAuth login and committed.\n\nSYMPHONY_DONE",
                "total_cost_usd": 0.42,
                "usage": {
                    "input_tokens": 100,
                    "cache_creation_input_tokens": 30,
                    "cache_read_input_tokens": 40,
                    "output_tokens": 50,
                },
                "modelUsage": {
                    "claude-opus-4-8[1m]": {
                        "inputTokens": 100,
                        "outputTokens": 50,
                        "cacheCreationInputTokens": 30,
                        "cacheReadInputTokens": 40,
                    }
                },
            }
        )
        events = [
            RunnerEvent(kind="started", pid=4242),
            RunnerEvent(kind="stdout", line=json.dumps({"type": "system"})),
            RunnerEvent(kind="stdout", line=result_line),
            RunnerEvent(kind="exit", returncode=0),
        ]
        runner = _FakeRunner(events, commit_on_implement=True)

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

        # 🚀 starting comment + stage-transition comment.
        assert linear.post_comment.await_count == 2

        # Issue enters Implement, then moves into the configured Review lane
        # after the PR is opened.
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]

        # Workspace was acquired + released.
        workspace.acquire.assert_awaited_once()
        workspace.release.assert_called_once()

        # Runner was spawned with the implement prompt for the right agent.
        assert runner.captured_spec is not None
        assert runner.captured_spec.workspace_path == workspace_path
        assert runner.captured_spec.stage == "implement"
        assert runner.captured_spec.command[0] == "claude"
        prompt_arg = runner.captured_spec.command[-1]
        assert "Add authentication" in prompt_arg
        assert "Need OAuth login" in prompt_arg
        assert "feature" in prompt_arg
        assert "backend" in prompt_arg

        # Branch was pushed before PR open.
        push_fn.assert_awaited_once()

        # PR opened with the prescribed title and body.
        gh.ensure_pr.assert_awaited_once()
        kwargs = gh.ensure_pr.await_args.kwargs
        assert kwargs["title"] == "[ENG-1] Add authentication"
        assert kwargs["repo"] == "org/repo"
        assert kwargs["base"] == "trunk"
        assert kwargs["head"] == "symphony/eng-1"
        assert kwargs["linear_url"] == "https://linear.app/team/issue/ENG-1"
        gh.repo_default_branch.assert_awaited_once_with("org/repo")

        # Per-issue cost accumulated from streaming JSON.
        cost = await db.runs.cost_for_issue(conn, "iss-1")
        assert cost == pytest.approx(0.42)

        # Per-run log file exists at {log_root}/{run_id}.log and captured
        # the streaming JSON.
        logs = list(log_root.glob("*.log"))
        assert len(logs) == 1
        log_text = logs[0].read_text()
        assert '"type": "result"' in log_text or '"type":"result"' in log_text

        # Two run rows: the completed Implement run, and the live Review
        # monitor row recorded immediately after pinging `@codex review`
        # on the PR.
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 2
        assert history[0].stage == "implement"
        assert history[0].status == "completed"
        assert history[0].termination_kind == ""
        assert history[0].termination_detail == ""
        assert history[0].pid == 4242
        assert history[0].input_tokens == 100
        assert history[0].output_tokens == 50
        assert history[0].cache_write_tokens == 30
        assert history[0].cache_read_tokens == 40

        # Per-(provider, model) attribution written at run end, summing back
        # to the run-level token columns (±0 for Claude's exact split).
        per_model = await db.run_model_usage.list_for_run(conn, history[0].id)
        assert len(per_model) == 1
        assert per_model[0].provider == "claude"
        assert per_model[0].model == "claude-opus-4-8[1m]"
        assert per_model[0].input_tokens == history[0].input_tokens
        assert per_model[0].output_tokens == history[0].output_tokens
        assert per_model[0].cache_write_tokens == history[0].cache_write_tokens
        assert per_model[0].cache_read_tokens == history[0].cache_read_tokens

        assert history[1].stage == "review"
        assert history[1].status == "running"
        assert await db.runs.has_running_or_completed(conn, "iss-1") is True
        gh.pr_comment.assert_awaited_with(42, "@codex review", repo="org/repo")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_start_review_stage_writes_review_state_before_live_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        gh = MagicMock()
        gh.pr_comment = AsyncMock()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Add authentication",
            team_key="ENG",
        )

        observed_pr_numbers: list[int | None] = []
        real_create_if_no_active = db.runs.create_if_no_active

        async def create_if_no_active_spy(*args: object, **kwargs: object) -> bool:
            state = await db.review_state.get(conn, "iss-1")
            observed_pr_numbers.append(state.pr_number)
            return await real_create_if_no_active(  # type: ignore[arg-type]
                *args, **kwargs
            )

        monkeypatch.setattr(db.runs, "create_if_no_active", create_if_no_active_spy)

        run = await orch._start_review_stage(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            storage_issue_id="iss-1",
            pr_url="https://github.com/org/repo/pull/42",
            post_codex_review=False,
        )

        assert observed_pr_numbers == [42]
        assert run.stage == "review"
        assert run.status == "running"
    finally:
        await conn.close()


@pytest.mark.parametrize("auto_merge", [True, False])
@pytest.mark.asyncio
async def test_false_false_review_binding_opens_pr_without_review_stage(
    tmp_path: Path,
    auto_merge: bool,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=auto_merge)
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
        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            commit_on_implement=True,
        )

        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Add authentication",
            team_key="ENG",
        )
        await db.review_state.begin_review(
            conn,
            "iss-1",
            pr_number=41,
            pr_url="https://github.com/org/repo/pull/41",
            github_repo="org/repo",
            issue_label="feature",
        )
        await db.review_state.set_signature(conn, "iss-1", "codex_inline:stale")
        await db.review_state.bump_iteration(conn, "iss-1")
        await db.review_state.bump_ci_fetch_failures(conn, "iss-1")
        await db.review_state.set_codex_lgtm_comment_id(conn, "iss-1", "comment-41")

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

        await _scan_and_wait(orch, binding)

        gh.ensure_pr.assert_awaited_once()
        gh.pr_comment.assert_not_awaited()
        assert linear.move_issue.await_args_list == [call("iss-1", "state-progress")]

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement"]
        assert history[0].status == "completed"

        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert len(candidates) == 1
        assert candidates[0].pr_number == 42
        assert candidates[0].github_repo == "org/repo"
        assert candidates[0].binding_key

        review = await db.review_state.get(conn, "iss-1")
        assert review.pr_number == 42
        assert review.pr_url == "https://github.com/org/repo/pull/42"
        assert review.github_repo == "org/repo"
        assert review.issue_label == ""
        assert review.iteration == 0
        assert review.last_trigger_signature == ""
        assert review.ci_fetch_failures == 0
        assert review.codex_lgtm_comment_id == ""
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_dispatch_falls_back_when_base_lookup_fails(
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
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(side_effect=GitHubError("boom"))

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner(
                [RunnerEvent(kind="exit", returncode=0)], commit_on_implement=True
            ),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        gh.ensure_pr.assert_awaited_once()
        assert gh.ensure_pr.await_args.kwargs["base"] is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        # Implement run completes, Review monitor is recorded.
        assert [r.stage for r in history] == ["implement", "review"]
        assert history[0].status == "completed"
        assert history[1].status == "running"
    finally:
        await conn.close()


def _blocked_runner(message: str) -> _FakeRunner:
    result_line = json.dumps(
        {"type": "result", "subtype": "success", "result": message}
    )
    return _FakeRunner(
        [
            RunnerEvent(kind="started", pid=4242),
            RunnerEvent(kind="stdout", line=result_line),
            RunnerEvent(kind="exit", returncode=0),
        ],
        # No commit: a blocked agent leaves HEAD where it was.
        commit_on_implement=False,
    )


async def _run_blocked_dispatch(
    tmp_path: Path, conn: object, runner: _FakeRunner
) -> object:
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
    gh.ensure_pr = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="trunk")
    push_fn = AsyncMock()

    orch = Orchestrator(
        cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
    )
    orch._states = {"ENG": _states()}  # noqa: SLF001
    await _scan_and_wait(orch, cfg.repos[0])
    return gh, push_fn


@pytest.mark.asyncio
async def test_implement_blocked_marker_captures_reason_and_skips_push(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        reason = "authorize the Supabase MCP at https://example.com/oauth then $retry"
        runner = _blocked_runner(f"Set up the scaffold.\n\nSYMPHONY_BLOCKED: {reason}")
        gh, push_fn = await _run_blocked_dispatch(tmp_path, conn, runner)

        # A blocked run never opens a PR or pushes.
        gh.ensure_pr.assert_not_awaited()
        push_fn.assert_not_awaited()

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].stage == "implement"
        assert history[0].termination_kind == "blocked"
        # Reason captured verbatim on the run record.
        assert history[0].termination_detail == reason
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_blocked_classifier_fallback_for_mch14(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        # Verbatim MCH-14 final message: no marker, no commits — the classifier
        # fallback must recognise the human-action ask as blocked, not completed.
        message = (
            "I need you to authorize the Supabase MCP server before I can "
            "continue. Please open the URL and approve access, then let me know."
        )
        runner = _blocked_runner(message)
        gh, push_fn = await _run_blocked_dispatch(tmp_path, conn, runner)

        gh.ensure_pr.assert_not_awaited()
        push_fn.assert_not_awaited()

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].termination_kind == "blocked"
        assert "authorize" in history[0].termination_detail.lower()
    finally:
        await conn.close()


def _api_error_runner(status: int) -> _FakeRunner:
    """An implement runner that exits 0 carrying only a transient provider API
    error (claude synthetic assistant + `is_error`/`api_error_status` result)
    and no completion marker — HEAD never advances."""
    text = f'API Error: {status} {{"type":"error","error":{{"message":"overloaded"}}}}'
    return _FakeRunner(
        [
            RunnerEvent(kind="started", pid=4242),
            RunnerEvent(
                kind="stdout",
                line=json.dumps(
                    {
                        "type": "assistant",
                        "message": {
                            "model": "<synthetic>",
                            "content": [{"type": "text", "text": text}],
                        },
                    }
                ),
            ),
            RunnerEvent(
                kind="stdout",
                line=json.dumps(
                    {
                        "type": "result",
                        "subtype": "error_during_execution",
                        "is_error": True,
                        "result": text,
                        "api_error_status": status,
                    }
                ),
            ),
            RunnerEvent(kind="exit", returncode=0),
        ],
        commit_on_implement=False,
    )


@pytest.mark.asyncio
async def test_implement_transient_api_error_surfaces_in_termination_detail(
    tmp_path: Path,
) -> None:
    """An implement run that exits 0 with only an `API Error: 500` fails the
    completion gate, and the termination detail carries the real cause instead
    of the generic completion-contract text."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        runner = _api_error_runner(500)
        gh, push_fn = await _run_blocked_dispatch(tmp_path, conn, runner)

        gh.ensure_pr.assert_not_awaited()
        push_fn.assert_not_awaited()

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].stage == "implement"
        assert history[0].status == "failed"
        assert history[0].termination_detail.startswith("API Error: 500")
        assert "completion contract" not in history[0].termination_detail
    finally:
        await conn.close()


def _head_sha(workspace_path: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=workspace_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


async def _run_already_satisfied_dispatch(
    tmp_path: Path,
    conn: object,
    ref: str,
    *,
    fail_done_move: bool = False,
    dirty: bool = False,
) -> tuple[object, object, object]:
    """Dispatch an implement run whose agent emits SYMPHONY_ALREADY_DONE (no
    commit) in a real git workspace. ``{head}`` in *ref* is substituted with
    the workspace HEAD sha (a real ancestor). When *fail_done_move* is set, the
    Linear move to the Done lane raises (the move to any other lane still
    succeeds). When *dirty* is set, the agent leaves an uncommitted file in the
    workspace. Returns (gh, push_fn, linear)."""
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
    if fail_done_move:

        async def _move(issue_id: str, state_id: str) -> None:
            if state_id == "state-done":
                raise LinearError("Done transition refused")

        linear.move_issue.side_effect = _move

    workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
    workspace_path.mkdir(parents=True)
    _init_git_workspace_with_base(workspace_path)
    ref = ref.replace("{head}", _head_sha(workspace_path))
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=workspace_path)
    workspace.release = MagicMock()

    gh = MagicMock()
    gh.ensure_pr = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="trunk")
    push_fn = AsyncMock()

    # No commit: the scope already landed, so HEAD does not advance.
    runner = _blocked_runner(f"All criteria already met.\n\nSYMPHONY_ALREADY_DONE: {ref}")
    runner.dirty_on_implement = dirty

    orch = Orchestrator(
        cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
    )
    orch._states = {"ENG": _states()}  # noqa: SLF001
    await _scan_and_wait(orch, cfg.repos[0])
    return gh, push_fn, linear


@pytest.mark.asyncio
async def test_already_satisfied_closes_done_without_pr_or_wait(tmp_path: Path) -> None:
    """SYMPHONY_ALREADY_DONE naming a commit that IS an ancestor of HEAD: the
    issue closes as Done with an auto-comment, no PR/push, no operator wait."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        gh, push_fn, linear = await _run_already_satisfied_dispatch(
            tmp_path, conn, "{head} (org/repo#291)"
        )

        # No PR opened, nothing pushed.
        gh.ensure_pr.assert_not_awaited()
        push_fn.assert_not_awaited()

        # Issue moved to the terminal Done lane.
        assert call("iss-1", "state-done") in linear.move_issue.await_args_list

        # Run completed (not failed); no operator wait raised.
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].stage == "implement"
        assert history[0].status == "completed"
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_already_satisfied_unverifiable_ref_still_fails(tmp_path: Path) -> None:
    """Guard preserved: an already-done claim whose ref is NOT an ancestor of
    HEAD must not auto-close — it falls back to the failed no-op path."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        # A hex SHA that does not exist in the workspace history.
        gh, push_fn, linear = await _run_already_satisfied_dispatch(
            tmp_path, conn, "deadbeefdeadbeef"
        )

        gh.ensure_pr.assert_not_awaited()
        push_fn.assert_not_awaited()

        # Never closed as Done.
        assert call("iss-1", "state-done") not in linear.move_issue.await_args_list

        # Parked as a failed implement run on an operator wait.
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status != "completed"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_already_satisfied_done_move_failure_parks_instead_of_stranding(
    tmp_path: Path,
) -> None:
    """A verifiable already-done ref whose move to Done raises must NOT complete
    the run: completing it would strand the issue in In Progress with no PR and
    no `$retry` path. The close bails to the failed/operator-wait path instead."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        gh, push_fn, linear = await _run_already_satisfied_dispatch(
            tmp_path, conn, "{head} (org/repo#291)", fail_done_move=True
        )

        gh.ensure_pr.assert_not_awaited()
        push_fn.assert_not_awaited()

        # The Done transition was attempted but raised, so the run must not be
        # marked completed — it parks on an operator wait with a `$retry` path.
        assert call("iss-1", "state-done") in linear.move_issue.await_args_list
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status != "completed"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_already_satisfied_dirty_tree_does_not_close(tmp_path: Path) -> None:
    """Guard preserved: an already-done claim whose ref IS a verifiable ancestor
    but whose workspace has uncommitted edits must NOT auto-close. A dirty tree
    means the agent did work it failed to commit — the no-op claim is false, so
    it falls back to the failed no-op path instead of closing as Done."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        gh, push_fn, linear = await _run_already_satisfied_dispatch(
            tmp_path, conn, "{head} (org/repo#291)", dirty=True
        )

        gh.ensure_pr.assert_not_awaited()
        push_fn.assert_not_awaited()

        # Never closed as Done despite the verifiable ref.
        assert call("iss-1", "state-done") not in linear.move_issue.await_args_list

        # Parked as a failed implement run on an operator wait.
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status != "completed"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_blocked_run_opens_wait_then_retry_resumes_fresh_run_with_handoff(
    tmp_path: Path,
) -> None:
    """blocked → IMPLEMENT_BLOCKED wait + verbatim handoff comment; survives a
    daemon restart; `$retry` clears the wait and dispatches a FRESH implement
    run in the same workspace whose prompt carries the handoff block and which
    sees the prior run's uncommitted work."""
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
        _init_git_workspace(workspace_path)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        reason = "authorize the Supabase MCP at https://example.com/oauth then $retry"

        def _leave_uncommitted_work(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                (workspace_path / "wip.py").write_text("partial work\n")

        blocked_runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {
                            "type": "result",
                            "subtype": "success",
                            "result": f"Scaffolded the client.\n\nSYMPHONY_BLOCKED: {reason}",
                        }
                    ),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_leave_uncommitted_work,
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=blocked_runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # Blocked run parks the issue — no PR.
        gh.ensure_pr.assert_not_awaited()

        # A dedicated IMPLEMENT_BLOCKED wait was opened (not IMPLEMENT_FAILED).
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_BLOCKED

        # The handoff comment reproduces the verbatim human-action ask + $retry.
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any(reason in body and "$retry" in body for body in posted), (
            "expected a blocked handoff comment with the verbatim reason"
        )

        # Uncommitted work is left in the workspace.
        assert (workspace_path / "wip.py").exists()

        # --- Daemon restart: the wait is reloaded from SQLite. ---
        retry_comment = LinearComment(
            id="c-retry",
            body="$retry token=sk-operator-123",
            created_at="2026-05-10T01:00:00+00:00",
            author_name="user",
            author_is_me=False,
            external_thread_type=None,
        )
        fresh_runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=5151),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )
        restarted_linear = AsyncMock()
        restarted_linear.issues_in_state = AsyncMock(return_value=[_issue()])
        restarted_linear.lookup_issue = AsyncMock(return_value=_issue())
        restarted_linear.comments_since = AsyncMock(return_value=[retry_comment])
        restarted_linear.move_issue = AsyncMock(return_value=None)
        restarted_linear.post_comment = AsyncMock(return_value="cmt-2")
        restarted = Orchestrator(
            cfg,
            restarted_linear,
            conn,
            runner=fresh_runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        restarted._states = {"ENG": _states()}  # noqa: SLF001

        # `$retry` moves the issue back to Ready and clears the wait.
        await restarted._poll_slash_commands()  # noqa: SLF001
        restarted_linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
        assert await db.operator_waits.get(conn, "iss-1") is None

        # The fresh implement run reuses the same workspace; the prior
        # uncommitted work is still there.
        assert (workspace_path / "wip.py").exists()

        # Dispatch the fresh run; its prompt carries the handoff block.
        await restarted._dispatch_one(cfg.repos[0], _issue())  # noqa: SLF001
        assert fresh_runner.specs, "expected a fresh implement run to be dispatched"
        fresh_prompt = fresh_runner.specs[-1].command[-1]
        assert reason in fresh_prompt
        assert "token=sk-operator-123" in fresh_prompt
        assert "git status" in fresh_prompt
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pr_error", "expected_error_text"),
    [
        (GitHubError("gh pr create: HTTP 401"), "HTTP 401"),
        (TimeoutError("gh timed out"), "gh timed out"),
    ],
)
async def test_pr_create_failure_parks_deliver_failed_then_retry_opens_pr(
    tmp_path: Path,
    pr_error: Exception,
    expected_error_text: str,
) -> None:
    """A post-completion-gate delivery failure (pr_create raises) parks the
    issue as `deliver_failed` — NOT implement_failed — without re-dispatching
    the agent or rewinding to the ready lane. `$retry` resumes the delivery
    path: it opens the PR and advances to review/merge, never re-running the
    agent or the completion gate (so it can't dead-end on "HEAD did not
    advance"). Regression for the VIB-203 transient `gh pr create` 401 loop.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
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
        gh.ensure_pr = AsyncMock(side_effect=pr_error)
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()

        def _commit(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                advance_head(spec.workspace_path)

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit,
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

        await _scan_and_wait(orch, binding)

        # pr_create raised → issue parked as deliver_failed (not implement_failed).
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_DELIVER_FAILED
        run_id = wait.run_id

        # The branch was pushed and the PR open was attempted exactly once.
        push_fn.assert_awaited_once()
        assert gh.ensure_pr.await_count == 1

        # The agent ran exactly once and the issue was NOT rewound to ready
        # (which would re-dispatch the agent into the re-park loop).
        assert len([s for s in runner.specs if s.stage == "implement"]) == 1
        assert call("iss-1", "state-todo") not in linear.move_issue.await_args_list

        # The parked comment captured the verbatim delivery error.
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any(expected_error_text in body for body in posted)

        # --- `$retry` resumes delivery; pr_create now succeeds. ---
        gh.ensure_pr = AsyncMock(
            return_value="https://github.com/org/repo/pull/42"
        )

        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            run_id,
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T01:00:00+00:00",
            ),
        )

        # PR opened on resume; the agent was NOT re-invoked.
        gh.ensure_pr.assert_awaited_once()
        assert workspace.acquire.await_count == 2
        assert workspace.release.call_count == 2
        assert len([s for s in runner.specs if s.stage == "implement"]) == 1

        # Wait cleared, run completed, merge candidate registered on the new PR.
        assert await db.operator_waits.get(conn, "iss-1") is None
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement"]
        assert history[0].status == "completed"
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert len(candidates) == 1
        assert candidates[0].pr_number == 42
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_deliver_failed_retry_keeps_wait_when_delivery_raises(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
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

        def _commit(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                advance_head(spec.workspace_path)

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit,
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

        await _scan_and_wait(orch, binding)
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        run_id = wait.run_id

        orch._deliver_implement_run = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=RuntimeError("unexpected delivery crash")
        )

        with pytest.raises(RuntimeError, match="unexpected delivery crash"):
            await orch._handle_slash_intent(  # noqa: SLF001
                "iss-1",
                run_id,
                SlashIntent(
                    kind=SlashKind.RETRY,
                    comment_id="c-retry",
                    created_at="2026-05-10T01:00:00+00:00",
                ),
            )

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_DELIVER_FAILED
        assert wait.run_id == run_id
        assert run_id in orch._deliver_failed_run_bindings  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_in_memory_deliver_retry_reacquires_and_reparks_stale_workspace(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
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
        stale_path = tmp_path / "ws" / "org_srepo" / "eng-1-stale"
        stale_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(side_effect=[workspace_path, stale_path])
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(side_effect=GitHubError("gh pr create: HTTP 401"))
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")
        push_fn = AsyncMock()

        def _commit(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                advance_head(spec.workspace_path)

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit,
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

        await _scan_and_wait(orch, binding)

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        run_id = wait.run_id

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

        assert workspace.acquire.await_count == 2
        assert workspace.release.call_count == 2
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_not_awaited()
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_DELIVER_FAILED
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any(
            "refusing to deliver without proving branch work" in body
            for body in posted
        )
        assert len([s for s in runner.specs if s.stage == "implement"]) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_no_review_deliver_retry_skips_duplicate_stage_done_after_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
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

        def _commit(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                advance_head(spec.workspace_path)

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit,
        )

        real_upsert = db.issue_prs.upsert
        upsert_calls = 0

        async def flaky_upsert(*args: object, **kwargs: object) -> None:
            nonlocal upsert_calls
            upsert_calls += 1
            if upsert_calls == 1:
                raise RuntimeError("issue_prs write failed")
            await real_upsert(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(db.issue_prs, "upsert", flaky_upsert)

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

        await _scan_and_wait(orch, binding)

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        run_id = wait.run_id
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert sum("**Implement → Merge**" in body for body in posted) == 1

        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            run_id,
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T01:00:00+00:00",
            ),
        )

        assert await db.operator_waits.get(conn, "iss-1") is None
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert sum("**Implement → Merge**" in body for body in posted) == 1
        assert upsert_calls == 2
        assert workspace.acquire.await_count == 2
        assert workspace.release.call_count == 2
        assert len([s for s in runner.specs if s.stage == "implement"]) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_first_handoff_persists_recovery_wait_before_completed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
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

        def _commit(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                advance_head(spec.workspace_path)

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit,
        )

        real_update_status = db.runs.update_status
        checked_completed = False

        async def crash_after_completed(
            *args: object,
            **kwargs: object,
        ) -> None:
            nonlocal checked_completed
            await real_update_status(*args, **kwargs)  # type: ignore[arg-type]
            run_id = str(args[1])
            status = str(args[2])
            if status != "completed":
                return
            wait = await db.operator_waits.get(conn, "iss-1")
            assert wait is not None
            assert wait.kind == db.operator_waits.KIND_DELIVER_FAILED
            assert wait.run_id == run_id
            checked_completed = True
            raise RuntimeError("daemon died after completed")

        monkeypatch.setattr(db.runs, "update_status", crash_after_completed)

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

        with pytest.raises(RuntimeError, match="daemon died after completed"):
            await _scan_and_wait(orch, binding)

        assert checked_completed
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_DELIVER_FAILED
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[0].status == "completed"
        assert await db.issue_prs.get_for_issue(conn, issue_id="iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_no_review_deliver_retry_skips_stage_done_when_handoff_metadata_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
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

        def _commit(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                advance_head(spec.workspace_path)

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit,
        )

        real_begin_review = db.review_state.begin_review
        begin_calls = 0

        async def flaky_begin_review(*args: object, **kwargs: object) -> None:
            nonlocal begin_calls
            begin_calls += 1
            if begin_calls == 1:
                raise RuntimeError("review_state write failed")
            await real_begin_review(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(db.review_state, "begin_review", flaky_begin_review)

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

        await _scan_and_wait(orch, binding)

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        run_id = wait.run_id
        state = await db.review_state.get(conn, "iss-1")
        assert state.pr_number is None
        issue_pr = await db.issue_prs.get_for_issue(conn, issue_id="iss-1")
        assert issue_pr is None
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert sum("**Implement → Merge**" in body for body in posted) == 1

        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            run_id,
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T01:00:00+00:00",
            ),
        )

        assert await db.operator_waits.get(conn, "iss-1") is None
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert sum("**Implement → Merge**" in body for body in posted) == 1
        assert begin_calls == 2
        assert workspace.acquire.await_count == 2
        assert workspace.release.call_count == 2
        assert len([s for s in runner.specs if s.stage == "implement"]) == 1
    finally:
        await conn.close()


@pytest.mark.parametrize("base_branch", [None, "trunk"])
@pytest.mark.asyncio
async def test_reconstructed_deliver_retry_reparks_when_branch_work_unproven(
    tmp_path: Path,
    base_branch: str | None,
) -> None:
    """A restart-reconstructed delivery retry must prove the branch has work."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        run_id = "implement-run"

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
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id=run_id,
            kind=db.operator_waits.KIND_DELIVER_FAILED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at="2026-05-10T00:01:00+00:00",
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        if base_branch is None:
            gh.repo_default_branch = AsyncMock(
                side_effect=GitHubError("default branch unavailable")
            )
        else:
            gh.repo_default_branch = AsyncMock(return_value=base_branch)
        gh.ensure_pr = AsyncMock()
        push_fn = AsyncMock()

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        for i in range(2):
            await orch._handle_slash_intent(  # noqa: SLF001
                "iss-1",
                run_id,
                SlashIntent(
                    kind=SlashKind.RETRY,
                    comment_id=f"c-retry-{i}",
                    created_at=f"2026-05-10T00:0{i + 2}:00+00:00",
                ),
            )
            wait = await db.operator_waits.get(conn, "iss-1")
            assert wait is not None
            assert wait.kind == db.operator_waits.KIND_DELIVER_FAILED

        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        assert workspace.acquire.await_count == 2
        assert workspace.release.call_count == 2
        assert run_id not in orch._pending_deliveries  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_dispatch_marks_failed_on_runner_error(tmp_path: Path) -> None:
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
        gh.ensure_pr = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        events = [
            RunnerEvent(kind="stderr", line="boom"),
            RunnerEvent(kind="exit", returncode=2),
        ]
        runner = _FakeRunner(events)

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # No PR opened on failure.
        gh.ensure_pr.assert_not_awaited()
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        assert history[0].termination_kind == "agent_nonzero_exit"
        assert history[0].termination_kind != "unknown"
        assert "return code 2" in history[0].termination_detail
        assert history[0].exit_returncode == 2
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
        assert wait.run_id == history[0].id
        # Failure comment is now posted so the operator knows what went wrong.
        posted_bodies = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any("Implement stage failed" in b for b in posted_bodies), (
            "expected a failed() comment to be posted"
        )
        assert any("$retry" in b for b in posted_bodies)
        assert not any("Will auto-retry shortly." in b for b in posted_bodies)

        # Even if Linear still reports the issue in the ready lane before the
        # state move is visible, the durable operator wait suppresses a retry
        # loop in the same process.
        assert await orch._scan_binding(cfg.repos[0]) == []  # noqa: SLF001

        # After a daemon restart, the persisted wait is restored and `$retry`
        # clears it by moving the issue back to Ready for the next poll.
        retry_comment = LinearComment(
            id="c-retry",
            body="$retry",
            created_at="2026-05-10T01:00:00+00:00",
            author_name="user",
            author_is_me=False,
            external_thread_type=None,
        )
        restarted_linear = AsyncMock()
        restarted_linear.comments_since = AsyncMock(return_value=[retry_comment])
        restarted_linear.move_issue = AsyncMock(return_value=None)
        restarted_linear.post_comment = AsyncMock(return_value="cmt-2")
        restarted = Orchestrator(
            cfg,
            restarted_linear,
            conn,
            runner=_FakeRunner([]),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        restarted._states = {"ENG": _states()}  # noqa: SLF001

        await restarted._poll_slash_commands()  # noqa: SLF001

        restarted_linear.move_issue.assert_awaited_once_with("iss-1", "state-todo")
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_after_agent_failure_with_commits_runs_agent_before_publish(
    tmp_path: Path,
) -> None:
    """A failed agent run can leave commits; `$retry` must still re-run the
    implementer instead of treating branch-ahead as safe to publish."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        def _commit_then_fail(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                (workspace_path / "partial.py").write_text("print('partial')\n")
                _git(workspace_path, "add", "-A")
                _git(workspace_path, "commit", "-m", "partial agent work")

        first_runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="stderr", line="boom"),
                RunnerEvent(kind="exit", returncode=2),
            ],
            on_run=_commit_then_fail,
        )
        first_linear = AsyncMock()
        first_linear.issues_in_state = AsyncMock(return_value=[_issue()])
        first_linear.lookup_issue = AsyncMock(return_value=_issue())
        first_linear.post_comment = AsyncMock(return_value="cmt-1")
        first_linear.move_issue = AsyncMock()
        first_push = AsyncMock()
        first_orch = Orchestrator(
            cfg,
            first_linear,
            conn,
            runner=first_runner,
            gh=gh,
            workspace=workspace,
            push_fn=first_push,
        )
        first_orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(first_orch, binding)

        assert [s.stage for s in first_runner.specs] == ["implement"]
        first_push.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        failed_run = history[0]
        assert failed_run.status == "failed"
        assert failed_run.termination_kind == "agent_nonzero_exit"
        assert await db.operator_waits.get(conn, "iss-1") is not None

        await first_orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            "iss-1",
            failed_run.id,
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T01:00:00+00:00",
                text="$retry",
            ),
        )
        assert await db.operator_waits.get(conn, "iss-1") is None

        def _commit_retry(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "retry work")

        retry_runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=5151),
                RunnerEvent(
                    kind="stdout",
                    line=_done_result_line("Retry implemented.\n\nSYMPHONY_DONE"),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit_retry,
        )
        retry_linear = AsyncMock()
        retry_linear.issues_in_state = AsyncMock(return_value=[_issue()])
        retry_linear.lookup_issue = AsyncMock(return_value=_issue())
        retry_linear.post_comment = AsyncMock(return_value="cmt-2")
        retry_linear.move_issue = AsyncMock()
        retry_push = AsyncMock()
        retry_orch = Orchestrator(
            cfg,
            retry_linear,
            conn,
            runner=retry_runner,
            gh=gh,
            workspace=workspace,
            push_fn=retry_push,
        )
        retry_orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(retry_orch, binding)

        assert [s.stage for s in retry_runner.specs] == ["implement"]
        retry_push.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_dispatch_marks_failed_on_runner_exception(
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
        gh.ensure_pr = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_ExplodingRunner(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        gh.ensure_pr.assert_not_awaited()
        workspace.release.assert_called_once()
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_manual_dispatch_failure_rolls_back_to_original_state(
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

        issue = _issue(state_id="state-blocked", state_name="Blocked")
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _FakeRunner(
            [
                RunnerEvent(kind="stderr", line="boom"),
                RunnerEvent(kind="exit", returncode=2),
            ]
        )
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

        await orch._dispatch_one(cfg.repos[0], issue)  # noqa: SLF001

        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-blocked"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failed_implement_stop_keeps_wait_when_blocked_state_missing(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(
            update={
                "linear_states": LinearStates(
                    ready="Todo", code_review="Needs Approval", blocked="Missing"
                )
            }
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Add authentication",
            team_key="ENG",
        )
        await db.runs.create(
            conn,
            id="failed-run",
            issue_id="iss-1",
            stage="implement",
            status="failed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(cfg, linear, conn, runner=_FakeRunner([]), gh=MagicMock())
        orch._states = {"ENG": {"Todo": "state-todo"}}  # noqa: SLF001
        await orch._track_implement_failed_wait(  # noqa: SLF001
            "iss-1",
            "failed-run",
            binding,
        )

        await orch._handle_implement_failed_slash_intent(  # noqa: SLF001
            "iss-1",
            "failed-run",
            SlashIntent(
                kind=SlashKind.STOP,
                comment_id="c-stop",
                created_at="2026-05-10T00:05:00+00:00",
            ),
        )

        linear.move_issue.assert_not_awaited()
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
        assert orch._dispatch_run_ids["iss-1"] == "failed-run"  # noqa: SLF001
        posted = [str(c.args[1]) for c in linear.post_comment.await_args_list]
        assert any("missing blocked state" in body for body in posted)
    finally:
        await conn.close()


class _RecordingRunner:
    """Like `_FakeRunner` but keeps every spec and supports a per-run hook."""

    def __init__(
        self,
        events: list[RunnerEvent],
        on_run: Callable[[RunnerSpec], None] | None = None,
    ) -> None:
        self.events = events
        self.specs: list[RunnerSpec] = []
        self.on_run = on_run

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.specs.append(spec)
        if self.on_run is not None:
            self.on_run(spec)
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        pass


def _git(workspace: Path, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-c", "user.name=t",
            "-c", "user.email=t@example.com",
            *args,
        ],
        cwd=workspace,
        check=True,
        capture_output=True,
    )


def _init_git_workspace(workspace: Path) -> None:
    _git(workspace, "init", "-q")
    _git(workspace, "commit", "--allow-empty", "-m", "init")


def _dirty_gate_fixture(tmp_path: Path) -> dict[str, object]:
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
    _init_git_workspace(workspace_path)
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=workspace_path)
    workspace.release = MagicMock()

    gh = MagicMock()
    gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
    gh.pr_comment = AsyncMock()
    gh.repo_default_branch = AsyncMock(return_value="trunk")

    return {
        "cfg": cfg,
        "linear": linear,
        "workspace_path": workspace_path,
        "workspace": workspace,
        "gh": gh,
        "push_fn": AsyncMock(),
    }


@pytest.mark.asyncio
async def test_dirty_tree_blocks_push_after_one_failed_fix_turn(
    tmp_path: Path,
) -> None:
    """Uncommitted files + a fix turn that doesn't clean up → no push, no
    PR, implement run fails into the operator-wait path with the file
    list on Linear."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        fx = _dirty_gate_fixture(tmp_path)
        workspace_path: Path = fx["workspace_path"]  # type: ignore[assignment]
        (workspace_path / "feature.py").write_text("print('hi')\n")

        def _advance_head_on_implement(spec: RunnerSpec) -> None:
            # The agent committed its work (HEAD advances) but left feature.py
            # uncommitted; the empty commit satisfies the completion gate so
            # the dirty-tree gate is the one that blocks the push.
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "agent work")

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_advance_head_on_implement,
        )
        orch = Orchestrator(
            fx["cfg"],
            fx["linear"],
            conn,
            runner=runner,
            gh=fx["gh"],
            workspace=fx["workspace"],
            push_fn=fx["push_fn"],
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, fx["cfg"].repos[0])  # type: ignore[union-attr]

        fx["push_fn"].assert_not_awaited()  # type: ignore[union-attr]
        fx["gh"].ensure_pr.assert_not_awaited()  # type: ignore[union-attr]

        # Exactly one fix turn after the implement turn.
        assert len(runner.specs) == 2
        fix_prompt = runner.specs[1].command[-1]
        assert "uncommitted" in fix_prompt
        assert "feature.py" in fix_prompt

        history = await db.runs.history_for_issue(conn, "iss-1")
        by_stage = {r.stage: r for r in history}
        assert by_stage["implement"].status == "failed"
        assert "implement_fix" in by_stage

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED

        posted = [
            str(c.args[1])
            for c in fx["linear"].post_comment.await_args_list  # type: ignore[union-attr]
        ]
        assert any("feature.py" in body for body in posted), (
            "expected the uncommitted file list in a Linear comment"
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_dirty_tree_fix_turn_commits_and_push_proceeds(
    tmp_path: Path,
) -> None:
    """The one fix turn commits the leftovers → push and PR happen."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        fx = _dirty_gate_fixture(tmp_path)
        workspace_path: Path = fx["workspace_path"]  # type: ignore[assignment]
        (workspace_path / "feature.py").write_text("print('hi')\n")

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )

        def _commit_on_fix_turn(spec: RunnerSpec) -> None:
            # Implement turn: agent commits its work (HEAD advances) but leaves
            # feature.py uncommitted, so the completion gate passes and the
            # dirty-tree gate's single fix turn cleans up the leftover.
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "agent work")
            if len(runner.specs) == 2:
                _git(workspace_path, "add", "-A")
                _git(workspace_path, "commit", "-m", "commit leftovers")

        runner.on_run = _commit_on_fix_turn

        orch = Orchestrator(
            fx["cfg"],
            fx["linear"],
            conn,
            runner=runner,
            gh=fx["gh"],
            workspace=fx["workspace"],
            push_fn=fx["push_fn"],
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, fx["cfg"].repos[0])  # type: ignore[union-attr]

        assert len(runner.specs) == 2
        fx["push_fn"].assert_awaited_once()  # type: ignore[union-attr]
        fx["gh"].ensure_pr.assert_awaited_once()  # type: ignore[union-attr]

        history = await db.runs.history_for_issue(conn, "iss-1")
        by_stage = {r.stage: r for r in history}
        assert by_stage["implement"].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_clean_tree_skips_fix_turn_entirely(tmp_path: Path) -> None:
    """Clean working tree → no extra agent turn, push proceeds."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        fx = _dirty_gate_fixture(tmp_path)
        workspace_path: Path = fx["workspace_path"]  # type: ignore[assignment]

        def _advance_head_on_implement(spec: RunnerSpec) -> None:
            # Agent committed its work and left a clean tree; the empty commit
            # satisfies the completion gate so push proceeds with no fix turn.
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "agent work")

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_advance_head_on_implement,
        )
        orch = Orchestrator(
            fx["cfg"],
            fx["linear"],
            conn,
            runner=runner,
            gh=fx["gh"],
            workspace=fx["workspace"],
            push_fn=fx["push_fn"],
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, fx["cfg"].repos[0])  # type: ignore[union-attr]

        assert len(runner.specs) == 1
        fx["push_fn"].assert_awaited_once()  # type: ignore[union-attr]
        fx["gh"].ensure_pr.assert_awaited_once()  # type: ignore[union-attr]
    finally:
        await conn.close()


def _done_result_line(message: str) -> str:
    return json.dumps({"type": "result", "subtype": "success", "result": message})


async def _seed_publish_failed_implement_run(
    conn, *, run_id: str = "publish-failed-run"
) -> None:
    issue = _issue()
    await db.issues.upsert(
        conn,
        id=issue.id,
        identifier=issue.identifier,
        title=issue.title,
        team_key=issue.team_key,
    )
    await db.runs.create(
        conn,
        id=run_id,
        issue_id=issue.id,
        stage="implement",
        status="running",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )
    await db.runs.update_status(
        conn,
        run_id,
        "failed",
        ended_at="2026-05-10T00:01:00+00:00",
        kind=db.runs.PUBLISH_FAILED_KIND,
        detail="push failed: boom",
    )


@pytest.mark.asyncio
async def test_branch_already_ahead_short_circuits_to_publish(tmp_path: Path) -> None:
    """An implement (re)dispatch on a branch already ahead of base skips the
    agent and the completion gate entirely and proceeds straight to the
    agent-free publish step (push + ensure_pr)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
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
        # HEAD is one commit ahead of `trunk` (the resolved base): this can
        # happen on a fresh dispatch when the branch already carries work.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        # Would advance HEAD / record a spec if the agent ran — it must not.
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        # The agent never ran: no completion gate, no fix turns.
        assert runner.specs == []
        # Publish ran: branch pushed, PR ensured (idempotently).
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [r.stage for r in history] == ["implement"]
        assert history[-1].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_review_infra_failure_retry_short_circuits_skips_agent(
    tmp_path: Path,
) -> None:
    """A prior implement run that failed at the local-review gate for a
    reviewer-infra reason (no verdict) is safe to resume agent-free: the
    commits already passed the completion gate. The $retry re-dispatch must
    skip the implementer (re-running it would find nothing to do and trip the
    "HEAD did not advance" contract) and proceed straight to publish.

    Regression for SYM-133: the reviewer 400'd, implement was re-dispatched,
    and the agent re-run failed because HEAD could not advance."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        # Prior implement run: passed completion gate, then the local-review
        # gate failed for a reviewer-infra reason — tagged accordingly.
        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        await db.runs.create(
            conn,
            id="local-review-infra-failed-run",
            issue_id=issue.id,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "local-review-infra-failed-run",
            "failed",
            ended_at="2026-05-10T00:01:00+00:00",
            kind=db.runs.LOCAL_REVIEW_INFRA_FAILED_KIND,
            detail="reviewer emitted no verdict marker",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        # HEAD already carries the prior run's committed work, one ahead of base.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior implement work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        # The agent was NOT re-invoked despite the prior failed implement run.
        assert runner.specs == []
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        history = await db.runs.history_for_issue(conn, "iss-1")
        # Prior failed run plus the resumed run that short-circuited to publish.
        assert [r.stage for r in history] == ["implement", "implement"]
        assert history[-1].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_untagged_prior_failure_with_sibling_local_review_runs_agent(
    tmp_path: Path,
) -> None:
    """An implement run that failed with an untagged/heuristic kind (e.g.
    "unknown") is NOT treated as agent-free-resumable, even when a sibling
    failed local-review run exists — only the explicit
    LOCAL_REVIEW_INFRA_FAILED_KIND qualifies. A fix-run failure can leave
    partial commits, so an untagged failure must re-run the implementer."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        # Prior implement run failed with a legacy/heuristic kind (no explicit
        # LOCAL_REVIEW_INFRA_FAILED_KIND), as pre-fix rows were written.
        await db.runs.create(
            conn,
            id="legacy-implement-run",
            issue_id=issue.id,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "legacy-implement-run",
            "failed",
            ended_at="2026-05-10T00:01:00+00:00",
            kind="unknown",
            detail="reviewer emitted no verdict marker",
        )
        # Its sibling local-review run, terminally failed — the durable signal.
        await db.runs.create(
            conn,
            id="legacy-local-review-run",
            issue_id=issue.id,
            stage="local_review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:30+00:00",
        )
        await db.runs.update_status(
            conn,
            "legacy-local-review-run",
            "failed",
            ended_at="2026-05-10T00:01:00+00:00",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior implement work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        # Untagged failure → the implementer is re-dispatched (not skipped),
        # and publish is not reached on this no-op recording run.
        assert runner.specs != []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_untagged_failure_noop_with_done_marker_delivers_branch_ahead(
    tmp_path: Path,
) -> None:
    """SYM-133 stall: a prior implement run failed with an untagged kind
    ("unknown"), so the re-dispatch is NOT short-circuited and the agent runs
    again. The branch already carries the prior run's committed work, so the
    agent correctly no-ops and emits SYMPHONY_DONE without advancing HEAD this
    run. That explicit vouch on a branch ahead of base must complete and deliver
    (push + ensure_pr) instead of failing the completion gate and trapping the
    issue in an endless implement-retry loop.

    Counterpart to test_untagged_prior_failure_with_sibling_local_review_runs_agent:
    same untagged prior failure, but here the re-run agent emits a DONE marker,
    which (with the branch ahead) is the agent affirming the branch is ready."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        # Prior implement run failed with an untagged/heuristic kind — NOT a
        # delivery- or local-review-resume kind, so the short-circuit is skipped
        # and the agent is re-dispatched.
        await db.runs.create(
            conn,
            id="prior-noop-implement-run",
            issue_id=issue.id,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "prior-noop-implement-run",
            "failed",
            ended_at="2026-05-10T00:01:00+00:00",
            kind="unknown",
            detail=(
                "implement run exited 0 but did not satisfy the completion "
                "contract: HEAD did not advance"
            ),
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        # HEAD carries the prior run's committed work, one ahead of base.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior implement work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        # The re-run agent does NOT advance HEAD (no on_run commit) but emits an
        # explicit SYMPHONY_DONE: it re-ran with the branch in view and vouched.
        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(
                    kind="stdout",
                    line=_done_result_line(
                        "Work already committed on this branch.\n\nSYMPHONY_DONE"
                    ),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        # The agent re-ran (not short-circuited), no-opped, but the branch-ahead
        # vouch delivered: pushed + PR ensured, run completed, no operator wait.
        assert [s.stage for s in runner.specs] == ["implement"]
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [r.stage for r in history] == ["implement", "implement"]
        assert history[-1].status == "completed"
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_setup_failure_releases_workspace_and_fails_run(
    tmp_path: Path,
) -> None:
    """If setup after acquire fails before the branch decision, the workspace
    is released and the implement run is marked failed instead of staying
    live."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._resolve_base_branch = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=RuntimeError("base boom")
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        workspace.release.assert_called_once()
        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert len(history) == 1
        assert history[0].status == "failed"
        assert "base boom" in history[0].termination_detail
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_with_pending_handoff_runs_agent_and_consumes_prompt(
    tmp_path: Path,
) -> None:
    """A blocked-run `$retry` handoff is an explicit reason to run the agent
    even when the branch is already ahead of base; the handoff must be included
    in the prompt and consumed by that same orchestrator."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior blocked work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        def _commit_handoff_retry(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                _git(workspace_path, "commit", "--allow-empty", "-m", "handoff retry")

        runner = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(
                    kind="stdout",
                    line=_done_result_line("Resumed and finished.\n\nSYMPHONY_DONE"),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit_handoff_retry,
        )
        push_fn = AsyncMock()
        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._implement_handoffs["iss-1"] = _ImplementHandoff(  # noqa: SLF001
            blocked_reason="authorize the deployment OAuth URL",
            operator_comment="$retry token=available",
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert [s.stage for s in runner.specs] == ["implement"]
        prompt = runner.specs[0].command[-1]
        assert "authorize the deployment OAuth URL" in prompt
        assert "$retry token=available" in prompt
        assert "iss-1" not in orch._implement_handoffs  # noqa: SLF001
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_releases_workspace_when_gate_raises(
    tmp_path: Path,
) -> None:
    """If a pre-push gate raises on the short-circuit path, the workspace is
    still released, the run is failed/parked, and publish (push + ensure_pr)
    never runs."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        # HEAD one commit ahead of `trunk`: the short-circuit branch is taken.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        # A gate blows up (e.g. a subprocess/db error inside verify/dirty-tree).
        orch._run_prepush_gates = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=RuntimeError("gate boom")
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        # The workspace was released *before* the gate ran, so the raise could
        # not leak it; the run failed closed before publish.
        workspace.release.assert_called_once()
        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].status == "failed"
        assert "gate boom" in history[-1].termination_detail
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_halts_before_publish_when_gate_fails(
    tmp_path: Path,
) -> None:
    """If a pre-push gate halts the run (proceed=False) on the short-circuit
    path, the workspace is released and publish (push + ensure_pr) never runs;
    the run returns without raising."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace, push_fn=push_fn
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        # A gate halts the run (recorded its own state) and returns proceed=False.
        orch._run_prepush_gates = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=(False, None)
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        workspace.release.assert_called_once()
        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_resume_at_publish_after_delivery_failure_skips_agent(
    tmp_path: Path,
) -> None:
    """A push failure parks the run; the re-dispatch resumes at publish —
    the branch is already ahead of base, so the agent is skipped and only
    push + ensure_pr run."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        # Base only; the first run's agent will advance HEAD over `trunk`.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        def _commit_on_implement(spec: RunnerSpec) -> None:
            if spec.stage == "implement":
                (workspace_path / "feature.py").write_text("print('hi')\n")
                _git(workspace_path, "add", "-A")
                _git(workspace_path, "commit", "-m", "agent work")

        runner1 = _RecordingRunner(
            [
                RunnerEvent(kind="started", pid=4242),
                RunnerEvent(
                    kind="stdout",
                    line=_done_result_line("Implemented it.\n\nSYMPHONY_DONE"),
                ),
                RunnerEvent(kind="exit", returncode=0),
            ],
            on_run=_commit_on_implement,
        )
        failing_push = AsyncMock(side_effect=RuntimeError("push boom"))

        linear1 = AsyncMock()
        linear1.post_comment = AsyncMock(return_value="cmt-1")
        linear1.move_issue = AsyncMock()
        orch1 = Orchestrator(
            cfg, linear1, conn, runner=runner1, gh=gh, workspace=workspace,
            push_fn=failing_push,
        )
        orch1._states = {"ENG": _states()}  # noqa: SLF001

        await orch1._dispatch_one(binding, _issue())  # noqa: SLF001

        # First run: agent ran, push attempted and failed, no PR.
        assert [s.stage for s in runner1.specs] == ["implement"]
        failing_push.assert_awaited_once()
        gh.ensure_pr.assert_not_awaited()
        h1 = await db.runs.history_for_issue(conn, "iss-1")
        assert h1[0].status == "failed"

        # --- Resume: re-dispatch with a healthy push. The branch is ahead of
        #     base now, so the agent must not run again. ---
        runner2 = _RecordingRunner(
            [RunnerEvent(kind="started", pid=5151), RunnerEvent(kind="exit", returncode=0)]
        )
        good_push = AsyncMock()
        linear2 = AsyncMock()
        linear2.post_comment = AsyncMock(return_value="cmt-2")
        linear2.move_issue = AsyncMock()
        orch2 = Orchestrator(
            cfg, linear2, conn, runner=runner2, gh=gh, workspace=workspace,
            push_fn=good_push,
        )
        orch2._states = {"ENG": _states()}  # noqa: SLF001

        await orch2._dispatch_one(binding, _issue())  # noqa: SLF001

        # The agent was skipped on resume.
        assert runner2.specs == []
        good_push.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        h2 = await db.runs.history_for_issue(conn, "iss-1")
        completed = [r for r in h2 if r.stage == "implement" and r.status == "completed"]
        assert len(completed) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_publish_failed_retry_without_resolved_base_runs_agent_not_publish(
    tmp_path: Path,
) -> None:
    """A publish-failed retry is not enough to publish when the base cannot be
    resolved and the current checkout cannot prove deliverable commits."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        await _seed_publish_failed_implement_run(conn)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(side_effect=GitHubError("boom"))

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=5151), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()
        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert [s.stage for s in runner.specs] == ["implement"]
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()

        history = await db.runs.history_for_issue(conn, "iss-1")
        impl = [r for r in history if r.stage == "implement"]
        assert impl[-1].status == "failed"
        assert "completion contract" in impl[-1].termination_detail
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_publish_failed_retry_with_base_and_no_ahead_runs_agent_not_publish(
    tmp_path: Path,
) -> None:
    """A prior publish_failed run is not enough to publish when a resolved
    base proves the current branch has no commits to deliver."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        await _seed_publish_failed_implement_run(conn)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock()
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=5151), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()
        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert [s.stage for s in runner.specs] == ["implement"]
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        impl = [r for r in history if r.stage == "implement"]
        assert impl[-1].status == "failed"
        assert "completion contract" in impl[-1].termination_detail
    finally:
        await conn.close()


def _head_sha(workspace: Path) -> str:
    out = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workspace, check=True, capture_output=True
    )
    return out.stdout.decode().strip()


def _local_review_binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        local_review=True,
        remote_review=False,
        auto_merge=False,
        # The PR-summary post path needs verdicts on the result; this test
        # only cares about routing, so keep the thread quiet.
        post_local_review_pr_summary=False,
        linear_states=LinearStates(
            ready="Todo",
            local_code_review="Local Code Review",
            code_review="",
            needs_approval="Needs Approval",
        ),
    )


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_reruns_local_review_not_parked(
    tmp_path: Path,
) -> None:
    """Branch-ahead short-circuit with a `local_review` binding: the pre-push
    gates re-run, so publish gets a real APPROVED verdict — NOT the `None` its
    handoff would mis-read as "local-only review did not approve" and park.
    The implementer agent is still skipped."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _local_review_binding()
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        # HEAD already one commit ahead of `trunk`: a prior run committed.
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")
        await _seed_publish_failed_implement_run(conn)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001
        # The reused branch is re-reviewed; drive it to APPROVED so the
        # routing decision under test is exercised without a real reviewer.
        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=LoopResult(
                outcome=LoopOutcome.APPROVED, iterations=1, verdicts=()
            )
        )

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        # The implementer agent never ran (short-circuit), but the local
        # review gate did re-run on the reused workspace.
        assert runner.specs == []
        orch._run_local_review_phase.assert_awaited_once()  # noqa: SLF001
        assert (  # noqa: SLF001
            orch._run_local_review_phase.await_args.kwargs["allow_fixes"] is False
        )
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        history = await db.runs.history_for_issue(conn, "iss-1")
        impl = [r for r in history if r.stage == "implement"]
        assert impl and impl[-1].status == "completed"
        # The review stage was started and NOT failed as "did not approve".
        review = [r for r in history if r.stage == "review"]
        assert review, "an approved local-only PR should start the review stage"
        assert all(r.status != "failed" for r in review)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_records_verify_pass(
    tmp_path: Path,
) -> None:
    """Branch-ahead short-circuit with a `verify_cmd` binding re-runs the
    verify gate, so the green SHA is recorded — the merge gate still treats
    the pushed head as verified instead of routing to operator approval."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            agent="claude",
            branch_prefix="symphony",
            local_review=False,
            remote_review=False,
            auto_merge=False,
            verify_cmd="true",
            linear_states=LinearStates(
                ready="Todo",
                local_code_review="",
                code_review="",
                needs_approval="Needs Approval",
            ),
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")
        await _seed_publish_failed_implement_run(conn)
        head = _head_sha(workspace_path)

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        push_fn = AsyncMock()
        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        # The implementer agent never ran; `verify_cmd="true"` runs as a
        # subprocess (no fix turn → no runner spec) and passes.
        assert runner.specs == []
        push_fn.assert_awaited_once()
        gh.ensure_pr.assert_awaited_once()

        # The merge gate keys off the recorded green SHA for the pushed head.
        assert await db.issue_prs.has_verify_passed(
            conn, issue_id="iss-1", github_repo="org/repo", head_sha=head
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_dirty_tree_fails_without_fix_turn(
    tmp_path: Path,
) -> None:
    """Dirty branch-ahead resume fails closed instead of spawning the
    dirty-tree implement_fix agent."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _no_review_binding(auto_merge=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")
        (workspace_path / "leftover.txt").write_text("uncommitted\n")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].status == "failed"
        assert "working tree dirty during publish resume" in history[-1].termination_detail
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_branch_ahead_short_circuit_verify_failure_skips_fix_turn(
    tmp_path: Path,
) -> None:
    """Red verify on branch-ahead resume fails closed without a verify_fix
    runner turn."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            agent="claude",
            branch_prefix="symphony",
            local_review=False,
            remote_review=False,
            auto_merge=False,
            verify_cmd="false",
            linear_states=LinearStates(
                ready="Todo",
                local_code_review="",
                code_review="",
                needs_approval="Needs Approval",
            ),
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        _init_git_workspace(workspace_path)
        _git(workspace_path, "branch", "trunk")
        _git(workspace_path, "commit", "--allow-empty", "-m", "prior agent work")

        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="trunk")

        runner = _RecordingRunner(
            [RunnerEvent(kind="started", pid=4242), RunnerEvent(kind="exit", returncode=0)]
        )
        push_fn = AsyncMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        orch = Orchestrator(
            cfg, linear, conn, runner=runner, gh=gh, workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await orch._dispatch_one(binding, _issue())  # noqa: SLF001

        assert runner.specs == []
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        impl = [r for r in history if r.stage == "implement"]
        assert impl[-1].status == "failed"
        assert "fix turn disabled for publish resume" in impl[-1].termination_detail
    finally:
        await conn.close()


def test_pr_title_and_body_format() -> None:
    """The PR title format is `[<LINEAR_ID>] <issue title>` and the body
    contains `Relates to <linear-url>`. Verified by introspecting the
    helper that builds them so the format stays pinned even when the
    full e2e flow is mocked."""
    from symphony.orchestrator.poll import build_pr_body, build_pr_title

    assert build_pr_title(_issue()) == "[ENG-1] Add authentication"
    body = build_pr_body(_issue())
    assert "https://linear.app/team/issue/ENG-1" in body
