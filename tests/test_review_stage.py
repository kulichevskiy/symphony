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
import contextlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.github.client import CheckRun, GitHub, GitHubError, PRChecks
from symphony.linear.client import LinearComment, LinearError, LinearIssue
from symphony.linear.slash import SlashIntent, SlashKind
from symphony.orchestrator import poll as poll_module
from symphony.orchestrator.poll import (
    Orchestrator,
    SlashHandlerFailure,
    build_fix_runner_command,
    build_merge_runner_command,
    build_runner_command,
    pr_number_from_url,
)
from symphony.orchestrator.poll import _review as review_module
from symphony.orchestrator.reconcile import reconcile
from symphony.pipeline.cost_guard import UsageDelta
from symphony.pipeline.local_review import LocalVerdict, LocalVerdictKind
from symphony.pipeline.local_review_loop import LoopOutcome, LoopResult
from symphony.pipeline.review_classifier import Verdict, VerdictKind

from ._workspace_helpers import advance_head


def test_codex_lgtm_reaction_carries_reviewed_commit_sha() -> None:
    """The "Reviewed commit: <sha>" line is threaded onto the reaction so the
    classifier can reject the approval once HEAD moves past that commit."""
    entries = [
        {
            "user": {"login": "chatgpt-codex-connector[bot]"},
            "created_at": "2026-06-19T18:25:40Z",
            "body": (
                "Codex Review: Didn't find any major issues. Delightful!\n\n"
                "**Reviewed commit:** `2668682eeb`\n"
            ),
        },
    ]
    reactions = poll_module._codex_lgtm_reactions_from_issue_comments(entries)
    assert len(reactions) == 1
    assert reactions[0].content == "+1"
    assert reactions[0].commit_sha == "2668682eeb"


def test_codex_lgtm_reaction_without_reviewed_commit_has_empty_sha() -> None:
    """Absent a "Reviewed commit" line, the reaction keeps the legacy
    time-only shape (empty commit_sha)."""
    entries = [
        {
            "user": {"login": "chatgpt-codex-connector[bot]"},
            "created_at": "2026-06-19T18:25:40Z",
            "body": "Codex Review: Didn't find any major issues.",
        },
    ]
    reactions = poll_module._codex_lgtm_reactions_from_issue_comments(entries)
    assert len(reactions) == 1
    assert reactions[0].commit_sha == ""


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent], *, commit_on_implement: bool = False) -> None:
        self.events = events
        self.commit_on_implement = commit_on_implement
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        if self.commit_on_implement and spec.stage == "implement":
            advance_head(spec.workspace_path)
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
    codex_model: str | None = "gpt-5.1-codex",
    issue_label: str | None = None,
    local_review: bool = False,
    remote_review: bool = True,
) -> RepoBinding:
    # `codex_model=None` leaves the legacy field unset (out of model_fields_set)
    # so a binding can pair `agent` with a `roles:` model cell without tripping
    # the legacy/matrix conflict guard.
    kwargs: dict[str, object] = dict(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent=agent,
        issue_label=issue_label,
        branch_prefix="symphony",
        local_review=local_review,
        remote_review=remote_review,
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )
    if codex_model is not None:
        kwargs["codex_model"] = codex_model
    return RepoBinding(**kwargs)  # type: ignore[arg-type]


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


def _ready_issue(issue_id: str = "iss-2", identifier: str = "ENG-2") -> LinearIssue:
    return LinearIssue(
        id=issue_id,
        identifier=identifier,
        title="Fresh task",
        description="Start later.",
        url=f"https://linear.app/team/issue/{identifier}",
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


def _issue_in_review(*, labels: list[str] | None = None) -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-na",
        state_name="Needs Approval",
        state_type="started",
        team_key="ENG",
        labels=labels if labels is not None else ["feature"],
    )


def _issue_in_code_review(*, labels: list[str] | None = None) -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-review",
        state_name="In Review",
        state_type="started",
        team_key="ENG",
        labels=labels if labels is not None else ["feature"],
    )


def _issue_needs_input(*, labels: list[str] | None = None) -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-input",
        state_name="Needs Input",
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


def _comment(
    body: str,
    *,
    cid: str = "c1",
    created_at: str = "2026-05-11T12:00:00+00:00",
) -> LinearComment:
    return LinearComment(
        id=cid,
        body=body,
        created_at=created_at,
        author_name="user",
        author_is_me=False,
        external_thread_type=None,
    )


@pytest.fixture(autouse=True)
def _default_workspace_head_sha(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_workspace_head_sha(_workspace_path: Path) -> str:
        return "after-fix-sha"

    async def fake_workspace_ref_sha(_workspace_path: Path, _ref: str) -> str:
        return "before-fix-sha"

    async def fake_git_fetch_branch(_workspace_path: Path, _branch: str) -> None:
        return None

    for module in (poll_module, review_module):
        monkeypatch.setattr(module, "_workspace_head_sha", fake_workspace_head_sha)
        monkeypatch.setattr(module, "_workspace_ref_sha", fake_workspace_ref_sha)
        monkeypatch.setattr(module, "_git_fetch_branch", fake_git_fetch_branch)
    # SYM-150: the implement/deliver stages read these from `poll._lifecycle`.
    monkeypatch.setattr(poll_module._lifecycle, "_workspace_head_sha", fake_workspace_head_sha)


def test_github_commit_url_uses_configured_host() -> None:
    assert (
        poll_module._github_commit_url("ghe.example.com/org/repo", "abc123")
        == "https://ghe.example.com/org/repo/commit/abc123"
    )
    assert (
        poll_module._github_commit_url("org/repo", "abc123")
        == "https://github.com/org/repo/commit/abc123"
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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The autouse fixture pins HEAD sha constant; a successful implement run
    # must advance HEAD so the completion gate classifies it as completed.
    _head_shas = iter(["base-sha", "advanced-sha"])

    async def _advancing_head(_workspace_path: Path) -> str:
        return next(_head_shas, "advanced-sha")

    monkeypatch.setattr(poll_module, "_workspace_head_sha", _advancing_head)
    # SYM-150: lifecycle methods read _workspace_head_sha from _lifecycle's namespace.
    monkeypatch.setattr(poll_module._lifecycle, "_workspace_head_sha", _advancing_head)
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
        gh.pr_issue_comments = AsyncMock(return_value=[])
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
            c
            for c in gh.pr_comment.await_args_list
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
        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-progress"),
            call("iss-1", "state-na"),
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_handoff_uses_code_review_and_merge_failure_uses_needs_approval(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            linear_states=LinearStates(
                ready="Todo",
                code_review="In Review",
                needs_approval="Needs Input",
            ),
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner([]),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {  # noqa: SLF001
            "ENG": {
                "Todo": "state-todo",
                "In Progress": "state-progress",
                "In Review": "state-review",
                "Needs Input": "state-input",
                "Blocked": "state-bl",
                "Done": "state-done",
            }
        }

        await orch._move_issue_to_review_state(binding=binding, issue=issue)  # noqa: SLF001
        await orch._mark_merge_needs_approval(  # noqa: SLF001
            binding=binding,
            issue=issue,
            pr_url="https://github.com/org/repo/pull/42",
            run_id="merge-run",
            reason="branch protection blocked",
            create_run=True,
        )

        assert linear.move_issue.await_args_list == [
            call("iss-1", "state-review"),
            call("iss-1", "state-input"),
        ]
        history = await db.runs.history_for_issue(conn, issue.id)
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
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
    started_at: str = "2026-05-10T00:00:00+00:00",
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
        started_at=started_at,
    )


async def _seed_local_review_cycle(conn, *, local_review_status: str) -> None:
    await db.runs.create(
        conn,
        id=f"implement-for-{local_review_status}-local-review",
        issue_id="iss-1",
        stage="implement",
        status="completed",
        pid=1234,
        started_at="2026-05-09T23:58:00+00:00",
    )
    await db.runs.create(
        conn,
        id=f"{local_review_status}-local-review",
        issue_id="iss-1",
        stage="local_review",
        status=local_review_status,
        pid=None,
        started_at="2026-05-09T23:59:00+00:00",
    )


async def _poll_review_and_wait(orch: Orchestrator) -> list[asyncio.Task[None]]:
    tasks = await orch._poll_review_runs()  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)
    return tasks


async def _seed_active_local_only_pr_with_stale_local_review(conn) -> None:  # type: ignore[no-untyped-def]
    await _seed_active_review(conn)
    await db.runs.create(
        conn,
        id="implement-run",
        issue_id="iss-1",
        stage="implement",
        status="completed",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )
    await db.issue_prs.upsert(
        conn,
        issue_id="iss-1",
        github_repo="org/repo",
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        created_at="2026-05-10T00:01:00+00:00",
    )
    await db.runs.create(
        conn,
        id="pre-ci-local-review",
        issue_id="iss-1",
        stage="local_review",
        status="completed",
        pid=None,
        started_at="2026-05-10T00:02:00+00:00",
    )


def _failing_ci_verdict() -> Verdict:
    return Verdict(
        kind=VerdictKind.CHANGES_REQUESTED,
        rule="failing_ci",
        trigger_signature="ci:head-sha:lint",
        failing_checks=("lint",),
    )


def _failing_ci_checks() -> PRChecks:
    return PRChecks([CheckRun(name="lint", state="FAILURE", bucket="fail", link=None)])


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
        linear.move_issue = AsyncMock(return_value=None)
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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="ruff found a lint failure")
        gh.pr_comment = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])

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
        command = runner.captured_spec.command
        assert command[:3] == [
            "codex",
            "exec",
            "--json",
        ]
        assert "--dangerously-bypass-approvals-and-sandbox" in command
        assert "--sandbox" not in command
        assert "workspace-write" not in command
        # Permissions/approval --config knobs are gone (superseded by the bypass).
        assert not any("default_permissions" in a or "approval_policy" in a for a in command)
        assert command[command.index("--model") + 1] == "gpt-5.1-codex-max"
        prompt = command[-1]
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
async def test_red_ci_fix_run_repush_and_codex_reping_use_db_github_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OAuth in UI 4/7 acceptance: a review-fix re-push and its `@codex
    review` re-ping both use the connected DB GitHub token, with no
    `GH_TOKEN` in env and no ambient `gh` auth (review fix — push auth was
    previously only wired for the first delivery push, and `self._gh` API
    calls were never DB-token-aware at all)."""
    from symphony.crypto import CredentialCipher

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.oauth_connections.set_connection(
            conn,
            provider="github",
            credential="gho_db_secret",
            cipher=CredentialCipher("enc-key"),
        )
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding(agent="codex", codex_model="gpt-5.1-codex-max")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            symphony_encryption_key="enc-key",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        advance_head(workspace_path)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        # Ambient `gh` — must never be called once a DB GitHub row is
        # connected; any call on it fails the test loudly instead of
        # silently using stale/ambient auth.
        gh = MagicMock()
        gh.pr_checks = AsyncMock(side_effect=AssertionError("ambient gh.pr_checks used"))
        gh.pr_view = AsyncMock(side_effect=AssertionError("ambient gh.pr_view used"))
        gh.pr_comment = AsyncMock(side_effect=AssertionError("ambient gh.pr_comment used"))

        from symphony.orchestrator.poll._git import _push_auth_subprocess_env  # noqa: PLC0415

        push_seen_auth: list[str] = []

        async def _push_fn(path: Path, branch: str) -> None:
            env = _push_auth_subprocess_env(path) or {}
            count = int(env.get("GIT_CONFIG_COUNT", "0"))
            headers = {
                env[f"GIT_CONFIG_KEY_{i}"]: env[f"GIT_CONFIG_VALUE_{i}"] for i in range(count)
            }
            push_seen_auth.append(headers.get("http.https://github.com/.extraheader", ""))

        gh_ctor_calls: list[dict[str, object]] = []

        class _RecordingGitHub:
            def __init__(self, **kwargs: object) -> None:
                gh_ctor_calls.append(kwargs)
                self.pr_checks = AsyncMock(
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
                self.pr_view = AsyncMock(
                    return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"}
                )
                self.check_log_tail = AsyncMock(return_value="ruff found a lint failure")
                self.pr_comment = AsyncMock()
                self.pr_issue_comments = AsyncMock(return_value=[])

        monkeypatch.setattr("symphony.orchestrator.poll._base.GitHub", _RecordingGitHub)

        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=999),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )

        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=_push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_review_and_wait(orch)

        # The re-push happened while the workspace's local git config
        # carried the DB token as a Basic auth header.
        assert len(push_seen_auth) == 1
        assert push_seen_auth[0]
        import base64 as _b64

        expected = _b64.b64encode(b"x-access-token:gho_db_secret").decode()
        assert expected in push_seen_auth[0]

        # `@codex review` re-ping and every other GitHub read/write in this
        # run went through a client built with the DB token.
        assert gh_ctor_calls
        assert all(kwargs["token"] == "gho_db_secret" for kwargs in gh_ctor_calls)
        gh.pr_checks.assert_not_awaited()
        gh.pr_view.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_red_ci_local_only_reruns_local_review_before_push(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_local_only_pr_with_stale_local_review(conn)
        binding = _binding(local_review=True, remote_review=False)
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
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
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
        orch._run_fix_agent = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=(UsageDelta(cost_usd=0.01), "exit", 0)
        )

        async def approve_local_review(**kwargs: object) -> LoopResult:
            assert kwargs["parent_run_id"] != "review-run"
            assert push_fn.await_count == 0
            candidate = await db.issue_prs.get_for_issue(conn, issue_id="iss-1")
            assert candidate is not None
            assert not await orch._local_review_completed_for_issue(candidate)  # noqa: SLF001
            await db.runs.create(
                conn,
                id="post-ci-local-review",
                issue_id="iss-1",
                stage="local_review",
                status="completed",
                pid=None,
                started_at=datetime.now(UTC).isoformat(),
            )
            return LoopResult(
                outcome=LoopOutcome.APPROVED,
                iterations=1,
                verdicts=(),
            )

        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=approve_local_review
        )

        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")

        dispatched = await orch._dispatch_ci_fix_run(  # noqa: SLF001
            run=run,
            binding=binding,
            issue=_issue_in_progress(),
            checks=_failing_ci_checks(),
            verdict=_failing_ci_verdict(),
            iteration=1,
        )

        assert dispatched is True
        orch._run_local_review_phase.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.pr_comment.assert_not_awaited()

        candidate = await db.issue_prs.get_for_issue(conn, issue_id="iss-1")
        assert candidate is not None
        assert await orch._local_review_completed_for_issue(candidate)  # noqa: SLF001
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history][-2:] == [
            "review_fix",
            "local_review",
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [LoopOutcome.EXHAUSTED, LoopOutcome.STUCK_LOOP],
)
async def test_red_ci_local_only_parks_non_converged_local_review(
    tmp_path: Path,
    outcome: LoopOutcome,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_local_only_pr_with_stale_local_review(conn)
        binding = _binding(local_review=True, remote_review=False)
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
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
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
        orch._run_fix_agent = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=(UsageDelta(cost_usd=0.01), "exit", 0)
        )
        findings = "src/auth.py:12 missing token validation"

        async def non_converged_local_review(**_: object) -> LoopResult:
            await db.runs.create(
                conn,
                id="post-ci-local-review",
                issue_id="iss-1",
                stage="local_review",
                status="failed",
                pid=None,
                started_at=datetime.now(UTC).isoformat(),
            )
            return LoopResult(
                outcome=outcome,
                iterations=2,
                verdicts=(
                    LocalVerdict(
                        kind=LocalVerdictKind.CHANGES_REQUESTED,
                        findings=findings,
                    ),
                ),
                error=f"{outcome.value} test outcome",
            )

        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=non_converged_local_review
        )

        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")

        dispatched = await orch._dispatch_ci_fix_run(  # noqa: SLF001
            run=run,
            binding=binding,
            issue=_issue_in_progress(),
            checks=_failing_ci_checks(),
            verdict=_failing_ci_verdict(),
            iteration=1,
        )

        assert dispatched is True
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.pr_comment.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any(findings in body for body in posted), posted

        history = await db.runs.history_for_issue(conn, "iss-1")
        monitor = next(r for r in history if r.id == "review-run")
        assert monitor.status == "needs_approval"
        assert findings in monitor.termination_detail
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [
        LoopOutcome.REVIEWER_FAILED,
        LoopOutcome.FIX_RUN_FAILED,
    ],
)
async def test_red_ci_local_only_blocks_local_review_infra_failure(
    tmp_path: Path,
    outcome: LoopOutcome,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_local_only_pr_with_stale_local_review(conn)
        binding = _binding(local_review=True, remote_review=False)
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
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
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
        orch._run_fix_agent = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            return_value=(UsageDelta(cost_usd=0.01), "exit", 0)
        )

        async def failed_local_review(**_: object) -> LoopResult:
            await db.runs.create(
                conn,
                id="post-ci-local-review",
                issue_id="iss-1",
                stage="local_review",
                status="failed",
                pid=None,
                started_at=datetime.now(UTC).isoformat(),
            )
            return LoopResult(
                outcome=outcome,
                iterations=1,
                verdicts=(),
                error=f"{outcome.value} test outcome",
            )

        orch._run_local_review_phase = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=failed_local_review
        )

        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")

        dispatched = await orch._dispatch_ci_fix_run(  # noqa: SLF001
            run=run,
            binding=binding,
            issue=_issue_in_progress(),
            checks=_failing_ci_checks(),
            verdict=_failing_ci_verdict(),
            iteration=1,
        )

        assert dispatched is False
        push_fn.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-bl")

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED
        history = await db.runs.history_for_issue(conn, "iss-1")
        monitor = next(r for r in history if r.id == "review-run")
        fix_run = next(r for r in history if r.stage == "review_fix")
        assert monitor.status == "failed"
        assert fix_run.status == "completed"
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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])

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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_issue_comments = AsyncMock(return_value=[])

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
        orch._states = {"ENG": _states()}  # noqa: SLF001

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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock()
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])

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
        gh.pr_view = AsyncMock(side_effect=GitHubError("pr view failed"))
        gh.head_sha = AsyncMock(side_effect=GitHubError("head lookup failed"))
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])

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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

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
            "#!/usr/bin/env python3\nimport sys\nsys.stderr.write('network down\\n')\nsys.exit(1)\n"
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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])

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
async def test_active_review_polls_when_issue_is_in_configured_review_state(
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
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        await _poll_review_and_wait(orch)

        gh.pr_checks.assert_awaited_once_with(42, repo="org/repo")
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[0].status == "running"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_rechecks_deliver_failed_wait_before_dispatch(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        binding = _binding()
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=MagicMock())
        run = next(
            run for run in await db.runs.history_for_issue(conn, "iss-1") if run.id == "review-run"
        )
        await db.runs.create(
            conn,
            id="implement-run",
            issue_id="iss-1",
            stage="implement",
            status="completed",
            pid=None,
            started_at="2026-05-09T23:59:00+00:00",
        )

        async def park_after_refresh(_run_id: str) -> bool:
            await db.operator_waits.upsert(
                conn,
                issue_id="iss-1",
                run_id="implement-run",
                kind=db.operator_waits.KIND_DELIVER_FAILED,
                linear_team_key=binding.linear_team_key,
                github_repo=binding.github_repo,
                issue_label=binding.issue_label or "",
                created_at="2026-05-10T00:01:00+00:00",
                provider=binding.provider,
                tracker_provider=binding.tracker_provider,
                tracker_site=binding.tracker_site,
            )
            return False

        orch._review_rearm_retry_pending = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=park_after_refresh
        )
        orch._poll_review_run = AsyncMock(return_value=True)  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_review_run_with_limits(  # noqa: SLF001
            run, binding, _issue_in_review()
        )

        orch._poll_review_run.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_active_review_closes_when_issue_leaves_review_active_states(
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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])

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
async def test_review_poll_is_not_blocked_by_implement_semaphore(tmp_path: Path) -> None:
    """CI polling runs immediately even when all implement slots are occupied."""
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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        # Hold all implement slots — review polling must NOT be blocked.
        await orch._global_dispatch_sem.acquire()  # noqa: SLF001
        tasks: list[asyncio.Task[None]] = []
        try:
            tasks = await orch._poll_review_runs()  # noqa: SLF001
            if tasks:
                await asyncio.gather(*tasks)

            # CI was polled despite implement semaphore being full.
            gh.pr_checks.assert_awaited_once_with(42, repo="org/repo")
            history = await db.runs.history_for_issue(conn, "iss-1")
            assert history[0].status == "running"
        finally:
            orch._global_dispatch_sem.release()  # noqa: SLF001
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_fix_run_is_gated_by_review_semaphore(tmp_path: Path) -> None:
    """When the review fix semaphore is full, dispatch waits; polling still detects the signal."""
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
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)

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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])

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

        # Hold the review fix semaphore — dispatch must block.
        await orch._review_fix_sem.acquire()  # noqa: SLF001
        tasks: list[asyncio.Task[None]] = []
        try:
            tasks = await orch._poll_review_runs()  # noqa: SLF001
            # Wait for the task to get through all the mock awaits (CI fetch,
            # head SHA, log tail) and reach the semaphore block, instead of a
            # fixed sleep that can flake under CI load.
            for _ in range(500):
                if gh.pr_checks.await_count:
                    break
                await asyncio.sleep(0.01)

            # Task is alive but runner never started — stuck on _review_fix_sem.
            assert len(tasks) == 1
            assert not tasks[0].done()
            assert not runner.started.is_set()
            # Polling ran: CI was checked despite the semaphore being held.
            gh.pr_checks.assert_awaited_once_with(42, repo="org/repo")
        finally:
            orch._review_fix_sem.release()  # noqa: SLF001
            runner.release.set()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_active_review_fix_reserves_capacity_before_new_implementation(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        binding = _binding().model_copy(update={"max_concurrent": 1})
        cfg = Config(
            repos=[binding],
            global_max_concurrent=1,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.issues_in_state = AsyncMock(return_value=[_ready_issue()])
        linear.post_comment = AsyncMock(return_value="cmt-1")

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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])

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

        review_tasks = await orch._poll_review_runs()  # noqa: SLF001
        try:
            await asyncio.wait_for(runner.started.wait(), timeout=1)
            assert orch._dispatch_capacity(binding) == 0  # noqa: SLF001

            implementation_tasks = await orch._scan_binding(binding)  # noqa: SLF001

            assert implementation_tasks == []
            cur = await conn.execute("SELECT 1 FROM runs WHERE issue_id = 'iss-2'")
            assert await cur.fetchone() is None
        finally:
            runner.release.set()
            if review_tasks:
                await asyncio.gather(*review_tasks, return_exceptions=True)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_scheduled_slot_release_preserves_shared_issue_owner(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding().model_copy(update={"max_concurrent": 3})
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            global_max_concurrent=1,
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
        )
        binding_key = poll_module._binding_key(binding)  # noqa: SLF001

        orch._reserve_scheduled_slot(  # noqa: SLF001
            issue_id="iss-1",
            binding_key=binding_key,
        )
        orch._reserve_scheduled_slot(  # noqa: SLF001
            issue_id="iss-1",
            binding_key=binding_key,
        )
        orch._release_scheduled_slot(  # noqa: SLF001
            issue_id="iss-1",
            binding_key=binding_key,
        )

        assert "iss-1" in orch._scheduled_issue_ids  # noqa: SLF001
        assert orch._scheduled_issue_refcounts["iss-1"] == 1  # noqa: SLF001
        assert orch._dispatch_capacity(binding) == 0  # noqa: SLF001
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
        linear.lookup_issue = AsyncMock(side_effect=[_issue_in_progress(), _issue_done()])

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
async def test_stop_intent_cancels_review_monitor_and_active_fix_run(
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
        linear.comments_since = AsyncMock(return_value=[_comment("$stop")])

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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])

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

        tasks = await orch._poll_review_runs()  # noqa: SLF001
        try:
            await asyncio.wait_for(runner.started.wait(), timeout=1)
            fix_run_id = orch._dispatch_run_ids["iss-1"]  # noqa: SLF001

            await orch._poll_slash_commands()  # noqa: SLF001

            assert runner.killed_run_ids == [fix_run_id]
            assert runner.release.is_set()
            assert "review-run" not in orch._review_poll_run_ids  # noqa: SLF001
            assert "iss-1" not in orch._review_poll_issue_ids  # noqa: SLF001
            assert orch._dispatch_run_ids["iss-1"] == "review-run"  # noqa: SLF001
            assert "review-run" in orch._operator_wait_run_ids  # noqa: SLF001
            history = await db.runs.history_for_issue(conn, "iss-1")
            monitor = history[0]
            assert monitor.id == "review-run"
            assert monitor.status == "interrupted"
            fix_run = next(r for r in history if r.id == fix_run_id)
            assert fix_run.status == "interrupted"
            wait = await db.operator_waits.get(conn, "iss-1")
            assert wait is not None
            assert wait.run_id == "review-run"
            assert wait.kind == db.operator_waits.KIND_REVIEW_STOPPED
            posted = [c.args[1] for c in linear.post_comment.await_args_list]
            assert any("$retry" in body and "$approve" in body for body in posted)
        finally:
            runner.release.set()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stop_intent_keeps_review_live_when_active_fix_run_kill_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    review_task: asyncio.Task[bool] | None = None
    try:
        await _seed_active_review(conn)
        fix_run_id = "fix-run"
        await db.runs.create(
            conn,
            id=fix_run_id,
            issue_id="iss-1",
            stage="review_fix",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:30+00:00",
        )
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$stop")])
        linear.post_comment = AsyncMock(return_value="cmt-1")

        runner = MagicMock()
        runner.kill = AsyncMock(side_effect=RuntimeError("still running"))
        orch = Orchestrator(cfg, linear, conn, runner=runner, gh=MagicMock())

        review_task = asyncio.create_task(asyncio.Event().wait())
        orch._review_poll_run_ids.add("review-run")  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = "review-run"  # noqa: SLF001
        orch._review_poll_run_tasks["review-run"] = review_task  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = fix_run_id  # noqa: SLF001
        orch._active_run_ids.add(fix_run_id)  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001

        runner.kill.assert_awaited_once_with(fix_run_id)
        assert "review-run" in orch._review_poll_run_ids  # noqa: SLF001
        assert orch._review_poll_issue_ids["iss-1"] == "review-run"  # noqa: SLF001
        assert orch._review_poll_run_tasks["review-run"] is review_task  # noqa: SLF001
        assert orch._dispatch_run_ids["iss-1"] == fix_run_id  # noqa: SLF001
        assert fix_run_id in orch._active_run_ids  # noqa: SLF001
        assert not review_task.cancelled()

        history = await db.runs.history_for_issue(conn, "iss-1")
        review_run = next(r for r in history if r.id == "review-run")
        fix_run = next(r for r in history if r.id == fix_run_id)
        assert review_run.status == "running"
        assert fix_run.status == "running"
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("could not stop active review fix-run" in b for b in posted)
    finally:
        if review_task is not None:
            review_task.cancel()
            await asyncio.gather(review_task, return_exceptions=True)
        await conn.close()


@pytest.mark.asyncio
async def test_stop_intent_cancels_review_monitor_run(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        runner = AsyncMock()
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = Orchestrator(cfg, linear, conn, runner=runner, gh=MagicMock())
        review_task = asyncio.create_task(asyncio.Event().wait())
        orch._review_poll_run_ids.add("review-run")  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = "review-run"  # noqa: SLF001
        orch._review_poll_run_tasks["review-run"] = review_task  # noqa: SLF001

        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            "review-run",
            SlashIntent(
                kind=SlashKind.STOP,
                comment_id="c-stop",
                created_at="2026-05-10T00:01:00+00:00",
            ),
        )
        await asyncio.gather(review_task, return_exceptions=True)

        runner.kill.assert_not_awaited()
        assert "review-run" not in orch._review_poll_run_ids  # noqa: SLF001
        assert "iss-1" not in orch._review_poll_issue_ids  # noqa: SLF001
        assert "review-run" not in orch._review_poll_run_tasks  # noqa: SLF001
        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")
        assert run.status == "interrupted"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.run_id == "review-run"
        assert wait.kind == db.operator_waits.KIND_REVIEW_STOPPED
        assert orch._dispatch_run_ids["iss-1"] == "review-run"  # noqa: SLF001
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Review monitor stopped" in body for body in posted)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_after_stopped_review_monitor_restarts_review(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    review_task: asyncio.Task[bool] | None = None
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)
        linear.comments_since = AsyncMock(
            side_effect=[
                [
                    _comment(
                        "$stop",
                        cid="c-stop",
                        created_at="2026-05-10T00:01:00+00:00",
                    )
                ],
                [
                    _comment(
                        "$retry",
                        cid="c-retry",
                        created_at="2026-05-10T00:02:00+00:00",
                    )
                ],
            ]
        )

        gh = MagicMock()
        gh.pr_comment = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001
        review_task = asyncio.create_task(asyncio.Event().wait())
        orch._review_poll_run_ids.add("review-run")  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = "review-run"  # noqa: SLF001
        orch._review_poll_run_tasks["review-run"] = review_task  # noqa: SLF001

        poll_started = asyncio.Event()
        poll_release = asyncio.Event()

        async def blocked_review_poll(*_args) -> None:  # type: ignore[no-untyped-def]
            poll_started.set()
            await poll_release.wait()

        orch._poll_review_run_with_limits = blocked_review_poll  # type: ignore[method-assign]  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001
        await asyncio.gather(review_task, return_exceptions=True)

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_REVIEW_STOPPED
        assert wait.run_id == "review-run"
        assert orch._dispatch_run_ids["iss-1"] == "review-run"  # noqa: SLF001

        await orch._poll_slash_commands()  # noqa: SLF001
        await asyncio.wait_for(poll_started.wait(), timeout=1)

        history = await db.runs.history_for_issue(conn, "iss-1")
        running = [r for r in history if r.stage == "review" and r.status == "running"]
        assert len(running) == 1
        assert running[0].id != "review-run"
        assert orch._review_poll_issue_ids["iss-1"] == running[0].id  # noqa: SLF001
        assert running[0].id in orch._review_poll_run_ids  # noqa: SLF001
        gh.pr_comment.assert_awaited_once_with(42, "@codex review", repo="org/repo")
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert "review-run" not in orch._operator_wait_run_ids  # noqa: SLF001
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Review monitor stopped" in body for body in bodies)
        assert any("Resumed" in body for body in bodies)
    finally:
        if review_task is not None:
            review_task.cancel()
            await asyncio.gather(review_task, return_exceptions=True)
        if "orch" in locals():
            for task in tuple(orch._review_poll_tasks):  # noqa: SLF001
                task.cancel()
            await asyncio.gather(
                *tuple(orch._review_poll_tasks),  # noqa: SLF001
                return_exceptions=True,
            )
        await conn.close()


# --- Fix-runs go through the binding agent ---------------------------------


def test_build_fix_runner_command_uses_claude_when_binding_is_claude() -> None:
    argv = build_fix_runner_command("claude", "fix this")
    assert argv[0] == "claude"
    assert "fix this" in argv


@pytest.mark.asyncio
async def test_review_fix_run_carries_fix_role_model(tmp_path: Path) -> None:
    """Path 3 (remote `@codex` review fix): `_run_fix_agent` builds a command
    carrying the resolved `fix` role's `--model`."""
    from symphony.config import RoleConfig

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(agent="claude", codex_model=None).model_copy(
            update={"roles": {"fix": RoleConfig(model="sonnet")}}
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(cfg, AsyncMock(), conn, runner=MagicMock(), gh=MagicMock())
        captured: dict[str, list[str]] = {}

        async def fake_run_runner(**kwargs: object) -> tuple[UsageDelta, str, int]:
            captured["command"] = kwargs["command"]  # type: ignore[assignment]
            return UsageDelta(), "exit", 0

        orch._run_runner = fake_run_runner  # type: ignore[method-assign]  # noqa: SLF001
        await orch._run_fix_agent(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            run_id="fix-run",
            workspace_path=tmp_path / "ws",
            prompt="fix the bug",
            prior_total=0.0,
        )
        argv = captured["command"]
        assert argv[argv.index("--model") + 1] == "sonnet"
    finally:
        await conn.close()


def test_build_fix_runner_command_uses_codex_when_binding_is_codex(
    tmp_path: Path,
) -> None:
    argv = build_fix_runner_command(
        "codex",
        "fix this",
        codex_model="gpt-5.1-codex-max",
        workspace_path=tmp_path,
    )
    assert argv[:3] == [
        "codex",
        "exec",
        "--json",
    ]
    assert "--dangerously-bypass-approvals-and-sandbox" in argv
    assert "--sandbox" not in argv
    assert "workspace-write" not in argv
    assert "--config" not in argv
    assert argv[argv.index("--model") + 1] == "gpt-5.1-codex-max"
    assert "fix this" in argv


def test_build_fix_runner_command_passes_claude_model() -> None:
    """Resolved `fix` role model → `--model`; unset → CLI default (no flag)."""
    with_model = build_fix_runner_command("claude", "fix this", claude_model="opus")
    assert with_model[with_model.index("--model") + 1] == "opus"
    assert "--model" not in build_fix_runner_command("claude", "fix this")


def test_build_fix_runner_command_passes_configured_codex_model(
    tmp_path: Path,
) -> None:
    argv = build_fix_runner_command(
        "codex",
        "fix this",
        codex_model="gpt-5.1-codex-max",
        workspace_path=tmp_path,
    )
    assert argv[argv.index("--model") + 1] == "gpt-5.1-codex-max"
    assert argv[-1] == "fix this"


def test_build_runner_command_allows_git_writes_for_codex_implement(
    tmp_path: Path,
) -> None:
    argv = build_runner_command(
        "codex",
        "implement this",
        workspace_path=tmp_path,
    )
    assert "--dangerously-bypass-approvals-and-sandbox" in argv
    assert "--sandbox" not in argv
    assert "workspace-write" not in argv
    assert "--config" not in argv
    assert argv[-1] == "implement this"


# --- effort threads through every runner command builder (SYM-192) ---------


def test_build_fix_runner_command_threads_effort_claude() -> None:
    argv = build_fix_runner_command("claude", "fix this", effort="high")
    assert argv[argv.index("--effort") + 1] == "high"
    assert "--effort" not in build_fix_runner_command("claude", "fix this")


def test_build_fix_runner_command_threads_effort_codex(tmp_path: Path) -> None:
    argv = build_fix_runner_command(
        "codex", "fix this", codex_model="gpt-5.1-codex", effort="high", workspace_path=tmp_path
    )
    assert 'model_reasoning_effort="high"' in argv


def test_build_merge_runner_command_threads_model_and_effort() -> None:
    argv = build_merge_runner_command("claude", "merge this", claude_model="opus", effort="max")
    assert argv[argv.index("--model") + 1] == "opus"
    assert argv[argv.index("--effort") + 1] == "max"


@pytest.mark.asyncio
async def test_merge_command_reflects_matrix_override(tmp_path: Path) -> None:
    """A matrix `implement` model/effort override reaches the merge-run argv
    (the merge stage resolves the `implement` role)."""
    from symphony.config import RoleConfig

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(agent="claude", codex_model=None).model_copy(
            update={"roles": {"implement": RoleConfig(model="opus", effort="max")}}
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(cfg, AsyncMock(), conn, runner=MagicMock(), gh=MagicMock())
        captured = await _capture_stage_command(orch, "_run_stage_command")
        await orch._run_merge_agent(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            run_id="merge-run",
            workspace_path=tmp_path / "ws",
            pr_url="https://github.com/org/repo/pull/42",
            prior_total=0.0,
        )
        argv = captured["command"]
        assert argv[argv.index("--model") + 1] == "opus"
        assert argv[argv.index("--effort") + 1] == "max"
    finally:
        await conn.close()


async def _capture_stage_command(orch: Orchestrator, attr: str) -> dict[str, list[str]]:
    captured: dict[str, list[str]] = {}

    async def fake(**kwargs: object) -> tuple[UsageDelta, str, int]:
        captured["command"] = kwargs["command"]  # type: ignore[assignment]
        return UsageDelta(), "exit", 0

    setattr(orch, attr, fake)  # noqa: B010
    return captured


@pytest.mark.asyncio
async def test_implement_command_reflects_matrix_override(tmp_path: Path) -> None:
    """A matrix `implement` override of agent/model/effort reaches the actual
    implement argv (codex path carries model + reasoning effort)."""
    from symphony.config import RoleConfig

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            branch_prefix="symphony",
            linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
            roles={
                "implement": RoleConfig(agent="codex", model="gpt-5.1-codex-max", effort="high")
            },
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(cfg, AsyncMock(), conn, runner=MagicMock(), gh=MagicMock())
        captured = await _capture_stage_command(orch, "_run_stage_command")
        await orch._run_agent(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            run_id="impl-run",
            workspace_path=tmp_path / "ws",
            prior_total=0.0,
        )
        argv = captured["command"]
        assert argv[0] == "codex"
        assert argv[argv.index("--model") + 1] == "gpt-5.1-codex-max"
        assert 'model_reasoning_effort="high"' in argv
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_fix_command_reflects_matrix_effort(tmp_path: Path) -> None:
    """A matrix `fix` effort override reaches the fix-run argv."""
    from symphony.config import RoleConfig

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(agent="claude", codex_model=None).model_copy(
            update={"roles": {"fix": RoleConfig(model="sonnet", effort="high")}}
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(cfg, AsyncMock(), conn, runner=MagicMock(), gh=MagicMock())
        captured = await _capture_stage_command(orch, "_run_runner")
        await orch._run_fix_agent(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            run_id="fix-run",
            workspace_path=tmp_path / "ws",
            prompt="fix the bug",
            prior_total=0.0,
        )
        argv = captured["command"]
        assert argv[argv.index("--model") + 1] == "sonnet"
        assert argv[argv.index("--effort") + 1] == "high"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_fix_command_reflects_matrix_override(tmp_path: Path) -> None:
    """A matrix `accept` override of agent/model/effort reaches the acceptance
    fix argv (codex path)."""
    from symphony.config import RoleConfig

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            branch_prefix="symphony",
            linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
            roles={"accept": RoleConfig(agent="codex", model="gpt-5.1-codex-max", effort="low")},
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(cfg, AsyncMock(), conn, runner=MagicMock(), gh=MagicMock())
        captured = await _capture_stage_command(orch, "_run_runner")
        await orch._run_acceptance_fix_agent(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            run_id="acc-run",
            workspace_path=tmp_path / "ws",
            prompt="address acceptance feedback",
            prior_total=0.0,
        )
        argv = captured["command"]
        assert argv[0] == "codex"
        assert argv[argv.index("--model") + 1] == "gpt-5.1-codex-max"
        assert 'model_reasoning_effort="low"' in argv
    finally:
        await conn.close()


# --- PR URL parser ---------------------------------------------------------


def test_pr_number_from_url_parses_github_url() -> None:
    assert pr_number_from_url("https://github.com/org/repo/pull/42") == 42
    assert pr_number_from_url("https://github.com/org/repo/pull/1234\n") == 1234


def test_pr_number_from_url_returns_none_for_garbage() -> None:
    assert pr_number_from_url("") is None
    assert pr_number_from_url("not a url") is None


@pytest.mark.asyncio
async def test_start_review_stage_ignores_terminal_review_when_other_stage_live(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        await db.review_state.begin_review(
            conn,
            issue.id,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label=None,
        )
        await db.runs.create(
            conn,
            id="old-review",
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "old-review",
            "failed",
            ended_at="2026-05-10T00:01:00+00:00",
        )
        await db.runs.create(
            conn,
            id="live-local-review",
            issue_id=issue.id,
            stage="local_review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:02:00+00:00",
        )

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
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001

        run = await orch._start_review_stage(  # noqa: SLF001
            binding=binding,
            issue=issue,
            storage_issue_id=issue.id,
            pr_url="https://github.com/org/repo/pull/43",
        )

        assert run.status == "running"
        assert run.id != "old-review"
        history = await db.runs.history_for_issue(conn, issue.id)
        review_rows = [row for row in history if row.stage == "review"]
        assert [row.status for row in review_rows] == ["failed", "running"]
        latest = await db.runs.latest_for_issue_stage(conn, issue_id=issue.id, stage="review")
        assert latest is not None
        assert latest.id == run.id
        assert latest.status == "running"
        state = await db.review_state.get(conn, issue.id)
        assert state.pr_number == 43
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_start_review_stage_adopts_live_review_and_persists_pr_row(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
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
            id="live-review",
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

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
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001

        run = await orch._start_review_stage(  # noqa: SLF001
            binding=binding,
            issue=issue,
            storage_issue_id=issue.id,
            pr_url="https://github.com/org/repo/pull/43",
        )

        assert run.id == "live-review"
        issue_pr = await db.issue_prs.get_for_issue(conn, issue_id=issue.id)
        assert issue_pr is not None
        assert issue_pr.pr_number == 43
        assert issue_pr.pr_url == "https://github.com/org/repo/pull/43"
        assert issue_pr.created_at == "2026-05-10T00:00:00+00:00"
        candidates = await db.issue_prs.list_merge_candidates(conn)
        assert [candidate.pr_number for candidate in candidates] == [43]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_start_review_stage_adopts_live_review_and_reposts_unconfirmed_bot(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        await db.review_state.begin_review(
            conn,
            issue.id,
            pr_number=43,
            pr_url="https://github.com/org/repo/pull/43",
            github_repo=binding.github_repo,
            issue_label=binding.issue_label,
        )
        await db.runs.create(
            conn,
            id="live-review",
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

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
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001

        run = await orch._start_review_stage(  # noqa: SLF001
            binding=binding,
            issue=issue,
            storage_issue_id=issue.id,
            pr_url="https://github.com/org/repo/pull/43",
        )

        assert run.id == "live-review"
        gh.pr_comment.assert_awaited_once_with(43, "@codex review", repo="org/repo")
        state = await db.review_state.get(conn, issue.id)
        assert state.codex_review_requested_at
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_start_review_stage_adopted_live_review_reposts_bot_for_replacement_pr(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        await db.review_state.begin_review(
            conn,
            issue.id,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo=binding.github_repo,
            issue_label=binding.issue_label,
        )
        old_requested_at = "2026-05-10T00:00:30+00:00"
        await db.review_state.set_codex_review_requested_at(conn, issue.id, old_requested_at)
        await db.runs.create(
            conn,
            id="live-review",
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

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
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001

        run = await orch._start_review_stage(  # noqa: SLF001
            binding=binding,
            issue=issue,
            storage_issue_id=issue.id,
            pr_url="https://github.com/org/repo/pull/43",
        )

        assert run.id == "live-review"
        gh.pr_comment.assert_awaited_once_with(43, "@codex review", repo="org/repo")
        state = await db.review_state.get(conn, issue.id)
        assert state.pr_number == 43
        assert state.codex_review_requested_at
        assert state.codex_review_requested_at != old_requested_at
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_start_review_stage_adopted_live_review_preserves_pr_timestamp(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            binding_key="old-binding",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-09T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="live-review",
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

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
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001

        run = await orch._start_review_stage(  # noqa: SLF001
            binding=binding,
            issue=issue,
            storage_issue_id=issue.id,
            pr_url="https://github.com/org/repo/pull/43",
        )

        assert run.id == "live-review"
        issue_pr = await db.issue_prs.get_for_issue(conn, issue_id=issue.id)
        assert issue_pr is not None
        assert issue_pr.pr_number == 43
        assert issue_pr.pr_url == "https://github.com/org/repo/pull/43"
        assert issue_pr.created_at == "2026-05-09T00:00:00+00:00"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_start_review_stage_adopted_live_review_keeps_pr_metadata_for_bad_url(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        issue = _issue()
        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        await db.review_state.begin_review(
            conn,
            issue.id,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo=binding.github_repo,
            issue_label=None,
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=issue.id,
            github_repo=binding.github_repo,
            binding_key="old-binding",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-09T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="live-review",
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )

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
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001

        run = await orch._start_review_stage(  # noqa: SLF001
            binding=binding,
            issue=issue,
            storage_issue_id=issue.id,
            pr_url="not-a-pr-url",
        )

        assert run.id == "live-review"
        state = await db.review_state.get(conn, issue.id)
        assert state.pr_number == 42
        assert state.pr_url == "https://github.com/org/repo/pull/42"
        issue_pr = await db.issue_prs.get_for_issue(conn, issue_id=issue.id)
        assert issue_pr is not None
        assert issue_pr.pr_number == 42
        assert issue_pr.pr_url == "https://github.com/org/repo/pull/42"
    finally:
        await conn.close()


# --- Failure visibility and retry ------------------------------------------


async def _seed_failed_review(  # type: ignore[no-untyped-def]
    conn,
    *,
    run_id: str = "review-run",
    status: str = "failed",
) -> None:
    """Seed: review run exists but is dead (monitor died)."""
    await db.issues.upsert(conn, id="iss-1", identifier="ENG-1", title="Add auth", team_key="ENG")
    await db.review_state.begin_review(
        conn,
        "iss-1",
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        github_repo="org/repo",
        issue_label=None,
    )
    await db.runs.create(
        conn,
        id=run_id,
        issue_id="iss-1",
        stage="review",
        status="running",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
    )
    await db.runs.update_status(conn, run_id, status, ended_at="2026-05-10T00:05:00+00:00")
    await db.issue_prs.upsert(
        conn,
        issue_id="iss-1",
        github_repo="org/repo",
        binding_key="",
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        created_at="2026-05-10T00:00:00+00:00",
    )


@pytest.mark.asyncio
async def test_review_failure_does_not_register_operator_wait(
    tmp_path: Path,
) -> None:
    """When _fail_review_run fires, no operator_wait is created — auto-retry
    via _resurrect_review_runs instead of requiring a manual /retry."""
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
            "#!/usr/bin/env python3\nimport sys\nsys.stderr.write('network down\\n')\nsys.exit(1)\n"
        )
        gh_shim.chmod(0o755)

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=GitHub(gh_path=str(gh_shim)))
        await _poll_review_and_wait(orch)

        # Review run should be failed.
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[0].status == "failed"
        # No operator wait — resurrection handles auto-retry.
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_operator_wait_moves_to_needs_input_lane(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue_in_code_review()
        binding = RepoBinding(
            linear_team_key="ENG",
            github_repo="org/repo",
            linear_states=LinearStates(
                ready="Todo",
                code_review="In Review",
                needs_approval="Needs Input",
            ),
        )
        assert poll_module._review_issue_is_active(issue, binding) is True  # noqa: SLF001
        assert (  # noqa: SLF001
            poll_module._review_issue_is_active(_issue_needs_input(), binding) is False
        )

        await db.issues.upsert(
            conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        await db.review_state.begin_review(
            conn,
            issue.id,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label=None,
        )
        run = db.runs.Run(
            id="review-run",
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
            ended_at=None,
            cost_usd=0.0,
        )
        await db.runs.create(
            conn,
            id=run.id,
            issue_id=run.issue_id,
            stage=run.stage,
            status=run.status,
            pid=run.pid,
            started_at=run.started_at,
        )

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=MagicMock())
        orch._states = {  # noqa: SLF001
            "ENG": {
                "Todo": "state-todo",
                "In Progress": "state-progress",
                "In Review": "state-review",
                "Needs Input": "state-input",
                "Blocked": "state-bl",
                "Done": "state-done",
            }
        }

        await orch._fail_review_run(  # noqa: SLF001
            run=run,
            binding=binding,
            issue=issue,
            error="review fix-run failed",
            last_log="",
            auto_retry=False,
            operator_wait=True,
        )

        linear.move_issue.assert_awaited_once_with(issue.id, "state-input")
        wait = await db.operator_waits.get(conn, issue.id)
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_REVIEW_FAILED
        history = await db.runs.history_for_issue(conn, issue.id)
        assert history[-1].status == "failed"
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["failed", "interrupted"])
async def test_dead_review_monitor_is_resurrected_after_cooldown(
    tmp_path: Path,
    terminal_status: str,
) -> None:
    """_resurrect_review_runs creates a new review run for an orphaned PR."""
    from datetime import UTC, datetime, timedelta

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_failed_review(conn, status=terminal_status)
        # Simulate the run ended > REVIEW_RESURRECT_COOLDOWN_SECS ago.
        old_ended_at = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
        await conn.execute("UPDATE runs SET ended_at = ? WHERE id = 'review-run'", (old_ended_at,))
        await conn.commit()

        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)

        gh = MagicMock()
        gh.pr_checks = AsyncMock(return_value=PRChecks())
        gh.pr_view = AsyncMock(return_value={"headRefOid": "sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001
        tasks = await orch._resurrect_review_runs()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        history = await db.runs.history_for_issue(conn, "iss-1")
        live = [r for r in history if r.stage == "review" and r.status == "running"]
        assert len(live) == 1, "expected one new running review run"
        # Operator gets a Linear comment so they know the review restarted.
        assert linear.post_comment.await_count >= 1
        posted = linear.post_comment.await_args.args[1]
        assert "Resumed" in posted
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_completed_review_monitor_rearmed_when_remote_review_unapproved(
    tmp_path: Path,
) -> None:
    """A review run that COMPLETED without ever monitoring the GitHub review
    (e.g. remote_review was flipped on after the review stage finished) is
    re-armed so the PR's @codex feedback gets a watcher again."""
    from datetime import UTC, datetime, timedelta

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="Add auth", team_key="ENG"
        )
        await db.review_state.begin_review(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label=None,
        )
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            binding_key="",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="review-run",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        # Completed monitor, ended past the resurrection cooldown.
        old_ended = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
        await db.runs.update_status(conn, "review-run", "completed", ended_at=old_ended)

        cfg = Config(
            repos=[_binding()],  # remote_review=True by default
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)

        gh = MagicMock()
        gh.pr_checks = AsyncMock(return_value=PRChecks())
        gh.pr_view = AsyncMock(return_value={"headRefOid": "sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001
        tasks = await orch._resurrect_review_runs()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        history = await db.runs.history_for_issue(conn, "iss-1")
        live = [r for r in history if r.stage == "review" and r.status == "running"]
        assert len(live) == 1, "expected the completed monitor to be re-armed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_completed_review_monitor_not_rearmed_when_approved(
    tmp_path: Path,
) -> None:
    """A completed-monitor PR that is already APPROVED is left alone — it is
    awaiting merge, not review."""
    from datetime import UTC, datetime, timedelta
    from unittest.mock import AsyncMock as _AsyncMock

    from symphony.pipeline.review_classifier import Verdict, VerdictKind

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="Add auth", team_key="ENG"
        )
        await db.review_state.begin_review(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label=None,
        )
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            binding_key="",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="review-run",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        old_ended = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
        await db.runs.update_status(conn, "review-run", "completed", ended_at=old_ended)

        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001
        orch._review_verdict_for_pr = _AsyncMock(  # noqa: SLF001
            return_value=Verdict(kind=VerdictKind.APPROVED, rule="approved")
        )

        tasks = await orch._resurrect_review_runs()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        history = await db.runs.history_for_issue(conn, "iss-1")
        live = [r for r in history if r.stage == "review" and r.status == "running"]
        assert live == [], "approved PR must not be re-monitored"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_startup_reconcile_pidless_review_run_is_resurrected_on_next_tick(
    tmp_path: Path,
) -> None:
    """A host restart strands in-process review monitors with pid=NULL.

    Startup reconciliation must make that row terminal, then the normal poll
    tick should immediately resurrect the open PR monitor.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            binding_key="",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )

        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)
        linear.issues_in_state = AsyncMock(return_value=[])
        linear.comments_since = AsyncMock(return_value=[])

        flipped = await reconcile(conn, linear)
        assert flipped == 1
        original = await db.runs.latest_for_issue_stage(conn, issue_id="iss-1", stage="review")
        assert original is not None
        assert original.status == db.runs.INTERRUPTED_STATUS
        assert original.ended_at is None

        gh = MagicMock()
        gh.pr_checks = AsyncMock(return_value=PRChecks())
        gh.pr_view = AsyncMock(return_value={"headRefOid": "sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001
        tasks = await orch._tick()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        history = await db.runs.history_for_issue(conn, "iss-1")
        live = [r for r in history if r.stage == "review" and r.status == "running"]
        assert len(live) == 1
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Host restarted" in body for body in bodies)
        assert any("Resumed" in body for body in bodies)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_resurrection_rearms_codex_review_for_unreviewed_head_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A monitor resurrected after the @codex ping was lost must re-arm review.

    This matches the VIB-52 failure mode: a dead pid=NULL review monitor, then
    a current PR HEAD with only stale Codex signals from older commits. The
    prior CI dedupe signature must survive the re-arm.
    """
    from datetime import UTC, datetime, timedelta

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, signature="ci:head-sha:lint")
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            binding_key="",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        old_ended_at = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
        await db.runs.update_status(conn, "review-run", "interrupted", ended_at=old_ended_at)

        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)
        linear.issues_in_state = AsyncMock(return_value=[])
        linear.comments_since = AsyncMock(return_value=[])

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-19T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(commit_sha="stale-sha")])
        gh.pr_review_comments = AsyncMock(
            return_value=[
                _codex_inline_comment(
                    commit_sha="stale-sha",
                    created_at="2026-05-19T11:30:00Z",
                )
            ]
        )
        gh.pr_comment = AsyncMock()

        def issue_comments_after_ping(*_args, **_kwargs) -> list[dict]:
            if gh.pr_comment.await_count:
                return [_codex_review_request_comment()]
            return []

        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(side_effect=issue_comments_after_ping)

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001

        monkeypatch.setattr(
            orch,
            "_schedule_review_poll",
            lambda *_args, **_kwargs: asyncio.create_task(asyncio.sleep(0)),
        )

        tasks = await orch._tick()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        review_pings = [
            c for c in gh.pr_comment.await_args_list if c.args[:2] == (42, "@codex review")
        ]
        assert len(review_pings) == 1
        state = await db.review_state.get(conn, "iss-1")
        assert state.last_trigger_signature == "ci:head-sha:lint"

        history = await db.runs.history_for_issue(conn, "iss-1")
        resurrected = next(
            r
            for r in history
            if r.stage == "review" and r.status == "running" and r.id != "review-run"
        )
        await db.runs.update_status(conn, resurrected.id, "interrupted", ended_at=old_ended_at)

        tasks = await orch._tick()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        review_pings = [
            c for c in gh.pr_comment.await_args_list if c.args[:2] == (42, "@codex review")
        ]
        assert len(review_pings) == 1
        state = await db.review_state.get(conn, "iss-1")
        assert state.last_trigger_signature == "ci:head-sha:lint"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_resurrection_rearm_defers_when_request_comments_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not duplicate-ping when the request-comment dedupe read is down."""
    from datetime import UTC, datetime, timedelta

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            binding_key="",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        old_ended_at = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
        await db.runs.update_status(conn, "review-run", "interrupted", ended_at=old_ended_at)

        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)
        linear.issues_in_state = AsyncMock(return_value=[])
        linear.comments_since = AsyncMock(return_value=[])

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-19T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(commit_sha="stale-sha")])
        gh.pr_review_comments = AsyncMock(
            return_value=[
                _codex_inline_comment(
                    commit_sha="stale-sha",
                    created_at="2026-05-19T11:30:00Z",
                )
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        issue_comment_calls = 0

        async def issue_comments_after_transient_failure(
            *_args: object, **_kwargs: object
        ) -> list[dict]:
            nonlocal issue_comment_calls
            issue_comment_calls += 1
            if issue_comment_calls == 1:
                raise GitHubError("temporary issue comment fetch failure")
            return [_codex_review_request_comment()]

        gh.pr_issue_comments = AsyncMock(side_effect=issue_comments_after_transient_failure)
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001
        captured_runs: list[db.runs.Run] = []

        def capture_review_poll(
            run: db.runs.Run,
            _binding: RepoBinding,
            _issue: LinearIssue,
        ) -> asyncio.Task[None]:
            captured_runs.append(run)
            return asyncio.create_task(asyncio.sleep(0))

        monkeypatch.setattr(orch, "_schedule_review_poll", capture_review_poll)

        tasks = await orch._resurrect_review_runs()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        assert len(captured_runs) == 1
        resurrected = captured_runs[0]
        assert gh.pr_comment.await_count == 0
        assert await db.runs.has_review_rearm_retry(conn, resurrected.id)

        await orch._poll_review_run_with_limits(  # noqa: SLF001
            resurrected,
            _binding(),
            _issue_in_review(),
        )

        assert gh.pr_comment.await_count == 0
        assert not await db.runs.has_review_rearm_retry(conn, resurrected.id)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_resurrection_retries_rearm_after_classification_failure(
    tmp_path: Path,
) -> None:
    """Transient GitHub read errors during resurrection do not drop the re-arm."""
    from datetime import UTC, datetime, timedelta

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            binding_key="",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        old_ended_at = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
        await db.runs.update_status(conn, "review-run", "interrupted", ended_at=old_ended_at)

        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)
        linear.issues_in_state = AsyncMock(return_value=[])
        linear.comments_since = AsyncMock(return_value=[])

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-19T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(commit_sha="stale-sha")])
        gh.pr_review_comments = AsyncMock(
            side_effect=[GitHubError("temporary comments fetch failure"), [], []]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._tick()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        review_pings = [
            c for c in gh.pr_comment.await_args_list if c.args[:2] == (42, "@codex review")
        ]
        assert len(review_pings) == 1
        assert gh.pr_review_comments.await_count >= 2
        assert not orch._review_rearm_retry_run_ids  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_resurrection_rearm_retry_survives_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An inconclusive resurrection re-arm is retried after orchestrator restart."""
    from datetime import UTC, datetime, timedelta

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            binding_key="",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        old_ended_at = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
        await db.runs.update_status(conn, "review-run", "interrupted", ended_at=old_ended_at)

        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)
        linear.issues_in_state = AsyncMock(return_value=[])
        linear.comments_since = AsyncMock(return_value=[])

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-19T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(commit_sha="stale-sha")])
        gh.pr_review_comments = AsyncMock(
            side_effect=[GitHubError("temporary comments fetch failure"), [], []]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001

        captured_runs: list[db.runs.Run] = []

        def capture_review_poll(
            run: db.runs.Run,
            _binding: RepoBinding,
            _issue: LinearIssue,
        ) -> asyncio.Task[None]:
            captured_runs.append(run)
            return asyncio.create_task(asyncio.sleep(0))

        monkeypatch.setattr(orch, "_schedule_review_poll", capture_review_poll)
        tasks = await orch._resurrect_review_runs()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        assert len(captured_runs) == 1
        resurrected = captured_runs[0]
        assert await db.runs.has_review_rearm_retry(conn, resurrected.id)
        assert gh.pr_comment.await_count == 0

        restarted = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        restarted._states = {"ENG": _states()}  # noqa: SLF001
        tasks = await restarted._poll_review_runs()  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        review_pings = [
            c for c in gh.pr_comment.await_args_list if c.args[:2] == (42, "@codex review")
        ]
        assert len(review_pings) == 1
        assert not await db.runs.has_review_rearm_retry(conn, resurrected.id)
        assert not restarted._review_rearm_retry_run_ids  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_rearm_retry_clears_when_fallback_poll_dispatches_feedback(
    tmp_path: Path,
) -> None:
    """A failed re-arm classifier must not leave a stale retry after fallback.

    The re-arm path can fail on head commit lookup, while the normal poll path
    treats that same lookup as optional and can still consume current inline
    feedback. Once that happens, the retry marker is stale.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        await db.runs.mark_review_rearm_retry(conn, "review-run")
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock(return_value=None)

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "head-sha", "mergeable": "MERGEABLE"},
                {"headRefOid": "head-sha", "mergeable": "MERGEABLE"},
                {"headRefOid": "after-fix-sha", "mergeable": "MERGEABLE"},
                {"headRefOid": "after-fix-sha", "mergeable": "MERGEABLE"},
            ]
        )
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(
            side_effect=[
                GitHubError("temporary commit lookup failure"),
                GitHubError("temporary commit lookup failure"),
                "2026-05-19T12:05:00Z",
                "2026-05-19T12:05:00Z",
            ]
        )
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(
            side_effect=[
                [_codex_inline_comment(commit_sha="head-sha")],
                [_codex_inline_comment(commit_sha="head-sha")],
                [],
                [],
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner(
            [RunnerEvent(kind="started", pid=999), RunnerEvent(kind="exit", returncode=0)]
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
        assert not await db.runs.has_review_rearm_retry(conn, "review-run")
        assert not orch._review_rearm_retry_run_ids  # noqa: SLF001
        assert gh.pr_comment.await_count == 1

        await asyncio.sleep(0)
        await _poll_review_and_wait(orch)

        assert gh.pr_comment.await_count == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_rearms_missing_codex_signal_once_per_head(
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
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-20T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(commit_sha="stale-sha")])
        gh.pr_review_comments = AsyncMock(
            return_value=[
                _codex_inline_comment(
                    commit_sha="stale-sha",
                    created_at="2026-05-20T11:30:00Z",
                )
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        await _poll_review_and_wait(orch)

        gh.pr_comment.assert_awaited_once_with(42, "@codex review", repo="org/repo")
        assert ("review-run", "head-sha") in orch._review_no_signal_rearm_heads  # noqa: SLF001

        await _poll_review_and_wait(orch)

        gh.pr_comment.assert_awaited_once_with(42, "@codex review", repo="org/repo")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_does_not_rearm_no_signal_for_local_only(
    tmp_path: Path,
) -> None:
    # local-only bindings (local_review=True, remote_review=False) drop the
    # legacy @codex fallback: no-signal must never re-trigger a remote review,
    # even when the local reviewer failed.
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        await _seed_local_review_cycle(conn, local_review_status="failed")
        cfg = Config(
            repos=[_binding(local_review=True, remote_review=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-20T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(commit_sha="stale-sha")])
        gh.pr_review_comments = AsyncMock(
            return_value=[
                _codex_inline_comment(
                    commit_sha="stale-sha",
                    created_at="2026-05-20T11:30:00Z",
                )
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        await _poll_review_and_wait(orch)

        gh.pr_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_does_not_rearm_no_signal_for_local_approval(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        await _seed_local_review_cycle(conn, local_review_status="completed")
        cfg = Config(
            repos=[_binding(local_review=True, remote_review=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-20T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(commit_sha="stale-sha")])
        gh.pr_review_comments = AsyncMock(
            return_value=[
                _codex_inline_comment(
                    commit_sha="stale-sha",
                    created_at="2026-05-20T11:30:00Z",
                )
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        await _poll_review_and_wait(orch)

        gh.pr_comment.assert_not_awaited()
        assert not orch._review_no_signal_rearm_heads  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_ignores_codex_feedback_when_remote_review_disabled(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding(local_review=True, remote_review=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "CONFLICTING"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-20T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry()])
        gh.pr_review_comments = AsyncMock(return_value=[_codex_inline_comment()])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "id": 123,
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Didn't find any major issues.",
                    "created_at": "2026-05-20T12:01:00Z",
                }
            ]
        )
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner(
            [RunnerEvent(kind="started", pid=999), RunnerEvent(kind="exit", returncode=0)]
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

        assert runner.captured_spec is None
        workspace.acquire.assert_not_awaited()
        push_fn.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
        gh.pr_reviews.assert_awaited_once_with(42, repo="org/repo")
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reactions.assert_not_awaited()
        gh.pr_issue_comments.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_honors_human_changes_requested_when_remote_review_disabled(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        cfg = Config(
            repos=[_binding(local_review=True, remote_review=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        human_review = {
            "user": {"login": "human-reviewer"},
            "state": "CHANGES_REQUESTED",
            "commit_id": "head-sha",
            "submitted_at": "2026-05-20T12:02:00Z",
            "body": "Please fix the persisted handoff before merge.",
        }
        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-20T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(), human_review])
        gh.pr_review_comments = AsyncMock(return_value=[_codex_inline_comment()])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner(
            [RunnerEvent(kind="started", pid=999), RunnerEvent(kind="exit", returncode=0)]
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
        prompt = runner.captured_spec.command[-1]
        assert "Please fix the persisted handoff before merge." in prompt
        gh.pr_reviews.assert_awaited_once_with(42, repo="org/repo")
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reactions.assert_not_awaited()
        gh.pr_issue_comments.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")

        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 1
        assert state.last_trigger_signature == "human_cr:head-sha:human-reviewer"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_rearms_missing_codex_signal_after_head_changes(
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
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-20T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(commit_sha="stale-sha")])
        gh.pr_review_comments = AsyncMock(
            return_value=[
                _codex_inline_comment(
                    commit_sha="stale-sha",
                    created_at="2026-05-20T11:30:00Z",
                )
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        await _poll_review_and_wait(orch)

        gh.pr_comment.assert_awaited_once_with(42, "@codex review", repo="org/repo")

        gh.pr_view = AsyncMock(
            return_value={"headRefOid": "next-head-sha", "mergeable": "MERGEABLE"}
        )
        gh.commit_committed_at = AsyncMock(return_value="2026-05-20T12:10:00Z")

        await _poll_review_and_wait(orch)

        assert gh.pr_comment.await_count == 2
        assert ("review-run", "head-sha") in orch._review_no_signal_rearm_heads  # noqa: SLF001
        assert ("review-run", "next-head-sha") in orch._review_no_signal_rearm_heads  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_poll_does_not_rearm_when_codex_lgtm_is_current(
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
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-20T12:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "id": 9999,
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Codex Review: Didn't find any major issues.",
                    "created_at": "2026-05-20T12:01:00Z",
                }
            ]
        )
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        await _poll_review_and_wait(orch)

        gh.pr_comment.assert_not_awaited()
        assert not orch._review_no_signal_rearm_heads  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_dead_review_monitor_not_resurrected_within_cooldown(
    tmp_path: Path,
) -> None:
    """Resurrection is suppressed when the last failure is too recent."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_failed_review(conn)
        # ended_at is already 2026-05-10T00:05:00Z — far in the past from
        # the DB seed, but we set it to "now" to simulate a recent failure.
        from datetime import UTC, datetime

        recent_ended_at = datetime.now(UTC).isoformat()
        await conn.execute(
            "UPDATE runs SET ended_at = ? WHERE id = 'review-run'", (recent_ended_at,)
        )
        await conn.commit()

        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=MagicMock())
        tasks = await orch._resurrect_review_runs()  # noqa: SLF001

        assert tasks == [], "should not resurrect within cooldown window"
        history = await db.runs.history_for_issue(conn, "iss-1")
        live = [r for r in history if r.stage == "review" and r.status == "running"]
        assert len(live) == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_slash_command_restarts_review_monitor(tmp_path: Path) -> None:
    """After review failure, /retry creates a new review run and re-posts @codex review."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_failed_review(conn)

        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        gh.pr_comment = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])

        from symphony.linear.client import LinearComment

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._states = {"ENG": _states()}  # noqa: SLF001
        # Manually set up the operator wait as _fail_review_run would have.
        await orch._track_review_failed_wait("iss-1", "review-run", _binding())  # noqa: SLF001

        retry_comment = LinearComment(
            id="c-retry",
            body="$retry",
            created_at="2026-05-10T01:00:00+00:00",
            author_name="user",
            author_is_me=False,
            external_thread_type=None,
        )
        linear.comments_since = AsyncMock(return_value=[retry_comment])

        await orch._poll_slash_commands()  # noqa: SLF001
        await asyncio.sleep(0)  # let spawned tasks settle

        # A new running review run should exist.
        history = await db.runs.history_for_issue(conn, "iss-1")
        running = [r for r in history if r.stage == "review" and r.status == "running"]
        assert len(running) == 1
        # @codex review should have been re-posted.
        gh.pr_comment.assert_awaited_once_with(42, "@codex review", repo="org/repo")
        # Operator wait should be cleared.
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_slash_reruns_hybrid_local_gate_before_remote_review(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(local_review=True, remote_review=True)
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Add auth",
            team_key="ENG",
        )
        await db.runs.create(
            conn,
            id="implement-run",
            issue_id="iss-1",
            stage="implement",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="local-review-run",
            issue_id="iss-1",
            stage="local_review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "local-review-run",
            "failed",
            ended_at="2026-05-10T00:02:00+00:00",
            kind="stuck_loop",
            detail="local review stuck loop",
        )
        await db.review_state.begin_review(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label=None,
        )
        await db.runs.create(
            conn,
            id="review-run",
            issue_id="iss-1",
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:03:00+00:00",
        )
        await db.runs.update_status(
            conn,
            "review-run",
            "needs_approval",
            ended_at="2026-05-10T00:04:00+00:00",
            kind="stuck_loop",
            detail="local review stuck loop",
        )

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        gh = MagicMock()
        gh.pr_comment = AsyncMock()
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
        schedule_mock = MagicMock()
        orch._schedule_review_poll = schedule_mock  # type: ignore[method-assign]  # noqa: SLF001
        local_review_phase = AsyncMock(
            return_value=LoopResult(
                outcome=LoopOutcome.APPROVED,
                iterations=1,
                verdicts=(
                    LocalVerdict(
                        kind=LocalVerdictKind.APPROVED,
                        findings="",
                    ),
                ),
            )
        )
        orch._run_local_review_phase = local_review_phase  # type: ignore[method-assign]  # noqa: SLF001
        await orch._track_review_failed_wait(  # noqa: SLF001
            "iss-1", "review-run", binding
        )

        await orch._handle_review_failed_slash_intent(  # noqa: SLF001
            "iss-1",
            "review-run",
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T01:00:00+00:00",
            ),
        )

        local_review_phase.assert_awaited_once()
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.pr_comment.assert_awaited_once_with(42, "@codex review", repo="org/repo")
        assert await db.operator_waits.get(conn, "iss-1") is None
        assert linear.move_issue.await_args_list == [call("iss-1", "state-na")]
    finally:
        await conn.close()


@pytest.mark.parametrize(
    "kind",
    [SlashKind.APPROVE, SlashKind.REJECT, SlashKind.STOP],
)
@pytest.mark.asyncio
async def test_merge_command_keeps_operator_wait_when_lookup_fails(
    tmp_path: Path,
    kind: SlashKind,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Add auth",
            team_key="ENG",
        )
        await _seed_active_review(conn)
        await db.runs.create(
            conn,
            id="merge-run",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="merge-run",
            kind=db.operator_waits.KIND_MERGE,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label="",
            created_at="2026-05-10T00:00:00+00:00",
        )

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = MagicMock()
        linear.team_states = AsyncMock(return_value=_states())
        linear.lookup_issue = AsyncMock(side_effect=LinearError("lookup down"))
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=MagicMock())
        orch._dispatch_run_ids["iss-1"] = "merge-run"  # noqa: SLF001
        orch._operator_wait_run_ids.add("merge-run")  # noqa: SLF001
        orch._merge_needs_approval_bindings["merge-run"] = binding  # noqa: SLF001

        with pytest.raises(SlashHandlerFailure) as excinfo:
            await orch._handle_merge_needs_approval_slash_intent(  # noqa: SLF001
                "iss-1",
                "merge-run",
                SlashIntent(
                    kind=kind,
                    comment_id="c-command",
                    created_at="2026-05-10T00:01:00+00:00",
                ),
            )
        assert "look up" in excinfo.value.reason

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.run_id == "merge-run"
        assert orch._dispatch_run_ids["iss-1"] == "merge-run"  # noqa: SLF001
        assert "merge-run" in orch._operator_wait_run_ids  # noqa: SLF001
        assert orch._merge_needs_approval_bindings["merge-run"] is binding  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_command_keeps_operator_wait_when_merge_dispatch_dedupes(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding()
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="Add auth",
            team_key="ENG",
        )
        await _seed_active_review(conn)
        await db.runs.create(
            conn,
            id="merge-run",
            issue_id="iss-1",
            stage="merge",
            status="needs_approval",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="fix-run",
            issue_id="iss-1",
            stage="review_fix",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:30+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="merge-run",
            kind=db.operator_waits.KIND_MERGE,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label="",
            created_at="2026-05-10T00:00:00+00:00",
        )

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock()
        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=MagicMock())
        orch._dispatch_run_ids["iss-1"] = "merge-run"  # noqa: SLF001
        orch._operator_wait_run_ids.add("merge-run")  # noqa: SLF001
        orch._merge_needs_approval_bindings["merge-run"] = binding  # noqa: SLF001

        await orch._handle_merge_needs_approval_slash_intent(  # noqa: SLF001
            "iss-1",
            "merge-run",
            SlashIntent(
                kind=SlashKind.APPROVE,
                comment_id="c-command",
                created_at="2026-05-10T00:01:00+00:00",
            ),
        )
        tasks = list(orch._dispatch_tasks)  # noqa: SLF001
        assert len(tasks) == 1
        await asyncio.gather(*tasks)

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.run_id == "merge-run"
        assert orch._dispatch_run_ids["iss-1"] == "merge-run"  # noqa: SLF001
        assert "merge-run" in orch._operator_wait_run_ids  # noqa: SLF001
        assert orch._merge_needs_approval_bindings["merge-run"] is binding  # noqa: SLF001
        linear.post_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_on_active_review_monitor_retriggers_review(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, signature="codex_inline:stale")
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        orch._review_poll_run_ids.add("review-run")  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = "review-run"  # noqa: SLF001

        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            "review-run",
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T00:01:00+00:00",
            ),
        )

        gh.pr_comment.assert_awaited_once_with(42, "@codex review", repo="org/repo")
        state = await db.review_state.get(conn, "iss-1")
        assert state.last_trigger_signature == "manual_retry:review-run:c-retry"
        history = await db.runs.history_for_issue(conn, "iss-1")
        running_review_runs = [r for r in history if r.stage == "review" and r.status == "running"]
        assert [r.id for r in running_review_runs] == ["review-run"]
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Review retry requested" in body for body in posted)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_unhandled_retry_slash_posts_rejection(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=MagicMock())
        await orch._handle_slash_intent(  # noqa: SLF001
            "iss-1",
            "implement-run",
            SlashIntent(
                kind=SlashKind.RETRY,
                comment_id="c-retry",
                created_at="2026-05-10T00:01:00+00:00",
            ),
        )

        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("$retry" in body and "ignored" in body for body in posted)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_skip_review_advances_to_merge(tmp_path: Path) -> None:
    """`$skip-review` during active review polling marks the review run completed
    and directly schedules merge, bypassing the Codex verdict."""
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
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()

        from symphony.linear.client import LinearComment

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)

        # Manually register the review run as if _schedule_review_poll had run.
        review_run_id = "review-run"
        orch._review_poll_run_ids.add(review_run_id)  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = review_run_id  # noqa: SLF001

        skip_comment = LinearComment(
            id="c-skip",
            body="$skip-review",
            created_at="2026-05-10T01:00:00+00:00",
            author_name="user",
            author_is_me=False,
            external_thread_type=None,
        )
        linear.comments_since = AsyncMock(return_value=[skip_comment])

        await orch._poll_slash_commands()  # noqa: SLF001
        await asyncio.sleep(0)

        # Review run should now be completed.
        history = await db.runs.history_for_issue(conn, "iss-1")
        review_run = next(r for r in history if r.stage == "review")
        assert review_run.status == "completed"

        # A merge run should have been created.
        merge_runs = [r for r in history if r.stage == "merge"]
        assert len(merge_runs) >= 1

        # Linear comment should mention skip/merge.
        assert linear.post_comment.called
        posted_body = linear.post_comment.call_args[0][1]
        assert "skip" in posted_body.lower() or "merge" in posted_body.lower()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_skip_review_works_when_fix_run_active(tmp_path: Path) -> None:
    """`$skip-review` must succeed even when a concurrent review_fix run is the
    active dispatch run — the monitor run_id is looked up from _review_poll_issue_ids."""
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
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()

        from symphony.linear.client import LinearComment

        runner = MagicMock()
        runner.kill = AsyncMock()
        orch = Orchestrator(cfg, linear, conn, runner=runner, gh=gh)

        # Register the review monitor.
        review_run_id = "review-run"
        orch._review_poll_run_ids.add(review_run_id)  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = review_run_id  # noqa: SLF001

        # Simulate a concurrent fix-run being the active dispatch run.
        fix_run_id = "fix-run"
        orch._dispatch_run_ids["iss-1"] = fix_run_id  # noqa: SLF001
        orch._active_run_ids.add(fix_run_id)  # noqa: SLF001

        skip_comment = LinearComment(
            id="c-skip",
            body="$skip-review",
            created_at="2026-05-10T01:00:00+00:00",
            author_name="user",
            author_is_me=False,
            external_thread_type=None,
        )
        linear.comments_since = AsyncMock(return_value=[skip_comment])

        await orch._poll_slash_commands()  # noqa: SLF001
        await asyncio.sleep(0)

        # Review run should now be completed (not rejected).
        history = await db.runs.history_for_issue(conn, "iss-1")
        review_run = next(r for r in history if r.stage == "review")
        assert review_run.status == "completed"

        # A merge run should have been created.
        merge_runs = [r for r in history if r.stage == "merge"]
        assert len(merge_runs) >= 1

        # Must NOT have posted a rejection message.
        for call in linear.post_comment.call_args_list:
            body = call[0][1]
            assert "cannot skip" not in body
        runner.kill.assert_awaited_once_with(fix_run_id)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_skip_review_aborts_when_active_fix_run_cannot_be_killed(
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
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        runner = MagicMock()
        runner.kill = AsyncMock(side_effect=RuntimeError("still running"))

        from symphony.linear.client import LinearComment

        orch = Orchestrator(cfg, linear, conn, runner=runner, gh=gh)

        review_run_id = "review-run"
        orch._review_poll_run_ids.add(review_run_id)  # noqa: SLF001
        orch._review_poll_issue_ids["iss-1"] = review_run_id  # noqa: SLF001

        fix_run_id = "fix-run"
        orch._dispatch_run_ids["iss-1"] = fix_run_id  # noqa: SLF001
        orch._active_run_ids.add(fix_run_id)  # noqa: SLF001

        skip_comment = LinearComment(
            id="c-skip",
            body="$skip-review",
            created_at="2026-05-10T01:00:00+00:00",
            author_name="user",
            author_is_me=False,
            external_thread_type=None,
        )
        linear.comments_since = AsyncMock(return_value=[skip_comment])

        await orch._poll_slash_commands()  # noqa: SLF001

        history = await db.runs.history_for_issue(conn, "iss-1")
        review_run = next(r for r in history if r.stage == "review")
        assert review_run.status == "running"
        assert not [r for r in history if r.stage == "merge"]
        assert orch._dispatch_run_ids["iss-1"] == fix_run_id  # noqa: SLF001
        assert orch._review_poll_issue_ids["iss-1"] == review_run_id  # noqa: SLF001
        runner.kill.assert_awaited_once_with(fix_run_id)
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("could not stop active review fix-run" in b for b in posted)
    finally:
        await conn.close()


@pytest.mark.parametrize("failure_mode", ["agent", "continue"])
@pytest.mark.asyncio
async def test_merge_conflict_fix_run_aborts_rebase_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
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
        linear.post_comment = AsyncMock(return_value="cmt-1")

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.repo_default_branch = AsyncMock(return_value="main")

        sync_workspace = AsyncMock(return_value=None)
        monkeypatch.setattr(review_module, "_sync_workspace_to_remote", sync_workspace)
        monkeypatch.setattr(review_module, "_git_fetch", AsyncMock(return_value=None))
        monkeypatch.setattr(review_module, "_git_rebase", AsyncMock(return_value=False))
        monkeypatch.setattr(
            review_module,
            "_git_conflicted_files",
            AsyncMock(return_value=["conflicted.py"]),
        )
        abort_rebase = AsyncMock(return_value=None)
        monkeypatch.setattr(review_module, "_git_abort_rebase", abort_rebase)
        add_and_continue = AsyncMock(
            side_effect=RuntimeError("continue failed") if failure_mode == "continue" else None
        )
        monkeypatch.setattr(
            review_module,
            "_git_add_and_continue_rebase",
            add_and_continue,
        )

        returncode = 0 if failure_mode == "continue" else 1
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=returncode)])
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            force_push_fn=AsyncMock(),
        )

        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")
        issue = _issue_in_progress()
        result = await orch._dispatch_merge_conflict_fix_run(  # noqa: SLF001
            run=run,
            binding=cfg.repos[0],
            issue=issue,
            iteration=1,
        )

        assert result is False
        sync_workspace.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        abort_rebase.assert_awaited_once_with(workspace_path)
        if failure_mode == "continue":
            add_and_continue.assert_awaited_once_with(workspace_path, ["conflicted.py"])
        else:
            add_and_continue.assert_not_awaited()
        workspace.release.assert_called_once_with(cfg.repos[0], issue)

        history = await db.runs.history_for_issue(conn, "iss-1")
        monitor = next(r for r in history if r.id == "review-run")
        fix_run = next(r for r in history if r.stage == "review_fix")
        assert monitor.status == "failed"
        assert fix_run.status == "failed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_fix_reports_status_when_rebase_has_no_unresolved_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
        linear.post_comment = AsyncMock(return_value="cmt-1")

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.repo_default_branch = AsyncMock(return_value="main")

        sync_workspace = AsyncMock(return_value=None)
        monkeypatch.setattr(review_module, "_sync_workspace_to_remote", sync_workspace)
        monkeypatch.setattr(review_module, "_git_fetch", AsyncMock(return_value=None))
        monkeypatch.setattr(review_module, "_git_rebase", AsyncMock(return_value=False))
        monkeypatch.setattr(
            review_module,
            "_git_conflicted_files",
            AsyncMock(return_value=[]),
        )
        monkeypatch.setattr(
            review_module,
            "_git_status_short",
            AsyncMock(return_value="M  ui/src/pages/GoalDashboard.test.tsx"),
        )
        abort_rebase = AsyncMock(return_value=None)
        monkeypatch.setattr(review_module, "_git_abort_rebase", abort_rebase)

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            force_push_fn=AsyncMock(),
        )

        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")
        issue = _issue_in_progress()
        result = await orch._dispatch_merge_conflict_fix_run(  # noqa: SLF001
            run=run,
            binding=cfg.repos[0],
            issue=issue,
            iteration=1,
        )

        assert result is False
        sync_workspace.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        abort_rebase.assert_awaited_once_with(workspace_path)
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("rebase failed with no unresolved paths" in b for b in posted), posted
        assert any("GoalDashboard.test.tsx" in b for b in posted), posted
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_fix_uses_synced_head_as_noop_baseline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
        linear.post_comment = AsyncMock(return_value="cmt-1")

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.repo_default_branch = AsyncMock(return_value="main")
        gh.pr_comment = AsyncMock()

        synced = False

        async def sync_workspace(_workspace_path: Path, _branch: str) -> None:
            nonlocal synced
            synced = True

        async def workspace_head(_workspace_path: Path) -> str:
            return "remote-head-sha" if synced else "stale-local-sha"

        async def workspace_ref(_workspace_path: Path, _ref: str) -> str:
            return "remote-head-sha" if synced else "stale-local-sha"

        monkeypatch.setattr(review_module, "_sync_workspace_to_remote", sync_workspace)
        monkeypatch.setattr(review_module, "_workspace_head_sha", workspace_head)
        monkeypatch.setattr(review_module, "_workspace_ref_sha", workspace_ref)
        monkeypatch.setattr(review_module, "_git_fetch", AsyncMock(return_value=None))
        monkeypatch.setattr(review_module, "_git_rebase", AsyncMock(return_value=True))

        force_push = AsyncMock()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=_FakeRunner([]),
            gh=gh,
            workspace=workspace,
            force_push_fn=force_push,
        )

        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")
        issue = _issue_in_progress()
        result = await orch._dispatch_merge_conflict_fix_run(  # noqa: SLF001
            run=run,
            binding=cfg.repos[0],
            issue=issue,
            iteration=1,
        )

        assert result is False
        force_push.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert not any("Fix pushed" in b for b in posted), posted
        assert any("completed without advancing symphony/eng-1" in b for b in posted), posted

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_REVIEW_FAILED

        history = await db.runs.history_for_issue(conn, "iss-1")
        monitor = next(r for r in history if r.id == "review-run")
        fix_run = next(r for r in history if r.stage == "review_fix")
        assert monitor.status == "failed"
        assert fix_run.status == "failed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_fix_run_continues_through_later_conflicts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
        linear.post_comment = AsyncMock(return_value="cmt-1")

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.repo_default_branch = AsyncMock(return_value="main")
        gh.pr_comment = AsyncMock()

        sync_workspace = AsyncMock(return_value=None)
        monkeypatch.setattr(review_module, "_sync_workspace_to_remote", sync_workspace)
        monkeypatch.setattr(review_module, "_git_fetch", AsyncMock(return_value=None))
        monkeypatch.setattr(review_module, "_git_rebase", AsyncMock(return_value=False))
        conflicted_files = AsyncMock(side_effect=[["first.py"], ["second.py"]])
        monkeypatch.setattr(
            review_module,
            "_git_conflicted_files",
            conflicted_files,
        )
        abort_rebase = AsyncMock(return_value=None)
        monkeypatch.setattr(review_module, "_git_abort_rebase", abort_rebase)
        add_and_continue = AsyncMock(side_effect=[False, True])
        monkeypatch.setattr(
            review_module,
            "_git_add_and_continue_rebase",
            add_and_continue,
        )

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        force_push = AsyncMock()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            force_push_fn=force_push,
        )

        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")
        issue = _issue_in_progress()
        result = await orch._dispatch_merge_conflict_fix_run(  # noqa: SLF001
            run=run,
            binding=cfg.repos[0],
            issue=issue,
            iteration=1,
        )

        assert result is True
        sync_workspace.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        assert conflicted_files.await_count == 2
        assert add_and_continue.await_args_list == [
            call(workspace_path, ["first.py"]),
            call(workspace_path, ["second.py"]),
        ]
        abort_rebase.assert_not_awaited()
        force_push.assert_awaited_once_with(workspace_path, "symphony/eng-1")

        history = await db.runs.history_for_issue(conn, "iss-1")
        fix_run = next(r for r in history if r.stage == "review_fix")
        assert fix_run.status == "completed"
    finally:
        await conn.close()


# --- Reviewer comment fix-runs ---------------------------------------------


def _codex_inline_comment(
    *,
    commit_sha: str = "head-sha",
    created_at: str = "2026-05-11T17:52:34Z",
) -> dict:
    return {
        "user": {"login": "chatgpt-codex-connector[bot]"},
        "body": "Mark dry-run items with a terminal status",
        "commit_id": commit_sha,
        "original_commit_id": commit_sha,
        "created_at": created_at,
        "path": "backend/app/routes/optimize.py",
        "line": 42,
    }


def _codex_review_entry(*, commit_sha: str = "head-sha", state: str = "COMMENTED") -> dict:
    return {
        "user": {"login": "chatgpt-codex-connector[bot]"},
        "state": state,
        "commit_id": commit_sha,
        "submitted_at": "2026-05-11T17:52:34Z",
        "body": "review body",
    }


def _codex_review_request_comment(
    *,
    created_at: str = "2026-05-19T12:01:00Z",
) -> dict:
    return {
        "id": 1001,
        "user": {"login": "review-operator"},
        "body": "@codex review",
        "created_at": created_at,
    }


@pytest.mark.asyncio
async def test_codex_inline_comment_dispatches_fix_run_and_posts_linear_activity(
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

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry()])
        gh.pr_review_comments = AsyncMock(return_value=[_codex_inline_comment()])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner(
            [RunnerEvent(kind="started", pid=999), RunnerEvent(kind="exit", returncode=0)]
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

        # Fix run should have been dispatched.
        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "review"
        prompt = runner.captured_spec.command[-1]
        assert "Mark dry-run items" in prompt
        assert "backend/app/routes/optimize.py" in prompt

        # Linear activity comments: one before dispatch, one after push.
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Reviewer feedback detected" in b for b in posted), posted
        assert any("Fix pushed" in b for b in posted), posted

        # Branch pushed and @codex review re-triggered.
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.pr_comment.assert_awaited_with(42, "@codex review", repo="org/repo")

        # Iteration and signature persisted.
        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 1
        assert state.last_trigger_signature.startswith("codex_inline:")

        # A review_fix run was created and completed.
        history = await db.runs.history_for_issue(conn, "iss-1")
        fix_runs = [r for r in history if r.stage == "review_fix"]
        assert len(fix_runs) == 1
        assert fix_runs[0].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_fix_without_new_commit_parks_without_retrigger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unchanged_head(_workspace_path: Path) -> str:
        return "same-head-sha"

    async def unchanged_ref(_workspace_path: Path, _ref: str) -> str:
        return "same-head-sha"

    monkeypatch.setattr(review_module, "_workspace_head_sha", unchanged_head)
    monkeypatch.setattr(review_module, "_workspace_ref_sha", unchanged_ref)

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

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry()])
        gh.pr_review_comments = AsyncMock(return_value=[_codex_inline_comment()])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner(
            [RunnerEvent(kind="started", pid=999), RunnerEvent(kind="exit", returncode=0)]
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
        push_fn.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()

        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Reviewer feedback detected" in b for b in posted), posted
        assert not any("Fix pushed" in b for b in posted), posted
        assert any(
            "completed without advancing symphony/eng-1" in b
            and "Reply with `$retry` or `$approve`" in b
            for b in posted
        ), posted

        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_REVIEW_FAILED
        assert wait.run_id == "review-run"

        history = await db.runs.history_for_issue(conn, "iss-1")
        monitor = next(r for r in history if r.id == "review-run")
        fix_run = next(r for r in history if r.stage == "review_fix")
        assert monitor.status == "failed"
        assert fix_run.status == "failed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_fix_pushes_existing_local_commit_from_prior_push_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def local_head(_workspace_path: Path) -> str:
        return "local-unpushed-sha"

    async def remote_ref(_workspace_path: Path, _ref: str) -> str:
        return "remote-head-sha"

    monkeypatch.setattr(review_module, "_workspace_head_sha", local_head)
    monkeypatch.setattr(review_module, "_workspace_ref_sha", remote_ref)

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

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.pr_reviews = AsyncMock(side_effect=[[_codex_review_entry()], []])
        gh.pr_review_comments = AsyncMock(side_effect=[[_codex_inline_comment()], []])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner(
            [RunnerEvent(kind="started", pid=999), RunnerEvent(kind="exit", returncode=0)]
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

        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.pr_comment.assert_awaited_with(42, "@codex review", repo="org/repo")
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Fix pushed" in b for b in posted), posted
        assert await db.operator_waits.get(conn, "iss-1") is None

        history = await db.runs.history_for_issue(conn, "iss-1")
        fix_run = next(r for r in history if r.stage == "review_fix")
        assert fix_run.status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_fix_skips_retrigger_when_pr_is_already_approved(
    tmp_path: Path,
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
        await _seed_active_review(conn)

        gh = MagicMock()
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "head-sha",
                    "submitted_at": "2026-05-11T18:00:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:55:00Z")
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=gh,
        )
        state = await db.review_state.get(conn, "iss-1")

        await orch._retrigger_codex_review_unless_approved(  # noqa: SLF001
            binding=binding,
            issue=_issue_in_progress(),
            state=state,
        )

        gh.pr_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retrigger_is_noop_when_remote_review_disabled(
    tmp_path: Path,
) -> None:
    # A local-only binding (remote_review=False) must never re-trigger @codex,
    # even when the PR has no approval. The guard short-circuits before any
    # GitHub lookup, so the call is a clean no-op that reports success (no
    # resurrection retry).
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(local_review=True, remote_review=False)
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        await _seed_active_review(conn)

        gh = MagicMock()
        gh.pr_view = AsyncMock()
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(cfg, AsyncMock(), conn, runner=MagicMock(), gh=gh)
        state = await db.review_state.get(conn, "iss-1")

        result = await orch._retrigger_codex_review_unless_approved(  # noqa: SLF001
            binding=binding,
            issue=_issue_in_progress(),
            state=state,
            require_no_signal=True,
        )

        assert result is True
        gh.pr_view.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_retrigger_skips_when_codex_inline_feedback_is_current(
    tmp_path: Path,
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
        await _seed_active_review(conn)

        gh = MagicMock()
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.pr_review_comments = AsyncMock(return_value=[_codex_inline_comment()])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.pr_comment = AsyncMock()

        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=gh,
        )
        state = await db.review_state.get(conn, "iss-1")

        await orch._retrigger_codex_review_unless_approved(  # noqa: SLF001
            binding=binding,
            issue=_issue_in_progress(),
            state=state,
            require_no_signal=True,
        )

        gh.pr_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_fix_skips_retrigger_when_codex_lgtm_already_arrived(
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

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.pr_reviews = AsyncMock(side_effect=[[_codex_review_entry()], []])
        gh.pr_review_comments = AsyncMock(side_effect=[[_codex_inline_comment()], []])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(
            side_effect=[
                [],
                [
                    {
                        "id": 9999,
                        "user": {"login": "chatgpt-codex-connector[bot]"},
                        "body": (
                            "Codex Review: Didn't find any major issues. "
                            "More of your lovely PRs please."
                        ),
                        "created_at": "2026-05-11T18:00:00Z",
                    }
                ],
                [
                    {
                        "id": 9999,
                        "user": {"login": "chatgpt-codex-connector[bot]"},
                        "body": (
                            "Codex Review: Didn't find any major issues. "
                            "More of your lovely PRs please."
                        ),
                        "created_at": "2026-05-11T18:00:00Z",
                    }
                ],
            ]
        )
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner(
            [RunnerEvent(kind="started", pid=999), RunnerEvent(kind="exit", returncode=0)]
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

        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.pr_comment.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_codex_signals_before_head_commit_do_not_dispatch(
    tmp_path: Path,
) -> None:
    """GitHub can re-anchor old inline comments to the current diff. The
    review monitor must still treat comments created before the current head
    commit as stale, it must not announce an old Codex LGTM as current, and
    the live monitor should re-arm remote review for the unreviewed head.
    """
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
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-13T19:03:52Z")
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(
            return_value=[
                _codex_inline_comment(
                    commit_sha="head-sha",
                    created_at="2026-05-13T17:58:44Z",
                )
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "id": 9999,
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Codex Review: Didn't find any major issues. Hooray!",
                    "created_at": "2026-05-13T19:02:36Z",
                }
            ]
        )
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        orch = Orchestrator(cfg, linear, conn, runner=runner, gh=gh)

        await _poll_review_and_wait(orch)

        assert runner.captured_spec is None
        gh.pr_comment.assert_awaited_once_with(42, "@codex review", repo="org/repo")
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert not any("Reviewer feedback detected" in b for b in posted), posted
        assert not any("Codex reviewed" in b for b in posted), posted

        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 0
        assert state.last_trigger_signature == ""
        assert state.codex_lgtm_comment_id == ""
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_later_codex_lgtm_issue_comment_supersedes_inline_feedback(
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
        linear.lookup_issue = AsyncMock(return_value=_issue_in_review())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="unit", state="SUCCESS", bucket="pass", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-13T19:00:00Z")
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(
            return_value=[
                _codex_inline_comment(
                    commit_sha="head-sha",
                    created_at="2026-05-13T19:05:00Z",
                )
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "id": 9999,
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Codex Review: Didn't find any major issues. Hooray!",
                    "created_at": "2026-05-13T19:20:00Z",
                }
            ]
        )

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        orch = Orchestrator(cfg, linear, conn, runner=runner, gh=gh)

        await _poll_review_and_wait(orch)

        assert runner.captured_spec is None
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Codex reviewed" in b and "no issues" in b for b in posted), posted
        assert not any("Reviewer feedback detected" in b for b in posted), posted

        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 0
        assert state.last_trigger_signature == ""
        assert state.codex_lgtm_comment_id == "9999"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_codex_inline_comment_dedup_skips_identical_back_to_back(
    tmp_path: Path,
) -> None:
    from symphony.pipeline.review_classifier import ReviewComment, _comment_key, _stable_digest

    comment = _codex_inline_comment()
    rc = ReviewComment(
        user_login="chatgpt-codex-connector[bot]",
        body=comment["body"],
        commit_sha=comment["commit_id"],
        created_at=comment["created_at"],
        path=comment["path"],
        line=comment["line"],
    )
    sig = "codex_inline:" + _stable_digest([_comment_key(rc)])

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, signature=sig)
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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry()])
        gh.pr_review_comments = AsyncMock(return_value=[_codex_inline_comment()])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        orch = Orchestrator(cfg, linear, conn, runner=runner, gh=gh)

        await _poll_review_and_wait(orch)

        assert runner.captured_spec is None
        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failing_ci_dispatch_retrigger_checks_existing_approval(
    tmp_path: Path,
) -> None:
    """A fresh red CI signature still avoids inline review reads."""
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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_comment = AsyncMock()
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])

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
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_review_and_wait(orch)

        assert runner.captured_spec is not None
        # The retrigger guard checks approval state, but does not need inline
        # review comments once red CI has already selected the fix path.
        gh.pr_reviews.assert_awaited_once_with(42, repo="org/repo")
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reactions.assert_awaited_once_with(42, repo="org/repo")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_deduped_failing_ci_still_dispatches_fresh_codex_inline_feedback(
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
        linear.post_comment = AsyncMock(return_value="cmt-1")

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
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.commit_committed_at = AsyncMock(return_value="2026-05-11T17:50:00Z")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry()])
        gh.pr_review_comments = AsyncMock(return_value=[_codex_inline_comment()])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
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
        prompt = runner.captured_spec.command[-1]
        assert "Mark dry-run items" in prompt
        gh.check_log_tail.assert_not_awaited()

        state = await db.review_state.get(conn, "iss-1")
        assert state.last_trigger_signature.startswith("codex_inline:")
        history = await db.runs.history_for_issue(conn, "iss-1")
        fix_runs = [r for r in history if r.stage == "review_fix"]
        assert len(fix_runs) == 1
        assert fix_runs[0].status == "completed"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_deduped_failing_ci_still_dispatches_human_review_when_remote_disabled(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn, signature="ci:head-sha:lint")
        cfg = Config(
            repos=[_binding(local_review=True, remote_review=False)],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue_in_progress())
        linear.post_comment = AsyncMock(return_value="cmt-1")

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        human_review = {
            "user": {"login": "human-reviewer"},
            "state": "CHANGES_REQUESTED",
            "commit_id": "head-sha",
            "submitted_at": "2026-05-20T12:02:00Z",
            "body": "Please fix the persisted handoff before merge.",
        }
        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                [CheckRun(name="lint", state="FAILURE", bucket="fail", link=None)]
            )
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.commit_committed_at = AsyncMock(return_value="2026-05-20T12:00:00Z")
        gh.check_log_tail = AsyncMock(return_value="lint failed")
        gh.pr_reviews = AsyncMock(return_value=[_codex_review_entry(), human_review])
        gh.pr_review_comments = AsyncMock(return_value=[_codex_inline_comment()])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.pr_comment = AsyncMock()

        runner = _FakeRunner(
            [RunnerEvent(kind="started", pid=999), RunnerEvent(kind="exit", returncode=0)]
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
        prompt = runner.captured_spec.command[-1]
        assert "Please fix the persisted handoff before merge." in prompt
        gh.check_log_tail.assert_not_awaited()
        gh.pr_reviews.assert_awaited_once_with(42, repo="org/repo")
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reactions.assert_not_awaited()
        gh.pr_issue_comments.assert_not_awaited()
        gh.pr_comment.assert_not_awaited()
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")

        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 1
        assert state.last_trigger_signature == "human_cr:head-sha:human-reviewer"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_codex_lgtm_comment_posts_to_linear_once(tmp_path: Path) -> None:
    """When Codex posts a 'no major issues' issue comment, a Linear notification
    is posted exactly once — subsequent polls are deduped by comment ID."""
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

        gh = MagicMock()
        gh.pr_checks = AsyncMock(
            return_value=PRChecks([CheckRun(name="ci", state="FAILURE", bucket="fail", link=None)])
        )
        gh.pr_view = AsyncMock(return_value={"headRefOid": "head-sha", "mergeable": "MERGEABLE"})
        gh.head_sha = AsyncMock(return_value="head-sha")
        gh.check_log_tail = AsyncMock(return_value="test failed")
        gh.pr_comment = AsyncMock()
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "id": 9999,
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Codex Review: Didn't find any major issues. Delightful!",
                    "created_at": "2026-05-10T00:01:00Z",
                }
            ]
        )

        workspace_path = tmp_path / "ws" / "org_repo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

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
        orch._states = {"ENG": _states()}  # noqa: SLF001

        # First poll: should post the Codex LGTM comment to Linear.
        await _poll_review_and_wait(orch)
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Codex reviewed" in b and "no issues" in b for b in posted), posted

        # Comment ID should be persisted.
        state = await db.review_state.get(conn, "iss-1")
        assert state.codex_lgtm_comment_id == "9999"

        # Second poll: same comment ID — should NOT re-post.
        linear.post_comment.reset_mock()
        await _poll_review_and_wait(orch)
        posted_again = [c.args[1] for c in linear.post_comment.await_args_list]
        assert not any("Codex reviewed" in b for b in posted_again), posted_again
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_codex_lgtm_comment_linear_failure_does_not_crash_or_dedupe(
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
        linear.post_comment = AsyncMock(side_effect=LinearError("linear down"))

        gh = MagicMock()
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "id": 9999,
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Codex Review: Didn't find any major issues. Delightful!",
                    "created_at": "2026-05-10T00:01:00Z",
                }
            ]
        )

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")
        state = await db.review_state.get(conn, "iss-1")

        await orch._maybe_post_codex_lgtm(  # noqa: SLF001
            run=run,
            binding=cfg.repos[0],
            issue=_issue_in_progress(),
            state=state,
            pr_number=42,
        )

        linear.post_comment.assert_awaited_once()
        state = await db.review_state.get(conn, "iss-1")
        assert state.codex_lgtm_comment_id == ""
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_codex_lgtm_comment_ignores_previous_review_cycle(
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
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "id": 9999,
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Codex Review: Didn't find any major issues. Delightful!",
                    "created_at": "2026-05-09T23:59:59Z",
                }
            ]
        )

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")
        state = await db.review_state.get(conn, "iss-1")

        await orch._maybe_post_codex_lgtm(  # noqa: SLF001
            run=run,
            binding=cfg.repos[0],
            issue=_issue_in_progress(),
            state=state,
            pr_number=42,
        )

        linear.post_comment.assert_not_awaited()
        state = await db.review_state.get(conn, "iss-1")
        assert state.codex_lgtm_comment_id == ""
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_codex_lgtm_comment_survives_review_monitor_restart(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(
            conn,
            started_at="2026-05-10T00:10:00+00:00",
        )
        await db.issue_prs.upsert(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        cfg = Config(
            repos=[_binding()],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")

        gh = MagicMock()
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "id": 9999,
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Codex Review: Didn't find any major issues. Delightful!",
                    "created_at": "2026-05-10T00:05:00Z",
                }
            ]
        )

        orch = Orchestrator(cfg, linear, conn, runner=MagicMock(), gh=gh)
        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")
        state = await db.review_state.get(conn, "iss-1")

        await orch._maybe_post_codex_lgtm(  # noqa: SLF001
            run=run,
            binding=cfg.repos[0],
            issue=_issue_in_progress(),
            state=state,
            pr_number=42,
        )

        linear.post_comment.assert_awaited_once()
        state = await db.review_state.get(conn, "iss-1")
        assert state.codex_lgtm_comment_id == "9999"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_concurrent_ci_fix_dispatch_creates_single_review_fix_run(
    tmp_path: Path,
) -> None:
    """Two fix-run dispatches racing on one issue must yield exactly one live
    ``review_fix`` run (SYM-152). The loser bails: it releases the workspace it
    acquired and does not clobber ``_dispatch_run_ids``.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_active_review(conn)
        binding = _binding()
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
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.check_log_tail = AsyncMock(return_value="lint failed")
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

        agent_entered = asyncio.Event()
        release = asyncio.Event()

        async def blocking_agent(**_kwargs: object) -> tuple[UsageDelta, str, int]:
            agent_entered.set()
            await release.wait()
            return (UsageDelta(cost_usd=0.01), "exit", 0)

        orch._run_fix_agent = AsyncMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=blocking_agent
        )

        history = await db.runs.history_for_issue(conn, "iss-1")
        run = next(r for r in history if r.id == "review-run")

        async def dispatch() -> bool:
            return await orch._dispatch_ci_fix_run(  # noqa: SLF001
                run=run,
                binding=binding,
                issue=_issue_in_progress(),
                checks=_failing_ci_checks(),
                verdict=_failing_ci_verdict(),
                iteration=1,
            )

        # First dispatch wins the atomic insert and parks in the blocked agent.
        winner = asyncio.create_task(dispatch())
        await agent_entered.wait()

        # Second dispatch races in while the first review_fix run is still live.
        # The fix makes it bail before the agent; without it the loser inserts a
        # second run and blocks in the agent forever (caught here as a timeout
        # rather than a hung suite).
        loser_dispatched = await asyncio.wait_for(dispatch(), timeout=5)

        assert loser_dispatched is False
        # Loser released exactly the workspace it acquired (winner is still
        # parked in the agent and has not released yet).
        assert workspace.release.call_count == 1

        live = [
            r
            for r in await db.runs.history_for_issue(conn, "iss-1")
            if r.stage == "review_fix" and r.status == "running"
        ]
        assert len(live) == 1
        # Loser did not clobber the winner's dispatch id.
        assert orch._dispatch_run_ids["iss-1"] == live[0].id  # noqa: SLF001

        winner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await winner

        fix_runs = [
            r for r in await db.runs.history_for_issue(conn, "iss-1") if r.stage == "review_fix"
        ]
        assert len(fix_runs) == 1
    finally:
        await conn.close()
