"""End-to-end dispatch with `review_strategy=local`.

A binding configured for local review must (a) run the in-workspace
reviewer after the implementer succeeds and (b) skip the `@codex review`
PR ping when the local pass approves. The remote review monitor row is
still created — CI checks and human approvals still drive merge.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.pipeline.local_review import VERDICT_APPROVED_MARKER


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


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
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

        # Issue still moved into the review state so CI/operator can act.
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-na" in move_targets

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
async def test_local_strategy_no_pr_summary_when_not_approved(
    tmp_path: Path,
) -> None:
    """When local-review fails / exhausts / etc., don't post the
    summary — the @codex fallback ping is enough audit trail."""
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
            cfg, linear, conn, runner=runner, gh=gh,
            workspace=workspace, push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, cfg.repos[0])

        # @codex fallback ping should fire; local-review summary should not.
        codex_calls = [
            c for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body"))
            == "@codex review"
        ]
        summary_calls = [
            c for c in gh.pr_comment.await_args_list
            if "local reviewer" in str(c).lower()
        ]
        assert len(codex_calls) == 1
        assert len(summary_calls) == 0
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
    should return `None`, the gate function should fall back to the
    remote `@codex` ping, and the PR must still be created.
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

        # PR created despite the local-review fault.
        gh.pr_create.assert_awaited_once()
        # Remote @codex review fired as the safety net.
        codex_calls = [
            c for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body"))
            == "@codex review"
        ]
        assert len(codex_calls) == 1, (
            "expected @codex fallback when local-review phase raised"
        )
        # Implement run still recorded.
        history = await db.runs.history_for_issue(conn, "iss-1")
        stages = {h.stage for h in history}
        assert "implement" in stages
        assert "review" in stages  # remote review monitor created
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_local_strategy_falls_back_to_codex_when_reviewer_fails(
    tmp_path: Path,
) -> None:
    """The reviewer subprocess fails to spawn → safety net kicks in."""
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

        # Reviewer crashed → remote @codex review must be posted as the safety net.
        codex_pings = [
            c
            for c in gh.pr_comment.await_args_list
            if (c.args[1] if len(c.args) >= 2 else c.kwargs.get("body"))
            == "@codex review"
        ]
        assert len(codex_pings) == 1, (
            "expected remote @codex fallback when local reviewer fails"
        )
    finally:
        await conn.close()
