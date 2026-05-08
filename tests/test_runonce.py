"""Tests for symphony.runonce orchestration.

Covers the M2 happy path and the empty-diff / failed-exit guards. We patch the
network-touching helpers (`view_issue`, `tracked_issues`, `name_with_owner`,
`open_pr`, `comment_pr`), the worktree helper
(`ensure_worktree`), the agent runner (`run_agent`), and the local git helpers
(`_commits_ahead`, `_git_push`) so the orchestration logic itself is what we
exercise.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from symphony import runonce as ro_mod
from symphony.config import (
    AgentConfig,
    Config,
    GitConfig,
    GithubConfig,
    OrchestratorConfig,
    PathsConfig,
    RepoConfig,
)
from symphony.events import EventLog
from symphony.github import GithubError, Issue, IssueComment, TrackedIssue, PR
from symphony.types import AgentResult


def _make_config(tmp_path: Path) -> Config:
    return Config(
        repo=RepoConfig(path=tmp_path / "repo", default_branch="main"),
        github=GithubConfig(label="auto"),
        git=GitConfig(author_name="Symphony", author_email="sym@example.com"),
        orchestrator=OrchestratorConfig(
            poll_interval_s=60,
            max_concurrent=3,
            review_round_cap=10,
            codex_renudge_after_min=10,
            codex_giveup_after_min=30,
        ),
        agent=AgentConfig(model="claude-opus-4-7", max_turns=50),
        paths=PathsConfig(
            worktree_root=tmp_path / "wts",
            prompts_dir=Path(__file__).parent.parent / "prompts",
        ),
    )


def _ok_agent_result() -> AgentResult:
    return AgentResult(
        session_id="s",
        exit_code=0,
        success=True,
        is_error=False,
        duration_ms=1,
        num_turns=1,
        total_cost_usd=0.0,
        final_text="done",
        raw_events=[],
        stderr="",
    )


def _failed_agent_result() -> AgentResult:
    return AgentResult(
        session_id="s",
        exit_code=1,
        success=False,
        is_error=True,
        duration_ms=1,
        num_turns=1,
        total_cost_usd=0.0,
        final_text=None,
        raw_events=[],
        stderr="boom",
    )


def _patch_happy_path(
    monkeypatch,
    tmp_path,
    *,
    agent_result=None,
    head_before="aaa1111",
    head_after="bbb2222",
    to_push=1,
    loop_outcome_kind=None,
) -> dict[str, Any]:
    """Wire stubs for each external dep and capture the calls."""
    calls: dict[str, Any] = {}
    cfg = _make_config(tmp_path)
    config_path = tmp_path / "symphony.toml"
    config_path.write_text("# stub\n")

    monkeypatch.setattr(ro_mod, "load_config", lambda p: (calls.setdefault("load", p), cfg)[1])

    issue = Issue(
        number=3,
        title="happy path",
        body="do thing",
        labels=["auto"],
        comments=[IssueComment(author="ak", body="hi")],
    )
    monkeypatch.setattr(
        ro_mod, "view_issue", lambda n, *, repo_path: (calls.setdefault("view", (n, repo_path)), issue)[1]
    )
    monkeypatch.setattr(
        ro_mod,
        "get_issue_state",
        lambda n, *, repo_path: (calls.setdefault("issue_state", (n, repo_path)), "OPEN")[1],
    )
    monkeypatch.setattr(
        ro_mod,
        "tracked_issues",
        lambda n, *, repo_path: [
            TrackedIssue(
                number=1, title="dep", state="CLOSED", state_reason="COMPLETED",
                pr_url="https://x/pr/8",
            ),
            TrackedIssue(
                number=2, title="open dep", state="OPEN", state_reason=None, pr_url=None,
            ),
        ],
    )
    monkeypatch.setattr(
        ro_mod, "name_with_owner", lambda repo_path: ("kulichevskiy", "symphony")
    )
    wt = tmp_path / "wts" / "symphony-3"
    wt.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        ro_mod,
        "ensure_worktree",
        lambda **kw: (calls.setdefault("worktree", kw), wt)[1],
    )

    prompts_seen: dict[str, str] = {}

    async def _fake_run_agent(prompt, workdir, **kwargs):
        prompts_seen["prompt"] = prompt
        prompts_seen["workdir"] = workdir
        prompts_seen["kwargs"] = kwargs
        return agent_result if agent_result is not None else _ok_agent_result()

    monkeypatch.setattr(ro_mod, "run_agent", _fake_run_agent)
    calls["agent"] = prompts_seen

    head_calls: list[str] = []

    def _fake_head_sha(worktree):
        head_calls.append(str(worktree))
        # First call (pre-agent) returns head_before; second (post-agent) returns head_after.
        return head_before if len(head_calls) == 1 else head_after

    monkeypatch.setattr(ro_mod, "_head_sha", _fake_head_sha)
    calls["head_sha_calls"] = head_calls
    monkeypatch.setattr(
        ro_mod,
        "_commits_to_push",
        lambda worktree, branch, base: (
            calls.setdefault("to_push", (worktree, branch, base)),
            to_push,
        )[1],
    )
    monkeypatch.setattr(
        ro_mod, "_git_push", lambda worktree, branch: calls.setdefault("push", (worktree, branch))
    )

    pr = PR(number=99, url="https://github.com/kulichevskiy/symphony/pull/99")

    def _fake_open_pr(**kw):
        calls["open_pr"] = kw
        return pr

    monkeypatch.setattr(ro_mod, "open_pr", _fake_open_pr)
    # Default: no existing PR for the branch — open_pr will be called.
    monkeypatch.setattr(
        ro_mod,
        "find_open_pr_for_branch",
        lambda branch, **kw: (calls.setdefault("find_pr", (branch, kw)), None)[1],
    )
    monkeypatch.setattr(
        ro_mod, "comment_pr", lambda **kw: calls.setdefault("comment_pr", kw)
    )
    monkeypatch.setattr(
        ro_mod, "merge_pr", lambda **kw: calls.setdefault("merge_pr", kw)
    )
    monkeypatch.setattr(ro_mod, "is_pr_merged", lambda **kw: True)
    monkeypatch.setattr(
        ro_mod,
        "_cleanup_after_merge",
        lambda **kw: calls.setdefault("cleanup_after_merge", kw),
    )

    # Stub the review loop so existing tests don't have to thread its inputs.
    from symphony.reviewer import LoopOutcome, LoopOutcomeKind
    if loop_outcome_kind is None:
        loop_outcome_kind = LoopOutcomeKind.APPROVED

    async def _fake_loop(**kw):
        calls["drive_review_loop"] = kw
        return LoopOutcome(
            kind=loop_outcome_kind,
            rounds_used=0,
            last_session_id=kw.get("initial_session_id"),
            head_sha="head-sha",
        )

    monkeypatch.setattr(ro_mod, "drive_review_loop", _fake_loop)

    return {"calls": calls, "config_path": config_path, "cfg": cfg, "wt": wt, "pr": pr}


@pytest.mark.asyncio
async def test_run_once_happy_path_creates_pr_with_closes_marker(monkeypatch, tmp_path):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    calls = fixture["calls"]
    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.pr is not None
    assert res.pr.number == 99
    assert res.skipped is False

    # Worktree was created with sanitized repo name and bot identity
    wt_args = calls["worktree"]
    assert wt_args["repo_name"] == "symphony"
    assert wt_args["issue_number"] == 3
    assert wt_args["base_branch"] == "main"
    assert wt_args["author_name"] == "Symphony"
    assert wt_args["author_email"] == "sym@example.com"

    # Agent received the rendered prompt with Closes-style sections
    prompt = calls["agent"]["prompt"]
    assert "issue #3: happy path" in prompt
    assert "auto/3" in prompt
    assert "kulichevskiy/symphony" in prompt
    assert "Satisfied dependencies" in prompt
    # Open dep (#2) is not satisfied → must not appear
    assert "#2" not in prompt
    # Satisfied dep (#1) is rendered with PR URL
    assert "#1 dep (PR https://x/pr/8)" in prompt

    # Push was performed before PR creation
    assert calls["push"] == (fixture["wt"], "auto/3")

    # PR body has Closes marker and Symphony footer
    body = calls["open_pr"]["body"]
    assert "Closes #3" in body
    assert "Auto-generated by Symphony." in body
    assert calls["open_pr"]["head"] == "auto/3"
    assert calls["open_pr"]["base"] == "main"
    assert calls["open_pr"]["title"] == "happy path"

    # PR lookup was disambiguated by base branch and head-repo owner so a
    # stranger's same-named branch from a fork can't get the @codex nudge.
    assert calls["find_pr"][0] == "auto/3"
    assert calls["find_pr"][1]["base_branch"] == "main"
    assert calls["find_pr"][1]["expected_owner"] == "kulichevskiy"

    # @codex review nudge posted
    assert calls["comment_pr"]["pr_number"] == 99
    assert calls["comment_pr"]["body"] == "@codex review"

    # Review loop ran with the cfg-derived timers and the agent's session id.
    loop_kwargs = calls["drive_review_loop"]
    assert loop_kwargs["pr_number"] == 99
    assert loop_kwargs["branch"] == "auto/3"
    # Round cap, re-nudge and give-up timers are derived from cfg.
    assert loop_kwargs["poll_interval_s"] == fixture["cfg"].orchestrator.poll_interval_s
    assert loop_kwargs["round_cap"] == fixture["cfg"].orchestrator.review_round_cap
    assert loop_kwargs["re_nudge_after_s"] == fixture["cfg"].orchestrator.codex_renudge_after_min * 60.0
    assert loop_kwargs["give_up_after_s"] == fixture["cfg"].orchestrator.codex_giveup_after_min * 60.0

    # Merge is attempted only after the review loop reports approval.
    assert calls["merge_pr"]["pr_number"] == 99
    assert calls["merge_pr"]["match_head_commit"] == "head-sha"


@pytest.mark.asyncio
async def test_run_once_does_not_merge_when_review_loop_does_not_approve(monkeypatch, tmp_path):
    from symphony.reviewer import LoopOutcomeKind

    fixture = _patch_happy_path(
        monkeypatch,
        tmp_path,
        loop_outcome_kind=LoopOutcomeKind.AUTO_STUCK_ROUNDS,
    )
    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])
    assert res.skipped is False
    assert res.loop_outcome is not None
    assert res.loop_outcome.kind == LoopOutcomeKind.AUTO_STUCK_ROUNDS
    assert "merge_pr" not in fixture["calls"]


@pytest.mark.asyncio
async def test_run_once_writes_event_log(monkeypatch, tmp_path):
    fixture = _patch_happy_path(monkeypatch, tmp_path)

    await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    event_log = EventLog.for_repo(fixture["cfg"].repo.path)
    kinds = [e.kind for e in event_log.iter_events(issue_number=3)]
    assert kinds[:4] == ["agent-start", "agent-exit", "push", "pr-open"]
    assert kinds[-1] == "merge"
    pr_event = [e for e in event_log.iter_events(issue_number=3) if e.kind == "pr-open"][0]
    assert pr_event.payload["number"] == 99
    assert pr_event.payload["reused"] is False


@pytest.mark.asyncio
async def test_run_once_cleans_up_worktree_and_branches_after_successful_merge(
    monkeypatch, tmp_path
):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    calls = fixture["calls"]

    order: list[str] = []

    def _record_merge(**kw):
        order.append("merge_pr")
        calls["merge_pr"] = kw

    def _record_cleanup(**kw):
        order.append("cleanup")
        calls["cleanup_after_merge"] = kw

    monkeypatch.setattr(ro_mod, "merge_pr", _record_merge)
    monkeypatch.setattr(ro_mod, "_cleanup_after_merge", _record_cleanup)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.loop_outcome is not None
    assert res.loop_outcome.kind.value == "approved"
    assert order == ["merge_pr", "cleanup"]
    assert calls["cleanup_after_merge"] == {
        "repo_path": fixture["cfg"].repo.path,
        "worktree": fixture["wt"],
        "branch": "auto/3",
    }


@pytest.mark.asyncio
async def test_run_once_skips_cleanup_when_merge_pending(monkeypatch, tmp_path):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    monkeypatch.setattr(ro_mod, "is_pr_merged", lambda **kw: False)

    await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert "cleanup_after_merge" not in fixture["calls"]


@pytest.mark.asyncio
async def test_run_once_skips_cleanup_when_merge_fails(monkeypatch, tmp_path):
    fixture = _patch_happy_path(monkeypatch, tmp_path)

    def _fail_merge(**kw):
        fixture["calls"]["merge_pr"] = kw
        raise GithubError("blocked")

    monkeypatch.setattr(ro_mod, "merge_pr", _fail_merge)

    await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert "cleanup_after_merge" not in fixture["calls"]


def test_cleanup_after_merge_removes_worktree_before_branch(monkeypatch, tmp_path):
    """The whole reason this helper exists: `git branch -D` refuses to delete
    a branch that's still checked out in a worktree, so the worktree teardown
    has to come first. Regressing the order would resurrect the original bug.
    """
    import subprocess as sp

    invocations: list[list[str]] = []

    class _Result:
        returncode = 0
        stdout = ""
        stderr = ""

    def _fake_run(cmd, **kwargs):
        invocations.append(list(cmd))
        return _Result()

    monkeypatch.setattr(sp, "run", _fake_run)
    monkeypatch.setattr(ro_mod.subprocess, "run", _fake_run)

    repo = tmp_path / "repo"
    worktree = tmp_path / "wts" / "symphony-3"
    ro_mod._cleanup_after_merge(
        repo_path=repo,
        worktree=worktree,
        branch="auto/3",
    )

    actions = [cmd[1] if len(cmd) > 1 else cmd[0] for cmd in invocations]
    assert actions == ["worktree", "branch", "push"]
    assert invocations[0] == [
        "git",
        "worktree",
        "remove",
        "--force",
        str(worktree),
    ]
    assert invocations[1] == ["git", "branch", "-D", "auto/3"]
    assert invocations[2] == ["git", "push", "origin", "--delete", "auto/3"]


def test_cleanup_after_merge_warns_but_does_not_raise_on_failure(
    monkeypatch, tmp_path, caplog
):
    class _FailResult:
        returncode = 1
        stdout = ""
        stderr = "boom"

    def _fake_run(cmd, **kwargs):
        return _FailResult()

    monkeypatch.setattr(ro_mod.subprocess, "run", _fake_run)

    caplog.set_level("WARNING", logger=ro_mod.log.name)
    ro_mod._cleanup_after_merge(
        repo_path=tmp_path,
        worktree=tmp_path / "wt",
        branch="auto/3",
    )
    messages = [record.getMessage() for record in caplog.records]
    assert any("git worktree remove" in m for m in messages)


@pytest.mark.asyncio
async def test_run_once_records_merge_failure_as_terminal_outcome(monkeypatch, tmp_path):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    calls = fixture["calls"]

    def _fail_merge(**kw):
        calls["merge_pr"] = kw
        raise GithubError("merge blocked")

    monkeypatch.setattr(ro_mod, "merge_pr", _fail_merge)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.loop_outcome is not None
    assert res.loop_outcome.kind.value == "merge_failed"
    assert calls["merge_pr"]["match_head_commit"] == "head-sha"

    event_log = EventLog.for_repo(fixture["cfg"].repo.path)
    events = event_log.iter_events(issue_number=3)
    assert events[-1].kind == "run-terminal"
    assert events[-1].payload["outcome"] == "merge_failed"
    assert events[-1].payload["error"] == "merge blocked"


@pytest.mark.asyncio
async def test_run_once_does_not_emit_merge_until_pr_is_merged(monkeypatch, tmp_path):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    monkeypatch.setattr(ro_mod, "is_pr_merged", lambda **kw: False)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.loop_outcome is not None
    assert res.loop_outcome.kind.value == "merge_pending"

    event_log = EventLog.for_repo(fixture["cfg"].repo.path)
    events = event_log.iter_events(issue_number=3)
    assert events[-1].kind == "run-terminal"
    assert events[-1].payload["outcome"] == "merge_pending"
    assert "merge" not in [e.kind for e in events]


@pytest.mark.asyncio
async def test_run_once_retries_prior_merge_failure_without_round1_agent(
    monkeypatch, tmp_path
):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    existing = PR(number=42, url="https://x/pr/42")
    fixture["calls"].pop("find_pr", None)
    event_log = EventLog.for_repo(fixture["cfg"].repo.path)
    event_log.emit(
        "run-terminal",
        issue_number=3,
        payload={"outcome": "merge_failed", "rounds_used": 0},
    )

    def _existing(branch, **kw):
        fixture["calls"]["find_pr"] = (branch, kw)
        return existing

    monkeypatch.setattr(ro_mod, "find_open_pr_for_branch", _existing)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.skipped is False
    assert res.pr == existing
    assert "prompt" not in fixture["calls"]["agent"]
    assert "push" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]
    assert fixture["calls"]["drive_review_loop"]["initial_session_id"] is None
    assert fixture["calls"]["merge_pr"]["pr_number"] == 42
    retry_event = event_log.iter_events(issue_number=3)[-2]
    assert retry_event.kind == "pr-open"
    assert retry_event.payload["merge_retry"] is True


@pytest.mark.asyncio
async def test_run_once_waits_for_open_merge_pending_pr_without_remerging(
    monkeypatch, tmp_path
):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    existing = PR(number=42, url="https://x/pr/42")
    fixture["calls"].pop("find_pr", None)
    event_log = EventLog.for_repo(fixture["cfg"].repo.path)
    event_log.emit(
        "run-terminal",
        issue_number=3,
        payload={
            "pr_number": 42,
            "url": "https://x/pr/42",
            "outcome": "merge_pending",
            "rounds_used": 2,
            "head_sha": "reviewed-sha",
        },
    )

    def _existing(branch, **kw):
        fixture["calls"]["find_pr"] = (branch, kw)
        return existing

    merge_checks = []

    def _not_merged(**kw):
        merge_checks.append(kw)
        return False

    monkeypatch.setattr(ro_mod, "find_open_pr_for_branch", _existing)
    monkeypatch.setattr(
        ro_mod,
        "get_pr_head_sha",
        lambda **kw: "reviewed-sha",
    )
    monkeypatch.setattr(ro_mod, "is_pr_merged", _not_merged)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.skipped is False
    assert res.pr == existing
    assert res.loop_outcome is not None
    assert res.loop_outcome.kind.value == "merge_pending"
    assert res.loop_outcome.rounds_used == 2
    assert res.loop_outcome.head_sha == "reviewed-sha"
    assert merge_checks == [{"repo_path": fixture["cfg"].repo.path, "pr_number": 42}]
    assert "prompt" not in fixture["calls"]["agent"]
    assert "push" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]
    assert "drive_review_loop" not in fixture["calls"]
    assert "merge_pr" not in fixture["calls"]
    events = event_log.iter_events(issue_number=3)
    assert events[-1].kind == "run-terminal"
    assert events[-1].payload["outcome"] == "merge_pending"
    assert events[-1].payload["merge_retry"] is True
    assert events[-1].payload["reason"] == "open PR still pending merge"


@pytest.mark.asyncio
async def test_run_once_reruns_review_when_open_merge_pending_head_changes(
    monkeypatch, tmp_path
):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    existing = PR(number=42, url="https://x/pr/42")
    fixture["calls"].pop("find_pr", None)
    order: list[str] = []
    event_log = EventLog.for_repo(fixture["cfg"].repo.path)
    event_log.emit(
        "run-terminal",
        issue_number=3,
        payload={
            "pr_number": 42,
            "url": "https://x/pr/42",
            "outcome": "merge_pending",
            "rounds_used": 2,
            "head_sha": "old-sha",
        },
    )

    def _existing(branch, **kw):
        fixture["calls"]["find_pr"] = (branch, kw)
        return existing

    monkeypatch.setattr(ro_mod, "find_open_pr_for_branch", _existing)
    monkeypatch.setattr(ro_mod, "get_pr_head_sha", lambda **kw: "new-sha")

    def _fake_sync(*args):
        order.append("sync")
        fixture["calls"]["sync_worktree"] = args

    def _fake_comment(**kw):
        order.append("comment")
        fixture["calls"]["comment_pr"] = kw

    monkeypatch.setattr(ro_mod, "_sync_worktree_to_pr_head", _fake_sync)
    monkeypatch.setattr(ro_mod, "comment_pr", _fake_comment)

    async def _fake_loop(**kw):
        order.append("review")
        fixture["calls"]["drive_review_loop"] = kw
        return ro_mod.LoopOutcome(
            kind=ro_mod.LoopOutcomeKind.APPROVED,
            rounds_used=0,
            last_session_id=kw.get("initial_session_id"),
            head_sha="new-sha",
        )

    monkeypatch.setattr(ro_mod, "drive_review_loop", _fake_loop)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.skipped is False
    assert res.pr == existing
    assert "prompt" not in fixture["calls"]["agent"]
    assert "push" not in fixture["calls"]
    assert fixture["calls"]["sync_worktree"] == (
        fixture["wt"],
        "auto/3",
        "new-sha",
    )
    assert order[:3] == ["sync", "comment", "review"]
    assert fixture["calls"]["comment_pr"] == {
        "repo_path": fixture["cfg"].repo.path,
        "pr_number": 42,
        "body": "@codex review",
    }
    assert fixture["calls"]["drive_review_loop"]["initial_session_id"] is None
    assert fixture["calls"]["merge_pr"]["pr_number"] == 42
    events = event_log.iter_events(issue_number=3)
    retry_event = events[-2]
    assert retry_event.kind == "pr-open"
    assert retry_event.payload["merge_retry"] is True
    assert retry_event.payload["head_changed"] is True
    assert retry_event.payload["previous_head_sha"] == "old-sha"
    assert retry_event.payload["head_sha"] == "new-sha"


@pytest.mark.asyncio
async def test_run_once_records_merge_when_retry_pr_closed_after_merge(
    monkeypatch, tmp_path
):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    fixture["calls"].pop("find_pr", None)
    event_log = EventLog.for_repo(fixture["cfg"].repo.path)
    event_log.emit(
        "run-terminal",
        issue_number=3,
        payload={
            "pr_number": 42,
            "url": "https://x/pr/42",
            "outcome": "merge_pending",
            "rounds_used": 2,
            "head_sha": "reviewed-sha",
        },
    )

    def _missing(branch, **kw):
        fixture["calls"]["find_pr"] = (branch, kw)
        return None

    monkeypatch.setattr(ro_mod, "find_open_pr_for_branch", _missing)
    monkeypatch.setattr(ro_mod, "is_pr_merged", lambda **kw: True)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.skipped is False
    assert res.pr == PR(number=42, url="https://x/pr/42")
    assert res.loop_outcome is not None
    assert res.loop_outcome.kind.value == "approved"
    assert "prompt" not in fixture["calls"]["agent"]
    assert "push" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]
    events = event_log.iter_events(issue_number=3)
    assert events[-1].kind == "merge"
    assert events[-1].payload["merge_retry"] is True
    assert events[-1].payload["pr_number"] == 42


@pytest.mark.asyncio
async def test_run_once_stops_when_merge_retry_pr_is_unavailable(
    monkeypatch, tmp_path
):
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    fixture["calls"].pop("find_pr", None)
    event_log = EventLog.for_repo(fixture["cfg"].repo.path)
    event_log.emit(
        "run-terminal",
        issue_number=3,
        payload={
            "pr_number": 42,
            "url": "https://x/pr/42",
            "outcome": "merge_failed",
            "rounds_used": 2,
            "head_sha": "reviewed-sha",
        },
    )

    def _missing(branch, **kw):
        fixture["calls"]["find_pr"] = (branch, kw)
        return None

    monkeypatch.setattr(ro_mod, "find_open_pr_for_branch", _missing)
    monkeypatch.setattr(ro_mod, "is_pr_merged", lambda **kw: False)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.skipped is False
    assert res.pr == PR(number=42, url="https://x/pr/42")
    assert res.loop_outcome is not None
    assert res.loop_outcome.kind.value == "merge_unavailable"
    assert "prompt" not in fixture["calls"]["agent"]
    assert "push" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]
    events = event_log.iter_events(issue_number=3)
    assert events[-1].kind == "run-terminal"
    assert events[-1].payload["outcome"] == "merge_unavailable"
    assert events[-1].payload["error"] == "no open PR found for merge retry"


@pytest.mark.asyncio
async def test_run_once_reuses_existing_pr_on_redispatch(monkeypatch, tmp_path):
    """Regression: re-dispatch with an existing open PR must not call open_pr
    (which would fail with `gh`'s duplicate-PR error and abort before the
    @codex review nudge gets posted on the new commit).
    """
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    existing = PR(number=42, url="https://x/pr/42")
    fixture["calls"].pop("find_pr", None)

    def _existing(branch, **kw):
        fixture["calls"]["find_pr"] = (branch, kw)
        return existing

    monkeypatch.setattr(ro_mod, "find_open_pr_for_branch", _existing)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])
    assert res.skipped is False
    assert res.pr == existing
    # open_pr must NOT have been called when an existing PR is found.
    assert "open_pr" not in fixture["calls"]
    # but @codex review still goes out, on the existing PR
    assert fixture["calls"]["comment_pr"]["pr_number"] == 42


@pytest.mark.asyncio
async def test_run_once_skips_push_when_truly_empty(monkeypatch, tmp_path):
    """Empty-diff means agent didn't move HEAD AND the branch has nothing
    over origin/<base>. Both conditions are required — a stranded local
    commit alone must NOT trigger this skip (covered by the regression below).
    """
    fixture = _patch_happy_path(
        monkeypatch,
        tmp_path,
        head_before="same000",
        head_after="same000",
        to_push=0,
    )
    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])
    assert res.skipped is True
    assert res.skip_reason == "empty-diff"
    assert res.pr is None
    assert "push" not in fixture["calls"]
    assert "open_pr" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]


@pytest.mark.asyncio
async def test_run_once_skips_when_origin_already_at_head(monkeypatch, tmp_path):
    """Regression for the previously-pushed re-dispatch case: a branch that
    was already pushed and the agent makes no further commits. ``HEAD`` is
    still ahead of ``origin/<base>`` (it has the prior feature commit), but
    ``HEAD == origin/<branch>`` so there's nothing new to push. When no PR is
    open, the run must skip — otherwise we'd post a redundant @codex review for
    the same SHA.

    The fixture sets ``to_push=0`` (the helper compares against
    ``origin/<branch>`` when that ref exists, so an in-sync branch is 0).
    """
    fixture = _patch_happy_path(
        monkeypatch,
        tmp_path,
        head_before="abc1234",
        head_after="abc1234",
        to_push=0,
    )
    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])
    assert res.skipped is True
    assert res.skip_reason == "empty-diff"
    assert "push" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]


@pytest.mark.asyncio
async def test_run_once_reviews_existing_pr_on_empty_redispatch(monkeypatch, tmp_path):
    """A no-op redispatch can still need to observe/merge the open PR.

    Review-loop outcomes like idle/stuck are retried by the orchestrator, and
    Codex or CI may approve the existing head before the retry runs. In that
    case run_once must evaluate the open PR instead of returning empty-diff.
    """
    fixture = _patch_happy_path(
        monkeypatch,
        tmp_path,
        head_before="abc1234",
        head_after="abc1234",
        to_push=0,
    )
    existing = PR(number=42, url="https://x/pr/42")
    fixture["calls"].pop("find_pr", None)

    def _existing(branch, **kw):
        fixture["calls"]["find_pr"] = (branch, kw)
        return existing

    monkeypatch.setattr(ro_mod, "find_open_pr_for_branch", _existing)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])
    assert res.skipped is False
    assert res.pr == existing
    assert res.loop_outcome is not None
    assert fixture["calls"]["drive_review_loop"]["pr_number"] == 42
    assert (
        fixture["calls"]["drive_review_loop"]["poll_interval_s"]
        == fixture["cfg"].orchestrator.poll_interval_s
    )
    assert "push" not in fixture["calls"]
    assert "open_pr" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]


@pytest.mark.asyncio
async def test_run_once_pushes_stranded_commits_on_noop_agent(monkeypatch, tmp_path):
    """Regression: a prior run that crashed after commit but before push
    leaves local commits ahead of origin. A subsequent rerun where the agent
    legitimately exits without further changes must still push the stranded
    commits and open/refresh the PR — otherwise the work is silently abandoned.
    """
    fixture = _patch_happy_path(
        monkeypatch,
        tmp_path,
        head_before="stranded123",
        head_after="stranded123",  # agent didn't advance HEAD
        to_push=1,  # but there is unpushed work
    )
    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])
    assert res.skipped is False
    assert res.pr is not None
    assert "push" in fixture["calls"]
    assert "comment_pr" in fixture["calls"]


@pytest.mark.asyncio
async def test_run_once_skips_when_issue_already_closed(monkeypatch, tmp_path):
    """Closes the dispatch race where `gh issue list --state open` returns an
    issue whose Closes-link merge has not yet propagated. run_once must
    re-check state and bail before any worktree/agent/PR work happens.
    """
    fixture = _patch_happy_path(monkeypatch, tmp_path)
    fixture["calls"].pop("issue_state", None)

    def _closed(n, *, repo_path):
        fixture["calls"]["issue_state"] = (n, repo_path)
        return "CLOSED"

    monkeypatch.setattr(ro_mod, "get_issue_state", _closed)

    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])

    assert res.skipped is True
    assert res.skip_reason == "issue-closed"
    assert res.pr is None
    assert res.worktree is None
    assert res.loop_outcome is not None
    assert res.loop_outcome.rounds_used == 0
    # Nothing past the closed-issue guard ran: no worktree, no agent, no
    # push, no PR, no comment.
    assert "view" not in fixture["calls"]
    assert "worktree" not in fixture["calls"]
    assert "prompt" not in fixture["calls"]["agent"]
    assert "push" not in fixture["calls"]
    assert "open_pr" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]

    event_log = EventLog.for_repo(fixture["cfg"].repo.path)
    events = event_log.iter_events(issue_number=3)
    assert [e.kind for e in events] == ["run_once.skipped"]
    assert events[0].payload["reason"] == "issue-closed"


@pytest.mark.asyncio
async def test_run_once_skips_push_on_agent_failure(monkeypatch, tmp_path):
    fixture = _patch_happy_path(
        monkeypatch, tmp_path, agent_result=_failed_agent_result()
    )
    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])
    assert res.skipped is True
    assert res.skip_reason == "agent-failed"
    assert res.pr is None
    # Crucially: no push, no PR, no comment.
    assert "push" not in fixture["calls"]
    assert "open_pr" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]
