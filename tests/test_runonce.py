"""Tests for symphony.runonce orchestration.

Covers the M2 happy path and the empty-diff / failed-exit guards. We patch the
network-touching helpers (`view_issue`, `tracked_issues`, `name_with_owner`,
`open_pr`, `comment_pr`, `arm_auto_merge`), the worktree helper
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
from symphony.github import Issue, IssueComment, TrackedIssue, PR
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
        ro_mod, "_git_push", lambda worktree, branch: calls.setdefault("push", (worktree, branch))
    )

    pr = PR(number=99, url="https://github.com/kulichevskiy/symphony/pull/99")

    def _fake_open_pr(**kw):
        calls["open_pr"] = kw
        return pr

    monkeypatch.setattr(ro_mod, "open_pr", _fake_open_pr)
    monkeypatch.setattr(
        ro_mod, "comment_pr", lambda **kw: calls.setdefault("comment_pr", kw)
    )
    monkeypatch.setattr(
        ro_mod, "arm_auto_merge", lambda **kw: calls.setdefault("arm_auto_merge", kw)
    )

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

    # @codex review nudge posted
    assert calls["comment_pr"]["pr_number"] == 99
    assert calls["comment_pr"]["body"] == "@codex review"

    # Auto-merge armed (squash + delete-branch are the wrapper defaults)
    assert calls["arm_auto_merge"]["pr_number"] == 99


@pytest.mark.asyncio
async def test_run_once_skips_push_when_head_did_not_advance(monkeypatch, tmp_path):
    """Regression: re-dispatch into a worktree already ahead of origin/<base>.

    Previously the empty-diff guard used ``git rev-list --count`` which is
    cumulative — a reused worktree with prior commits would push/open a PR
    even when the agent did nothing this run. Comparing pre/post HEAD catches
    the true no-op case.
    """
    fixture = _patch_happy_path(
        monkeypatch, tmp_path, head_before="same000", head_after="same000"
    )
    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])
    assert res.skipped is True
    assert res.skip_reason == "empty-diff"
    assert res.pr is None
    assert "push" not in fixture["calls"]
    assert "open_pr" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]
    assert "arm_auto_merge" not in fixture["calls"]


@pytest.mark.asyncio
async def test_run_once_skips_push_on_agent_failure(monkeypatch, tmp_path):
    fixture = _patch_happy_path(
        monkeypatch, tmp_path, agent_result=_failed_agent_result()
    )
    res = await ro_mod.run_once(issue_number=3, config_path=fixture["config_path"])
    assert res.skipped is True
    assert res.skip_reason == "agent-failed"
    assert res.pr is None
    # Crucially: no push, no PR, no comment, no merge arm.
    assert "push" not in fixture["calls"]
    assert "open_pr" not in fixture["calls"]
    assert "comment_pr" not in fixture["calls"]
    assert "arm_auto_merge" not in fixture["calls"]
