"""Tests for the pure verdict evaluator and review-loop driver."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from symphony import reviewer as reviewer_mod
from symphony.github import CheckRun, Reaction, Review, ReviewComment
from symphony.reviewer import (
    CODEX_BOT_LOGIN,
    LoopOutcomeKind,
    ReviewSnapshot,
    VerdictKind,
    drive_review_loop,
    evaluate_verdict,
    fetch_snapshot,
    select_resume_session,
)
from symphony.types import AgentResult


HEAD = "head000sha"
HEAD_AT = "2026-05-06T07:00:00Z"


def _review(*, who, state, body="", sha=HEAD, at="2026-05-06T07:30:00Z", id=1) -> Review:
    return Review(
        id=id,
        user_login=who,
        state=state,
        body=body,
        commit_sha=sha,
        submitted_at=at,
    )


def _comment(
    *,
    who=CODEX_BOT_LOGIN,
    sha=HEAD,
    body="fix this",
    line=42,
    id=1,
    at="2026-05-06T07:30:00Z",
    review_id=0,
) -> ReviewComment:
    return ReviewComment(
        id=id,
        user_login=who,
        path="src/x.py",
        line=line,
        body=body,
        commit_sha=sha,
        created_at=at,
        review_id=review_id,
    )


def _reaction(*, who, content="+1", at) -> Reaction:
    return Reaction(user_login=who, content=content, created_at=at)


def _check(*, name="ci", conclusion=None, status="completed", source="check_run") -> CheckRun:
    return CheckRun(
        name=name,
        status=status,
        conclusion=conclusion,
        details_url=None,
        source=source,
    )


def _eval(**overrides):
    base = dict(
        head_sha=HEAD,
        head_committed_at=HEAD_AT,
        reviews=[],
        review_comments=[],
        reactions=[],
        checks=[],
    )
    base.update(overrides)
    return evaluate_verdict(**base)


# ---- pending baseline ----


def test_no_signals_is_pending():
    v = _eval()
    assert v.kind == VerdictKind.PENDING
    assert v.review_comments == []


def test_only_eyes_reaction_is_pending():
    v = _eval(reactions=[_reaction(who=CODEX_BOT_LOGIN, content="eyes", at="2026-05-06T07:30:00Z")])
    assert v.kind == VerdictKind.PENDING


def test_stale_review_on_old_sha_ignored():
    v = _eval(reviews=[_review(who=CODEX_BOT_LOGIN, state="COMMENTED", body="x" * 5000, sha="oldsha")])
    assert v.kind == VerdictKind.PENDING


def test_stale_codex_comment_on_old_sha_ignored():
    v = _eval(review_comments=[_comment(sha="oldsha")])
    assert v.kind == VerdictKind.PENDING


# ---- changes requested ----


def test_codex_inline_comment_on_head_is_changes_requested():
    c = _comment()
    v = _eval(review_comments=[c])
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.review_comments == [c]


def test_only_latest_codex_review_comments_block():
    old = _comment(at="2026-05-06T07:10:00Z", review_id=1)
    v = _eval(
        reviews=[
            _review(who=CODEX_BOT_LOGIN, state="COMMENTED", id=1, at="2026-05-06T07:10:00Z"),
            _review(who=CODEX_BOT_LOGIN, state="COMMENTED", id=2, at="2026-05-06T07:30:00Z"),
        ],
        review_comments=[old],
    )
    assert v.kind == VerdictKind.PENDING


def test_codex_substantive_review_body_without_inline_is_changes_requested():
    big_body = "x" * 800  # > 750 boilerplate threshold
    v = _eval(reviews=[_review(who=CODEX_BOT_LOGIN, state="COMMENTED", body=big_body)])
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.last_review_body == big_body


def test_codex_boilerplate_review_alone_stays_pending():
    """Regression: bodies near the boilerplate length must NOT be classified
    as substantive feedback. Observed Codex boilerplate length is ~621 chars.
    """
    boilerplate = "x" * 621
    v = _eval(reviews=[_review(who=CODEX_BOT_LOGIN, state="COMMENTED", body=boilerplate)])
    assert v.kind == VerdictKind.PENDING


def test_human_changes_requested_review_wins():
    v = _eval(reviews=[_review(who="alice", state="CHANGES_REQUESTED", body="needs work")])
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.last_review_body == "needs work"


def test_latest_human_review_state_for_reviewer_wins():
    v = _eval(
        reviews=[
            _review(who="alice", state="CHANGES_REQUESTED", body="old", id=1),
            _review(who="alice", state="APPROVED", body="fixed", id=2),
        ]
    )
    assert v.kind == VerdictKind.APPROVED


def test_latest_human_changes_requested_still_blocks():
    v = _eval(
        reviews=[
            _review(who="alice", state="APPROVED", body="old", id=1),
            _review(who="alice", state="CHANGES_REQUESTED", body="new", id=2),
        ]
    )
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.last_review_body == "new"


def test_latest_changes_requested_from_any_human_blocks_approval():
    v = _eval(
        reviews=[
            _review(who="alice", state="APPROVED", body="lgtm", id=1),
            _review(who="bob", state="CHANGES_REQUESTED", body="blocked", id=2),
        ]
    )
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.last_review_body == "blocked"


def test_failing_ci_check_is_changes_requested():
    v = _eval(checks=[_check(name="test", conclusion="failure")])
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.ci_failures[0].name == "test"


def test_cancelled_ci_check_is_changes_requested():
    v = _eval(checks=[_check(name="test", conclusion="cancelled")])
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.ci_failures[0].name == "test"


@pytest.mark.parametrize("conclusion", ["startup_failure", "stale"])
def test_other_failed_ci_conclusions_are_changes_requested(conclusion):
    v = _eval(checks=[_check(name="test", conclusion=conclusion)])
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.ci_failures[0].name == "test"


def test_in_progress_check_is_not_failure():
    v = _eval(checks=[_check(name="test", status="in_progress", conclusion=None)])
    assert v.kind == VerdictKind.PENDING
    assert v.pending_checks[0].name == "test"


def test_in_progress_check_blocks_codex_approval_reaction():
    v = _eval(
        checks=[_check(name="test", status="in_progress", conclusion=None)],
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T07:30:00Z")],
    )
    assert v.kind == VerdictKind.PENDING


def test_in_progress_check_blocks_human_approval():
    v = _eval(
        checks=[_check(name="test", status="in_progress", conclusion=None)],
        reviews=[_review(who="alice", state="APPROVED", body="lgtm")],
    )
    assert v.kind == VerdictKind.PENDING


def test_failing_ci_takes_priority_over_codex_approval_reaction():
    v = _eval(
        checks=[_check(name="test", conclusion="failure")],
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T07:30:00Z")],
    )
    assert v.kind == VerdictKind.CHANGES_REQUESTED


def test_latest_check_by_name_overrides_stale_failure():
    v = _eval(
        checks=[
            _check(name="test", conclusion="success"),
            _check(name="test", conclusion="failure"),
        ],
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T07:30:00Z")],
    )
    assert v.kind == VerdictKind.APPROVED


def test_check_run_and_status_with_same_name_are_separate_signals():
    v = _eval(
        checks=[
            _check(name="ci", conclusion="success", source="check_run"),
            _check(name="ci", conclusion="failure", source="status"),
        ],
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T07:30:00Z")],
    )
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.ci_failures[0].source == "status"


def test_latest_pending_check_by_name_overrides_stale_success():
    v = _eval(
        checks=[
            _check(name="test", status="in_progress", conclusion=None),
            _check(name="test", conclusion="success"),
        ],
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T07:30:00Z")],
    )
    assert v.kind == VerdictKind.PENDING


# ---- approved ----


def test_codex_plus_one_reaction_on_head_is_approved():
    v = _eval(
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T07:30:00Z")]
    )
    assert v.kind == VerdictKind.APPROVED


def test_codex_plus_one_at_head_commit_second_is_approved():
    v = _eval(
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at=HEAD_AT)]
    )
    assert v.kind == VerdictKind.APPROVED


def test_codex_plus_one_before_head_commit_is_ignored():
    """Reaction predates HEAD — refers to an earlier commit, must not approve."""
    v = _eval(
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T06:00:00Z")]
    )
    assert v.kind == VerdictKind.PENDING


def test_codex_plus_one_with_fresh_changes_requested_is_changes_requested():
    """A Codex inline comment on HEAD outranks a stale-or-fresh +1."""
    v = _eval(
        review_comments=[_comment(at="2026-05-06T07:31:00Z")],
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T07:30:00Z")],
    )
    assert v.kind == VerdictKind.CHANGES_REQUESTED


def test_codex_plus_one_after_inline_comment_supersedes_comment():
    v = _eval(
        review_comments=[_comment(at="2026-05-06T07:10:00Z")],
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T07:30:00Z")],
    )
    assert v.kind == VerdictKind.APPROVED


def test_codex_plus_one_after_substantive_review_supersedes_review():
    v = _eval(
        reviews=[
            _review(
                who=CODEX_BOT_LOGIN,
                state="COMMENTED",
                body="x" * 800,
                at="2026-05-06T07:10:00Z",
            )
        ],
        reactions=[_reaction(who=CODEX_BOT_LOGIN, at="2026-05-06T07:30:00Z")],
    )
    assert v.kind == VerdictKind.APPROVED


def test_human_approval_does_not_override_codex_inline_comment():
    c = _comment()
    v = _eval(
        reviews=[_review(who="alice", state="APPROVED", body="lgtm")],
        review_comments=[c],
    )
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.review_comments == [c]


def test_human_approved_review_wins():
    v = _eval(reviews=[_review(who="alice", state="APPROVED", body="lgtm")])
    assert v.kind == VerdictKind.APPROVED


def test_non_plus_one_reaction_does_not_approve():
    v = _eval(reactions=[_reaction(who=CODEX_BOT_LOGIN, content="heart", at="2026-05-06T07:30:00Z")])
    assert v.kind == VerdictKind.PENDING


def test_plus_one_from_random_user_does_not_approve():
    v = _eval(reactions=[_reaction(who="random", at="2026-05-06T07:30:00Z")])
    assert v.kind == VerdictKind.PENDING


# ---- select_resume_session ----


def test_resume_for_first_three_rounds():
    assert select_resume_session(0, "sess-A") == "sess-A"
    assert select_resume_session(1, "sess-A") == "sess-A"
    assert select_resume_session(2, "sess-A") == "sess-A"


def test_no_resume_from_round_three_onwards():
    """Rounds 4-10 (0-indexed 3-9) start fresh so a wrong-headed early
    context doesn't entrench the agent."""
    assert select_resume_session(3, "sess-A") is None
    assert select_resume_session(9, "sess-A") is None


def test_no_resume_when_no_session_id():
    assert select_resume_session(0, None) is None


# ---- drive_review_loop ----


def _snap(*, head_sha="head1", reviews=(), comments=(), reactions=(), checks=()) -> ReviewSnapshot:
    return ReviewSnapshot(
        head_sha=head_sha,
        head_committed_at="2026-05-06T07:00:00Z",
        reviews=list(reviews),
        review_comments=list(comments),
        reactions=list(reactions),
        checks=list(checks),
    )


def _approved_snap(head_sha="head1") -> ReviewSnapshot:
    return _snap(
        head_sha=head_sha,
        reactions=[Reaction(user_login=CODEX_BOT_LOGIN, content="+1", created_at="2026-05-06T07:30:00Z")],
    )


def _changes_snap(head_sha="head1", body="please fix") -> ReviewSnapshot:
    return _snap(
        head_sha=head_sha,
        comments=[
            ReviewComment(
                id=1,
                user_login=CODEX_BOT_LOGIN,
                path="x.py",
                line=1,
                body=body,
                commit_sha=head_sha,
                created_at="2026-05-06T07:30:00Z",
            )
        ],
    )


def _pending_snap(head_sha="head1") -> ReviewSnapshot:
    return _snap(head_sha=head_sha)


def test_fetch_snapshot_uses_pinned_head_for_checks(monkeypatch, tmp_path):
    calls: dict[str, str | None] = {}
    monkeypatch.setattr(
        reviewer_mod,
        "get_pr_head_sha",
        lambda pr_number, repo_path: "shaH",
    )
    monkeypatch.setattr(
        reviewer_mod,
        "get_commit_committed_at",
        lambda sha, repo_path: "2026-05-06T07:00:00Z",
    )
    monkeypatch.setattr(reviewer_mod, "list_pr_reviews", lambda pr_number, repo_path: [])
    monkeypatch.setattr(
        reviewer_mod, "list_pr_review_comments", lambda pr_number, repo_path: []
    )
    monkeypatch.setattr(reviewer_mod, "list_pr_reactions", lambda pr_number, repo_path: [])

    def fake_checks(pr_number, *, repo_path, head_sha=None):
        calls["head_sha"] = head_sha
        return []

    monkeypatch.setattr(reviewer_mod, "list_pr_checks", fake_checks)

    snap = fetch_snapshot(pr_number=10, repo_path=tmp_path)

    assert snap.head_sha == "shaH"
    assert calls["head_sha"] == "shaH"


def _make_cfg(tmp_path: Path):
    return SimpleNamespace(
        repo=SimpleNamespace(path=tmp_path / "repo", default_branch="main"),
        agent=SimpleNamespace(model="claude-opus-4-7", max_turns=50),
        paths=SimpleNamespace(prompts_dir=Path(__file__).parent.parent / "prompts"),
    )


def _ok_agent_result(session_id="sess-A") -> AgentResult:
    return AgentResult(
        session_id=session_id,
        exit_code=0,
        success=True,
        is_error=False,
        duration_ms=10,
        num_turns=1,
        total_cost_usd=0.0,
        final_text="done",
        raw_events=[],
        stderr="",
    )


def _failed_agent_result() -> AgentResult:
    return AgentResult(
        session_id="sess-A",
        exit_code=1,
        success=False,
        is_error=True,
        duration_ms=10,
        num_turns=1,
        total_cost_usd=0.0,
        final_text=None,
        raw_events=[],
        stderr="boom",
    )


class _Driver:
    """Helpers for driving the loop in tests."""

    def __init__(self, snapshots, *, agent_results=None):
        self._snaps = list(snapshots)
        self._agents = list(agent_results or [_ok_agent_result()] * 50)
        self.calls: dict[str, list] = {
            "push": [], "comment_pr": [], "label_issue": [],
            "agent_resume": [], "render": [], "snapshot": [],
            "head_sha": [], "to_push": [], "merge_pr": [],
        }
        self.now = 0.0
        self._head_sha = "head1"

    def snapshot(self, *, pr_number, repo_path):
        self.calls["snapshot"].append((pr_number, str(repo_path)))
        return self._snaps.pop(0) if self._snaps else _pending_snap()

    async def run_agent(self, prompt, workdir, **kwargs):
        self.calls["agent_resume"].append(kwargs.get("resume_session"))
        # advance HEAD on successful agent runs to mimic a commit
        result = self._agents.pop(0)
        if result.success:
            self._head_sha = f"head{len(self.calls['agent_resume']) + 1}"
        return result

    def head_sha(self, worktree):
        self.calls["head_sha"].append(str(worktree))
        return self._head_sha

    def commits_to_push(self, worktree, branch, base):
        self.calls["to_push"].append((str(worktree), branch, base))
        return 1  # always indicate a commit ready to push after a successful agent run

    def push(self, worktree, branch):
        self.calls["push"].append((str(worktree), branch))

    def comment_pr(self, *, repo_path, pr_number, body):
        self.calls["comment_pr"].append((str(repo_path), pr_number, body))

    def label_issue(self, number, label, *, repo_path):
        self.calls["label_issue"].append((number, label, str(repo_path)))

    def merge_pr(self, *, repo_path, pr_number, match_head_commit=None):
        self.calls["merge_pr"].append((str(repo_path), pr_number, match_head_commit))

    def render(self, *, cfg, sha, comments, ci_failures, review_body=""):
        self.calls["render"].append(
            {
                "sha": sha,
                "n_comments": len(comments),
                "n_ci": len(ci_failures),
                "review_body": review_body,
            }
        )
        return f"render({sha})"

    async def sleep(self, _seconds):
        # tests don't really sleep; bump the virtual clock instead
        self.now += _seconds

    def time_now(self):
        return self.now


def _spawn_loop(driver, cfg, **overrides):
    kwargs = dict(
        cfg=cfg,
        issue_number=4,
        pr_number=11,
        branch="auto/4",
        worktree=cfg.repo.path / "wt",
        initial_session_id="sess-A",
        poll_interval_s=30.0,
        re_nudge_after_s=600.0,
        give_up_after_s=1800.0,
        round_cap=10,
        snapshot_fn=driver.snapshot,
        run_agent_fn=driver.run_agent,
        render_review_prompt=driver.render,
        push_fn=driver.push,
        head_sha_fn=driver.head_sha,
        commits_to_push_fn=driver.commits_to_push,
        comment_pr_fn=driver.comment_pr,
        label_issue_fn=driver.label_issue,
        merge_pr_fn=driver.merge_pr,
        sleep_fn=driver.sleep,
        now_fn=driver.time_now,
    )
    kwargs.update(overrides)
    return drive_review_loop(**kwargs)


@pytest.mark.asyncio
async def test_loop_returns_on_first_approved(tmp_path):
    cfg = _make_cfg(tmp_path)
    driver = _Driver([_approved_snap()])
    outcome = await _spawn_loop(driver, cfg)
    assert outcome.kind == LoopOutcomeKind.APPROVED
    assert outcome.rounds_used == 0
    assert driver.calls["merge_pr"] == [(str(cfg.repo.path), 11, "head1")]
    # No agent run, no push, no extra comment posted on the immediate-approve path.
    assert driver.calls["agent_resume"] == []
    assert driver.calls["push"] == []
    assert driver.calls["label_issue"] == []


@pytest.mark.asyncio
async def test_loop_retries_after_merge_failure(tmp_path):
    cfg = _make_cfg(tmp_path)
    driver = _Driver([_approved_snap(), _approved_snap()])
    merge_attempts = []

    def flaky_merge(*, repo_path, pr_number, match_head_commit=None):
        merge_attempts.append((str(repo_path), pr_number, match_head_commit))
        if len(merge_attempts) == 1:
            raise RuntimeError("head changed")

    outcome = await _spawn_loop(driver, cfg, merge_pr_fn=flaky_merge)

    assert outcome.kind == LoopOutcomeKind.APPROVED
    assert merge_attempts == [
        (str(cfg.repo.path), 11, "head1"),
        (str(cfg.repo.path), 11, "head1"),
    ]
    assert driver.calls["label_issue"] == []


@pytest.mark.asyncio
async def test_loop_auto_stuck_when_approved_merge_keeps_failing(tmp_path):
    cfg = _make_cfg(tmp_path)
    driver = _Driver([_approved_snap()] * 10)

    def failing_merge(*, repo_path, pr_number, match_head_commit=None):
        raise RuntimeError("merge queue unavailable")

    outcome = await _spawn_loop(
        driver,
        cfg,
        merge_pr_fn=failing_merge,
        poll_interval_s=30.0,
        give_up_after_s=60.0,
    )

    assert outcome.kind == LoopOutcomeKind.AUTO_STUCK_IDLE
    assert driver.calls["label_issue"] == [(4, "auto-stuck", str(cfg.repo.path))]


@pytest.mark.asyncio
async def test_loop_handles_changes_then_approval(tmp_path):
    cfg = _make_cfg(tmp_path)
    driver = _Driver([_changes_snap("head1"), _approved_snap("head2")])
    outcome = await _spawn_loop(driver, cfg)
    assert outcome.kind == LoopOutcomeKind.APPROVED
    assert outcome.rounds_used == 1
    assert driver.calls["merge_pr"] == [(str(cfg.repo.path), 11, "head2")]
    # Agent re-invoked once, with resume (round 0), pushed and re-nudged.
    assert driver.calls["agent_resume"] == ["sess-A"]
    assert len(driver.calls["push"]) == 1
    assert any("@codex review" == body for _, _, body in driver.calls["comment_pr"])


@pytest.mark.asyncio
async def test_loop_passes_summary_review_body_to_retry_prompt(tmp_path):
    cfg = _make_cfg(tmp_path)
    summary = "please handle the summary-only request"
    driver = _Driver(
        [
            _snap(
                head_sha="head1",
                reviews=[
                    _review(
                        who="alice",
                        state="CHANGES_REQUESTED",
                        body=summary,
                        sha="head1",
                    )
                ],
            ),
            _approved_snap("head2"),
        ]
    )

    outcome = await _spawn_loop(driver, cfg)

    assert outcome.kind == LoopOutcomeKind.APPROVED
    assert driver.calls["render"][0]["review_body"] == summary
    assert driver.calls["render"][0]["n_comments"] == 0


@pytest.mark.asyncio
async def test_loop_resumes_first_three_rounds_then_fresh(tmp_path):
    """Rounds 0-2 resume the original session; rounds 3-9 run fresh."""
    cfg = _make_cfg(tmp_path)
    # 5 changes-requested snapshots in a row, then approve.
    snaps = [_changes_snap(f"head{i}") for i in range(1, 6)] + [_approved_snap("head6")]
    driver = _Driver(snaps)
    outcome = await _spawn_loop(driver, cfg)
    assert outcome.kind == LoopOutcomeKind.APPROVED
    assert outcome.rounds_used == 5
    assert driver.calls["agent_resume"] == [
        "sess-A",  # round 0 → resume
        "sess-A",  # round 1 → resume
        "sess-A",  # round 2 → resume
        None,      # round 3 → fresh
        None,      # round 4 → fresh
    ]


@pytest.mark.asyncio
async def test_loop_auto_stuck_after_round_cap(tmp_path):
    cfg = _make_cfg(tmp_path)
    snaps = [_changes_snap(f"head{i}") for i in range(1, 20)]  # plenty
    driver = _Driver(snaps)
    outcome = await _spawn_loop(driver, cfg, round_cap=10)
    assert outcome.kind == LoopOutcomeKind.AUTO_STUCK_ROUNDS
    assert outcome.rounds_used == 10
    assert driver.calls["merge_pr"] == []
    assert driver.calls["label_issue"] == [(4, "auto-stuck", str(cfg.repo.path))]


@pytest.mark.asyncio
async def test_loop_auto_stuck_after_idle_giveup(tmp_path):
    """30+ minutes of PENDING without activity → auto-stuck."""
    cfg = _make_cfg(tmp_path)
    # Many PENDING snapshots; loop ticks at 30s each, 1800s = 60 ticks.
    driver = _Driver([_pending_snap()] * 200)
    outcome = await _spawn_loop(
        driver, cfg, poll_interval_s=30.0, re_nudge_after_s=600.0, give_up_after_s=1800.0
    )
    assert outcome.kind == LoopOutcomeKind.AUTO_STUCK_IDLE
    assert outcome.rounds_used == 0
    assert driver.calls["label_issue"] == [(4, "auto-stuck", str(cfg.repo.path))]


@pytest.mark.asyncio
async def test_loop_does_not_idle_give_up_while_checks_are_pending(tmp_path):
    cfg = _make_cfg(tmp_path)
    pending = _snap(checks=[_check(name="ci", status="in_progress", conclusion=None)])
    snaps = [pending] * 20 + [_approved_snap()]
    driver = _Driver(snaps)
    outcome = await _spawn_loop(
        driver,
        cfg,
        poll_interval_s=30.0,
        re_nudge_after_s=60.0,
        give_up_after_s=90.0,
    )
    assert outcome.kind == LoopOutcomeKind.APPROVED
    assert driver.calls["label_issue"] == []


@pytest.mark.asyncio
async def test_loop_resets_idle_timer_when_head_advances_while_pending(tmp_path):
    cfg = _make_cfg(tmp_path)
    snaps = [
        _pending_snap("head1"),
        _pending_snap("head1"),
        _pending_snap("head2"),
        _pending_snap("head2"),
        _approved_snap("head2"),
    ]
    driver = _Driver(snaps)
    outcome = await _spawn_loop(
        driver,
        cfg,
        poll_interval_s=30.0,
        re_nudge_after_s=600.0,
        give_up_after_s=90.0,
    )

    assert outcome.kind == LoopOutcomeKind.APPROVED
    assert driver.calls["label_issue"] == []
    assert driver.calls["merge_pr"] == [(str(cfg.repo.path), 11, "head2")]


@pytest.mark.asyncio
async def test_loop_renudges_at_idle_threshold_then_returns_on_approval(tmp_path):
    """At 10 min PENDING, post one re-nudge; then approval lands."""
    cfg = _make_cfg(tmp_path)
    # 25 pending snapshots (12.5 min) followed by approval.
    snaps = [_pending_snap()] * 25 + [_approved_snap()]
    driver = _Driver(snaps)
    outcome = await _spawn_loop(driver, cfg)
    assert outcome.kind == LoopOutcomeKind.APPROVED
    # Exactly one re-nudge posted (no double-nudges within the same idle window).
    nudges = [c for c in driver.calls["comment_pr"] if c[2] == "@codex review"]
    assert len(nudges) == 1


@pytest.mark.asyncio
async def test_loop_returns_agent_failed_on_subprocess_failure(tmp_path):
    cfg = _make_cfg(tmp_path)
    failed = _failed_agent_result()
    driver = _Driver([_changes_snap()], agent_results=[failed])
    outcome = await _spawn_loop(driver, cfg)
    assert outcome.kind == LoopOutcomeKind.AGENT_FAILED
    assert outcome.rounds_used == 0
    assert outcome.agent_result is failed
    # No push or comment after a failed agent run.
    assert driver.calls["push"] == []
    assert driver.calls["comment_pr"] == []
    assert driver.calls["merge_pr"] == []
