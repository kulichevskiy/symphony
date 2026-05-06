"""Review-loop verdict parsing and polling.

The verdict logic is split into a *pure* function — :func:`evaluate_verdict` —
that takes fetched GitHub data and returns a structured outcome, and an *async
driver* — :func:`poll_until_terminal` — that fetches that data on a cadence
until a terminal verdict (approved, stuck, or give-up) lands.

Pure verdict logic is testable without touching the network, the clock, or
``gh``. The driver wires it up.

Verdict mapping per SYMPHONY.md M0 spike findings:

- **APPROVED** = a fresh ``+1`` reaction by ``chatgpt-codex-connector[bot]`` on
  the PR with ``created_at`` after the HEAD commit's committer date, AND no
  fresh ``CHANGES_REQUESTED`` signal on HEAD. A non-Codex reviewer's
  ``APPROVED`` review on HEAD also counts.
- **CHANGES_REQUESTED** = (a) any failing CI check on HEAD, (b) any inline
  Codex review comment on HEAD, (c) a Codex ``COMMENTED`` review on HEAD whose
  body is substantively longer than the standard "About Codex in GitHub"
  boilerplate, or (d) a non-Codex ``CHANGES_REQUESTED`` review on HEAD.
- **PENDING** = none of the above.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

from .github import (
    CheckRun,
    Reaction,
    Review,
    ReviewComment,
    get_commit_committed_at,
    get_pr_head_sha,
    list_pr_checks,
    list_pr_reactions,
    list_pr_review_comments,
    list_pr_reviews,
)

log = logging.getLogger(__name__)

CODEX_BOT_LOGIN = "chatgpt-codex-connector[bot]"

# Codex's review body always contains an "About Codex in GitHub" details
# block. The empty-suggestions-only body is ~621 chars in observed traffic
# (see docs/spike-evidence/codex-review-sample.json). Pick a threshold above
# that with a comfortable margin so a body of "boilerplate + tiny addendum"
# isn't mistaken for substantive feedback.
CODEX_BOILERPLATE_THRESHOLD = 750


class VerdictKind(StrEnum):
    APPROVED = "approved"
    CHANGES_REQUESTED = "changes_requested"
    PENDING = "pending"


@dataclass(frozen=True)
class Verdict:
    """Result of evaluating the review state at a given moment.

    Carries the inline review comments and CI failures that drove a
    ``CHANGES_REQUESTED`` so callers can render them into the next review
    prompt without re-fetching.
    """

    kind: VerdictKind
    review_comments: list[ReviewComment] = field(default_factory=list)
    ci_failures: list[CheckRun] = field(default_factory=list)
    last_review_body: str = ""


def _parse_iso(ts: str) -> datetime | None:
    if not ts:
        return None
    # GitHub returns "...Z"; Python's fromisoformat takes "+00:00" before 3.11
    # and "Z" since 3.11. We're on >= 3.11 per pyproject, so this is fine.
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def evaluate_verdict(
    *,
    head_sha: str,
    head_committed_at: str,
    reviews: list[Review],
    review_comments: list[ReviewComment],
    reactions: list[Reaction],
    checks: list[CheckRun],
    codex_login: str = CODEX_BOT_LOGIN,
) -> Verdict:
    """Pure verdict evaluation. See module docstring for the rules."""
    fresh_reviews = [r for r in reviews if r.commit_sha == head_sha]
    fresh_comments = [c for c in review_comments if c.commit_sha == head_sha]

    # 1. CI failures take priority — they're concrete, fast feedback that
    #    Codex review can't override.
    failing_checks = [
        c for c in checks if c.status == "completed" and c.conclusion == "failure"
    ]
    if failing_checks:
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            review_comments=[c for c in fresh_comments if c.user_login == codex_login],
            ci_failures=failing_checks,
        )

    # 2. Explicit human verdicts on HEAD trump everything else.
    for r in fresh_reviews:
        if r.user_login != codex_login:
            if r.state == "CHANGES_REQUESTED":
                return Verdict(
                    kind=VerdictKind.CHANGES_REQUESTED,
                    review_comments=fresh_comments,
                    last_review_body=r.body,
                )
            if r.state == "APPROVED":
                return Verdict(kind=VerdictKind.APPROVED)

    # 3. Codex review-comments on HEAD = changes requested. Boilerplate-body
    #    reviews always come paired with inline comments when there's
    #    feedback, so this is the most reliable single signal.
    codex_fresh_comments = [c for c in fresh_comments if c.user_login == codex_login]
    if codex_fresh_comments:
        codex_reviews = [
            r for r in fresh_reviews
            if r.user_login == codex_login and r.state == "COMMENTED"
        ]
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            review_comments=codex_fresh_comments,
            last_review_body=codex_reviews[-1].body if codex_reviews else "",
        )

    # 4. A Codex COMMENTED review with body length above the boilerplate
    #    threshold means there's substantive content even if no inline
    #    comments were attached. Conservative — false negative is fine here
    #    (we'd just stay PENDING and re-poll).
    codex_substantive = [
        r for r in fresh_reviews
        if r.user_login == codex_login
        and r.state == "COMMENTED"
        and len(r.body) > CODEX_BOILERPLATE_THRESHOLD
    ]
    if codex_substantive:
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            last_review_body=codex_substantive[-1].body,
        )

    # 5. Approval via Codex ``+1`` reaction on the PR. Must be after HEAD's
    #    committer time, otherwise it's stale (referring to an earlier
    #    commit that's since been replaced).
    head_dt = _parse_iso(head_committed_at)
    if head_dt is not None:
        for rxn in reactions:
            if rxn.user_login != codex_login or rxn.content != "+1":
                continue
            rxn_dt = _parse_iso(rxn.created_at)
            if rxn_dt is not None and rxn_dt > head_dt:
                return Verdict(kind=VerdictKind.APPROVED)

    return Verdict(kind=VerdictKind.PENDING)


@dataclass(frozen=True)
class ReviewSnapshot:
    """All the data needed to evaluate a verdict at one polling tick."""

    head_sha: str
    head_committed_at: str
    reviews: list[Review]
    review_comments: list[ReviewComment]
    reactions: list[Reaction]
    checks: list[CheckRun]


def fetch_snapshot(*, pr_number: int, repo_path: Path) -> ReviewSnapshot:
    """One round-trip to GitHub to gather verdict inputs."""
    head = get_pr_head_sha(pr_number, repo_path=repo_path)
    return ReviewSnapshot(
        head_sha=head,
        head_committed_at=get_commit_committed_at(head, repo_path=repo_path),
        reviews=list_pr_reviews(pr_number, repo_path=repo_path),
        review_comments=list_pr_review_comments(pr_number, repo_path=repo_path),
        reactions=list_pr_reactions(pr_number, repo_path=repo_path),
        checks=list_pr_checks(pr_number, repo_path=repo_path),
    )


# ---- Polling driver ----

# Rounds 1-3 resume the session; rounds 4+ start fresh. Per SYMPHONY.md the
# resume is bounded so the prompt-cache hot path stays useful early but
# stops entrenching bad context if the agent's mental model is wrong by
# round 4.
RESUME_ROUNDS = 3


class LoopOutcomeKind(StrEnum):
    APPROVED = "approved"
    AUTO_STUCK_ROUNDS = "auto_stuck_rounds"
    AUTO_STUCK_IDLE = "auto_stuck_idle"
    AGENT_FAILED = "agent_failed"


@dataclass(frozen=True)
class LoopOutcome:
    kind: LoopOutcomeKind
    rounds_used: int
    last_session_id: str | None
    head_sha: str


def select_resume_session(round_index: int, current_session_id: str | None) -> str | None:
    """Decide whether to ``--resume <id>`` for round ``round_index`` (0-indexed).

    Rounds 0-2 (the first three review rounds) resume the original session;
    rounds 3+ run fresh so a wrong-headed early context can't trap us.
    """
    if round_index < RESUME_ROUNDS and current_session_id:
        return current_session_id
    return None


async def drive_review_loop(
    *,
    cfg: Any,
    issue_number: int,
    pr_number: int,
    branch: str,
    worktree: Path,
    initial_session_id: str | None,
    poll_interval_s: float = 30.0,
    re_nudge_after_s: float = 600.0,
    give_up_after_s: float = 1800.0,
    round_cap: int = 10,
    # Injectable dependencies — defaulting to the live module-level helpers.
    snapshot_fn: Callable[..., ReviewSnapshot] | None = None,
    run_agent_fn: Callable[..., Awaitable[Any]] | None = None,
    render_review_prompt: Callable[..., str] | None = None,
    push_fn: Callable[[Path, str], None] | None = None,
    head_sha_fn: Callable[[Path], str] | None = None,
    commits_to_push_fn: Callable[[Path, str, str], int] | None = None,
    comment_pr_fn: Callable[..., None] | None = None,
    label_issue_fn: Callable[..., None] | None = None,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    now_fn: Callable[[], float] = None,  # type: ignore[assignment]
) -> LoopOutcome:
    """Poll the PR until terminal state or a timeout/round cap.

    All side-effecting deps are injectable so unit tests can drive the loop
    deterministically without touching the network, the clock, or git.
    """
    # Resolve defaults lazily so tests can monkeypatch the module-level
    # functions without `drive_review_loop`'s defaults capturing the
    # originals at definition time.
    if snapshot_fn is None:
        snapshot_fn = fetch_snapshot
    if now_fn is None:
        import time
        now_fn = time.monotonic
    if run_agent_fn is None or render_review_prompt is None or push_fn is None or head_sha_fn is None or commits_to_push_fn is None or comment_pr_fn is None or label_issue_fn is None:
        # Live defaults — imported lazily to avoid a cycle with runonce.
        from . import runonce as _runonce
        from .agent import run_agent as _run_agent
        from .github import comment_pr as _comment_pr, label_issue as _label_issue

        if run_agent_fn is None:
            run_agent_fn = _run_agent
        if push_fn is None:
            push_fn = _runonce._git_push
        if head_sha_fn is None:
            head_sha_fn = _runonce._head_sha
        if commits_to_push_fn is None:
            commits_to_push_fn = _runonce._commits_to_push
        if comment_pr_fn is None:
            comment_pr_fn = _comment_pr
        if label_issue_fn is None:
            label_issue_fn = _label_issue
        if render_review_prompt is None:
            render_review_prompt = _default_render_review_prompt

    session_id = initial_session_id
    rounds_used = 0
    last_activity = now_fn()
    nudged_during_idle = False
    last_seen_head_sha = ""

    while True:
        await sleep_fn(poll_interval_s)
        snap = snapshot_fn(pr_number=pr_number, repo_path=cfg.repo.path)
        verdict = evaluate_verdict(
            head_sha=snap.head_sha,
            head_committed_at=snap.head_committed_at,
            reviews=snap.reviews,
            review_comments=snap.review_comments,
            reactions=snap.reactions,
            checks=snap.checks,
        )

        if verdict.kind == VerdictKind.APPROVED:
            return LoopOutcome(
                kind=LoopOutcomeKind.APPROVED,
                rounds_used=rounds_used,
                last_session_id=session_id,
                head_sha=snap.head_sha,
            )

        if verdict.kind == VerdictKind.CHANGES_REQUESTED:
            if rounds_used >= round_cap:
                label_issue_fn(issue_number, "auto-stuck", repo_path=cfg.repo.path)
                return LoopOutcome(
                    kind=LoopOutcomeKind.AUTO_STUCK_ROUNDS,
                    rounds_used=rounds_used,
                    last_session_id=session_id,
                    head_sha=snap.head_sha,
                )

            prompt = render_review_prompt(
                cfg=cfg,
                sha=snap.head_sha,
                comments=verdict.review_comments,
                ci_failures=verdict.ci_failures,
            )

            resume = select_resume_session(rounds_used, session_id)

            head_before = head_sha_fn(worktree)
            agent_result = await run_agent_fn(
                prompt,
                worktree,
                model=cfg.agent.model,
                max_turns=cfg.agent.max_turns,
                resume_session=resume,
            )

            if not agent_result.success:
                log.error(
                    "review-loop agent run failed (round %d, exit=%d)",
                    rounds_used,
                    agent_result.exit_code,
                )
                return LoopOutcome(
                    kind=LoopOutcomeKind.AGENT_FAILED,
                    rounds_used=rounds_used,
                    last_session_id=session_id,
                    head_sha=snap.head_sha,
                )

            # Capture (possibly fresh) session id for the next round.
            if agent_result.session_id:
                session_id = agent_result.session_id

            head_after = head_sha_fn(worktree)
            to_push = commits_to_push_fn(worktree, branch, cfg.repo.default_branch)
            if head_after != head_before or to_push > 0:
                push_fn(worktree, branch)
                comment_pr_fn(
                    repo_path=cfg.repo.path,
                    pr_number=pr_number,
                    body="@codex review",
                )
            else:
                log.warning(
                    "review round %d: agent exited cleanly with no new commits",
                    rounds_used,
                )

            rounds_used += 1
            last_activity = now_fn()
            nudged_during_idle = False
            last_seen_head_sha = snap.head_sha
            continue

        # PENDING — check timers
        elapsed = now_fn() - last_activity
        if elapsed >= give_up_after_s:
            label_issue_fn(issue_number, "auto-stuck", repo_path=cfg.repo.path)
            return LoopOutcome(
                kind=LoopOutcomeKind.AUTO_STUCK_IDLE,
                rounds_used=rounds_used,
                last_session_id=session_id,
                head_sha=snap.head_sha,
            )
        if elapsed >= re_nudge_after_s and not nudged_during_idle:
            comment_pr_fn(
                repo_path=cfg.repo.path,
                pr_number=pr_number,
                body="@codex review",
            )
            nudged_during_idle = True


def _default_render_review_prompt(
    *,
    cfg: Any,
    sha: str,
    comments: list[ReviewComment],
    ci_failures: list[CheckRun],
) -> str:
    """Fallback renderer used when the caller didn't pass one. Renders
    `prompts/review.md.j2` against the cfg's prompts_dir."""
    from .prompts import make_env, render

    env = make_env(cfg.paths.prompts_dir)
    return render(
        env,
        "review.md.j2",
        {"sha": sha, "comments": comments, "ci_failures": ci_failures},
    )
