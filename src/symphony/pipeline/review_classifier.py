"""Pure decision over `(comments, ci, snapshot)` for the Review stage.

The classifier is a pure function: it inspects PR review comments, CI
check runs, and a PR snapshot (head SHA, head commit time, reviews,
reactions, mergeability) and returns a `Verdict` plus a stable
`trigger_signature` that callers use to dedup consecutive fix-runs.

Splitting the eight rules into one priority-ordered function keeps the
contract testable without hitting the network or `gh`. The driver in
`orchestrator/` wraps it with the side-effecting calls.

Rules (priority order — first match wins):

  1. Failing required (or unknown-required) CI check → CHANGES_REQUESTED.
  2. Pending required CI check (no failures) → PENDING.
  3. mergeable=CONFLICTING → CHANGES_REQUESTED (merge_conflict). Checked
     before comment/review rules so a conflict is always detected even
     when stale Codex inline comments from a prior fix-run sit on HEAD.
  4. Codex inline review comment on HEAD → CHANGES_REQUESTED, unless a
     strictly newer Codex approval signal on HEAD supersedes all of them.
  5. Substantive Codex `COMMENTED` review on HEAD → CHANGES_REQUESTED,
     unless a strictly newer Codex approval signal supersedes it.
  6. Human `CHANGES_REQUESTED` on HEAD → CHANGES_REQUESTED.
  7. Codex approval signals: "any major issues" in COMMENTED
     review, or `+1` reaction (after HEAD commit time) → APPROVED when
     mergeable.
  8. Human `APPROVED` → APPROVED when mergeable.
  9. Approved + mergeable=UNKNOWN → PENDING.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

# Exact-match login. Substring matches must not classify (an impostor
# with `notchatgpt-codex-connector[bot]` as their login is not the bot).
CODEX_BOT_LOGIN = "chatgpt-codex-connector[bot]"

# Codex's review body always carries an "About Codex in GitHub" boilerplate
# block. Observed empty-suggestion bodies sit ~621 chars; a threshold above
# that with margin keeps short addenda from being mistaken for substantive
# feedback. False negatives here are acceptable — we just stay PENDING and
# re-poll.
CODEX_BOILERPLATE_THRESHOLD = 750
CODEX_NO_ISSUES_MARKER = "any major issues"

# PRD §pipeline: review iteration cap = 12. The orchestrator escapes to
# Needs Approval once this many fix-runs have been dispatched.
DEFAULT_REVIEW_ITERATION_CAP = 12

# GitHub check conclusions that indicate the required check did not pass.
BLOCKING_CHECK_CONCLUSIONS = frozenset(
    {
        "action_required",
        "cancelled",
        "failure",
        "stale",
        "startup_failure",
        "timed_out",
    }
)


class VerdictKind(StrEnum):
    APPROVED = "approved"
    CHANGES_REQUESTED = "changes_requested"
    PENDING = "pending"


@dataclass(frozen=True)
class ReviewComment:
    user_login: str
    body: str
    commit_sha: str
    created_at: str
    path: str = ""
    line: int | None = None


@dataclass(frozen=True)
class CheckRun:
    name: str
    status: str  # queued | in_progress | completed
    conclusion: str | None  # success | failure | cancelled | ...
    required: bool | None = None  # None = unknown


@dataclass(frozen=True)
class Reaction:
    user_login: str
    content: str  # +1, -1, eyes, ...
    created_at: str
    # SHA the approval explicitly names, when known (e.g. Codex's "Reviewed
    # commit: <sha>" line). Empty for genuine GitHub reactions, which carry no
    # commit reference and are validated by time alone.
    commit_sha: str = ""


@dataclass(frozen=True)
class Review:
    user_login: str
    state: str  # APPROVED | CHANGES_REQUESTED | COMMENTED | DISMISSED
    commit_sha: str
    submitted_at: str
    body: str = ""


@dataclass(frozen=True)
class ReviewSnapshot:
    head_sha: str
    head_committed_at: str
    reactions: tuple[Reaction, ...] = ()
    reviews: tuple[Review, ...] = ()
    mergeable: str | None = None  # MERGEABLE | CONFLICTING | UNKNOWN


@dataclass(frozen=True)
class Verdict:
    kind: VerdictKind
    trigger_signature: str = ""
    rule: str = ""  # which of the eight rules fired
    failing_checks: tuple[str, ...] = ()
    pending_checks: tuple[str, ...] = ()
    codex_comments: tuple[ReviewComment, ...] = field(default_factory=tuple)
    last_review_body: str = ""
    merge_conflict: bool = False


def is_codex_author(login: str) -> bool:
    """Strict equality with the Codex bot login.

    Substring matches are rejected so an account whose login *contains*
    `chatgpt-codex-connector[bot]` (an impostor, a typo, a similarly
    named bot) cannot drive the review verdict.
    """
    return login == CODEX_BOT_LOGIN


def _parse_iso(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _sha_refers_to(named: str, head: str) -> bool:
    """True when an (often abbreviated) SHA refers to the full HEAD OID.

    Git/GitHub abbreviate commit SHAs (Codex names ~10 hex chars) while
    `headRefOid` is the full 40-char OID. Treat the shorter as a prefix of the
    longer, case-insensitively. With no HEAD to compare against, do not match.
    """
    named = named.strip().lower()
    head = head.strip().lower()
    if not named or not head:
        return False
    return head.startswith(named) or named.startswith(head)


def _comment_key(c: ReviewComment) -> str:
    return f"{c.commit_sha}|{c.path}|{c.line}|{c.body}"


def _stable_digest(parts: Iterable[str]) -> str:
    h = hashlib.sha256()
    for part in parts:
        data = part.encode("utf-8")
        h.update(f"{len(data)}:".encode("ascii"))
        h.update(data)
    return h.hexdigest()[:16]


def _latest_reviews_by_author(reviews: Iterable[Review]) -> list[Review]:
    latest: dict[str, Review] = {}
    latest_seen_at: dict[str, datetime] = {}
    for review in reviews:
        submitted_at = _parse_iso(review.submitted_at) or datetime.min.replace(tzinfo=UTC)
        previous_at = latest_seen_at.get(review.user_login)
        if previous_at is None or submitted_at >= previous_at:
            latest[review.user_login] = review
            latest_seen_at[review.user_login] = submitted_at
    return list(latest.values())


def _required_check_verdict(ci: list[CheckRun], *, head_sha: str) -> Verdict | None:
    failing = [
        check
        for check in ci
        if check.required is not False
        and check.status == "completed"
        and check.conclusion in BLOCKING_CHECK_CONCLUSIONS
    ]
    if failing:
        names = sorted(check.name for check in failing)
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            trigger_signature=f"ci:{head_sha}:" + ",".join(names),
            rule="failing_ci",
            failing_checks=tuple(names),
        )

    pending = [check for check in ci if check.required is not False and check.status != "completed"]
    if pending:
        return Verdict(
            kind=VerdictKind.PENDING,
            rule="pending_ci",
            pending_checks=tuple(check.name for check in pending),
        )
    return None


def _signal_is_fresh(ts: str, *, head_dt: datetime | None, unknown_is_fresh: bool) -> bool:
    if head_dt is None:
        return True
    signal_dt = _parse_iso(ts)
    if signal_dt is None:
        return unknown_is_fresh
    return signal_dt >= head_dt


def _codex_review_has_no_issues_marker(review: Review, *, head_dt: datetime | None) -> bool:
    return (
        review.state == "COMMENTED"
        and is_codex_author(review.user_login)
        and CODEX_NO_ISSUES_MARKER in review.body.casefold()
        and _signal_is_fresh(review.submitted_at, head_dt=head_dt, unknown_is_fresh=True)
    )


def _codex_review_has_approval_emoji(review: Review, *, head_dt: datetime | None) -> bool:
    return (
        review.state == "COMMENTED"
        and is_codex_author(review.user_login)
        and len(review.body) < CODEX_BOILERPLATE_THRESHOLD
        and "👍" in review.body
        and _signal_is_fresh(review.submitted_at, head_dt=head_dt, unknown_is_fresh=True)
    )


def _codex_review_is_approval(review: Review, *, head_dt: datetime | None) -> bool:
    return _codex_review_has_no_issues_marker(
        review, head_dt=head_dt
    ) or _codex_review_has_approval_emoji(review, head_dt=head_dt)


def _newer_approval_time(
    current: datetime | None,
    ts: str,
    *,
    head_dt: datetime | None,
    require_head_timestamp: bool,
) -> datetime | None:
    if require_head_timestamp and head_dt is None:
        return current
    signal_dt = _parse_iso(ts)
    if signal_dt is None or (head_dt is not None and signal_dt < head_dt):
        return current
    if current is None or signal_dt > current:
        return signal_dt
    return current


def _codex_approval_time(
    *,
    snapshot: ReviewSnapshot,
    fresh_reviews: list[Review],
    head_dt: datetime | None,
) -> datetime | None:
    approval_at: datetime | None = None
    for reaction in snapshot.reactions:
        if not is_codex_author(reaction.user_login) or reaction.content != "+1":
            continue
        if reaction.commit_sha and not _sha_refers_to(reaction.commit_sha, snapshot.head_sha):
            continue
        approval_at = _newer_approval_time(
            approval_at,
            reaction.created_at,
            head_dt=head_dt,
            require_head_timestamp=True,
        )

    for review in fresh_reviews:
        if _codex_review_is_approval(review, head_dt=head_dt):
            approval_at = _newer_approval_time(
                approval_at,
                review.submitted_at,
                head_dt=head_dt,
                require_head_timestamp=False,
            )
    return approval_at


def _superseded_by_approval(ts: str, *, approval_at: datetime | None) -> bool:
    if approval_at is None:
        return False
    signal_dt = _parse_iso(ts)
    return signal_dt is not None and approval_at > signal_dt


def _review_feedback_verdict(
    *,
    comments: list[ReviewComment],
    snapshot: ReviewSnapshot,
    fresh_reviews: list[Review],
    latest_human_reviews: list[Review],
    head_dt: datetime | None,
    codex_approval_at: datetime | None,
) -> Verdict | None:
    codex_on_head = [
        comment
        for comment in comments
        if is_codex_author(comment.user_login)
        and comment.commit_sha == snapshot.head_sha
        and _signal_is_fresh(comment.created_at, head_dt=head_dt, unknown_is_fresh=False)
        and not _superseded_by_approval(comment.created_at, approval_at=codex_approval_at)
    ]
    if codex_on_head:
        digest = _stable_digest(sorted(_comment_key(comment) for comment in codex_on_head))
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            trigger_signature=f"codex_inline:{digest}",
            rule="codex_inline",
            codex_comments=tuple(codex_on_head),
        )

    codex_substantive = [
        review
        for review in fresh_reviews
        if is_codex_author(review.user_login)
        and review.state == "COMMENTED"
        and len(review.body) > CODEX_BOILERPLATE_THRESHOLD
        and not _codex_review_is_approval(review, head_dt=head_dt)
        and _signal_is_fresh(review.submitted_at, head_dt=head_dt, unknown_is_fresh=True)
        and not _superseded_by_approval(review.submitted_at, approval_at=codex_approval_at)
    ]
    if codex_substantive:
        body = codex_substantive[-1].body
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            trigger_signature=f"codex_review:{_stable_digest((body,))}",
            rule="codex_review",
            last_review_body=body,
        )

    human_changes_requested = [
        review for review in latest_human_reviews if review.state == "CHANGES_REQUESTED"
    ]
    if human_changes_requested:
        logins = sorted({review.user_login for review in human_changes_requested})
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            trigger_signature=f"human_cr:{snapshot.head_sha}:" + ",".join(logins),
            rule="human_changes_requested",
            last_review_body=human_changes_requested[-1].body,
        )
    return None


def _approval_verdict(
    *,
    snapshot: ReviewSnapshot,
    fresh_reviews: list[Review],
    latest_human_reviews: list[Review],
    head_dt: datetime | None,
    codex_approval_at: datetime | None,
) -> Verdict:
    codex_approved = (
        codex_approval_at is not None
        or any(
            _codex_review_has_no_issues_marker(review, head_dt=head_dt) for review in fresh_reviews
        )
        or any(
            _codex_review_has_approval_emoji(review, head_dt=head_dt) for review in fresh_reviews
        )
    )
    human_approved = any(review.state == "APPROVED" for review in latest_human_reviews)
    if not (codex_approved or human_approved):
        return Verdict(kind=VerdictKind.PENDING, rule="no_signal")
    if snapshot.mergeable != "MERGEABLE":
        return Verdict(kind=VerdictKind.PENDING, rule="approved_unknown_mergeable")
    rule_name = "codex_approved" if codex_approved else "human_approved"
    return Verdict(kind=VerdictKind.APPROVED, rule=rule_name)


def review_classifier(
    *,
    comments: list[ReviewComment],
    ci: list[CheckRun],
    snapshot: ReviewSnapshot,
) -> Verdict:
    """Classify the current review state. See module docstring for rules."""
    if check_verdict := _required_check_verdict(ci, head_sha=snapshot.head_sha):
        return check_verdict

    # Rule 3 — merge conflict blocks regardless of review/approval state.
    # Checked before comment rules so a conflict is always detected even
    # when stale Codex inline comments from a prior fix-run sit on HEAD.
    if snapshot.mergeable == "CONFLICTING":
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            trigger_signature=f"merge_conflict:{snapshot.head_sha}",
            rule="merge_conflict",
            merge_conflict=True,
        )

    head_dt = _parse_iso(snapshot.head_committed_at)

    fresh_reviews = [r for r in snapshot.reviews if r.commit_sha == snapshot.head_sha]
    latest_human_reviews = [
        r for r in _latest_reviews_by_author(fresh_reviews) if not is_codex_author(r.user_login)
    ]

    codex_approval_at = _codex_approval_time(
        snapshot=snapshot,
        fresh_reviews=fresh_reviews,
        head_dt=head_dt,
    )
    feedback = _review_feedback_verdict(
        comments=comments,
        snapshot=snapshot,
        fresh_reviews=fresh_reviews,
        latest_human_reviews=latest_human_reviews,
        head_dt=head_dt,
        codex_approval_at=codex_approval_at,
    )
    if feedback is not None:
        return feedback
    return _approval_verdict(
        snapshot=snapshot,
        fresh_reviews=fresh_reviews,
        latest_human_reviews=latest_human_reviews,
        head_dt=head_dt,
        codex_approval_at=codex_approval_at,
    )


def should_dispatch_fix_run(*, prev_signature: str, new_signature: str) -> bool:
    """Dedup gate over consecutive fix-runs.

    True iff `new_signature` is non-empty and differs from
    `prev_signature`. Same trigger twice in a row would otherwise have
    us ask the agent to fix exactly the same thing again, which is the
    stuck-loop pattern we explicitly avoid.
    """
    if not new_signature:
        return False
    return new_signature != prev_signature


def has_hit_iteration_cap(*, iteration: int, cap: int = DEFAULT_REVIEW_ITERATION_CAP) -> bool:
    """True once `iteration` reaches the cap (12 by default)."""
    return iteration >= cap


__all__ = [
    "CODEX_BOILERPLATE_THRESHOLD",
    "CODEX_BOT_LOGIN",
    "CheckRun",
    "DEFAULT_REVIEW_ITERATION_CAP",
    "Reaction",
    "Review",
    "ReviewComment",
    "ReviewSnapshot",
    "Verdict",
    "VerdictKind",
    "has_hit_iteration_cap",
    "is_codex_author",
    "review_classifier",
    "should_dispatch_fix_run",
]
