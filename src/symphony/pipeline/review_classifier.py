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
  4. Codex inline review comment on HEAD → CHANGES_REQUESTED.
  5. Substantive Codex `COMMENTED` review on HEAD → CHANGES_REQUESTED.
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
        submitted_at = _parse_iso(review.submitted_at) or datetime.min.replace(
            tzinfo=UTC
        )
        previous_at = latest_seen_at.get(review.user_login)
        if previous_at is None or submitted_at >= previous_at:
            latest[review.user_login] = review
            latest_seen_at[review.user_login] = submitted_at
    return list(latest.values())


def review_classifier(
    *,
    comments: list[ReviewComment],
    ci: list[CheckRun],
    snapshot: ReviewSnapshot,
) -> Verdict:
    """Classify the current review state. See module docstring for rules."""

    # Rule 1 — failed required (or unknown-required) CI check.
    failing = [
        c
        for c in ci
        if c.required is not False
        and c.status == "completed"
        and c.conclusion in BLOCKING_CHECK_CONCLUSIONS
    ]
    if failing:
        names = sorted(c.name for c in failing)
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            trigger_signature=f"ci:{snapshot.head_sha}:" + ",".join(names),
            rule="failing_ci",
            failing_checks=tuple(names),
        )

    pending = [c for c in ci if c.required is not False and c.status != "completed"]
    # Rule 2 — pending required CI check (no failures).
    if pending:
        return Verdict(
            kind=VerdictKind.PENDING,
            rule="pending_ci",
            pending_checks=tuple(c.name for c in pending),
        )

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

    def fresh_for_head(ts: str) -> bool:
        if head_dt is None:
            return True
        signal_dt = _parse_iso(ts)
        return signal_dt is not None and signal_dt >= head_dt

    fresh_reviews = [
        r for r in snapshot.reviews if r.commit_sha == snapshot.head_sha
    ]
    latest_human_reviews = [
        r
        for r in _latest_reviews_by_author(fresh_reviews)
        if not is_codex_author(r.user_login)
    ]

    # Rule 4 — Codex inline review comments on HEAD.
    codex_on_head = [
        c
        for c in comments
        if is_codex_author(c.user_login)
        and c.commit_sha == snapshot.head_sha
        and fresh_for_head(c.created_at)
    ]
    if codex_on_head:
        keys = sorted(_comment_key(c) for c in codex_on_head)
        # Keep the signature compact while staying stable across interpreter
        # restarts so persisted dedup state remains meaningful.
        digest = _stable_digest(keys)
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            trigger_signature=f"codex_inline:{digest}",
            rule="codex_inline",
            codex_comments=tuple(codex_on_head),
        )

    # Rule 5 — Codex `COMMENTED` review with substantive body on HEAD.
    codex_substantive = [
        r
        for r in fresh_reviews
        if is_codex_author(r.user_login)
        and r.state == "COMMENTED"
        and len(r.body) > CODEX_BOILERPLATE_THRESHOLD
    ]
    if codex_substantive:
        body = codex_substantive[-1].body
        digest = _stable_digest((body,))
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            trigger_signature=f"codex_review:{digest}",
            rule="codex_review",
            last_review_body=body,
        )

    # Rule 6 — human `CHANGES_REQUESTED` on HEAD.
    human_cr = [
        r
        for r in latest_human_reviews
        if r.state == "CHANGES_REQUESTED"
    ]
    if human_cr:
        logins = sorted({r.user_login for r in human_cr})
        return Verdict(
            kind=VerdictKind.CHANGES_REQUESTED,
            trigger_signature=f"human_cr:{snapshot.head_sha}:" + ",".join(logins),
            rule="human_changes_requested",
            last_review_body=human_cr[-1].body,
        )

    # Rule 7 — Codex approval signals.
    codex_approval_at: datetime | None = None
    # Check for Codex +1 reaction after HEAD commit
    for rxn in snapshot.reactions:
        if not is_codex_author(rxn.user_login) or rxn.content != "+1":
            continue
        rxn_dt = _parse_iso(rxn.created_at)
        if rxn_dt is None or head_dt is None or rxn_dt < head_dt:
            continue
        if codex_approval_at is None or rxn_dt > codex_approval_at:
            codex_approval_at = rxn_dt
    # Check for Codex "no major issues" text in COMMENTED review
    codex_no_issues = any(
        r.state == "COMMENTED"
        and is_codex_author(r.user_login)
        and CODEX_NO_ISSUES_MARKER in r.body.casefold()
        for r in fresh_reviews
    )
    # Check for 👍 emoji in Codex COMMENTED review body
    codex_emoji_approve = any(
        r.state == "COMMENTED"
        and is_codex_author(r.user_login)
        and "👍" in r.body
        for r in fresh_reviews
    )
    codex_approved = codex_approval_at is not None or codex_no_issues or codex_emoji_approve

    # Rule 8 — human `APPROVED`.
    human_approved = any(
        r.state == "APPROVED" for r in latest_human_reviews
    )
    approved = codex_approved or human_approved

    if approved:
        # Rule 9 — mergeable still computing or unavailable; do not race
        # `gh pr merge`.
        if snapshot.mergeable != "MERGEABLE":
            return Verdict(
                kind=VerdictKind.PENDING,
                rule="approved_unknown_mergeable",
            )
        rule_name = (
            "codex_approved"
            if codex_approved
            else "human_approved"
        )
        return Verdict(kind=VerdictKind.APPROVED, rule=rule_name)

    return Verdict(kind=VerdictKind.PENDING, rule="no_signal")


def should_dispatch_fix_run(
    *, prev_signature: str, new_signature: str
) -> bool:
    """Dedup gate over consecutive fix-runs.

    True iff `new_signature` is non-empty and differs from
    `prev_signature`. Same trigger twice in a row would otherwise have
    us ask the agent to fix exactly the same thing again, which is the
    stuck-loop pattern we explicitly avoid.
    """
    if not new_signature:
        return False
    return new_signature != prev_signature


def has_hit_iteration_cap(
    *, iteration: int, cap: int = DEFAULT_REVIEW_ITERATION_CAP
) -> bool:
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
