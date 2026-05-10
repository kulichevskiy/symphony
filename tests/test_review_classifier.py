"""Pure tests for `pipeline.review_classifier`.

The classifier is a pure function over `(comments, ci, snapshot)` that
returns a `Verdict` and a stable trigger signature. The eight rules from
the design doc each have at least one targeted test here:

  1. Failing required CI check → CHANGES_REQUESTED.
  2. Pending required CI check (no failures) → PENDING.
  3. Codex inline review comment on HEAD → CHANGES_REQUESTED.
  4. Substantive Codex `COMMENTED` review on HEAD → CHANGES_REQUESTED.
  5. Human `CHANGES_REQUESTED` on HEAD → CHANGES_REQUESTED.
  6. Approval (Codex +1 after HEAD commit time, or human APPROVED) →
     APPROVED when mergeable.
  7. Approved + mergeable=CONFLICTING → CHANGES_REQUESTED (merge_conflict).
  8. Approved + mergeable=UNKNOWN → PENDING.

Plus: the author detector rejects substring matches, the trigger
signature dedups identical consecutive triggers, and the iteration cap
helper fires at the configured cap.
"""

from __future__ import annotations

import pytest

from symphony.pipeline.review_classifier import (
    CODEX_BOT_LOGIN,
    DEFAULT_REVIEW_ITERATION_CAP,
    CheckRun,
    Reaction,
    Review,
    ReviewComment,
    ReviewSnapshot,
    Verdict,
    VerdictKind,
    has_hit_iteration_cap,
    is_codex_author,
    review_classifier,
    should_dispatch_fix_run,
)

HEAD_SHA = "deadbeef"
HEAD_COMMITTED_AT = "2025-01-01T12:00:00Z"
LATER = "2025-01-01T13:00:00Z"
EARLIER = "2024-12-31T12:00:00Z"


def _snap(
    *,
    head_sha: str = HEAD_SHA,
    head_committed_at: str = HEAD_COMMITTED_AT,
    reactions: tuple[Reaction, ...] = (),
    reviews: tuple[Review, ...] = (),
    mergeable: str | None = "MERGEABLE",
) -> ReviewSnapshot:
    return ReviewSnapshot(
        head_sha=head_sha,
        head_committed_at=head_committed_at,
        reactions=reactions,
        reviews=reviews,
        mergeable=mergeable,
    )


# --- Author detection ------------------------------------------------------


def test_is_codex_author_exact_match() -> None:
    assert is_codex_author(CODEX_BOT_LOGIN) is True


def test_is_codex_author_rejects_substring() -> None:
    # `notchatgpt-codex-connector[bot]` *contains* the Codex login but is a
    # different (potentially adversarial) account — must not be classified
    # as the bot.
    assert is_codex_author(f"not{CODEX_BOT_LOGIN}") is False
    assert is_codex_author(f"{CODEX_BOT_LOGIN}-extra") is False
    assert is_codex_author("") is False
    assert is_codex_author("chatgpt-codex") is False


# --- Rule 1: failing CI ----------------------------------------------------


def test_rule_1_failing_required_ci_marks_changes_requested() -> None:
    ci = [
        CheckRun(name="unit", status="completed", conclusion="success", required=True),
        CheckRun(name="lint", status="completed", conclusion="failure", required=True),
    ]
    v = review_classifier(comments=[], ci=ci, snapshot=_snap())
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.rule == "failing_ci"
    assert "lint" in v.failing_checks
    assert v.trigger_signature.startswith("ci:")


def test_rule_1_failing_ci_with_unknown_required_still_blocks() -> None:
    # When `required` is None (unknown), still treat a completed failure
    # as blocking — better to over-report than to silently pass with
    # missing metadata.
    ci = [
        CheckRun(name="security", status="completed", conclusion="failure", required=None),
    ]
    v = review_classifier(comments=[], ci=ci, snapshot=_snap())
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.rule == "failing_ci"


def test_rule_1_explicitly_optional_failure_does_not_block() -> None:
    ci = [
        CheckRun(name="flaky", status="completed", conclusion="failure", required=False),
    ]
    v = review_classifier(comments=[], ci=ci, snapshot=_snap())
    assert v.kind != VerdictKind.CHANGES_REQUESTED


# --- Rule 2: pending CI ----------------------------------------------------


def test_rule_2_pending_required_ci_marks_pending() -> None:
    ci = [
        CheckRun(name="unit", status="in_progress", conclusion=None, required=True),
    ]
    v = review_classifier(comments=[], ci=ci, snapshot=_snap())
    assert v.kind == VerdictKind.PENDING
    assert v.rule == "pending_ci"
    assert "unit" in v.pending_checks


# --- Rule 3: Codex inline comments on HEAD --------------------------------


def test_rule_3_codex_inline_comment_on_head_changes_requested() -> None:
    comments = [
        ReviewComment(
            user_login=CODEX_BOT_LOGIN,
            body="Consider extracting this into a helper.",
            commit_sha=HEAD_SHA,
            created_at=LATER,
            path="src/foo.py",
            line=42,
        ),
    ]
    v = review_classifier(comments=comments, ci=[], snapshot=_snap())
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.rule == "codex_inline"
    assert v.codex_comments == tuple(comments)
    assert v.trigger_signature.startswith("codex_inline:")


def test_rule_3_codex_comment_on_stale_sha_ignored() -> None:
    comments = [
        ReviewComment(
            user_login=CODEX_BOT_LOGIN,
            body="Stale feedback.",
            commit_sha="oldsha",
            created_at=EARLIER,
        ),
    ]
    v = review_classifier(comments=comments, ci=[], snapshot=_snap())
    assert v.kind != VerdictKind.CHANGES_REQUESTED


# --- Rule 4: Codex substantive review body --------------------------------


def test_rule_4_codex_substantive_review_body_changes_requested() -> None:
    long_body = "These are detailed feedback notes. " * 30  # >> boilerplate threshold
    reviews = (
        Review(
            user_login=CODEX_BOT_LOGIN,
            state="COMMENTED",
            commit_sha=HEAD_SHA,
            submitted_at=LATER,
            body=long_body,
        ),
    )
    v = review_classifier(comments=[], ci=[], snapshot=_snap(reviews=reviews))
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.rule == "codex_review"
    assert v.last_review_body == long_body


def test_rule_4_boilerplate_review_does_not_trigger() -> None:
    short_body = "Looks good — see About Codex in GitHub."
    reviews = (
        Review(
            user_login=CODEX_BOT_LOGIN,
            state="COMMENTED",
            commit_sha=HEAD_SHA,
            submitted_at=LATER,
            body=short_body,
        ),
    )
    v = review_classifier(comments=[], ci=[], snapshot=_snap(reviews=reviews))
    assert v.kind != VerdictKind.CHANGES_REQUESTED


# --- Rule 5: human CHANGES_REQUESTED --------------------------------------


def test_rule_5_human_changes_requested_on_head() -> None:
    reviews = (
        Review(
            user_login="alice",
            state="CHANGES_REQUESTED",
            commit_sha=HEAD_SHA,
            submitted_at=LATER,
            body="Please address the comments.",
        ),
    )
    v = review_classifier(comments=[], ci=[], snapshot=_snap(reviews=reviews))
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.rule == "human_changes_requested"
    assert v.trigger_signature.startswith("human_cr:")


# --- Rule 6: approval ------------------------------------------------------


def test_rule_6_codex_plus_one_after_head_commit_marks_approved() -> None:
    reactions = (
        Reaction(user_login=CODEX_BOT_LOGIN, content="+1", created_at=LATER),
    )
    v = review_classifier(comments=[], ci=[], snapshot=_snap(reactions=reactions))
    assert v.kind == VerdictKind.APPROVED
    assert v.rule == "approved"


def test_rule_6_codex_plus_one_before_head_commit_is_stale() -> None:
    reactions = (
        Reaction(user_login=CODEX_BOT_LOGIN, content="+1", created_at=EARLIER),
    )
    v = review_classifier(comments=[], ci=[], snapshot=_snap(reactions=reactions))
    assert v.kind != VerdictKind.APPROVED


def test_rule_6_human_approval_counts() -> None:
    reviews = (
        Review(
            user_login="alice",
            state="APPROVED",
            commit_sha=HEAD_SHA,
            submitted_at=LATER,
        ),
    )
    v = review_classifier(comments=[], ci=[], snapshot=_snap(reviews=reviews))
    assert v.kind == VerdictKind.APPROVED


# --- Rule 7: approved + CONFLICTING ---------------------------------------


def test_rule_7_approved_but_conflicting_marks_changes_requested() -> None:
    reactions = (
        Reaction(user_login=CODEX_BOT_LOGIN, content="+1", created_at=LATER),
    )
    snap = _snap(reactions=reactions, mergeable="CONFLICTING")
    v = review_classifier(comments=[], ci=[], snapshot=snap)
    assert v.kind == VerdictKind.CHANGES_REQUESTED
    assert v.rule == "merge_conflict"
    assert v.merge_conflict is True
    assert v.trigger_signature.startswith("merge_conflict:")


# --- Rule 8: approved + UNKNOWN -------------------------------------------


def test_rule_8_approved_but_unknown_mergeable_stays_pending() -> None:
    reactions = (
        Reaction(user_login=CODEX_BOT_LOGIN, content="+1", created_at=LATER),
    )
    snap = _snap(reactions=reactions, mergeable="UNKNOWN")
    v = review_classifier(comments=[], ci=[], snapshot=snap)
    assert v.kind == VerdictKind.PENDING


# --- Substring rejection inside the classifier -----------------------------


def test_classifier_rejects_bot_substring_for_inline_comments() -> None:
    # An impostor account whose login *contains* the bot login must not
    # have its inline comments treated as Codex feedback.
    comments = [
        ReviewComment(
            user_login=f"not{CODEX_BOT_LOGIN}",
            body="This is a fake codex bot's comment.",
            commit_sha=HEAD_SHA,
            created_at=LATER,
        ),
    ]
    v = review_classifier(comments=comments, ci=[], snapshot=_snap())
    assert v.kind != VerdictKind.CHANGES_REQUESTED
    assert v.rule != "codex_inline"


# --- Trigger-signature dedup ----------------------------------------------


def test_trigger_signature_stable_across_runs() -> None:
    """Two classifications of the same input produce identical signatures."""
    ci = [CheckRun(name="lint", status="completed", conclusion="failure", required=True)]
    a = review_classifier(comments=[], ci=ci, snapshot=_snap())
    b = review_classifier(comments=[], ci=ci, snapshot=_snap())
    assert a.trigger_signature == b.trigger_signature


def test_should_dispatch_fix_run_dedupes_identical_signatures() -> None:
    """Same trigger twice in a row → no second fix-run dispatched."""
    assert should_dispatch_fix_run(prev_signature="ci:lint", new_signature="ci:lint") is False


def test_should_dispatch_fix_run_fires_on_new_signature() -> None:
    assert should_dispatch_fix_run(prev_signature="ci:lint", new_signature="ci:unit") is True


def test_should_dispatch_fix_run_fires_when_no_previous() -> None:
    assert should_dispatch_fix_run(prev_signature="", new_signature="ci:lint") is True


def test_should_dispatch_fix_run_rejects_empty_signature() -> None:
    """A verdict with no trigger (e.g. APPROVED, PENDING) never spawns a fix-run."""
    assert should_dispatch_fix_run(prev_signature="ci:lint", new_signature="") is False


# --- Iteration cap ---------------------------------------------------------


def test_iteration_cap_default_is_twelve() -> None:
    # PRD §pipeline: review iteration cap = 12.
    assert DEFAULT_REVIEW_ITERATION_CAP == 12


def test_has_hit_iteration_cap_fires_at_cap() -> None:
    assert has_hit_iteration_cap(iteration=12) is True


def test_has_hit_iteration_cap_below_cap_is_false() -> None:
    assert has_hit_iteration_cap(iteration=11) is False


def test_has_hit_iteration_cap_custom_cap_honored() -> None:
    assert has_hit_iteration_cap(iteration=3, cap=3) is True
    assert has_hit_iteration_cap(iteration=2, cap=3) is False


# --- Verdict is immutable -------------------------------------------------


def test_verdict_is_frozen_dataclass() -> None:
    from dataclasses import FrozenInstanceError

    v = Verdict(kind=VerdictKind.PENDING)
    with pytest.raises(FrozenInstanceError):
        v.kind = VerdictKind.APPROVED  # type: ignore[misc]
