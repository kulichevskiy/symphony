"""Canonical Retry + validation-only Skip across every stage (SYM-245, slice 2/9).

SYM-244 gave the implement stage a durable control row and one canonical Retry.
This slice extends that seam to the rest of the pipeline: every park an operator
can act on records its stage's failed attempt, so Retry is offered from one place
instead of a per-stage command handler, and Skip exists for the two *validation*
stages only — review and acceptance. Implement, delivery and merge produce or
publish the artifact itself, so stepping over them would advance the pipeline
past work that never happened.

Skip is additionally scoped to the stage's input fingerprint (the PR head SHA
where one applies): a skip approves *what was reviewed*, so pushing new commits
invalidates it and validation is required again.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import pytest

from symphony import db
from symphony.config import Config
from symphony.linear.slash import SlashIntent, SlashKind
from symphony.orchestrator.poll import Orchestrator
from symphony.pipeline import controls

from .test_slash_polling import (
    _binding,
    _issue,
    _make_orch,
    _seed_operator_wait,
    _seed_review_state,
)

ACTIONS = controls.ControlAction
MODES = controls.PipelineMode
OUTCOMES = controls.AttemptOutcome

ISSUE_ID = "iss-1"
RUN_ID = "run-1"


async def _seed_issue(conn: aiosqlite.Connection) -> None:
    await db.issues.upsert(
        conn,
        id=ISSUE_ID,
        identifier="ENG-1",
        title="t",
        team_key="ENG",
    )


def _intent(kind: SlashKind, *, comment_id: str = "c-1") -> SlashIntent:
    return SlashIntent(
        kind=kind,
        comment_id=comment_id,
        created_at="2026-08-28T10:00:00+00:00",
        text=f"${kind.value}",
    )


# --------------------------------------------------------------------------
# The stage taxonomy: what Retry and Skip are offered for
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "stage"),
    [
        (db.operator_waits.KIND_IMPLEMENT_FAILED, controls.IMPLEMENT_STAGE),
        (db.operator_waits.KIND_IMPLEMENT_BLOCKED, controls.IMPLEMENT_STAGE),
        (db.operator_waits.KIND_DELIVER_FAILED, controls.DELIVERY_STAGE),
        (db.operator_waits.KIND_REVIEW_FAILED, controls.REVIEW_STAGE),
        (db.operator_waits.KIND_REVIEW_STOPPED, controls.REVIEW_STAGE),
        (db.operator_waits.KIND_REVIEW_CAP, controls.REVIEW_STAGE),
        (db.operator_waits.KIND_ACCEPTANCE_BLOCKED, controls.ACCEPTANCE_STAGE),
        (db.operator_waits.KIND_ACCEPTANCE_REJECTED, controls.ACCEPTANCE_STAGE),
    ],
)
def test_every_operator_actionable_park_maps_to_a_pipeline_stage(kind: str, stage: str) -> None:
    assert controls.stage_for_wait_kind(kind) == stage


@pytest.mark.parametrize(
    "kind",
    [db.operator_waits.KIND_MERGE, db.operator_waits.KIND_BUDGET_EXCEEDED],
)
def test_parks_outside_this_slice_map_to_no_stage(kind: str) -> None:
    assert controls.stage_for_wait_kind(kind) is None


@pytest.mark.parametrize(
    ("stage", "skippable"),
    [
        (controls.IMPLEMENT_STAGE, False),
        (controls.DELIVERY_STAGE, False),
        ("merge", False),
        (controls.REVIEW_STAGE, True),
        (controls.ACCEPTANCE_STAGE, True),
    ],
)
def test_skip_is_offered_for_validation_stages_only(stage: str, skippable: bool) -> None:
    allowed = controls.allowed_actions(MODES.PLAYING, OUTCOMES.FAILED, stage=stage)
    # Retry is canonical for every failed stage; Skip is not.
    assert ACTIONS.RETRY in allowed
    assert (ACTIONS.SKIP in allowed) is skippable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "stage", "skippable"),
    [
        (db.operator_waits.KIND_IMPLEMENT_FAILED, controls.IMPLEMENT_STAGE, False),
        (db.operator_waits.KIND_IMPLEMENT_BLOCKED, controls.IMPLEMENT_STAGE, False),
        (db.operator_waits.KIND_DELIVER_FAILED, controls.DELIVERY_STAGE, False),
        (db.operator_waits.KIND_REVIEW_FAILED, controls.REVIEW_STAGE, True),
        (db.operator_waits.KIND_REVIEW_STOPPED, controls.REVIEW_STAGE, True),
        (db.operator_waits.KIND_REVIEW_CAP, controls.REVIEW_STAGE, True),
        (db.operator_waits.KIND_ACCEPTANCE_BLOCKED, controls.ACCEPTANCE_STAGE, True),
        (db.operator_waits.KIND_ACCEPTANCE_REJECTED, controls.ACCEPTANCE_STAGE, True),
    ],
)
async def test_a_durable_park_exposes_canonical_retry_for_its_stage(
    tmp_path: Path, kind: str, stage: str, skippable: bool
) -> None:
    """Every park an operator can answer reads back as *that stage* having
    failed, with Retry offered — even with no control row, which is what an
    issue parked before this slice landed looks like after the upgrade."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await db.runs.create(
            conn,
            id=RUN_ID,
            issue_id=ISSUE_ID,
            stage=stage,
            status="failed",
            pid=None,
            started_at="2026-08-28T09:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id=ISSUE_ID,
            run_id=RUN_ID,
            kind=kind,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-08-28T09:30:00+00:00",
        )
        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.stage == stage
        assert snap.outcome is OUTCOMES.FAILED
        assert snap.run_id == RUN_ID
        assert ACTIONS.RETRY in snap.allowed_actions
        assert (ACTIONS.SKIP in snap.allowed_actions) is skippable
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_skip_is_rejected_for_a_mandatory_stage(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.IMPLEMENT_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="agent exited 2",
            run_id=RUN_ID,
            at="2026-08-28T10:00:00+00:00",
        )
        result = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.SKIP,
            actor="web:a-1",
            action_id="a-1",
            at="2026-08-28T10:01:00+00:00",
        )
        assert not result.accepted
        assert result.rejection is not None
        # Nothing was written: the mandatory stage is still failed and
        # still retryable.
        assert result.snapshot.outcome is OUTCOMES.FAILED
        assert await controls.history(conn, ISSUE_ID) == []
    finally:
        await conn.close()


# --------------------------------------------------------------------------
# Skip is scoped to the stage's input fingerprint
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_skip_is_scoped_to_the_input_fingerprint_and_expires_when_it_changes(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.REVIEW_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="CI red",
            run_id=RUN_ID,
            at="2026-08-28T10:00:00+00:00",
        )
        skipped = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.SKIP,
            actor="web:a-1",
            action_id="a-1",
            at="2026-08-28T10:01:00+00:00",
            fingerprint="sha-old",
        )
        assert skipped.accepted
        assert skipped.snapshot.outcome is OUTCOMES.SKIPPED
        assert skipped.snapshot.mode is MODES.PLAYING

        # Same input: the skip still stands, and the stage is not re-offered.
        unchanged = await controls.snapshot(conn, ISSUE_ID, fingerprint="sha-old")
        assert unchanged.outcome is OUTCOMES.SKIPPED
        assert ACTIONS.SKIP not in unchanged.allowed_actions

        # New commits on the PR: the skip approved a different input, so it
        # expires and the stage needs validating again.
        changed = await controls.snapshot(conn, ISSUE_ID, fingerprint="sha-new")
        assert changed.outcome is OUTCOMES.FAILED
        assert ACTIONS.RETRY in changed.allowed_actions
        assert ACTIONS.SKIP in changed.allowed_actions

        # And a fresh Skip for the new input is accepted rather than rejected
        # as "already skipped".
        again = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.SKIP,
            actor="web:a-2",
            action_id="a-2",
            at="2026-08-28T10:02:00+00:00",
            fingerprint="sha-new",
        )
        assert again.accepted
        assert (await controls.snapshot(conn, ISSUE_ID, fingerprint="sha-new")).outcome is (
            OUTCOMES.SKIPPED
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_retry_and_skip_return_to_playing_and_a_repeat_failure_returns_to_failed(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.REVIEW_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="CI red",
            run_id=RUN_ID,
            at="2026-08-28T10:00:00+00:00",
        )
        retried = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="web:a-1",
            action_id="a-1",
            at="2026-08-28T10:01:00+00:00",
        )
        assert retried.accepted
        assert retried.snapshot.mode is MODES.PLAYING
        assert retried.snapshot.outcome is OUTCOMES.PENDING
        # Retry targets exactly the failed stage; nothing earlier is replayed.
        assert retried.snapshot.stage == controls.REVIEW_STAGE
        assert [(a.action, a.stage) for a in await controls.history(conn, ISSUE_ID)] == [
            ("retry", controls.REVIEW_STAGE)
        ]

        # The fresh attempt fails too: back to failed, with Retry/Skip offered.
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.REVIEW_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="CI red again",
            run_id="run-2",
            at="2026-08-28T10:30:00+00:00",
        )
        again = await controls.snapshot(conn, ISSUE_ID)
        assert again.mode is MODES.PLAYING
        assert again.outcome is OUTCOMES.FAILED
        assert ACTIONS.RETRY in again.allowed_actions
        assert ACTIONS.SKIP in again.allowed_actions
    finally:
        await conn.close()


# --------------------------------------------------------------------------
# The park sites record their stage's failure
# --------------------------------------------------------------------------


def _orch(conn: aiosqlite.Connection, tmp_path: Path) -> Orchestrator:
    cfg = Config(repos=[_binding()], db_path=tmp_path / "s.sqlite")
    linear = AsyncMock()
    linear.lookup_issue = AsyncMock(return_value=_issue())
    linear.post_comment = AsyncMock(return_value="cmt-1")
    return _make_orch(cfg, linear, conn)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tracker_name", "stage"),
    [
        ("_track_review_failed_wait", controls.REVIEW_STAGE),
        ("_track_review_stopped_wait", controls.REVIEW_STAGE),
        ("_track_deliver_failed_wait", controls.DELIVERY_STAGE),
        ("_track_acceptance_rejected_wait", controls.ACCEPTANCE_STAGE),
    ],
)
async def test_park_records_its_stage_outcome_with_the_wait(
    tmp_path: Path, tracker_name: str, stage: str
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(conn, kind=db.operator_waits.KIND_REVIEW_FAILED, stage=stage)
        await db.operator_waits.delete(conn, ISSUE_ID, RUN_ID)
        orch = _orch(conn, tmp_path)
        await getattr(orch, tracker_name)(ISSUE_ID, RUN_ID, _binding())

        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None
        assert (row.stage, row.outcome, row.run_id) == (stage, "failed", RUN_ID)
        snap = await controls.snapshot(conn, ISSUE_ID)
        assert ACTIONS.RETRY in snap.allowed_actions
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_blocked_park_records_its_reason_through_the_new_plumbing(
    tmp_path: Path,
) -> None:
    """`_track_acceptance_blocked_wait` is the one park writer that passes a
    non-default `reason` through `_record_stage_park` — it must survive."""
    from symphony.pipeline.acceptance_classifier import AcceptanceVerdict

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await db.runs.create(
            conn,
            id=RUN_ID,
            issue_id=ISSUE_ID,
            stage="acceptance",
            status="running",
            pid=None,
            started_at="2026-08-28T09:00:00+00:00",
        )
        orch = _orch(conn, tmp_path)
        verdict = AcceptanceVerdict(
            kind="blocked",
            criteria=[],
            cost=0.0,
            hero_screenshot_url="",
            details="needs a human call on scope",
        )
        await orch._track_acceptance_blocked_wait(  # noqa: SLF001
            binding=_binding(),
            issue=_issue(),
            pr_number=42,
            run_id=RUN_ID,
            verdict=verdict,
        )

        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None
        assert (row.stage, row.outcome, row.run_id) == (
            controls.ACCEPTANCE_STAGE,
            "failed",
            RUN_ID,
        )
        assert row.reason == "needs a human call on scope"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_cap_park_records_its_reason_and_created_at_through_the_new_plumbing(
    tmp_path: Path,
) -> None:
    """The review-cap park (`_review.py`'s stuck-loop handoff) is the other
    park writer that passes non-default `reason`/`created_at` through
    `_record_stage_park` — both must survive."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_issue(conn)
        await db.runs.create(
            conn,
            id=RUN_ID,
            issue_id=ISSUE_ID,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-08-28T09:00:00+00:00",
        )
        orch = _orch(conn, tmp_path)
        await orch._record_stage_park(  # noqa: SLF001
            ISSUE_ID,
            RUN_ID,
            _binding(),
            kind=db.operator_waits.KIND_REVIEW_CAP,
            reason="review hit the 3-iteration cap",
            created_at="2026-08-28T09:05:00+00:00",
        )

        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None
        assert (row.stage, row.outcome, row.run_id) == (
            controls.REVIEW_STAGE,
            "failed",
            RUN_ID,
        )
        assert row.reason == "review hit the 3-iteration cap"
        wait = await db.operator_waits.get(conn, ISSUE_ID)
        assert wait is not None
        assert wait.created_at == "2026-08-28T09:05:00+00:00"
    finally:
        await conn.close()


# --------------------------------------------------------------------------
# Command handlers route through the canonical transition
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_review_failed_retry_goes_through_the_canonical_transition(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn, kind=db.operator_waits.KIND_REVIEW_FAILED, stage="review", status="failed"
        )
        await _seed_review_state(conn)
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.REVIEW_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="CI red",
            run_id=RUN_ID,
            at="2026-08-28T09:30:00+00:00",
        )
        orch = _orch(conn, tmp_path)
        orch._schedule_review_poll = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._dispatch_run_ids[ISSUE_ID] = RUN_ID  # noqa: SLF001
        orch._operator_wait_run_ids.add(RUN_ID)  # noqa: SLF001
        orch._review_failed_run_bindings[RUN_ID] = _binding()  # noqa: SLF001

        await orch._handle_review_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, RUN_ID, _intent(SlashKind.RETRY, comment_id="c-retry")
        )
        assert [(a.action, a.stage) for a in await controls.history(conn, ISSUE_ID)] == [
            ("retry", controls.REVIEW_STAGE)
        ]
        assert orch._schedule_review_poll.call_count == 1  # type: ignore[attr-defined]  # noqa: SLF001

        # A re-delivered `$retry` for the same comment cannot start a second
        # review attempt: the canonical transition already consumed it.
        await db.operator_waits.upsert(
            conn,
            issue_id=ISSUE_ID,
            run_id=RUN_ID,
            kind=db.operator_waits.KIND_REVIEW_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-08-28T10:05:00+00:00",
        )
        orch._review_failed_run_bindings[RUN_ID] = _binding()  # noqa: SLF001
        await orch._handle_review_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, RUN_ID, _intent(SlashKind.RETRY, comment_id="c-retry")
        )
        assert orch._schedule_review_poll.call_count == 1  # type: ignore[attr-defined]  # noqa: SLF001
        assert len(await controls.history(conn, ISSUE_ID)) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_failed_review_exposes_skip_and_it_advances_to_merge(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn, kind=db.operator_waits.KIND_REVIEW_FAILED, stage="review", status="failed"
        )
        await _seed_review_state(conn)
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.REVIEW_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="CI red",
            run_id=RUN_ID,
            at="2026-08-28T09:30:00+00:00",
        )
        assert ACTIONS.SKIP in (await controls.snapshot(conn, ISSUE_ID)).allowed_actions

        orch = _orch(conn, tmp_path)
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._review_failed_run_bindings[RUN_ID] = _binding()  # noqa: SLF001
        gh = MagicMock()
        gh.pr_view = AsyncMock(return_value={"headRefOid": "sha-old"})
        orch._gh_client = AsyncMock(return_value=gh)  # type: ignore[method-assign]  # noqa: SLF001

        await orch._handle_review_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, RUN_ID, _intent(SlashKind.SKIP_REVIEW, comment_id="c-skip")
        )
        assert [(a.action, a.stage) for a in await controls.history(conn, ISSUE_ID)] == [
            ("skip", controls.REVIEW_STAGE)
        ]
        assert (await controls.snapshot(conn, ISSUE_ID)).outcome is OUTCOMES.SKIPPED
        assert orch._schedule_merge.call_count == 1  # type: ignore[attr-defined]  # noqa: SLF001
        assert await db.operator_waits.get(conn, ISSUE_ID) is None
        # The skip approved the commits that were on the PR at the time; a push
        # since then invalidates it and review is required again.
        assert (await controls.snapshot(conn, ISSUE_ID, fingerprint="sha-new")).outcome is (
            OUTCOMES.FAILED
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_skip_failed_review_releases_the_accepted_skip_when_a_side_effect_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If a side effect after the accepted Skip raises, the accepted
    transition must be released — not left `SKIPPED` with no Retry/Skip on
    offer and no daemon-restart recovery path (`reconcile_interrupted_retries`
    only resets `PENDING` rows), which is strictly worse than a stuck Retry
    (SYM-245 review)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn, kind=db.operator_waits.KIND_REVIEW_FAILED, stage="review", status="failed"
        )
        await _seed_review_state(conn)
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.REVIEW_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="CI red",
            run_id=RUN_ID,
            at="2026-08-28T09:30:00+00:00",
        )

        orch = _orch(conn, tmp_path)
        orch._review_failed_run_bindings[RUN_ID] = _binding()  # noqa: SLF001
        gh = MagicMock()
        gh.pr_view = AsyncMock(return_value={"headRefOid": "sha-old"})
        orch._gh_client = AsyncMock(return_value=gh)  # type: ignore[method-assign]  # noqa: SLF001

        async def _boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("db died mid-skip")

        monkeypatch.setattr(db.issue_prs, "mark_review_bypassed", _boom)

        with pytest.raises(RuntimeError, match="db died mid-skip"):
            await orch._handle_review_failed_slash_intent(  # noqa: SLF001
                ISSUE_ID, RUN_ID, _intent(SlashKind.SKIP_REVIEW, comment_id="c-skip")
            )

        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.outcome is OUTCOMES.FAILED
        assert ACTIONS.SKIP in snap.allowed_actions
        assert ACTIONS.RETRY in snap.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_skip_failed_review_re_arms_ingress_when_schedule_merge_raises(
    tmp_path: Path,
) -> None:
    """Unlike the sibling test above, the raise here happens in
    `_schedule_merge` — *after* `_clear_operator_wait` has already popped
    `_dispatch_run_ids`/`_operator_wait_run_ids`/`_review_failed_run_bindings`
    *and* deleted the durable `operator_waits` row. Releasing the accepted
    Skip without re-arming the in-memory dicts *and* re-inserting the durable
    row would leave no ingress for a re-delivered `$skip-review` to land on:
    `_handle_slash_intent`'s stale-wait guard and
    `_restore_operator_wait_binding` both authorize a re-delivered command
    against the durable row, not process memory (SYM-245 review)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn, kind=db.operator_waits.KIND_REVIEW_FAILED, stage="review", status="failed"
        )
        await _seed_review_state(conn)
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.REVIEW_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="CI red",
            run_id=RUN_ID,
            at="2026-08-28T09:30:00+00:00",
        )

        orch = _orch(conn, tmp_path)
        orch._review_failed_run_bindings[RUN_ID] = _binding()  # noqa: SLF001
        orch._dispatch_run_ids[ISSUE_ID] = RUN_ID  # noqa: SLF001
        orch._operator_wait_run_ids.add(RUN_ID)  # noqa: SLF001
        gh = MagicMock()
        gh.pr_view = AsyncMock(return_value={"headRefOid": "sha-old"})
        orch._gh_client = AsyncMock(return_value=gh)  # type: ignore[method-assign]  # noqa: SLF001
        orch._schedule_merge = MagicMock(  # type: ignore[method-assign]  # noqa: SLF001
            side_effect=RuntimeError("merge scheduling died")
        )

        with pytest.raises(RuntimeError, match="merge scheduling died"):
            await orch._handle_review_failed_slash_intent(  # noqa: SLF001
                ISSUE_ID, RUN_ID, _intent(SlashKind.SKIP_REVIEW, comment_id="c-skip")
            )

        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.outcome is OUTCOMES.FAILED
        assert ACTIONS.SKIP in snap.allowed_actions
        assert ACTIONS.RETRY in snap.allowed_actions
        assert orch._dispatch_run_ids.get(ISSUE_ID) == RUN_ID  # noqa: SLF001
        assert RUN_ID in orch._operator_wait_run_ids  # noqa: SLF001
        assert orch._review_failed_run_bindings.get(RUN_ID) is not None  # noqa: SLF001
        wait = await db.operator_waits.get(conn, ISSUE_ID)
        assert wait is not None
        assert wait.run_id == RUN_ID
        assert wait.kind == db.operator_waits.KIND_REVIEW_FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_deliver_failed_retry_goes_through_the_canonical_transition(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn, kind=db.operator_waits.KIND_DELIVER_FAILED, stage="implement", status="failed"
        )
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.DELIVERY_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="push rejected",
            run_id=RUN_ID,
            at="2026-08-28T09:30:00+00:00",
        )
        orch = _orch(conn, tmp_path)
        orch._deliver_failed_run_bindings[RUN_ID] = _binding()  # noqa: SLF001
        ctx = MagicMock()
        ctx.retry_workspace_acquired = False
        orch._resolve_pending_delivery = AsyncMock(return_value=ctx)  # type: ignore[method-assign]  # noqa: SLF001
        orch._deliver_implement_run = AsyncMock(return_value="")  # type: ignore[method-assign]  # noqa: SLF001

        await orch._handle_deliver_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, RUN_ID, _intent(SlashKind.RETRY, comment_id="c-retry")
        )
        assert [(a.action, a.stage) for a in await controls.history(conn, ISSUE_ID)] == [
            ("retry", controls.DELIVERY_STAGE)
        ]
        # Delivery is mandatory: no Skip on offer for the park.
        parked = await controls.snapshot(conn, ISSUE_ID)
        assert ACTIONS.SKIP not in controls.allowed_actions(
            MODES.PLAYING, OUTCOMES.FAILED, stage=parked.stage
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_skip_records_a_canonical_skip_scoped_to_the_pr_head(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_ACCEPTANCE_REJECTED,
            stage="acceptance",
            status="failed",
        )
        await db.acceptance_state.begin_acceptance(
            conn,
            ISSUE_ID,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="sha-old",
            mode="static",
            preview_url="",
            extracted_criteria="",
        )
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.ACCEPTANCE_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="acceptance rejected",
            run_id=RUN_ID,
            at="2026-08-28T09:30:00+00:00",
        )
        orch = _orch(conn, tmp_path)
        orch._schedule_merge = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001
        orch._acceptance_rejected_run_bindings[RUN_ID] = _binding()  # noqa: SLF001

        await orch._handle_acceptance_rejected_slash_intent(  # noqa: SLF001
            ISSUE_ID, RUN_ID, _intent(SlashKind.SKIP_ACCEPTANCE, comment_id="c-skip")
        )
        assert [(a.action, a.stage) for a in await controls.history(conn, ISSUE_ID)] == [
            ("skip", controls.ACCEPTANCE_STAGE)
        ]
        row = await db.pipeline_controls.get(conn, ISSUE_ID)
        assert row is not None
        assert row.outcome == "skipped"
        assert row.fingerprint == "sha-old"
        # A push to the PR invalidates the skip: acceptance is required again.
        expired = await controls.snapshot(conn, ISSUE_ID, fingerprint="sha-new")
        assert expired.outcome is OUTCOMES.FAILED
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_skip_acceptance_rejected_releases_the_accepted_skip_when_a_side_effect_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mirrors `test_skip_failed_review_releases_the_accepted_skip_when_a_side_effect_raises`:
    if a side effect after the accepted Skip raises, the accepted transition
    must be released — not left `SKIPPED` with no Retry/Skip on offer (SYM-245
    review)."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn,
            kind=db.operator_waits.KIND_ACCEPTANCE_REJECTED,
            stage="acceptance",
            status="failed",
        )
        await db.acceptance_state.begin_acceptance(
            conn,
            ISSUE_ID,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="sha-old",
            mode="static",
            preview_url="",
            extracted_criteria="",
        )
        await controls.record_stage_outcome(
            conn,
            ISSUE_ID,
            stage=controls.ACCEPTANCE_STAGE,
            outcome=OUTCOMES.FAILED,
            reason="acceptance rejected",
            run_id=RUN_ID,
            at="2026-08-28T09:30:00+00:00",
        )
        orch = _orch(conn, tmp_path)
        orch._acceptance_rejected_run_bindings[RUN_ID] = _binding()  # noqa: SLF001

        async def _boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("db died mid-skip")

        monkeypatch.setattr(db.acceptance_state, "record_verdict", _boom)

        with pytest.raises(RuntimeError, match="db died mid-skip"):
            await orch._handle_acceptance_rejected_slash_intent(  # noqa: SLF001
                ISSUE_ID, RUN_ID, _intent(SlashKind.SKIP_ACCEPTANCE, comment_id="c-skip")
            )

        snap = await controls.snapshot(conn, ISSUE_ID)
        assert snap.outcome is OUTCOMES.FAILED
        assert ACTIONS.SKIP in snap.allowed_actions
        assert ACTIONS.RETRY in snap.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_a_new_park_after_an_accepted_retry_is_failed_again_not_masked(
    tmp_path: Path,
) -> None:
    """An accepted Retry leaves the row `pending`. The next park must overwrite
    it with its own stage's failure — otherwise the pending row would mask the
    new park and Retry would never be offered again."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn, kind=db.operator_waits.KIND_IMPLEMENT_FAILED, stage="implement", status="failed"
        )
        orch = _orch(conn, tmp_path)
        await orch._track_implement_failed_wait(ISSUE_ID, RUN_ID, _binding())  # noqa: SLF001
        retried = await controls.apply(
            conn,
            ISSUE_ID,
            ACTIONS.RETRY,
            actor="web:a-1",
            action_id="a-1",
            at="2026-08-28T10:00:00+00:00",
        )
        assert retried.accepted
        assert (await controls.snapshot(conn, ISSUE_ID)).outcome is OUTCOMES.PENDING

        # The fresh attempt blocks on a human action instead.
        await db.runs.create(
            conn,
            id="run-2",
            issue_id=ISSUE_ID,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-08-28T10:05:00+00:00",
        )
        await orch._track_implement_blocked_wait(ISSUE_ID, "run-2", _binding())  # noqa: SLF001
        parked = await controls.snapshot(conn, ISSUE_ID)
        assert parked.stage == controls.IMPLEMENT_STAGE
        assert parked.outcome is OUTCOMES.FAILED
        assert parked.run_id == "run-2"
        assert ACTIONS.RETRY in parked.allowed_actions
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stopping_a_review_park_settles_it_so_retry_is_no_longer_offered(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn, kind=db.operator_waits.KIND_REVIEW_FAILED, stage="review", status="failed"
        )
        await _seed_review_state(conn)
        orch = _orch(conn, tmp_path)
        await orch._track_review_failed_wait(ISSUE_ID, RUN_ID, _binding())  # noqa: SLF001
        assert ACTIONS.RETRY in (await controls.snapshot(conn, ISSUE_ID)).allowed_actions

        orch._review_failed_run_bindings[RUN_ID] = _binding()  # noqa: SLF001
        await orch._handle_review_failed_slash_intent(  # noqa: SLF001
            ISSUE_ID, RUN_ID, _intent(SlashKind.STOP, comment_id="c-stop")
        )
        settled = await controls.snapshot(conn, ISSUE_ID)
        assert settled.outcome is OUTCOMES.SKIPPED
        assert ACTIONS.RETRY not in settled.allowed_actions
        assert ACTIONS.SKIP not in settled.allowed_actions
        assert await db.operator_waits.get(conn, ISSUE_ID) is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_blocked_implement_retry_carries_no_operator_command_text(
    tmp_path: Path,
) -> None:
    """The blocked reason survives as handoff context; the operator's command
    text does not become an instruction payload for the fresh attempt — the
    new run re-reads the tracker instead."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_operator_wait(
            conn, kind=db.operator_waits.KIND_IMPLEMENT_BLOCKED, stage="implement", status="blocked"
        )
        await db.runs.update_status(
            conn,
            RUN_ID,
            db.runs.FAILED_STATUS,
            ended_at="2026-08-28T09:00:00+00:00",
            kind="blocked",
            detail="need a staging token",
        )
        orch = _orch(conn, tmp_path)
        orch._implement_blocked_run_bindings[RUN_ID] = _binding()  # noqa: SLF001

        intent = SlashIntent(
            kind=SlashKind.RETRY,
            comment_id="c-retry",
            created_at="2026-08-28T10:00:00+00:00",
            text="$retry here is the token: hunter2",
        )
        await orch._handle_implement_blocked_slash_intent(ISSUE_ID, RUN_ID, intent)  # noqa: SLF001

        handoff = orch._implement_handoffs[ISSUE_ID]  # noqa: SLF001
        assert handoff.blocked_reason == "need a staging token"
        assert not hasattr(handoff, "operator_comment")
        assert [(a.action, a.stage) for a in await controls.history(conn, ISSUE_ID)] == [
            ("retry", controls.IMPLEMENT_STAGE)
        ]
    finally:
        await conn.close()
