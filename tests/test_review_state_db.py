"""Persistence layer for the review iteration counter and last-trigger
signature.

The Review stage needs durable state so that:
  - iteration count survives an orchestrator restart (the cap of 12
    spans many minutes / hours of polling),
  - the most recent trigger signature is recoverable on restart so that
    dedup logic still works across crashes.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.db import review_state


@pytest.mark.asyncio
async def test_review_state_starts_at_zero(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        state = await review_state.get(conn, "iss-1")
        assert state.iteration == 0
        assert state.last_trigger_signature == ""
        assert state.ci_fetch_failures == 0
        assert state.pr_number is None
        assert state.pr_url == ""
        assert state.github_repo == ""
        assert state.issue_label == ""
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_state_bump_iteration_persists(tmp_path: Path) -> None:
    db_path = tmp_path / "s.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await review_state.bump_iteration(conn, "iss-1")
        await review_state.bump_iteration(conn, "iss-1")
        await review_state.bump_iteration(conn, "iss-1")
        s = await review_state.get(conn, "iss-1")
        assert s.iteration == 3
    finally:
        await conn.close()

    # Reopen and confirm persistence.
    conn2 = await db.connect(db_path)
    try:
        s = await review_state.get(conn2, "iss-1")
        assert s.iteration == 3
    finally:
        await conn2.close()


@pytest.mark.asyncio
async def test_review_state_bump_iteration_records_transition(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await conn.execute(
            """
            INSERT INTO review_state (
                issue_id, iteration, last_trigger_signature, ci_fetch_failures,
                pr_number, pr_url, github_repo, issue_label, codex_lgtm_comment_id
            )
            VALUES ('iss-1', 1, '', 0, NULL, '', '', '', '')
            """
        )
        await conn.commit()

        assert await review_state.bump_iteration(conn, "iss-1") == 2

        transitions = await db.state_transitions.list_for_issue(conn, "iss-1")
        assert [(t.table_name, t.field, t.old_value, t.new_value) for t in transitions] == [
            ("review_state", "iteration", "1", "2")
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_state_signature_persists(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await review_state.set_signature(conn, "iss-1", "ci:lint")
        s = await review_state.get(conn, "iss-1")
        assert s.last_trigger_signature == "ci:lint"

        await review_state.set_signature(conn, "iss-1", "codex_inline:abc")
        s = await review_state.get(conn, "iss-1")
        assert s.last_trigger_signature == "codex_inline:abc"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_state_reset_clears_counter(tmp_path: Path) -> None:
    """After a stage transition (e.g. Review → Merge) the counter resets."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await review_state.bump_iteration(conn, "iss-1")
        await review_state.bump_iteration(conn, "iss-1")
        await review_state.set_signature(conn, "iss-1", "ci:lint")
        await review_state.reset(conn, "iss-1")
        s = await review_state.get(conn, "iss-1")
        assert s.iteration == 0
        assert s.last_trigger_signature == ""
        assert s.ci_fetch_failures == 0
        assert s.pr_number is None
        assert s.pr_url == ""
        assert s.github_repo == ""
        assert s.issue_label == ""
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_state_begin_review_records_pr_metadata(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await review_state.begin_review(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            github_repo="org/repo",
            issue_label="auto",
        )
        s = await review_state.get(conn, "iss-1")
        assert s.iteration == 0
        assert s.last_trigger_signature == ""
        assert s.ci_fetch_failures == 0
        assert s.pr_number == 42
        assert s.pr_url == "https://github.com/org/repo/pull/42"
        assert s.github_repo == "org/repo"
        assert s.issue_label == "auto"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_review_state_ci_fetch_failures_persist_and_reset(tmp_path: Path) -> None:
    db_path = tmp_path / "s.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        assert await review_state.bump_ci_fetch_failures(conn, "iss-1") == 1
        assert await review_state.bump_ci_fetch_failures(conn, "iss-1") == 2
    finally:
        await conn.close()

    conn2 = await db.connect(db_path)
    try:
        s = await review_state.get(conn2, "iss-1")
        assert s.ci_fetch_failures == 2
        await review_state.reset_ci_fetch_failures(conn2, "iss-1")
        s = await review_state.get(conn2, "iss-1")
        assert s.ci_fetch_failures == 0
    finally:
        await conn2.close()
