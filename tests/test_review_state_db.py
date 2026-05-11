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
    finally:
        await conn.close()
