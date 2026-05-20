"""Persistence for the Acceptance stage handoff state."""

from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db
from symphony.db import acceptance_state


@pytest.mark.asyncio
async def test_acceptance_state_starts_at_zero(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )

        state = await acceptance_state.get(conn, "iss-1")

        assert state.iteration == 0
        assert state.pr_number is None
        assert state.pr_url == ""
        assert state.mode == "off"
        assert state.preview_url == ""
        assert state.extracted_criteria == ""
        assert state.last_verdict == ""
        assert state.last_artifacts_url == ""
        assert state.infra_retries == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_state_begin_and_verdict_persist(tmp_path: Path) -> None:
    db_path = tmp_path / "s.sqlite"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn, id="iss-1", identifier="ENG-1", title="t", team_key="ENG"
        )
        await acceptance_state.begin_acceptance(
            conn,
            "iss-1",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            mode="code_only",
            preview_url="",
            extracted_criteria="[]",
        )
        await acceptance_state.record_verdict(
            conn,
            "iss-1",
            verdict="pass",
            artifacts_url="https://artifacts.example/acceptance/42",
        )
    finally:
        await conn.close()

    conn2 = await db.connect(db_path)
    try:
        state = await acceptance_state.get(conn2, "iss-1")
        assert state.iteration == 0
        assert state.pr_number == 42
        assert state.pr_url == "https://github.com/org/repo/pull/42"
        assert state.mode == "code_only"
        assert state.preview_url == ""
        assert state.extracted_criteria == "[]"
        assert state.last_verdict == "pass"
        assert state.last_artifacts_url == "https://artifacts.example/acceptance/42"
        assert state.infra_retries == 0
    finally:
        await conn2.close()
