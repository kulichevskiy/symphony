from __future__ import annotations

from pathlib import Path

import pytest

from symphony import db


@pytest.mark.asyncio
async def test_state_transitions_roll_back_with_parent_transaction(
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

        await conn.execute("UPDATE review_state SET iteration = 2 WHERE issue_id = 'iss-1'")
        await db.state_transitions.record_transition(
            conn, "iss-1", "review_state", "iteration", 1, 2
        )
        await conn.rollback()

        state = await db.review_state.get(conn, "iss-1")
        assert state.iteration == 1
        assert await db.state_transitions.list_for_issue(conn, "iss-1") == []
    finally:
        await conn.close()
