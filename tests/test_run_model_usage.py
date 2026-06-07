"""DAO for the `run_model_usage` child table.

Rows attribute a run's tokens to (provider, model). `replace_for_run`
rewrites the full set for a run idempotently so re-parsing a growing log
mid-run never double-counts.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from symphony import db
from symphony.agent.model_usage import ModelUsage
from symphony.orchestrator.poll import _parse_local_review_model_usage


async def _seed_run(conn, run_id: str = "r1") -> None:
    await db.issues.upsert(conn, id="i1", identifier="SYM-1", title="t", team_key="SYM")
    await db.runs.create(
        conn,
        id=run_id,
        issue_id="i1",
        stage="implement",
        status="completed",
        pid=None,
        started_at="2026-06-07T10:00:00Z",
    )


@pytest.mark.asyncio
async def test_replace_for_run_inserts_rows(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_run(conn)
        await db.run_model_usage.replace_for_run(
            conn,
            "r1",
            [
                ModelUsage("claude", "claude-opus-4-8", 100, 20, 30, 40),
                ModelUsage("codex", "gpt-5.5", 10, 2, 0, 3),
            ],
        )
        rows = await db.run_model_usage.list_for_run(conn, "r1")
    finally:
        await conn.close()

    by_model = {(r.provider, r.model): r for r in rows}
    assert by_model[("claude", "claude-opus-4-8")].input_tokens == 100
    assert by_model[("claude", "claude-opus-4-8")].cache_read_tokens == 40
    assert by_model[("codex", "gpt-5.5")].output_tokens == 2


@pytest.mark.asyncio
async def test_replace_for_run_is_idempotent(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_run(conn)
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "m", 1, 1, 0, 0)]
        )
        # A later, larger snapshot fully replaces the earlier one.
        await db.run_model_usage.replace_for_run(
            conn, "r1", [ModelUsage("claude", "m", 5, 5, 0, 0)]
        )
        rows = await db.run_model_usage.list_for_run(conn, "r1")
    finally:
        await conn.close()

    assert len(rows) == 1
    assert rows[0].input_tokens == 5


@pytest.mark.asyncio
async def test_run_sum_reconciles_with_runs_tokens(tmp_path: Path) -> None:
    """The per-model split must sum back to the run-level totals."""
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        await _seed_run(conn)
        await db.runs.add_usage(
            conn,
            "r1",
            cost_usd=0.0,
            input_tokens=110,
            output_tokens=22,
            cache_write_tokens=30,
            cache_read_tokens=43,
        )
        await db.run_model_usage.replace_for_run(
            conn,
            "r1",
            [
                ModelUsage("claude", "opus", 100, 20, 30, 40),
                ModelUsage("claude", "haiku", 10, 2, 0, 3),
            ],
        )
        rows = await db.run_model_usage.list_for_run(conn, "r1")
        run = await db.runs.get_with_issue(conn, "r1")
    finally:
        await conn.close()

    assert sum(r.input_tokens for r in rows) == run.run.input_tokens
    assert sum(r.output_tokens for r in rows) == run.run.output_tokens
    assert sum(r.cache_write_tokens for r in rows) == run.run.cache_write_tokens
    assert sum(r.cache_read_tokens for r in rows) == run.run.cache_read_tokens


def test_parse_local_review_role_files_by_model(tmp_path: Path) -> None:
    """fix-* transcripts attribute to the implementer model, review-* to the
    reviewer model; Claude's `modelUsage` wins over the passed codex model."""
    (tmp_path / "fix-1.out.log").write_text(
        json.dumps(
            {
                "type": "result",
                "modelUsage": {
                    "claude-opus-4-8": {
                        "inputTokens": 100,
                        "outputTokens": 20,
                        "cacheCreationInputTokens": 0,
                        "cacheReadInputTokens": 0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "review-1.out.log").write_text(
        "\n".join(
            [
                json.dumps({"type": "thread.started"}),
                json.dumps(
                    {
                        "type": "token_count",
                        "info": {
                            "total_token_usage": {
                                "input_tokens": 40,
                                "output_tokens": 8,
                            }
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    # An unrelated file is ignored (no fix-/review- prefix).
    (tmp_path / "notes.out.log").write_text("garbage", encoding="utf-8")

    usages = _parse_local_review_model_usage(
        tmp_path,
        implementer_codex_model="gpt-5.1-codex",
        reviewer_codex_model="gpt-5.5",
    )
    assert set(usages) == {
        ModelUsage("claude", "claude-opus-4-8", 100, 20, 0, 0),
        ModelUsage("codex", "gpt-5.5", 40, 8, 0, 0),
    }
