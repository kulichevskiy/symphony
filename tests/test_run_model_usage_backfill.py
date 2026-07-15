from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner

from symphony import db
from symphony.cli import main
from symphony.db.token_backfill import CodexModels, run_model_usage_backfill

_BINDING_STATES = """
    linear_states:
      ready: Todo
      in_progress: In Progress
      code_review: In Review
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
"""


async def _seed_run(
    db_path: Path,
    *,
    run_id: str,
    issue_id: str | None = None,
    team_key: str = "ENG",
    stage: str = "implement",
    status: str = "completed",
    started_at: str = "2026-05-31T00:00:00+00:00",
    tokens: tuple[int, int, int, int] = (0, 0, 0, 0),
) -> None:
    storage_issue_id = issue_id or f"iss-{run_id}"
    conn = await db.connect(db_path)
    try:
        await db.issues.upsert(
            conn,
            id=storage_issue_id,
            identifier=f"{team_key}-{storage_issue_id}",
            title="test",
            team_key=team_key,
        )
        await db.runs.create(
            conn,
            id=run_id,
            issue_id=storage_issue_id,
            stage=stage,
            status="running",
            pid=None,
            started_at=started_at,
        )
        await conn.execute(
            """
            UPDATE runs
               SET status = ?,
                   ended_at = ?,
                   input_tokens = ?,
                   output_tokens = ?,
                   cache_write_tokens = ?,
                   cache_read_tokens = ?
             WHERE id = ?
            """,
            (
                status,
                "2026-05-31T00:01:00+00:00",
                *tokens,
                run_id,
            ),
        )
        await conn.commit()
    finally:
        await conn.close()


def _write_log(path: Path, *events: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        event if isinstance(event, str) else json.dumps(event, separators=(",", ":"))
        for event in events
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _model_rows(db_path: Path) -> dict[str, dict[tuple[str, str], tuple[int, ...]]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT run_id, provider, model,
                   input_tokens, output_tokens,
                   cache_write_tokens, cache_read_tokens
            FROM run_model_usage
            ORDER BY run_id, provider, model
            """
        ).fetchall()
    finally:
        conn.close()
    out: dict[str, dict[tuple[str, str], tuple[int, ...]]] = {}
    for row in rows:
        out.setdefault(str(row["run_id"]), {})[(str(row["provider"]), str(row["model"]))] = (
            int(row["input_tokens"]),
            int(row["output_tokens"]),
            int(row["cache_write_tokens"]),
            int(row["cache_read_tokens"]),
        )
    return out


@pytest.mark.asyncio
async def test_backfill_claude_run_matches_run_totals_and_is_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(
        db_path,
        run_id="implement-run",
        tokens=(150, 70, 35, 27),
    )
    _write_log(
        log_root / "implement-run.log",
        "not json",
        {
            "type": "result",
            "modelUsage": {
                "claude-opus-4-8": {
                    "inputTokens": 100,
                    "outputTokens": 50,
                    "cacheCreationInputTokens": 30,
                    "cacheReadInputTokens": 20,
                },
                "claude-haiku-4-5": {
                    "inputTokens": 50,
                    "outputTokens": 20,
                    "cacheCreationInputTokens": 5,
                    "cacheReadInputTokens": 7,
                },
            },
        },
    )

    first = run_model_usage_backfill(db_path=db_path, log_root=log_root)
    rows = _model_rows(db_path)
    assert first.updated == 1
    assert rows["implement-run"] == {
        ("claude", "claude-opus-4-8"): (100, 50, 30, 20),
        ("claude", "claude-haiku-4-5"): (50, 20, 5, 7),
    }
    # Per-model sums reconcile with the run-level token columns.
    totals = [sum(vals) for vals in zip(*rows["implement-run"].values(), strict=True)]
    assert totals == [150, 70, 35, 27]

    second = run_model_usage_backfill(db_path=db_path, log_root=log_root)
    assert second.updated == 1
    assert _model_rows(db_path) == rows


@pytest.mark.asyncio
async def test_backfill_codex_run_without_binding_is_unknown(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(db_path, run_id="codex-run", tokens=(900, 120, 0, 80))
    _write_log(
        log_root / "codex-run.log",
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 900,
                "output_tokens": 120,
                "cached_input_tokens": 80,
            },
        },
    )

    result = run_model_usage_backfill(db_path=db_path, log_root=log_root)
    assert result.updated == 1
    assert _model_rows(db_path)["codex-run"] == {
        ("codex", "unknown"): (900, 120, 0, 80),
    }


@pytest.mark.asyncio
async def test_backfill_codex_run_resolves_model_from_binding(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(
        db_path,
        run_id="codex-run",
        team_key="ENG",
        tokens=(900, 120, 0, 80),
    )
    _write_log(
        log_root / "codex-run.log",
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 900,
                "output_tokens": 120,
                "cached_input_tokens": 80,
            },
        },
    )

    result = run_model_usage_backfill(
        db_path=db_path,
        log_root=log_root,
        codex_models_by_team={
            "ENG": CodexModels(implementer="gpt-5-codex", reviewer="gpt-5"),
        },
    )
    assert result.updated == 1
    assert _model_rows(db_path)["codex-run"] == {
        ("codex", "gpt-5-codex"): (900, 120, 0, 80),
    }


@pytest.mark.asyncio
async def test_backfill_local_review_splits_roles_by_binding(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    issue_id = "iss-shared"
    await _seed_run(
        db_path,
        run_id="implement-parent",
        issue_id=issue_id,
        team_key="ENG",
        stage="implement",
        started_at="2026-05-31T00:00:00+00:00",
    )
    await _seed_run(
        db_path,
        run_id="local-review-row",
        issue_id=issue_id,
        team_key="ENG",
        stage="local_review",
        started_at="2026-05-31T00:02:00+00:00",
    )
    review_dir = log_root / "local_review" / "implement-parent"
    # Reviewer is codex; fixer is codex with a different model.
    _write_log(
        review_dir / "fix-0.out.log",
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 300,
                "output_tokens": 40,
                "cached_input_tokens": 13,
            },
        },
    )
    _write_log(
        review_dir / "review-0.out.log",
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 200,
                "output_tokens": 30,
                "cached_input_tokens": 7,
            },
        },
    )

    result = run_model_usage_backfill(
        db_path=db_path,
        log_root=log_root,
        codex_models_by_team={
            "ENG": CodexModels(implementer="gpt-5-codex", fix="gpt-5-codex-fix", reviewer="gpt-5"),
        },
    )
    assert result.updated == 1
    assert _model_rows(db_path)["local-review-row"] == {
        ("codex", "gpt-5-codex-fix"): (300, 40, 0, 13),
        ("codex", "gpt-5"): (200, 30, 0, 7),
    }


@pytest.mark.asyncio
async def test_backfill_review_fix_stage_uses_fix_model_not_implementer(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(
        db_path,
        run_id="review-fix-run",
        team_key="ENG",
        stage="review_fix",
        tokens=(900, 120, 0, 80),
    )
    _write_log(
        log_root / "review-fix-run.log",
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 900,
                "output_tokens": 120,
                "cached_input_tokens": 80,
            },
        },
    )

    result = run_model_usage_backfill(
        db_path=db_path,
        log_root=log_root,
        codex_models_by_team={
            "ENG": CodexModels(implementer="gpt-5-codex", fix="gpt-5-codex-fix"),
        },
    )
    assert result.updated == 1
    assert _model_rows(db_path)["review-fix-run"] == {
        ("codex", "gpt-5-codex-fix"): (900, 120, 0, 80),
    }


@pytest.mark.asyncio
async def test_backfill_verify_stage_uses_fix_model_not_implementer(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(
        db_path,
        run_id="verify-run",
        team_key="ENG",
        stage="verify",
        tokens=(900, 120, 0, 80),
    )
    _write_log(
        log_root / "verify-run.log",
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 900,
                "output_tokens": 120,
                "cached_input_tokens": 80,
            },
        },
    )

    result = run_model_usage_backfill(
        db_path=db_path,
        log_root=log_root,
        codex_models_by_team={
            "ENG": CodexModels(implementer="gpt-5-codex", fix="gpt-5-codex-fix"),
        },
    )
    assert result.updated == 1
    assert _model_rows(db_path)["verify-run"] == {
        ("codex", "gpt-5-codex-fix"): (900, 120, 0, 80),
    }


@pytest.mark.asyncio
async def test_backfill_acceptance_fix_stage_uses_accept_model(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(
        db_path,
        run_id="acceptance-fix-run",
        team_key="ENG",
        stage="acceptance_fix",
        tokens=(900, 120, 0, 80),
    )
    _write_log(
        log_root / "acceptance-fix-run.log",
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 900,
                "output_tokens": 120,
                "cached_input_tokens": 80,
            },
        },
    )

    result = run_model_usage_backfill(
        db_path=db_path,
        log_root=log_root,
        codex_models_by_team={
            "ENG": CodexModels(implementer="gpt-5-codex", accept="gpt-5-codex-accept"),
        },
    )
    assert result.updated == 1
    assert _model_rows(db_path)["acceptance-fix-run"] == {
        ("codex", "gpt-5-codex-accept"): (900, 120, 0, 80),
    }


@pytest.mark.asyncio
async def test_backfill_local_review_verify_uses_verifier_model_not_reviewer(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    issue_id = "iss-shared"
    await _seed_run(
        db_path,
        run_id="implement-parent",
        issue_id=issue_id,
        team_key="ENG",
        stage="implement",
        started_at="2026-05-31T00:00:00+00:00",
    )
    await _seed_run(
        db_path,
        run_id="local-review-row",
        issue_id=issue_id,
        team_key="ENG",
        stage="local_review",
        started_at="2026-05-31T00:02:00+00:00",
    )
    review_dir = log_root / "local_review" / "implement-parent"
    # review_find and review_verify are configured to different Codex models.
    _write_log(
        review_dir / "review-0.out.log",
        {
            "type": "turn.completed",
            "usage": {"input_tokens": 200, "output_tokens": 30, "cached_input_tokens": 7},
        },
    )
    _write_log(
        review_dir / "review-0-verify.out.log",
        {
            "type": "turn.completed",
            "usage": {"input_tokens": 100, "output_tokens": 15, "cached_input_tokens": 3},
        },
    )

    result = run_model_usage_backfill(
        db_path=db_path,
        log_root=log_root,
        codex_models_by_team={
            "ENG": CodexModels(reviewer="gpt-5", verifier="gpt-5-codex-verify"),
        },
    )
    assert result.updated == 1
    assert _model_rows(db_path)["local-review-row"] == {
        ("codex", "gpt-5"): (200, 30, 0, 7),
        ("codex", "gpt-5-codex-verify"): (100, 15, 0, 3),
    }


@pytest.mark.asyncio
async def test_backfill_skips_running_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(db_path, run_id="live-run", status="running")
    _write_log(
        log_root / "live-run.log",
        {
            "type": "result",
            "modelUsage": {
                "claude-opus-4-8": {
                    "inputTokens": 100,
                    "outputTokens": 50,
                    "cacheCreationInputTokens": 30,
                    "cacheReadInputTokens": 20,
                },
            },
        },
    )

    result = run_model_usage_backfill(db_path=db_path, log_root=log_root)
    assert result.updated == 0
    assert _model_rows(db_path) == {}


@pytest.mark.asyncio
async def test_cli_backfill_model_usage(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(db_path, run_id="implement-run", tokens=(100, 50, 30, 20))
    _write_log(
        log_root / "implement-run.log",
        {
            "type": "result",
            "modelUsage": {
                "claude-opus-4-8": {
                    "inputTokens": 100,
                    "outputTokens": 50,
                    "cacheCreationInputTokens": 30,
                    "cacheReadInputTokens": 20,
                },
            },
        },
    )

    result = CliRunner().invoke(
        main,
        [
            "runs",
            "backfill-model-usage",
            "--db",
            str(db_path),
            "--log-root",
            str(log_root),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "updated: 1" in result.output
    assert _model_rows(db_path)["implement-run"] == {
        ("claude", "claude-opus-4-8"): (100, 50, 30, 20),
    }


@pytest.mark.asyncio
async def test_cli_backfill_model_usage_config_resolves_fix_and_accept_models(
    tmp_path: Path,
) -> None:
    """`roles.fix`/`roles.accept` pinning a different Codex model than
    `roles.implement` must backfill review-fix/acceptance-fix runs with
    their own role's model, not the implementer's (SYM-192 review)."""
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    await _seed_run(
        db_path,
        run_id="review-fix-run",
        team_key="ENG",
        stage="review_fix",
        tokens=(900, 120, 0, 80),
    )
    await _seed_run(
        db_path,
        run_id="acceptance-fix-run",
        team_key="ENG",
        stage="acceptance_fix",
        tokens=(900, 120, 0, 80),
    )
    for run_id in ("review-fix-run", "acceptance-fix-run"):
        _write_log(
            log_root / f"{run_id}.log",
            {
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 900,
                    "output_tokens": 120,
                    "cached_input_tokens": 80,
                },
            },
        )

    config_path = tmp_path / "cfg.yaml"
    config_path.write_text(
        f"""
roles:
  implement:
    agent: codex
    model: gpt-5.1-codex-max
  fix:
    agent: codex
    model: gpt-5.5
  accept:
    agent: codex
    model: gpt-5.5
repos:
  - linear_team_key: ENG
    github_repo: org/repo
{_BINDING_STATES}
"""
    )

    result = CliRunner().invoke(
        main,
        [
            "runs",
            "backfill-model-usage",
            "--db",
            str(db_path),
            "--log-root",
            str(log_root),
            "--config",
            str(config_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert _model_rows(db_path)["review-fix-run"] == {
        ("codex", "gpt-5.5"): (900, 120, 0, 80),
    }
    assert _model_rows(db_path)["acceptance-fix-run"] == {
        ("codex", "gpt-5.5"): (900, 120, 0, 80),
    }
