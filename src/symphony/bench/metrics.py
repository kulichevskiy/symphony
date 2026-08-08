from __future__ import annotations

import asyncio
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

import aiosqlite

from ..tokens import effective_tokens

_LOCAL_REVIEW_MARKERS = (
    "<<<VERDICT:APPROVED>>>",
    "<<<VERDICT:CHANGES_REQUESTED>>>",
)
_LOCAL_FINDING_RE = re.compile(r"(?m)^[ \t]{0,3}[-*][ \t]+(.+)$")
_LOCAL_SEVERITY_RE = re.compile(
    r"^\*{0,2}`?\[(Critical|Major|Minor)\]", re.IGNORECASE
)
_LOCAL_AUXILIARY_FILE_RE = re.compile(r"^review-\d+-(?:find|spec|bug)(?:-attempt-\d+)?\.last\.txt$")


async def snapshot_candidate(db_path: Path, log_root: Path | None = None) -> dict[str, object]:
    """Read comparable metrics with this frozen harness engine, never candidate code."""
    uri = await asyncio.to_thread(lambda: f"{db_path.resolve().as_uri()}?mode=ro")
    conn = await aiosqlite.connect(uri, uri=True)
    conn.row_factory = aiosqlite.Row
    try:
        await conn.execute("PRAGMA query_only = ON")
        cursor = await conn.execute(
            """
            SELECT id, stage, status, started_at, ended_at, cost_usd, input_tokens, output_tokens,
                   cache_write_tokens, cache_read_tokens
            FROM runs
            ORDER BY started_at, id
            """
        )
        rows = await cursor.fetchall()
        review_cursor = await conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM state_transitions
            WHERE table_name = 'review_state'
              AND field = 'codex_review_requested_at'
              AND COALESCE(new_value, '') <> ''
            """
        )
        review_row = await review_cursor.fetchone()
        model_usage_cursor = await conn.execute(
            """
            SELECT run_id, provider,
                   SUM(input_tokens) AS input_tokens,
                   SUM(output_tokens) AS output_tokens,
                   SUM(cache_write_tokens) AS cache_write_tokens,
                   SUM(cache_read_tokens) AS cache_read_tokens
            FROM run_model_usage
            GROUP BY run_id, provider
            ORDER BY run_id, provider
            """
        )
        model_usage_rows = await model_usage_cursor.fetchall()
    finally:
        await conn.close()

    statuses = Counter(str(row["status"]) for row in rows)
    nested_stages = {"review", "local_review", "local_review_fix"}
    active_seconds = 0.0
    for row in rows:
        if row["stage"] in nested_stages or row["ended_at"] is None:
            continue
        started = datetime.fromisoformat(str(row["started_at"]))
        ended = datetime.fromisoformat(str(row["ended_at"]))
        active_seconds += max(0.0, (ended - started).total_seconds())
    tokens = sum(
        effective_tokens(
            int(row["input_tokens"]),
            int(row["output_tokens"]),
            int(row["cache_write_tokens"]),
            int(row["cache_read_tokens"]),
        )
        for row in rows
    )
    local_metrics = (
        await asyncio.to_thread(local_review_metrics, log_root) if log_root is not None else {}
    )
    input_tokens = sum(int(row["input_tokens"]) for row in rows)
    output_tokens = sum(int(row["output_tokens"]) for row in rows)
    cache_write_tokens = sum(int(row["cache_write_tokens"]) for row in rows)
    cache_read_tokens = sum(int(row["cache_read_tokens"]) for row in rows)
    token_keys = (
        "input_tokens",
        "output_tokens",
        "cache_write_tokens",
        "cache_read_tokens",
    )
    model_totals_by_run: dict[str, list[int]] = {}
    for row in model_usage_rows:
        totals = model_totals_by_run.setdefault(str(row["run_id"]), [0, 0, 0, 0])
        for index, key in enumerate(token_keys):
            totals[index] += int(row[key])
    run_ids = {str(row["id"]) for row in rows}
    unreconciled = set(model_totals_by_run) - run_ids
    for row in rows:
        run_id = str(row["id"])
        expected = tuple(int(row[key]) for key in token_keys)
        actual = tuple(model_totals_by_run.get(run_id, [0, 0, 0, 0]))
        if actual != expected:
            unreconciled.add(run_id)
    raw_tokens: int | None = None
    if not unreconciled:
        raw_tokens = sum(
            int(row["input_tokens"])
            + int(row["output_tokens"])
            + (
                0
                if row["provider"] == "codex"
                else int(row["cache_write_tokens"]) + int(row["cache_read_tokens"])
            )
            for row in model_usage_rows
        )
    metrics: dict[str, object] = {
        "active_agent_seconds": active_seconds,
        "agent_launches": (
            sum(1 for row in rows if row["stage"] not in nested_stages)
            + int(local_metrics.get("local_review_agent_launches", 0))
        ),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_write_tokens": cache_write_tokens,
        "cache_read_tokens": cache_read_tokens,
        "raw_tokens": raw_tokens,
        "token_metrics_unavailable": raw_tokens is None,
        "cost_usd": sum(float(row["cost_usd"]) for row in rows),
        "effective_tokens": tokens,
        "remote_review_state_transitions": (
            int(review_row["total"]) if review_row is not None else 0
        ),
        "runs_by_status": dict(sorted(statuses.items())),
        **local_metrics,
    }
    return metrics


def local_review_metrics(log_root: Path | None) -> dict[str, int]:
    """Count final local-review verdicts and their explicitly graded findings."""
    counts = Counter[str]()
    review_root = log_root / "local_review" if log_root is not None else None
    if review_root is not None and review_root.is_dir():
        counts["agent_launches"] = sum(
            1
            for pattern in ("*/review-*.out.log", "*/fix-*.out.log")
            for _path in review_root.glob(pattern)
        )
        for path in review_root.glob("*/review-*.last.txt"):
            if _LOCAL_AUXILIARY_FILE_RE.fullmatch(path.name):
                continue
            try:
                message = path.read_text(encoding="utf-8")
            except OSError:
                continue
            markers = [(message.rfind(marker), marker) for marker in _LOCAL_REVIEW_MARKERS]
            marker_at, marker = max(markers)
            if marker_at < 0:
                counts["unparseable_rounds"] += 1
                continue
            counts["rounds"] += 1
            if marker != "<<<VERDICT:CHANGES_REQUESTED>>>":
                continue
            findings_at = message.lower().rfind("## findings", 0, marker_at)
            body_at = findings_at + len("## findings") if findings_at >= 0 else 0
            for match in _LOCAL_FINDING_RE.finditer(message[body_at:marker_at]):
                counts["findings"] += 1
                severity = _LOCAL_SEVERITY_RE.match(match.group(1).strip())
                key = severity.group(1).lower() if severity is not None else "unclassified"
                counts[key] += 1
    return {
        "local_review_agent_launches": counts["agent_launches"],
        "local_review_rounds": counts["rounds"],
        "local_review_unparseable_rounds": counts["unparseable_rounds"],
        "local_review_findings": counts["findings"],
        "local_review_critical": counts["critical"],
        "local_review_major": counts["major"],
        "local_review_minor": counts["minor"],
        "local_review_unclassified": counts["unclassified"],
    }
