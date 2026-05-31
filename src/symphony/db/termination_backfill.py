"""Backfill historical run termination telemetry from per-run logs."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .runs import (
    NEEDS_APPROVAL_STATUS,
    TERMINAL_NON_SUCCESS_STATUSES,
    TERMINATION_DETAIL_MAX_BYTES,
    TERMINATION_DETAIL_MAX_LINES,
    _truncate_termination_detail,
)

BACKFILL_PREFIX = "[backfill] "

_MONITOR_STAGES = frozenset({"review", "local_review"})
_LOCAL_REVIEW_LOG_RE = re.compile(r"^(review|fix)-(\d+)\.(out|err)\.log$")
_ERROR_EVENT_TYPES = frozenset({"turn.failed", "error"})
_TRANSCRIPT_EVENT_TYPES = frozenset(
    {
        "assistant",
        "agent_message",
        "item.completed",
        "item.started",
        "result",
        "token_count",
        "turn.completed",
        "turn.failed",
        "turn.started",
        "user_message",
    }
)


@dataclass(frozen=True)
class TerminationClassification:
    kind: str
    detail: str


@dataclass(frozen=True)
class BackfillResult:
    updated: int
    aggregate: tuple[tuple[str, int], ...]


def classify_log_termination(*, log_path: Path, stage: str) -> TerminationClassification:
    if not log_path.exists():
        return TerminationClassification(
            kind="unknown",
            detail=_backfill_detail(f"missing log: {log_path}"),
        )

    try:
        raw = _read_log_text(log_path)
    except OSError as exc:
        return TerminationClassification(
            kind="unknown",
            detail=_backfill_detail(f"could not read log: {type(exc).__name__}: {exc}"),
        )

    events = _json_events(raw)
    last_event = events[-1] if events else None
    last_type = _event_type(last_event)
    has_transcript = _has_agent_transcript(events, raw)

    error_event = _last_error_event(events)
    if error_event is not None:
        return TerminationClassification(
            kind="agent_nonzero_exit",
            detail=_backfill_detail(_error_event_detail(error_event), raw=raw),
        )

    if last_type == "turn.completed":
        return TerminationClassification(
            kind="agent_nonzero_exit",
            detail=_backfill_detail(
                "last agent event was turn.completed, but run status is terminal non-success",
                raw=raw,
            ),
        )

    if has_transcript:
        return TerminationClassification(
            kind="stall_timeout",
            detail=_backfill_detail("agent transcript ended mid-flight", raw=raw),
        )

    if stage in _MONITOR_STAGES:
        return TerminationClassification(
            kind="monitor_terminated",
            detail=_backfill_detail(
                f"{stage} monitor terminated without agent transcript",
                raw=raw,
            ),
        )

    return TerminationClassification(
        kind="unknown",
        detail=_backfill_detail("no agent transcript found in log", raw=raw),
    )


def run_backfill(*, db_path: Path, log_root: Path) -> BackfillResult:
    if not db_path.exists():
        raise FileNotFoundError(f"database not found: {db_path}")
    if not log_root.exists():
        raise FileNotFoundError(f"log root not found: {log_root}")
    if not log_root.is_dir():
        raise NotADirectoryError(f"log root is not a directory: {log_root}")

    statuses = tuple(sorted(TERMINAL_NON_SUCCESS_STATUSES))
    placeholders = ",".join("?" * len(statuses))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT id, issue_id, stage, status, started_at
            FROM runs
            WHERE status IN ({placeholders})
              AND COALESCE(termination_kind, '') = ''
              AND COALESCE(termination_detail, '') = ''
            ORDER BY started_at ASC, id ASC
            """,
            statuses,
        ).fetchall()

        updated = 0
        with conn:
            for row in rows:
                run_id = str(row["id"])
                classification = _classify_run_termination(
                    conn=conn,
                    row=row,
                    log_root=log_root,
                )
                cur = conn.execute(
                    f"""
                    UPDATE runs
                       SET termination_kind = ?,
                           termination_detail = ?,
                           exit_returncode = NULL
                     WHERE id = ?
                       AND status IN ({placeholders})
                       AND COALESCE(termination_kind, '') = ''
                       AND COALESCE(termination_detail, '') = ''
                    """,
                    (
                        classification.kind,
                        classification.detail,
                        run_id,
                        *statuses,
                    ),
                )
                updated += cur.rowcount

        aggregate = _aggregate_by_kind(conn, statuses=statuses)
    finally:
        conn.close()

    return BackfillResult(updated=updated, aggregate=aggregate)


def _classify_run_termination(
    *, conn: sqlite3.Connection, row: sqlite3.Row, log_root: Path
) -> TerminationClassification:
    status = str(row["status"])
    if status == NEEDS_APPROVAL_STATUS:
        return TerminationClassification(
            kind="awaiting_human_merge",
            detail=_backfill_detail("historical needs_approval operator handoff"),
        )

    stage = str(row["stage"])
    return classify_log_termination(
        log_path=_log_path_for_row(conn=conn, log_root=log_root, row=row),
        stage=stage,
    )


def _log_path_for_row(
    *, conn: sqlite3.Connection, log_root: Path, row: sqlite3.Row
) -> Path:
    run_id = str(row["id"])
    if str(row["stage"]) != "local_review":
        return log_root / f"{run_id}.log"

    parent_run_id = _local_review_parent_run_id(conn=conn, row=row)
    if parent_run_id:
        return log_root / "local_review" / parent_run_id
    return log_root / "local_review" / run_id


def _local_review_parent_run_id(
    *, conn: sqlite3.Connection, row: sqlite3.Row
) -> str | None:
    parent = conn.execute(
        """
        SELECT id
        FROM runs
        WHERE issue_id = ?
          AND stage = 'implement'
          AND started_at <= ?
        ORDER BY started_at DESC, id DESC
        LIMIT 1
        """,
        (row["issue_id"], row["started_at"]),
    ).fetchone()
    if parent is None:
        return None
    return str(parent["id"])


def _read_log_text(log_path: Path) -> str:
    if log_path.is_dir():
        return _read_log_directory(log_path)
    return log_path.read_text(encoding="utf-8", errors="replace")


def _read_log_directory(log_path: Path) -> str:
    parts: list[str] = []
    for child in sorted(
        (
            path
            for path in log_path.iterdir()
            if path.is_file() and path.name.endswith(".log")
        ),
        key=_local_review_log_sort_key,
    ):
        text = child.read_text(encoding="utf-8", errors="replace").rstrip()
        if text:
            parts.append(f"== {child.name} ==\n{text}")
    return "\n".join(parts)


def _local_review_log_sort_key(path: Path) -> tuple[int, int, int, str]:
    match = _LOCAL_REVIEW_LOG_RE.match(path.name)
    if match is None:
        return (1, 0, 0, path.name)
    phase, iteration, stream = match.groups()
    phase_index = int(iteration) * 2 + (0 if phase == "review" else 1)
    stream_index = 0 if stream == "out" else 1
    return (0, phase_index, stream_index, path.name)


def _json_events(raw: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in raw.splitlines():
        text = line.strip()
        if not text.startswith("{"):
            continue
        try:
            event = json.loads(text)
        except (TypeError, ValueError):
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def _event_type(event: dict[str, Any] | None) -> str:
    if event is None:
        return ""
    value = event.get("type")
    return value if isinstance(value, str) else ""


def _has_agent_transcript(events: list[dict[str, Any]], raw: str) -> bool:
    if any(_event_type(event) in _TRANSCRIPT_EVENT_TYPES for event in events):
        return True
    return '"type":"command_execution"' in raw or '"type":"agent_message"' in raw


def _last_error_event(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    for event in reversed(events):
        if _event_type(event) in _ERROR_EVENT_TYPES:
            return event
    return None


def _error_event_detail(event: dict[str, Any]) -> str:
    event_type = _event_type(event) or "error"
    error = event.get("error")
    if isinstance(error, dict):
        for key in ("message", "detail", "details", "reason"):
            value = error.get(key)
            if isinstance(value, str) and value.strip():
                return f"{event_type}: {value.strip()}"
        return f"{event_type}: {json.dumps(error, sort_keys=True)}"
    if isinstance(error, str) and error.strip():
        return f"{event_type}: {error.strip()}"
    for key in ("message", "detail", "details", "reason"):
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            return f"{event_type}: {value.strip()}"
    return event_type


def _backfill_detail(message: str, *, raw: str = "") -> str:
    body = message.strip() or "unknown"
    if raw.strip():
        body = f"{body}\n\nLog tail:\n{raw.rstrip()}"
    max_body_bytes = max(
        0,
        TERMINATION_DETAIL_MAX_BYTES - len(BACKFILL_PREFIX.encode("utf-8")),
    )
    body = _truncate_termination_detail(
        body,
        max_bytes=max_body_bytes,
        max_lines=TERMINATION_DETAIL_MAX_LINES,
    )
    return f"{BACKFILL_PREFIX}{body}"


def _aggregate_by_kind(
    conn: sqlite3.Connection, *, statuses: tuple[str, ...]
) -> tuple[tuple[str, int], ...]:
    placeholders = ",".join("?" * len(statuses))
    rows = conn.execute(
        f"""
        SELECT termination_kind, COUNT(*) AS n
        FROM runs
        WHERE status IN ({placeholders})
          AND COALESCE(termination_kind, '') <> ''
        GROUP BY termination_kind
        ORDER BY n DESC, termination_kind ASC
        """,
        statuses,
    ).fetchall()
    return tuple((str(row["termination_kind"]), int(row["n"])) for row in rows)
