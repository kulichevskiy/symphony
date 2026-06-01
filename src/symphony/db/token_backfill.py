"""Backfill historical run token telemetry from stream-json stdout logs."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from symphony.agent.process import Usage, parse_event_line

_LOG_SUFFIX = ".log"
_OUT_LOG_SUFFIX = ".out.log"


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0


@dataclass(frozen=True)
class BackfillResult:
    updated: int
    skipped: int


def run_backfill(*, db_path: Path, log_root: Path) -> BackfillResult:
    if not db_path.exists():
        raise FileNotFoundError(f"database not found: {db_path}")
    if not log_root.exists():
        raise FileNotFoundError(f"log root not found: {log_root}")
    if not log_root.is_dir():
        raise NotADirectoryError(f"log root is not a directory: {log_root}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT id, issue_id, stage, started_at
            FROM runs
            ORDER BY started_at ASC, id ASC
            """
        ).fetchall()
        run_ids = {str(row["id"]) for row in rows}
        paths_by_run_id, local_review_paths_by_parent = _index_logs(
            log_root=log_root,
            run_ids=run_ids,
        )

        updated = 0
        skipped = 0
        with conn:
            for row in rows:
                run_id = str(row["id"])
                paths = _paths_for_row(
                    conn=conn,
                    row=row,
                    paths_by_run_id=paths_by_run_id,
                    local_review_paths_by_parent=local_review_paths_by_parent,
                )
                if not paths:
                    skipped += 1
                    continue

                usage = _read_usage(paths)
                if usage is None:
                    skipped += 1
                    continue

                cur = conn.execute(
                    """
                    UPDATE runs
                       SET input_tokens = ?,
                           output_tokens = ?,
                           cache_write_tokens = ?,
                           cache_read_tokens = ?
                     WHERE id = ?
                    """,
                    (
                        usage.input_tokens,
                        usage.output_tokens,
                        usage.cache_write_tokens,
                        usage.cache_read_tokens,
                        run_id,
                    ),
                )
                updated += cur.rowcount
    finally:
        conn.close()

    return BackfillResult(updated=updated, skipped=skipped)


def _index_logs(
    *, log_root: Path, run_ids: set[str]
) -> tuple[dict[str, tuple[Path, ...]], dict[str, tuple[Path, ...]]]:
    by_run: dict[str, list[Path]] = {}
    local_review_by_parent: dict[str, list[Path]] = {}
    for path in sorted(log_root.rglob(f"*{_LOG_SUFFIX}")):
        if not path.is_file():
            continue
        relative_parts = path.relative_to(log_root).parts
        if (
            len(relative_parts) >= 3
            and relative_parts[0] == "local_review"
            and relative_parts[1]
        ):
            if path.name.endswith(_OUT_LOG_SUFFIX):
                local_review_by_parent.setdefault(relative_parts[1], []).append(path)
            continue

        for candidate in _candidate_run_ids(path):
            if candidate in run_ids:
                by_run.setdefault(candidate, []).append(path)
                break

    return (
        {run_id: tuple(paths) for run_id, paths in by_run.items()},
        {run_id: tuple(paths) for run_id, paths in local_review_by_parent.items()},
    )


def _candidate_run_ids(path: Path) -> tuple[str, ...]:
    candidates: list[str] = []
    name = path.name
    if name.endswith(_OUT_LOG_SUFFIX):
        candidates.append(name[: -len(_OUT_LOG_SUFFIX)])
    if name.endswith(_LOG_SUFFIX):
        candidates.append(name[: -len(_LOG_SUFFIX)])
    if path.parent.name and path.parent.name != "logs":
        candidates.append(path.parent.name)
    return tuple(dict.fromkeys(candidate for candidate in candidates if candidate))


def _paths_for_row(
    *,
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    paths_by_run_id: dict[str, tuple[Path, ...]],
    local_review_paths_by_parent: dict[str, tuple[Path, ...]],
) -> tuple[Path, ...]:
    run_id = str(row["id"])
    if str(row["stage"]) == "local_review":
        parent_run_id = _local_review_parent_run_id(conn=conn, row=row)
        if parent_run_id:
            paths = local_review_paths_by_parent.get(parent_run_id)
            if paths:
                return paths
    return paths_by_run_id.get(run_id, ())


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


def _read_usage(paths: tuple[Path, ...]) -> TokenUsage | None:
    total = TokenUsage()
    found = False
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return None
        for line in lines:
            usage = _usage_from_line(line)
            if usage is None:
                continue
            total = _sum_usage(total, usage)
            found = True
    if not found:
        return None
    return total


def _usage_from_line(line: str) -> Usage | None:
    text = line.strip()
    if not text.startswith("{"):
        return None
    return parse_event_line(text)


def _sum_usage(left: TokenUsage, right: Usage) -> TokenUsage:
    return TokenUsage(
        input_tokens=left.input_tokens + right.input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
        cache_write_tokens=left.cache_write_tokens + right.cache_write_tokens,
        cache_read_tokens=left.cache_read_tokens + right.cache_read_tokens,
    )
