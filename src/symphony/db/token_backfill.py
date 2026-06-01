"""Backfill historical run token telemetry from stream-json stdout logs."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from symphony.agent.process import Usage, parse_event_line
from symphony.db.runs import LIVE_STATUSES

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
        live_statuses = tuple(LIVE_STATUSES)
        live_placeholders = ",".join("?" * len(live_statuses))
        rows = conn.execute(
            f"""
            SELECT id, issue_id, stage, started_at
            FROM runs
            WHERE status NOT IN ({live_placeholders})
            ORDER BY started_at ASC, id ASC
            """,
            live_statuses,
        ).fetchall()
        skipped = int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM runs
                WHERE status IN ({live_placeholders})
                """,
                live_statuses,
            ).fetchone()[0]
        )
        run_ids = {str(row["id"]) for row in rows}
        paths_by_run_id, local_review_paths_by_parent = _index_logs(
            log_root=log_root,
            run_ids=run_ids,
        )

        updated = 0
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

                usage = _read_usage(
                    paths,
                    cumulative_codex_deltas=str(row["stage"]) == "local_review",
                )
                if usage is None:
                    skipped += 1
                    continue

                cur = conn.execute(
                    f"""
                    UPDATE runs
                       SET input_tokens = ?,
                           output_tokens = ?,
                           cache_write_tokens = ?,
                           cache_read_tokens = ?
                     WHERE id = ?
                       AND status NOT IN ({live_placeholders})
                    """,
                    (
                        usage.input_tokens,
                        usage.output_tokens,
                        usage.cache_write_tokens,
                        usage.cache_read_tokens,
                        run_id,
                        *live_statuses,
                    ),
                )
                if cur.rowcount == 0:
                    skipped += 1
                else:
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


def _read_usage(
    paths: tuple[Path, ...], *, cumulative_codex_deltas: bool = False
) -> TokenUsage | None:
    path_usages: list[tuple[Path, Usage]] = []
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return None
        path_usage: Usage | None = None
        # Codex token events are cumulative within one process log; only sum
        # across separate log files.
        for line in lines:
            usage = _usage_from_line(line)
            if usage is None:
                continue
            path_usage = usage
        if path_usage is not None:
            path_usages.append((path, path_usage))
    if not path_usages:
        return None
    if cumulative_codex_deltas:
        return _sum_local_review_usage(path_usages)

    total = TokenUsage()
    for _, usage in path_usages:
        total = _sum_usage(total, _token_usage_from_event(usage))
    return total


def _sum_local_review_usage(path_usages: list[tuple[Path, Usage]]) -> TokenUsage:
    total = TokenUsage()
    codex_watermarks: dict[str, TokenUsage] = {}
    for path, usage in sorted(path_usages, key=lambda item: _local_review_sort_key(item[0])):
        path_usage = _token_usage_from_event(usage)
        if usage.cost_usd > 0:
            total = _sum_usage(total, path_usage)
            continue

        key = _local_review_sequence_key(path)
        watermark = codex_watermarks.get(key, TokenUsage())
        delta = _cumulative_delta(watermark, path_usage)
        codex_watermarks[key] = _max_usage(watermark, path_usage)
        total = _sum_usage(total, delta)
    return total


def _local_review_sort_key(path: Path) -> tuple[str, int, str]:
    stem = _out_log_stem(path)
    sequence = _local_review_sequence_key(path)
    prefix = f"{sequence}-"
    if stem.startswith(prefix):
        suffix = stem[len(prefix) :]
        if suffix.isdecimal():
            return (sequence, int(suffix), stem)
    return (sequence, 0, stem)


def _local_review_sequence_key(path: Path) -> str:
    stem = _out_log_stem(path)
    if stem.startswith("review-"):
        return "review"
    if stem.startswith("fix-"):
        return "fix"
    return stem


def _out_log_stem(path: Path) -> str:
    name = path.name
    if name.endswith(_OUT_LOG_SUFFIX):
        return name[: -len(_OUT_LOG_SUFFIX)]
    if name.endswith(_LOG_SUFFIX):
        return name[: -len(_LOG_SUFFIX)]
    return path.stem


def _usage_from_line(line: str) -> Usage | None:
    text = line.strip()
    if not text.startswith("{"):
        return None
    return parse_event_line(text)


def _token_usage_from_event(usage: Usage) -> TokenUsage:
    return TokenUsage(
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cache_write_tokens=usage.cache_write_tokens,
        cache_read_tokens=usage.cache_read_tokens,
    )


def _sum_usage(left: TokenUsage, right: TokenUsage) -> TokenUsage:
    return TokenUsage(
        input_tokens=left.input_tokens + right.input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
        cache_write_tokens=left.cache_write_tokens + right.cache_write_tokens,
        cache_read_tokens=left.cache_read_tokens + right.cache_read_tokens,
    )


def _cumulative_delta(previous: TokenUsage, current: TokenUsage) -> TokenUsage:
    return TokenUsage(
        input_tokens=max(current.input_tokens - previous.input_tokens, 0),
        output_tokens=max(current.output_tokens - previous.output_tokens, 0),
        cache_write_tokens=max(
            current.cache_write_tokens - previous.cache_write_tokens, 0
        ),
        cache_read_tokens=max(
            current.cache_read_tokens - previous.cache_read_tokens, 0
        ),
    )


def _max_usage(left: TokenUsage, right: TokenUsage) -> TokenUsage:
    return TokenUsage(
        input_tokens=max(left.input_tokens, right.input_tokens),
        output_tokens=max(left.output_tokens, right.output_tokens),
        cache_write_tokens=max(left.cache_write_tokens, right.cache_write_tokens),
        cache_read_tokens=max(left.cache_read_tokens, right.cache_read_tokens),
    )
