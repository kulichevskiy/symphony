#!/usr/bin/env python3
"""Daily symphonyd SQLite backup and log pruning helper."""

from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path("/opt/symphonyd/config.yaml")
DEFAULT_BACKUP_KEEP = 7
DEFAULT_LOG_RETENTION_DAYS = 14


@dataclass(frozen=True)
class MaintenanceResult:
    backup_path: Path
    removed_backups: list[Path]
    removed_logs: list[Path]


def _expand(path: str | Path) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(str(path)))).resolve()


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer, got {raw!r}") from exc


def _load_config_paths(config_path: Path) -> tuple[Path, Path]:
    if not config_path.is_file():
        raise SystemExit(f"config file not found: {config_path}")

    raw = yaml.safe_load(config_path.read_text())
    if not isinstance(raw, dict):
        raise SystemExit(f"config file must contain a YAML mapping: {config_path}")

    try:
        db_path = _expand(raw["db_path"])
        log_root = _expand(raw["log_root"])
    except KeyError as exc:
        raise SystemExit(f"config file is missing required key: {exc.args[0]}") from exc

    return db_path, log_root


def _backup_timestamp(now: datetime) -> str:
    return now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _sqlite_dot_quote(path: Path) -> str:
    escaped = str(path).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def backup_sqlite(db_path: Path, *, now: datetime, sqlite3_bin: str) -> Path:
    if not db_path.is_file():
        raise SystemExit(f"SQLite database not found: {db_path}")

    backup_path = Path(f"{db_path}.bak.{_backup_timestamp(now)}")
    if backup_path.exists():
        raise SystemExit(f"backup path already exists: {backup_path}")

    subprocess.run(
        [
            sqlite3_bin,
            str(db_path),
            f".backup main {_sqlite_dot_quote(backup_path)}",
        ],
        check=True,
    )
    return backup_path


def rotate_backups(db_path: Path, *, keep: int) -> list[Path]:
    if keep < 1:
        raise SystemExit("backup keep count must be at least 1")

    backups = sorted(db_path.parent.glob(f"{db_path.name}.bak.*"), reverse=True)
    removed: list[Path] = []
    for stale in backups[keep:]:
        stale.unlink()
        removed.append(stale)
    return removed


def prune_logs(log_root: Path, *, retention_days: int, now: datetime) -> list[Path]:
    if retention_days < 0:
        raise SystemExit("log retention days must be zero or greater")

    log_root.mkdir(parents=True, exist_ok=True)
    cutoff = now.timestamp() - timedelta(days=retention_days).total_seconds()
    removed: list[Path] = []
    for log_path in sorted(log_root.glob("*.log")):
        if log_path.is_symlink() or not log_path.is_file():
            continue
        if log_path.stat().st_mtime < cutoff:
            log_path.unlink()
            removed.append(log_path)
    return removed


def run_maintenance(
    *,
    db_path: Path,
    log_root: Path,
    backup_keep: int,
    log_retention_days: int,
    sqlite3_bin: str,
    now: datetime | None = None,
) -> MaintenanceResult:
    current_time = now or datetime.now(UTC)
    backup_path = backup_sqlite(db_path, now=current_time, sqlite3_bin=sqlite3_bin)
    removed_backups = rotate_backups(db_path, keep=backup_keep)
    removed_logs = prune_logs(
        log_root, retention_days=log_retention_days, now=current_time
    )
    return MaintenanceResult(
        backup_path=backup_path,
        removed_backups=removed_backups,
        removed_logs=removed_logs,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Back up symphonyd SQLite state and prune old run logs."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(os.environ.get("SYMPHONYD_CONFIG", DEFAULT_CONFIG_PATH)),
        help="Path to config.yaml; ignored for paths passed explicitly.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=os.environ.get("SYMPHONYD_DB_PATH"),
        help="SQLite DB path override.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=os.environ.get("SYMPHONYD_LOG_ROOT"),
        help="Log root override.",
    )
    parser.add_argument(
        "--backup-keep",
        type=int,
        default=_env_int("SYMPHONYD_BACKUP_KEEP", DEFAULT_BACKUP_KEEP),
        help=f"Number of newest backups to keep. Default: {DEFAULT_BACKUP_KEEP}.",
    )
    parser.add_argument(
        "--log-retention-days",
        type=int,
        default=_env_int(
            "SYMPHONYD_LOG_RETENTION_DAYS", DEFAULT_LOG_RETENTION_DAYS
        ),
        help=(
            "Delete *.log files older than this many days. "
            f"Default: {DEFAULT_LOG_RETENTION_DAYS}."
        ),
    )
    parser.add_argument(
        "--sqlite3",
        default=os.environ.get("SQLITE3", "sqlite3"),
        help="sqlite3 executable. Default: sqlite3.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.db_path is None or args.log_root is None:
        config_db_path, config_log_root = _load_config_paths(_expand(args.config))
        db_path = _expand(args.db_path) if args.db_path is not None else config_db_path
        log_root = (
            _expand(args.log_root) if args.log_root is not None else config_log_root
        )
    else:
        db_path = _expand(args.db_path)
        log_root = _expand(args.log_root)

    result = run_maintenance(
        db_path=db_path,
        log_root=log_root,
        backup_keep=args.backup_keep,
        log_retention_days=args.log_retention_days,
        sqlite3_bin=args.sqlite3,
    )
    print(f"created backup: {result.backup_path}")
    print(f"removed backups: {len(result.removed_backups)}")
    print(f"removed logs: {len(result.removed_logs)}")


if __name__ == "__main__":
    main()
