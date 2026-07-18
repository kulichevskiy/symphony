"""Versioned schema migrations (Config v2 1/9).

`connect()` opens the DB and brings it to the head schema version by applying
any pending files from `migrations/` in order. The daemon is the sole
migrator — UI pools open plain connections after boot, so they never race DDL.

Boot behavior:

  * `PRAGMA busy_timeout` makes a concurrent opener (deploy overlap: the old
    and new container briefly share the volume) wait for the writer instead of
    failing with `database table is locked`;
  * pending migrations apply inside a single `BEGIN IMMEDIATE` transaction —
    one writer, one commit; a crash mid-migration rolls back cleanly;
  * before an *upgrade* (pending work on an already-versioned DB) the DB file
    is copied aside as `<name>.bak-<target>`, so even a migration failure
    outside the transaction's reach is recoverable by copying one file back.

Migration files: `NNN_name.sql` (plain SQL, split on complete statements) or
`NNN_name.py` (data-move escape hatch exposing `async def migrate(conn)`;
no commits inside — the runner owns the transaction). The version is the
leading integer; applied versions are recorded in `schema_version`.

Foreign-key enforcement is per-connection in SQLite, so `connect()` issues
`PRAGMA foreign_keys = ON` rather than embedding it in a migration.
"""

from __future__ import annotations

import importlib.util
import logging
import re
import sqlite3
from pathlib import Path

import aiosqlite

log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
# Generous: must outlast a slow migration running on the other side of a
# deploy overlap, plus ordinary writer contention.
BUSY_TIMEOUT_MS = 60_000

_MIGRATION_FILE_RE = re.compile(r"^(\d{3,})_\w+\.(sql|py)$")


async def connect(path: Path) -> aiosqlite.Connection:
    """Open (creating if needed) the SQLite database and migrate it to head."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(str(path))
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA foreign_keys = ON")
    # Timeout first: on a DB not yet in WAL mode, the journal-mode switch
    # itself needs the write lock and must wait out a deploy-overlap writer.
    await conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    await conn.execute("PRAGMA journal_mode=WAL")
    await apply_migrations(conn, db_path=path)
    return conn


async def apply_migrations(
    conn: aiosqlite.Connection,
    *,
    db_path: Path | None = None,
    migrations_dir: Path | None = None,
) -> None:
    """Apply every migration newer than the DB's recorded version, in order,
    in one transaction. A no-op when the DB is already at head."""
    migrations_dir = migrations_dir or MIGRATIONS_DIR
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version ("
        "  version    INTEGER PRIMARY KEY,"
        "  name       TEXT NOT NULL,"
        "  applied_at TEXT NOT NULL"
        ")"
    )
    await conn.commit()

    current = await _current_version(conn)
    all_migrations = _discover(migrations_dir)
    pending = [(v, p) for v, p in all_migrations if v > current]
    if not pending:
        return
    target = pending[-1][0]

    # Belt-and-suspenders next to the transaction below: restore-by-copy
    # covers anything a rollback can't (e.g. a future non-transactional
    # step). Taken through SQLite's backup API, not a file copy — in WAL
    # mode committed data can still live in `-wal`, which a bare copy of the
    # main file would miss. Skipped on first boot — nothing to lose yet.
    if db_path is not None and current > 0:
        backup = db_path.with_name(f"{db_path.name}.bak-{target:03d}")
        backup_conn = await aiosqlite.connect(str(backup))
        try:
            await conn.backup(backup_conn)
        finally:
            await backup_conn.close()
        log.info("DB backed up to %s before migrating v%03d -> v%03d", backup, current, target)

    await conn.execute("BEGIN IMMEDIATE")
    try:
        # Two daemons can race to this point during a deploy overlap: both
        # read the same `current` above, the loser waits on the winner's
        # write lock, then must NOT replay the migrations the winner already
        # applied — re-read the version now that we hold the lock.
        current = await _current_version(conn)
        pending = [(v, p) for v, p in all_migrations if v > current]
        if not pending:
            await conn.rollback()
            return
        target = pending[-1][0]
        for version, path in pending:
            if path.suffix == ".sql":
                for statement in _split_statements(path.read_text(encoding="utf-8")):
                    await conn.execute(statement)
            else:
                await _run_python_migration(path, conn)
            await conn.execute(
                "INSERT INTO schema_version (version, name, applied_at)"
                " VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
                (version, path.stem),
            )
        await conn.commit()
    except BaseException:
        await conn.rollback()
        raise
    log.info("schema migrated to v%03d (%d migration(s) applied)", target, len(pending))


async def _current_version(conn: aiosqlite.Connection) -> int:
    cur = await conn.execute("SELECT MAX(version) FROM schema_version")
    row = await cur.fetchone()
    return row[0] if row is not None and row[0] is not None else 0


def _discover(migrations_dir: Path) -> list[tuple[int, Path]]:
    """`(version, path)` for every migration file, ordered. Rejects duplicate
    version numbers — two files claiming one slot is always a merge mistake."""
    found: dict[int, Path] = {}
    for entry in sorted(migrations_dir.iterdir()):
        match = _MIGRATION_FILE_RE.match(entry.name)
        if match is None:
            continue
        version = int(match.group(1))
        if version in found:
            raise RuntimeError(
                f"duplicate migration version {version:03d}: {found[version].name} and {entry.name}"
            )
        found[version] = entry
    return sorted(found.items())


def _split_statements(sql: str) -> list[str]:
    """Split a migration script into individual statements.

    `executescript` would issue an implicit COMMIT and break the runner's
    single transaction, so statements are executed one by one instead.
    Accumulation is semicolon-by-semicolon (not line-by-line, which would glue
    two same-line statements into one un-executable string);
    `sqlite3.complete_statement` handles semicolons inside strings, triggers,
    and comments correctly.
    """
    statements: list[str] = []
    buffer = ""
    for chunk in re.split(r"(;)", sql):
        buffer += chunk
        if chunk == ";" and sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            if _has_effective_sql(statement):
                statements.append(statement)
            buffer = ""
    tail = buffer.strip()
    if tail and _has_effective_sql(tail):
        # Final statement without a trailing semicolon.
        statements.append(tail)
    return statements


def _has_effective_sql(fragment: str) -> bool:
    """Whether `fragment` contains anything besides `--` comments and blank
    lines (executing a comment-only string is an error, not a no-op)."""
    return any(line.strip() and not line.strip().startswith("--") for line in fragment.splitlines())


async def _run_python_migration(path: Path, conn: aiosqlite.Connection) -> None:
    """Load `NNN_name.py` and run its `async def migrate(conn)`. The module
    must not commit — the runner owns the surrounding transaction."""
    spec = importlib.util.spec_from_file_location(f"symphony_migration_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load migration module {path.name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    migrate = getattr(module, "migrate", None)
    if migrate is None:
        raise RuntimeError(f"migration {path.name} defines no `async def migrate(conn)`")
    await migrate(conn)
