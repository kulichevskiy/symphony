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
    outside the transaction's reach is recoverable by copying one file back;
  * a DB that predates this runner (schema present, but no `schema_version` —
    older code built it from a monolithic `schema.sql`) is *adopted*: the
    baseline is recorded as applied rather than replayed, and later files apply
    on top. Without this, first boot on such a DB crashed re-running the
    baseline ("table repos already exists"). Adoption requires *every* baseline
    object (tables and indexes) to be present; a legacy DB missing any is
    refused (an operator-facing error) rather than stamped at head with a gap.

Migration files: `NNN_name.sql` (plain SQL, split on complete statements) or
`NNN_name.py` (data-move escape hatch exposing `async def migrate(conn)`;
no commits inside — the runner owns the transaction). The version is the
leading integer; applied versions are recorded in `schema_version`.

Foreign-key enforcement is per-connection in SQLite, so `connect()` issues
`PRAGMA foreign_keys = ON` rather than embedding it in a migration.
"""

from __future__ import annotations

import asyncio
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
    all_migrations = _discover(migrations_dir)
    head = all_migrations[-1][0] if all_migrations else 0

    # Everything — the version-table DDL, the version read, the downgrade
    # guard, and the no-op decision — happens under one write lock. A boot
    # that peeks at the version outside the lock can race a deploy-overlap
    # migrator: two daemons double-create the table on a brand-new DB, or an
    # older image reads a stale pre-migration version, takes the no-op path,
    # and boots onto a schema a newer image commits moments later.
    await conn.execute("BEGIN IMMEDIATE")
    try:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version ("
            "  version    INTEGER PRIMARY KEY,"
            "  name       TEXT NOT NULL,"
            "  applied_at TEXT NOT NULL"
            ")"
        )
        current = await _current_version(conn)
        baseline = all_migrations[0] if all_migrations else None
        if current == 0 and baseline is not None and await _should_adopt_baseline(conn, baseline):
            # A DB created before this runner existed (older code applied a
            # monolithic schema.sql, with no `schema_version` table). Its schema
            # already matches the baseline, so *record* the baseline version
            # instead of replaying its DDL — re-running it would collide
            # ("table repos already exists"). Later files apply normally on top.
            current = await _adopt_baseline(conn, baseline)
        _reject_downgrade(current, head)
        pending = [(v, p) for v, p in all_migrations if v > current]
        if not pending:
            await conn.commit()  # keep the version table on a fresh DB
            return
        target = pending[-1][0]
        # Belt-and-suspenders next to the surrounding transaction:
        # restore-by-copy covers anything a rollback can't (e.g. a future
        # non-transactional step). Taken through SQLite's backup API, not a
        # file copy — in WAL mode committed data can still live in `-wal`.
        # Taken *under the write lock* so an overlapping old daemon can't
        # commit rows between the backup and the migration it protects.
        # Skipped on first boot — nothing to lose yet.
        if db_path is not None and current > 0:
            backup = db_path.with_name(f"{db_path.name}.bak-{target:03d}")
            await asyncio.to_thread(_backup_db, str(db_path), str(backup))
            log.info("DB backed up to %s before migrating v%03d -> v%03d", backup, current, target)
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


# Named schema objects a migration creates: CREATE [UNIQUE] TABLE|INDEX|VIEW|
# TRIGGER [IF NOT EXISTS] <name>. Indexes matter as much as tables — a missing
# baseline UNIQUE index breaks the ON CONFLICT target it backs.
_CREATE_OBJECT_RE = re.compile(
    r"(?im)^\s*CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|VIEW|TRIGGER)\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?[\"'`\[]?(\w+)"
)


async def _application_tables(conn: aiosqlite.Connection) -> set[str]:
    """Table names the DB carries, excluding SQLite internals and the runner's
    own `schema_version`."""
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
        " AND name NOT LIKE 'sqlite_%' AND name != 'schema_version'"
    )
    return {row[0] for row in await cur.fetchall()}


async def _schema_object_names(conn: aiosqlite.Connection) -> set[str]:
    """Every named object the DB carries (tables, indexes, views, triggers),
    excluding SQLite internals (`sqlite_autoindex_*` for PK/UNIQUE constraints)
    and the runner's own `schema_version`."""
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'index', 'view', 'trigger')"
        " AND name NOT LIKE 'sqlite_%' AND name != 'schema_version'"
    )
    return {row[0] for row in await cur.fetchall()}


def _baseline_object_names(baseline_path: Path) -> set[str]:
    """Every object the baseline migration creates (tables *and* indexes), read
    statically from its SQL — the invariant a pre-versioning DB must satisfy in
    full to be adopted. A `.py` baseline can't be read this way; return empty
    (validation skipped)."""
    if baseline_path.suffix != ".sql":
        return set()
    return set(_CREATE_OBJECT_RE.findall(baseline_path.read_text(encoding="utf-8")))


async def _should_adopt_baseline(conn: aiosqlite.Connection, baseline: tuple[int, Path]) -> bool:
    """Whether a version-0 DB should be adopted at baseline rather than have the
    baseline replayed. True only for a *complete* pre-versioning DB — one that
    predates this runner (older code built it from a monolithic `schema.sql`,
    leaving no `schema_version`) and carries every baseline object.

    A DB with no application tables is genuinely fresh → False (replay baseline).
    A DB with some tables but missing baseline objects (a table *or* an index —
    e.g. the UNIQUE index an `ON CONFLICT` target needs) is an inconsistent
    legacy state: adopting it would stamp `schema_version` at head while an
    object the app relies on is absent (a runtime crash later), so refuse to
    boot with an operator-facing error instead."""
    if not await _application_tables(conn):
        return False
    missing = _baseline_object_names(baseline[1]) - await _schema_object_names(conn)
    if missing:
        raise RuntimeError(
            "database carries application tables but has no schema_version and is "
            f"missing baseline objects {sorted(missing)} — an unexpected "
            "pre-versioning state this runner won't silently adopt. Restore a "
            "known-good backup or migrate the DB to the full baseline manually."
        )
    return True


async def _adopt_baseline(conn: aiosqlite.Connection, baseline: tuple[int, Path]) -> int:
    """Record the baseline migration as already applied without running its DDL,
    adopting a pre-versioning DB into the runner. Returns the baseline version."""
    version, path = baseline
    await conn.execute(
        "INSERT INTO schema_version (version, name, applied_at)"
        " VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
        (version, path.stem),
    )
    log.info("adopted pre-versioning DB at baseline v%03d (%s)", version, path.stem)
    return version


def _backup_db(src_path: str, dest_path: str) -> None:
    """Snapshot `src_path` into `dest_path` via SQLite's backup API on an
    independent read connection (WAL-safe, unlike a bare file copy). Runs in a
    thread; the caller holds the migration write lock, so no other writer can
    slip rows in between the backup and the migration it protects. (Not
    `aiosqlite`'s `.backup()` on the migrating connection — backing up a
    connection with an open transaction crashes.)"""
    src = sqlite3.connect(src_path)
    try:
        dest = sqlite3.connect(dest_path)
        try:
            src.backup(dest)
        finally:
            dest.close()
    finally:
        src.close()


def _reject_downgrade(current: int, head: int) -> None:
    """A DB migrated past this image's migration set means the deployment was
    rolled back to an older image. Old code running on a newer schema corrupts
    silently — refuse to boot instead."""
    if current > head:
        raise RuntimeError(
            f"the database is at schema version {current:03d}, newer than this "
            f"build's latest migration {head:03d} — the deployment looks rolled "
            "back to an older image. Deploy an image at or above the DB's "
            "version (or restore the matching state.sqlite backup)."
        )


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
