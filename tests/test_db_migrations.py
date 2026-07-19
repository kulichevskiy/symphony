"""Versioned migration runner (Config v2 1/9).

`db.connect` migrates a DB to head at open: fresh DBs get the full baseline,
already-versioned DBs get only the pending files, upgrades are backed up
first, failures roll back, and a concurrent writer makes the runner wait
(busy_timeout) instead of crashing with `database table is locked`.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sqlite3
from pathlib import Path

import aiosqlite
import pytest

from symphony import db
from symphony.db.schema import MIGRATIONS_DIR, apply_migrations, connect


async def _table_names(conn: aiosqlite.Connection) -> set[str]:
    cur = await conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    return {row["name"] for row in await cur.fetchall()}


async def _versions(conn: aiosqlite.Connection) -> list[tuple[int, str]]:
    cur = await conn.execute("SELECT version, name FROM schema_version ORDER BY version")
    return [(row["version"], row["name"]) for row in await cur.fetchall()]


# The real migration set's head version + applied (version, name) rows —
# tests assert against these instead of hardcoding "001" so adding a real
# migration doesn't rewrite the suite.
_REAL = sorted(
    (int(f.name[:3]), f.stem) for f in MIGRATIONS_DIR.iterdir() if f.suffix in (".sql", ".py")
)
_HEAD = _REAL[-1][0]
_NEXT = _HEAD + 1


def _dir_with_baseline(tmp_path: Path) -> Path:
    """A migrations dir seeded with the real migration set, ready for extras."""
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    for f in MIGRATIONS_DIR.iterdir():
        if f.suffix in (".sql", ".py"):
            shutil.copy2(f, migrations / f.name)
    return migrations


async def test_fresh_db_boots_at_head(tmp_path: Path) -> None:
    conn = await connect(tmp_path / "state.sqlite")
    try:
        tables = await _table_names(conn)
        assert {"issues", "runs", "oauth_connections", "config_bindings"} <= tables
        assert await _versions(conn) == _REAL
    finally:
        await conn.close()


async def test_reboot_is_noop_and_makes_no_backup(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite"
    conn = await connect(path)
    await conn.close()
    conn = await connect(path)
    try:
        assert await _versions(conn) == _REAL
    finally:
        await conn.close()
    assert not [name for name in os.listdir(tmp_path) if ".bak-" in name]


async def test_pending_migration_applies_and_backs_up(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite"
    conn = await connect(path)  # v1
    await conn.close()

    migrations = _dir_with_baseline(tmp_path)
    (migrations / f"{_NEXT:03d}_add_widgets.sql").write_text(
        "-- adds the widgets table\n"
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY, note TEXT NOT NULL DEFAULT '');\n"
    )
    conn = await aiosqlite.connect(str(path))
    conn.row_factory = aiosqlite.Row
    try:
        await apply_migrations(conn, db_path=path, migrations_dir=migrations)
        assert "widgets" in await _table_names(conn)
        assert await _versions(conn) == [*_REAL, (_NEXT, f"{_NEXT:03d}_add_widgets")]
    finally:
        await conn.close()
    assert (tmp_path / f"state.sqlite.bak-{_NEXT:03d}").exists()


async def test_failing_migration_rolls_back(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite"
    conn = await connect(path)  # v1
    await conn.close()

    migrations = _dir_with_baseline(tmp_path)
    (migrations / f"{_NEXT:03d}_broken.sql").write_text(
        "CREATE TABLE almost (id INTEGER PRIMARY KEY);\n"
        "CREATE TABLE broken (id INTEGER PRIMARY KEY, CONSTRAINT nonsense;\n"
    )
    conn = await aiosqlite.connect(str(path))
    conn.row_factory = aiosqlite.Row
    try:
        with pytest.raises(sqlite3.OperationalError):
            await apply_migrations(conn, db_path=path, migrations_dir=migrations)
        # Rolled back: the partial table is gone and the version untouched.
        assert "almost" not in await _table_names(conn)
        assert await _versions(conn) == _REAL
    finally:
        await conn.close()
    # The pre-upgrade backup exists for restore-by-copy recovery.
    assert (tmp_path / f"state.sqlite.bak-{_NEXT:03d}").exists()


async def test_python_migration_escape_hatch(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite"
    conn = await connect(path)  # v1
    await conn.close()

    migrations = _dir_with_baseline(tmp_path)
    (migrations / f"{_NEXT:03d}_seed_repo.py").write_text(
        "async def migrate(conn):\n"
        "    await conn.execute(\n"
        "        \"INSERT INTO repos (linear_team_key, github_repo) VALUES ('T', 'o/r')\"\n"
        "    )\n"
    )
    conn = await aiosqlite.connect(str(path))
    conn.row_factory = aiosqlite.Row
    try:
        await apply_migrations(conn, db_path=path, migrations_dir=migrations)
        cur = await conn.execute("SELECT github_repo FROM repos WHERE linear_team_key = 'T'")
        row = await cur.fetchone()
        assert row is not None and row["github_repo"] == "o/r"
        assert await _versions(conn) == [*_REAL, (_NEXT, f"{_NEXT:03d}_seed_repo")]
    finally:
        await conn.close()


async def test_duplicate_versions_rejected(tmp_path: Path) -> None:
    migrations = _dir_with_baseline(tmp_path)
    (migrations / f"{_NEXT:03d}_first.sql").write_text("CREATE TABLE a (id INTEGER PRIMARY KEY);\n")
    (migrations / f"{_NEXT:03d}_second.sql").write_text(
        "CREATE TABLE b (id INTEGER PRIMARY KEY);\n"
    )
    conn = await aiosqlite.connect(str(tmp_path / "state.sqlite"))
    conn.row_factory = aiosqlite.Row
    try:
        with pytest.raises(RuntimeError, match=f"duplicate migration version {_NEXT:03d}"):
            await apply_migrations(conn, migrations_dir=migrations)
    finally:
        await conn.close()


async def test_concurrent_writer_waits_instead_of_crashing(tmp_path: Path) -> None:
    """Deploy overlap: another connection holds the write lock while the
    runner boots. With busy_timeout the runner waits it out; without it this
    scenario was the 2026-07-18 `database table is locked` crash-loop."""
    path = tmp_path / "state.sqlite"
    conn = await connect(path)  # v1, WAL mode set
    await conn.close()

    migrations = _dir_with_baseline(tmp_path)
    (migrations / f"{_NEXT:03d}_add_widgets.sql").write_text(
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"
    )

    holder = await aiosqlite.connect(str(path))
    try:
        await holder.execute("BEGIN IMMEDIATE")
        await holder.execute("INSERT INTO repos (linear_team_key, github_repo) VALUES ('X', 'o/r')")

        migrator = await aiosqlite.connect(str(path))
        migrator.row_factory = aiosqlite.Row
        await migrator.execute("PRAGMA busy_timeout = 10000")
        try:
            task = asyncio.create_task(
                apply_migrations(migrator, db_path=path, migrations_dir=migrations)
            )
            await asyncio.sleep(0.3)  # runner is now blocked on the holder's lock
            assert not task.done()
            await holder.commit()  # release; the runner proceeds instead of crashing
            await asyncio.wait_for(task, timeout=10)
            assert "widgets" in await _table_names(migrator)
        finally:
            await migrator.close()
    finally:
        await holder.close()


async def test_db_facade_exports_runner(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        assert await _versions(conn) == _REAL
        assert db.apply_migrations is apply_migrations
    finally:
        await conn.close()


async def test_same_line_statements_split_correctly(tmp_path: Path) -> None:
    """Two statements on one line must execute as two statements (a line-based
    splitter would glue them into one un-executable string)."""
    path = tmp_path / "state.sqlite"
    conn = await connect(path)  # v1
    await conn.close()

    migrations = _dir_with_baseline(tmp_path)
    (migrations / f"{_NEXT:03d}_same_line.sql").write_text(
        "CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (note TEXT DEFAULT 'x; y');\n"
    )
    conn = await aiosqlite.connect(str(path))
    conn.row_factory = aiosqlite.Row
    try:
        await apply_migrations(conn, db_path=path, migrations_dir=migrations)
        assert {"a", "b"} <= await _table_names(conn)
    finally:
        await conn.close()


async def test_backup_is_wal_aware(tmp_path: Path) -> None:
    """Committed rows living only in the -wal file must reach the backup —
    a bare file copy of state.sqlite would miss them."""
    path = tmp_path / "state.sqlite"
    conn = await connect(path)  # v1, WAL on
    await conn.execute("INSERT INTO repos (linear_team_key, github_repo) VALUES ('T', 'o/r')")
    await conn.commit()

    migrations = _dir_with_baseline(tmp_path)
    (migrations / f"{_NEXT:03d}_noop_table.sql").write_text(
        "CREATE TABLE c (id INTEGER PRIMARY KEY);\n"
    )
    try:
        await apply_migrations(conn, db_path=path, migrations_dir=migrations)
    finally:
        await conn.close()

    backup = await aiosqlite.connect(str(tmp_path / f"state.sqlite.bak-{_NEXT:03d}"))
    try:
        cur = await backup.execute("SELECT github_repo FROM repos WHERE linear_team_key = 'T'")
        row = await cur.fetchone()
        assert row is not None and row[0] == "o/r"
    finally:
        await backup.close()


async def test_two_migrators_racing_apply_once(tmp_path: Path) -> None:
    """Deploy overlap: two daemons boot with the same pending migration. The
    loser must re-read the version under the lock and no-op instead of
    replaying the winner's migration (which would crash on CREATE TABLE)."""
    path = tmp_path / "state.sqlite"
    conn = await connect(path)  # v1
    await conn.close()

    migrations = _dir_with_baseline(tmp_path)
    (migrations / f"{_NEXT:03d}_add_widgets.sql").write_text(
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"
    )

    async def _migrator() -> None:
        c = await aiosqlite.connect(str(path))
        c.row_factory = aiosqlite.Row
        await c.execute("PRAGMA busy_timeout = 10000")
        try:
            await apply_migrations(c, db_path=path, migrations_dir=migrations)
        finally:
            await c.close()

    await asyncio.gather(_migrator(), _migrator())

    conn = await aiosqlite.connect(str(path))
    conn.row_factory = aiosqlite.Row
    try:
        assert "widgets" in await _table_names(conn)
        assert await _versions(conn) == [*_REAL, (_NEXT, f"{_NEXT:03d}_add_widgets")]
    finally:
        await conn.close()


async def test_db_newer_than_image_refuses_to_boot(tmp_path: Path) -> None:
    """A DB migrated past this build's migration set (deployment rolled back
    to an older image) must refuse to boot, not silently run old code on a
    newer schema."""
    path = tmp_path / "state.sqlite"
    conn = await connect(path)  # v1
    await conn.execute(
        "INSERT INTO schema_version (version, name, applied_at)"
        " VALUES (999, '999_from_the_future', '2026-01-01T00:00:00Z')"
    )
    await conn.commit()
    await conn.close()

    conn = await aiosqlite.connect(str(path))
    conn.row_factory = aiosqlite.Row
    try:
        with pytest.raises(RuntimeError, match="newer than this build"):
            await apply_migrations(conn, db_path=path)
    finally:
        await conn.close()


async def test_old_image_waits_for_lock_then_refuses_newer_schema(tmp_path: Path) -> None:
    """Codex r4: while a newer image holds the migration lock applying v002,
    an older image (head v001) must wait — not take a stale no-op path — and
    then refuse the now-newer schema."""
    path = tmp_path / "state.sqlite"
    conn = await connect(path)  # v1
    await conn.close()

    holder = await aiosqlite.connect(str(path))
    try:
        await holder.execute("BEGIN IMMEDIATE")
        await holder.execute(
            "INSERT INTO schema_version (version, name, applied_at)"
            f" VALUES ({_NEXT}, '{_NEXT:03d}_new_stuff', '2026-01-01T00:00:00Z')"
        )

        old_image = await aiosqlite.connect(str(path))
        old_image.row_factory = aiosqlite.Row
        await old_image.execute("PRAGMA busy_timeout = 10000")
        try:
            # Old image's migration set = baseline only (head v001).
            task = asyncio.create_task(apply_migrations(old_image, db_path=path))
            await asyncio.sleep(0.3)
            assert not task.done()  # blocked on the newer image's lock
            await holder.commit()
            with pytest.raises(RuntimeError, match="newer than this build"):
                await asyncio.wait_for(task, timeout=10)
        finally:
            await old_image.close()
    finally:
        await holder.close()
