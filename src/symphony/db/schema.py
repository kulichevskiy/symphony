"""Schema bootstrap. Reads the checked-in `schema.sql` and applies it.

Foreign-key enforcement is per-connection in SQLite, so we issue
`PRAGMA foreign_keys = ON` here rather than embedding it in the script.
"""

from __future__ import annotations

from pathlib import Path

import aiosqlite

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


async def connect(path: Path) -> aiosqlite.Connection:
    """Open (creating if needed) the SQLite database and apply the schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(str(path))
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA foreign_keys = ON")
    await apply_schema(conn)
    return conn


async def apply_schema(conn: aiosqlite.Connection) -> None:
    sql = _SCHEMA_PATH.read_text()
    await conn.executescript(sql)
    await conn.commit()
