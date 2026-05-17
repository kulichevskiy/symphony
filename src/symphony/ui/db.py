"""Read-only SQLite access for UI request handlers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

import aiosqlite


def _read_only_uri(path: Path) -> str:
    raw_path = quote(str(path), safe="/:")
    return f"file:{raw_path}?mode=ro&uri=true"


@dataclass
class ReadOnlyDbPool:
    path: Path
    _conn: aiosqlite.Connection | None = None

    async def open(self) -> None:
        if self._conn is not None:
            return
        conn = await aiosqlite.connect(_read_only_uri(self.path), uri=True)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys = ON")
        self._conn = conn

    async def connection(self) -> aiosqlite.Connection:
        await self.open()
        if self._conn is None:
            raise RuntimeError("read-only DB pool is not open")
        return self._conn

    async def close(self) -> None:
        if self._conn is None:
            return
        await self._conn.close()
        self._conn = None


async def open_read_only_pool(path: Path) -> ReadOnlyDbPool:
    pool = ReadOnlyDbPool(path)
    await pool.open()
    return pool
