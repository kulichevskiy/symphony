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
    await conn.execute("PRAGMA journal_mode=WAL")
    await apply_schema(conn)
    return conn


async def apply_schema(conn: aiosqlite.Connection) -> None:
    sql = _SCHEMA_PATH.read_text()
    await conn.executescript(sql)
    await _migrate(conn)
    await conn.commit()


async def _migrate(conn: aiosqlite.Connection) -> None:
    """Idempotent column adds for tables that pre-existed prior schema bumps."""
    cur = await conn.execute("PRAGMA table_info(comment_cursors)")
    cols = {row[1] for row in await cur.fetchall()}
    if "last_seen_ids" not in cols:
        await conn.execute(
            "ALTER TABLE comment_cursors "
            "ADD COLUMN last_seen_ids TEXT NOT NULL DEFAULT '[]'"
        )

    cur = await conn.execute("PRAGMA table_info(review_state)")
    review_cols = {row[1] for row in await cur.fetchall()}
    if "ci_fetch_failures" not in review_cols:
        await conn.execute(
            "ALTER TABLE review_state "
            "ADD COLUMN ci_fetch_failures INTEGER NOT NULL DEFAULT 0"
        )
    if "pr_number" not in review_cols:
        await conn.execute("ALTER TABLE review_state ADD COLUMN pr_number INTEGER")
    if "pr_url" not in review_cols:
        await conn.execute(
            "ALTER TABLE review_state ADD COLUMN pr_url TEXT NOT NULL DEFAULT ''"
        )
    if "github_repo" not in review_cols:
        await conn.execute(
            "ALTER TABLE review_state ADD COLUMN github_repo TEXT NOT NULL DEFAULT ''"
        )
    if "issue_label" not in review_cols:
        await conn.execute(
            "ALTER TABLE review_state ADD COLUMN issue_label TEXT NOT NULL DEFAULT ''"
        )
    if "codex_lgtm_comment_id" not in review_cols:
        await conn.execute(
            "ALTER TABLE review_state "
            "ADD COLUMN codex_lgtm_comment_id TEXT NOT NULL DEFAULT ''"
        )

    cur = await conn.execute("PRAGMA table_info(webhook_deliveries)")
    cols = {row[1] for row in await cur.fetchall()}
    if "status" not in cols:
        await conn.execute(
            "ALTER TABLE webhook_deliveries "
            "ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'"
        )
        await conn.execute("UPDATE webhook_deliveries SET status = 'handled'")

    cur = await conn.execute("PRAGMA table_info(issue_prs)")
    cols = {row[1] for row in await cur.fetchall()}
    if "binding_key" not in cols:
        await conn.execute(
            "ALTER TABLE issue_prs ADD COLUMN binding_key TEXT NOT NULL DEFAULT ''"
        )

    # Drop the legacy FK on comment_events.issue_id (see schema.sql comment).
    cur = await conn.execute("PRAGMA foreign_key_list(comment_events)")
    if await cur.fetchall():
        await conn.executescript(
            """
            CREATE TABLE comment_events_new (
                comment_id TEXT PRIMARY KEY,
                issue_id   TEXT NOT NULL,
                seen_at    TEXT NOT NULL
            );
            INSERT INTO comment_events_new (comment_id, issue_id, seen_at)
                SELECT comment_id, issue_id, seen_at FROM comment_events;
            DROP TABLE comment_events;
            ALTER TABLE comment_events_new RENAME TO comment_events;
            CREATE INDEX IF NOT EXISTS idx_comment_events_issue
                ON comment_events(issue_id);
            """
        )
