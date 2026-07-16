"""Schema bootstrap. Reads the checked-in `schema.sql` and applies it.

Foreign-key enforcement is per-connection in SQLite, so we issue
`PRAGMA foreign_keys = ON` here rather than embedding it in the script.
"""

from __future__ import annotations

import json
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
    cur = await conn.execute("PRAGMA table_info(issues)")
    issue_cols = {row[1] for row in await cur.fetchall()}
    if "tracker_issue_id" not in issue_cols:
        await conn.execute(
            "ALTER TABLE issues ADD COLUMN tracker_issue_id TEXT NOT NULL DEFAULT ''"
        )
        await conn.execute("UPDATE issues SET tracker_issue_id = id WHERE tracker_issue_id = ''")
    if "provider" not in issue_cols:
        await conn.execute("ALTER TABLE issues ADD COLUMN provider TEXT NOT NULL DEFAULT 'linear'")
    if "site" not in issue_cols:
        await conn.execute("ALTER TABLE issues ADD COLUMN site TEXT NOT NULL DEFAULT 'default'")
    if "granted_token_budget" not in issue_cols:
        await conn.execute("ALTER TABLE issues ADD COLUMN granted_token_budget INTEGER DEFAULT 0")
    await conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_issues_tracker_identity
        ON issues(provider, site, tracker_issue_id)
        """
    )

    cur = await conn.execute("PRAGMA table_info(runs)")
    run_cols = {row[1] for row in await cur.fetchall()}
    for col in (
        "input_tokens",
        "output_tokens",
        "cache_write_tokens",
        "cache_read_tokens",
    ):
        if col not in run_cols:
            await conn.execute(f"ALTER TABLE runs ADD COLUMN {col} INTEGER NOT NULL DEFAULT 0")
    if "termination_kind" not in run_cols:
        await conn.execute("ALTER TABLE runs ADD COLUMN termination_kind TEXT NOT NULL DEFAULT ''")
    if "termination_detail" not in run_cols:
        await conn.execute(
            "ALTER TABLE runs ADD COLUMN termination_detail TEXT NOT NULL DEFAULT ''"
        )
    if "exit_returncode" not in run_cols:
        await conn.execute("ALTER TABLE runs ADD COLUMN exit_returncode INTEGER")
    if "stage_done_announced_at" not in run_cols:
        await conn.execute(
            "ALTER TABLE runs ADD COLUMN stage_done_announced_at TEXT NOT NULL DEFAULT ''"
        )
    if "binding_key" not in run_cols:
        await conn.execute("ALTER TABLE runs ADD COLUMN binding_key TEXT NOT NULL DEFAULT ''")

    cur = await conn.execute("PRAGMA table_info(comment_cursors)")
    cols = {row[1] for row in await cur.fetchall()}
    if "last_seen_ids" not in cols:
        await conn.execute(
            "ALTER TABLE comment_cursors ADD COLUMN last_seen_ids TEXT NOT NULL DEFAULT '[]'"
        )

    cur = await conn.execute("PRAGMA table_info(review_state)")
    review_cols = {row[1] for row in await cur.fetchall()}
    if "ci_fetch_failures" not in review_cols:
        await conn.execute(
            "ALTER TABLE review_state ADD COLUMN ci_fetch_failures INTEGER NOT NULL DEFAULT 0"
        )
    if "pr_number" not in review_cols:
        await conn.execute("ALTER TABLE review_state ADD COLUMN pr_number INTEGER")
    if "pr_url" not in review_cols:
        await conn.execute("ALTER TABLE review_state ADD COLUMN pr_url TEXT NOT NULL DEFAULT ''")
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
            "ALTER TABLE review_state ADD COLUMN codex_lgtm_comment_id TEXT NOT NULL DEFAULT ''"
        )
    if "codex_review_requested_at" not in review_cols:
        await conn.execute(
            "ALTER TABLE review_state ADD COLUMN codex_review_requested_at TEXT NOT NULL DEFAULT ''"
        )

    cur = await conn.execute("PRAGMA table_info(webhook_deliveries)")
    cols = {row[1] for row in await cur.fetchall()}
    if "status" not in cols:
        await conn.execute(
            "ALTER TABLE webhook_deliveries ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'"
        )
        await conn.execute("UPDATE webhook_deliveries SET status = 'handled'")

    cur = await conn.execute("PRAGMA table_info(issue_prs)")
    cols = {row[1] for row in await cur.fetchall()}
    if "binding_key" not in cols:
        await conn.execute("ALTER TABLE issue_prs ADD COLUMN binding_key TEXT NOT NULL DEFAULT ''")
    if "parked_at" not in cols:
        await conn.execute("ALTER TABLE issue_prs ADD COLUMN parked_at TEXT")
    if "review_bypassed" not in cols:
        await conn.execute(
            "ALTER TABLE issue_prs ADD COLUMN review_bypassed INTEGER NOT NULL DEFAULT 0"
        )

    cur = await conn.execute("PRAGMA table_info(acceptance_state)")
    cols = {row[1] for row in await cur.fetchall()}
    if "pr_head_sha" not in cols:
        await conn.execute(
            "ALTER TABLE acceptance_state ADD COLUMN pr_head_sha TEXT NOT NULL DEFAULT ''"
        )
    if "infra_retries" not in cols:
        await conn.execute(
            "ALTER TABLE acceptance_state ADD COLUMN infra_retries INTEGER NOT NULL DEFAULT 0"
        )

    cur = await conn.execute("PRAGMA table_info(merge_conflict_fix_marks)")
    cols = {row[1] for row in await cur.fetchall()}
    if "head_sha" not in cols:
        await conn.execute(
            "ALTER TABLE merge_conflict_fix_marks ADD COLUMN head_sha TEXT NOT NULL DEFAULT ''"
        )

    cur = await conn.execute("PRAGMA table_info(operator_waits)")
    cols = {row[1] for row in await cur.fetchall()}
    if "tracker_provider" not in cols:
        await conn.execute(
            "ALTER TABLE operator_waits ADD COLUMN tracker_provider TEXT NOT NULL DEFAULT 'linear'"
        )
        await conn.execute(
            """
            UPDATE operator_waits
               SET tracker_provider = COALESCE(
                   (SELECT provider FROM issues WHERE issues.id = operator_waits.issue_id),
                   'linear'
               )
            """
        )
    if "tracker_site" not in cols:
        await conn.execute(
            "ALTER TABLE operator_waits ADD COLUMN tracker_site TEXT NOT NULL DEFAULT 'default'"
        )
        await conn.execute(
            """
            UPDATE operator_waits
               SET tracker_site = COALESCE(
                   (SELECT site FROM issues WHERE issues.id = operator_waits.issue_id),
                   'default'
               )
            """
        )
    cur = await conn.execute("PRAGMA table_info(operator_waits)")
    cols = {row[1] for row in await cur.fetchall()}
    if "local_review_outcome" not in cols:
        await conn.execute("ALTER TABLE operator_waits ADD COLUMN local_review_outcome TEXT")
    cur = await conn.execute("PRAGMA table_info(operator_waits)")
    cols = {row[1] for row in await cur.fetchall()}
    if "provider" not in cols:
        await conn.execute(
            "ALTER TABLE operator_waits ADD COLUMN provider TEXT NOT NULL DEFAULT 'linear'"
        )
        await conn.execute(
            """
            UPDATE operator_waits
               SET provider = COALESCE(
                   (SELECT provider FROM issues WHERE issues.id = operator_waits.issue_id),
                   NULLIF(tracker_provider, ''),
                   'linear'
               )
            """
        )

    # Bindings created under the interim CRUD (SYM-190..193, before the
    # repo-scoped secret table existed) still carry `webhook_secret` inside
    # `config_bindings.payload`. Drain any such value into
    # `config_repo_secrets` and strip it from the payload so the field can
    # never mis-log as a spurious "cleared" on the binding's first routine
    # edit (SYM-194 review). Idempotent: once a payload's `webhook_secret` key
    # is gone, later runs find nothing to do.
    #
    # Two legacy bindings can share a `github_repo`; `ORDER BY id` plus
    # last-non-empty-wins matches `config_import.import_config`'s documented
    # tie-break for the same collapse, so both cutover paths land the same
    # secret for a given repo instead of depending on unordered row-scan luck
    # (SYM-194 review).
    cur = await conn.execute("SELECT id, payload, github_repo FROM config_bindings ORDER BY id")
    repo_secrets: dict[str, str] = {}
    for row in await cur.fetchall():
        try:
            payload = json.loads(row["payload"])
        except json.JSONDecodeError:
            continue
        if "webhook_secret" not in payload:
            continue
        secret = payload.pop("webhook_secret", None)
        await conn.execute(
            "UPDATE config_bindings SET payload = ? WHERE id = ?",
            (json.dumps(payload, separators=(",", ":")), row["id"]),
        )
        if secret:
            repo_secrets[str(row["github_repo"])] = secret
    for github_repo, secret in repo_secrets.items():
        existing = await conn.execute(
            "SELECT 1 FROM config_repo_secrets WHERE github_repo = ?", (github_repo,)
        )
        if await existing.fetchone() is not None:
            # A real repo-secret row already exists (e.g. the operator already
            # set one through the UI) — never clobber it with a stale legacy
            # value.
            continue
        await conn.execute(
            """
            INSERT INTO config_repo_secrets (github_repo, secret, version, updated_at, updated_by)
            VALUES (?, ?, 1, '', 'migration')
            """,
            (github_repo, secret),
        )
