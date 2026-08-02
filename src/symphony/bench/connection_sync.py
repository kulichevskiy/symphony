from __future__ import annotations

from pathlib import Path

from .. import db


async def snapshot_connections(control_path: Path, destination: Path) -> None:
    """Create a candidate-readable DB containing only encrypted connection rows."""
    source = await db.connect(control_path)
    target = await db.connect(destination)
    try:
        generations = await (
            await source.execute("SELECT provider, generation FROM oauth_credential_generations")
        ).fetchall()
        connections = await (
            await source.execute(
                """
                SELECT provider, credential, refresh_token, status, expires_at,
                       updated_at, updated_by, generation
                FROM oauth_connections
                """
            )
        ).fetchall()
        await target.execute("DELETE FROM oauth_connections")
        await target.execute("DELETE FROM oauth_credential_generations")
        await target.executemany(
            "INSERT INTO oauth_credential_generations (provider, generation) VALUES (?, ?)",
            [tuple(row) for row in generations],
        )
        await target.executemany(
            """
            INSERT INTO oauth_connections (
                provider, credential, refresh_token, status, expires_at,
                updated_at, updated_by, generation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [tuple(row) for row in connections],
        )
        await target.commit()
    finally:
        await source.close()
        await target.close()


async def sync_connections(candidate_path: Path, control_path: Path) -> int:
    """Copy only candidate generations newer than the control-plane row."""
    candidate = await db.connect(candidate_path)
    control = await db.connect(control_path)
    copied = 0
    try:
        cursor = await candidate.execute(
            """
            SELECT provider, credential, refresh_token, status, expires_at,
                   updated_at, updated_by, generation
            FROM oauth_connections
            ORDER BY provider
            """
        )
        rows = await cursor.fetchall()
        for row in rows:
            await control.execute("BEGIN IMMEDIATE")
            try:
                current = await control.execute(
                    """
                    SELECT generations.generation,
                           connections.provider IS NOT NULL AS connected
                    FROM oauth_credential_generations AS generations
                    LEFT JOIN oauth_connections AS connections
                      ON connections.provider = generations.provider
                    WHERE generations.provider = ?
                    """,
                    (row["provider"],),
                )
                current_row = await current.fetchone()
                if current_row is not None and not bool(current_row["connected"]):
                    await control.rollback()
                    continue
                current_generation = (
                    int(current_row["generation"]) if current_row is not None else 0
                )
                candidate_generation = int(row["generation"])
                if candidate_generation <= current_generation:
                    await control.rollback()
                    continue
                await control.execute(
                    """
                    INSERT INTO oauth_credential_generations (provider, generation)
                    VALUES (?, ?)
                    ON CONFLICT(provider) DO UPDATE SET generation = excluded.generation
                    """,
                    (row["provider"], candidate_generation),
                )
                await control.execute(
                    """
                    INSERT INTO oauth_connections (
                        provider, credential, refresh_token, status, expires_at,
                        updated_at, updated_by, generation
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(provider) DO UPDATE SET
                        credential = excluded.credential,
                        refresh_token = excluded.refresh_token,
                        status = excluded.status,
                        expires_at = excluded.expires_at,
                        updated_at = excluded.updated_at,
                        updated_by = excluded.updated_by,
                        generation = excluded.generation
                    """,
                    tuple(row),
                )
                await control.commit()
                copied += 1
            except BaseException:
                await control.rollback()
                raise
    finally:
        await candidate.close()
        await control.close()
    return copied
