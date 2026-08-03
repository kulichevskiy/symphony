from __future__ import annotations

from pathlib import Path
from typing import Any

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
    """Offer candidate refreshes to control, resolving equal generations deterministically."""
    candidate = await db.connect(candidate_path)
    control = await db.connect(control_path)
    copied = 0
    try:
        cursor = await candidate.execute(
            """
            SELECT connections.provider, connections.credential,
                   connections.refresh_token, connections.status,
                   connections.expires_at, connections.updated_at,
                   connections.updated_by,
                   connections.generation AS credential_generation,
                   generations.generation AS sequence_generation
            FROM oauth_connections AS connections
            JOIN oauth_credential_generations AS generations
              ON generations.provider = connections.provider
            ORDER BY connections.provider
            """
        )
        rows = await cursor.fetchall()
        for row in rows:
            await control.execute("BEGIN IMMEDIATE")
            try:
                current = await control.execute(
                    """
                    SELECT generations.generation AS sequence_generation,
                           connections.provider IS NOT NULL AS connected,
                           connections.credential,
                           connections.refresh_token,
                           connections.status,
                           connections.expires_at,
                           connections.updated_at,
                           connections.updated_by,
                           connections.generation AS credential_generation
                    FROM oauth_credential_generations AS generations
                    LEFT JOIN oauth_connections AS connections
                      ON connections.provider = generations.provider
                    WHERE generations.provider = ?
                    """,
                    (row["provider"],),
                )
                current_row = await current.fetchone()
                if current_row is not None and not bool(current_row["connected"]):
                    candidate_sequence = int(row["sequence_generation"])
                    current_sequence = int(current_row["sequence_generation"])
                    if candidate_sequence > current_sequence:
                        await control.execute(
                            """
                            UPDATE oauth_credential_generations
                            SET generation = ?
                            WHERE provider = ?
                            """,
                            (candidate_sequence, row["provider"]),
                        )
                        await control.commit()
                        copied += 1
                    else:
                        await control.rollback()
                    continue
                current_sequence = (
                    int(current_row["sequence_generation"])
                    if current_row is not None
                    else 0
                )
                control_credential_generation = (
                    int(current_row["credential_generation"])
                    if current_row is not None and bool(current_row["connected"])
                    else 0
                )
                candidate_sequence = int(row["sequence_generation"])
                candidate_credential_generation = int(row["credential_generation"])
                merged_sequence = max(current_sequence, candidate_sequence)
                candidate_is_newer = candidate_credential_generation > (
                    control_credential_generation
                ) or (
                    candidate_credential_generation == control_credential_generation
                    and current_row is not None
                    and _connection_rank(row) > _connection_rank(current_row)
                )
                if not candidate_is_newer:
                    if merged_sequence > current_sequence:
                        await control.execute(
                            """
                            UPDATE oauth_credential_generations
                            SET generation = ?
                            WHERE provider = ?
                            """,
                            (merged_sequence, row["provider"]),
                        )
                        await control.commit()
                        copied += 1
                    else:
                        await control.rollback()
                    continue
                await control.execute(
                    """
                    INSERT INTO oauth_credential_generations (provider, generation)
                    VALUES (?, ?)
                    ON CONFLICT(provider) DO UPDATE SET generation = excluded.generation
                    """,
                    (row["provider"], merged_sequence),
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
                    (
                        row["provider"],
                        row["credential"],
                        row["refresh_token"],
                        row["status"],
                        row["expires_at"],
                        row["updated_at"],
                        row["updated_by"],
                        candidate_credential_generation,
                    ),
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


async def mirror_connections(control_path: Path, candidate_path: Path) -> int:
    """Mirror authoritative control rows and disconnect tombstones into a candidate."""
    control = await db.connect(control_path)
    candidate = await db.connect(candidate_path)
    copied = 0
    try:
        cursor = await control.execute(
            """
            SELECT generations.provider,
                   generations.generation AS sequence_generation,
                   connections.provider IS NOT NULL AS connected,
                   connections.credential,
                   connections.refresh_token,
                   connections.status,
                   connections.expires_at,
                   connections.updated_at,
                   connections.updated_by,
                   connections.generation AS credential_generation
            FROM oauth_credential_generations AS generations
            LEFT JOIN oauth_connections AS connections
              ON connections.provider = generations.provider
            ORDER BY generations.provider
            """
        )
        rows = await cursor.fetchall()
        for row in rows:
            await candidate.execute("BEGIN IMMEDIATE")
            try:
                current = await candidate.execute(
                    """
                    SELECT generations.generation AS sequence_generation,
                           connections.provider IS NOT NULL AS connected,
                           connections.credential,
                           connections.refresh_token,
                           connections.status,
                           connections.expires_at,
                           connections.updated_at,
                           connections.updated_by,
                           connections.generation AS credential_generation
                    FROM oauth_credential_generations AS generations
                    LEFT JOIN oauth_connections AS connections
                      ON connections.provider = generations.provider
                    WHERE generations.provider = ?
                    """,
                    (row["provider"],),
                )
                current_row = await current.fetchone()
                control_sequence = int(row["sequence_generation"])
                candidate_sequence = (
                    int(current_row["sequence_generation"])
                    if current_row is not None
                    else 0
                )
                control_connected = bool(row["connected"])
                candidate_connected = bool(
                    current_row is not None and current_row["connected"]
                )
                merged_sequence = max(control_sequence, candidate_sequence)
                if control_connected and candidate_connected:
                    assert current_row is not None
                    control_credential_generation = int(row["credential_generation"])
                    candidate_credential_generation = int(
                        current_row["credential_generation"]
                    )
                    candidate_is_newer = candidate_credential_generation > (
                        control_credential_generation
                    ) or (
                        candidate_credential_generation == control_credential_generation
                        and _connection_rank(current_row) > _connection_rank(row)
                    )
                    if candidate_is_newer:
                        if merged_sequence > candidate_sequence:
                            await candidate.execute(
                                """
                                UPDATE oauth_credential_generations
                                SET generation = ?
                                WHERE provider = ?
                                """,
                                (merged_sequence, row["provider"]),
                            )
                            await candidate.commit()
                            copied += 1
                        else:
                            await candidate.rollback()
                        continue
                    same_connection = (
                        candidate_credential_generation == control_credential_generation
                        and _connection_rank(current_row) == _connection_rank(row)
                    )
                    if same_connection and merged_sequence == candidate_sequence:
                        await candidate.rollback()
                        continue
                elif not control_connected and not candidate_connected:
                    if merged_sequence == candidate_sequence:
                        await candidate.rollback()
                        continue
                await candidate.execute(
                    """
                    INSERT INTO oauth_credential_generations (provider, generation)
                    VALUES (?, ?)
                    ON CONFLICT(provider) DO UPDATE SET generation = excluded.generation
                    """,
                    (row["provider"], merged_sequence),
                )
                if bool(row["connected"]):
                    await candidate.execute(
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
                        (
                            row["provider"],
                            row["credential"],
                            row["refresh_token"],
                            row["status"],
                            row["expires_at"],
                            row["updated_at"],
                            row["updated_by"],
                            row["credential_generation"],
                        ),
                    )
                else:
                    await candidate.execute(
                        "DELETE FROM oauth_connections WHERE provider = ?",
                        (row["provider"],),
                    )
                await candidate.commit()
                copied += 1
            except BaseException:
                await candidate.rollback()
                raise
    finally:
        await control.close()
        await candidate.close()
    return copied


async def reconcile_connections(candidate_path: Path, control_path: Path) -> tuple[int, int]:
    """Converge one candidate with control; control disconnects remain authoritative."""
    control_updates = await sync_connections(candidate_path, control_path)
    candidate_updates = await mirror_connections(control_path, candidate_path)
    return control_updates, candidate_updates


def _connection_rank(row: Any) -> tuple[str, ...]:
    return tuple(
        str(row[key] or "")
        for key in (
            "updated_at",
            "expires_at",
            "updated_by",
            "credential",
            "refresh_token",
            "status",
        )
    )
