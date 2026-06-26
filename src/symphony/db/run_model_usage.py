"""DAO for the `run_model_usage` child table.

Each row attributes a slice of a run's token usage to a (provider, model)
pair. `replace_for_run` rewrites the full set for one run in a single
transaction; because the orchestrator re-parses the (growing) run log at
run end and replaces wholesale, repeated writes are idempotent and never
double-count.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import aiosqlite

from symphony.agent.model_usage import ModelUsage


@dataclass(frozen=True)
class RunModelUsageRow:
    run_id: str
    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    cache_write_tokens: int
    cache_read_tokens: int


async def replace_for_run(
    conn: aiosqlite.Connection,
    run_id: str,
    usages: Iterable[ModelUsage],
) -> None:
    """Replace all rows for `run_id` with `usages` (same-key rows merged)."""
    merged: dict[tuple[str, str], list[int]] = {}
    for usage in usages:
        acc = merged.setdefault((usage.provider, usage.model), [0, 0, 0, 0])
        acc[0] += usage.input_tokens
        acc[1] += usage.output_tokens
        acc[2] += usage.cache_write_tokens
        acc[3] += usage.cache_read_tokens

    await conn.execute("DELETE FROM run_model_usage WHERE run_id = ?", (run_id,))
    if merged:
        await conn.executemany(
            """
            INSERT INTO run_model_usage (
                run_id, provider, model,
                input_tokens, output_tokens, cache_write_tokens, cache_read_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (run_id, provider, model, acc[0], acc[1], acc[2], acc[3])
                for (provider, model), acc in merged.items()
            ],
        )
    await conn.commit()


async def list_for_run(conn: aiosqlite.Connection, run_id: str) -> list[RunModelUsageRow]:
    cur = await conn.execute(
        """
        SELECT run_id, provider, model,
               input_tokens, output_tokens, cache_write_tokens, cache_read_tokens
        FROM run_model_usage
        WHERE run_id = ?
        ORDER BY provider, model
        """,
        (run_id,),
    )
    rows = await cur.fetchall()
    return [
        RunModelUsageRow(
            run_id=row["run_id"],
            provider=row["provider"],
            model=row["model"],
            input_tokens=int(row["input_tokens"]),
            output_tokens=int(row["output_tokens"]),
            cache_write_tokens=int(row["cache_write_tokens"]),
            cache_read_tokens=int(row["cache_read_tokens"]),
        )
        for row in rows
    ]
