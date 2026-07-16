"""Repo-scoped webhook-secret DAO (SYM-194): one row per GitHub repo, own
`version` for optimistic locking, write-only value. The secret is shared across
a repo's bindings, so its version — not any single binding's — guards concurrent
edits from two tabs editing different bindings of the same repo."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from symphony import db


async def _conn(tmp_path: Path):  # type: ignore[no-untyped-def]
    return await db.connect(tmp_path / "state.sqlite")


@pytest.mark.asyncio
async def test_set_and_get_round_trip(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        assert await db.config_repo_secrets.get(conn, "org/repo") is None
        row = await db.config_repo_secrets.set_secret(
            conn,
            github_repo="org/repo",
            secret="s3cr3t",
            expected_version=0,
            updated_at="2026-07-16T00:00:00Z",
            updated_by="op@x",
        )
        assert row.version == 1
        got = await db.config_repo_secrets.get(conn, "org/repo")
        assert got is not None
        assert got.secret == "s3cr3t"
        assert got.version == 1
        assert got.updated_by == "op@x"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_replace_bumps_version(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        await db.config_repo_secrets.set_secret(
            conn, github_repo="org/repo", secret="a", expected_version=0
        )
        row = await db.config_repo_secrets.set_secret(
            conn, github_repo="org/repo", secret="b", expected_version=1
        )
        assert row.version == 2
        got = await db.config_repo_secrets.get(conn, "org/repo")
        assert got is not None and got.secret == "b"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_version_rejected(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        await db.config_repo_secrets.set_secret(
            conn, github_repo="org/repo", secret="a", expected_version=0
        )
        with pytest.raises(db.config_repo_secrets.StaleVersionError) as exc:
            await db.config_repo_secrets.set_secret(
                conn, github_repo="org/repo", secret="b", expected_version=0
            )
        assert exc.value.current_version == 1
        assert conn.in_transaction is False
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_none_expected_version_is_unconditional(tmp_path: Path) -> None:
    """`expected_version=None` overwrites without a conflict check — the
    back-compat path for a save that carries a secret value but no loaded
    repo-secret version."""
    conn = await _conn(tmp_path)
    try:
        await db.config_repo_secrets.set_secret(
            conn, github_repo="org/repo", secret="a", expected_version=0
        )
        row = await db.config_repo_secrets.set_secret(
            conn, github_repo="org/repo", secret="b", expected_version=None
        )
        assert row.version == 2
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_clear_stores_empty_and_reports_unset(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        await db.config_repo_secrets.set_secret(
            conn, github_repo="org/repo", secret="a", expected_version=0
        )
        row = await db.config_repo_secrets.set_secret(
            conn, github_repo="org/repo", secret="", expected_version=1
        )
        assert row.version == 2 and row.secret == ""
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_list_all_map(tmp_path: Path) -> None:
    conn = await _conn(tmp_path)
    try:
        await db.config_repo_secrets.set_secret(
            conn, github_repo="org/a", secret="sa", expected_version=0
        )
        await db.config_repo_secrets.set_secret(
            conn, github_repo="org/b", secret="", expected_version=0
        )
        rows = await db.config_repo_secrets.list_all(conn)
        by_repo = {r.github_repo: r.secret for r in rows}
        assert by_repo == {"org/a": "sa", "org/b": ""}
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_reload_picks_up_rows_written_by_another_connection(tmp_path: Path) -> None:
    """A `RepoSecretView` built at boot must reflect a secret row written later
    by a different connection (e.g. `config-import` writing directly to the
    DB) without losing its identity, since callers (the webhook verifier
    closure) hold a reference to the original object (SYM-194 review fix)."""
    db_path = tmp_path / "state.sqlite"
    conn_a = await db.connect(db_path)
    conn_b = await db.connect(db_path)
    try:
        view = await db.config_repo_secrets.load_view(conn_a)
        assert view.as_map() == {}
        await db.config_repo_secrets.set_secret(
            conn_b, github_repo="org/repo", secret="s3cr3t", expected_version=0
        )
        await view.reload(conn_a)
        assert view.as_map() == {"org/repo": "s3cr3t"}
    finally:
        await conn_a.close()
        await conn_b.close()


@pytest.mark.asyncio
async def test_concurrent_writers_only_one_wins(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite"
    conn_a = await db.connect(db_path)
    conn_b = await db.connect(db_path)
    try:
        results = await asyncio.gather(
            db.config_repo_secrets.set_secret(
                conn_a, github_repo="org/repo", secret="a", expected_version=0
            ),
            db.config_repo_secrets.set_secret(
                conn_b, github_repo="org/repo", secret="b", expected_version=0
            ),
            return_exceptions=True,
        )
        successes = [r for r in results if not isinstance(r, BaseException)]
        failures = [r for r in results if isinstance(r, db.config_repo_secrets.StaleVersionError)]
        assert len(successes) == 1
        assert len(failures) == 1
        assert failures[0].current_version == 1
    finally:
        await conn_a.close()
        await conn_b.close()
