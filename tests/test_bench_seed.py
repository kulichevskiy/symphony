import asyncio
import json
import sqlite3
from pathlib import Path

from click.testing import CliRunner

from symphony import db
from symphony.cli import main
from symphony.crypto import CredentialCipher


async def _control_db(path: Path) -> None:
    conn = await db.connect(path)
    try:
        await db.oauth_connections.set_connection(
            conn,
            provider="github",
            credential="secret-token",
            cipher=CredentialCipher("test-encryption-key"),
            updated_by="test",
        )
    finally:
        await conn.close()


async def _replace_connection(path: Path, token: str) -> None:
    conn = await db.connect(path)
    try:
        await db.oauth_connections.set_connection(
            conn,
            provider="github",
            credential=token,
            cipher=CredentialCipher("test-encryption-key"),
            updated_by="candidate-refresh",
        )
    finally:
        await conn.close()


def test_bench_seed_builds_candidate_db_from_profile(tmp_path: Path) -> None:
    db_path = tmp_path / "candidate.sqlite"
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "roles": {
                    "implement": {
                        "agent": "codex",
                        "model": "gpt-5.6-sol",
                        "effort": "high",
                    }
                },
                "knobs": {"global_max_concurrent": 1},
                "binding": {"local_review": True, "remote_review": True},
            }
        )
    )

    result = CliRunner().invoke(
        main,
        [
            "bench",
            "seed",
            "--db",
            str(db_path),
            "--profile",
            str(profile_path),
            "--linear-team",
            "BENCH",
            "--github-repo",
            "kulichevskiy/EXP-1-A1",
            "--issue-label",
            "symphony-bench",
            "--issue-title-prefix",
            "[EXP-1-A1]",
        ],
    )

    assert result.exit_code == 0, result.output
    with sqlite3.connect(db_path) as conn:
        payload = json.loads(conn.execute("SELECT payload FROM config_bindings").fetchone()[0])
        roles, knobs = conn.execute("SELECT roles, knobs FROM config_globals").fetchone()
    assert payload["project_key"] == "BENCH"
    assert payload["github_repo"] == "kulichevskiy/EXP-1-A1"
    assert payload["issue_label"] == "symphony-bench"
    assert payload["issue_title_prefix"] == "[EXP-1-A1]"
    assert payload["local_review"] is True
    assert payload["remote_review"] is True
    assert payload["webhook_enabled"] is False
    assert payload["verify_cmd"] == (
        "uv run pytest && uv run ruff check . && uv run mypy eventdesk "
        "&& cd frontend && npm ci && npm test -- --run && npm run build"
    )
    assert json.loads(roles)["implement"]["model"] == "gpt-5.6-sol"
    assert json.loads(knobs)["global_max_concurrent"] == 1


def test_bench_seed_rejects_profile_routing_overrides(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "binding": {
                    "github_repo": "kulichevskiy/production",
                    "issue_title_prefix": "",
                    "webhook_enabled": True,
                }
            }
        )
    )

    result = CliRunner().invoke(
        main,
        [
            "bench",
            "seed",
            "--db",
            str(tmp_path / "candidate.sqlite"),
            "--profile",
            str(profile_path),
            "--linear-team",
            "BENCH",
            "--github-repo",
            "kulichevskiy/EXP-1-A1",
        ],
    )

    assert result.exit_code != 0
    assert "github_repo, issue_title_prefix, webhook_enabled" in result.output


def test_bench_seed_copies_encrypted_control_connections(tmp_path: Path) -> None:
    control_path = tmp_path / "control.sqlite"
    candidate_path = tmp_path / "candidate.sqlite"
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}")
    asyncio.run(_control_db(control_path))

    result = CliRunner().invoke(
        main,
        [
            "bench",
            "seed",
            "--db",
            str(candidate_path),
            "--profile",
            str(profile_path),
            "--linear-team",
            "BENCH",
            "--github-repo",
            "kulichevskiy/EXP-1-A1",
            "--connections-db",
            str(control_path),
        ],
    )

    assert result.exit_code == 0, result.output
    with sqlite3.connect(candidate_path) as candidate:
        copied = candidate.execute(
            "SELECT provider, status, credential, generation FROM oauth_connections"
        ).fetchone()
    with sqlite3.connect(control_path) as control:
        original = control.execute(
            "SELECT provider, status, credential, generation FROM oauth_connections"
        ).fetchone()
    assert copied == original


def test_bench_sync_connections_copies_only_newer_candidate_generation(
    tmp_path: Path,
) -> None:
    control_path = tmp_path / "control.sqlite"
    candidate_path = tmp_path / "candidate.sqlite"
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}")
    asyncio.run(_control_db(control_path))
    seeded = CliRunner().invoke(
        main,
        [
            "bench",
            "seed",
            "--db",
            str(candidate_path),
            "--profile",
            str(profile_path),
            "--linear-team",
            "BENCH",
            "--github-repo",
            "kulichevskiy/EXP-1-A1",
            "--connections-db",
            str(control_path),
        ],
    )
    assert seeded.exit_code == 0, seeded.output
    asyncio.run(_replace_connection(candidate_path, "refreshed-token"))

    result = CliRunner().invoke(
        main,
        [
            "bench",
            "sync-connections",
            "--db",
            str(candidate_path),
            "--control-db",
            str(control_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == "synced 1 connection(s)\n"
    conn = asyncio.run(db.connect(control_path))

    async def read() -> tuple[str | None, int]:
        try:
            value = await db.oauth_connections.get_credential(
                conn, "github", CredentialCipher("test-encryption-key")
            )
            status = await db.oauth_connections.get_status(conn, "github")
            assert status is not None
            return value, status.generation
        finally:
            await conn.close()

    assert asyncio.run(read()) == ("refreshed-token", 2)

    repeated = CliRunner().invoke(
        main,
        [
            "bench",
            "sync-connections",
            "--db",
            str(candidate_path),
            "--control-db",
            str(control_path),
        ],
    )
    assert repeated.exit_code == 0
    assert repeated.output == "synced 0 connection(s)\n"
