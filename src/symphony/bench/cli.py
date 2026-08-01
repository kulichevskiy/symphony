from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import datetime
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any

import click
import httpx
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .. import db
from ..config import RepoBinding, binding_natural_key
from ..tokens import effective_tokens
from .app import create_bench_app
from .connection_sync import sync_connections as _sync_connections
from .eventdesk import harness_version
from .executor import RemoteCommands, create_executor_app
from .live import LiveBenchConfig, LiveTrialExecutor
from .models import ExperimentReport
from .report import render_markdown


class _BenchSecrets(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_token: str = Field(default="", validation_alias="SYMPHONY_BENCH_TOKEN")
    encryption_key: str = Field(default="", validation_alias="SYMPHONY_ENCRYPTION_KEY")
    executor_token: str = Field(default="", validation_alias="SYMPHONY_BENCH_EXECUTOR_TOKEN")


@click.group()
def verify() -> None:
    """Submit and inspect live Symphony benchmark experiments."""


@click.group()
def bench() -> None:
    """Candidate-side benchmark utilities."""


def _connection_options(command: Any) -> Any:
    command = click.option(
        "--token",
        envvar="SYMPHONY_BENCH_TOKEN",
        required=True,
        help="Bench API bearer token.",
    )(command)
    return click.option(
        "--url",
        envvar="SYMPHONY_BENCH_URL",
        required=True,
        help="Bench base URL.",
    )(command)


def _request(
    method: str, url: str, token: str, path: str, *, payload: dict[str, object] | None = None
) -> dict[str, Any]:
    try:
        response = httpx.request(
            method,
            f"{url.rstrip('/')}{path}",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise click.ClickException(f"bench request failed: {exc}") from exc
    body = response.json()
    if not isinstance(body, dict):
        raise click.ClickException("bench returned a non-object response")
    return body


@verify.command("submit")
@_connection_options
@click.option("--candidate-a", required=True, help="Git revision for candidate A.")
@click.option("--candidate-b", required=True, help="Git revision for candidate B.")
@click.option(
    "--profile-a",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Optional immutable JSON config snapshot for candidate A.",
)
@click.option(
    "--profile-b",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Optional immutable JSON config snapshot for candidate B.",
)
@click.option("--repetitions", type=click.IntRange(1, 10), default=3, show_default=True)
def submit(
    url: str,
    token: str,
    candidate_a: str,
    candidate_b: str,
    profile_a: Path | None,
    profile_b: Path | None,
    repetitions: int,
) -> None:
    """Queue an interleaved A/B experiment."""
    payload: dict[str, object] = {
        "candidate_a": candidate_a,
        "candidate_b": candidate_b,
        "repetitions": repetitions,
    }
    if profile_a is not None:
        payload["candidate_a_profile"] = _read_profile(profile_a)
    if profile_b is not None:
        payload["candidate_b_profile"] = _read_profile(profile_b)
    body = _request(
        "POST",
        url,
        token,
        "/experiments",
        payload=payload,
    )
    click.echo(f"{body['id']} {body['status']}")


def _read_profile(path: Path) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise click.ClickException(f"invalid profile {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise click.ClickException(f"invalid profile {path}: root must be an object")
    return {str(key): value for key, value in raw.items()}


@verify.command("status")
@click.argument("experiment_id")
@_connection_options
def experiment_status(experiment_id: str, url: str, token: str) -> None:
    """Print an experiment's current JSON state."""
    body = _request("GET", url, token, f"/experiments/{experiment_id}")
    click.echo(json.dumps(body, indent=2, sort_keys=True))


@verify.command("report")
@click.argument("experiment_id")
@_connection_options
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    show_default=True,
)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False))
def experiment_report(
    experiment_id: str,
    url: str,
    token: str,
    output_format: str,
    output: Path | None,
) -> None:
    """Render a reproducible report with aggregate and per-trial receipts."""
    body = _request("GET", url, token, f"/experiments/{experiment_id}/report")
    try:
        report = ExperimentReport.model_validate(body)
    except ValueError as exc:
        raise click.ClickException(f"bench returned an invalid report: {exc}") from exc
    rendered = (
        json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"
        if output_format == "json"
        else render_markdown(report)
    )
    if output is None:
        click.echo(rendered, nl=False)
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
        click.echo(output)


@bench.command("seed")
@click.option("--db", "db_path", type=click.Path(path_type=Path), required=True)
@click.option(
    "--profile",
    "profile_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
)
@click.option("--linear-team", required=True)
@click.option("--github-repo", required=True)
@click.option("--issue-label")
@click.option("--issue-title-prefix")
@click.option(
    "--connections-db",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Control DB whose encrypted OAuth connections should seed the candidate.",
)
def seed(
    db_path: Path,
    profile_path: Path,
    linear_team: str,
    github_repo: str,
    issue_label: str | None,
    issue_title_prefix: str | None,
    connections_db: Path | None,
) -> None:
    """Create an isolated candidate DB through this revision's own schema."""
    asyncio.run(
        _seed(
            db_path,
            profile_path,
            linear_team,
            github_repo,
            issue_label,
            issue_title_prefix,
            connections_db,
        )
    )


async def _seed(
    db_path: Path,
    profile_path: Path,
    linear_team: str,
    github_repo: str,
    issue_label: str | None,
    issue_title_prefix: str | None,
    connections_db: Path | None,
) -> None:
    try:
        profile_text = await asyncio.to_thread(profile_path.read_text, encoding="utf-8")
        raw = json.loads(profile_text)
    except (OSError, json.JSONDecodeError) as exc:
        raise click.ClickException(f"invalid profile: {exc}") from exc
    if not isinstance(raw, dict):
        raise click.ClickException("invalid profile: root must be an object")
    roles = raw.get("roles", {})
    knobs = raw.get("knobs", {})
    binding_overrides = raw.get("binding", {})
    if not all(isinstance(value, dict) for value in (roles, knobs, binding_overrides)):
        raise click.ClickException("invalid profile: roles, knobs, and binding must be objects")

    label = issue_label or github_repo.rsplit("/", 1)[-1]
    payload: dict[str, Any] = {
        "project_key": linear_team,
        "github_repo": github_repo,
        "issue_label": label,
        "issue_title_prefix": issue_title_prefix,
        "branch_prefix": "symphony",
        "max_concurrent": 1,
        "runner": "local",
        "webhook_enabled": False,
        "verify_cmd": (
            "uv run pytest && uv run ruff check . && uv run mypy eventdesk "
            "&& cd frontend && npm ci && npm test -- --run && npm run build"
        ),
        "states": {
            "ready": "Todo",
            "waiting": "Waiting",
            "in_progress": "In Progress",
            "local_code_review": "Local Code Review",
            "code_review": "In Review",
            "needs_approval": "Needs Input",
            "blocked": "Needs Input",
            "done": "Done",
        },
        **binding_overrides,
    }
    try:
        binding = RepoBinding.model_validate(payload)
    except ValueError as exc:
        raise click.ClickException(f"invalid profile binding: {exc}") from exc

    conn = await db.connect(db_path)
    try:
        await db.config_globals.set_globals(conn, roles=roles)
        if knobs:
            await db.config_globals.update_knobs(conn, knobs=knobs, expected_version=1)
        await db.config_bindings.insert(
            conn,
            payload=payload,
            key=binding_natural_key(binding),
            updated_by="bench-seed",
        )
        if connections_db is not None:
            await conn.execute("ATTACH DATABASE ? AS bench_control", (str(connections_db),))
            try:
                await conn.execute("DELETE FROM oauth_connections")
                await conn.execute("DELETE FROM oauth_credential_generations")
                await conn.execute(
                    """
                    INSERT INTO oauth_credential_generations (provider, generation)
                    SELECT provider, generation FROM bench_control.oauth_credential_generations
                    """
                )
                await conn.execute(
                    """
                    INSERT INTO oauth_connections (
                        provider, credential, refresh_token, status, expires_at,
                        updated_at, updated_by, generation
                    )
                    SELECT provider, credential, refresh_token, status, expires_at,
                           updated_at, updated_by, generation
                    FROM bench_control.oauth_connections
                    """
                )
                await conn.commit()
            finally:
                await conn.execute("DETACH DATABASE bench_control")
    finally:
        await conn.close()
    click.echo(f"seeded {db_path} for {linear_team} → {github_repo}")


@bench.command("sync-connections")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
)
@click.option(
    "--control-db",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
)
def sync_connections(db_path: Path, control_db: Path) -> None:
    """Copy runtime-refreshed OAuth rows back without overwriting newer control data."""
    count = asyncio.run(_sync_connections(db_path, control_db))
    click.echo(f"synced {count} connection(s)")


@bench.command("snapshot")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
)
def snapshot(db_path: Path) -> None:
    """Emit stable metrics without exposing the candidate's DB schema to the worker."""
    click.echo(json.dumps(asyncio.run(_snapshot(db_path)), separators=(",", ":")))


async def _snapshot(db_path: Path) -> dict[str, object]:
    conn = await db.connect(db_path)
    try:
        cursor = await conn.execute(
            """
            SELECT stage, status, started_at, ended_at, input_tokens, output_tokens,
                   cache_write_tokens, cache_read_tokens
            FROM runs
            ORDER BY started_at, id
            """
        )
        rows = await cursor.fetchall()
        review_cursor = await conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM state_transitions
            WHERE table_name = 'review_state'
              AND field = 'codex_review_requested_at'
              AND COALESCE(new_value, '') <> ''
            """
        )
        review_row = await review_cursor.fetchone()
    finally:
        await conn.close()
    statuses = Counter(str(row["status"]) for row in rows)
    active_seconds = 0.0
    for row in rows:
        if row["stage"] == "review" or row["ended_at"] is None:
            continue
        started = datetime.fromisoformat(str(row["started_at"]))
        ended = datetime.fromisoformat(str(row["ended_at"]))
        active_seconds += max(0.0, (ended - started).total_seconds())
    tokens = sum(
        effective_tokens(
            int(row["input_tokens"]),
            int(row["output_tokens"]),
            int(row["cache_write_tokens"]),
            int(row["cache_read_tokens"]),
        )
        for row in rows
    )
    return {
        "active_agent_seconds": active_seconds,
        "agent_launches": sum(1 for row in rows if row["stage"] != "review"),
        "effective_tokens": tokens,
        "remote_review_rounds": int(review_row["total"]) if review_row is not None else 0,
        "runs_by_status": dict(sorted(statuses.items())),
    }


@bench.command("serve")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path),
    envvar="SYMPHONY_BENCH_DB_PATH",
    default="/data/db/bench-control.sqlite",
    show_default=True,
)
@click.option(
    "--root",
    type=click.Path(path_type=Path),
    envvar="SYMPHONY_BENCH_ROOT",
    default="/data/bench",
    show_default=True,
)
@click.option(
    "--profile",
    "profile_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    envvar="SYMPHONY_BENCH_PROFILE",
)
@click.option("--api-token", envvar="SYMPHONY_BENCH_TOKEN")
@click.option("--github-owner", envvar="SYMPHONY_BENCH_GITHUB_OWNER", default="kulichevskiy")
@click.option(
    "--linear-team-id",
    envvar="SYMPHONY_BENCH_LINEAR_TEAM_ID",
    default="492bfef9-26d3-4469-9407-8bc1858ef9ef",
)
@click.option(
    "--linear-label-id",
    envvar="SYMPHONY_BENCH_LINEAR_LABEL_ID",
    default="b4b92569-f904-4a3f-bef1-ad22fa4851c7",
)
@click.option(
    "--linear-label-name",
    envvar="SYMPHONY_BENCH_LINEAR_LABEL_NAME",
    default="symphony-bench",
)
@click.option(
    "--symphony-repository",
    envvar="SYMPHONY_BENCH_REPOSITORY",
    default="https://github.com/kulichevskiy/symphony.git",
)
@click.option("--encryption-key", envvar="SYMPHONY_ENCRYPTION_KEY")
@click.option(
    "--executor-url",
    envvar="SYMPHONY_BENCH_EXECUTOR_URL",
    default="http://executor:8090",
    show_default=True,
)
@click.option("--executor-token", envvar="SYMPHONY_BENCH_EXECUTOR_TOKEN")
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", type=click.IntRange(1, 65535), default=8080, show_default=True)
def serve(
    db_path: Path,
    root: Path,
    profile_path: Path | None,
    api_token: str | None,
    github_owner: str,
    linear_team_id: str,
    linear_label_id: str,
    linear_label_name: str,
    symphony_repository: str,
    encryption_key: str | None,
    executor_url: str,
    executor_token: str | None,
    host: str,
    port: int,
) -> None:
    """Run the persistent Coolify bench worker."""
    import uvicorn

    mounted = _BenchSecrets()
    api_token = api_token or mounted.api_token
    encryption_key = encryption_key or mounted.encryption_key
    executor_token = executor_token or mounted.executor_token
    if not api_token:
        raise click.ClickException("SYMPHONY_BENCH_TOKEN is required")
    if not encryption_key:
        raise click.ClickException("SYMPHONY_ENCRYPTION_KEY is required")
    if not executor_token:
        raise click.ClickException("SYMPHONY_BENCH_EXECUTOR_TOKEN is required")
    asyncio.run(_initialize_control_db(db_path))
    packaged_profile = files("symphony.bench.assets").joinpath("profiles/current.json")
    if profile_path is not None:
        _serve_with_profile(
            profile_path,
            db_path=db_path,
            root=root,
            api_token=api_token,
            github_owner=github_owner,
            linear_team_id=linear_team_id,
            linear_label_id=linear_label_id,
            linear_label_name=linear_label_name,
            symphony_repository=symphony_repository,
            encryption_key=encryption_key,
            executor_url=executor_url,
            executor_token=executor_token,
            host=host,
            port=port,
            uvicorn_module=uvicorn,
        )
        return
    with as_file(packaged_profile) as resolved_profile:
        _serve_with_profile(
            resolved_profile,
            db_path=db_path,
            root=root,
            api_token=api_token,
            github_owner=github_owner,
            linear_team_id=linear_team_id,
            linear_label_id=linear_label_id,
            linear_label_name=linear_label_name,
            symphony_repository=symphony_repository,
            encryption_key=encryption_key,
            executor_url=executor_url,
            executor_token=executor_token,
            host=host,
            port=port,
            uvicorn_module=uvicorn,
        )


async def _initialize_control_db(path: Path) -> None:
    conn = await db.connect(path)
    await conn.close()


def _serve_with_profile(
    profile: Path,
    *,
    db_path: Path,
    root: Path,
    api_token: str,
    github_owner: str,
    linear_team_id: str,
    linear_label_id: str,
    linear_label_name: str,
    symphony_repository: str,
    encryption_key: str,
    executor_url: str,
    executor_token: str,
    host: str,
    port: int,
    uvicorn_module: Any,
) -> None:
    default_profile = _read_profile(profile)
    wall_time_cap_seconds = 8 * 60 * 60
    commands = RemoteCommands(
        base_url=executor_url,
        token=executor_token,
        timeout_seconds=wall_time_cap_seconds,
    )
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            control_db=db_path,
            github_owner=github_owner,
            linear_team_id=linear_team_id,
            linear_label_id=linear_label_id,
            linear_label_name=linear_label_name,
            symphony_repository=symphony_repository,
            encryption_key=encryption_key,
            wall_time_cap_seconds=wall_time_cap_seconds,
        ),
        commands=commands,
    )
    app = create_bench_app(
        db_path=db_path,
        api_token=api_token,
        execute=executor,
        default_profile=default_profile,
        resolve_revision=executor.resolve_revision,
        harness_version=harness_version(),
        recover_execution=commands.cancel_all,
    )
    uvicorn_module.run(app, host=host, port=port)


@bench.command("executor")
@click.option(
    "--root",
    type=click.Path(path_type=Path),
    envvar="SYMPHONY_BENCH_ROOT",
    default="/data/bench",
    show_default=True,
)
@click.option("--api-token", envvar="SYMPHONY_BENCH_EXECUTOR_TOKEN", required=True)
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", type=click.IntRange(1, 65535), default=8090, show_default=True)
def executor(root: Path, api_token: str, host: str, port: int) -> None:
    """Run the secret-isolated command executor on its private network."""
    import uvicorn

    uvicorn.run(create_executor_app(root=root, api_token=api_token), host=host, port=port)
