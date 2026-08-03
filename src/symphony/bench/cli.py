from __future__ import annotations

import asyncio
import json
import shutil
import time
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any

import click
import httpx
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .. import db
from ..config import RepoBinding, binding_natural_key
from .app import create_bench_app
from .campaign import harness_version
from .connection_sync import sync_connections as _sync_connections
from .executor import RemoteCommands, create_executor_app
from .grader import GraderInfrastructureError, SupportQueueGrader
from .harness import load_harness, snapshot_harness
from .live import LiveBenchConfig, LiveTrialExecutor
from .metrics import snapshot_candidate
from .models import ExperimentReport, Trial, TrialOutcome
from .report import render_markdown
from .reviewer import cleanup_stale_reviewer_credentials

_BENCH_BINDING_OVERRIDES = frozenset(
    {"allow_auto_merge", "auto_merge", "local_review", "remote_review"}
)


class _BenchSecrets(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_token: str = Field(default="", validation_alias="SYMPHONY_BENCH_TOKEN")
    encryption_key: str = Field(default="", validation_alias="SYMPHONY_ENCRYPTION_KEY")
    executor_token: str = Field(default="", validation_alias="SYMPHONY_BENCH_EXECUTOR_TOKEN")
    github_owner: str = Field(
        default="kulichevskiy", validation_alias="SYMPHONY_BENCH_GITHUB_OWNER"
    )
    linear_team_id: str = Field(
        default="492bfef9-26d3-4469-9407-8bc1858ef9ef",
        validation_alias="SYMPHONY_BENCH_LINEAR_TEAM_ID",
    )
    linear_label_id: str = Field(
        default="b4b92569-f904-4a3f-bef1-ad22fa4851c7",
        validation_alias="SYMPHONY_BENCH_LINEAR_LABEL_ID",
    )
    linear_label_name: str = Field(
        default="symphony-bench", validation_alias="SYMPHONY_BENCH_LINEAR_LABEL_NAME"
    )
    symphony_repository: str = Field(
        default="https://github.com/kulichevskiy/symphony.git",
        validation_alias="SYMPHONY_BENCH_REPOSITORY",
    )


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
    method: str,
    url: str,
    token: str,
    path: str,
    *,
    payload: dict[str, object] | None = None,
    timeout_seconds: float = 30,
) -> dict[str, Any]:
    try:
        response = httpx.request(
            method,
            f"{url.rstrip('/')}{path}",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=timeout_seconds,
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
        timeout_seconds=15 * 60,
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
    unsafe_binding_keys = set(binding_overrides) - _BENCH_BINDING_OVERRIDES
    if unsafe_binding_keys:
        joined = ", ".join(sorted(unsafe_binding_keys))
        raise click.ClickException(f"invalid profile binding override(s): {joined}")

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
            "uv run pytest && uv run ruff check . && uv run mypy support_queue "
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
    """Emit metrics with the harness-owned, read-only measurement engine."""
    click.echo(json.dumps(asyncio.run(_snapshot(db_path)), separators=(",", ":")))


async def _snapshot(db_path: Path) -> dict[str, object]:
    return await snapshot_candidate(db_path)


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
    "--root-a",
    type=click.Path(path_type=Path),
    envvar="SYMPHONY_BENCH_ROOT_A",
    default="/data/bench/a",
    show_default=True,
)
@click.option(
    "--root-b",
    type=click.Path(path_type=Path),
    envvar="SYMPHONY_BENCH_ROOT_B",
    default="/data/bench/b",
    show_default=True,
)
@click.option(
    "--private-root",
    type=click.Path(path_type=Path),
    envvar="SYMPHONY_BENCH_PRIVATE_ROOT",
    default="/data/db/bench-private",
    show_default=True,
)
@click.option(
    "--controls-root",
    type=click.Path(path_type=Path, exists=True, file_okay=False),
    envvar="SYMPHONY_BENCH_CONTROLS_ROOT",
    default="/run/symphony-bench-controls",
    show_default=True,
)
@click.option(
    "--profile",
    "profile_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    envvar="SYMPHONY_BENCH_PROFILE",
)
@click.option("--api-token", envvar="SYMPHONY_BENCH_TOKEN")
@click.option("--github-owner", envvar="SYMPHONY_BENCH_GITHUB_OWNER")
@click.option(
    "--linear-team-id",
    envvar="SYMPHONY_BENCH_LINEAR_TEAM_ID",
)
@click.option(
    "--linear-label-id",
    envvar="SYMPHONY_BENCH_LINEAR_LABEL_ID",
)
@click.option(
    "--linear-label-name",
    envvar="SYMPHONY_BENCH_LINEAR_LABEL_NAME",
)
@click.option(
    "--symphony-repository",
    envvar="SYMPHONY_BENCH_REPOSITORY",
)
@click.option("--encryption-key", envvar="SYMPHONY_ENCRYPTION_KEY")
@click.option(
    "--executor-a-url",
    envvar="SYMPHONY_BENCH_EXECUTOR_A_URL",
    default="http://bench-a:8090",
    show_default=True,
)
@click.option(
    "--executor-b-url",
    envvar="SYMPHONY_BENCH_EXECUTOR_B_URL",
    default="http://bench-b:8090",
    show_default=True,
)
@click.option("--executor-token", envvar="SYMPHONY_BENCH_EXECUTOR_TOKEN")
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", type=click.IntRange(1, 65535), default=8080, show_default=True)
def serve(
    db_path: Path,
    root_a: Path,
    root_b: Path,
    private_root: Path,
    controls_root: Path,
    profile_path: Path | None,
    api_token: str | None,
    github_owner: str | None,
    linear_team_id: str | None,
    linear_label_id: str | None,
    linear_label_name: str | None,
    symphony_repository: str | None,
    encryption_key: str | None,
    executor_a_url: str,
    executor_b_url: str,
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
    github_owner = github_owner or mounted.github_owner
    linear_team_id = linear_team_id or mounted.linear_team_id
    linear_label_id = linear_label_id or mounted.linear_label_id
    linear_label_name = linear_label_name or mounted.linear_label_name
    symphony_repository = symphony_repository or mounted.symphony_repository
    if not api_token:
        raise click.ClickException("SYMPHONY_BENCH_TOKEN is required")
    if not encryption_key:
        raise click.ClickException("SYMPHONY_ENCRYPTION_KEY is required")
    if not executor_token:
        raise click.ClickException("SYMPHONY_BENCH_EXECUTOR_TOKEN is required")
    asyncio.run(_initialize_control_db(db_path))
    packaged_profile = files("symphony.bench.assets").joinpath("profiles/current.json")

    def run_with_profile(resolved_profile: Path) -> None:
        _serve_with_profile(
            resolved_profile,
            db_path=db_path,
            root_a=root_a,
            root_b=root_b,
            private_root=private_root,
            controls_root=controls_root,
            api_token=api_token,
            github_owner=github_owner,
            linear_team_id=linear_team_id,
            linear_label_id=linear_label_id,
            linear_label_name=linear_label_name,
            symphony_repository=symphony_repository,
            encryption_key=encryption_key,
            executor_a_url=executor_a_url,
            executor_b_url=executor_b_url,
            executor_token=executor_token,
            host=host,
            port=port,
            uvicorn_module=uvicorn,
        )

    if profile_path is not None:
        run_with_profile(profile_path)
        return
    with as_file(packaged_profile) as resolved_profile:
        run_with_profile(resolved_profile)


async def _initialize_control_db(path: Path) -> None:
    conn = await db.connect(path)
    await conn.close()


def _cleanup_stale_grader_preflights(root: Path) -> int:
    removed = 0
    if not root.exists():
        return removed
    for path in root.glob(".grader-preflight-EXP-*"):
        if path.is_dir():
            path.chmod(path.stat().st_mode | 0o700)
            for directory in (item for item in path.rglob("*") if item.is_dir()):
                directory.chmod(directory.stat().st_mode | 0o700)
            shutil.rmtree(path)
            removed += 1
    return removed


async def _prepare_harness_with_preflight(
    experiment_id: str,
    *,
    private_root: Path,
    controls_root: Path,
    lanes: tuple[tuple[str, Path, RemoteCommands], ...],
) -> str:
    started = time.monotonic()
    snapshot = private_root / experiment_id / "_harness"
    try:
        version = await asyncio.to_thread(snapshot_harness, snapshot, controls_root=controls_root)
        frozen = load_harness(snapshot)
    except BaseException:
        await asyncio.to_thread(shutil.rmtree, snapshot, ignore_errors=True)
        raise

    async def validate_lane(name: str, root: Path, commands: RemoteCommands) -> dict[str, object]:
        preflight = root / f".grader-preflight-{experiment_id}"
        await asyncio.to_thread(preflight.mkdir, parents=True, exist_ok=False)
        try:
            seed = preflight / "seed"
            reference = preflight / "reference"
            await asyncio.to_thread(shutil.copytree, frozen.root / "support_queue", seed)
            await asyncio.to_thread(shutil.copytree, frozen.reference_root, reference)
            mutations: dict[str, Path] = {}
            for mutation_name, mutation_root in frozen.mutation_roots.items():
                mutation = preflight / mutation_name
                await asyncio.to_thread(shutil.copytree, mutation_root, mutation)
                mutations[mutation_name] = mutation
            backend_hidden = preflight / "backend_hidden_test.py"
            frontend_hidden = preflight / "frontend_hidden_test.tsx"
            await asyncio.to_thread(shutil.copyfile, frozen.backend_hidden_test, backend_hidden)
            await asyncio.to_thread(shutil.copyfile, frozen.frontend_hidden_test, frontend_hidden)
            controls = await SupportQueueGrader(commands).validate_controls(
                seed_root=seed,
                reference_root=reference,
                mutation_roots=mutations,
                results_root=preflight / "results",
                backend_hidden_test=backend_hidden,
                frontend_hidden_test=frontend_hidden,
                manifest=frozen.hidden_manifest,
            )
            return {"lane": name, "status": "passed", "controls": controls}
        finally:
            await asyncio.to_thread(shutil.rmtree, preflight, ignore_errors=True)

    results = await asyncio.gather(
        *(validate_lane(name, root, commands) for name, root, commands in lanes),
        return_exceptions=True,
    )
    failures = [result for result in results if isinstance(result, BaseException)]
    receipt = {
        "experiment_id": experiment_id,
        "harness_version": version,
        "status": "infrastructure_failed" if failures else "passed",
        "duration_seconds": round(time.monotonic() - started, 3),
        "lanes": [
            {"status": "infrastructure_failed", "error": str(result)}
            if isinstance(result, BaseException)
            else result
            for result in results
        ],
    }
    await asyncio.to_thread(
        (private_root / experiment_id / "preflight.json").write_text,
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if failures:
        detail = "; ".join(str(failure) for failure in failures)
        await asyncio.to_thread(shutil.rmtree, snapshot, ignore_errors=True)
        raise GraderInfrastructureError(f"grader preflight failed on a lane: {detail}")
    return version


def _serve_with_profile(
    profile: Path,
    *,
    db_path: Path,
    root_a: Path,
    root_b: Path,
    private_root: Path,
    controls_root: Path,
    api_token: str,
    github_owner: str,
    linear_team_id: str,
    linear_label_id: str,
    linear_label_name: str,
    symphony_repository: str,
    encryption_key: str,
    executor_a_url: str,
    executor_b_url: str,
    executor_token: str,
    host: str,
    port: int,
    uvicorn_module: Any,
) -> None:
    default_profile = _read_profile(profile)
    wall_time_cap_seconds = 8 * 60 * 60
    commands_a = RemoteCommands(
        base_url=executor_a_url,
        token=executor_token,
        timeout_seconds=wall_time_cap_seconds,
    )
    commands_b = RemoteCommands(
        base_url=executor_b_url,
        token=executor_token,
        timeout_seconds=wall_time_cap_seconds,
    )

    async def recover_execution() -> None:
        await asyncio.gather(commands_a.cancel_all(), commands_b.cancel_all())
        await asyncio.gather(
            asyncio.to_thread(cleanup_stale_reviewer_credentials, root_a),
            asyncio.to_thread(cleanup_stale_reviewer_credentials, root_b),
            asyncio.to_thread(_cleanup_stale_grader_preflights, root_a),
            asyncio.to_thread(_cleanup_stale_grader_preflights, root_b),
        )

    def live_executor(root: Path, commands: RemoteCommands) -> LiveTrialExecutor:
        return LiveTrialExecutor(
            config=LiveBenchConfig(
                root=root,
                private_root=private_root,
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

    executor_a = live_executor(root_a, commands_a)
    executor_b = live_executor(root_b, commands_b)

    async def execute(trial: Trial) -> TrialOutcome:
        executor = executor_b if trial.candidate == "B" else executor_a
        return await executor(trial)

    async def resolve_revision(revision: str) -> str:
        return await executor_a.resolve_revision(revision)

    async def prepare_harness(experiment_id: str) -> str:
        return await _prepare_harness_with_preflight(
            experiment_id,
            private_root=private_root,
            controls_root=controls_root,
            lanes=(("A", root_a, commands_a), ("B", root_b, commands_b)),
        )

    app = create_bench_app(
        db_path=db_path,
        api_token=api_token,
        execute=execute,
        default_profile=default_profile,
        resolve_revision=resolve_revision,
        harness_version=harness_version(),
        prepare_harness=prepare_harness,
        recover_execution=recover_execution,
        reports_root=private_root / "reports",
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
