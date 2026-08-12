from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import tempfile
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
from .maintainability import (
    ClaudeOpusMediumProbeAgent,
    SubprocessMutationRunner,
    WaitingOnCustomerVerifier,
    analyze_static,
    discover_mutants,
    load_existing_results,
    load_probe_receipt,
    run_maintenance_probe,
    run_mutation_pack,
    write_reports,
)
from .metrics import snapshot_candidate
from .models import Experiment, ExperimentReport, Trial, TrialOutcome
from .report import render_markdown
from .reviewer import cleanup_stale_reviewer_credentials

_BENCH_BINDING_OVERRIDES = frozenset(
    {
        "allow_auto_merge",
        "auto_merge",
        "local_review",
        "local_review_mode",
        "remote_review",
    }
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
@click.option(
    "--mode",
    type=click.Choice(("paired", "single")),
    default="paired",
    show_default=True,
)
@click.option("--candidate-a", required=True, help="Git revision for candidate A.")
@click.option("--candidate-b", help="Git revision for candidate B (paired mode only).")
@click.option(
    "--hypothesis",
    required=True,
    help="Plain-English claim that this experiment tests.",
)
@click.option(
    "--design",
    required=True,
    help="Plain-English explanation of how the experiment tests the hypothesis.",
)
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
    mode: str,
    candidate_a: str,
    candidate_b: str | None,
    hypothesis: str,
    design: str,
    profile_a: Path | None,
    profile_b: Path | None,
    repetitions: int,
) -> None:
    """Queue a paired experiment or one single-candidate trial."""
    if mode == "paired" and candidate_b is None:
        raise click.UsageError("--candidate-b is required in paired mode")
    if mode == "single":
        payload: dict[str, object] = {
            "mode": mode,
            "candidate_a": candidate_a,
            "hypothesis": hypothesis,
            "design": design,
            "repetitions": repetitions,
        }
    else:
        payload = {
            "candidate_a": candidate_a,
            "candidate_b": candidate_b,
            "hypothesis": hypothesis,
            "design": design,
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
        "required_status_checks": ["backend", "frontend"],
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


@bench.command("maintainability")
@click.option("--repository", required=True, help="Local checkout path or GitHub owner/repo.")
@click.option(
    "--output",
    type=click.Path(path_type=Path, file_okay=False),
    required=True,
)
@click.option(
    "--static-only",
    is_flag=True,
    help="Write deterministic static diagnostics without mutation tests or a maintenance probe.",
)
@click.option("--mutations", is_flag=True, help="Run the fixed mutation-operator pack.")
@click.option("--mutation-timeout", type=click.FloatRange(min=1), default=300, show_default=True)
@click.option(
    "--probe",
    is_flag=True,
    help="Run the waiting_on_customer change with Claude Opus medium.",
)
@click.option(
    "--probe-receipt",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    help="Import a production probe receipt into the canonical report.",
)
@click.option("--probe-timeout", type=click.FloatRange(min=60), default=7200, show_default=True)
def maintainability(
    repository: str,
    output: Path,
    static_only: bool,
    mutations: bool,
    mutation_timeout: float,
    probe: bool,
    probe_receipt: Path | None,
    probe_timeout: float,
) -> None:
    """Grade maintainability separately from the benchmark's functional score."""

    selected = sum((static_only, mutations, probe, probe_receipt is not None))
    if selected != 1:
        raise click.ClickException(
            "choose exactly one of --static-only, --mutations, --probe, or --probe-receipt"
        )
    existing_mutations, existing_probe, existing_errors = load_existing_results(output)
    local = Path(repository).expanduser()
    if local.exists() and static_only:
        checkout = local.resolve()
        write_reports(
            output=output,
            repository=repository,
            static=analyze_static(checkout),
            mutations=existing_mutations,
            probe=existing_probe,
            errors=existing_errors,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="symphony-maintainability-") as temporary:
            checkout = Path(temporary) / "checkout"
            if local.exists():
                shutil.copytree(local.resolve(), checkout, ignore=shutil.ignore_patterns(".git"))
            else:
                try:
                    subprocess.run(
                        ["gh", "repo", "clone", repository, str(checkout), "--", "--depth=1"],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                except (OSError, subprocess.CalledProcessError) as exc:
                    raise click.ClickException(f"could not clone {repository}: {exc}") from exc
            mutation_outcomes = existing_mutations
            errors = existing_errors
            probe_outcome = existing_probe
            if mutations:
                try:
                    subprocess.run(
                        ["uv", "sync", "--locked"],
                        cwd=checkout,
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    subprocess.run(
                        ["npm", "ci"],
                        cwd=checkout / "frontend",
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                except (OSError, subprocess.CalledProcessError) as exc:
                    raise click.ClickException(f"could not prepare mutation tests: {exc}") from exc
                try:
                    mutation_outcomes = run_mutation_pack(
                        checkout,
                        mutants=discover_mutants(checkout, limit=24),
                        runner=SubprocessMutationRunner(),
                        timeout_seconds=mutation_timeout,
                    )
                except RuntimeError as exc:
                    mutation_outcomes = ()
                    errors = (str(exc),)
            if probe:
                probe_outcome = run_maintenance_probe(
                    checkout,
                    agent=ClaudeOpusMediumProbeAgent(),
                    verifier=WaitingOnCustomerVerifier(),
                    timeout_seconds=probe_timeout,
                )
            if probe_receipt is not None:
                try:
                    identity_checkout = local.resolve() if local.exists() else checkout
                    baseline_sha = subprocess.run(
                        ["git", "rev-parse", "HEAD"],
                        cwd=identity_checkout,
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout.strip()
                    probe_outcome = load_probe_receipt(
                        probe_receipt,
                        repository=repository,
                        baseline_sha=baseline_sha,
                    )
                except (OSError, ValueError, json.JSONDecodeError, TypeError) as exc:
                    raise click.ClickException(f"invalid probe receipt: {exc}") from exc
                except subprocess.CalledProcessError as exc:
                    raise click.ClickException(
                        "could not resolve repository baseline for probe receipt"
                    ) from exc
            write_reports(
                output=output,
                repository=repository,
                static=analyze_static(checkout),
                mutations=mutation_outcomes,
                probe=probe_outcome,
                errors=errors,
            )
    click.echo(f"wrote {output / 'MAINTAINABILITY.json'} and MAINTAINABILITY.md")


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
    version = await _snapshot_harness(
        experiment_id,
        private_root=private_root,
        controls_root=controls_root,
    )
    try:
        return await _preflight_harness(
            experiment_id,
            private_root=private_root,
            lanes=lanes,
            expected_version=version,
        )
    except BaseException:
        await asyncio.to_thread(
            shutil.rmtree,
            private_root / experiment_id / "_harness",
            ignore_errors=True,
        )
        raise


async def _snapshot_harness(
    experiment_id: str,
    *,
    private_root: Path,
    controls_root: Path,
) -> str:
    snapshot = private_root / experiment_id / "_harness"
    try:
        version = await asyncio.to_thread(snapshot_harness, snapshot, controls_root=controls_root)
    except BaseException:
        await asyncio.to_thread(shutil.rmtree, snapshot, ignore_errors=True)
        raise
    return version


async def _preflight_harness(
    experiment_id: str,
    *,
    private_root: Path,
    lanes: tuple[tuple[str, Path, RemoteCommands], ...],
    expected_version: str,
) -> str:
    started = time.monotonic()
    snapshot = private_root / experiment_id / "_harness"
    frozen = load_harness(snapshot)
    if frozen.version != expected_version:
        raise GraderInfrastructureError(
            f"harness version changed: expected {expected_version}, got {frozen.version}"
        )
    version = frozen.version

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
        executor = executor_b if trial.execution_lane == "B" else executor_a
        return await executor(trial)

    async def publish_interrupted(trial: Trial) -> None:
        executor = executor_b if trial.execution_lane == "B" else executor_a
        await executor.publish_interrupted(trial)

    async def start_experiment(experiment: Experiment) -> str:
        return await executor_a.start_experiment(experiment)

    async def publish_failed_experiment(experiment: Experiment) -> None:
        await executor_a.publish_failed_experiment(experiment)

    async def resolve_revision(revision: str) -> str:
        return await executor_a.resolve_revision(revision)

    async def prepare_harness(experiment: Experiment) -> str:
        lane_by_name = {
            "A": ("A", root_a, commands_a),
            "B": ("B", root_b, commands_b),
        }
        lanes: tuple[tuple[str, Path, RemoteCommands], ...]
        if experiment.execution_lane == "AB":
            lanes = (lane_by_name["A"], lane_by_name["B"])
        elif experiment.execution_lane in {"A", "B"}:
            lanes = (lane_by_name[experiment.execution_lane],)
        else:
            raise RuntimeError("experiment has no execution lane")
        return await _preflight_harness(
            experiment.id,
            private_root=private_root,
            lanes=lanes,
            expected_version=experiment.harness_version,
        )

    async def snapshot_experiment_harness(experiment_id: str) -> str:
        return await _snapshot_harness(
            experiment_id,
            private_root=private_root,
            controls_root=controls_root,
        )

    app = create_bench_app(
        db_path=db_path,
        api_token=api_token,
        execute=execute,
        default_profile=default_profile,
        resolve_revision=resolve_revision,
        harness_version=harness_version(),
        snapshot_harness=snapshot_experiment_harness,
        prepare_harness=prepare_harness,
        recover_execution=recover_execution,
        recover_chronicle=executor_a.recover_chronicle,
        start_experiment=start_experiment,
        publish_interrupted=publish_interrupted,
        publish_failed_experiment=publish_failed_experiment,
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
