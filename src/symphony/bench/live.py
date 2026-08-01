from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import shutil
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeVar

from .. import db
from ..config import Secrets
from ..credentials import CredentialResolver, RunCredentials
from ..crypto import CredentialCipher
from ..ui.oauth import linear_provider
from .connection_sync import snapshot_connections, sync_connections
from .eventdesk import Campaign, eventdesk_campaign, materialize_eventdesk
from .github import Commands, GitHubRepository, GitHubSandbox, SubprocessCommands
from .grader import EventDeskGrader
from .linear import LinearCampaign, LinearIssueState, LinearSandbox
from .models import (
    Trial,
    TrialExecutionCancelled,
    TrialExecutionError,
    TrialOutcome,
)
from .reviewer import CodexFinalReviewer

T = TypeVar("T")


class GitHubProvisioner(Protocol):
    async def create_repository(self, *, name: str, source: Path) -> GitHubRepository: ...

    async def review_metrics(self, *, repository_slug: str) -> dict[str, int]: ...


class LinearProvisioner(Protocol):
    async def create_campaign(
        self,
        *,
        team_id: str,
        label: str,
        repo_url: str,
        campaign: Campaign,
    ) -> LinearCampaign: ...

    async def issue_states(self, issue_ids: tuple[str, ...]) -> tuple[LinearIssueState, ...]: ...


class ProductGrader(Protocol):
    async def grade(
        self,
        *,
        repository_slug: str,
        destination: Path,
        github_token: str,
    ) -> dict[str, object]: ...


class ProductReviewer(Protocol):
    async def review(self, *, checkout: Path) -> dict[str, object]: ...


@dataclass(frozen=True)
class LiveBenchConfig:
    root: Path
    control_db: Path
    github_owner: str
    linear_team_id: str
    symphony_repository: str
    encryption_key: str
    linear_label_id: str = "b4b92569-f904-4a3f-bef1-ad22fa4851c7"
    linear_label_name: str = "symphony-bench"
    poll_seconds: float = 20
    wall_time_cap_seconds: float = 8 * 60 * 60
    observed_token_cap: float = 300_000_000
    agent_launch_cap: int = 120
    receipt_timeout_seconds: float = 30
    provision_attempts: int = 3
    provision_retry_seconds: float = 5


class LiveTrialExecutor:
    def __init__(
        self,
        *,
        config: LiveBenchConfig,
        commands: Commands | None = None,
        credentials: RunCredentials | None = None,
        github: GitHubProvisioner | None = None,
        linear: LinearProvisioner | None = None,
        grader: ProductGrader | None = None,
        reviewer: ProductReviewer | None = None,
    ) -> None:
        self._config = config
        self._commands = commands or SubprocessCommands()
        self._credentials = credentials
        self._github = github
        self._linear = linear
        self._grader = grader or EventDeskGrader(self._commands)
        self._reviewer = reviewer or CodexFinalReviewer(
            commands=self._commands,
            control_db=config.control_db,
            encryption_key=config.encryption_key,
        )

    async def __call__(self, trial: Trial) -> TrialOutcome:
        cancelled_outcome: asyncio.Future[TrialOutcome] = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(self._execute_trial(trial, cancelled_outcome))
        try:
            done, _ = await asyncio.wait({task}, timeout=self._config.wall_time_cap_seconds)
        except asyncio.CancelledError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            outcome = cancelled_outcome.result() if cancelled_outcome.done() else TrialOutcome()
            raise TrialExecutionCancelled(outcome=outcome) from None
        if task in done:
            return task.result()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        outcome = cancelled_outcome.result() if cancelled_outcome.done() else TrialOutcome()
        raise TrialExecutionError("bench wall-time safety cap exceeded", outcome=outcome)

    async def _execute_trial(
        self, trial: Trial, cancelled_outcome: asyncio.Future[TrialOutcome]
    ) -> TrialOutcome:
        started = asyncio.get_running_loop().time()
        suffix = "SMOKE" if trial.candidate == "S" else f"{trial.candidate}{trial.repetition}"
        trial_name = f"{trial.experiment_id}-{suffix}"
        trial_root = self._config.root / trial.experiment_id / suffix
        eventdesk_root = trial_root / "eventdesk"
        candidate_root = trial_root / "symphony"
        repository: GitHubRepository | None = None
        campaign: LinearCampaign | None = None
        metrics: dict[str, object] = {}
        linear: LinearProvisioner | None = None
        owns_linear = False
        try:
            credentials = self._credentials or await self._resolve_credentials()
            if not credentials.github_token:
                raise RuntimeError("bench GitHub connection is missing; reconnect and retry")
            if not credentials.linear_token:
                raise RuntimeError("bench Linear connection is missing; reconnect and retry")

            await asyncio.to_thread(trial_root.mkdir, parents=True, exist_ok=False)
            await asyncio.to_thread(materialize_eventdesk, eventdesk_root)
            github = self._github or GitHubSandbox(
                owner=self._config.github_owner,
                token=credentials.github_token,
                commands=self._commands,
            )
            repository = await self._retry_provision(
                lambda: github.create_repository(name=trial_name, source=eventdesk_root)
            )

            owns_linear = self._linear is None
            linear = self._linear or LinearSandbox(
                credentials.linear_token,
                routing_label_id=self._config.linear_label_id,
            )
            campaign = await self._retry_provision(
                lambda: linear.create_campaign(
                    team_id=self._config.linear_team_id,
                    label=trial_name,
                    repo_url=repository.url,
                    campaign=eventdesk_campaign(),
                )
            )
            connections_db = trial_root / "connections.sqlite"
            await snapshot_connections(self._config.control_db, connections_db)
            await self._prepare_candidate(
                trial=trial,
                candidate_root=candidate_root,
                trial_root=trial_root,
                repository=repository,
                github_token=credentials.github_token,
                connections_db=connections_db,
            )
            metrics.update(
                await self._run_until_done(
                    linear=linear,
                    campaign=campaign,
                    candidate_root=candidate_root,
                    trial_root=trial_root,
                    started=started,
                )
            )
            metrics.update(
                await self._grader.grade(
                    repository_slug=repository.slug,
                    destination=trial_root,
                    github_token=credentials.github_token,
                )
            )
            metrics.update(await self._reviewer.review(checkout=trial_root / "final"))
            metrics.update(await github.review_metrics(repository_slug=repository.slug))
        except asyncio.CancelledError:
            outcome = await self._bounded_partial_outcome(
                repository=repository,
                campaign=campaign,
                linear=linear,
                candidate_root=candidate_root,
                candidate_db=trial_root / "candidate.sqlite",
                metrics=metrics,
                started=started,
            )
            if not cancelled_outcome.done():
                cancelled_outcome.set_result(outcome)
            raise
        except Exception as exc:
            outcome = await self._bounded_partial_outcome(
                repository=repository,
                campaign=campaign,
                linear=linear,
                candidate_root=candidate_root,
                candidate_db=trial_root / "candidate.sqlite",
                metrics=metrics,
                started=started,
            )
            raise TrialExecutionError(
                str(exc),
                outcome=outcome,
            ) from exc
        finally:
            if owns_linear:
                assert isinstance(linear, LinearSandbox)
                await linear.aclose()
        metrics["wall_seconds"] = asyncio.get_running_loop().time() - started
        return TrialOutcome(
            repository_url=repository.url,
            issue_urls=list(campaign.issue_urls),
            metrics=metrics,
        )

    async def _partial_outcome(
        self,
        *,
        repository: GitHubRepository | None,
        campaign: LinearCampaign | None,
        linear: LinearProvisioner | None,
        candidate_root: Path,
        candidate_db: Path,
        metrics: dict[str, object],
        started: float,
    ) -> TrialOutcome:
        candidate_db_exists, candidate_root_exists = await asyncio.gather(
            asyncio.to_thread(candidate_db.exists),
            asyncio.to_thread(candidate_root.exists),
        )
        if candidate_db_exists and candidate_root_exists:
            try:
                metrics.update(await self._candidate_snapshot(candidate_root, candidate_db))
            except Exception:  # noqa: BLE001 - best-effort failure receipt
                pass
        if campaign is not None and linear is not None:
            try:
                states = await linear.issue_states(campaign.issue_ids)
                metrics["completed_tickets"] = sum(state.type == "completed" for state in states)
            except Exception:  # noqa: BLE001 - best-effort failure receipt
                pass
        metrics["wall_seconds"] = asyncio.get_running_loop().time() - started
        return TrialOutcome(
            repository_url=repository.url if repository is not None else None,
            issue_urls=list(campaign.issue_urls) if campaign is not None else [],
            metrics=metrics,
        )

    async def _bounded_partial_outcome(
        self,
        *,
        repository: GitHubRepository | None,
        campaign: LinearCampaign | None,
        linear: LinearProvisioner | None,
        candidate_root: Path,
        candidate_db: Path,
        metrics: dict[str, object],
        started: float,
    ) -> TrialOutcome:
        try:
            return await asyncio.wait_for(
                self._partial_outcome(
                    repository=repository,
                    campaign=campaign,
                    linear=linear,
                    candidate_root=candidate_root,
                    candidate_db=candidate_db,
                    metrics=metrics,
                    started=started,
                ),
                timeout=self._config.receipt_timeout_seconds,
            )
        except TimeoutError:
            return TrialOutcome(
                repository_url=repository.url if repository is not None else None,
                issue_urls=list(campaign.issue_urls) if campaign is not None else [],
                metrics={
                    **metrics,
                    "wall_seconds": asyncio.get_running_loop().time() - started,
                    "partial_receipt_timed_out": True,
                },
            )

    async def resolve_revision(self, revision: str) -> str:
        """Resolve a branch/tag once at submission; trials only receive full SHAs."""
        if re.fullmatch(r"[0-9a-fA-F]{40}", revision):
            return revision.lower()
        credentials = self._credentials or await self._resolve_credentials()
        if not credentials.github_token:
            raise RuntimeError("bench GitHub connection is missing")
        output = await self._commands.run(
            [
                "git",
                "ls-remote",
                self._config.symphony_repository,
                revision,
                f"refs/heads/{revision}",
                f"refs/tags/{revision}",
            ],
            cwd=self._config.root,
            env={"GH_TOKEN": credentials.github_token},
        )
        matches = {
            line.split()[0].lower()
            for line in output.splitlines()
            if line.split() and re.fullmatch(r"[0-9a-fA-F]{40}", line.split()[0])
        }
        if len(matches) != 1:
            raise RuntimeError(f"revision {revision!r} resolved to {len(matches)} commits")
        return matches.pop()

    async def _retry_provision(self, operation: Callable[[], Awaitable[T]]) -> T:
        last_error: Exception | None = None
        for attempt in range(1, self._config.provision_attempts + 1):
            try:
                return await operation()
            except Exception as exc:  # noqa: BLE001 - bounded before agent dispatch
                last_error = exc
                if attempt == self._config.provision_attempts:
                    raise
                await asyncio.sleep(self._config.provision_retry_seconds)
        assert last_error is not None  # pragma: no cover
        raise last_error

    async def _resolve_credentials(self) -> RunCredentials:
        conn = await db.connect(self._config.control_db)
        try:
            secrets = Secrets()
            provider = linear_provider(
                secrets.linear_oauth_client_id, secrets.linear_oauth_client_secret
            )
            resolver = CredentialResolver(
                conn,
                CredentialCipher(self._config.encryption_key),
                linear_oauth_provider=provider if provider.configured else None,
            )
            return await resolver.resolve_run_credentials(
                github_fallback=os.environ.get("GH_TOKEN"),
                linear_fallback=os.environ.get("LINEAR_API_KEY"),
            )
        finally:
            await conn.close()

    async def _prepare_candidate(
        self,
        *,
        trial: Trial,
        candidate_root: Path,
        trial_root: Path,
        repository: GitHubRepository,
        github_token: str,
        connections_db: Path,
    ) -> None:
        await self._commands.run(
            [
                "git",
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                self._config.symphony_repository,
                candidate_root.name,
            ],
            cwd=trial_root,
            env={"GH_TOKEN": github_token},
        )
        profile_path = trial_root / "candidate-profile.json"
        await asyncio.to_thread(
            profile_path.write_text,
            json.dumps(trial.profile, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        await self._commands.run(
            ["git", "checkout", "--detach", trial.revision], cwd=candidate_root
        )
        await self._commands.run(["uv", "sync", "--frozen", "--no-dev"], cwd=candidate_root)
        await self._commands.run(
            [
                "uv",
                "run",
                "--frozen",
                "--no-sync",
                "--no-dev",
                "symphony",
                "bench",
                "seed",
                "--db",
                str(trial_root / "candidate.sqlite"),
                "--profile",
                str(profile_path),
                "--linear-team",
                "BENCH",
                "--github-repo",
                repository.slug,
                "--issue-label",
                self._config.linear_label_name,
                "--connections-db",
                str(connections_db),
            ],
            cwd=candidate_root,
            env={"SYMPHONY_ENCRYPTION_KEY": self._config.encryption_key},
        )
        # The candidate needs runtime code, not the grader source or its own
        # Git object database. Remove both before any agent is dispatched.
        await asyncio.to_thread(
            shutil.rmtree,
            candidate_root / "src/symphony/bench/assets/hidden",
            True,
        )
        await asyncio.to_thread(shutil.rmtree, candidate_root / ".git", True)

    async def _run_until_done(
        self,
        *,
        linear: LinearProvisioner,
        campaign: LinearCampaign,
        candidate_root: Path,
        trial_root: Path,
        started: float,
    ) -> dict[str, object]:
        candidate_db = trial_root / "candidate.sqlite"
        candidate_env = {
            "SYMPHONY_DB_PATH": str(candidate_db),
            "SYMPHONY_LOG_ROOT": str(trial_root / "logs"),
            "SYMPHONY_WORKSPACE_ROOT": str(trial_root / "workspaces"),
            "SYMPHONY_ENCRYPTION_KEY": self._config.encryption_key,
        }
        candidate = asyncio.create_task(
            self._commands.run(
                [
                    "uv",
                    "run",
                    "--frozen",
                    "--no-sync",
                    "--no-dev",
                    "symphony",
                ],
                cwd=candidate_root,
                env=candidate_env,
            )
        )
        try:
            while True:
                await asyncio.sleep(0)
                if candidate.done():
                    candidate.result()
                    raise RuntimeError("candidate Symphony exited before campaign completed")
                elapsed = asyncio.get_running_loop().time() - started
                if elapsed >= self._config.wall_time_cap_seconds:
                    raise RuntimeError("bench wall-time safety cap exceeded")
                await sync_connections(candidate_db, self._config.control_db)
                metrics = await self._candidate_snapshot(candidate_root, candidate_db)
                token_total = metrics.get("effective_tokens")
                launch_total = metrics.get("agent_launches")
                if not isinstance(token_total, (int, float)) or not isinstance(launch_total, int):
                    raise RuntimeError("candidate bench snapshot is missing safety metrics")
                if float(token_total) > self._config.observed_token_cap:
                    raise RuntimeError("bench observed-token safety cap exceeded")
                if launch_total > self._config.agent_launch_cap:
                    raise RuntimeError("bench agent-launch safety cap exceeded")

                states = await linear.issue_states(campaign.issue_ids)
                if all(state.type == "completed" for state in states):
                    metrics["completed_tickets"] = len(states)
                    return metrics
                failed = [
                    state.identifier
                    for state in states
                    if state.type == "canceled" or state.name == "Needs Input"
                ]
                if failed:
                    raise RuntimeError(f"candidate trial stopped on {', '.join(failed)}")
                await asyncio.sleep(self._config.poll_seconds)
        finally:
            candidate.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await candidate

    async def _candidate_snapshot(
        self, candidate_root: Path, candidate_db: Path
    ) -> dict[str, object]:
        snapshot_raw = await self._commands.run(
            [
                "uv",
                "run",
                "--frozen",
                "--no-sync",
                "--no-dev",
                "symphony",
                "bench",
                "snapshot",
                "--db",
                str(candidate_db),
            ],
            cwd=candidate_root,
        )
        try:
            parsed = json.loads(snapshot_raw.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError) as exc:
            raise RuntimeError("candidate returned an invalid bench snapshot") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("candidate returned a non-object bench snapshot")
        return {str(key): value for key, value in parsed.items()}
