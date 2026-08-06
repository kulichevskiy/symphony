from __future__ import annotations

import asyncio
import contextlib
import json
import logging
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
from .campaign import Campaign
from .chronicle import (
    failed_experiment_update,
    failed_run_update,
    launch_update,
    project_description,
    successful_run_update,
)
from .connection_sync import reconcile_connections, snapshot_connections
from .github import Commands, GitHubRepository, GitHubSandbox, SubprocessCommands
from .grader import GraderInfrastructureError, HiddenManifest, SupportQueueGrader
from .harness import FrozenHarness, load_harness
from .linear import LinearCampaign, LinearIssueState, LinearSandbox
from .metrics import snapshot_candidate
from .models import (
    Experiment,
    Trial,
    TrialExecutionCancelled,
    TrialExecutionError,
    TrialOutcome,
)
from .reviewer import CodexFinalReviewer

T = TypeVar("T")
CandidateSnapshotter = Callable[[Path, Path], Awaitable[dict[str, object]]]
log = logging.getLogger(__name__)
_TRIAL_DIRECTORY_RE = re.compile(r"[AB][1-9][0-9]*\Z")
_CHRONICLE_LOCKS: dict[str, asyncio.Lock] = {}


class GitHubProvisioner(Protocol):
    async def create_repository(self, *, name: str, source: Path) -> GitHubRepository: ...

    async def review_metrics(self, *, repository_slug: str, cwd: Path) -> dict[str, int]: ...

    async def archive_repository(self, *, repository_slug: str, cwd: Path) -> None: ...


class LinearProvisioner(Protocol):
    async def ensure_project(
        self,
        *,
        team_id: str,
        experiment_id: str,
        campaign: Campaign,
        project_description: str,
    ) -> str: ...

    async def create_campaign(
        self,
        *,
        team_id: str,
        label: str,
        repo_url: str,
        campaign: Campaign,
        project_description: str = "",
        project_id: str = "",
    ) -> LinearCampaign: ...

    async def issue_states(self, issue_ids: tuple[str, ...]) -> tuple[LinearIssueState, ...]: ...

    async def publish_project_update(
        self,
        *,
        project_id: str,
        health: str,
        body: str,
        event_key: str | None = None,
    ) -> None: ...


class ProductGrader(Protocol):
    async def grade(
        self,
        *,
        repository_slug: str,
        destination: Path,
        github_token: str,
        backend_hidden_test: Path,
        frontend_hidden_test: Path,
        manifest: HiddenManifest,
        checks: dict[str, list[str]] | None = None,
    ) -> dict[str, object]: ...


class ProductReviewer(Protocol):
    async def review(
        self,
        *,
        checkout: Path,
        spec_prompt: str | None = None,
        standards_prompt: str | None = None,
    ) -> dict[str, object]: ...


@dataclass(frozen=True)
class LiveBenchConfig:
    root: Path
    private_root: Path
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
        candidate_snapshotter: CandidateSnapshotter = snapshot_candidate,
    ) -> None:
        self._config = config
        self._commands = commands or SubprocessCommands()
        self._credentials = credentials
        self._github = github
        self._linear = linear
        self._grader = grader or SupportQueueGrader(self._commands)
        self._reviewer = reviewer or CodexFinalReviewer(
            commands=self._commands,
            control_db=config.control_db,
            encryption_key=config.encryption_key,
        )
        self._candidate_snapshotter = candidate_snapshotter

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
        await asyncio.to_thread(
            _archive_shared_trials, self._config.root, self._config.private_root
        )
        started = asyncio.get_running_loop().time()
        suffix = f"{trial.candidate}{trial.repetition}"
        trial_name = f"{trial.experiment_id}-{suffix}"
        trial_root = self._config.root / trial.experiment_id / suffix
        app_root = trial_root / "support_queue"
        candidate_root = trial_root / "symphony"
        repository: GitHubRepository | None = None
        github: GitHubProvisioner | None = None
        campaign: LinearCampaign | None = None
        metrics: dict[str, object] = {}
        linear: LinearProvisioner | None = None
        owns_linear = False
        archive_error: Exception | None = None
        try:
            frozen = self._load_trial_harness(trial)
            credentials = self._credentials or await self._resolve_credentials()
            if not credentials.linear_token:
                raise RuntimeError("bench Linear connection is missing; reconnect and retry")

            owns_linear = self._linear is None
            linear = self._linear or LinearSandbox(
                credentials.linear_token,
                routing_label_id=self._config.linear_label_id,
                authorization_resolver=(
                    self._resolve_linear_authorization if self._credentials is None else None
                ),
            )
            if not credentials.github_token:
                raise RuntimeError("bench GitHub connection is missing; reconnect and retry")

            await asyncio.to_thread(trial_root.mkdir, parents=True, exist_ok=False)
            await asyncio.to_thread(shutil.copytree, frozen.root / "support_queue", app_root)
            github = self._github or GitHubSandbox(
                owner=self._config.github_owner,
                token=credentials.github_token,
                commands=self._commands,
            )
            repository = await self._retry_provision(
                lambda: github.create_repository(name=trial_name, source=app_root)
            )

            campaign = await self._retry_provision(
                lambda: linear.create_campaign(
                    team_id=self._config.linear_team_id,
                    label=trial_name,
                    repo_url=repository.url,
                    campaign=frozen.campaign,
                    project_description=project_description(trial),
                    project_id=trial.linear_project_id,
                )
            )
            connections_db = trial_root / "connections.sqlite"
            await snapshot_connections(self._config.control_db, connections_db)
            await self._prepare_candidate(
                trial=trial,
                issue_label=trial_name,
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
                    backend_hidden_test=frozen.backend_hidden_test,
                    frontend_hidden_test=frozen.frontend_hidden_test,
                    manifest=frozen.hidden_manifest,
                    checks=frozen.regression_commands,
                )
            )
            metrics.update(
                await self._reviewer.review(
                    checkout=trial_root / "final",
                    spec_prompt=frozen.spec_prompt,
                    standards_prompt=frozen.standards_prompt,
                )
            )
            metrics.update(
                await github.review_metrics(repository_slug=repository.slug, cwd=trial_root)
            )
            metrics["wall_seconds"] = asyncio.get_running_loop().time() - started
            await self._publish_chronicle(
                linear=linear,
                project_id=campaign.project_id,
                event_key=f"{trial.experiment_id}:{suffix}:completed",
                health="onTrack",
                body=successful_run_update(
                    trial,
                    TrialOutcome(
                        repository_url=repository.url,
                        issue_urls=list(campaign.issue_urls),
                        metrics=metrics,
                    ),
                ),
                metrics=metrics,
            )
        except asyncio.CancelledError:
            outcome = await self._bounded_partial_outcome(
                repository=repository,
                github=github,
                campaign=campaign,
                linear=linear,
                candidate_root=candidate_root,
                candidate_db=trial_root / "candidate.sqlite",
                metrics=metrics,
                started=started,
            )
            await self._publish_failure_update(
                linear=linear,
                campaign=campaign,
                trial=trial,
                error="bench worker stopped during this run",
                outcome=outcome,
                event="interrupted",
            )
            if not cancelled_outcome.done():
                cancelled_outcome.set_result(outcome)
            raise
        except GraderInfrastructureError as exc:
            outcome = await self._bounded_partial_outcome(
                repository=repository,
                github=github,
                campaign=campaign,
                linear=linear,
                candidate_root=candidate_root,
                candidate_db=trial_root / "candidate.sqlite",
                metrics=metrics,
                started=started,
            )
            await self._publish_failure_update(
                linear=linear,
                campaign=campaign,
                trial=trial,
                error=f"infrastructure_failed: {exc}",
                outcome=outcome,
                event="failed",
            )
            raise TrialExecutionError(
                f"infrastructure_failed: {exc}",
                outcome=outcome,
            ) from exc
        except Exception as exc:
            outcome = await self._bounded_partial_outcome(
                repository=repository,
                github=github,
                campaign=campaign,
                linear=linear,
                candidate_root=candidate_root,
                candidate_db=trial_root / "candidate.sqlite",
                metrics=metrics,
                started=started,
            )
            await self._publish_failure_update(
                linear=linear,
                campaign=campaign,
                trial=trial,
                error=str(exc),
                outcome=outcome,
                event="failed",
            )
            raise TrialExecutionError(
                str(exc),
                outcome=outcome,
            ) from exc
        finally:
            try:
                await asyncio.to_thread(
                    _archive_trial_receipts,
                    trial_root,
                    self._config.private_root / trial.experiment_id / suffix,
                )
            except Exception:  # noqa: BLE001 - next trial retries cleanup before dispatch
                log.exception("could not archive bench receipts from %s", trial_root)
            if repository is not None and github is not None:
                try:
                    await self._retry_provision(
                        lambda: github.archive_repository(
                            repository_slug=repository.slug,
                            cwd=self._config.root,
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - reported after a successful trial
                    archive_error = exc
                    log.exception("could not archive benchmark repository %s", repository.slug)
            if archive_error is not None:
                metrics["wall_seconds"] = asyncio.get_running_loop().time() - started
                await self._publish_failure_update(
                    linear=linear,
                    campaign=campaign,
                    trial=trial,
                    error=f"could not archive benchmark repository: {archive_error}",
                    outcome=TrialOutcome(
                        repository_url=repository.url if repository is not None else None,
                        issue_urls=list(campaign.issue_urls) if campaign is not None else [],
                        metrics=metrics,
                    ),
                    event="archive-failed",
                )
            if owns_linear:
                assert isinstance(linear, LinearSandbox)
                await linear.aclose()
        metrics["wall_seconds"] = asyncio.get_running_loop().time() - started
        outcome = TrialOutcome(
            repository_url=repository.url,
            issue_urls=list(campaign.issue_urls),
            metrics=metrics,
        )
        if archive_error is not None:
            raise TrialExecutionError(
                f"could not archive benchmark repository: {archive_error}",
                outcome=outcome,
            )
        return outcome

    async def start_experiment(self, experiment: Experiment) -> str:
        frozen = load_harness(self._config.private_root / experiment.id / "_harness")
        linear, owns_linear = await self._chronicle_linear()
        try:
            project_id = await linear.ensure_project(
                team_id=self._config.linear_team_id,
                experiment_id=experiment.id,
                campaign=frozen.campaign,
                project_description=project_description(experiment),
            )
            launch_is_durable = await self._publish_chronicle(
                linear=linear,
                project_id=project_id,
                event_key=f"{experiment.id}:started",
                health="onTrack",
                body=launch_update(experiment),
            )
            if not launch_is_durable:
                raise RuntimeError("experiment launch update is neither delivered nor durable")
            return project_id
        finally:
            if owns_linear:
                assert isinstance(linear, LinearSandbox)
                await linear.aclose()

    async def publish_failed_experiment(self, experiment: Experiment) -> None:
        if not experiment.linear_project_id:
            return
        event_key = f"{experiment.id}:failed"
        body = failed_experiment_update(experiment)
        pending = await self._queue_chronicle_event(
            project_id=experiment.linear_project_id,
            event_key=event_key,
            health="offTrack",
            body=body,
        )
        try:
            linear, owns_linear = await self._chronicle_linear()
        except Exception:  # noqa: BLE001 - the durable event remains for restart recovery
            log.exception("could not connect to Linear for terminal experiment update")
            return
        try:
            await self._deliver_chronicle_event(
                linear=linear,
                pending=pending,
                project_id=experiment.linear_project_id,
                event_key=event_key,
                health="offTrack",
                body=body,
            )
        finally:
            if owns_linear:
                assert isinstance(linear, LinearSandbox)
                await linear.aclose()

    async def recover_chronicle(self) -> None:
        pending = sorted(
            (self._config.private_root / "_chronicle").glob("*.json"),
            key=_chronicle_recovery_order,
        )
        if not pending:
            return
        try:
            linear, owns_linear = await self._chronicle_linear()
        except Exception:  # noqa: BLE001 - leave all durable events for the next restart
            log.exception("could not connect to Linear to recover benchmark chronicle")
            return
        blocked_experiments: set[str] = set()
        try:
            for path in pending:
                experiment_id = path.stem.partition(":")[0]
                if experiment_id in blocked_experiments:
                    continue
                try:
                    payload = json.loads(await asyncio.to_thread(path.read_text, encoding="utf-8"))
                    await linear.publish_project_update(
                        project_id=str(payload["project_id"]),
                        event_key=str(payload["event_key"]),
                        health=str(payload["health"]),
                        body=str(payload["body"]),
                    )
                    await asyncio.to_thread(path.unlink, missing_ok=True)
                except Exception:  # noqa: BLE001 - leave the durable event for the next restart
                    blocked_experiments.add(experiment_id)
                    log.exception("could not recover benchmark chronicle event from %s", path)
        finally:
            if owns_linear:
                assert isinstance(linear, LinearSandbox)
                await linear.aclose()

    async def _chronicle_linear(self) -> tuple[LinearProvisioner, bool]:
        if self._linear is not None:
            return self._linear, False
        credentials = self._credentials or await self._resolve_credentials()
        if not credentials.linear_token:
            raise RuntimeError("bench Linear connection is missing; reconnect and retry")
        return (
            LinearSandbox(
                credentials.linear_token,
                routing_label_id=self._config.linear_label_id,
                authorization_resolver=(
                    self._resolve_linear_authorization if self._credentials is None else None
                ),
            ),
            True,
        )

    async def _publish_chronicle(
        self,
        *,
        linear: LinearProvisioner,
        project_id: str,
        event_key: str,
        health: str,
        body: str,
        metrics: dict[str, object] | None = None,
    ) -> bool:
        try:
            pending = await self._queue_chronicle_event(
                project_id=project_id,
                event_key=event_key,
                health=health,
                body=body,
            )
        except OSError as exc:
            pending = None
            if metrics is not None:
                metrics["linear_chronicle_error"] = str(exc)
            log.exception("could not persist benchmark chronicle event %s", event_key)
        delivered = await self._deliver_chronicle_event(
            linear=linear,
            pending=pending,
            project_id=project_id,
            event_key=event_key,
            health=health,
            body=body,
            metrics=metrics,
        )
        return pending is not None or delivered

    async def _queue_chronicle_event(
        self, *, project_id: str, event_key: str, health: str, body: str
    ) -> Path:
        pending = self._config.private_root / "_chronicle" / f"{event_key}.json"
        await asyncio.to_thread(
            _write_chronicle_event,
            pending,
            {"project_id": project_id, "event_key": event_key, "health": health, "body": body},
        )
        return pending

    async def _deliver_chronicle_event(
        self,
        *,
        linear: LinearProvisioner,
        pending: Path | None,
        project_id: str,
        event_key: str,
        health: str,
        body: str,
        metrics: dict[str, object] | None = None,
    ) -> bool:
        if pending is not None:
            experiment_id = pending.stem.partition(":")[0]
            lock = _CHRONICLE_LOCKS.setdefault(experiment_id, asyncio.Lock())
            async with lock:
                if not await asyncio.to_thread(pending.exists):
                    return True
                paths = sorted(
                    (
                        path
                        for path in pending.parent.glob("*.json")
                        if path.stem.partition(":")[0] == experiment_id
                    ),
                    key=_chronicle_recovery_order,
                )
                for path in paths:
                    try:
                        payload = json.loads(
                            await asyncio.to_thread(path.read_text, encoding="utf-8")
                        )
                        await linear.publish_project_update(
                            project_id=str(payload["project_id"]),
                            event_key=str(payload["event_key"]),
                            health=str(payload["health"]),
                            body=str(payload["body"]),
                        )
                    except Exception as exc:  # noqa: BLE001 - preserve the ordered outbox
                        if path == pending and metrics is not None:
                            metrics["linear_chronicle_error"] = str(exc)
                        log.exception("could not publish benchmark chronicle event from %s", path)
                        return False
                    try:
                        await asyncio.to_thread(path.unlink, missing_ok=True)
                    except OSError:
                        log.exception("could not remove delivered chronicle event %s", path)
                        return path == pending
                return not await asyncio.to_thread(pending.exists)
        try:
            await linear.publish_project_update(
                project_id=project_id,
                event_key=event_key,
                health=health,
                body=body,
            )
        except Exception as exc:  # noqa: BLE001 - chronicle delivery must not change trial result
            if metrics is not None:
                metrics["linear_chronicle_error"] = str(exc)
            log.exception("could not publish benchmark chronicle event %s", event_key)
            return False
        if pending is not None:
            try:
                await asyncio.to_thread(pending.unlink, missing_ok=True)
            except OSError:
                log.exception("could not remove delivered chronicle event %s", pending)
        return True

    async def publish_interrupted(self, trial: Trial) -> None:
        if not trial.linear_project_id:
            log.warning("no Linear project for interrupted trial %s", trial.experiment_id)
            return
        event_key = f"{trial.experiment_id}:{trial.candidate}{trial.repetition}:interrupted"
        body = failed_run_update(
            trial,
            "bench worker restarted during this run",
            TrialOutcome(metrics={"token_metrics_unavailable": True}),
        )
        pending = await self._queue_chronicle_event(
            project_id=trial.linear_project_id,
            event_key=event_key,
            health="offTrack",
            body=body,
        )
        try:
            linear, owns_linear = await self._chronicle_linear()
        except Exception:  # noqa: BLE001 - the durable event remains for restart recovery
            log.exception("could not connect to Linear for interrupted trial update")
            return
        try:
            await self._deliver_chronicle_event(
                linear=linear,
                pending=pending,
                project_id=trial.linear_project_id,
                event_key=event_key,
                health="offTrack",
                body=body,
            )
        finally:
            if owns_linear:
                assert isinstance(linear, LinearSandbox)
                await linear.aclose()

    async def _publish_failure_update(
        self,
        *,
        linear: LinearProvisioner | None,
        campaign: LinearCampaign | None,
        trial: Trial,
        error: str,
        outcome: TrialOutcome,
        event: str,
    ) -> None:
        project_id = campaign.project_id if campaign is not None else trial.linear_project_id
        if linear is None or not project_id:
            return
        await self._publish_chronicle(
            linear=linear,
            project_id=project_id,
            event_key=(
                f"{trial.experiment_id}:{trial.candidate}{trial.repetition}:{event}"
            ),
            health="offTrack",
            body=failed_run_update(trial, error, outcome),
            metrics=outcome.metrics,
        )

    def _load_trial_harness(self, trial: Trial) -> FrozenHarness:
        snapshot = self._config.private_root / trial.experiment_id / "_harness"
        if not snapshot.exists():
            raise RuntimeError(f"experiment harness snapshot is missing: {snapshot}")
        return load_harness(snapshot)

    async def _partial_outcome(
        self,
        *,
        repository: GitHubRepository | None,
        github: GitHubProvisioner | None,
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
        metrics.setdefault("raw_tokens", None)
        metrics.setdefault("token_metrics_unavailable", True)
        if campaign is not None and linear is not None:
            try:
                states = await linear.issue_states(campaign.issue_ids)
                metrics["completed_tickets"] = sum(state.type == "completed" for state in states)
            except Exception:  # noqa: BLE001 - best-effort failure receipt
                pass
        metrics.pop("remote_review_rounds", None)
        if repository is not None and github is not None:
            try:
                metrics.update(
                    await github.review_metrics(
                        repository_slug=repository.slug, cwd=candidate_root.parent
                    )
                )
            except Exception:  # noqa: BLE001 - best-effort failure receipt
                metrics["remote_review_metrics_unavailable"] = True
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
        github: GitHubProvisioner | None,
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
                    github=github,
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
            metrics.pop("remote_review_rounds", None)
            return TrialOutcome(
                repository_url=repository.url if repository is not None else None,
                issue_urls=list(campaign.issue_urls) if campaign is not None else [],
                metrics={
                    **metrics,
                    "raw_tokens": metrics.get("raw_tokens"),
                    "token_metrics_unavailable": metrics.get("token_metrics_unavailable", True),
                    "wall_seconds": asyncio.get_running_loop().time() - started,
                    "partial_receipt_timed_out": True,
                    "remote_review_metrics_unavailable": True,
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

    async def _resolve_linear_authorization(self) -> str:
        authorization = (await self._resolve_credentials()).linear_token
        if not authorization:
            raise RuntimeError("bench Linear connection is missing; reconnect and retry")
        return authorization

    async def _prepare_candidate(
        self,
        *,
        trial: Trial,
        issue_label: str,
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
            ["git", "checkout", "--detach", trial.revision],
            cwd=candidate_root,
            env={"GH_TOKEN": github_token},
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
                "--issue-title-prefix",
                f"[{issue_label}]",
                "--connections-db",
                str(connections_db),
            ],
            cwd=candidate_root,
            env={"SYMPHONY_ENCRYPTION_KEY": self._config.encryption_key},
        )
        # The candidate needs runtime code, not grader controls or its own Git
        # object database. Remove all three before any agent is dispatched.
        for private_asset in (
            "hidden",
            "feedback_inbox_reference",
            "support_queue_reference",
            "support_queue_mutations",
        ):
            await asyncio.to_thread(
                shutil.rmtree,
                candidate_root / "src/symphony/bench/assets" / private_asset,
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
                await self._sync_trial_connections(candidate_db)
                metrics = await self._checked_candidate_snapshot(candidate_root, candidate_db)

                states = await linear.issue_states(campaign.issue_ids)
                if all(state.type == "completed" for state in states):
                    while True:
                        await self._sync_trial_connections(candidate_db)
                        metrics = await self._checked_candidate_snapshot(
                            candidate_root, candidate_db
                        )
                        statuses = metrics.get("runs_by_status")
                        if not isinstance(statuses, dict):
                            raise RuntimeError("candidate bench snapshot is missing run statuses")
                        if int(statuses.get("running", 0)) == 0:
                            metrics["completed_tickets"] = len(states)
                            return metrics
                        if (
                            asyncio.get_running_loop().time() - started
                            >= self._config.wall_time_cap_seconds
                        ):
                            raise RuntimeError("bench wall-time safety cap exceeded")
                        await asyncio.sleep(self._config.poll_seconds)
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

    async def _checked_candidate_snapshot(
        self, candidate_root: Path, candidate_db: Path
    ) -> dict[str, object]:
        metrics = await self._candidate_snapshot(candidate_root, candidate_db)
        token_total = metrics.get("effective_tokens")
        launch_total = metrics.get("agent_launches")
        if not isinstance(token_total, (int, float)) or not isinstance(launch_total, int):
            raise RuntimeError("candidate bench snapshot is missing safety metrics")
        statuses = metrics.get("runs_by_status")
        running = int(statuses.get("running", 0)) if isinstance(statuses, dict) else 0
        if running == 0 and metrics.get("token_metrics_unavailable") is True:
            raise RuntimeError("candidate token metrics did not reconcile")
        if float(token_total) > self._config.observed_token_cap:
            raise RuntimeError("bench observed-token safety cap exceeded")
        if launch_total > self._config.agent_launch_cap:
            raise RuntimeError("bench agent-launch safety cap exceeded")
        return metrics

    async def _sync_trial_connections(self, candidate_db: Path) -> None:
        """Reconcile runtime OAuth refreshes in both directions by generation."""
        await reconcile_connections(candidate_db, self._config.control_db)

    async def _candidate_snapshot(
        self, candidate_root: Path, candidate_db: Path
    ) -> dict[str, object]:
        return await self._candidate_snapshotter(candidate_db, candidate_root.parent / "logs")


def _archive_shared_trials(root: Path, private_root: Path) -> None:
    """Remove every prior solution from the executor-visible volume before dispatch."""
    if not root.exists():
        return
    for experiment_root in root.glob("EXP-*"):
        if not experiment_root.is_dir():
            continue
        legacy_harness = experiment_root / "_harness"
        if legacy_harness.is_dir():
            private_harness = private_root / experiment_root.name / "_harness"
            if not private_harness.exists():
                shutil.copytree(legacy_harness, private_harness)
            shutil.rmtree(legacy_harness)
        for trial_root in experiment_root.iterdir():
            if trial_root.is_dir() and _TRIAL_DIRECTORY_RE.fullmatch(trial_root.name):
                _archive_trial_receipts(
                    trial_root, private_root / experiment_root.name / trial_root.name
                )
        with contextlib.suppress(OSError):
            experiment_root.rmdir()


def _archive_trial_receipts(trial_root: Path, destination: Path) -> None:
    """Keep audit receipts privately; discard candidate-readable source and hidden artifacts."""
    if not trial_root.exists():
        return
    destination.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    for pattern in (
        "candidate-profile.json",
        "candidate.sqlite*",
        "connections.sqlite*",
        "backend-hidden-junit.xml",
        "frontend-hidden.json",
        "hidden-feedback.sqlite*",
        "hidden-support.sqlite*",
        "final-*-review.json",
    ):
        for source in trial_root.glob(pattern):
            if not source.is_file():
                continue
            shutil.copy2(source, destination / source.name)
            copied.append(source.name)
    logs = trial_root / "logs"
    if logs.is_dir():
        shutil.copytree(logs, destination / "logs", dirs_exist_ok=True)
        copied.append("logs/")
    (destination / "receipt-manifest.json").write_text(
        json.dumps({"copied": sorted(copied)}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    shutil.rmtree(trial_root)


def _write_chronicle_event(path: Path, payload: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _chronicle_recovery_order(path: Path) -> tuple[str, int, str]:
    event_key = path.stem
    experiment_id, _, event = event_key.partition(":")
    if event == "started":
        stage = 0
    elif event == "failed":
        stage = 2
    else:
        stage = 1
    return experiment_id, stage, event_key
