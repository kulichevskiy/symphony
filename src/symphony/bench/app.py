from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Response, status

from .models import (
    BenchNotification,
    Experiment,
    ExperimentCreate,
    ExperimentReport,
    Trial,
)
from .report import persist_experiment_markdown, persist_trial_markdown
from .runner import ExperimentRunner, TrialExecutor
from .store import ExperimentStore

log = logging.getLogger(__name__)


def create_bench_app(
    *,
    db_path: Path,
    api_token: str,
    execute: TrialExecutor | None = None,
    idle_poll_seconds: float = 2,
    default_profile: dict[str, object] | None = None,
    resolve_revision: Callable[[str], Awaitable[str]] | None = None,
    harness_version: str = "",
    snapshot_harness: Callable[[str], Awaitable[str]] | None = None,
    prepare_harness: Callable[[Experiment], Awaitable[str]] | None = None,
    recover_execution: Callable[[], Awaitable[None]] | None = None,
    recover_chronicle: Callable[[], Awaitable[None]] | None = None,
    start_experiment: Callable[[Experiment], Awaitable[str]] | None = None,
    publish_interrupted: Callable[[Trial], Awaitable[None]] | None = None,
    publish_failed_experiment: Callable[[Experiment], Awaitable[None]] | None = None,
    reports_root: Path | None = None,
) -> FastAPI:
    if not api_token:
        raise ValueError("bench API token must not be empty")
    store = ExperimentStore(db_path)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if recover_execution is not None:
            await recover_execution()
        if recover_chronicle is not None:
            await recover_chronicle()
        if start_experiment is not None:
            for prepared_experiment in store.preparing():
                try:
                    project_id = await start_experiment(prepared_experiment)
                    store.activate(prepared_experiment.id, project_id)
                except Exception:  # noqa: BLE001 - retry preparation on the next restart
                    log.exception(
                        "could not resume experiment preparation %s", prepared_experiment.id
                    )
        interrupted_trials, interrupted_experiments = store.fail_interrupted()
        if publish_interrupted is not None:
            for trial in interrupted_trials:
                try:
                    await publish_interrupted(trial)
                    store.acknowledge_trial_chronicle_recovery(trial)
                except Exception:  # noqa: BLE001 - recovery must not block the worker restart
                    log.exception(
                        "could not publish interrupted trial %s %s%d",
                        trial.experiment_id,
                        trial.candidate,
                        trial.repetition,
                    )
        if publish_failed_experiment is not None:
            for experiment_id in interrupted_experiments:
                interrupted_experiment = store.get(experiment_id)
                if interrupted_experiment is None:
                    continue
                try:
                    await publish_failed_experiment(interrupted_experiment)
                    store.acknowledge_experiment_chronicle_recovery(experiment_id)
                except Exception:  # noqa: BLE001 - recovery must not block the worker restart
                    log.exception(
                        "could not publish interrupted experiment %s", experiment_id
                    )
        if interrupted_experiments:
            log.error(
                "marked %d interrupted experiment(s) failed on startup",
                len(interrupted_experiments),
            )
        if reports_root is not None:
            for trial in interrupted_trials:
                report = store.report(trial.experiment_id)
                if report is None:
                    raise RuntimeError(f"missing interrupted report for {trial.experiment_id}")
                record = next(
                    item
                    for item in report.trials
                    if item.candidate == trial.candidate and item.repetition == trial.repetition
                )
                markdown = await asyncio.to_thread(
                    persist_trial_markdown, reports_root, report, record
                )
                store.queue_notification(trial, markdown)
            for experiment_id in interrupted_experiments:
                report = store.report(experiment_id)
                if report is None:
                    raise RuntimeError(f"missing interrupted report for {experiment_id}")
                await asyncio.to_thread(persist_experiment_markdown, reports_root, report)
        if execute is None:
            yield
            return

        async def publish(trial: Trial) -> None:
            if reports_root is None:
                return
            report = store.report(trial.experiment_id)
            if report is None:
                raise RuntimeError(f"missing experiment report for {trial.experiment_id}")
            record = next(
                item
                for item in report.trials
                if item.candidate == trial.candidate and item.repetition == trial.repetition
            )
            markdown = await asyncio.to_thread(persist_trial_markdown, reports_root, report, record)
            store.queue_notification(trial, markdown)

        async def publish_experiment(experiment_id: str) -> None:
            report = store.report(experiment_id)
            if report is None:
                raise RuntimeError(f"missing experiment report for {experiment_id}")
            if reports_root is not None:
                await asyncio.to_thread(persist_experiment_markdown, reports_root, report)
            if report.experiment.status == "failed" and publish_failed_experiment is not None:
                await publish_failed_experiment(report.experiment)
                store.acknowledge_experiment_chronicle_recovery(experiment_id)

        runner = ExperimentRunner(
            store=store,
            execute=execute,
            publish=publish,
            publish_experiment=publish_experiment,
            prepare_experiment=prepare_harness,
        )

        async def drain() -> None:
            while True:
                try:
                    experiment_id = await runner.run_next()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("bench experiment failed")
                    experiment_id = None
                if experiment_id is None:
                    await asyncio.sleep(idle_poll_seconds)

        tasks = [asyncio.create_task(drain()) for _ in range(2)]
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if recover_execution is not None:
                await recover_execution()

    app = FastAPI(title="Symphony Bench", lifespan=lifespan)

    def require_token(authorization: Annotated[str | None, Header()] = None) -> None:
        expected = f"Bearer {api_token}"
        if authorization is None or not secrets.compare_digest(authorization, expected):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    auth = [Depends(require_token)]
    submission_lock = asyncio.Lock()

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    async def submit_one(request: ExperimentCreate) -> Experiment:
        defaults = default_profile or {}
        updates: dict[str, object] = {}
        if "candidate_a_profile" not in request.model_fields_set:
            updates["candidate_a_profile"] = defaults
        if request.mode == "paired" and "candidate_b_profile" not in request.model_fields_set:
            updates["candidate_b_profile"] = defaults
        if resolve_revision is not None:
            try:
                if request.mode == "single":
                    updates["candidate_a"] = await resolve_revision(request.candidate_a)
                elif request.candidate_a == request.candidate_b:
                    revision = await resolve_revision(request.candidate_a)
                    updates.update(candidate_a=revision, candidate_b=revision)
                else:
                    assert request.candidate_b is not None
                    candidate_a, candidate_b = await asyncio.gather(
                        resolve_revision(request.candidate_a),
                        resolve_revision(request.candidate_b),
                    )
                    updates.update(candidate_a=candidate_a, candidate_b=candidate_b)
            except Exception as exc:  # noqa: BLE001 - provider error becomes API validation
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"candidate revision could not be pinned: {exc}",
                ) from exc
        experiment_id = f"EXP-{uuid4().hex[:12].upper()}"
        pinned_harness = harness_version
        if snapshot_harness is not None:
            try:
                pinned_harness = await snapshot_harness(experiment_id)
            except Exception as exc:  # noqa: BLE001 - provider error becomes API failure
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"harness snapshot failed: {exc}",
                ) from exc
        experiment = store.create(
            request.model_copy(update=updates),
            harness_version=pinned_harness,
            experiment_id=experiment_id,
            ready=start_experiment is None,
        )
        if start_experiment is not None:
            try:
                project_id = await start_experiment(experiment)
            except Exception as exc:  # noqa: BLE001 - provider error becomes API failure
                store.set_status(experiment.id, "failed")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"experiment chronicle could not start: {exc}",
                ) from exc
            store.activate(experiment.id, project_id)
            experiment = experiment.model_copy(
                update={"status": "queued", "linear_project_id": project_id}
            )
        return experiment

    @app.post(
        "/experiments",
        response_model=Experiment,
        status_code=status.HTTP_201_CREATED,
        dependencies=auth,
    )
    async def submit(request: ExperimentCreate) -> Experiment:
        async with submission_lock:
            return await submit_one(request)

    @app.get(
        "/experiments/_notifications",
        response_model=list[BenchNotification],
        dependencies=auth,
    )
    @app.get("/notifications", response_model=list[BenchNotification], dependencies=auth)
    def get_notifications() -> list[BenchNotification]:
        return store.pending_notifications()

    @app.post(
        "/experiments/_notifications/{event_key}/ack",
        status_code=status.HTTP_204_NO_CONTENT,
        dependencies=auth,
    )
    @app.post(
        "/notifications/{event_key}/ack",
        status_code=status.HTTP_204_NO_CONTENT,
        dependencies=auth,
    )
    def acknowledge_notification(event_key: str) -> Response:
        if not store.acknowledge_notification(event_key):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.get("/experiments/{experiment_id}", response_model=Experiment, dependencies=auth)
    def get_experiment(experiment_id: str) -> Experiment:
        experiment = store.get(experiment_id)
        if experiment is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return experiment

    @app.get(
        "/experiments/{experiment_id}/report",
        response_model=ExperimentReport,
        dependencies=auth,
    )
    def get_report(experiment_id: str) -> ExperimentReport:
        report = store.report(experiment_id)
        if report is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return report

    return app
