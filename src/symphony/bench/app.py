from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, status

from .models import Experiment, ExperimentCreate, ExperimentReport
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
    prepare_harness: Callable[[str], Awaitable[str]] | None = None,
    recover_execution: Callable[[], Awaitable[None]] | None = None,
) -> FastAPI:
    if not api_token:
        raise ValueError("bench API token must not be empty")
    store = ExperimentStore(db_path)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if recover_execution is not None:
            await recover_execution()
        interrupted = store.fail_interrupted()
        if interrupted:
            log.error("marked %d interrupted bench run(s) failed on startup", interrupted)
        if execute is None:
            yield
            return
        runner = ExperimentRunner(store=store, execute=execute)

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

        task = asyncio.create_task(drain())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
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

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/experiments",
        response_model=Experiment,
        status_code=status.HTTP_201_CREATED,
        dependencies=auth,
    )
    async def submit(request: ExperimentCreate) -> Experiment:
        defaults = default_profile or {}
        updates: dict[str, object] = {}
        if "candidate_a_profile" not in request.model_fields_set:
            updates["candidate_a_profile"] = defaults
        if "candidate_b_profile" not in request.model_fields_set:
            updates["candidate_b_profile"] = defaults
        if resolve_revision is not None:
            try:
                if request.candidate_a == request.candidate_b:
                    revision = await resolve_revision(request.candidate_a)
                    updates.update(candidate_a=revision, candidate_b=revision)
                else:
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
        if prepare_harness is not None:
            try:
                pinned_harness = await prepare_harness(experiment_id)
            except Exception as exc:  # noqa: BLE001 - provider error becomes API failure
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"harness snapshot failed: {exc}",
                ) from exc
        return store.create(
            request.model_copy(update=updates),
            harness_version=pinned_harness,
            experiment_id=experiment_id,
        )

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
