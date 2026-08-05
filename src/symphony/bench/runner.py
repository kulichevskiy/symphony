from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from .models import (
    Experiment,
    Trial,
    TrialExecutionCancelled,
    TrialExecutionError,
    TrialOutcome,
    TrialStatus,
)
from .store import ExperimentStore

TrialExecutor = Callable[[Trial], Awaitable[TrialOutcome | None]]
TrialPublisher = Callable[[Trial], Awaitable[None]]
ExperimentPublisher = Callable[[str], Awaitable[None]]


class ExperimentRunner:
    def __init__(
        self,
        *,
        store: ExperimentStore,
        execute: TrialExecutor,
        publish: TrialPublisher | None = None,
        publish_experiment: ExperimentPublisher | None = None,
    ) -> None:
        self._store = store
        self._execute = execute
        self._publish = publish
        self._publish_experiment = publish_experiment

    async def run_next(self) -> str | None:
        experiment = self._store.claim_next()
        if experiment is None:
            return None
        try:
            first_failure: BaseException | None = None
            for pair in _trial_plan(experiment):
                tasks = [asyncio.create_task(self._run_trial(trial)) for trial in pair]
                try:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                except BaseException:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise
                if first_failure is None:
                    first_failure = next(
                        (result for result in results if isinstance(result, BaseException)),
                        None,
                    )
            if first_failure is not None:
                raise first_failure
        except BaseException:
            self._store.set_status(experiment.id, "failed")
            if self._publish_experiment is not None:
                await self._publish_experiment(experiment.id)
            raise
        self._store.set_status(experiment.id, "completed")
        if self._publish_experiment is not None:
            await self._publish_experiment(experiment.id)
        return experiment.id

    async def _run_trial(self, trial: Trial) -> None:
        self._store.start_trial(trial)
        try:
            outcome = await self._execute(trial)
        except TrialExecutionCancelled as exc:
            await self._finish_trial(
                trial,
                "failed",
                error="bench worker stopped during this trial",
                outcome=exc.outcome,
            )
            raise
        except TrialExecutionError as exc:
            await self._finish_trial(trial, "failed", error=str(exc), outcome=exc.outcome)
            raise
        except asyncio.CancelledError:
            await self._finish_trial(
                trial,
                "failed",
                error="bench worker stopped during this trial",
            )
            raise
        except Exception as exc:
            await self._finish_trial(trial, "failed", error=str(exc))
            raise
        await self._finish_trial(trial, "completed", outcome=outcome)

    async def _finish_trial(
        self,
        trial: Trial,
        status: TrialStatus,
        *,
        error: str | None = None,
        outcome: TrialOutcome | None = None,
    ) -> None:
        self._store.finish_trial(trial, status, error=error, outcome=outcome)
        if self._publish is not None:
            await self._publish(trial)


def _trial_plan(experiment: Experiment) -> list[tuple[Trial, ...]]:
    batches: list[tuple[Trial, ...]] = []
    for repetition in range(1, experiment.repetitions + 1):
        candidate_a = Trial(
            experiment_id=experiment.id,
            candidate="A",
            repetition=repetition,
            revision=experiment.candidate_a,
            profile=experiment.candidate_a_profile,
            system_version=experiment.system_version_a,
        )
        if experiment.mode == "single":
            batches.append((candidate_a,))
        else:
            if experiment.candidate_b is None:
                raise ValueError("paired experiment is missing candidate B")
            batches.append(
                (
                    candidate_a,
                    Trial(
                        experiment_id=experiment.id,
                        candidate="B",
                        repetition=repetition,
                        revision=experiment.candidate_b,
                        profile=experiment.candidate_b_profile,
                        system_version=experiment.system_version_b,
                    ),
                )
            )
    return batches
