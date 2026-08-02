from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from .models import (
    Experiment,
    Trial,
    TrialExecutionCancelled,
    TrialExecutionError,
    TrialOutcome,
)
from .store import ExperimentStore

TrialExecutor = Callable[[Trial], Awaitable[TrialOutcome | None]]


class ExperimentRunner:
    def __init__(self, *, store: ExperimentStore, execute: TrialExecutor) -> None:
        self._store = store
        self._execute = execute

    async def run_next(self) -> str | None:
        experiment = self._store.claim_next()
        if experiment is None:
            return None
        try:
            smoke, pairs = _trial_plan(experiment)
            await self._run_trial(smoke)
            for pair in pairs:
                results = await asyncio.gather(
                    *(self._run_trial(trial) for trial in pair),
                    return_exceptions=True,
                )
                failure = next(
                    (result for result in results if isinstance(result, BaseException)), None
                )
                if failure is not None:
                    raise failure
        except BaseException:
            self._store.set_status(experiment.id, "failed")
            raise
        self._store.set_status(experiment.id, "completed")
        return experiment.id

    async def _run_trial(self, trial: Trial) -> None:
        self._store.start_trial(trial)
        try:
            outcome = await self._execute(trial)
        except TrialExecutionCancelled as exc:
            self._store.finish_trial(
                trial,
                "failed",
                error="bench worker stopped during this trial",
                outcome=exc.outcome,
            )
            raise
        except TrialExecutionError as exc:
            self._store.finish_trial(trial, "failed", error=str(exc), outcome=exc.outcome)
            raise
        except asyncio.CancelledError:
            self._store.finish_trial(
                trial,
                "failed",
                error="bench worker stopped during this trial",
            )
            raise
        except Exception as exc:
            self._store.finish_trial(trial, "failed", error=str(exc))
            raise
        self._store.finish_trial(trial, "completed", outcome=outcome)


def _trial_plan(experiment: Experiment) -> tuple[Trial, list[tuple[Trial, Trial]]]:
    smoke = Trial(
        experiment_id=experiment.id,
        candidate="S",
        repetition=0,
        revision=experiment.candidate_a,
        profile=experiment.candidate_a_profile,
        system_version=experiment.system_version_a,
    )
    pairs: list[tuple[Trial, Trial]] = []
    for repetition in range(1, experiment.repetitions + 1):
        pairs.append(
            (
                Trial(
                    experiment_id=experiment.id,
                    candidate="A",
                    repetition=repetition,
                    revision=experiment.candidate_a,
                    profile=experiment.candidate_a_profile,
                    system_version=experiment.system_version_a,
                ),
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
    return smoke, pairs
