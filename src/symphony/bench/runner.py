from __future__ import annotations

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
        active_trial: Trial | None = None
        try:
            for trial in _trials(experiment):
                active_trial = trial
                self._store.start_trial(trial)
                outcome = await self._execute(trial)
                self._store.finish_trial(trial, "completed", outcome=outcome)
                active_trial = None
        except TrialExecutionCancelled as exc:
            if active_trial is not None:
                self._store.finish_trial(
                    active_trial,
                    "failed",
                    error="bench worker stopped during this trial",
                    outcome=exc.outcome,
                )
            self._store.set_status(experiment.id, "failed")
            raise
        except TrialExecutionError as exc:
            if active_trial is not None:
                self._store.finish_trial(
                    active_trial, "failed", error=str(exc), outcome=exc.outcome
                )
            self._store.set_status(experiment.id, "failed")
            raise
        except Exception as exc:
            if active_trial is not None:
                self._store.finish_trial(active_trial, "failed", error=str(exc))
            self._store.set_status(experiment.id, "failed")
            raise
        self._store.set_status(experiment.id, "completed")
        return experiment.id


def _trials(experiment: Experiment) -> list[Trial]:
    trials: list[Trial] = [
        Trial(
            experiment_id=experiment.id,
            candidate="S",
            repetition=0,
            revision=experiment.candidate_a,
            profile=experiment.candidate_a_profile,
            system_version=experiment.system_version_a,
        )
    ]
    for repetition in range(1, experiment.repetitions + 1):
        trials.extend(
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
    return trials
