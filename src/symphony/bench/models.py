from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field

ExperimentStatus = Literal["queued", "running", "completed", "failed"]
TrialStatus = Literal["running", "completed", "failed"]


class ExperimentCreate(BaseModel):
    candidate_a: str = Field(min_length=1)
    candidate_b: str = Field(min_length=1)
    candidate_a_profile: dict[str, object] = Field(default_factory=dict)
    candidate_b_profile: dict[str, object] = Field(default_factory=dict)
    repetitions: int = Field(default=3, ge=1, le=10)


class Experiment(ExperimentCreate):
    id: str
    status: ExperimentStatus
    created_at: datetime
    system_version_a: str = ""
    system_version_b: str = ""
    harness_version: str = ""

    @classmethod
    def queued(
        cls, *, experiment_id: str, request: ExperimentCreate, harness_version: str = ""
    ) -> Experiment:
        return cls(
            id=experiment_id,
            status="queued",
            created_at=datetime.now(UTC),
            system_version_a=system_version(request.candidate_a, request.candidate_a_profile),
            system_version_b=system_version(request.candidate_b, request.candidate_b_profile),
            harness_version=harness_version,
            **request.model_dump(),
        )


class Trial(BaseModel):
    experiment_id: str
    candidate: Literal["S", "A", "B"]
    repetition: int = Field(ge=0)
    revision: str
    profile: dict[str, object] = Field(default_factory=dict)
    system_version: str = ""


class TrialOutcome(BaseModel):
    repository_url: str | None = None
    issue_urls: list[str] = Field(default_factory=list)
    metrics: dict[str, object] = Field(default_factory=dict)


class TrialExecutionError(RuntimeError):
    def __init__(self, message: str, *, outcome: TrialOutcome) -> None:
        super().__init__(message)
        self.outcome = outcome


class TrialExecutionCancelled(asyncio.CancelledError):
    def __init__(self, *, outcome: TrialOutcome) -> None:
        super().__init__()
        self.outcome = outcome


class TrialRecord(Trial):
    status: TrialStatus
    started_at: datetime
    ended_at: datetime | None = None
    error: str | None = None
    repository_url: str | None = None
    issue_urls: list[str] = Field(default_factory=list)
    metrics: dict[str, object] = Field(default_factory=dict)


class ExperimentReport(BaseModel):
    experiment: Experiment
    trials: list[TrialRecord]


def system_version(revision: str, profile: dict[str, object]) -> str:
    payload = json.dumps(
        {"revision": revision, "profile": profile}, separators=(",", ":"), sort_keys=True
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]
