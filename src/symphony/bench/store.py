from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .models import (
    Experiment,
    ExperimentCreate,
    ExperimentReport,
    ExperimentStatus,
    Trial,
    TrialOutcome,
    TrialRecord,
    TrialStatus,
)


class ExperimentStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bench_experiments (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    candidate_a TEXT NOT NULL,
                    candidate_b TEXT NOT NULL,
                    candidate_a_profile TEXT NOT NULL DEFAULT '{}',
                    candidate_b_profile TEXT NOT NULL DEFAULT '{}',
                    system_version_a TEXT NOT NULL DEFAULT '',
                    system_version_b TEXT NOT NULL DEFAULT '',
                    harness_version TEXT NOT NULL DEFAULT '',
                    repetitions INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bench_trials (
                    experiment_id TEXT NOT NULL,
                    candidate TEXT NOT NULL,
                    repetition INTEGER NOT NULL,
                    revision TEXT NOT NULL,
                    profile TEXT NOT NULL DEFAULT '{}',
                    system_version TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    error TEXT,
                    repository_url TEXT,
                    issue_urls TEXT NOT NULL DEFAULT '[]',
                    metrics TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (experiment_id, candidate, repetition),
                    FOREIGN KEY (experiment_id) REFERENCES bench_experiments(id)
                )
                """
            )
            self._ensure_column(
                conn, "bench_experiments", "candidate_a_profile", "TEXT NOT NULL DEFAULT '{}'"
            )
            self._ensure_column(
                conn, "bench_experiments", "candidate_b_profile", "TEXT NOT NULL DEFAULT '{}'"
            )
            self._ensure_column(
                conn, "bench_experiments", "system_version_a", "TEXT NOT NULL DEFAULT ''"
            )
            self._ensure_column(
                conn, "bench_experiments", "system_version_b", "TEXT NOT NULL DEFAULT ''"
            )
            self._ensure_column(
                conn, "bench_experiments", "harness_version", "TEXT NOT NULL DEFAULT ''"
            )
            self._ensure_column(conn, "bench_trials", "profile", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column(conn, "bench_trials", "system_version", "TEXT NOT NULL DEFAULT ''")

    def create(self, request: ExperimentCreate, *, harness_version: str = "") -> Experiment:
        experiment = Experiment.queued(
            experiment_id=f"EXP-{uuid4().hex[:12].upper()}",
            request=request,
            harness_version=harness_version,
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO bench_experiments
                    (id, status, candidate_a, candidate_b, candidate_a_profile,
                     candidate_b_profile, system_version_a, system_version_b,
                     harness_version, repetitions, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    experiment.id,
                    experiment.status,
                    experiment.candidate_a,
                    experiment.candidate_b,
                    json.dumps(experiment.candidate_a_profile, sort_keys=True),
                    json.dumps(experiment.candidate_b_profile, sort_keys=True),
                    experiment.system_version_a,
                    experiment.system_version_b,
                    experiment.harness_version,
                    experiment.repetitions,
                    experiment.created_at.isoformat(),
                ),
            )
        return experiment

    def get(self, experiment_id: str) -> Experiment | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, status, candidate_a, candidate_b, candidate_a_profile,
                       candidate_b_profile, system_version_a, system_version_b,
                       harness_version, repetitions, created_at
                FROM bench_experiments WHERE id = ?
                """,
                (experiment_id,),
            ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["candidate_a_profile"] = json.loads(payload["candidate_a_profile"])
        payload["candidate_b_profile"] = json.loads(payload["candidate_b_profile"])
        return Experiment.model_validate(payload)

    def claim_next(self) -> Experiment | None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT id, status, candidate_a, candidate_b, candidate_a_profile,
                       candidate_b_profile, system_version_a, system_version_b,
                       harness_version, repetitions, created_at
                FROM bench_experiments
                WHERE status = 'queued'
                ORDER BY created_at, id
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE bench_experiments SET status = 'running' WHERE id = ?",
                (row["id"],),
            )
        claimed = dict(row)
        claimed["candidate_a_profile"] = json.loads(claimed["candidate_a_profile"])
        claimed["candidate_b_profile"] = json.loads(claimed["candidate_b_profile"])
        claimed["status"] = "running"
        return Experiment.model_validate(claimed)

    def set_status(self, experiment_id: str, status: ExperimentStatus) -> None:
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE bench_experiments SET status = ? WHERE id = ?",
                (status, experiment_id),
            )
        if cursor.rowcount != 1:
            raise KeyError(experiment_id)

    def fail_interrupted(self) -> int:
        """Close orphaned running work before this process claims a new experiment."""
        ended_at = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            trials = conn.execute(
                """
                UPDATE bench_trials
                SET status = 'failed', ended_at = ?,
                    error = 'bench worker restarted during this trial'
                WHERE status = 'running'
                """,
                (ended_at,),
            ).rowcount
            experiments = conn.execute(
                "UPDATE bench_experiments SET status = 'failed' WHERE status = 'running'"
            ).rowcount
        return max(trials, experiments)

    def start_trial(self, trial: Trial) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO bench_trials (
                    experiment_id, candidate, repetition, revision, profile,
                    system_version, status, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'running', ?)
                """,
                (
                    trial.experiment_id,
                    trial.candidate,
                    trial.repetition,
                    trial.revision,
                    json.dumps(trial.profile, sort_keys=True),
                    trial.system_version,
                    datetime.now(UTC).isoformat(),
                ),
            )

    def finish_trial(
        self,
        trial: Trial,
        status: TrialStatus,
        *,
        error: str | None = None,
        outcome: TrialOutcome | None = None,
    ) -> None:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE bench_trials
                SET status = ?, ended_at = ?, error = ?, repository_url = ?,
                    issue_urls = ?, metrics = ?
                WHERE experiment_id = ? AND candidate = ? AND repetition = ?
                """,
                (
                    status,
                    datetime.now(UTC).isoformat(),
                    error,
                    outcome.repository_url if outcome is not None else None,
                    json.dumps(outcome.issue_urls if outcome is not None else []),
                    json.dumps(outcome.metrics if outcome is not None else {}),
                    trial.experiment_id,
                    trial.candidate,
                    trial.repetition,
                ),
            )
        if cursor.rowcount != 1:
            raise KeyError((trial.experiment_id, trial.candidate, trial.repetition))

    def report(self, experiment_id: str) -> ExperimentReport | None:
        experiment = self.get(experiment_id)
        if experiment is None:
            return None
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT experiment_id, candidate, repetition, revision, profile,
                       system_version, status,
                       started_at, ended_at, error, repository_url, issue_urls, metrics
                FROM bench_trials
                WHERE experiment_id = ?
                ORDER BY repetition, CASE candidate WHEN 'A' THEN 0 ELSE 1 END
                """,
                (experiment_id,),
            ).fetchall()
        return ExperimentReport(
            experiment=experiment,
            trials=[
                TrialRecord.model_validate(
                    {
                        **dict(row),
                        "profile": json.loads(row["profile"]),
                        "issue_urls": json.loads(row["issue_urls"]),
                        "metrics": json.loads(row["metrics"]),
                    }
                )
                for row in rows
            ],
        )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
