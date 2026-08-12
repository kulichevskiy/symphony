from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .models import (
    BenchNotification,
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
                    mode TEXT NOT NULL DEFAULT 'paired',
                    candidate_a TEXT NOT NULL,
                    candidate_b TEXT NOT NULL,
                    hypothesis TEXT NOT NULL DEFAULT '',
                    design TEXT NOT NULL DEFAULT '',
                    candidate_a_profile TEXT NOT NULL DEFAULT '{}',
                    candidate_b_profile TEXT NOT NULL DEFAULT '{}',
                    system_version_a TEXT NOT NULL DEFAULT '',
                    system_version_b TEXT NOT NULL DEFAULT '',
                    executor_toolchain_version TEXT NOT NULL DEFAULT '',
                    harness_version TEXT NOT NULL DEFAULT '',
                    linear_project_id TEXT NOT NULL DEFAULT '',
                    execution_lane TEXT NOT NULL DEFAULT '',
                    chronicle_recovery_pending INTEGER NOT NULL DEFAULT 0,
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
                    execution_lane TEXT NOT NULL DEFAULT 'A',
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    error TEXT,
                    repository_url TEXT,
                    issue_urls TEXT NOT NULL DEFAULT '[]',
                    metrics TEXT NOT NULL DEFAULT '{}',
                    chronicle_recovery_pending INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (experiment_id, candidate, repetition),
                    FOREIGN KEY (experiment_id) REFERENCES bench_experiments(id)
                )
                """
            )
            self._ensure_column(conn, "bench_experiments", "mode", "TEXT NOT NULL DEFAULT 'paired'")
            self._ensure_column(conn, "bench_experiments", "hypothesis", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "bench_experiments", "design", "TEXT NOT NULL DEFAULT ''")
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
                conn,
                "bench_experiments",
                "executor_toolchain_version",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                conn, "bench_experiments", "harness_version", "TEXT NOT NULL DEFAULT ''"
            )
            self._ensure_column(
                conn, "bench_experiments", "linear_project_id", "TEXT NOT NULL DEFAULT ''"
            )
            self._ensure_column(
                conn, "bench_experiments", "execution_lane", "TEXT NOT NULL DEFAULT ''"
            )
            self._ensure_column(
                conn,
                "bench_experiments",
                "chronicle_recovery_pending",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_column(conn, "bench_trials", "profile", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column(conn, "bench_trials", "system_version", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "bench_trials", "execution_lane", "TEXT NOT NULL DEFAULT 'A'")
            conn.execute("UPDATE bench_trials SET execution_lane = 'B' WHERE candidate = 'B'")
            self._ensure_column(
                conn,
                "bench_trials",
                "chronicle_recovery_pending",
                "INTEGER NOT NULL DEFAULT 0",
            )
            conn.execute(
                """
                UPDATE bench_experiments
                SET hypothesis = 'Not recorded for experiments created before chronicle support.'
                WHERE hypothesis = ''
                """
            )
            conn.execute(
                """
                UPDATE bench_experiments
                SET design = 'Not recorded for experiments created before chronicle support.'
                WHERE design = ''
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bench_notifications (
                    event_key TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL,
                    candidate TEXT NOT NULL,
                    repetition INTEGER NOT NULL,
                    markdown TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    acknowledged_at TEXT,
                    FOREIGN KEY (experiment_id) REFERENCES bench_experiments(id)
                )
                """
            )

    def create(
        self,
        request: ExperimentCreate,
        *,
        harness_version: str = "",
        experiment_id: str | None = None,
        ready: bool = True,
    ) -> Experiment:
        experiment = Experiment.queued(
            experiment_id=experiment_id or f"EXP-{uuid4().hex[:12].upper()}",
            request=request,
            harness_version=harness_version,
        )
        if not ready:
            experiment = experiment.model_copy(update={"status": "preparing"})
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO bench_experiments
                    (id, status, mode, candidate_a, candidate_b, hypothesis, design,
                     candidate_a_profile,
                     candidate_b_profile, system_version_a, system_version_b,
                     executor_toolchain_version, harness_version, linear_project_id,
                     execution_lane, repetitions, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    experiment.id,
                    experiment.status,
                    experiment.mode,
                    experiment.candidate_a,
                    experiment.candidate_b or "",
                    experiment.hypothesis,
                    experiment.design,
                    json.dumps(experiment.candidate_a_profile, sort_keys=True),
                    json.dumps(experiment.candidate_b_profile, sort_keys=True),
                    experiment.system_version_a,
                    experiment.system_version_b,
                    experiment.executor_toolchain_version,
                    experiment.harness_version,
                    experiment.linear_project_id,
                    experiment.execution_lane or "",
                    experiment.repetitions,
                    experiment.created_at.isoformat(),
                ),
            )
        return experiment

    def get(self, experiment_id: str) -> Experiment | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, status, mode, candidate_a, candidate_b, hypothesis, design,
                       candidate_a_profile,
                       candidate_b_profile, system_version_a, system_version_b,
                       executor_toolchain_version, harness_version, linear_project_id,
                       execution_lane, repetitions, created_at
                FROM bench_experiments WHERE id = ?
                """,
                (experiment_id,),
            ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["candidate_b"] = payload["candidate_b"] or None
        payload["candidate_a_profile"] = json.loads(payload["candidate_a_profile"])
        payload["candidate_b_profile"] = json.loads(payload["candidate_b_profile"])
        payload["execution_lane"] = payload["execution_lane"] or None
        return Experiment.model_validate(payload)

    def claim_next(self) -> Experiment | None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            running = conn.execute(
                """
                SELECT mode, execution_lane FROM bench_experiments
                WHERE status = 'running'
                """
            ).fetchall()
            occupied: set[str] = set()
            for active in running:
                lane = str(active["execution_lane"])
                if lane == "AB" or (not lane and active["mode"] == "paired"):
                    occupied.update(("A", "B"))
                elif lane in {"A", "B"}:
                    occupied.add(lane)
                elif not lane:
                    occupied.add("A")
            row = conn.execute(
                """
                SELECT id, status, mode, candidate_a, candidate_b, hypothesis, design,
                       candidate_a_profile,
                       candidate_b_profile, system_version_a, system_version_b,
                       executor_toolchain_version, harness_version, linear_project_id,
                       execution_lane, repetitions, created_at
                FROM bench_experiments
                WHERE status = 'queued'
                ORDER BY created_at, id
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            if row["mode"] == "paired":
                if occupied:
                    return None
                execution_lane = "AB"
            elif "A" not in occupied:
                execution_lane = "A"
            elif "B" not in occupied:
                execution_lane = "B"
            else:
                return None
            conn.execute(
                """
                UPDATE bench_experiments SET status = 'running', execution_lane = ?
                WHERE id = ?
                """,
                (execution_lane, row["id"]),
            )
        claimed = dict(row)
        claimed["candidate_b"] = claimed["candidate_b"] or None
        claimed["candidate_a_profile"] = json.loads(claimed["candidate_a_profile"])
        claimed["candidate_b_profile"] = json.loads(claimed["candidate_b_profile"])
        claimed["status"] = "running"
        claimed["execution_lane"] = execution_lane
        return Experiment.model_validate(claimed)

    def preparing(self) -> list[Experiment]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id FROM bench_experiments
                WHERE status = 'preparing'
                ORDER BY created_at, id
                """
            ).fetchall()
        return [experiment for row in rows if (experiment := self.get(str(row["id"]))) is not None]

    def activate(self, experiment_id: str, project_id: str) -> None:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE bench_experiments
                SET status = 'queued', linear_project_id = ?
                WHERE id = ? AND status = 'preparing'
                """,
                (project_id, experiment_id),
            )
        if cursor.rowcount != 1:
            raise KeyError(experiment_id)

    def set_status(self, experiment_id: str, status: ExperimentStatus) -> None:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE bench_experiments
                SET status = ?, chronicle_recovery_pending =
                    CASE WHEN ? = 'failed' THEN 1 ELSE chronicle_recovery_pending END
                WHERE id = ?
                """,
                (status, status, experiment_id),
            )
        if cursor.rowcount != 1:
            raise KeyError(experiment_id)

    def set_harness_version(self, experiment_id: str, harness_version: str) -> None:
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE bench_experiments SET harness_version = ? WHERE id = ?",
                (harness_version, experiment_id),
            )
        if cursor.rowcount != 1:
            raise KeyError(experiment_id)

    def fail_interrupted(self) -> tuple[list[Trial], list[str]]:
        """Close orphaned running work before this process claims a new experiment."""
        ended_at = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE bench_trials
                SET status = 'failed', ended_at = ?,
                    error = 'bench worker restarted during this trial',
                    chronicle_recovery_pending = 1
                WHERE status = 'running'
                """,
                (ended_at,),
            )
            conn.execute(
                """
                UPDATE bench_experiments
                SET status = 'failed', chronicle_recovery_pending = 1
                WHERE status = 'running'
                """
            )
            trial_rows = conn.execute(
                """
                SELECT t.experiment_id, t.candidate, t.repetition, t.revision, t.profile,
                       t.system_version, t.execution_lane, e.hypothesis, e.design,
                       e.linear_project_id
                FROM bench_trials AS t
                JOIN bench_experiments AS e ON e.id = t.experiment_id
                WHERE t.chronicle_recovery_pending = 1
                ORDER BY t.started_at, t.experiment_id, t.candidate, t.repetition
                """
            ).fetchall()
            experiment_rows = conn.execute(
                """
                SELECT id FROM bench_experiments
                WHERE chronicle_recovery_pending = 1
                ORDER BY created_at, id
                """
            ).fetchall()
        trials = [
            Trial.model_validate(
                {
                    **dict(row),
                    "profile": json.loads(row["profile"]),
                }
            )
            for row in trial_rows
        ]
        return trials, [str(row["id"]) for row in experiment_rows]

    def acknowledge_trial_chronicle_recovery(self, trial: Trial) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE bench_trials SET chronicle_recovery_pending = 0
                WHERE experiment_id = ? AND candidate = ? AND repetition = ?
                """,
                (trial.experiment_id, trial.candidate, trial.repetition),
            )

    def acknowledge_experiment_chronicle_recovery(self, experiment_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE bench_experiments SET chronicle_recovery_pending = 0 WHERE id = ?
                """,
                (experiment_id,),
            )

    def start_trial(self, trial: Trial) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO bench_trials (
                    experiment_id, candidate, repetition, revision, profile,
                    system_version, execution_lane, status, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'running', ?)
                """,
                (
                    trial.experiment_id,
                    trial.candidate,
                    trial.repetition,
                    trial.revision,
                    json.dumps(trial.profile, sort_keys=True),
                    trial.system_version,
                    trial.execution_lane,
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
                       system_version, execution_lane, status,
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

    def queue_notification(self, trial: Trial, markdown: str) -> None:
        event_key = f"{trial.experiment_id}:{trial.candidate}{trial.repetition}"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO bench_notifications (
                    event_key, experiment_id, candidate, repetition, markdown, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_key,
                    trial.experiment_id,
                    trial.candidate,
                    trial.repetition,
                    markdown,
                    datetime.now(UTC).isoformat(),
                ),
            )

    def pending_notifications(self) -> list[BenchNotification]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT event_key, experiment_id, candidate, repetition, markdown, created_at
                FROM bench_notifications
                WHERE acknowledged_at IS NULL
                ORDER BY created_at, event_key
                """
            ).fetchall()
        return [BenchNotification.model_validate(dict(row)) for row in rows]

    def acknowledge_notification(self, event_key: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE bench_notifications
                SET acknowledged_at = ?
                WHERE event_key = ? AND acknowledged_at IS NULL
                """,
                (datetime.now(UTC).isoformat(), event_key),
            )
        return cursor.rowcount == 1

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
