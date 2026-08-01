import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from symphony.bench.app import create_bench_app
from symphony.bench.models import (
    ExperimentCreate,
    Trial,
    TrialExecutionError,
    TrialOutcome,
)
from symphony.bench.runner import ExperimentRunner
from symphony.bench.store import ExperimentStore


def test_submit_and_read_experiment_survives_restart(tmp_path: Path) -> None:
    db_path = tmp_path / "bench.sqlite"
    headers = {"Authorization": "Bearer test-token"}

    with TestClient(create_bench_app(db_path=db_path, api_token="test-token")) as client:
        unauthorized = client.post(
            "/experiments",
            json={"candidate_a": "ee195f4", "candidate_b": "ee195f4", "repetitions": 3},
        )
        assert unauthorized.status_code == 401

        submitted = client.post(
            "/experiments",
            headers=headers,
            json={"candidate_a": "ee195f4", "candidate_b": "ee195f4", "repetitions": 3},
        )
        assert submitted.status_code == 201
        body = submitted.json()
        assert body["id"].startswith("EXP-")
        assert body["status"] == "queued"
        assert body["candidate_a"] == "ee195f4"
        assert body["candidate_b"] == "ee195f4"
        assert body["repetitions"] == 3

    with TestClient(create_bench_app(db_path=db_path, api_token="test-token")) as client:
        status = client.get(f"/experiments/{body['id']}", headers=headers)

    assert status.status_code == 200
    assert status.json() == body


def test_submit_pins_revision_profile_and_harness_version(tmp_path: Path) -> None:
    seen: list[str] = []
    prepared: list[str] = []

    async def resolve(revision: str) -> str:
        seen.append(revision)
        return "a" * 40

    async def prepare(experiment_id: str) -> str:
        prepared.append(experiment_id)
        return "snapshotted-harness"

    app = create_bench_app(
        db_path=tmp_path / "bench.sqlite",
        api_token="token",
        default_profile={"binding": {"local_review": True}},
        resolve_revision=resolve,
        harness_version="harness-v1",
        prepare_harness=prepare,
    )
    with TestClient(app) as client:
        response = client.post(
            "/experiments",
            headers={"Authorization": "Bearer token"},
            json={"candidate_a": "main", "candidate_b": "main", "repetitions": 1},
        )

    assert response.status_code == 201
    body = response.json()
    assert seen == ["main"]
    assert body["candidate_a"] == "a" * 40
    assert body["candidate_b"] == "a" * 40
    assert body["candidate_a_profile"] == {"binding": {"local_review": True}}
    assert body["candidate_b_profile"] == body["candidate_a_profile"]
    assert body["system_version_a"] == body["system_version_b"]
    assert prepared == [body["id"]]
    assert body["harness_version"] == "snapshotted-harness"


@pytest.mark.asyncio
async def test_runner_interleaves_candidates_and_completes_experiment(tmp_path: Path) -> None:
    store = ExperimentStore(tmp_path / "bench.sqlite")
    experiment = store.create(
        ExperimentCreate(candidate_a="same-sha", candidate_b="same-sha", repetitions=3)
    )
    seen: list[tuple[str, int, str]] = []

    async def execute(trial: Trial) -> TrialOutcome:
        seen.append((trial.candidate, trial.repetition, trial.revision))
        return TrialOutcome(
            repository_url=f"https://github.com/example/{trial.candidate}{trial.repetition}",
            issue_urls=[f"https://linear.app/{trial.candidate}{trial.repetition}"],
            metrics={"effective_tokens": 10},
        )

    ran = await ExperimentRunner(store=store, execute=execute).run_next()

    assert ran == experiment.id
    assert seen == [
        ("S", 0, "same-sha"),
        ("A", 1, "same-sha"),
        ("B", 1, "same-sha"),
        ("A", 2, "same-sha"),
        ("B", 2, "same-sha"),
        ("A", 3, "same-sha"),
        ("B", 3, "same-sha"),
    ]
    assert store.get(experiment.id).status == "completed"  # type: ignore[union-attr]

    with TestClient(
        create_bench_app(db_path=tmp_path / "bench.sqlite", api_token="test-token")
    ) as client:
        report = client.get(
            f"/experiments/{experiment.id}/report",
            headers={"Authorization": "Bearer test-token"},
        )

    assert report.status_code == 200
    assert report.json()["experiment"]["status"] == "completed"
    assert [
        (trial["candidate"], trial["repetition"], trial["status"])
        for trial in report.json()["trials"]
    ] == [
        ("S", 0, "completed"),
        ("A", 1, "completed"),
        ("B", 1, "completed"),
        ("A", 2, "completed"),
        ("B", 2, "completed"),
        ("A", 3, "completed"),
        ("B", 3, "completed"),
    ]
    assert report.json()["trials"][0]["repository_url"].endswith("/S0")
    assert report.json()["trials"][0]["metrics"] == {"effective_tokens": 10}


def test_app_worker_drains_one_experiment_at_a_time(tmp_path: Path) -> None:
    seen: list[str] = []

    async def execute(trial: Trial) -> None:
        seen.append(f"{trial.candidate}{trial.repetition}")

    app = create_bench_app(
        db_path=tmp_path / "bench.sqlite",
        api_token="test-token",
        execute=execute,
        idle_poll_seconds=0.01,
    )
    headers = {"Authorization": "Bearer test-token"}
    with TestClient(app) as client:
        submitted = client.post(
            "/experiments",
            headers=headers,
            json={"candidate_a": "sha", "candidate_b": "sha", "repetitions": 1},
        ).json()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            current = client.get(f"/experiments/{submitted['id']}", headers=headers).json()
            if current["status"] == "completed":
                break
            time.sleep(0.01)

    assert current["status"] == "completed"
    assert seen == ["S0", "A1", "B1"]


def test_app_marks_interrupted_trial_failed_before_claiming_more(tmp_path: Path) -> None:
    db_path = tmp_path / "bench.sqlite"
    store = ExperimentStore(db_path)
    experiment = store.create(ExperimentCreate(candidate_a="sha", candidate_b="sha", repetitions=1))
    claimed = store.claim_next()
    assert claimed is not None
    trial = Trial(
        experiment_id=experiment.id,
        candidate="S",
        repetition=0,
        revision="sha",
    )
    store.start_trial(trial)

    with TestClient(create_bench_app(db_path=db_path, api_token="token")):
        pass

    report = store.report(experiment.id)
    assert report is not None
    assert report.experiment.status == "failed"
    assert report.trials[0].status == "failed"
    assert report.trials[0].error == "bench worker restarted during this trial"


@pytest.mark.asyncio
async def test_failed_trial_keeps_partial_receipts_and_metrics(tmp_path: Path) -> None:
    store = ExperimentStore(tmp_path / "bench.sqlite")
    experiment = store.create(ExperimentCreate(candidate_a="sha", candidate_b="sha", repetitions=1))

    async def fail(_trial: Trial) -> TrialOutcome:
        raise TrialExecutionError(
            "agent launch cap exceeded",
            outcome=TrialOutcome(
                repository_url="https://github.com/kulichevskiy/failed-trial",
                issue_urls=["https://linear.app/bench/issue/BENCH-1"],
                metrics={"effective_tokens": 123, "agent_launches": 120},
            ),
        )

    with pytest.raises(TrialExecutionError):
        await ExperimentRunner(store=store, execute=fail).run_next()

    report = store.report(experiment.id)
    assert report is not None
    trial = report.trials[0]
    assert trial.status == "failed"
    assert trial.repository_url == "https://github.com/kulichevskiy/failed-trial"
    assert trial.issue_urls == ["https://linear.app/bench/issue/BENCH-1"]
    assert trial.metrics == {"effective_tokens": 123, "agent_launches": 120}
