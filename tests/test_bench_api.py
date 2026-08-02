import asyncio
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from symphony.bench.app import create_bench_app
from symphony.bench.models import (
    EXECUTOR_TOOLCHAIN_VERSION,
    ExperimentCreate,
    Trial,
    TrialExecutionError,
    TrialOutcome,
    system_version,
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
    assert body["executor_toolchain_version"] == EXECUTOR_TOOLCHAIN_VERSION
    assert prepared == [body["id"]]
    assert body["harness_version"] == "snapshotted-harness"


def test_system_version_includes_executor_toolchain_identity() -> None:
    assert system_version("same-sha", {}, "toolchain-a") != system_version(
        "same-sha", {}, "toolchain-b"
    )


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


@pytest.mark.asyncio
async def test_runner_runs_each_candidate_pair_concurrently(tmp_path: Path) -> None:
    store = ExperimentStore(tmp_path / "bench.sqlite")
    store.create(ExperimentCreate(candidate_a="a", candidate_b="b", repetitions=2))
    waiting: dict[int, set[str]] = {}
    released: list[int] = []

    async def execute(trial: Trial) -> TrialOutcome:
        if trial.candidate == "S":
            return TrialOutcome()
        candidates = waiting.setdefault(trial.repetition, set())
        candidates.add(trial.candidate)
        deadline = asyncio.get_running_loop().time() + 1
        while candidates != {"A", "B"}:
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("candidate pair did not overlap")
            await asyncio.sleep(0)
        released.append(trial.repetition)
        return TrialOutcome()

    await ExperimentRunner(store=store, execute=execute).run_next()

    assert waiting == {1: {"A", "B"}, 2: {"A", "B"}}
    assert set(released) == {1, 2}


@pytest.mark.asyncio
async def test_runner_keeps_counterpart_receipt_when_one_parallel_trial_fails(
    tmp_path: Path,
) -> None:
    store = ExperimentStore(tmp_path / "bench.sqlite")
    experiment = store.create(ExperimentCreate(candidate_a="a", candidate_b="b", repetitions=2))

    async def execute(trial: Trial) -> TrialOutcome:
        if trial.candidate == "A":
            raise TrialExecutionError(
                "A failed",
                outcome=TrialOutcome(repository_url="https://github.com/example/A1"),
            )
        await asyncio.sleep(0)
        return TrialOutcome(repository_url=f"https://github.com/example/{trial.candidate}1")

    with pytest.raises(TrialExecutionError, match="A failed"):
        await ExperimentRunner(store=store, execute=execute).run_next()

    report = store.report(experiment.id)
    assert report is not None
    assert report.experiment.status == "failed"
    assert [(trial.candidate, trial.status) for trial in report.trials] == [
        ("S", "completed"),
        ("A", "failed"),
        ("B", "completed"),
    ]


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
