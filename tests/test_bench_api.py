import asyncio
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from symphony.bench.app import create_bench_app
from symphony.bench.grader import GraderInfrastructureError
from symphony.bench.models import (
    EXECUTOR_TOOLCHAIN_VERSION,
    ExperimentCreate,
    Trial,
    TrialExecutionError,
    TrialOutcome,
    read_executor_toolchain_version,
    system_version,
)
from symphony.bench.runner import ExperimentRunner
from symphony.bench.store import ExperimentStore


def test_executor_toolchain_version_reads_built_image_receipt(tmp_path: Path) -> None:
    receipt = tmp_path / "toolchain.txt"
    receipt.write_text("python=3.12.11;node=v22.18.0\n", encoding="utf-8")

    assert read_executor_toolchain_version(receipt) == "python=3.12.11;node=v22.18.0"


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


def test_submit_rejects_broken_grader_preflight_without_queueing_experiment(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "bench.sqlite"

    async def prepare(_experiment_id: str) -> str:
        raise GraderInfrastructureError("reference: expected 9 backend checks, got 0")

    app = create_bench_app(
        db_path=db_path,
        api_token="token",
        prepare_harness=prepare,
    )
    with TestClient(app) as client:
        response = client.post(
            "/experiments",
            headers={"Authorization": "Bearer token"},
            json={"candidate_a": "sha", "candidate_b": "sha", "repetitions": 1},
        )

    assert response.status_code == 500
    assert response.json()["detail"].startswith("infrastructure_failed:")
    assert ExperimentStore(db_path).claim_next() is None


def test_submit_does_not_run_preflight_while_an_experiment_is_active(tmp_path: Path) -> None:
    prepared: list[str] = []

    async def prepare(experiment_id: str) -> str:
        prepared.append(experiment_id)
        return "harness"

    app = create_bench_app(
        db_path=tmp_path / "bench.sqlite",
        api_token="token",
        prepare_harness=prepare,
    )
    headers = {"Authorization": "Bearer token"}
    with TestClient(app) as client:
        first = client.post(
            "/experiments",
            headers=headers,
            json={"candidate_a": "sha", "candidate_b": "sha", "repetitions": 1},
        )
        second = client.post(
            "/experiments",
            headers=headers,
            json={"candidate_a": "sha", "candidate_b": "sha", "repetitions": 1},
        )

    assert first.status_code == 201
    assert second.status_code == 409
    assert second.json()["detail"] == "another experiment is queued or running"
    assert len(prepared) == 1


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
        ("A", 1, "completed"),
        ("B", 1, "completed"),
        ("A", 2, "completed"),
        ("B", 2, "completed"),
        ("A", 3, "completed"),
        ("B", 3, "completed"),
    ]
    assert report.json()["trials"][0]["repository_url"].endswith("/A1")
    assert report.json()["trials"][0]["metrics"] == {"effective_tokens": 10}


@pytest.mark.asyncio
async def test_runner_runs_each_candidate_pair_concurrently(tmp_path: Path) -> None:
    store = ExperimentStore(tmp_path / "bench.sqlite")
    store.create(ExperimentCreate(candidate_a="a", candidate_b="b", repetitions=2))
    waiting: dict[int, set[str]] = {}
    released: list[int] = []

    async def execute(trial: Trial) -> TrialOutcome:
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
async def test_runner_cancels_sibling_and_keeps_both_failure_receipts(
    tmp_path: Path,
) -> None:
    store = ExperimentStore(tmp_path / "bench.sqlite")
    experiment = store.create(ExperimentCreate(candidate_a="a", candidate_b="b", repetitions=2))

    async def execute(trial: Trial) -> TrialOutcome:
        if trial.candidate == "A":
            await asyncio.sleep(0)
            raise TrialExecutionError(
                "A failed",
                outcome=TrialOutcome(repository_url="https://github.com/example/A1"),
            )
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    with pytest.raises(TrialExecutionError, match="A failed"):
        await ExperimentRunner(store=store, execute=execute).run_next()

    report = store.report(experiment.id)
    assert report is not None
    assert report.experiment.status == "failed"
    assert [(trial.candidate, trial.status) for trial in report.trials] == [
        ("A", "failed"),
        ("B", "failed"),
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
    assert seen == ["A1", "B1"]


def test_completed_trial_persists_report_and_notification_until_ack(tmp_path: Path) -> None:
    db_path = tmp_path / "bench.sqlite"
    reports_root = tmp_path / "reports"
    headers = {"Authorization": "Bearer test-token"}

    async def execute(trial: Trial) -> TrialOutcome:
        return TrialOutcome(
            repository_url=f"https://github.com/example/{trial.candidate}{trial.repetition}",
            issue_urls=["https://linear.app/example/BENCH-1"],
            metrics={
                "wall_seconds": 12.5,
                "input_tokens": 100,
                "output_tokens": 20,
                "cache_read_tokens": 30,
                "cache_write_tokens": 0,
                "effective_tokens": 117,
                "cost_usd": 1.25,
                "local_review_rounds": 2,
                "local_review_major": 1,
                "remote_review_rounds": 1,
                "remote_review_p2": 1,
                "hidden_checks_passed": 7,
                "hidden_checks_total": 7,
                "runs_by_status": {"completed": 2},
            },
        )

    app = create_bench_app(
        db_path=db_path,
        api_token="test-token",
        execute=execute,
        reports_root=reports_root,
        idle_poll_seconds=0.01,
    )
    with TestClient(app) as client:
        submitted = client.post(
            "/experiments",
            headers=headers,
            json={"candidate_a": "sha", "candidate_b": "sha", "repetitions": 1},
        ).json()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            notifications = client.get("/notifications", headers=headers).json()
            current = client.get(f"/experiments/{submitted['id']}", headers=headers).json()
            if len(notifications) == 2 and current["status"] == "completed":
                break
            time.sleep(0.01)

    report_path = reports_root / submitted["id"] / "A1.md"
    assert report_path.exists()
    final_path = reports_root / submitted["id"] / "FINAL.md"
    assert final_path.exists()
    assert "Status: **completed**" in final_path.read_text(encoding="utf-8")
    assert notifications[0]["event_key"] == f"{submitted['id']}:A1"
    assert notifications[0]["markdown"] == report_path.read_text(encoding="utf-8")
    for expected in (
        "Status: **completed**",
        "wall_seconds: `12.5`",
        "input_tokens: `100`",
        "cost_usd: `1.25`",
        "local_review_major: `1`",
        "remote_review_p2: `1`",
        "hidden_checks_passed: `7`",
        'runs_by_status: `{"completed": 2}`',
        "Next step:",
    ):
        assert expected in notifications[0]["markdown"]

    with TestClient(
        create_bench_app(
            db_path=db_path,
            api_token="test-token",
            reports_root=reports_root,
        )
    ) as client:
        pending = client.get("/experiments/_notifications", headers=headers).json()
        assert len(pending) == 2
        acknowledged = client.post(
            f"/experiments/_notifications/{pending[0]['event_key']}/ack",
            headers=headers,
        )
        assert acknowledged.status_code == 204
        assert len(client.get("/experiments/_notifications", headers=headers).json()) == 1


def test_failed_pair_persists_both_trial_receipts_and_final_report(tmp_path: Path) -> None:
    db_path = tmp_path / "bench.sqlite"
    reports_root = tmp_path / "reports"
    headers = {"Authorization": "Bearer test-token"}

    async def execute(trial: Trial) -> TrialOutcome:
        if trial.candidate == "A":
            raise TrialExecutionError(
                "candidate failed",
                outcome=TrialOutcome(metrics={"input_tokens": 10, "wall_seconds": 1}),
            )
        return TrialOutcome(metrics={"input_tokens": 20, "wall_seconds": 2})

    app = create_bench_app(
        db_path=db_path,
        api_token="test-token",
        execute=execute,
        reports_root=reports_root,
        idle_poll_seconds=0.01,
    )
    with TestClient(app) as client:
        submitted = client.post(
            "/experiments",
            headers=headers,
            json={"candidate_a": "sha", "candidate_b": "sha", "repetitions": 1},
        ).json()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            current = client.get(f"/experiments/{submitted['id']}", headers=headers).json()
            if current["status"] == "failed":
                break
            time.sleep(0.01)

    directory = reports_root / submitted["id"]
    assert (directory / "A1.md").exists()
    assert (directory / "B1.md").exists()
    assert "candidate failed" in (directory / "A1.md").read_text(encoding="utf-8")
    assert "Status: **failed**" in (directory / "FINAL.md").read_text(encoding="utf-8")


def test_restart_persists_interrupted_trial_and_final_receipts(tmp_path: Path) -> None:
    db_path = tmp_path / "bench.sqlite"
    reports_root = tmp_path / "reports"
    store = ExperimentStore(db_path)
    experiment = store.create(
        ExperimentCreate(candidate_a="sha", candidate_b="sha", repetitions=1)
    )
    claimed = store.claim_next()
    assert claimed is not None
    trial = Trial(
        experiment_id=experiment.id,
        candidate="A",
        repetition=1,
        revision="sha",
    )
    store.start_trial(trial)

    app = create_bench_app(
        db_path=db_path,
        api_token="token",
        reports_root=reports_root,
    )
    with TestClient(app) as client:
        notifications = client.get(
            "/notifications", headers={"Authorization": "Bearer token"}
        ).json()

    directory = reports_root / experiment.id
    assert (directory / "A1.md").exists()
    assert "bench worker restarted" in (directory / "A1.md").read_text(encoding="utf-8")
    assert "Status: **failed**" in (directory / "FINAL.md").read_text(encoding="utf-8")
    assert [notification["event_key"] for notification in notifications] == [
        f"{experiment.id}:A1"
    ]


def test_app_marks_interrupted_trial_failed_before_claiming_more(tmp_path: Path) -> None:
    db_path = tmp_path / "bench.sqlite"
    store = ExperimentStore(db_path)
    experiment = store.create(ExperimentCreate(candidate_a="sha", candidate_b="sha", repetitions=1))
    claimed = store.claim_next()
    assert claimed is not None
    trial = Trial(
        experiment_id=experiment.id,
        candidate="A",
        repetition=1,
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
