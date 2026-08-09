import asyncio
import json
from importlib.resources import files
from pathlib import Path

import httpx
import pytest
import respx
from click.testing import CliRunner

from symphony.bench import cli as cli_module
from symphony.bench.cli import (
    _BENCH_BINDING_OVERRIDES,
    _BenchSecrets,
    _cleanup_stale_grader_preflights,
    _prepare_harness_with_preflight,
    _serve_with_profile,
)
from symphony.bench.models import Experiment, ExperimentCreate, Trial, TrialOutcome
from symphony.cli import main


def test_bench_profile_allows_hybrid_local_review_mode() -> None:
    assert "local_review_mode" in _BENCH_BINDING_OVERRIDES


def test_packaged_bench_profile_keeps_review_limits_and_remote_gate() -> None:
    profile = json.loads(
        files("symphony.bench.assets").joinpath("profiles/current.json").read_text()
    )

    assert profile["knobs"]["local_review_iteration_cap"] == 5
    assert profile["binding"]["remote_review"] is True


def test_serve_routes_single_lane_b_and_preflights_only_assigned_lane(
    tmp_path: Path, monkeypatch
) -> None:
    captured: dict[str, object] = {}
    calls: list[tuple[str, str]] = []

    class FakeCommands:
        def __init__(self, *, base_url: str, **_kwargs: object) -> None:
            self.base_url = base_url

    class FakeExecutor:
        def __init__(self, *, config: object, **_kwargs: object) -> None:
            self.lane = Path(config.root).name  # type: ignore[attr-defined]

        async def __call__(self, trial: Trial) -> TrialOutcome:
            calls.append(("execute", self.lane))
            return TrialOutcome()

        async def publish_interrupted(self, trial: Trial) -> None:
            calls.append(("publish", self.lane))

        async def recover_chronicle(self) -> None:
            pass

        async def start_experiment(self, _experiment: object) -> str:
            return "project"

        async def publish_failed_experiment(self, _experiment: object) -> None:
            pass

        async def resolve_revision(self, revision: str) -> str:
            return revision

    async def prepare(
        _experiment_id: str, *, lanes: object, **_kwargs: object
    ) -> str:
        captured["lanes"] = [lane[0] for lane in lanes]  # type: ignore[union-attr]
        return "harness"

    async def snapshot(experiment_id: str, **_kwargs: object) -> str:
        captured["snapshotted"] = experiment_id
        return "harness"

    def create_app(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    class FakeUvicorn:
        @staticmethod
        def run(_app: object, **_kwargs: object) -> None:
            pass

    profile = tmp_path / "profile.json"
    profile.write_text("{}")
    monkeypatch.setattr(cli_module, "RemoteCommands", FakeCommands)
    monkeypatch.setattr(cli_module, "LiveTrialExecutor", FakeExecutor)
    monkeypatch.setattr(cli_module, "create_bench_app", create_app)
    monkeypatch.setattr(cli_module, "_preflight_harness", prepare)
    monkeypatch.setattr(cli_module, "_snapshot_harness", snapshot)

    _serve_with_profile(
        profile,
        db_path=tmp_path / "bench.sqlite",
        root_a=tmp_path / "bench-a",
        root_b=tmp_path / "bench-b",
        private_root=tmp_path / "private",
        controls_root=tmp_path / "controls",
        api_token="api",
        github_owner="owner",
        linear_team_id="team",
        linear_label_id="label",
        linear_label_name="Bench",
        symphony_repository="repo",
        encryption_key="key",
        executor_a_url="http://a",
        executor_b_url="http://b",
        executor_token="executor",
        host="127.0.0.1",
        port=8080,
        uvicorn_module=FakeUvicorn,
    )

    trial = Trial(
        experiment_id="EXP-1",
        candidate="A",
        repetition=1,
        revision="sha",
        execution_lane="B",
    )
    asyncio.run(captured["execute"](trial))  # type: ignore[operator]
    asyncio.run(captured["publish_interrupted"](trial))  # type: ignore[operator]
    asyncio.run(captured["snapshot_harness"]("EXP-1"))  # type: ignore[operator]
    experiment = Experiment.queued(
        experiment_id="EXP-1",
        request=ExperimentCreate(
            mode="single",
            candidate_a="sha",
            hypothesis="Hypothesis.",
            design="Design.",
            repetitions=1,
        ),
    ).model_copy(update={"execution_lane": "B"})
    asyncio.run(captured["prepare_harness"](experiment))  # type: ignore[operator]

    assert calls == [("execute", "bench-b"), ("publish", "bench-b")]
    assert captured["snapshotted"] == "EXP-1"
    assert captured["lanes"] == ["B"]


@respx.mock
def test_verify_submit_calls_bench_and_prints_experiment_id() -> None:
    route = respx.post("https://bench.example/experiments").mock(
        return_value=httpx.Response(
            201,
            json={
                "id": "EXP-123",
                "status": "queued",
                "candidate_a": "same-sha",
                "candidate_b": "same-sha",
                "repetitions": 3,
                "created_at": "2026-08-01T10:00:00Z",
            },
        )
    )

    result = CliRunner().invoke(
        main,
        [
            "verify",
            "submit",
            "--url",
            "https://bench.example",
            "--token",
            "secret",
            "--candidate-a",
            "same-sha",
            "--candidate-b",
            "same-sha",
            "--hypothesis",
            "The revised review process will finish the complete sample project.",
            "--design",
            "Run the project once with each version and compare completion, time, "
            "and review findings.",
            "--repetitions",
            "3",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output.endswith("EXP-123 queued\n")
    assert route.calls[0].request.headers["Authorization"] == "Bearer secret"
    assert route.calls[0].request.content == (
        b'{"candidate_a":"same-sha","candidate_b":"same-sha",'
        b'"hypothesis":"The revised review process will finish the complete sample project.",'
        b'"design":"Run the project once with each version and compare completion, time, '
        b'and review findings.",'
        b'"repetitions":3}'
    )
    assert route.calls[0].request.extensions["timeout"]["read"] == 15 * 60


@respx.mock
def test_verify_submit_single_omits_candidate_b() -> None:
    route = respx.post("https://bench.example/experiments").mock(
        return_value=httpx.Response(201, json={"id": "EXP-ONE", "status": "queued"})
    )

    result = CliRunner().invoke(
        main,
        [
            "verify",
            "submit",
            "--url",
            "https://bench.example",
            "--token",
            "secret",
            "--mode",
            "single",
            "--candidate-a",
            "same-sha",
            "--hypothesis",
            "The tested version will finish the complete sample project.",
            "--design",
            "Run one isolated copy and record completion, quality, time, and cost.",
            "--repetitions",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(route.calls[0].request.content) == {
        "mode": "single",
        "candidate_a": "same-sha",
        "hypothesis": "The tested version will finish the complete sample project.",
        "design": "Run one isolated copy and record completion, quality, time, and cost.",
        "repetitions": 1,
    }


def test_bench_settings_load_routing_from_mounted_env(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "\n".join(
            (
                "SYMPHONY_BENCH_GITHUB_OWNER=other-owner",
                "SYMPHONY_BENCH_LINEAR_TEAM_ID=other-team",
                "SYMPHONY_BENCH_LINEAR_LABEL_ID=other-label",
                "SYMPHONY_BENCH_LINEAR_LABEL_NAME=other-name",
                "SYMPHONY_BENCH_REPOSITORY=https://github.com/other/symphony.git",
            )
        )
    )

    settings = _BenchSecrets()

    assert settings.github_owner == "other-owner"
    assert settings.linear_team_id == "other-team"
    assert settings.linear_label_id == "other-label"
    assert settings.linear_label_name == "other-name"
    assert settings.symphony_repository == "https://github.com/other/symphony.git"


@pytest.mark.asyncio
async def test_harness_preflight_validates_controls_on_both_lanes_and_persists_receipt(
    tmp_path, monkeypatch, private_bench_controls
) -> None:
    seen: list[object] = []

    class FakeGrader:
        def __init__(self, commands: object) -> None:
            self.commands = commands

        async def validate_controls(self, **kwargs: object) -> dict[str, object]:
            seen.append(self.commands)
            assert kwargs["seed_root"].exists()
            assert kwargs["reference_root"].exists()
            assert sorted(kwargs["mutation_roots"]) == [
                "broken_accessibility",
                "broken_workflow",
            ]
            assert all(path.exists() for path in kwargs["mutation_roots"].values())
            assert kwargs["backend_hidden_test"].exists()
            assert kwargs["frontend_hidden_test"].exists()
            return {
                "reference": {"hidden_checks_passed": 24},
                "seed": {"hidden_checks_passed": 2},
            }

    monkeypatch.setattr(cli_module, "SupportQueueGrader", FakeGrader)
    commands_a = object()
    commands_b = object()

    version = await _prepare_harness_with_preflight(
        "EXP-PREFLIGHT",
        private_root=tmp_path / "private",
        controls_root=private_bench_controls,
        lanes=(
            ("A", tmp_path / "a", commands_a),
            ("B", tmp_path / "b", commands_b),
        ),
    )

    receipt = json.loads(
        (tmp_path / "private/EXP-PREFLIGHT/preflight.json").read_text(encoding="utf-8")
    )
    assert len(seen) == 2
    assert set(map(id, seen)) == {id(commands_a), id(commands_b)}
    assert receipt["status"] == "passed"
    assert [lane["lane"] for lane in receipt["lanes"]] == ["A", "B"]
    assert receipt["harness_version"] == version
    assert not (tmp_path / "a/.grader-preflight-EXP-PREFLIGHT").exists()
    assert not (tmp_path / "b/.grader-preflight-EXP-PREFLIGHT").exists()


def test_stale_grader_preflight_cleanup_removes_only_reserved_directories(tmp_path) -> None:
    stale = tmp_path / ".grader-preflight-EXP-OLD"
    readonly = stale / "reference/package"
    readonly.mkdir(parents=True)
    (readonly / "hidden.py").write_text("secret")
    readonly.chmod(0o555)
    readonly.parent.chmod(0o555)
    stale.chmod(0o555)
    unrelated = tmp_path / "keep"
    unrelated.mkdir()

    assert _cleanup_stale_grader_preflights(tmp_path) == 1
    assert not stale.exists()
    assert unrelated.exists()


@pytest.mark.asyncio
async def test_failed_preflight_keeps_receipt_but_removes_private_harness(
    tmp_path, monkeypatch, private_bench_controls
) -> None:
    class BrokenGrader:
        def __init__(self, _commands: object) -> None:
            pass

        async def validate_controls(self, **_kwargs: object) -> dict[str, object]:
            raise cli_module.GraderInfrastructureError("broken control")

    monkeypatch.setattr(cli_module, "SupportQueueGrader", BrokenGrader)

    with pytest.raises(cli_module.GraderInfrastructureError, match="broken control"):
        await _prepare_harness_with_preflight(
            "EXP-BROKEN",
            private_root=tmp_path / "private",
            controls_root=private_bench_controls,
            lanes=(
                ("A", tmp_path / "a", object()),
                ("B", tmp_path / "b", object()),
            ),
        )

    experiment_root = tmp_path / "private/EXP-BROKEN"
    assert (experiment_root / "preflight.json").exists()
    assert not (experiment_root / "_harness").exists()


@pytest.mark.asyncio
async def test_invalid_private_controls_leave_no_partial_harness(
    tmp_path, private_bench_controls
) -> None:
    manifest = private_bench_controls / "hidden/support_queue/manifest.json"
    manifest.write_text("{}", encoding="utf-8")

    with pytest.raises(cli_module.GraderInfrastructureError, match="manifest"):
        await _prepare_harness_with_preflight(
            "EXP-INVALID",
            private_root=tmp_path / "private",
            controls_root=private_bench_controls,
            lanes=(
                ("A", tmp_path / "a", object()),
                ("B", tmp_path / "b", object()),
            ),
        )

    assert not (tmp_path / "private/EXP-INVALID/_harness").exists()
