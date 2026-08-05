import json

import httpx
import pytest
import respx
from click.testing import CliRunner

from symphony.bench import cli as cli_module
from symphony.bench.cli import (
    _BenchSecrets,
    _cleanup_stale_grader_preflights,
    _prepare_harness_with_preflight,
)
from symphony.cli import main


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
            "--repetitions",
            "3",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output.endswith("EXP-123 queued\n")
    assert route.calls[0].request.headers["Authorization"] == "Bearer secret"
    assert route.calls[0].request.content == (
        b'{"candidate_a":"same-sha","candidate_b":"same-sha","repetitions":3}'
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
            "--repetitions",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(route.calls[0].request.content) == {
        "mode": "single",
        "candidate_a": "same-sha",
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
