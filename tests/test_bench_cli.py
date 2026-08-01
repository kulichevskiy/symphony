import httpx
import respx
from click.testing import CliRunner

from symphony.bench.cli import _BenchSecrets
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
