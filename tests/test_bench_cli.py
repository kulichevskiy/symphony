import httpx
import respx
from click.testing import CliRunner

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
