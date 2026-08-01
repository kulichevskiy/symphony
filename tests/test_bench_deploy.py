from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]


def test_bench_compose_separates_control_executor_and_public_network() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.bench.coolify.yml").read_text())
    services = compose["services"]

    assert services["worker"]["build"]["target"] == "runtime"
    assert services["executor"]["build"]["target"] == "bench-executor"
    assert set(services["worker"]["networks"]) == {"control", "execution"}
    assert services["executor"]["networks"] == ["execution"]
    assert services["connections"]["networks"] == ["connections"]
    assert services["caddy"]["networks"] == ["coolify", "control", "connections"]
    assert "coolify" not in services["executor"]["networks"]
    assert services["executor"]["volumes"] == ["bench_runs:/data/bench"]
    assert all(".env" not in volume for volume in services["executor"]["volumes"])
    assert "bench_db:/data/db" in services["worker"]["volumes"]
    assert compose["networks"]["control"]["internal"] is True


def test_caddy_exposes_only_control_api_and_connections_ui() -> None:
    caddy = (ROOT / "Caddyfile.bench.coolify").read_text()

    assert "reverse_proxy worker:8080" in caddy
    assert "reverse_proxy connections:8787" in caddy
    assert "executor" not in caddy
