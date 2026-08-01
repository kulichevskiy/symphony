from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]


def test_bench_compose_separates_control_executor_and_public_network() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.bench.coolify.yml").read_text())
    services = compose["services"]

    assert services["init"]["image"] == "alpine:3.22"
    assert services["init"]["command"] == [
        "sh",
        "-c",
        "chown -R 1000:1000 /data/db /data/bench",
    ]
    assert set(services["init"]["volumes"]) == {
        "bench_db:/data/db",
        "bench_runs:/data/bench",
    }
    assert services["worker"]["depends_on"]["init"]["condition"] == (
        "service_completed_successfully"
    )
    assert services["executor"]["depends_on"]["init"]["condition"] == (
        "service_completed_successfully"
    )
    assert services["worker"]["build"]["target"] == "runtime"
    assert services["executor"]["build"]["target"] == "bench-executor"
    assert set(services["worker"]["networks"]) == {"control", "execution"}
    assert services["executor"]["networks"] == ["execution"]
    assert services["connections"]["network_mode"] == "service:caddy"
    assert "networks" not in services["connections"]
    assert services["caddy"]["networks"] == ["coolify", "control"]
    assert services["caddy"]["labels"] == ["traefik.docker.network=coolify"]
    assert services["caddy"]["extra_hosts"] == ["connections:127.0.0.1"]
    assert services["caddy"]["environment"] == {"SERVICE_FQDN_CADDY": "${SERVICE_FQDN_CADDY}"}
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


def test_deploy_uses_ssh_stdin_when_coolify_api_is_local_only() -> None:
    script = (ROOT / "scripts/deploy-bench-coolify.sh").read_text()

    assert "urllib.request" in script
    assert "json.load(sys.stdin)" in script
    assert "api_transport=ssh" in script
    assert "COOLIFY_BENCH_REPOSITORY:-kulichevskiy/symphony" in script
    assert " -L " not in script
    assert "del(.project_uuid, .server_uuid, .environment_name, .autogenerate_domain)" in script
    assert 'select(contains("caddy-"))' in script
    assert 'echo "http://$service_fqdn"' in script
