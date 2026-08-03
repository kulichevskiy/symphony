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
        "chown -R 1000:1000 /data/db /data/bench-a /data/bench-b",
    ]
    assert set(services["init"]["volumes"]) == {
        "bench_db:/data/db",
        "bench_runs_a:/data/bench-a",
        "bench_runs_b:/data/bench-b",
    }
    assert services["worker"]["depends_on"]["init"]["condition"] == (
        "service_completed_successfully"
    )
    assert services["worker"]["build"]["target"] == "runtime"
    assert set(services["worker"]["networks"]) == {"control", "execution"}
    for lane, volume, root in (
        ("bench-a", "bench_runs_a", "/data/bench-a"),
        ("bench-b", "bench_runs_b", "/data/bench-b"),
    ):
        assert services[lane]["depends_on"]["init"]["condition"] == (
            "service_completed_successfully"
        )
        assert services[lane]["build"]["target"] == "bench-executor"
        assert services[lane]["networks"] == ["execution"]
        assert "coolify" not in services[lane]["networks"]
        assert services[lane]["volumes"] == [f"{volume}:{root}"]
        assert all(".env" not in item for item in services[lane]["volumes"])
    assert services["connections"]["network_mode"] == "service:caddy"
    assert "networks" not in services["connections"]
    assert services["caddy"]["networks"] == ["coolify", "control"]
    assert services["caddy"]["labels"] == ["traefik.docker.network=coolify"]
    assert services["caddy"]["extra_hosts"] == ["connections:127.0.0.1"]
    assert services["caddy"]["environment"] == {"SERVICE_FQDN_CADDY": "${SERVICE_FQDN_CADDY}"}
    assert services["worker"]["environment"]["SYMPHONY_BENCH_EXECUTOR_A_URL"] == (
        "http://bench-a:8090"
    )
    assert services["worker"]["environment"]["SYMPHONY_BENCH_EXECUTOR_B_URL"] == (
        "http://bench-b:8090"
    )
    assert "bench_db:/data/db" in services["worker"]["volumes"]
    assert services["worker"]["stop_grace_period"] == "90s"
    assert (
        "/opt/symphony-bench/controls-current:/run/symphony-bench-controls:ro"
        in services["worker"]["volumes"]
    )
    assert all(
        "controls-current" not in item
        for lane in ("bench-a", "bench-b")
        for item in services[lane]["volumes"]
    )
    assert compose["networks"]["control"]["internal"] is True


def test_bench_executor_image_excludes_all_private_grader_controls() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    bench_stage = dockerfile.split("FROM runtime AS bench-executor", 1)[1].split(
        "FROM runtime AS production", 1
    )[0]

    assert "/app/src/symphony/bench/assets/hidden" in bench_stage
    assert "/app/src/symphony/bench/assets/feedback_inbox_reference" in bench_stage
    assert "/app/src/symphony/bench/assets/support_queue_reference" in bench_stage
    assert "/app/src/symphony/bench/assets/support_queue_mutations" in bench_stage
    dockerignore = (ROOT / ".dockerignore").read_text()
    assert "src/symphony/bench/assets/feedback_inbox_reference" in dockerignore
    assert "src/symphony/bench/assets/hidden/feedback_inbox" in dockerignore
    assert "src/symphony/bench/assets/support_queue_reference" in dockerignore
    assert "src/symphony/bench/assets/support_queue_mutations" in dockerignore
    assert "src/symphony/bench/assets/hidden/support_queue" in dockerignore
    assert "symphony-bench-toolchain.txt" in dockerfile
    assert "dpkg-query" in dockerfile


def test_caddy_exposes_only_control_api_and_connections_ui() -> None:
    caddy = (ROOT / "Caddyfile.bench.coolify").read_text()

    assert "reverse_proxy worker:8080" in caddy
    assert "handle /notifications*" in caddy
    assert "reverse_proxy connections:8787" in caddy
    assert "executor" not in caddy
    compose = yaml.safe_load((ROOT / "docker-compose.bench.coolify.yml").read_text())
    embedded = compose["services"]["caddy"]["volumes"][0]["content"]
    assert "handle /notifications*" in embedded


def test_deploy_uses_ssh_stdin_when_coolify_api_is_local_only() -> None:
    script = (ROOT / "scripts/deploy-bench-coolify.sh").read_text()

    assert "urllib.request" in script
    assert "json.load(sys.stdin)" in script
    assert "api_transport=ssh" in script
    assert "COOLIFY_BENCH_REPOSITORY:-kulichevskiy/symphony" in script
    assert 'create_repository="https://github.com/${update_repository}.git"' in script
    assert '--arg repository "$update_repository"' in script
    assert "git_repository: $repository, instant_deploy: false" in script
    assert "docker_compose_domains" in script
    assert 'secure_domain="https://${domain#*://}"' in script
    assert '{name: "caddy", domain: $domain}' in script
    assert " -L " not in script
    assert "del(.project_uuid, .server_uuid, .environment_name, .autogenerate_domain)" in script
    assert 'select(contains("caddy-"))' in script
    assert 'echo "http://$service_fqdn"' in script
    assert "uploaded private controls bundle" in script
    assert "/opt/symphony-bench/controls-current" in script
    assert "sudo -n tar" in script
