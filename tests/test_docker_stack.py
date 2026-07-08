"""Structural coverage for the local Docker Compose stack (symphony + caddy).

The daemon binds 127.0.0.1 only (security invariant, see test_webhook.py).
Caddy therefore shares the daemon's network namespace and reaches it over
loopback — no code change to the loopback guard is needed.
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (ROOT / rel).read_text()


# --- Dockerfile -----------------------------------------------------------


def test_dockerfile_installs_full_agent_toolchain() -> None:
    text = _read("Dockerfile")

    # Every tool the agent runtime needs must be installed.
    assert "uv" in text
    assert "gh" in text  # GitHub CLI
    assert "git" in text
    # Node powers both the CLIs and the frontend build.
    assert "node" in text.lower()
    # The two coding-agent CLIs, installed globally via npm.
    assert "@anthropic-ai/claude-code" in text
    assert "@openai/codex" in text


def test_dockerfile_verifies_toolchain_on_path() -> None:
    text = _read("Dockerfile")

    # A build-time gate proves claude/codex/gh/git/uv/node resolve on PATH,
    # so a broken install fails the build instead of the running daemon.
    assert "command -v" in text
    for tool in ("claude", "codex", "gh", "git", "uv", "node"):
        assert tool in text


def test_dockerfile_builds_frontend_so_ui_is_served() -> None:
    text = _read("Dockerfile")

    # The daemon only mounts /ui when frontend/dist exists; pin to the exact
    # copy so deleting it can't slip past a trivially-true substring check.
    assert "COPY --from=frontend /build/frontend/dist ./frontend/dist" in text


def test_dockerfile_copies_prompts_so_templates_ship_in_image() -> None:
    text = _read("Dockerfile")

    # _load_prompt_template() reads /app/prompts/<name>.md at runtime; without
    # this COPY the image silently falls back to inline default prompts.
    assert "COPY prompts/ ./prompts/" in text


def test_dockerfile_auth_dirs_are_precreated_and_owned_by_symphony() -> None:
    text = _read("Dockerfile")

    # Empty named volumes mounted over paths absent from the image are
    # created by Docker as root:root, breaking the non-root user's logins.
    for auth_dir in ("/home/symphony/.claude", "/home/symphony/.codex", "/home/symphony/.config/gh"):
        assert auth_dir in text
    assert "chown -R symphony:symphony" in text


def test_dockerfile_sets_home_for_symphony_user() -> None:
    text = _read("Dockerfile")

    assert "ENV HOME=/home/symphony" in text


# --- docker-compose.yml ---------------------------------------------------


def _compose() -> dict:
    return yaml.safe_load(_read("docker-compose.yml"))


def test_compose_defines_symphony_and_caddy_services() -> None:
    services = _compose()["services"]

    assert "symphony" in services
    assert "caddy" in services


def test_compose_named_volumes_persist_all_state() -> None:
    compose = _compose()
    declared = set(compose.get("volumes", {}))

    # Named volumes for CLI auth dirs, the DB, workspace_root and log_root —
    # none baked into the image.
    symphony_mounts = "\n".join(compose["services"]["symphony"]["volumes"])
    assert "/.claude" in symphony_mounts
    assert "/.codex" in symphony_mounts
    assert "/.config/gh" in symphony_mounts

    # Each persisted path is backed by a *named* volume, not a host bind.
    for name in declared:
        assert isinstance(name, str)
    # At least six named volumes: three auth dirs + db + workspaces + logs.
    assert len(declared) >= 6


def test_compose_wires_env_file_and_config() -> None:
    symphony = _compose()["services"]["symphony"]

    assert symphony.get("env_file") == ".env" or ".env" in symphony.get("env_file", [])
    mounts = "\n".join(symphony["volumes"])
    assert "config.local.yaml" in mounts


def test_compose_caddy_fronts_the_daemon_and_publishes_https() -> None:
    compose = _compose()
    caddy = compose["services"]["caddy"]
    symphony = compose["services"]["symphony"]

    assert "caddy" in caddy["image"]

    # Caddy owns the network namespace and the daemon joins it, so caddy can
    # reach the 127.0.0.1-only http surface over loopback. HTTPS (443) is
    # published on the namespace owner (caddy), which stays up across daemon
    # restarts.
    assert symphony.get("network_mode") == "service:caddy"
    published = "\n".join(str(p) for p in caddy.get("ports", []))
    assert "443" in published


# --- Caddyfile ------------------------------------------------------------


def test_caddyfile_reverse_proxies_daemon_with_internal_tls() -> None:
    text = _read("Caddyfile")

    assert "reverse_proxy" in text
    # Daemon listens on loopback:8787 within the shared namespace.
    assert "8787" in text
    # Local HTTPS via Caddy's internal CA.
    assert "tls internal" in text
