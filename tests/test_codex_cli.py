"""Codex CLI argv and permissions-profile tests."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from symphony.agent.codex_cli import (
    CODEX_APPROVAL_POLICY_CONFIG,
    CODEX_DEFAULT_PERMISSIONS_CONFIG,
    SYMPHONY_PERMISSIONS_PROFILE,
    SYMPHONY_PERMISSIONS_PROFILE_TOML,
    CodexPermissionsProfileError,
    build_codex_workspace_write_command,
    ensure_symphony_permissions_profile,
)


def test_build_codex_workspace_write_command_uses_named_permissions_profile() -> None:
    argv = build_codex_workspace_write_command(
        prompt="fix this",
        codex_model="gpt-5.1-codex",
    )

    assert argv[:3] == ["codex", "exec", "--json"]
    assert "--sandbox" not in argv
    assert "workspace-write" not in argv
    configs = [argv[i + 1] for i, arg in enumerate(argv) if arg == "--config"]
    assert configs == [
        CODEX_DEFAULT_PERMISSIONS_CONFIG,
        CODEX_APPROVAL_POLICY_CONFIG,
    ]
    assert argv[argv.index("--model") + 1] == "gpt-5.1-codex"
    assert argv[-1] == "fix this"


def test_ensure_symphony_permissions_profile_creates_missing_config(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / ".codex" / "config.toml"

    path, created = ensure_symphony_permissions_profile(config_path)

    assert path == config_path
    assert created is True
    parsed = tomllib.loads(config_path.read_text(encoding="utf-8"))
    profile = parsed["permissions"][SYMPHONY_PERMISSIONS_PROFILE]
    assert profile["filesystem"][":project_roots"][".git"] == "write"
    assert profile["filesystem"][":project_roots"]["."] == "write"
    assert profile["filesystem"][":project_roots"][".agents"] == "read"
    assert profile["filesystem"][":project_roots"][".codex"] == "read"
    assert profile["network"]["enabled"] is False


def test_ensure_symphony_permissions_profile_preserves_existing_profile(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    existing = f"""
[permissions.{SYMPHONY_PERMISSIONS_PROFILE}.network]
enabled = true
""".lstrip()
    config_path.write_text(existing, encoding="utf-8")

    path, created = ensure_symphony_permissions_profile(config_path)

    assert path == config_path
    assert created is False
    assert config_path.read_text(encoding="utf-8") == existing


def test_ensure_symphony_permissions_profile_reports_invalid_toml(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text("[permissions.", encoding="utf-8")

    with pytest.raises(CodexPermissionsProfileError, match="invalid TOML"):
        ensure_symphony_permissions_profile(config_path)


def test_ensure_symphony_permissions_profile_reports_scalar_permissions(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text('permissions = "custom"\n', encoding="utf-8")

    with pytest.raises(CodexPermissionsProfileError, match="non-table"):
        ensure_symphony_permissions_profile(config_path)


def test_ensure_symphony_permissions_profile_reports_inline_permissions_table(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    existing = 'permissions = { other = { network = { enabled = false } } }\n'
    config_path.write_text(existing, encoding="utf-8")

    with pytest.raises(CodexPermissionsProfileError, match="other permissions"):
        ensure_symphony_permissions_profile(config_path)
    assert config_path.read_text(encoding="utf-8") == existing


def test_ensure_symphony_permissions_profile_creates_empty_inline_permissions_table(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    existing = "permissions = {}\n"
    config_path.write_text(existing, encoding="utf-8")

    path, created = ensure_symphony_permissions_profile(config_path)

    assert path == config_path
    assert created is True
    updated = config_path.read_text(encoding="utf-8")
    assert "permissions = {}" not in updated
    parsed = tomllib.loads(updated)
    assert SYMPHONY_PERMISSIONS_PROFILE in parsed["permissions"]


def test_ensure_symphony_permissions_profile_reports_quoted_inline_permissions_key(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    existing = '"permissions" = { other = { network = { enabled = false } } }\n'
    config_path.write_text(existing, encoding="utf-8")

    with pytest.raises(CodexPermissionsProfileError, match="other permissions"):
        ensure_symphony_permissions_profile(config_path)
    assert config_path.read_text(encoding="utf-8") == existing


def test_ensure_symphony_permissions_profile_creates_quoted_empty_inline_permissions_key(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    existing = '"permissions" = {} # no profiles yet\n'
    config_path.write_text(existing, encoding="utf-8")

    path, created = ensure_symphony_permissions_profile(config_path)

    assert path == config_path
    assert created is True
    updated = config_path.read_text(encoding="utf-8")
    assert '"permissions" = {}' not in updated
    parsed = tomllib.loads(updated)
    assert SYMPHONY_PERMISSIONS_PROFILE in parsed["permissions"]


def test_ensure_symphony_permissions_profile_reports_other_permissions_table(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    existing = "[permissions.other.network]\nenabled = false\n"
    config_path.write_text(existing, encoding="utf-8")

    with pytest.raises(CodexPermissionsProfileError, match="other permissions"):
        ensure_symphony_permissions_profile(config_path)
    assert config_path.read_text(encoding="utf-8") == existing


def test_ensure_symphony_permissions_profile_creates_empty_permissions_table(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    existing = "[permissions]\n"
    config_path.write_text(existing, encoding="utf-8")

    path, created = ensure_symphony_permissions_profile(config_path)

    assert path == config_path
    assert created is True
    parsed = tomllib.loads(config_path.read_text(encoding="utf-8"))
    assert SYMPHONY_PERMISSIONS_PROFILE in parsed["permissions"]


def test_ensure_symphony_permissions_profile_reports_scalar_profile(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        f'[permissions]\n{SYMPHONY_PERMISSIONS_PROFILE} = "custom"\n',
        encoding="utf-8",
    )

    with pytest.raises(CodexPermissionsProfileError, match="non-table"):
        ensure_symphony_permissions_profile(config_path)


def test_profile_toml_constant_is_valid() -> None:
    parsed = tomllib.loads(SYMPHONY_PERMISSIONS_PROFILE_TOML)
    assert SYMPHONY_PERMISSIONS_PROFILE in parsed["permissions"]
