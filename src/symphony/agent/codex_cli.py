"""Helpers for building Codex CLI argv."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

SYMPHONY_PERMISSIONS_PROFILE = "symphony-git"
CODEX_DEFAULT_PERMISSIONS_CONFIG = (
    f'default_permissions="{SYMPHONY_PERMISSIONS_PROFILE}"'
)
CODEX_APPROVAL_POLICY_CONFIG = 'approval_policy="never"'

SYMPHONY_PERMISSIONS_PROFILE_TOML = f"""
[permissions.{SYMPHONY_PERMISSIONS_PROFILE}.filesystem]
":root" = "read"
"/tmp" = "write"

[permissions.{SYMPHONY_PERMISSIONS_PROFILE}.filesystem.":project_roots"]
"." = "write"
".git" = "write"
".agents" = "read"
".codex" = "read"

[permissions.{SYMPHONY_PERMISSIONS_PROFILE}.network]
enabled = false
""".strip()


class CodexPermissionsProfileError(RuntimeError):
    """Raised when the Codex permissions profile cannot be ensured."""


def codex_config_path() -> Path:
    """Return the Codex config path used for the permissions profile."""
    if codex_home := os.environ.get("CODEX_HOME"):
        return Path(codex_home).expanduser() / "config.toml"
    return Path.home() / ".codex" / "config.toml"


def _has_symphony_permissions_profile(config: dict[str, Any]) -> bool:
    permissions = config.get("permissions")
    if not isinstance(permissions, dict):
        return False
    profile = permissions.get(SYMPHONY_PERMISSIONS_PROFILE)
    return isinstance(profile, dict)


def ensure_symphony_permissions_profile(
    config_path: Path | None = None,
) -> tuple[Path, bool]:
    """Ensure Codex has the named profile that can write managed `.git` dirs.

    Returns `(path, created)`. Existing profiles are treated as operator-owned:
    if the profile is already present, this function does not rewrite it.
    """
    path = config_path or codex_config_path()
    existing = ""
    if path.exists():
        try:
            existing = path.read_text(encoding="utf-8")
            parsed = tomllib.loads(existing)
        except tomllib.TOMLDecodeError as exc:
            raise CodexPermissionsProfileError(
                f"Codex config {path} is invalid TOML; add the "
                f"{SYMPHONY_PERMISSIONS_PROFILE!r} permissions profile manually."
            ) from exc
        except OSError as exc:
            raise CodexPermissionsProfileError(
                f"Could not read Codex config {path}: {exc}"
            ) from exc
        permissions = parsed.get("permissions")
        if permissions is not None and not isinstance(permissions, dict):
            raise CodexPermissionsProfileError(
                f"Codex config {path} defines 'permissions' as a non-table value; "
                f"add the {SYMPHONY_PERMISSIONS_PROFILE!r} permissions profile manually."
            )
        if (
            isinstance(permissions, dict)
            and SYMPHONY_PERMISSIONS_PROFILE in permissions
            and not isinstance(permissions[SYMPHONY_PERMISSIONS_PROFILE], dict)
        ):
            raise CodexPermissionsProfileError(
                f"Codex config {path} defines {SYMPHONY_PERMISSIONS_PROFILE!r} "
                "as a non-table value; add the permissions profile manually."
            )
        if _has_symphony_permissions_profile(parsed):
            return path, False

    separator = "" if not existing else ("\n" if existing.endswith("\n") else "\n\n")
    updated = f"{existing}{separator}{SYMPHONY_PERMISSIONS_PROFILE_TOML}\n"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(updated, encoding="utf-8")
    except OSError as exc:
        raise CodexPermissionsProfileError(
            f"Codex permissions profile {SYMPHONY_PERMISSIONS_PROFILE!r} is missing "
            f"and {path} could not be updated: {exc}. Add this block manually:\n\n"
            f"{SYMPHONY_PERMISSIONS_PROFILE_TOML}"
        ) from exc
    return path, True


def build_codex_workspace_write_command(
    *, prompt: str, codex_model: str
) -> list[str]:
    """Build `codex exec` argv for agents that must modify and commit."""
    return [
        "codex",
        "exec",
        "--json",
        "--config",
        CODEX_DEFAULT_PERMISSIONS_CONFIG,
        "--config",
        CODEX_APPROVAL_POLICY_CONFIG,
        "--model",
        codex_model,
        prompt,
    ]


__all__ = [
    "CODEX_APPROVAL_POLICY_CONFIG",
    "CODEX_DEFAULT_PERMISSIONS_CONFIG",
    "SYMPHONY_PERMISSIONS_PROFILE",
    "SYMPHONY_PERMISSIONS_PROFILE_TOML",
    "CodexPermissionsProfileError",
    "build_codex_workspace_write_command",
    "codex_config_path",
    "ensure_symphony_permissions_profile",
]
