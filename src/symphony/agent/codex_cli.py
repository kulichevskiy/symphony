"""Helpers for building Codex CLI argv."""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path

SYMPHONY_PERMISSIONS_PROFILE = "symphony-git"
CODEX_DEFAULT_PERMISSIONS_CONFIG = (
    f'default_permissions="{SYMPHONY_PERMISSIONS_PROFILE}"'
)
CODEX_APPROVAL_POLICY_CONFIG = 'approval_policy="never"'

# Grant write to the workspace via the `:workspace_roots` token. Codex >= 0.136
# dropped the older `:project_roots` table (it logs "is not recognized by this
# version" and silently ignores it, leaving the checkout and `.git` read-only so
# the agent can edit nothing and every commit fails). `:workspace_roots` is the
# current token for the agent's working root: under `codex exec` it grants write
# to both the project tree and its `.git`, which is what agents need to commit.
# (`:cwd`/`:workspace` do not grant the root; `:workspace_roots` is the one that
# resolves to the materialized workspace.) The flat filesystem table only
# accepts absolute / `~` / `:`-prefixed keys on 0.136, so per-subpath read-only
# pins (e.g. `.agents`/`.codex`) can no longer be expressed here; any such edits
# surface in the run's diff/review.
SYMPHONY_PERMISSIONS_PROFILE_TOML = f"""
[permissions.{SYMPHONY_PERMISSIONS_PROFILE}.filesystem]
":root" = "read"
"/tmp" = "write"
":workspace_roots" = "write"

[permissions.{SYMPHONY_PERMISSIONS_PROFILE}.network]
enabled = false
""".strip()

# Legacy filesystem token that Codex < 0.136 used to scope writes to the project
# roots. Profiles that still carry it are silently broken on current Codex and
# must be rewritten (see `ensure_symphony_permissions_profile`).
_LEGACY_PROJECT_ROOTS_TOKEN = ":project_roots"
_EMPTY_INLINE_PERMISSIONS_RE = re.compile(
    r"""^\s*(?:"permissions"|'permissions'|permissions)\s*=\s*\{\s*\}\s*(?:#.*)?$"""
)


class CodexPermissionsProfileError(RuntimeError):
    """Raised when the Codex permissions profile cannot be ensured."""


def codex_config_path() -> Path:
    """Return the Codex config path used for the permissions profile."""
    if codex_home := os.environ.get("CODEX_HOME"):
        return Path(codex_home).expanduser() / "config.toml"
    return Path.home() / ".codex" / "config.toml"


def _drop_empty_inline_permissions_assignment(config_text: str) -> tuple[str, bool]:
    """Remove a root `permissions = {}` assignment before appending tables."""
    lines: list[str] = []
    removed = False
    in_root = True
    for raw_line in config_text.splitlines(keepends=True):
        code = raw_line.split("#", 1)[0].strip()
        if in_root and _EMPTY_INLINE_PERMISSIONS_RE.match(raw_line.rstrip("\r\n")):
            removed = True
            continue
        if code.startswith("["):
            in_root = False
        lines.append(raw_line)
    return "".join(lines), removed


def _profile_uses_legacy_project_roots(profile: dict[str, object]) -> bool:
    """True if the profile relies on the legacy `:project_roots` filesystem token.

    Such a profile was written for Codex < 0.136 and is silently broken on
    current Codex (the workspace and `.git` stay read-only), so it must be
    rewritten rather than preserved.
    """
    filesystem = profile.get("filesystem")
    return (
        isinstance(filesystem, dict)
        and _LEGACY_PROJECT_ROOTS_TOKEN in filesystem
    )


def _strip_symphony_profile_tables(config_text: str) -> str:
    """Drop every `[permissions.<profile>...]` table for the managed profile."""
    prefix = f"permissions.{SYMPHONY_PERMISSIONS_PROFILE}"
    kept: list[str] = []
    skipping = False
    for raw_line in config_text.splitlines(keepends=True):
        stripped = raw_line.lstrip()
        if stripped.startswith("["):
            header = stripped[1:].split("]", 1)[0].strip()
            skipping = header == prefix or header.startswith(f"{prefix}.")
            if skipping:
                continue
        if skipping:
            continue
        kept.append(raw_line)
    return "".join(kept)


def ensure_symphony_permissions_profile(
    config_path: Path | None = None,
) -> tuple[Path, bool]:
    """Ensure Codex has the named profile that can write managed `.git` dirs.

    Returns `(path, created)`. Existing profiles are treated as operator-owned
    and left untouched, with one exception: a profile still using the legacy
    `:project_roots` filesystem token is silently broken on Codex >= 0.136, so
    it is rewritten to the current `:workspace_roots`-based block (and `created`
    is `True`).
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
        if isinstance(permissions, dict):
            profile = permissions.get(SYMPHONY_PERMISSIONS_PROFILE)
            if profile is None and permissions:
                raise CodexPermissionsProfileError(
                    f"Codex config {path} already defines other permissions; "
                    f"add the {SYMPHONY_PERMISSIONS_PROFILE!r} permissions profile manually."
                )
            if profile is not None and not isinstance(profile, dict):
                raise CodexPermissionsProfileError(
                    f"Codex config {path} defines {SYMPHONY_PERMISSIONS_PROFILE!r} "
                    "as a non-table value; add the permissions profile manually."
            )
            if isinstance(profile, dict):
                if not _profile_uses_legacy_project_roots(profile):
                    return path, False
                # Legacy profile written for an older Codex: strip its tables so
                # the current `:workspace_roots`-based block is re-appended below.
                existing = _strip_symphony_profile_tables(existing)
            if profile is None:
                existing, _ = _drop_empty_inline_permissions_assignment(existing)

    separator = "" if not existing else ("\n" if existing.endswith("\n") else "\n\n")
    updated = f"{existing}{separator}{SYMPHONY_PERMISSIONS_PROFILE_TOML}\n"
    try:
        tomllib.loads(updated)
    except tomllib.TOMLDecodeError as exc:
        raise CodexPermissionsProfileError(
            f"Codex config {path} could not be safely updated; add the "
            f"{SYMPHONY_PERMISSIONS_PROFILE!r} permissions profile manually."
        ) from exc
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
