"""Helpers for building Codex CLI argv."""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path

SYMPHONY_PERMISSIONS_PROFILE = "symphony-git"
CODEX_DEFAULT_PERMISSIONS_CONFIG = f'default_permissions="{SYMPHONY_PERMISSIONS_PROFILE}"'
CODEX_APPROVAL_POLICY_CONFIG = 'approval_policy="never"'

# Grant write to the workspace via the `:workspace_roots` token. Codex >= 0.136
# dropped the older `:project_roots` table (it logs "is not recognized by this
# version" and silently ignores it, leaving the checkout and `.git` read-only so
# the agent can edit nothing and every commit fails). `:workspace_roots` is the
# current token for the agent's working root (`:cwd`/`:workspace` do not grant
# the root). We scope it with per-subpath rules: `.` grants the project tree, and
# `.git` is granted explicitly because once subpaths are listed, `.git` reverts
# to read-only — verified under `codex exec`, where `.` alone leaves `.git`
# read-only and every commit fails with "Unable to create .git/index.lock:
# Operation not permitted". `.codex`/`.agents` are pinned read-only so an
# unattended agent can't rewrite the control dirs (its own config/instructions)
# that drive later runs in the same checkout.
#
# Network is ENABLED for the agent sandbox. With it disabled, codex's OS sandbox
# unshares a network namespace and brings up loopback (RTM_NEWADDR) — which
# fails inside our container ("bwrap: loopback: Failed RTM_NEWADDR: Operation
# not permitted"): a non-root process can't configure the fresh netns, and the
# capability can't be handed to bwrap (bounding-set caps aren't effective for a
# non-root exec, and bwrap rejects file caps on Debian's non-setuid build). The
# container is already the network/isolation boundary (127.0.0.1-only daemon,
# per-binding env allowlist), so the agent shares it instead of nesting another
# isolated netns.
SYMPHONY_PERMISSIONS_PROFILE_TOML = f"""
[permissions.{SYMPHONY_PERMISSIONS_PROFILE}.filesystem]
":root" = "read"
"/tmp" = "write"

[permissions.{SYMPHONY_PERMISSIONS_PROFILE}.filesystem.":workspace_roots"]
"." = "write"
".git" = "write"
".codex" = "read"
".agents" = "read"

[permissions.{SYMPHONY_PERMISSIONS_PROFILE}.network]
enabled = true
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
    return isinstance(filesystem, dict) and _LEGACY_PROJECT_ROOTS_TOKEN in filesystem


def _split_dotted_key(header: str) -> list[str] | None:
    """Split a TOML table header into its key parts, honoring quotes.

    Returns the unquoted components (e.g. `permissions."symphony-git".network`
    -> `["permissions", "symphony-git", "network"]`), or `None` if the header is
    malformed. Quoting matters: TOML treats `permissions.symphony-git` and
    `permissions."symphony-git"` as the same table path, so prefix matching on
    the raw text would miss a hand-written quoted profile and leave its tables in
    place (which then collide with the unquoted block we append).
    """
    parts: list[str] = []
    i, n = 0, len(header)
    while i < n:
        while i < n and header[i] in " \t":
            i += 1
        if i >= n:
            break
        if header[i] in "\"'":
            quote = header[i]
            i += 1
            start = i
            while i < n and header[i] != quote:
                i += 1
            if i >= n:
                return None  # unterminated quote
            parts.append(header[start:i])
            i += 1
        else:
            start = i
            while i < n and header[i] not in ".\"' \t":
                i += 1
            parts.append(header[start:i])
        while i < n and header[i] in " \t":
            i += 1
        if i < n:
            if header[i] == ".":
                i += 1
            else:
                return None  # unexpected character after a key part
    return parts


def _strip_symphony_profile_tables(config_text: str) -> str:
    """Drop every `[permissions.<profile>...]` table for the managed profile."""
    managed = ["permissions", SYMPHONY_PERMISSIONS_PROFILE]
    kept: list[str] = []
    skipping = False
    for raw_line in config_text.splitlines(keepends=True):
        stripped = raw_line.lstrip()
        if stripped.startswith("["):
            header = stripped[1:].split("]", 1)[0].strip()
            parts = _split_dotted_key(header)
            skipping = parts is not None and parts[:2] == managed
            if skipping:
                continue
        if skipping:
            continue
        kept.append(raw_line)
    return "".join(kept)


def _config_sets_default_permissions(config_text: str) -> bool:
    """True if `config_text` sets a top-level `default_permissions` key."""
    try:
        return "default_permissions" in tomllib.loads(config_text)
    except tomllib.TOMLDecodeError:
        return False


def _backfill_default_permissions(path: Path, existing: str) -> tuple[Path, bool]:
    """Add a top-level `default_permissions` to a config that already has a
    current profile but no default set.

    Codex >= 0.143 refuses to load any config that defines `[permissions]`
    tables without a top-level `default_permissions`. Older Symphony versions
    wrote the profile without it (the value was only passed per-invocation via
    `--config`), so an existing config can be structurally current yet fail to
    load. Prepend the key — it must live in the root scope, above any table
    header. Returns `(path, False)` untouched when a default is already set.
    """
    if _config_sets_default_permissions(existing):
        return path, False
    updated = f"{CODEX_DEFAULT_PERMISSIONS_CONFIG}\n{existing}"
    try:
        tomllib.loads(updated)
    except tomllib.TOMLDecodeError as exc:
        raise CodexPermissionsProfileError(
            f"Codex config {path} could not be safely updated; add "
            f"`{CODEX_DEFAULT_PERMISSIONS_CONFIG}` manually."
        ) from exc
    try:
        path.write_text(updated, encoding="utf-8")
    except OSError as exc:
        raise CodexPermissionsProfileError(
            f"Codex config {path} needs `{CODEX_DEFAULT_PERMISSIONS_CONFIG}` but "
            f"could not be updated: {exc}. Add it manually."
        ) from exc
    return path, True


def ensure_symphony_permissions_profile(
    config_path: Path | None = None,
) -> tuple[Path, bool]:
    """Ensure Codex has the named profile that can write managed `.git` dirs.

    Returns `(path, created)`. Existing profiles are treated as operator-owned
    and left untouched, with two exceptions: a profile still using the legacy
    `:project_roots` filesystem token is silently broken on Codex >= 0.136, so
    it is rewritten to the current `:workspace_roots`-based block; and a config
    that defines the profile but no top-level `default_permissions` (which Codex
    >= 0.143 requires whenever `[permissions]` tables exist) gets the key
    backfilled. Either case sets `created` to `True`.
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
                    # Profile is current. Codex >= 0.143 still refuses to load
                    # it without a top-level `default_permissions`; backfill it
                    # if unset, otherwise leave the config untouched.
                    return _backfill_default_permissions(path, existing)
                # Legacy profile written for an older Codex: strip its tables so
                # the current `:workspace_roots`-based block is re-appended below.
                stripped = _strip_symphony_profile_tables(existing)
                # `_strip_symphony_profile_tables` only removes
                # `[permissions.symphony-git...]` table sections. A profile
                # written as an inline table (`permissions = { "symphony-git" =
                # {...} }`) survives stripping, and re-appending the block would
                # duplicate the table path and corrupt the file. Don't write a
                # broken config — hand the operator the exact block to paste.
                leftover = tomllib.loads(stripped).get("permissions")
                if isinstance(leftover, dict) and SYMPHONY_PERMISSIONS_PROFILE in leftover:
                    raise CodexPermissionsProfileError(
                        f"Codex config {path} defines a stale "
                        f"{SYMPHONY_PERMISSIONS_PROFILE!r} profile as an inline "
                        "table that cannot be rewritten automatically. Replace "
                        f"it with this block:\n\n{SYMPHONY_PERMISSIONS_PROFILE_TOML}"
                    )
                existing = stripped
            if profile is None:
                existing, _ = _drop_empty_inline_permissions_assignment(existing)

    separator = "" if not existing else ("\n" if existing.endswith("\n") else "\n\n")
    updated = f"{existing}{separator}{SYMPHONY_PERMISSIONS_PROFILE_TOML}\n"
    # Codex >= 0.143 requires a top-level `default_permissions` once
    # `[permissions]` tables exist. Prepend it (root scope, above any table)
    # unless the operator already set one.
    if not _config_sets_default_permissions(existing):
        updated = f"{CODEX_DEFAULT_PERMISSIONS_CONFIG}\n{updated}"
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
    *, prompt: str, codex_model: str, effort: str | None = None
) -> list[str]:
    """Build `codex exec` argv for agents that must modify and commit.

    `effort` maps to `--config model_reasoning_effort="<v>"` (the same repeated
    `--config` pattern as the permission/approval knobs). Unset → no flag, so
    the Codex CLI default stands.
    """
    command = [
        "codex",
        "exec",
        "--json",
        "--config",
        CODEX_DEFAULT_PERMISSIONS_CONFIG,
        "--config",
        CODEX_APPROVAL_POLICY_CONFIG,
    ]
    if effort is not None:
        command += ["--config", f'model_reasoning_effort="{effort}"']
    command += ["--model", codex_model, prompt]
    return command


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
