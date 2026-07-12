"""The one effective-config assembly (SYM-188).

Composes "YAML system knobs + DB bindings + DB global roles matrix" into a
single `Config`. Every topology consumer goes through here — the daemon, the
UI API view, and the non-daemon CLI paths (preflight, manual dispatch) — so
none of them silently operates over an empty or stale binding set.

`repos:`/`roles:` in the YAML are ignored (a one-line warning when the DB has
bindings). Two boot gates protect a zero-binding start:

  (a) zero DB bindings + unresolved work in the DB (active runs / tracked open
      PRs / parked operator waits) — independent of the migration marker, so a
      bad bulk delete or DB restore is caught too; and
  (b) zero DB bindings + the YAML still carries a `repos:` topology.

A true fresh install (no bindings, no unresolved work, no YAML topology) boots
fine, so the first binding can be created later through the UI.
"""

from __future__ import annotations

import logging
import os

import aiosqlite
from dotenv import dotenv_values
from pydantic import ValidationError

from . import db
from .config import Config, RepoBinding, RoleConfig

_log = logging.getLogger(__name__)

_IMPORT_HINT = "run the importer (`symphony config-import --config <yaml>`)"


class ConfigBootError(RuntimeError):
    """Raised when the effective config is unsafe to boot from."""


async def _has_unresolved_work(conn: aiosqlite.Connection) -> bool:
    """Work in the DB that resolves its binding by iterating the loaded set."""
    if await db.runs.list_live(conn):
        return True
    if await db.issue_prs.has_open(conn):
        return True
    if await db.operator_waits.list_all(conn):
        return True
    return False


def _resolve_bindings(cfg: Config) -> None:
    """Apply tracker-context defaults and resolve `env:` key names to values on
    each assembled binding — mirrors what `Config.load` does for YAML repos.

    Disabled bindings skip `resolve_env`: they're kept around only so
    restart/restore paths can resolve them, and must never block boot because
    an operator has since removed the `env:` secret they refer to.
    """
    env_source: dict[str, str] = {
        key: value for key, value in dotenv_values(".env").items() if value is not None
    }
    env_source.update(os.environ)
    for binding in cfg.repos:
        binding.apply_tracker_secret_defaults(jira_base_url=cfg.jira_base_url)
        if binding.enabled:
            binding.resolve_env(env_source)


async def assemble_effective_config(
    conn: aiosqlite.Connection, base: Config, *, boot_gates: bool = True
) -> Config:
    """Assemble the effective `Config` from `base` (YAML system knobs) + the DB.

    `base` is a `Config.load`-produced config; its `repos`/`roles` are replaced
    by the DB's bindings and global matrix.

    `boot_gates` (the daemon path) enforces the two zero-binding gates — refuse
    a start that would orphan live work or silently dispatch nothing. The
    non-daemon one-shot CLI paths (preflight, manual dispatch) pass
    `boot_gates=False`: they still prefer DB bindings when present, but degrade
    gracefully to the YAML topology on an empty DB so they keep working during
    the transition (before the operator has run the importer).
    """
    stored = await db.config_bindings.list_all(conn)
    globals_row = await db.config_globals.get(conn)

    if not stored:
        if not boot_gates:
            # One-shot CLI, empty DB: operate over the YAML topology as-is.
            return base
        if await _has_unresolved_work(conn):
            raise ConfigBootError(
                "the config DB has zero bindings but still holds unresolved work "
                "(active runs, tracked open PRs, or parked operator waits); "
                f"refusing to start and orphan it — {_IMPORT_HINT}"
            )
        if base.repos:
            raise ConfigBootError(
                "the config DB has zero bindings but the YAML still contains a "
                f"`repos:` section (now ignored); {_IMPORT_HINT} before starting"
            )
        # True fresh install: no bindings, no work, no YAML topology.
        return base.model_copy(update={"repos": [], "roles": {}})

    if base.repos:
        _log.warning(
            "YAML `repos:`/`roles:` are ignored; %d binding(s) load from the DB",
            len(stored),
        )

    # `list_all` already returns dispatch-evaluation order (priority, then the
    # stable natural-key tiebreak). Disabled bindings are kept in `cfg.repos`
    # — restart/restore paths (open PRs, operator waits, live runs) resolve
    # their binding by iterating it — but marked `enabled=False` so dispatch
    # and manual-dispatch skip them for new work.
    try:
        bindings = [
            RepoBinding.model_validate({**row.payload, "enabled": row.enabled}) for row in stored
        ]
        global_roles = (
            {name: RoleConfig.model_validate(cell) for name, cell in globals_row.roles.items()}
            if globals_row
            else {}
        )
        effective = base.model_copy(update={"repos": bindings, "roles": global_roles})
        _resolve_bindings(effective)
        # `model_copy` skips `Config`'s model_validators, so DB-sourced role
        # combos (family/effort mismatches, legacy-field conflicts) never ran
        # through the same check YAML `repos:`/`roles:` gets at load time.
        effective.validate_roles_matrix()
    except (ValidationError, ValueError) as e:
        raise ConfigBootError(f"invalid role configuration in the config DB: {e}") from e
    return effective
