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
from pydantic import TypeAdapter, ValidationError

from . import db
from .config import Config, RepoBinding, RoleConfig, RoleName

_log = logging.getLogger(__name__)

_IMPORT_HINT = "run the importer (`symphony config-import --config <yaml>`)"

# `TypeAdapter` gets us the same "unknown role name" rejection YAML gets for
# free from `dict[RoleName, RoleConfig]` field typing — `config_globals.roles`
# is raw DB JSON, so it never passes through that pydantic field parse.
_GlobalRolesAdapter = TypeAdapter(dict[RoleName, RoleConfig])


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
    each assembled binding — mirrors what `Config.load` does for YAML repos."""
    env_source: dict[str, str] = {
        key: value for key, value in dotenv_values(".env").items() if value is not None
    }
    env_source.update(os.environ)
    for binding in cfg.repos:
        binding.apply_tracker_secret_defaults(jira_base_url=cfg.jira_base_url)
        binding.resolve_env(env_source)


# Operational knobs an operator may override from the DB (Config v2 7/9):
# knob name -> (min, max) accepted by the API and the assembly overlay. All
# are hot-reloaded at the daemon's tick boundary (`_reload_bindings`
# reassembles the effective config every tick); none require a restart.
OPERATIONAL_KNOBS: dict[str, tuple[int, int]] = {
    "poll_interval_secs": (5, 3600),
    "global_max_concurrent": (1, 64),
    "stall_timeout_secs": (30, 24 * 3600),
    # 0 disables the wall-clock cap.
    "wall_clock_timeout_secs": (0, 24 * 3600),
    "command_timeout_secs": (30, 24 * 3600),
    "reconcile_interval_secs": (30, 24 * 3600),
    "review_iteration_cap": (1, 100),
    "local_review_iteration_cap": (1, 100),
    "activity_comment_interval_secs": (30, 24 * 3600),
}


def _apply_knobs(cfg: Config, knobs: dict[str, object]) -> Config:
    """Overlay DB-stored knob overrides onto `cfg`. Defensive against a
    hand-edited row: unknown keys and out-of-range/non-int values are skipped
    with a warning rather than failing the assembly (the API validates
    strictly on write; this guards restored/edited DBs)."""
    updates: dict[str, int] = {}
    for name, raw in knobs.items():
        spec = OPERATIONAL_KNOBS.get(name)
        if spec is None:
            _log.warning("ignoring unknown operational knob %r from the DB", name)
            continue
        if isinstance(raw, bool) or not isinstance(raw, int):
            _log.warning("ignoring non-integer operational knob %r=%r", name, raw)
            continue
        lo, hi = spec
        if not (lo <= raw <= hi):
            _log.warning("ignoring out-of-range operational knob %r=%r", name, raw)
            continue
        updates[name] = raw
    if not updates:
        return cfg
    return cfg.model_copy(update=updates)


async def assemble_effective_config(
    conn: aiosqlite.Connection,
    base: Config,
    *,
    boot_gates: bool = True,
    yaml_has_repos_topology: bool = False,
    is_reload: bool = False,
) -> Config:
    """Assemble the effective `Config` from `base` (YAML system knobs) + the DB.

    `base` is a `Config.load`-produced config; its `repos`/`roles` are replaced
    by the DB's bindings and global matrix.

    `yaml_has_repos_topology` reports whether the source YAML still declares a
    `repos:`/`roles:` section, per `Config.peek_repos_topology`. It must come
    from the caller rather than `base.repos`: once the DB owns bindings,
    callers load `base` with `resolve_repos=False`, which strips `repos:`/
    `roles:` before validation — so `base.repos` is always empty at that point
    and can never reveal a leftover YAML topology.

    `boot_gates` (the daemon path) enforces the two zero-binding gates — refuse
    a start that would orphan live work or silently dispatch nothing. The
    non-daemon one-shot CLI paths (preflight, manual dispatch) pass
    `boot_gates=False`: they still prefer DB bindings when present, but degrade
    gracefully to the YAML topology on an empty DB so they keep working during
    the transition (before the operator has run the importer).

    `is_reload` (the daemon's mid-run tick reload, SYM-189) also implies
    `boot_gates=False` semantics but must never degrade to the YAML topology
    on an empty DB: the DB already owns topology past boot, so "zero bindings"
    here means every binding was deleted, not "importer never ran". Returning
    `base` unchanged in that case would keep serving a deleted binding's
    `repos` forever, so this returns an empty topology instead.
    """
    try:
        stored = await db.config_bindings.list_all(conn)
    except ValueError as e:
        # `list_all` decodes each row's payload JSON eagerly; a hand-edited or
        # restored row with malformed JSON must fail boot with the same clean
        # error every other config problem gets, not a raw traceback.
        raise ConfigBootError(f"malformed config binding payload in the DB: {e}") from e
    try:
        globals_row = await db.config_globals.get(conn)
    except ValueError as e:
        # Same concern as above: `get` decodes `roles` JSON eagerly, and a
        # restored or hand-edited row with malformed JSON must not raise
        # `JSONDecodeError` (a `ValueError` subclass) past this assembly.
        raise ConfigBootError(f"malformed config globals payload in the DB: {e}") from e

    if not stored:
        if is_reload:
            # Mid-run reload, empty DB: every binding was deleted since boot —
            # collapse to an empty topology rather than falling through to the
            # one-shot-CLI "degrade to YAML" behavior below, which would leave
            # `base.repos` (the *previous* effective config's bindings) intact.
            return _apply_knobs(
                base.model_copy(update={"repos": [], "roles": {}}),
                globals_row.knobs if globals_row else {},
            )
        if not boot_gates:
            # One-shot CLI, empty DB: operate over the YAML topology as-is
            # (knob overrides still apply — they're DB state, not topology).
            return _apply_knobs(base, globals_row.knobs if globals_row else {})
        if await _has_unresolved_work(conn):
            raise ConfigBootError(
                "the config DB has zero bindings but still holds unresolved work "
                "(active runs, tracked open PRs, or parked operator waits); "
                f"refusing to start and orphan it — {_IMPORT_HINT}"
            )
        # A migrated DB already owns topology — a lingering YAML `repos:` here
        # just means the operator hasn't cleaned up the file, not that the
        # importer was never run (e.g. every binding was later deleted via the
        # UI). Only gate on it pre-migration, where it signals a forgotten
        # importer run.
        if yaml_has_repos_topology and not (globals_row and globals_row.migrated_at):
            raise ConfigBootError(
                "the config DB has zero bindings but the YAML still contains a "
                f"`repos:` section (now ignored); {_IMPORT_HINT} before starting"
            )
        # True fresh install: no bindings, no work, no YAML topology.
        return _apply_knobs(
            base.model_copy(update={"repos": [], "roles": {}}),
            globals_row.knobs if globals_row else {},
        )

    if yaml_has_repos_topology:
        _log.warning(
            "YAML `repos:`/`roles:` are ignored; %d binding(s) load from the DB",
            len(stored),
        )

    # `list_all` already returns dispatch-evaluation order (priority, then the
    # stable natural-key tiebreak). The row's `enabled` column is stamped onto
    # each binding; assembly loads every row (enabled + disabled) so disabled
    # bindings stay visible to the follow-up pollers, and the orchestrator's
    # dispatch scan + launch gate enforce the disable semantics (SYM-193).
    try:
        bindings = [
            RepoBinding.model_validate({**row.payload, "enabled": row.enabled}) for row in stored
        ]
        global_roles = _GlobalRolesAdapter.validate_python(globals_row.roles) if globals_row else {}
        effective = _apply_knobs(
            base.model_copy(update={"repos": bindings, "roles": global_roles}),
            globals_row.knobs if globals_row else {},
        )
        _resolve_bindings(effective)
        # `model_copy` skips `Config`'s model_validators, so DB-sourced role
        # combos (family/effort mismatches, legacy-field conflicts) never ran
        # through the same check YAML `repos:`/`roles:` gets at load time.
        effective.validate_roles_matrix()
    except (ValidationError, ValueError) as e:
        raise ConfigBootError(f"invalid role configuration in the config DB: {e}") from e
    return effective
