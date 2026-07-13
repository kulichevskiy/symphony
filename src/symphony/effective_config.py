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


_ROLE_NAMES: tuple[RoleName, ...] = ("implement", "review_find", "review_verify", "fix", "accept")


def _synthesize_legacy_role_fields(cfg: Config) -> None:
    """Bridge until every per-stage consumer reads `resolved_role` (SYM-192).

    Several runtime paths still read the legacy top-level role fields directly
    (`binding.agent` for codex resume/activity support and usage-cost
    attribution, `binding.codex_model`, the reviewer/local-review model
    fields). DB payloads are legacy-free by construction, so a migrated codex
    binding would otherwise hit those paths with claude defaults — wrong final
    -message parsing, wrong cost attribution, even claude spawned for fix
    turns. Derive the legacy fields from the resolved matrix instead.

    Synthesis must not change any `resolved_role` output (legacy fields are
    the resolution *fallback*, so a per-role mixed matrix — e.g. a codex
    implement cell over an inherited claude fix — could be altered by a
    synthesized `agent`). Each binding's five roles are resolved before and
    after; on any difference the synthesis is reverted for that binding and
    the direct readers keep seeing defaults, exactly as they would for the
    same exotic matrix in YAML.

    Runs *after* `validate_roles_matrix()`: synthesized fields land in
    `model_fields_set` and would otherwise trip the legacy/matrix conflict
    guard, which exists to stop *operators* setting the same knob twice.
    """
    for binding in cfg.repos:
        before = {name: binding.resolved_role(name, cfg.roles) for name in _ROLE_NAMES}
        impl, fix, acc = before["implement"], before["fix"], before["accept"]
        rf, rv = before["review_find"], before["review_verify"]
        update: dict[str, str] = {}
        if impl.agent == fix.agent == acc.agent:
            update["agent"] = impl.agent
            if impl.agent == "codex" and impl.model and impl.model == fix.model == acc.model:
                update["codex_model"] = impl.model
        if rf.agent == rv.agent:
            update["reviewer_agent"] = rf.agent
            if rf.agent == "codex" and rf.model and rf.model == rv.model:
                update["reviewer_codex_model"] = rf.model
        if rf.agent == "claude" and rf.model:
            update["local_review_claude_model"] = rf.model
        if rv.agent == "claude" and rv.model:
            update["local_review_verifier_claude_model"] = rv.model
        original = {field: getattr(binding, field) for field in update}
        for field, value in update.items():
            setattr(binding, field, value)
        after = {name: binding.resolved_role(name, cfg.roles) for name in _ROLE_NAMES}
        if after != before:
            for field, value in original.items():
                setattr(binding, field, value)
            _log.warning(
                "binding %s/%s: per-role matrix too mixed to synthesize legacy "
                "role fields; direct legacy-field readers see defaults until "
                "SYM-192 moves them to resolved_role",
                binding.project_key,
                binding.github_repo,
            )


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
            return base.model_copy(update={"repos": [], "roles": {}})
        if not boot_gates:
            # One-shot CLI, empty DB: operate over the YAML topology as-is.
            return base
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
        return base.model_copy(update={"repos": [], "roles": {}})

    if yaml_has_repos_topology:
        _log.warning(
            "YAML `repos:`/`roles:` are ignored; %d binding(s) load from the DB",
            len(stored),
        )

    # `list_all` already returns dispatch-evaluation order (priority, then the
    # stable natural-key tiebreak). The row's `enabled` column is stamped onto
    # each binding, but this slice gives it no semantics — every row loads and
    # dispatches as enabled, and the importer refuses `enabled: false` input.
    # The lifecycle (dispatch skip, launch gate, drain guard) ships in SYM-193.
    try:
        bindings = [
            RepoBinding.model_validate({**row.payload, "enabled": row.enabled}) for row in stored
        ]
        global_roles = _GlobalRolesAdapter.validate_python(globals_row.roles) if globals_row else {}
        effective = base.model_copy(update={"repos": bindings, "roles": global_roles})
        _resolve_bindings(effective)
        # `model_copy` skips `Config`'s model_validators, so DB-sourced role
        # combos (family/effort mismatches, legacy-field conflicts) never ran
        # through the same check YAML `repos:`/`roles:` gets at load time.
        effective.validate_roles_matrix()
    except (ValidationError, ValueError) as e:
        raise ConfigBootError(f"invalid role configuration in the config DB: {e}") from e
    _synthesize_legacy_role_fields(effective)
    return effective
