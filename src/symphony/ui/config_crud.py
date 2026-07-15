"""Binding CRUD for the config page (SYM-190).

Full create/read/update/delete of repo bindings against the config DB, live —
picked up by the daemon at the next tick (SYM-189). Companion to the read-only
`config_view` projection; this is the mutating surface.

Every write:
  * validates the payload through `RepoBinding.model_validate` (field errors
    carry `loc` paths the form maps to inputs), re-derives `tracker_site` from
    the global `jira_base_url` so a Jira binding without its own `base_url`
    keys on the site it actually resolves to at runtime (not the "default"
    placeholder), then assembles the full effective config and re-runs the
    roles-matrix validators (family/effort cross-checks) exactly as boot does;
  * rejects legacy top-level role fields and env `key` names unknown to the
    server env (fail closed, listing the available names);
  * accepts the `enabled` toggle (SYM-193): a disabled binding stops new
    dispatch (scan skip + launch gate) while staying loaded so in-flight work
    drains. DELETE and edits to the natural key or a branch-affecting field
    (branch_prefix, base_branch) are drain-guarded — allowed only when the
    binding has no running runs, tracked open PRs, parked operator waits, or
    in-memory scheduled slots; otherwise a 409 with the blocker list;
  * rejects `webhook_enabled: true` with no resolvable secret (per-binding or
    the global `GITHUB_WEBHOOK_SECRET`) — the daemon's hot-reload path
    (`cli._live_github_webhook_settings`) swallows that misconfiguration by
    disabling *every* repo's webhook verification, not just this one;
  * rejects a binding whose provider has no resolvable tracker credentials
    (`LINEAR_API_KEY`, or `JIRA_BASE_URL`/`JIRA_EMAIL`/`JIRA_API_TOKEN`) — a
    fresh DB-owned install boots with zero bindings, skipping `cli._run`'s
    boot-time credential check, so this write path is the only gate the first
    binding of a kind ever passes through; without it the binding saves
    cleanly and then fails every hot-reload tick
    (`Orchestrator._reload_bindings` swallows the `for_binding` `ValueError`
    and just logs it) with nothing surfacing to the operator;
  * rejects an exact-duplicate selector (same tracker scope + normalized label
    + ready state) among *enabled* bindings — disabled bindings are exempt so a
    replacement can be staged;
  * runs under optimistic locking (the row's `version`; a stale write → 409);
  * stamps `updated_at`/`updated_by` (auth email, or the literal `local`
    without Auth0) and logs a field-level diff (secret-bearing fields redacted
    to set/cleared/changed flags, never values).

The global roles matrix is edited through the same router (SYM-191):
`GET/PUT /api/config/roles` carries its own optimistic-lock `version`, and every
save (binding *and* matrix) re-runs the preflight per-model claude capability
check so an unsupported `(model, effort)` pair fails in the form, not at
dispatch. `GET /api/config/options` serves claude efforts *per model* from the
same source.

Secrets never appear in a response: `webhook_secret` is dropped from the served
payload in favour of a `webhook_secret_set` flag. `mcp_servers` entries have no
`resolve_env`-style name indirection (unlike the top-level `env` field), so an
operator embeds literal credentials straight into a server's `env`/`headers` —
those are redacted to per-key `true` flags the same way, and restored from the
stored row on write when the operator round-trips them untouched.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import warnings
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, get_args

import aiosqlite
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, TypeAdapter, ValidationError

from ..agent.claude_models import _resolve_alias_model_id, fetch_claude_effort_capabilities
from ..agent.codex_models import SUPPORTED_CODEX_EFFORTS, SUPPORTED_CODEX_MODELS
from ..config import (
    _LEGACY_ROLE_FIELDS,
    CLAUDE_MODEL_ALIASES,
    SUPPORTED_CLAUDE_EFFORTS,
    Config,
    RepoBinding,
    RoleConfig,
    RoleName,
    _synthetic_matrix_validation_binding,
    binding_natural_key,
)
from ..db import config_bindings, config_globals, issue_prs, operator_waits, runs

_ROLES_ADAPTER: TypeAdapter[dict[RoleName, RoleConfig]] = TypeAdapter(dict[RoleName, RoleConfig])

_log = logging.getLogger(__name__)

# Fields whose *values* must never leave the process or reach the daemon log.
_SECRET_FIELDS = frozenset({"webhook_secret"})

# Sub-fields of an `mcp_servers` entry that may carry literal credential
# material: a stdio server's `env`, or an http/sse server's auth `headers`.
_MCP_SECRET_SUBFIELDS = frozenset({"env", "headers"})

# Control keys owned by dedicated columns, never part of the sparse payload.
_CONTROL_KEYS = frozenset({"enabled", "priority", "version", "id"})

# Legacy alias spellings `RepoBinding` accepts via `AliasChoices` in addition to
# their canonical field names (`project_key`, `states`) — not in `model_fields`
# but not "unknown" either.
_FIELD_ALIASES = frozenset({"linear_team_key", "linear_states"})


class BindingWrite(BaseModel):
    """Create/update request. `payload` is the sparse operator-set
    `RepoBinding` field dict; `version` is required for updates (optimistic
    lock) and ignored for creates."""

    payload: dict[str, Any]
    enabled: bool = True
    priority: int = 0
    version: int | None = None


class RolesWrite(BaseModel):
    """Global roles-matrix write. `roles` is the `{role: {agent, model,
    effort}}` matrix; `version` is the optimistic-lock version the client
    loaded (0 on a fresh, never-migrated DB)."""

    roles: dict[str, Any]
    version: int


def _dump_roles(roles: dict[RoleName, RoleConfig]) -> dict[str, dict[str, Any]]:
    """Sparse dump of a validated roles matrix — drop unset (`None`) cell
    fields and any role whose cell is entirely empty, so an "inherit" choice is
    stored as absence, not as an explicit null the UI would misread as a set
    value."""
    out: dict[str, dict[str, Any]] = {}
    for name, role in roles.items():
        cell = role.model_dump(exclude_none=True)
        if cell:
            out[name] = cell
    return out


def _now_iso(clock: Callable[[], datetime] | None) -> str:
    now = clock() if clock is not None else datetime.now(UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


def _sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Strip control keys before persisting; legacy role fields are rejected by
    the DAO, but reject them here too so the operator gets a `loc` path."""
    return {k: v for k, v in payload.items() if k not in _CONTROL_KEYS}


def _redact_mcp_servers(mcp_servers: dict[str, Any]) -> dict[str, Any]:
    """Replace each server's secret-bearing sub-field values with `True` (key
    names only, never values) so a GET response — or the daemon log — never
    carries a literal `mcp_servers` credential."""
    redacted: dict[str, Any] = {}
    for name, entry in mcp_servers.items():
        if not isinstance(entry, dict):
            redacted[name] = entry
            continue
        out = dict(entry)
        for sub in _MCP_SECRET_SUBFIELDS:
            sub_value = out.get(sub)
            if isinstance(sub_value, dict):
                out[sub] = {k: True for k in sub_value}
        redacted[name] = out
    return redacted


def _restore_mcp_secrets(old_servers: Any, new_servers: dict[str, Any]) -> dict[str, Any]:
    """Undo `_redact_mcp_servers` on write: a sub-field value of exactly
    `True` means the operator round-tripped the redacted GET payload
    untouched for that key — restore the real value from the stored row. Any
    other value (including an explicit empty string) is a real edit and
    passes through unchanged."""
    old_servers = old_servers if isinstance(old_servers, dict) else {}
    restored: dict[str, Any] = {}
    for name, entry in new_servers.items():
        if not isinstance(entry, dict):
            restored[name] = entry
            continue
        old_entry = old_servers.get(name)
        old_entry = old_entry if isinstance(old_entry, dict) else {}
        out = dict(entry)
        for sub in _MCP_SECRET_SUBFIELDS:
            sub_value = out.get(sub)
            if not isinstance(sub_value, dict):
                continue
            old_sub = old_entry.get(sub)
            old_sub = old_sub if isinstance(old_sub, dict) else {}
            out[sub] = {
                k: (old_sub[k] if v is True and k in old_sub else v) for k, v in sub_value.items()
            }
        restored[name] = out
    return restored


def _serialize(row: config_bindings.StoredBinding) -> dict[str, Any]:
    """One binding for the API — secret values redacted."""
    payload = dict(row.payload)
    webhook_secret_set = bool(payload.pop("webhook_secret", None))
    mcp_servers = payload.get("mcp_servers")
    if isinstance(mcp_servers, dict):
        payload["mcp_servers"] = _redact_mcp_servers(mcp_servers)
    return {
        "id": row.id,
        "version": row.version,
        "enabled": row.enabled,
        "priority": row.priority,
        "updated_at": row.updated_at,
        "updated_by": row.updated_by,
        "project_key": row.project_key,
        "github_repo": row.github_repo,
        "issue_label": row.issue_label,
        "tracker_provider": row.tracker_provider,
        "tracker_site": row.tracker_site,
        "webhook_secret_set": webhook_secret_set,
        "payload": payload,
    }


def _validation_error(loc: list[Any], msg: str) -> HTTPException:
    """A 422 shaped like FastAPI's own — a `detail` list of `{loc, msg}` the
    form maps to fields."""
    return HTTPException(status_code=422, detail=[{"loc": loc, "msg": msg}])


def _selector(binding: RepoBinding) -> tuple[str, str, str, str, str]:
    """The dispatch selector: tracker scope + normalized label + ready state.
    Repo-independent (dispatch matches on scope+label, not GitHub repo), so two
    enabled bindings sharing it would claim the same issues — the exact
    duplicate the write path rejects. Differing ready states (Backlog vs Todo)
    make it a legitimate two-lane setup, kept distinct here."""
    return (
        binding.tracker_provider,
        binding.tracker_site,
        binding.project_key,
        binding.issue_label or "",
        binding.states.ready,
    )


def _env_key_source() -> dict[str, str]:
    import os

    from dotenv import dotenv_values

    source: dict[str, str] = {k: v for k, v in dotenv_values(".env").items() if v is not None}
    source.update(os.environ)
    return source


def _validate_binding(payload: dict[str, Any], *, jira_base_url: str) -> RepoBinding:
    """Field-level validation. Raises a 422 `HTTPException` carrying `loc`
    paths for pydantic errors, legacy role fields, and unknown env key names."""
    legacy = sorted(_LEGACY_ROLE_FIELDS & payload.keys())
    if legacy:
        raise _validation_error(
            [legacy[0]],
            f"legacy role field(s) {', '.join(legacy)} are not allowed; role "
            f"config lives in the `roles` matrix only",
        )
    try:
        binding = RepoBinding.model_validate(payload)
    except ValidationError as e:
        raise HTTPException(
            status_code=422,
            detail=[{"loc": list(err["loc"]), "msg": err["msg"]} for err in e.errors()],
        ) from e
    # `RepoBinding` has no `extra="forbid"` (pydantic default `extra="ignore"`),
    # so a typo'd key would otherwise pass validation, persist verbatim, and be
    # silently ignored by the daemon forever — fail closed instead.
    unknown = sorted(
        k for k in payload if k not in RepoBinding.model_fields and k not in _FIELD_ALIASES
    )
    if unknown:
        raise _validation_error(
            [unknown[0]],
            f"unknown field(s) {', '.join(unknown)}; not part of RepoBinding "
            f"(known fields: {', '.join(sorted(RepoBinding.model_fields))})",
        )
    # `model_validate` derives `tracker_site` with no global `jira_base_url`,
    # so a Jira binding relying on that global (no per-binding `base_url`)
    # would key on the "default" placeholder instead of the site it actually
    # resolves to at runtime — re-derive it here so the persisted natural key
    # matches `assemble_effective_config`'s resolution byte-for-byte (same fix
    # as `config_import.py`).
    binding.apply_tracker_secret_defaults(jira_base_url=jira_base_url)
    source = _env_key_source()
    unknown = sorted(name for name in binding.env.values() if name not in source)
    if unknown:
        available = ", ".join(sorted(source)) or "(none)"
        raise _validation_error(
            ["env"],
            f"env key name(s) not found in the server env: {', '.join(unknown)}; "
            f"available: {available}",
        )
    return binding


# Branch-resolving fields: later stages resolve branches from the *current*
# binding row after hot reload, so changing these mid-PR would point fix,
# delivery, and reconciliation paths at a branch other than the dispatched one.
# Edits to them are drain-guarded exactly like a natural-key change (SYM-193).
_BRANCH_FIELDS: tuple[tuple[str, Any], ...] = (
    ("branch_prefix", "symphony"),
    ("base_branch", None),
)


def _binding_key_str(row: config_bindings.StoredBinding) -> str:
    """The stored row's natural key as the JSON string runs/PRs are stamped
    with (`_binding_storage_key`) — same components, same order, same compact
    separators — so a drain query attributes work back to this binding."""
    return json.dumps(
        [
            row.project_key,
            row.github_repo,
            row.issue_label,
            row.tracker_provider,
            row.tracker_site,
        ],
        separators=(",", ":"),
    )


def _row_natural_key(
    row: config_bindings.StoredBinding,
) -> tuple[str, str, str, str, str]:
    return (
        row.project_key,
        row.github_repo,
        row.issue_label,
        row.tracker_provider,
        row.tracker_site,
    )


async def _drain_blockers(
    conn: aiosqlite.Connection,
    row: config_bindings.StoredBinding,
    *,
    scheduled_slots: Callable[[tuple[str, str, str, str, str]], int] | None,
) -> dict[str, Any] | None:
    """The binding's active work that a DELETE / natural-key / branch edit
    would orphan (SYM-193): running runs (attributed via the stamped key),
    tracked open PRs, parked operator waits in its scope, and in-memory
    scheduled dispatch/fix-run slots the daemon reserved before a run row
    exists. Returns the blocker map, or `None` when the binding is drained."""
    binding_key = _binding_key_str(row)
    natural_key = _row_natural_key(row)
    running = await runs.live_identifiers_for_binding_key(
        conn,
        binding_key,
        legacy_team_key=row.project_key,
        legacy_github_repo=row.github_repo,
    )
    open_prs = await issue_prs.open_identifiers_for_binding_key(
        conn,
        binding_key,
        legacy_team_key=row.project_key,
        legacy_github_repo=row.github_repo,
    )
    waits = await operator_waits.open_identifiers_for_natural_key(conn, natural_key)
    scheduled = scheduled_slots(natural_key) if scheduled_slots is not None else 0
    if running or open_prs or waits or scheduled:
        return {
            "running_runs": running,
            "open_prs": open_prs,
            "operator_waits": waits,
            "scheduled_slots": scheduled,
        }
    return None


def _drain_conflict(action: str, blockers: dict[str, Any]) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "msg": (
                f"cannot {action} a binding with active work; drain it first "
                f"(finish or stop the blocking issues/PRs)"
            ),
            "blockers": blockers,
        },
    )


def _branch_or_key_changed(
    old: config_bindings.StoredBinding,
    new_key: tuple[str, str, str, str, str],
    new_payload: dict[str, Any],
) -> bool:
    """Whether an update changes the natural key or a branch-resolving field —
    the edits the drain guard protects (a rename would strand parked work; a
    branch change would repoint later stages off the dispatched branch)."""
    if _row_natural_key(old) != new_key:
        return True
    for field, default in _BRANCH_FIELDS:
        if old.payload.get(field, default) != new_payload.get(field, default):
            return True
    return False


def _validate_webhook_secret(binding: RepoBinding, base: Config) -> None:
    """Fail closed at save time: a `webhook_enabled` binding with no
    resolvable secret (its own `webhook_secret` or the global
    `GITHUB_WEBHOOK_SECRET`) makes `cli._live_github_webhook_settings` swallow
    the resulting error on the daemon's next hot reload and silently disable
    *every* repo's webhook verification, not just this one."""
    if binding.webhook_enabled and not binding.webhook_secret and not base.github_webhook_secret:
        raise _validation_error(
            ["webhook_secret"],
            "webhook_enabled requires a webhook_secret when no global "
            "GITHUB_WEBHOOK_SECRET is configured; set one or disable "
            "webhook_enabled",
        )


def _validate_tracker_credentials(binding: RepoBinding, base: Config) -> None:
    """Fail closed at save time: a binding whose provider has no resolvable
    tracker credentials in `base` (the daemon's live `Config`, populated from
    `Secrets` at boot) would otherwise save cleanly and then fail every
    hot-reload tick — `Orchestrator._reload_bindings` swallows the resulting
    `for_binding` `ValueError` and just logs it, so the binding never
    dispatches and nothing surfaces to the operator. A fresh DB-owned install
    boots with zero bindings, skipping `cli._run`'s boot-time
    `LINEAR_API_KEY` check, so this is the only gate the first binding of a
    kind ever passes through.
    """
    if binding.provider == "linear" and not base.linear_api_key:
        raise _validation_error(
            ["provider"],
            "LINEAR_API_KEY is not configured in the server env; set it "
            "before saving a linear binding",
        )
    if binding.provider == "jira":
        missing = [
            name
            for name, value in (
                ("JIRA_BASE_URL", binding.base_url or base.jira_base_url),
                ("JIRA_EMAIL", base.jira_email),
                ("JIRA_API_TOKEN", base.jira_api_token),
            )
            if not value
        ]
        if missing:
            raise _validation_error(
                ["provider"],
                f"{', '.join(missing)} not configured in the server env; set "
                f"them (or the binding's own base_url) before saving a jira "
                f"binding",
            )


async def _other_bindings(
    conn: aiosqlite.Connection,
    base: Config,
    *,
    exclude_id: int | None,
) -> list[RepoBinding]:
    """All enabled/disabled bindings but `exclude_id`, re-listed fresh from the
    DB. Cheap (no network) — safe to call again right before a write, inside
    the lock, to see a binding that landed during an earlier network-bound
    check (SYM-191 review)."""
    existing = await config_bindings.list_all(conn)
    others: list[RepoBinding] = []
    for row in existing:
        if row.id == exclude_id:
            continue
        other = RepoBinding.model_validate({**row.payload, "enabled": row.enabled})
        # A stored Jira binding relying on the global `jira_base_url` (no
        # per-binding `base_url`) has no `tracker_site` of its own in the
        # payload; re-derive it the same way `_validate_binding` does for the
        # candidate, or the duplicate check below would compare the
        # candidate's real site against this row's stale "default" placeholder.
        other.apply_tracker_secret_defaults(jira_base_url=base.jira_base_url)
        others.append(other)
    return others


def _reject_duplicate_selector(candidate: RepoBinding, others: list[RepoBinding]) -> None:
    """Raise a 422 if enabled `candidate` shares its dispatch selector with an
    enabled binding in `others`. Must be re-run against a fresh `others`
    (`_other_bindings`) inside `lock` immediately before the insert/update it
    guards — the pre-lock trial in `_assemble_and_validate` only reflects DB
    state as of before the (potentially slow) capability check, so a binding
    that lands during that wait would otherwise be missed (SYM-191 review)."""
    if not candidate.enabled:
        return
    candidate_selector = _selector(candidate)
    for other in others:
        if other.enabled and _selector(other) == candidate_selector:
            raise _validation_error(
                ["issue_label"],
                "an enabled binding with the same tracker scope, label and "
                "ready state already exists (exact-duplicate selector); "
                "change the label, the ready state, or disable one",
            )


async def _reject_stale_globals(conn: aiosqlite.Connection, validated_version: int) -> None:
    """Raise a 409 if the global roles matrix's `version` has moved since
    `_assemble_and_validate` ran. Must be re-run inside `lock` immediately
    before the insert/update it guards: a global roles PUT landing during the
    (possibly 30s) pre-lock capability check would otherwise be invisible to
    this binding's save — that write bumps `config_globals.version`, not this
    binding's own row version, so the binding's own optimistic lock can't
    catch it (SYM-191 review)."""
    current = await config_globals.get(conn)
    current_version = current.version if current is not None else 0
    if current_version != validated_version:
        raise HTTPException(
            status_code=409,
            detail={
                "msg": "global roles matrix changed while validating this binding; "
                "reload and retry",
            },
        )


async def _assemble_and_validate(
    conn: aiosqlite.Connection,
    base: Config,
    candidate: RepoBinding,
    *,
    exclude_id: int | None,
) -> tuple[list[str], int]:
    """Assemble the trial effective config (system knobs + all DB bindings, the
    candidate swapped in for `exclude_id`, + global matrix) and re-run the
    roles-matrix validators. Returns non-blocking warnings (lost review-family
    diversity) and the global-matrix `version` validated against (0 when no
    globals row exists), so the caller can recheck it hasn't moved before
    committing. Raises a 422 on a duplicate selector or a validator error."""
    others = await _other_bindings(conn, base, exclude_id=exclude_id)
    _reject_duplicate_selector(candidate, others)

    globals_row = await config_globals.get(conn)
    global_roles: dict[RoleName, RoleConfig] = {}
    if globals_row is not None:
        from pydantic import TypeAdapter

        global_roles = TypeAdapter(dict[RoleName, RoleConfig]).validate_python(globals_row.roles)
    globals_version = globals_row.version if globals_row is not None else 0

    trial = base.model_copy(update={"repos": [*others, candidate], "roles": global_roles})
    caught = _run_matrix_validators(trial)
    # Scoped to the candidate alone: a stale/now-unsupported claude pair on an
    # unrelated binding (e.g. saved fail-open before ANTHROPIC_API_KEY was set)
    # must not block *this* save. The all-bindings sweep is reserved for
    # `put_roles`, where a global change really does reach every binding.
    await _reject_unsupported_efforts(trial, bindings=[candidate])
    # `validate_roles_matrix` warns for every binding in the trial set; only the
    # candidate's own warnings are relevant to this save (others are unchanged).
    prefix = f"binding {candidate.project_key}/{candidate.github_repo}:"
    wgs = [
        str(w.message)
        for w in caught
        if issubclass(w.category, UserWarning) and str(w.message).startswith(prefix)
    ]
    return wgs, globals_version


def _run_matrix_validators(trial: Config) -> list[warnings.WarningMessage]:
    """Run the roles-matrix validators over `trial`, returning the captured
    warnings. Raises a 422 on any validator error (family/effort structural
    check, legacy/matrix conflict, duplicate)."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            trial.validate_roles_matrix()
        except (ValidationError, ValueError) as e:
            raise _validation_error(["roles"], str(e)) from e
    return caught


def _binding_anthropic_key(
    binding: RepoBinding, env_source: dict[str, str], process_key: str
) -> str:
    """Resolve the ANTHROPIC_API_KEY a binding actually runs with, mirroring
    preflight's precedence (`cli._preflight_validate_capabilities`): a
    binding's own (unresolved) `env: {ANTHROPIC_API_KEY: <source name>}`
    overrides the process key entirely — even to "" for an unresolvable
    source — since the runner merges `{**os.environ, **spec.env}` and an
    empty override really does mean no key for that binding's subprocess.
    `_validate_binding` already rejects an unresolvable source name, so the
    lookup here is safe."""
    if "ANTHROPIC_API_KEY" not in binding.env:
        return process_key
    return env_source.get(binding.env["ANTHROPIC_API_KEY"], "")


async def _reject_unsupported_efforts(
    trial: Config, *, bindings: list[RepoBinding] | None = None, only_inherited: bool = False
) -> None:
    """Re-run the preflight capability check for every resolved *claude*
    `(model, effort)` pair in `trial` — the same online Models-API source
    preflight uses, keyed the same way (per binding, its own `env:`
    ANTHROPIC_API_KEY taking precedence over the process key) — so a
    deployment whose key lives only in a binding's `env:` is validated the
    same way it will actually run, not silently skipped. Likewise, a binding's
    own `ANTHROPIC_DEFAULT_SONNET_MODEL`/`OPUS`/`HAIKU` (via its `env:`
    mapping) takes precedence over the process-wide var when resolving a bare
    `sonnet`/`opus`/`haiku` alias, matching the runner's `{**os.environ,
    **spec.env}` precedence — otherwise this would validate the alias against
    whatever the process pins it to, not what that binding's subprocess
    actually runs (SYM-191 review). codex efforts are
    the fixed family enum (already checked structurally by
    `validate_roles_matrix`). A `None` result (no ANTHROPIC_API_KEY anywhere
    for that binding) skips the pair: the daemon may run claude via CLI auth,
    and the structural family check already ran — exactly preflight's
    fail-open-on-no-key behavior. A key IS present but the Models-API call
    itself fails (auth, network, malformed response) raises `ValueError`,
    which this save fails closed on (422) rather than silently accepting any
    effort for that pair — a present-but-broken key means something is wrong,
    not "no key configured" (SYM-191 review).

    A claude role with `effort` set but `model=None` (e.g. `implement: {agent:
    claude, effort: xhigh}` with no model anywhere in the chain — the ordinary
    default, since claude builders pass no `--model`) fails closed too: there
    is no known model to check the effort against, and the CLI's own default
    model may not support it, so the mismatch would only surface at dispatch
    instead of at save (SYM-191 review).

    `bindings` scopes the sweep: `_assemble_and_validate` passes just the
    candidate, so a stale pair on an unrelated binding can't block this save.
    `put_roles` leaves it `None` (all of `trial.repos`); zero bindings there
    falls back to the synthetic binding so an unsupported global pair still
    can't save silently.

    `only_inherited` (set by `put_roles`) skips a binding/role pair whose
    `agent`, `model`, *and* `effort` are all pinned by the binding itself —
    only then is the resolved pair fully binding-owned and untouched by a
    global roles write, so a stale/unsupported override on an unrelated role
    or binding can't block an otherwise-unrelated global edit. A binding role
    that pins `model`/`effort` but leaves `agent` inherited must still be
    rechecked: the global write can flip its resolved agent (e.g. claude ->
    codex or back), changing which family's efforts that pinned model/effort
    pair is validated against (SYM-191 review)."""
    env_source = _env_key_source()
    process_key = env_source.get("ANTHROPIC_API_KEY", "")
    pairs: set[tuple[str, str, str, str]] = set()  # (key, model, effort, resolved_model)
    for binding in (
        bindings
        if bindings is not None
        else (trial.repos or [_synthetic_matrix_validation_binding()])
    ):
        binding_key = _binding_anthropic_key(binding, env_source, process_key)
        # Same merge the runner uses for the actual subprocess
        # (`{**os.environ, **spec.env}`): a binding pinning
        # `ANTHROPIC_DEFAULT_SONNET_MODEL` etc. via its own (unresolved) `env:`
        # mapping runs against that pin, not the process-wide var, so the alias
        # must resolve the same way here or this validates the wrong model
        # (SYM-191 review). `binding.env` here maps var name -> source name, not
        # the value itself (see `_binding_anthropic_key`), so resolve each
        # through `env_source` before merging.
        binding_env = {
            **env_source,
            **{var: env_source.get(source, "") for var, source in binding.env.items()},
        }
        for name in get_args(RoleName):
            if only_inherited:
                binding_role = binding.roles.get(name)
                if (
                    binding_role is not None
                    and binding_role.agent is not None
                    and binding_role.model is not None
                    and binding_role.effort is not None
                ):
                    continue
            role = binding.resolved_role(name, trial.roles)
            if role.agent != "claude" or role.effort is None:
                continue
            if role.model is None:
                raise _validation_error(
                    ["roles"],
                    f"role {name!r} sets effort {role.effort!r} for claude with no "
                    "resolved model; pin an explicit model on this role to set an "
                    "effort override",
                )
            resolved_model = _resolve_alias_model_id(role.model, binding_env)
            pairs.add((binding_key, role.model, role.effort, resolved_model))
    caps: dict[tuple[str, str], list[str] | None] = {}
    for key, model, effort, resolved_model in sorted(pairs):
        cache_key = (key, resolved_model)
        if cache_key not in caps:
            try:
                caps[cache_key] = await fetch_claude_effort_capabilities(resolved_model, key)
            except ValueError as e:
                raise _validation_error(["roles"], str(e)) from e
        supported = caps[cache_key]
        if supported is not None and effort not in supported:
            raise _validation_error(
                ["roles"],
                f"claude model {model!r} does not support effort {effort!r}; "
                f"supported: {', '.join(supported)}",
            )


def _diff(
    old: config_bindings.StoredBinding | None, new: dict[str, Any], *, enabled: bool, priority: int
) -> dict[str, Any]:
    """Field-level diff for the daemon log. Secret-bearing fields log only a
    set/cleared/changed flag, never a value."""
    old_payload = old.payload if old is not None else {}
    changes: dict[str, Any] = {}
    for key in sorted(set(old_payload) | set(new)):
        before, after = old_payload.get(key), new.get(key)
        if before == after:
            continue
        if key in _SECRET_FIELDS:
            changes[key] = "changed" if before and after else ("cleared" if before else "set")
        elif key == "mcp_servers":
            # `mcp_servers` entries carry literal credentials (see module
            # docstring) — log which servers changed, never their contents.
            before_names = sorted(before) if isinstance(before, dict) else []
            after_names = sorted(after) if isinstance(after, dict) else []
            changes[key] = {"servers_before": before_names, "servers_after": after_names}
        else:
            changes[key] = {"from": before, "to": after}
    if old is None or old.enabled != enabled:
        changes["enabled"] = {"from": old.enabled if old else None, "to": enabled}
    if old is None or old.priority != priority:
        changes["priority"] = {"from": old.priority if old else None, "to": priority}
    return changes


def create_config_crud_router(
    conn_provider: Callable[[], Awaitable[aiosqlite.Connection]],
    *,
    config_provider: Config | Callable[[], Config | None] | None,
    write_lock: Any = None,
    auth_dependency: Callable[..., Awaitable[dict[str, Any]]] | None = None,
    clock: Callable[[], datetime] | None = None,
    scheduled_slots: Callable[[tuple[str, str, str, str, str]], int] | None = None,
) -> APIRouter:
    """Router mounting the binding CRUD + options endpoints under `/api/config`.

    `conn_provider` resolves a connection dedicated to this router — never the
    daemon's shared connection. The DAO methods below `commit()` on success;
    a `commit()` on a connection the orchestrator also writes through would
    flush whatever unrelated, not-yet-committed statements the orchestrator
    happened to have pending on it at that moment (SYM-190).

    `write_lock` guards each write's transaction against the daemon's
    tick-boundary binding reload, which runs on the orchestrator's own
    connection (SYM-189). When `None` (tests without a daemon) a private lock
    is used.
    """
    import asyncio

    router = APIRouter(prefix="/api/config")
    lock = write_lock if write_lock is not None else asyncio.Lock()

    def _base_config() -> Config:
        current = config_provider() if callable(config_provider) else config_provider
        return current if current is not None else Config()

    # Resolve `updated_by` from the auth token email, or `local` without Auth0.
    if auth_dependency is None:

        async def _updated_by() -> str:
            return "local"
    else:

        async def _updated_by(  # type: ignore[misc]
            claims: dict[str, Any] = Depends(auth_dependency),  # noqa: B008
        ) -> str:
            email = claims.get("email")
            return email if isinstance(email, str) and email else "local"

    @router.get("/options")
    async def get_options() -> dict[str, Any]:
        # Claude efforts are *per model*: the same Models-API capability source
        # preflight and the save path use, so the form's effort dropdown offers
        # exactly what the selected model accepts. A `None`/error result (no
        # ANTHROPIC_API_KEY, or an API error) falls back to the family-wide set
        # so the dropdown is never empty; the save path re-checks regardless.
        family_efforts = sorted(SUPPORTED_CLAUDE_EFFORTS)
        process_key = _env_key_source().get("ANTHROPIC_API_KEY", "")
        aliases = sorted(CLAUDE_MODEL_ALIASES)

        async def _caps(alias: str) -> list[str] | None:
            try:
                return await fetch_claude_effort_capabilities(alias, process_key)
            except ValueError:
                return None

        results = await asyncio.gather(*(_caps(alias) for alias in aliases))
        claude_efforts_by_model: dict[str, list[str]] = {
            alias: caps if caps is not None else family_efforts
            for alias, caps in zip(aliases, results, strict=True)
        }
        return {
            "agent_families": ["claude", "codex"],
            "codex_models": sorted(SUPPORTED_CODEX_MODELS),
            "claude_aliases": sorted(CLAUDE_MODEL_ALIASES),
            "codex_efforts": sorted(SUPPORTED_CODEX_EFFORTS),
            # Family-wide union — the fallback the form uses for a full
            # `claude-*` model ID typed in advanced JSON (no per-alias entry).
            "claude_efforts": family_efforts,
            "claude_efforts_by_model": claude_efforts_by_model,
            "merge_strategies": ["squash", "merge", "rebase"],
            # Lets the form default a new binding's `webhook_enabled` to what
            # will actually save: without a global secret, the write path
            # rejects the field's own `True` default unless the operator also
            # sets a per-binding `webhook_secret`.
            "github_webhook_secret_configured": bool(_base_config().github_webhook_secret),
        }

    @router.get("/roles")
    async def get_roles() -> dict[str, Any]:
        conn = await conn_provider()
        row = await config_globals.get(conn)
        if row is None:
            # Fresh, never-migrated DB: no globals row → empty matrix at
            # version 0, the version a first PUT must send.
            return {"roles": {}, "version": 0}
        return {"roles": row.roles, "version": row.version}

    @router.put("/roles")
    async def put_roles(
        body: RolesWrite = Body(...),  # noqa: B008
        updated_by: str = Depends(_updated_by),  # noqa: B008
    ) -> dict[str, Any]:
        base = _base_config()
        try:
            global_roles = _ROLES_ADAPTER.validate_python(body.roles)
        except ValidationError as e:
            raise HTTPException(
                status_code=422,
                detail=[{"loc": ["roles", *err["loc"]], "msg": err["msg"]} for err in e.errors()],
            ) from e
        stored = _dump_roles(global_roles)
        conn = await conn_provider()
        # Assembly + validation (including the external Models-API capability
        # check) runs before the lock is acquired: it's a read-only trial
        # against the DB's current state, and the online check can take up to
        # the full 30s httpx timeout — awaiting it inside the lock would
        # serialize every other config write behind it (SYM-191 review). The
        # actual write below re-validates the version, so a write that lands
        # between this check and the lock still fails safe as a 409.
        existing = await config_bindings.list_all(conn)
        others: list[RepoBinding] = []
        for row in existing:
            other = RepoBinding.model_validate({**row.payload, "enabled": row.enabled})
            other.apply_tracker_secret_defaults(jira_base_url=base.jira_base_url)
            others.append(other)
        trial = base.model_copy(update={"repos": others, "roles": global_roles})
        caught = _run_matrix_validators(trial)
        # Scoped to pairs that actually resolve through the edited global
        # cells: a binding pinning its own explicit `(model, effort)` for a
        # role is unaffected by this write and must not block it (SYM-191
        # review) — that binding's own save already validated its pair.
        await _reject_unsupported_efforts(trial, only_inherited=True)
        binding_snapshot = {row.id: row.version for row in existing}
        async with lock:
            # Cheap, DB-only recheck: a binding created/updated/deleted during
            # the (possibly 30s) capability check above isn't reflected in
            # `trial`, and a binding write bumps its own row's version, not
            # `config_globals.version` — so the write below can't detect that
            # staleness on its own. Bindings don't change often enough for
            # this to cost real retries; fail safe with a 409 rather than
            # commit against a stale binding set (SYM-191 review).
            fresh = await config_bindings.list_all(conn)
            if {row.id: row.version for row in fresh} != binding_snapshot:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "msg": "bindings changed while validating the roles matrix; "
                        "reload and retry",
                    },
                )
            try:
                new_version = await config_globals.update_roles(
                    conn, roles=stored, expected_version=body.version
                )
            except config_globals.StaleVersionError as e:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "msg": "roles matrix was modified by another writer; reload and retry",
                        "current_version": e.current_version,
                    },
                ) from e
        # A global-matrix edit reaches every binding, so every binding's
        # diversity warning is relevant here (unlike a single-binding save).
        wgs = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
        _log.info("config roles matrix updated by %s → version %s", updated_by, new_version)
        return {"roles": stored, "version": new_version, "warnings": wgs}

    @router.get("/bindings")
    async def list_bindings() -> list[dict[str, Any]]:
        conn = await conn_provider()
        out: list[dict[str, Any]] = []
        for row in await config_bindings.list_all(conn):
            # `active_work` drives the card's indicator and previews whether a
            # delete/rename would be drain-blocked (SYM-193).
            blockers = await _drain_blockers(conn, row, scheduled_slots=scheduled_slots)
            out.append({**_serialize(row), "active_work": blockers is not None})
        return out

    @router.get("/bindings/{binding_id}")
    async def get_binding(binding_id: int) -> dict[str, Any]:
        conn = await conn_provider()
        row = await config_bindings.get(conn, binding_id)
        if row is None:
            raise HTTPException(status_code=404, detail="binding not found")
        return _serialize(row)

    @router.post("/bindings", status_code=201)
    async def create_binding(
        body: BindingWrite = Body(...),  # noqa: B008
        updated_by: str = Depends(_updated_by),  # noqa: B008
    ) -> dict[str, Any]:
        payload = _sanitize_payload(body.payload)
        base = _base_config()
        binding = _validate_binding(payload, jira_base_url=base.jira_base_url)
        binding.enabled = body.enabled
        _validate_webhook_secret(binding, base)
        _validate_tracker_credentials(binding, base)
        conn = await conn_provider()
        # Assembly + validation (including the external Models-API capability
        # check, up to the full 30s httpx timeout) runs before the lock is
        # acquired — see the matching comment in `put_roles` for why.
        wgs, globals_version = await _assemble_and_validate(conn, base, binding, exclude_id=None)
        async with lock:
            # Re-check the cheap, DB-state-dependent duplicate-selector
            # invariant against a fresh listing, now that the lock excludes
            # concurrent writers — the trial above ran before the lock (and
            # after the slow capability check), so a same-selector binding
            # from a racing create/update could have landed since (SYM-191
            # review).
            _reject_duplicate_selector(binding, await _other_bindings(conn, base, exclude_id=None))
            await _reject_stale_globals(conn, globals_version)
            try:
                new_id = await config_bindings.insert(
                    conn,
                    payload=payload,
                    key=binding_natural_key(binding),
                    enabled=body.enabled,
                    priority=body.priority,
                    updated_at=_now_iso(clock),
                    updated_by=updated_by,
                )
            except sqlite3.IntegrityError as e:
                # The failed `INSERT` left a write transaction open on this
                # connection (never committed) — roll it back before raising,
                # or it holds the write lock until some unrelated later
                # commit closes it (same concern as the stale-version paths
                # in `db/config_bindings.py`).
                await conn.rollback()
                raise _validation_error(
                    ["github_repo"],
                    "a binding with this project/repo/label/provider/site already exists",
                ) from e
            row = await config_bindings.get(conn, new_id)
        _log.info(
            "config binding %s created by %s: %s",
            new_id,
            updated_by,
            _diff(None, payload, enabled=body.enabled, priority=body.priority),
        )
        assert row is not None
        return {**_serialize(row), "warnings": wgs}

    @router.put("/bindings/{binding_id}")
    async def update_binding(
        binding_id: int,
        body: BindingWrite = Body(...),  # noqa: B008
        updated_by: str = Depends(_updated_by),  # noqa: B008
    ) -> dict[str, Any]:
        if body.version is None:
            raise _validation_error(["version"], "version is required for an update")
        payload = _sanitize_payload(body.payload)
        base = _base_config()
        conn = await conn_provider()
        old = await config_bindings.get(conn, binding_id)
        if old is None:
            raise HTTPException(status_code=404, detail="binding not found")
        # The served payload redacts `webhook_secret` (see `_serialize`); if
        # the write omits it, keep the stored secret rather than dropping it
        # on every edit that round-trips the redacted GET response. An
        # explicit (even empty) value in the payload still overrides it.
        if "webhook_secret" not in payload and old.payload.get("webhook_secret"):
            payload["webhook_secret"] = old.payload["webhook_secret"]
        # Same round-trip concern for `mcp_servers`' redacted `env`/`headers`
        # (see `_redact_mcp_servers`/`_restore_mcp_secrets`).
        if isinstance(payload.get("mcp_servers"), dict):
            payload["mcp_servers"] = _restore_mcp_secrets(
                old.payload.get("mcp_servers"), payload["mcp_servers"]
            )
        binding = _validate_binding(payload, jira_base_url=base.jira_base_url)
        binding.enabled = body.enabled
        _validate_webhook_secret(binding, base)
        _validate_tracker_credentials(binding, base)
        # Assembly + validation (including the external Models-API capability
        # check, up to the full 30s httpx timeout) runs before the lock is
        # acquired — see the matching comment in `put_roles` for why. `old` is
        # read here too (not inside the lock); the write below still carries
        # its own optimistic-lock `version` check, so a concurrent change
        # lands as a 409, not a lost update.
        wgs, globals_version = await _assemble_and_validate(
            conn, base, binding, exclude_id=binding_id
        )
        async with lock:
            # Drain guard: a natural-key or branch-affecting edit would detach
            # live work from the row it was dispatched under (a rename strands
            # parked waits; a branch change repoints later stages), so it's
            # allowed only for a drained binding (SYM-193). Ordinary edits
            # (label-free field tweaks, enable/disable, cap changes) are exempt.
            if _branch_or_key_changed(old, binding_natural_key(binding), payload):
                blockers = await _drain_blockers(conn, old, scheduled_slots=scheduled_slots)
                if blockers is not None:
                    raise _drain_conflict("rename or re-point", blockers)
            # Same re-check as `create_binding` — see that comment.
            _reject_duplicate_selector(
                binding, await _other_bindings(conn, base, exclude_id=binding_id)
            )
            await _reject_stale_globals(conn, globals_version)
            try:
                row = await config_bindings.update(
                    conn,
                    binding_id,
                    payload=payload,
                    key=binding_natural_key(binding),
                    enabled=body.enabled,
                    priority=body.priority,
                    expected_version=body.version,
                    updated_at=_now_iso(clock),
                    updated_by=updated_by,
                )
            except config_bindings.StaleVersionError as e:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "msg": "binding was modified by another writer; reload and retry",
                        "current_version": e.current_version,
                    },
                ) from e
            except sqlite3.IntegrityError as e:
                # Same concern as `create_binding`: roll back the open write
                # transaction from the failed `UPDATE` before raising.
                await conn.rollback()
                raise _validation_error(
                    ["github_repo"],
                    "a binding with this project/repo/label/provider/site already exists",
                ) from e
        _log.info(
            "config binding %s updated by %s: %s",
            binding_id,
            updated_by,
            _diff(old, payload, enabled=body.enabled, priority=body.priority),
        )
        return {**_serialize(row), "warnings": wgs}

    @router.delete("/bindings/{binding_id}", status_code=204)
    async def delete_binding(
        binding_id: int,
        version: int = Query(...),
        updated_by: str = Depends(_updated_by),  # noqa: B008
    ) -> None:
        conn = await conn_provider()
        async with lock:
            # Drain guard: refuse to delete a binding with active work, or the
            # DELETE would orphan its open PRs / parked waits / in-flight runs
            # (SYM-193). Checked inside the lock so a scan can't reserve a slot
            # between the check and the delete.
            existing = await config_bindings.get(conn, binding_id)
            if existing is not None:
                blockers = await _drain_blockers(conn, existing, scheduled_slots=scheduled_slots)
                if blockers is not None:
                    raise _drain_conflict("delete", blockers)
            try:
                await config_bindings.delete(conn, binding_id, expected_version=version)
            except config_bindings.StaleVersionError as e:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "msg": "binding was modified by another writer; reload and retry",
                        "current_version": e.current_version,
                    },
                ) from e
        _log.info("config binding %s deleted by %s", binding_id, updated_by)

    return router
