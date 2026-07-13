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
  * rejects `enabled: false` outright, mirroring the importer
    (`config_import.py`): the binding lifecycle (dispatch skip, launch gate,
    drain guard) ships in SYM-193, and until then every DB row dispatches
    regardless of this column, so persisting a disabled row would silently
    keep dispatching it;
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

Secrets never appear in a response: `webhook_secret` is dropped from the served
payload in favour of a `webhook_secret_set` flag. `mcp_servers` entries have no
`resolve_env`-style name indirection (unlike the top-level `env` field), so an
operator embeds literal credentials straight into a server's `env`/`headers` —
those are redacted to per-key `true` flags the same way, and restored from the
stored row on write when the operator round-trips them untouched.
"""

from __future__ import annotations

import logging
import sqlite3
import warnings
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import aiosqlite
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, ValidationError

from ..agent.codex_models import SUPPORTED_CODEX_EFFORTS, SUPPORTED_CODEX_MODELS
from ..config import (
    _LEGACY_ROLE_FIELDS,
    CLAUDE_MODEL_ALIASES,
    SUPPORTED_CLAUDE_EFFORTS,
    Config,
    RepoBinding,
    RoleConfig,
    RoleName,
    binding_natural_key,
)
from ..db import config_bindings, config_globals

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


def _reject_disabled_write(enabled: bool) -> None:
    """Mirror the importer's refusal (`config_import.py`): the binding
    lifecycle (dispatch skip, launch gate, drain guard) ships in SYM-193, and
    until then `assemble_effective_config` dispatches every DB row as enabled
    regardless of this column — so persisting `enabled: false` would silently
    keep dispatching the row anyway."""
    if not enabled:
        raise _validation_error(
            ["enabled"],
            "disabling a binding has no effect yet; the binding lifecycle "
            "(disable/drain) ships in SYM-193 and this build would dispatch "
            "the row anyway — leave it enabled or delete it",
        )


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


async def _assemble_and_validate(
    conn: aiosqlite.Connection,
    base: Config,
    candidate: RepoBinding,
    *,
    exclude_id: int | None,
) -> list[str]:
    """Assemble the trial effective config (system knobs + all DB bindings, the
    candidate swapped in for `exclude_id`, + global matrix) and re-run the
    roles-matrix validators. Returns non-blocking warnings (lost review-family
    diversity). Raises a 422 on a duplicate selector or a validator error."""
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

    if candidate.enabled:
        candidate_selector = _selector(candidate)
        for other in others:
            if other.enabled and _selector(other) == candidate_selector:
                raise _validation_error(
                    ["issue_label"],
                    "an enabled binding with the same tracker scope, label and "
                    "ready state already exists (exact-duplicate selector); "
                    "change the label, the ready state, or disable one",
                )

    globals_row = await config_globals.get(conn)
    global_roles: dict[RoleName, RoleConfig] = {}
    if globals_row is not None:
        from pydantic import TypeAdapter

        global_roles = TypeAdapter(dict[RoleName, RoleConfig]).validate_python(globals_row.roles)

    trial = base.model_copy(update={"repos": [*others, candidate], "roles": global_roles})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            trial.validate_roles_matrix()
        except (ValidationError, ValueError) as e:
            raise _validation_error(["roles"], str(e)) from e
    # `validate_roles_matrix` warns for every binding in the trial set; only the
    # candidate's own warnings are relevant to this save (others are unchanged).
    prefix = f"binding {candidate.project_key}/{candidate.github_repo}:"
    return [
        str(w.message)
        for w in caught
        if issubclass(w.category, UserWarning) and str(w.message).startswith(prefix)
    ]


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
        return {
            "agent_families": ["claude", "codex"],
            "codex_models": sorted(SUPPORTED_CODEX_MODELS),
            "claude_aliases": sorted(CLAUDE_MODEL_ALIASES),
            "codex_efforts": sorted(SUPPORTED_CODEX_EFFORTS),
            "claude_efforts": sorted(SUPPORTED_CLAUDE_EFFORTS),
            "merge_strategies": ["squash", "merge", "rebase"],
            # Lets the form default a new binding's `webhook_enabled` to what
            # will actually save: without a global secret, the write path
            # rejects the field's own `True` default unless the operator also
            # sets a per-binding `webhook_secret`.
            "github_webhook_secret_configured": bool(_base_config().github_webhook_secret),
        }

    @router.get("/bindings")
    async def list_bindings() -> list[dict[str, Any]]:
        conn = await conn_provider()
        return [_serialize(row) for row in await config_bindings.list_all(conn)]

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
        _reject_disabled_write(body.enabled)
        payload = _sanitize_payload(body.payload)
        base = _base_config()
        binding = _validate_binding(payload, jira_base_url=base.jira_base_url)
        binding.enabled = body.enabled
        _validate_webhook_secret(binding, base)
        _validate_tracker_credentials(binding, base)
        conn = await conn_provider()
        async with lock:
            wgs = await _assemble_and_validate(conn, base, binding, exclude_id=None)
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
        _reject_disabled_write(body.enabled)
        payload = _sanitize_payload(body.payload)
        base = _base_config()
        conn = await conn_provider()
        async with lock:
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
            wgs = await _assemble_and_validate(conn, base, binding, exclude_id=binding_id)
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
