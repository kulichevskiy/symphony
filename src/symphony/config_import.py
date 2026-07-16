"""One-off importer: legacy `config.local.yaml` topology → SQLite (SYM-188).

Run once per environment at cutover. Reads a YAML document, converts the six
legacy top-level role fields into the roles matrix by applying the *existing*
legacy resolution over the operator-set fields only (so documented cross-field
inheritance — e.g. a codex reviewer inheriting an operator-set binding
`codex_model` — survives, while untouched cells stay absent as true inherit),
stamps `priority` from YAML list order, and writes the sparse binding payloads
plus the global roles matrix and the migration marker into the DB.

It refuses to double-import unless `replace=True` (which also serves the
export→restore path). A binding with `enabled: false` imports as a disabled
row: the `enabled` column has runtime semantics (dispatch skip, launch gate,
drain guard — SYM-193), so the row is stored disabled rather than refused.
Payloads are sparse and legacy-free by construction.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import aiosqlite
import yaml

from . import db
from .config import (
    _LEGACY_ROLE_FIELDS,
    Config,
    RepoBinding,
    RoleConfig,
    RoleName,
    Secrets,
    binding_natural_key,
)
from .config_export import WEBHOOK_SECRET_PLACEHOLDER
from .db.runs import LIVE_STATUSES

_ALL_ROLES: tuple[RoleName, ...] = (
    "implement",
    "review_find",
    "review_verify",
    "fix",
    "accept",
)


class ConfigImportError(RuntimeError):
    """Raised when the importer refuses to run (e.g. a double-import)."""


@dataclass(frozen=True)
class ImportResult:
    bindings: int
    replaced: bool
    runs_backfilled: int = 0
    prs_backfilled: int = 0


def _storage_key(binding: RepoBinding) -> str:
    """The binding's natural key as the JSON string runs/PRs are stamped with
    (`_binding_storage_key`) — same components, same order, same compact
    separators."""
    return json.dumps(list(binding_natural_key(binding)), separators=(",", ":"))


def _candidates(
    bindings: list[RepoBinding],
    *,
    team: str,
    provider: str,
    site: str,
    repo: str | None,
) -> list[RepoBinding]:
    """Bindings that could own an issue in `team`/`provider`/`site` (and `repo`
    when known). More than one means the row is ambiguous — the label that
    would disambiguate is not recoverable from the run/PR row, and the issue's
    current tracker labels are mutable, so it is never guessed."""
    out: list[RepoBinding] = []
    for b in bindings:
        if b.project_key != team or b.tracker_provider != provider or b.tracker_site != site:
            continue
        if repo is not None and b.github_repo != repo:
            continue
        out.append(b)
    return out


def _resolve_binding(
    candidates: list[RepoBinding],
    bindings: list[RepoBinding],
    mapping: dict[str, Any],
    identifier: str,
) -> RepoBinding | None:
    """The binding to stamp a row with: the sole candidate when unambiguous, or
    the operator's explicit `issue→natural-key` mapping entry otherwise.
    Returns `None` when the row can't be attributed without guessing."""
    if len(candidates) == 1:
        return candidates[0]
    target = mapping.get(identifier)
    if target is not None:
        want = tuple(target)
        for b in bindings:
            if binding_natural_key(b) == want:
                return b
    return None


async def _issue_repo(conn: aiosqlite.Connection, issue_id: str) -> str | None:
    """The single GitHub repo an issue has an open/known PR in, or `None` when
    there is none or more than one (leaving the repo dimension unconstrained)."""
    cur = await conn.execute(
        "SELECT DISTINCT github_repo FROM issue_prs WHERE issue_id = ?", (issue_id,)
    )
    repos = [str(row[0]) for row in await cur.fetchall()]
    return repos[0] if len(repos) == 1 else None


async def _backfill_binding_keys(
    conn: aiosqlite.Connection,
    bindings: list[RepoBinding],
    *,
    issue_bindings: dict[str, Any] | None,
) -> tuple[int, int]:
    """Stamp the dispatched binding key onto still-active run rows AND open-PR
    rows that carry an empty binding key (older-schema rows recovered only by
    team+repo, ambiguous once a repo has multiple label/catch-all bindings).

    Automatic only when unambiguous (one candidate binding). Ambiguous rows are
    never guessed from the issue's mutable tracker labels — a wrong key would
    let the real binding be deleted or route follow-ups to the wrong repo.
    Instead the importer refuses the cutover, listing the unattributable rows,
    unless the operator supplies an explicit `issue→binding` mapping (or drains
    them first). Runs inside the import transaction, so a refusal rolls the
    whole cutover back."""
    mapping = issue_bindings or {}
    refused: list[str] = []
    runs_done = 0
    prs_done = 0

    placeholders = ",".join("?" * len(LIVE_STATUSES))
    cur = await conn.execute(
        f"""
        SELECT r.id AS run_id, r.issue_id AS issue_id, i.identifier AS identifier,
               i.team_key AS team_key, i.provider AS provider, i.site AS site
          FROM runs r
          JOIN issues i ON i.id = r.issue_id
         WHERE r.binding_key = '' AND r.status IN ({placeholders})
        """,
        LIVE_STATUSES,
    )
    for row in await cur.fetchall():
        repo = await _issue_repo(conn, str(row["issue_id"]))
        cands = _candidates(
            bindings,
            team=str(row["team_key"]),
            provider=str(row["provider"]),
            site=str(row["site"]),
            repo=repo,
        )
        binding = _resolve_binding(cands, bindings, mapping, str(row["identifier"]))
        if binding is None:
            refused.append(f"run {row['issue_id']} ({row['identifier']})")
            continue
        await conn.execute(
            "UPDATE runs SET binding_key = ? WHERE id = ? AND binding_key = ''",
            (_storage_key(binding), row["run_id"]),
        )
        runs_done += 1

    cur = await conn.execute(
        """
        SELECT p.issue_id AS issue_id, p.github_repo AS github_repo,
               i.identifier AS identifier, i.team_key AS team_key,
               i.provider AS provider, i.site AS site
          FROM issue_prs p
          JOIN issues i ON i.id = p.issue_id
         WHERE p.binding_key = '' AND p.merged_at IS NULL
        """
    )
    for row in await cur.fetchall():
        cands = _candidates(
            bindings,
            team=str(row["team_key"]),
            provider=str(row["provider"]),
            site=str(row["site"]),
            repo=str(row["github_repo"]),
        )
        binding = _resolve_binding(cands, bindings, mapping, str(row["identifier"]))
        if binding is None:
            refused.append(f"PR {row['issue_id']} @ {row['github_repo']} ({row['identifier']})")
            continue
        await conn.execute(
            "UPDATE issue_prs SET binding_key = ? WHERE issue_id = ? AND github_repo = ? "
            "AND binding_key = ''",
            (_storage_key(binding), row["issue_id"], row["github_repo"]),
        )
        prs_done += 1

    if refused:
        raise ConfigImportError(
            "cannot attribute in-flight work to a single binding (ambiguous or "
            "unmatched): " + "; ".join(sorted(refused)) + ". Drain these rows "
            "first, or pass an explicit issue→binding mapping."
        )
    return runs_done, prs_done


def normalize_claude_model(model: str | None) -> str | None:
    """Map a full `claude-*` ID to its matrix alias where one exists; keep it
    verbatim otherwise (matrix validation accepts full `claude-*` IDs too)."""
    if not model or not model.startswith("claude-"):
        return model
    low = model.lower()
    for alias in ("opus", "sonnet", "haiku"):
        if alias in low:
            return alias
    return model


def _base_binding(raw_repo: dict[str, Any]) -> RepoBinding:
    """The binding with every legacy role field and its `roles:` block removed
    — i.e. what resolves from the global matrix + back-compat defaults alone."""
    stripped = {k: v for k, v in raw_repo.items() if k not in _LEGACY_ROLE_FIELDS and k != "roles"}
    return RepoBinding.model_validate(stripped)


def _baseline_model(
    base: RepoBinding,
    name: RoleName,
    agent: Literal["claude", "codex"],
    global_roles: dict[RoleName, RoleConfig],
) -> str | None:
    """What the reloaded (legacy-free) binding resolves a role's model to when
    no per-binding model cell is present: the global cell if set, else the
    back-compat default *for the operator's resolved agent* (not the base
    binding's default agent — the reloaded binding carries the emitted agent
    cell, so its model fallback follows that agent)."""
    g = global_roles.get(name)
    if g is not None and g.model is not None:
        return g.model
    return base._default_role_model(name, agent, global_roles)  # noqa: SLF001


def _sparse_matrix(
    operator: RepoBinding,
    base: RepoBinding,
    global_roles: dict[RoleName, RoleConfig],
) -> dict[str, dict[str, Any]]:
    """Per-binding matrix cells that differ between the operator's fully-resolved
    roles and the legacy/matrix-free baseline. A cell is emitted only when the
    operator's config actually moved it off the value the reloaded binding
    would otherwise resolve to; equal cells stay absent (true inherit), keeping
    the payload sparse and the binding open to future global edits."""
    matrix: dict[str, dict[str, Any]] = {}
    for name in _ALL_ROLES:
        op = operator.resolved_role(name, global_roles)
        baseline = base.resolved_role(name, global_roles)
        cell: dict[str, Any] = {}
        if op.agent != baseline.agent:
            cell["agent"] = op.agent
        base_model = _baseline_model(base, name, op.agent, global_roles)
        if op.model != base_model:
            cell["model"] = normalize_claude_model(op.model) if op.agent == "claude" else op.model
        if op.effort != baseline.effort:
            cell["effort"] = op.effort
        if cell:
            matrix[name] = cell
    return matrix


def build_payload(
    raw_repo: dict[str, Any],
    operator: RepoBinding,
    global_roles: dict[RoleName, RoleConfig],
) -> dict[str, Any]:
    """Sparse, legacy-free payload for one binding: the operator-set non-role
    fields verbatim, plus the consolidated roles matrix (if any). `enabled` is
    excluded — it lives in its own `config_bindings` column, not the payload
    (see the caller, which stamps that column from `binding.enabled`).
    `webhook_secret` is excluded too — it moves to the repo-scoped secret table
    (SYM-194), never a binding payload."""
    payload = {
        k: v
        for k, v in raw_repo.items()
        if k not in _LEGACY_ROLE_FIELDS
        and k not in ("roles", "review_strategy", "enabled", "webhook_secret")
    }
    if "review_strategy" in raw_repo:
        # Deprecated enum, already resolved into `operator.local_review` /
        # `remote_review` by the model validator; materialize the booleans so
        # the payload never re-fires the field's deprecation warning on load.
        payload.setdefault("local_review", operator.local_review)
        payload.setdefault("remote_review", operator.remote_review)
    matrix = _sparse_matrix(operator, _base_binding(raw_repo), global_roles)
    if matrix:
        payload["roles"] = matrix
    return payload


def _global_roles_dump(cfg: Config) -> dict[str, Any]:
    return {name: role.model_dump(exclude_none=True) for name, role in cfg.roles.items()}


def _load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text()) or {}


async def import_config(
    path: Path,
    conn: aiosqlite.Connection,
    *,
    replace: bool = False,
    updated_by: str = "importer",
    now: str = "",
    issue_bindings: dict[str, Any] | None = None,
) -> ImportResult:
    """Import `path`'s YAML topology into the DB. Idempotency is guarded: a
    second import without `replace=True` raises `ConfigImportError`."""
    existing = await db.config_bindings.count(conn)
    globals_row = await db.config_globals.get(conn)
    # Any existing globals row counts as "already touched", not just one with a
    # migration marker: a UI `PUT /api/config/roles` on a fresh DB creates the
    # row with `migrated_at=""` (see config_globals.update_roles), and a truly
    # fresh DB has no row at all — so `globals_row is not None` alone is the
    # correct guard.
    already_imported = existing > 0 or globals_row is not None
    if already_imported and not replace:
        raise ConfigImportError(
            "config already imported (DB has bindings or a migration marker); "
            "pass replace=True to overwrite"
        )

    raw = _load_yaml(path)
    # Validate the whole topology through the model (family checks, roles
    # matrix, states) without resolving env/secrets — the payload keeps env
    # *key names*, not values.
    cfg = Config.model_validate(raw)
    raw_repos: list[dict[str, Any]] = list(raw.get("repos", []) or [])
    # `model_validate` derives `tracker_site` with no global `jira_base_url`,
    # so a Jira binding relying on that global (no per-binding `base_url`)
    # would key on the "default" placeholder instead of the site it actually
    # resolves to at runtime — re-derive it here so the persisted natural key
    # matches `assemble_effective_config`'s resolution byte-for-byte.
    jira_base_url = Secrets().jira_base_url
    for binding in cfg.repos:
        binding.apply_tracker_secret_defaults(jira_base_url=jira_base_url)

    # The delete, every row insert, and the globals write land in one
    # transaction: a later failure (duplicate natural key, bad payload) rolls
    # back the whole import instead of leaving `--replace` having deleted the
    # current bindings with nothing written in their place.
    try:
        if replace:
            await conn.execute("DELETE FROM config_bindings")
            await conn.execute("DELETE FROM config_repo_secrets")

        for priority, (raw_repo, binding) in enumerate(zip(raw_repos, cfg.repos, strict=True)):
            payload = build_payload(raw_repo, binding, cfg.roles)
            await db.config_bindings.insert(
                conn,
                payload=payload,
                key=binding_natural_key(binding),
                enabled=binding.enabled,
                priority=priority,
                updated_at=now,
                updated_by=updated_by,
                commit=False,
            )

        # Per-binding YAML webhook secrets move into the repo-scoped table so
        # verification survives cutover without manual re-entry (SYM-194). The
        # secret is per repo, so two bindings on one repo collapse to one row
        # (last non-empty wins); the value never re-enters a binding payload.
        repo_secrets: dict[str, str] = {}
        for binding in cfg.repos:
            # An un-edited restore export carries the export placeholder, not a
            # real secret — skip it so verification stays cleanly unset (and
            # fails loudly) rather than trusting a bogus value (SYM-195).
            if binding.webhook_secret and binding.webhook_secret != WEBHOOK_SECRET_PLACEHOLDER:
                repo_secrets[binding.github_repo] = binding.webhook_secret
        for github_repo, secret in repo_secrets.items():
            await db.config_repo_secrets.set_secret(
                conn,
                github_repo=github_repo,
                secret=secret,
                expected_version=0,
                updated_at=now,
                updated_by=updated_by,
                commit=False,
            )

        await db.config_globals.set_globals(
            conn,
            roles=_global_roles_dump(cfg),
            migrated_at=now or "migrated",
            version=1,
            commit=False,
        )

        # Cutover backfill: stamp the dispatched binding key onto still-active
        # runs and open PRs so the drain guard and follow-up pollers can
        # attribute them. Refuses (rolling back the whole import) on any row it
        # can't attribute without guessing (SYM-195).
        runs_done, prs_done = await _backfill_binding_keys(
            conn, list(cfg.repos), issue_bindings=issue_bindings
        )
    except Exception:
        await conn.rollback()
        raise
    await conn.commit()
    return ImportResult(
        bindings=len(cfg.repos),
        replaced=replace,
        runs_backfilled=runs_done,
        prs_backfilled=prs_done,
    )
