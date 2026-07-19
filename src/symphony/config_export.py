"""Config export: DB bindings + global roles matrix → YAML backup (SYM-195).

A read-only, human-readable snapshot of the config DB. Since Config v2 9/9
there is no importer to feed it back through — YAML is no longer a config
source — so an operator restoring from a backup reads this file and recreates
the bindings on the Config page in the UI. Two shapes remain, both valid text:

  * ``restore`` (default): the full binding + roles document, disabled bindings
    emitted as real YAML with ``enabled: false`` so a snapshot reflects them.
    The install's actual ``db_path`` is stamped in for reference.
  * ``downgrade``: a ``repos:``/``roles:`` section shaped for a pre-DB build's
    loader. Disabled bindings are commented out with an explicit note.

Both shapes carry the global roles matrix (sparse binding payloads inherit from
it) and emit write-only webhook secrets as an explicit placeholder — never the
stored value (the no-secrets-in-responses contract holds everywhere). The
affected bindings are marked and the operator re-enters each by hand.
``mcp_servers`` entries carry their own literal credentials (a stdio server's
``env``, an http/sse server's auth ``headers``) with no such name-indirection —
those are redacted to per-key ``true`` the same way the loaded-config read view
redacts them (see ``ui.config_crud``), and re-entered by hand the same way.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal

import yaml

from .db.config_bindings import StoredBinding

# A sentinel the operator must replace by hand — and which the importer skips
# rather than storing, so an un-edited restore leaves the secret unset (which
# fails verification loudly) instead of installing this literal string.
WEBHOOK_SECRET_PLACEHOLDER = "__REPLACE_WITH_WEBHOOK_SECRET__"

# Sub-fields of an `mcp_servers` entry that may carry literal credential
# material: a stdio server's `env`, or an http/sse server's auth `headers`.
# Shared with `ui.config_crud`, which redacts the same fields for `GET`
# responses — one definition of "what counts as an mcp credential".
MCP_SECRET_SUBFIELDS = frozenset({"env", "headers"})

ExportMode = Literal["restore", "downgrade"]


def redact_mcp_servers(mcp_servers: dict[str, Any]) -> dict[str, Any]:
    """Replace each server's secret-bearing sub-field values with `True` (key
    names only, never values) so an export — or a `GET` response — never
    carries a literal `mcp_servers` credential."""
    redacted: dict[str, Any] = {}
    for name, entry in mcp_servers.items():
        if not isinstance(entry, dict):
            redacted[name] = entry
            continue
        out = dict(entry)
        for sub in MCP_SECRET_SUBFIELDS:
            sub_value = out.get(sub)
            if isinstance(sub_value, dict):
                out[sub] = {k: True for k in sub_value}
        redacted[name] = out
    return redacted


def _binding_dict(row: StoredBinding, *, mode: ExportMode, needs_secret: bool) -> dict[str, Any]:
    """The YAML mapping for one binding: its sparse operator-set payload, plus
    an ``enabled: false`` flag (restore mode only — downgrade comments the whole
    binding out instead) and a webhook-secret placeholder when the repo has one
    set. `mcp_servers` credentials (a stdio server's `env`, an http/sse
    server's auth `headers`) are redacted the same way as the loaded-config
    read view — never the stored value.

    A Jira binding with no explicit per-binding `base_url` has its
    `tracker_site` resolved from the global `JIRA_BASE_URL` at write time; that
    resolved value — not a recipe to re-derive it — is stamped in explicitly,
    since re-deriving from whatever `JIRA_BASE_URL` happens to be set at import
    time would silently change the natural key if it moved since export."""
    out: dict[str, Any] = dict(row.payload)
    mcp_servers = out.get("mcp_servers")
    if isinstance(mcp_servers, dict):
        out["mcp_servers"] = redact_mcp_servers(mcp_servers)
    if row.tracker_provider == "jira" and "base_url" not in out:
        out["tracker_site"] = row.tracker_site
    if mode == "restore" and not row.enabled:
        out["enabled"] = False
    if needs_secret:
        out["webhook_secret"] = WEBHOOK_SECRET_PLACEHOLDER
    return out


def _binding_block(d: dict[str, Any], *, commented: bool) -> str:
    """One binding rendered as a YAML list item indented under ``repos:``. When
    ``commented`` each line is prefixed with ``#`` so a downgrade-mode disabled
    binding survives as an inert, deliberately-uncommentable block."""
    dumped = yaml.safe_dump([d], sort_keys=False, default_flow_style=False, allow_unicode=True)
    lines = ["  " + ln if ln else ln for ln in dumped.splitlines()]
    if commented:
        lines = ["#" + ln for ln in lines]
    return "\n".join(lines)


def _binding_flow_line(d: dict[str, Any]) -> str:
    """One binding as a single-line flow-style mapping, comma-terminated, for
    the all-disabled-downgrade `repos: [...]` shape below — a block-style item
    can't be commented out under a `repos: []` scalar (YAML has already closed
    the value there), but a flow item on its own line inside `[...]` can, and
    uncommenting it individually still parses."""
    dumped = yaml.safe_dump(
        d, sort_keys=False, default_flow_style=True, allow_unicode=True, width=float("inf")
    ).strip()
    return f"#{dumped},"


_HEADERS: dict[ExportMode, list[str]] = {
    "restore": [
        "# Symphony config export — backup snapshot (mode: restore)",
        "# Read-only backup: recreate these bindings on the Config page in the UI.",
        "# `db_path` below is this install's actual DB path, carried for reference.",
        "# Webhook secret VALUES are never exported; bindings needing one carry a",
        f"# `webhook_secret: {WEBHOOK_SECRET_PLACEHOLDER}` placeholder — re-enter each by hand.",
        "# mcp_servers env/headers credential VALUES are never exported either; each",
        "# redacted key carries `true` in place of the value — re-enter each by hand.",
    ],
    "downgrade": [
        "# Symphony config export — mode: downgrade",
        "# Paste the repos:/roles: sections into config.local.yaml on a pre-DB build.",
        "# NOTE: disabled bindings are commented out — the pre-DB build has no `enabled`",
        "# semantics and would silently re-enable them. Uncomment deliberately to restore.",
        "# Webhook secret VALUES are never exported; re-enter each placeholder by hand.",
        "# mcp_servers env/headers credential VALUES are never exported either; each",
        "# redacted key carries `true` in place of the value — re-enter each by hand.",
    ],
}


def export_config(
    rows: Iterable[StoredBinding],
    global_roles: dict[str, Any],
    repos_with_secrets: set[str],
    *,
    mode: ExportMode = "restore",
    db_path: Path | None = None,
) -> str:
    """Render the effective config as a YAML backup document. Bindings are
    ordered by ``priority`` then natural key (the same stable order dispatch
    uses), so a snapshot reflects routing faithfully.

    ``db_path`` (restore mode only) is stamped into the document as a top-level
    field for reference — it records which DB this backup was taken from."""
    ordered = sorted(
        rows,
        key=lambda r: (
            r.priority,
            r.project_key,
            r.github_repo,
            r.issue_label,
            r.tracker_provider,
            r.tracker_site,
        ),
    )
    lines = list(_HEADERS[mode])
    if mode == "restore" and db_path is not None:
        lines.append(
            yaml.safe_dump(
                {"db_path": str(db_path)}, sort_keys=False, default_flow_style=False
            ).rstrip("\n")
        )
    has_live_item = any(not (mode == "downgrade" and not r.enabled) for r in ordered)
    if not ordered:
        lines.append("repos: []")
    elif has_live_item:
        lines.append("repos:")
        for row in ordered:
            commented = mode == "downgrade" and not row.enabled
            d = _binding_dict(row, mode=mode, needs_secret=row.github_repo in repos_with_secrets)
            lines.append(_binding_block(d, commented=commented))
    else:
        # Downgrade with every binding disabled: a block-style item commented
        # out under `repos: []` can never be validly uncommented (the value is
        # already closed at `[]`) — use a flow-style `repos: [...]` instead, so
        # each commented entry lives on its own line inside the brackets and
        # uncommenting it individually keeps the document valid.
        lines.append("repos: [")
        for row in ordered:
            d = _binding_dict(row, mode=mode, needs_secret=row.github_repo in repos_with_secrets)
            lines.append(_binding_flow_line(d))
        lines.append("]")
    roles_doc = yaml.safe_dump(
        {"roles": global_roles or {}}, sort_keys=False, default_flow_style=False, allow_unicode=True
    ).rstrip("\n")
    lines.append(roles_doc)
    return "\n".join(lines) + "\n"
