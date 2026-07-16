"""Config export: DB bindings + global roles matrix → YAML (SYM-195).

Two modes serve two distinct recovery scenarios (comments don't survive YAML
parsing, so one format can't serve both — see docs/config-db-ui-design.md):

  * ``restore`` (default): a document fed back through the import script in
    ``--replace`` mode on a DB-backed build. Disabled bindings are emitted as
    real YAML with ``enabled: false`` so the round-trip preserves them.
  * ``downgrade``: a ``repos:``/``roles:`` section to paste into
    ``config.local.yaml`` on a pre-DB build whose loader still reads them.
    Disabled bindings are commented out with an explicit note — the pre-DB
    build has no ``enabled`` semantics and would silently re-enable a paused
    binding, so re-enabling is a deliberate uncomment.

Both modes carry the global roles matrix (sparse binding payloads inherit from
it, so a bindings-only export would silently revert fleet-wide defaults on
restore) and emit write-only webhook secrets as an explicit placeholder — never
the stored value (the no-secrets-in-responses contract holds everywhere). The
affected bindings are marked and the operator re-enters each by hand. The
importer treats the placeholder as absence, so an un-edited restore never
installs a bogus secret (see ``config_import``).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Literal

import yaml

from .db.config_bindings import StoredBinding

# A sentinel the operator must replace by hand — and which the importer skips
# rather than storing, so an un-edited restore leaves the secret unset (which
# fails verification loudly) instead of installing this literal string.
WEBHOOK_SECRET_PLACEHOLDER = "__REPLACE_WITH_WEBHOOK_SECRET__"

ExportMode = Literal["restore", "downgrade"]


def _binding_dict(row: StoredBinding, *, mode: ExportMode, needs_secret: bool) -> dict[str, Any]:
    """The YAML mapping for one binding: its sparse operator-set payload, plus
    an ``enabled: false`` flag (restore mode only — downgrade comments the whole
    binding out instead) and a webhook-secret placeholder when the repo has one
    set."""
    out: dict[str, Any] = dict(row.payload)
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


_HEADERS: dict[ExportMode, list[str]] = {
    "restore": [
        "# Symphony config export — mode: restore",
        "# Feed back through `symphony config-import --config <this> --replace`.",
        "# Webhook secret VALUES are never exported; bindings needing one carry a",
        f"# `webhook_secret: {WEBHOOK_SECRET_PLACEHOLDER}` placeholder — re-enter each by hand.",
    ],
    "downgrade": [
        "# Symphony config export — mode: downgrade",
        "# Paste the repos:/roles: sections into config.local.yaml on a pre-DB build.",
        "# NOTE: disabled bindings are commented out — the pre-DB build has no `enabled`",
        "# semantics and would silently re-enable them. Uncomment deliberately to restore.",
        "# Webhook secret VALUES are never exported; re-enter each placeholder by hand.",
    ],
}


def export_config(
    rows: Iterable[StoredBinding],
    global_roles: dict[str, Any],
    repos_with_secrets: set[str],
    *,
    mode: ExportMode = "restore",
) -> str:
    """Render the effective config as a YAML document. Bindings are ordered by
    ``priority`` then natural key (the same stable order dispatch uses and the
    importer stamps priority back from), so a restore reproduces routing
    bit-for-bit."""
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
    # A downgrade with every binding disabled leaves only commented list items
    # under `repos:`, which YAML would read as null — emit an explicit `[]` then
    # so the document still parses as a valid (empty) `repos:` list.
    has_live_item = any(not (mode == "downgrade" and not r.enabled) for r in ordered)
    lines.append("repos:" if has_live_item else "repos: []")
    for row in ordered:
        commented = mode == "downgrade" and not row.enabled
        d = _binding_dict(row, mode=mode, needs_secret=row.github_repo in repos_with_secrets)
        lines.append(_binding_block(d, commented=commented))
    roles_doc = yaml.safe_dump(
        {"roles": global_roles or {}}, sort_keys=False, default_flow_style=False, allow_unicode=True
    ).rstrip("\n")
    lines.append(roles_doc)
    return "\n".join(lines) + "\n"
