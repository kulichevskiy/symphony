# Config in the DB, editable via UI

Spec for moving repo bindings and the roles matrix out of `config.local.yaml`
into SQLite, with full CRUD on the existing `/ui/config` page.

## Problem Statement

Changing which teams Symphony works, which agent/model/effort each pipeline
role uses, or a team's concurrency cap requires editing a YAML file that, in
production, is mounted read-only into the container from the host. The
operator must SSH to the host, edit `/opt/symphony/config.local.yaml` with
sudo, and restart the daemon. A restart mid-run is exactly the failure mode
that recently wedged an issue in a retry loop (interrupted rebase incident),
so every config tweak carries operational risk. The `/ui/config` page already
renders bindings and the resolved role matrix, but it is read-only — the
operator can see the config but cannot act on it.

## Solution

Bindings and the roles matrix move into SQLite as the single source of truth.
The existing `/ui/config` page grows from read-only cards into full CRUD: the
operator creates, edits, disables, and deletes bindings from the browser, and
picks agent, model, and effort per pipeline role from dropdowns whose allowed
values come from the backend. Changes are picked up by the orchestrator at
the next poll tick — no daemon restart, no SSH. The YAML file keeps only
system-level knobs (ports, paths, intervals, global caps); its `repos:` and
`roles:` sections are ignored entirely. A one-off migration script imports
the existing YAML topology into the DB.

## User Stories

1. As an operator, I want to see every binding with its repo, team, label,
   enabled state, and resolved role matrix on one page, so that I know at a
   glance what Symphony is configured to work on.
2. As an operator, I want to create a new binding from the UI, so that I can
   onboard a new team/repo pair without touching the production host.
3. As an operator, I want to edit an existing binding from the UI, so that
   routine changes (label, caps, review switches) don't require SSH + sudo +
   restart.
4. As an operator, I want to pick the agent (claude/codex) per pipeline role
   from a dropdown, so that I can rebalance the implement/review families in
   two clicks.
5. As an operator, I want to pick the model per role from a dropdown limited
   to that family's supported models, so that I cannot save a model the agent
   CLI would reject at dispatch time.
6. As an operator, I want to pick the reasoning effort per role from a
   dropdown limited to that family's supported efforts, so that I can trade
   cost for depth per stage without memorizing the valid values.
7. As an operator, I want an explicit "inherit" option in every role cell, so
   that I can tell an override apart from the global/back-compat default.
8. As an operator, I want to edit the global roles matrix on the same page,
   so that a fleet-wide default (e.g. all reviewers to a cheaper model) is
   one edit, not one per binding.
9. As an operator, I want my changes to take effect within one poll tick
   (~a minute) without a daemon restart, so that config changes stop being
   deploy-grade events.
10. As an operator, I want in-flight runs to keep the config they started
    with, so that editing a binding never yanks a model out from under a
    running agent.
11. As an operator, I want to disable a binding with a toggle, so that I can
    pause new dispatches for a team while its in-flight work finishes
    normally.
12. As an operator, I want deletion of a binding with active work to be
    rejected with the list of blocking issues/PRs, so that I cannot silently
    orphan open PRs mid-pipeline.
13. As an operator, I want edits to a binding's identity fields (team, repo,
    label, provider, site) to be guarded the same way as deletion, so that a
    rename cannot detach live work from its binding.
14. As an operator, I want validation errors shown inline on the exact form
    field, so that I can fix a bad value without decoding a server log.
15. As an operator, I want a non-blocking warning when my role choices put
    the reviewer in the same agent family as the implementer, so that I don't
    silently lose cross-family review diversity.
16. As an operator, I want rarely-used binding fields editable through an
    "advanced" JSON section, so that no field becomes uneditable after YAML
    stops carrying bindings.
17. As an operator, I want the dropdowns' allowed values served by the
    backend, so that adding a new supported model is a backend change and the
    UI can never offer a stale list.
18. As an operator, I want secret values never to appear in any config API
    response, so that opening the config page is safe on a shared screen.
19. As an operator, I want the binding's agent-env entries to reference
    secret key *names* validated against the server's env at save time, so
    that a typo'd key fails my save loudly instead of stranding a future run.
20. As an operator, I want to set or replace a binding's webhook secret
    without ever being able to read it back, so that write access doesn't
    imply read access to credentials.
21. As an operator, I want concurrent edits detected and rejected with a
    conflict error, so that two browser tabs cannot silently overwrite each
    other.
22. As an operator, I want each binding to record who last changed it and
    when, so that I can answer "why did the model change" without a separate
    audit system.
23. As an operator, I want a YAML export of the current bindings, so that I
    can keep a git-diffable backup and have an escape hatch if the UI is
    down.
24. As an operator, I want a one-off migration script that imports my
    existing YAML bindings (normalizing legacy role fields into the matrix),
    so that the cutover is one command with no manual re-entry.
25. As an operator, I want the daemon to boot and run correctly even if the
    YAML still contains `repos:`/`roles:` sections, so that the cutover
    doesn't couple deploy success to a manual file edit on the host.
26. As an operator, I want to see whether a binding currently has active
    work before disabling or deleting it, so that I can predict the effect
    of the action.
27. As an operator, I want creating a duplicate binding (same tracker
    project, repo, label, provider, site) to be rejected, so that the same
    issue can never be dispatched by two bindings at once.

## Implementation Decisions

- **Source of truth: SQLite.** Two new tables: one row per binding with a
  JSON payload column (the full binding document, validated by the existing
  pydantic binding model), plus `version`, `enabled`, `updated_at`,
  `updated_by`; and a single-document table for global config (the global
  roles matrix, migration marker). A unique index on the binding's natural
  key — (provider, tracker site, project key, github repo, issue label), the
  same tuple the orchestrator already uses as the binding key — rejects
  duplicates at the DB layer.
- **JSON payload, not normalized columns.** The binding model has ~40 fields
  and grows steadily; nested structures (roles, states, acceptance, MCP
  servers) don't normalize well. Pydantic remains the single schema; adding
  a field requires no DB migration.
- **YAML loses bindings entirely.** The config loader stops reading `repos:`
  and `roles:` from YAML — if present they are ignored (a one-line startup
  warning is logged for discoverability, nothing more). YAML keeps
  system-level knobs: ports, paths, poll/reconcile intervals, global caps,
  timeouts, UI thresholds. Those still require a restart to change, which is
  acceptable at their change frequency.
- **Migration is a one-off script**, run manually once per environment. It
  reads the YAML, normalizes the six legacy top-level role fields (agent,
  codex_model, reviewer_agent, reviewer_codex_model, and the two
  local-review claude model fields) into the roles matrix, and inserts the
  bindings and global matrix into the DB. No auto-seed at boot. The DB is
  legacy-free from day one, so the legacy/matrix conflict guard is not part
  of the UI path.
- **Hot-apply at the tick boundary.** The orchestrator re-reads enabled
  bindings from the DB at the start of every poll tick (the daemon and the
  UI API share one process and one SQLite connection, so no IPC is needed).
  Contract: a change affects the *next* dispatch; in-flight runs keep the
  configuration they captured at dispatch. The one-shot tracker-queue scope
  prune becomes a reaction to the binding set changing rather than a boot
  flag.
- **Binding lifecycle.** New `enabled` flag on the binding: disabled means
  no new dispatches, in-flight work finishes normally, and the binding's
  tracker-queue lanes are cleared. Delete — and any edit that changes the
  natural key — is allowed only for a *drained* binding: no running runs and
  no tracked open PRs in its scope; otherwise the API returns a conflict
  with the list of blockers.
- **Form scope.** The existing `/ui/config` page is extended in place:
  binding cards become editable (drawer/form), a create button and per-card
  enabled toggle and delete are added, and the global roles matrix gets its
  own editor card. The form gives dedicated widgets to the frequently-used
  fields (identity, tracker states, concurrency, review switches, merge
  strategy, verify command) and the 5-role × (agent, model, effort) matrix;
  every remaining field is editable in a collapsible raw-JSON section
  validated server-side by the same pydantic model. Legacy role fields are
  not exposed in the UI at all.
- **Options endpoint.** A read endpoint serves the allowed enum values —
  agent families, supported codex models, claude model aliases, both effort
  sets, merge strategies — so the frontend hardcodes nothing.
- **Validation.** On every write the server validates the payload through
  the binding model, then assembles the full effective config (YAML system
  knobs + all DB bindings + global matrix) and runs the existing model
  validators, so cross-binding checks and family checks behave exactly as
  they do at boot today. Validation errors return with field paths the form
  maps to inputs; the same-family review-diversity warning is returned as a
  non-blocking warning and rendered as a banner — the save succeeds.
- **Secrets.** Agent-env entries store secret key *names* only (unchanged
  semantics); resolution against the server env happens on each DB read, and
  saves fail closed listing available key names when a name is unknown. The
  webhook secret is write-only: responses carry only a set/unset flag; the
  advanced JSON view masks it too.
- **Concurrency and audit.** Optimistic locking via a per-row version — the
  UI sends the version it loaded and receives a conflict on mismatch. Every
  write stamps `updated_at`/`updated_by` (email from the auth token) and
  logs a field-level diff to the daemon log. No role-based access control:
  every authenticated user is a config admin, matching the single-operator
  deployment model.
- **API surface.** REST under the config prefix: list/create bindings,
  get/update/delete a binding by id, get/put the global roles matrix, get
  options, and a YAML export of the current bindings for backup.

## Testing Decisions

A good test asserts external behavior at the highest existing seam — the
HTTP response, the orchestrator's observable dispatch decisions, the DB rows
a script produces — never the internals that produce them.

- **Primary seam — HTTP API**, using the existing app test harness (real
  FastAPI app over a real temp SQLite): CRUD round-trips, duplicate natural
  key rejected, version-conflict on stale writes, drain guard returning the
  blocker list, webhook-secret masking on read and write-only replace,
  fail-closed env-key validation, options payload, YAML export shape, field
  validation errors carrying paths, and the diversity warning as a
  non-blocking response element. Prior art: the existing `/api/issues` and
  command-endpoint tests.
- **Orchestrator seam**, using the existing orchestrator harness (real DB,
  mocked tracker): a binding inserted into the DB is scanned on the next
  tick; a disabled binding stops dispatching but leaves the in-flight run
  untouched; a binding-set change re-prunes tracker-queue scopes. Prior art:
  the tracker-queue scan tests.
- **Migration seam**: the script as a callable — YAML fixture in, DB rows
  out, legacy fields normalized into the matrix, second invocation refuses
  to double-import. Prior art: DAO-level tests over a temp DB.
- **Frontend**, extending the existing ConfigPage vitest suite: form renders
  from fetched data, dropdowns populate from the options response, inherit
  vs. override display, warning banner, conflict and validation error
  rendering. Prior art: existing ConfigPage and HomePage tests.

## Out of Scope

- Editing YAML system knobs (ports, paths, intervals, global caps) from the
  UI — they stay file-based and restart-applied.
- Role-based access control or a separate admin list — all authenticated
  users may edit.
- Config version history, rollback UI, or a full audit table — `updated_by`
  plus log diffs only; history is a future slice if it hurts.
- Killing or migrating in-flight runs on config change — they always finish
  with the config they captured.
- Secrets management UI (editing `.env`) — secret values never enter the DB
  or the UI; only key names are referenced.
- Converting the webhook secret to an env-key reference — it keeps its
  current stored-value semantics, just write-only.
- A CLI importer beyond the one-off migration script.
- Multi-instance coordination — the design assumes the current
  single-process daemon+UI deployment.

## Further Notes

- The natural-key uniqueness plus the drain guard together mean the binding
  key stays stable for anything the daemon currently tracks, which is what
  keeps workspace paths, tracker-queue scopes, and issue storage ids
  coherent without a data migration.
- The export endpoint doubles as the rollback story: if the feature has to
  be reverted, the export is a valid `repos:`/`roles:` YAML section to paste
  back into the config file.
- Production note: the deploy that ships this must be followed by running
  the migration script inside the container (the YAML is readable there);
  until the script runs, the daemon sees zero bindings and idles — safe, but
  worth doing in the same maintenance window.
