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
  JSON payload column, plus `version`, `enabled`, `updated_at`,
  `updated_by`; and a single-document table for global config (the global
  roles matrix, migration marker) carrying the same `version` column so
  fleet-wide role edits get the same optimistic-locking protection as
  binding edits. A unique index on the binding's natural key rejects
  duplicates at the DB layer. The persisted key is byte-compatible with the
  orchestrator's existing binding-key tuple — same components (project key,
  github repo, issue label, tracker provider, tracker site — the registered
  provider name, not the concrete tracker type) *in the same order*, since
  persisted PR handoff keys and stale-key fallbacks already parse that
  layout positionally. The label is normalized to the empty string in the
  index (SQLite treats NULLs as distinct, so a nullable label column would
  let the common unlabeled catch-all binding be configured twice).
- **Selector disjointness, not just key uniqueness.** Dispatch matches
  issues by tracker scope and label, not by GitHub repo, so key uniqueness
  alone still allows two enabled bindings to compete for the same issues
  (same tracker scope + label with different repos, or an unlabeled
  catch-all overlapping every labeled binding in its scope). The write path
  validates selector disjointness among *enabled* bindings: two enabled
  bindings in the same tracker scope are rejected when their labels are
  equal or either is unlabeled. Disabled bindings are exempt, so an operator
  can stage a replacement binding before switching over. One ambiguity
  survives validation by nature: issues can carry multiple labels, so an
  issue tagged with two different bindings' labels matches both. That
  cannot double-dispatch (per-issue active-run dedupe already guarantees at
  most one live run per issue), but which binding wins must be deterministic
  — bindings are evaluated in stable natural-key order, and the spec
  documents that a multi-labeled issue goes to the first matching enabled
  binding in that order.
- **JSON payload, not normalized columns — and sparse.** The binding model
  has ~40 fields and grows steadily; nested structures (roles, states,
  acceptance, MCP servers) don't normalize well. Pydantic remains the single
  schema; adding a field requires no DB migration. The payload stores only
  operator-set fields (never a full dump with defaults materialized):
  a full dump would serialize legacy role defaults as if the operator had
  set them, tripping the existing legacy/matrix conflict guard on the next
  validation. The write path additionally rejects payloads that contain any
  legacy role field outright, so the DB stays legacy-free by construction.
- **YAML loses bindings entirely.** The config loader stops reading `repos:`
  and `roles:` from YAML — when the DB has bindings, their presence is
  ignored with a one-line startup warning; when the DB has none, it is a
  boot error (see the boot gate below). YAML keeps
  system-level knobs: ports, paths, poll/reconcile intervals, global caps,
  timeouts, UI thresholds. Those still require a restart to change, which is
  acceptable at their change frequency.
- **Migration is a manually-run importer**, executed once per environment at
  cutover. It reads a YAML document and converts the six legacy top-level
  role fields (agent, codex_model, reviewer_agent, reviewer_codex_model, and
  the two local-review claude model fields) into the roles matrix by
  applying the *existing legacy resolution logic over operator-set fields
  only*: a matrix cell is persisted when it derives from a field the
  operator actually set — including documented cross-field inheritance,
  e.g. codex review roles inheriting an operator-set binding codex model
  when no reviewer model is pinned — and left absent (true inherit)
  otherwise. Persisting the fully-resolved matrix would freeze every
  default as a per-binding override, cutting those bindings off from future
  global-matrix edits and making the UI's "inherit" display a lie; naive
  field-to-cell copying would drop the inheritance cases or fail family
  validation. It then inserts the bindings and global matrix into the DB,
  stamps the migration marker, and backfills the binding-key stamp onto any
  still-active run rows (resolvable issue→binding at import time), so the
  drain guard is correct for work that was dispatched by the pre-DB build.
  No auto-seed at boot. The same script doubles as the restore path for an
  export (refusing to overwrite existing rows unless explicitly told to
  replace them). The DB is legacy-free from day one, so the legacy/matrix
  conflict guard is not part of the UI path.
- **Boot gate against a zero-binding start over live work.** Loading zero
  bindings is only safe when there is nothing that needs a binding. If the
  DB has zero bindings but *does* contain unresolved work (active runs,
  tracked open PRs, or parked operator waits — all of which resolve their
  binding by iterating the loaded set), the daemon refuses to start with an
  error naming the migration/import script. The check deliberately does
  *not* key off the migration marker: a bad bulk delete or DB restore after
  a successful cutover leaves the same hazard, and the gate must catch that
  too. A second, independent gate covers the quiet-window cutover: if the DB
  has zero bindings but the YAML still contains an (ignored) `repos:`
  section, the daemon also refuses to start — booting "successfully" while
  silently dispatching nothing is the worse failure. A true fresh install —
  no unresolved work, no YAML topology — boots fine.
- **Hot-apply at the tick boundary.** The orchestrator re-reads *all*
  bindings — enabled and disabled — from the DB at the start of every poll
  tick (the daemon and the UI API share one process and one SQLite
  connection, so no IPC is needed). The contract is *per stage*, stated
  precisely: a running agent process is immutable once spawned (its argv and
  env were captured at dispatch), but each subsequent stage of the same
  issue — the next fix-run, the review monitor's decisions, the merge — uses
  the binding row current at that stage's start. Editing the merge strategy
  while a PR is in review affects the upcoming merge; that is intended
  behavior, not drift, and it avoids persisting per-run config snapshots.
  The one-shot tracker-queue scope prune becomes a reaction to the binding
  set changing rather than a boot flag. Two pieces of in-memory state must
  follow the reload: the tracker registry (built once at boot today) hot-adds
  a client when a binding introduces a provider/site context the process
  hasn't seen — keyed by the *full binding context* (for Jira that includes
  the project, not just provider/site, since the registry keys Jira trackers
  per project) and failing the save closed if the required tracker
  credentials are absent from the environment — and per-binding concurrency
  limiters are rebuilt when a binding's cap changes, so an edited
  `max_concurrent` actually takes effect instead of the first-use semaphore
  enforcing the old capacity forever. The same rule covers the webhook
  verifier: it is initialized from settings at app startup today, so a
  repo-secret replacement from the UI must hot-swap the verifier's view of
  repo secrets, or GitHub signs with the new secret while Symphony checks
  the old one until a restart.
- **Binding lifecycle.** New `enabled` flag on the binding: disabled means
  the *dispatch scan* skips it — no new issues start — while the binding
  stays loaded and visible to every follow-up poller (review monitors,
  merge-candidate polling, operator-wait resolution all locate their binding
  by iterating the loaded set), so in-flight work drains to completion
  instead of stalling. Disabling also clears the binding's tracker-queue
  lanes. Delete — and any edit that changes the natural key *or a
  branch-affecting field* (branch prefix, base branch: later stages resolve
  branches from the current row after hot reload, so changing these mid-PR
  would point fix, delivery, and reconciliation paths at a different branch
  than was dispatched) — is allowed only for a *drained* binding: no running runs, no tracked open PRs, no
  parked operator waits in its scope (a parked issue awaiting `$retry` or
  approval resolves its binding by the original natural key, so a rename
  would strand it), and no in-memory scheduled dispatch or fix-run slots —
  the daemon reserves a slot before the run row exists, so the guard must
  consult that reservation state too (same process, directly queryable) or a
  delete racing a scan could remove the binding a scheduled task is about to
  start with. Otherwise the API returns a conflict with the list of
  blockers. To attribute pre-PR work correctly, each run row records the
  binding key it was dispatched under: today a run before PR handoff is only
  traceable to its issue, not its binding, so a drain guard built on the
  current schema would either miss a live implement run or over-block every
  binding sharing the team. The stamped key also gives audit and the UI a
  stable answer to "which binding ran this".
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
  semantics), and saves fail closed listing available key names when a name
  is unknown. Resolution has two strictly separated read paths: the
  storage/API path always serves raw key names and never resolves; a
  resolved copy of the binding is materialized only at the moment a binding
  is handed to dispatch, and that copy is never written back or served. (The
  current loader resolves in place at boot; that in-place mutation goes
  away.) The webhook secret is stored *per GitHub repo*, not per binding —
  webhook signature verification is keyed by repo today, so two bindings on
  the same repo can only ever use one secret; per-binding storage would let
  the UI save a secret that verification never consults. The repo-secret
  record carries its own `version` participating in the optimistic-locking
  check (binding-row versions can't protect it: two tabs editing different
  bindings of the same repo would otherwise race on the shared secret
  without a conflict). The binding form
  surfaces it as the repo's secret (shared across that repo's bindings), and
  it is write-only: responses carry only a set/unset flag, and the advanced
  JSON view masks it too. Audit diffs redact secret-bearing fields
  unconditionally — the field-level diff logs only set/cleared/changed
  flags for them, never values, so routine secret rotation can't leak
  credentials into daemon logs. Update semantics preserve it across ordinary
  edits — an omitted or masked value means "keep the stored secret";
  replacing requires sending a new value and clearing requires an explicit
  clear marker, so a routine save of unrelated fields can never wipe or
  corrupt a repo's webhook secret.
- **Concurrency and audit.** Optimistic locking via a per-row version — the
  UI sends the version it loaded and receives a conflict on mismatch. Every
  write stamps `updated_at`/`updated_by` (email from the auth token) and
  logs a field-level diff to the daemon log. No role-based access control:
  every authenticated user is a config admin, matching the single-operator
  deployment model.
- **Roles matrix reaches every command path.** Editable agent/model/effort
  is only honest if every stage actually consumes it: today several dispatch
  paths still read the legacy per-binding fields (implementer agent/model on
  the binding, the local-review claude model fields), and the fix-runner
  command builder does not accept an effort flag. Part of this work is
  routing all five roles through the resolved-role lookup — implement,
  review_find, review_verify, fix, and accept command construction all take
  their agent, model, *and* effort from the matrix — and removing the
  legacy-field reads from dispatch paths. Otherwise a DB-only role edit
  saves successfully while some stages keep running old defaults.
- **One effective-config assembly for every consumer.** The composition
  "YAML system knobs + DB bindings + DB global matrix" lives in a single
  assembly step that every topology consumer goes through — the daemon, the
  UI API, *and* the non-daemon CLI paths (preflight checks, manual issue
  dispatch), which today assemble topology from the YAML loader and would
  otherwise silently operate over an empty binding set.
- **API surface.** REST under the config prefix: list/create bindings,
  get/update/delete a binding by id, get/put the global roles matrix (with
  the same version check), get options, and a YAML export for backup — the
  export always carries the global roles matrix alongside the bindings,
  since sparse binding payloads inherit from it and a bindings-only export
  would silently revert fleet-wide role defaults on restore.

## Testing Decisions

A good test asserts external behavior at the highest existing seam — the
HTTP response, the orchestrator's observable dispatch decisions, the DB rows
a script produces — never the internals that produce them.

- **Primary seam — HTTP API**, using the existing app test harness (real
  FastAPI app over a real temp SQLite): CRUD round-trips, duplicate natural
  key rejected — including two unlabeled bindings on the same project/repo —
  selector-overlap rejection (same tracker scope, equal labels or one
  unlabeled) with disabled bindings exempt, version-conflict on stale writes
  (bindings and the global matrix), drain guard returning the blocker list
  (runs attributed via their stamped binding key, PRs, operator waits,
  scheduled slots), repo-scoped webhook-secret masking on read,
  preserve-on-omit and write-only
  replace, fail-closed env-key validation, options payload, YAML export
  shape, field validation errors carrying paths, and the diversity warning
  as a non-blocking response element. Prior art: the existing `/api/issues`
  and command-endpoint tests.
- **Orchestrator seam**, using the existing orchestrator harness (real DB,
  mocked tracker): a binding inserted into the DB is scanned on the next
  tick; a disabled binding stops dispatching but stays visible to review and
  merge pollers and to operator-wait resolution, leaving in-flight work
  untouched; a binding-set change re-prunes tracker-queue scopes; a binding
  added with a previously unseen tracker provider/site context gets a
  hot-added registry client and is scanned; a `max_concurrent` edit is
  enforced by the rebuilt limiter on the next scheduling pass; the boot gate
  refuses a zero-binding start over live work but allows a fresh install.
  Prior art: the tracker-queue scan tests.
- **Migration seam**: the script as a callable — YAML fixture in, DB rows
  out, legacy fields resolved through the existing legacy-default logic
  (including the codex-reviewer-model inheritance case) rather than copied
  field-to-cell, refusal to double-import without the explicit replace flag,
  and a round-trip through export → import-in-replace-mode. Prior art:
  DAO-level tests over a temp DB.
- **Command-construction seam**: for each of the five roles, a matrix
  override of agent/model/effort is asserted against the actual argv the
  stage builds (including the fix and local-review paths that today read
  legacy fields, and the effort flag on every runner command). Prior art:
  the existing runner-command/prompt construction tests.
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
- The export endpoint serves two distinct recovery scenarios, and only one
  of them is "paste into YAML". (1) *Downgrade*: rolling back to a pre-DB
  build whose loader still reads `repos:` from YAML — there the export is a
  valid section to paste into the config file. Because the pre-DB build has
  no `enabled` semantics and would silently re-enable a paused binding, the
  downgrade export emits disabled bindings commented out with an explicit
  note, so re-enabling is a deliberate uncomment. (2) *Restore on the
  DB-backed build* (bad bulk edit, corrupted rows): the YAML is fed back
  through the migration/import script in replace mode, since the DB-backed
  loader ignores YAML bindings by design; disabled state round-trips intact
  here. In both cases the export includes the global roles matrix, and
  write-only webhook secrets are emitted as an explicit placeholder (never
  the stored value — the no-secrets-in-responses contract holds everywhere)
  and must be re-entered by hand; the export marks exactly which bindings
  need it.
- Production note: the deploy that ships this must be paired with running
  the migration script inside the container (the YAML is readable there).
  Order doesn't matter for safety — the boot gate refuses a zero-binding
  start while live work exists, so a deploy that lands before the migration
  fails loudly instead of orphaning in-flight issues — but doing both in one
  maintenance window avoids the crash-loop window entirely.
