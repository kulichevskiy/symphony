# Config & Credentials v2 — env+DB single-source architecture

Spec for the second-generation configuration and credential architecture:
`config.local.yaml` dies, the DB becomes the sole source of all operational
config and all provider credentials, migrations become versioned and
crash-safe, and agent runs consume credentials through per-run materialization
instead of a shared HOME file.

Decided 2026-07-18 after a grilling session, motivated by the SYM-196..200
deploy incident (schema `table is locked` crash-loop, zero-bindings boot
refusal requiring a manual SSH `config-import`, missing
`SYMPHONY_ENCRYPTION_KEY` surfacing as silent OAuth 503s, and the Claude
credential poisoning loop that failed SYM-201 three times).

**Clean slate**: this design deliberately drops migration compatibility with
existing instances. Daemons are stopped for the cutover, the DB starts fresh
(run history is not preserved), and the operator reconnects providers and
recreates bindings through the UI. No deprecation windows, no data-import
shims from YAML or env.

## Problem Statement

The operator — today the author, tomorrow any team self-hosting Symphony —
cannot install or operate the product without SSH. Runtime config is split
across three sources (`config.local.yaml`, `.env`, DB tables) and provider
credentials across three more (env vars, mounted auth volumes, encrypted
`oauth_connections` rows). The seams between those sources are exactly where
production breaks:

- Deploying a new version applies schema DDL with no versioning, no lock
  discipline, and no backup: a container-overlap race during the 2026-07-18
  deploy left the daemon in a `database table is locked` crash-loop.
- The YAML→DB bindings cutover required a manual, undocumented
  `config-import` over SSH; until then the daemon refused to boot.
- A missing encryption key let the daemon boot fine and then 503 every
  OAuth connect attempt with no boot-time signal.
- Agent CLIs (claude/codex) authenticate off a shared HOME-volume file.
  Concurrent runs race each other's one-shot refresh-token rotation
  ("Not logged in" hot loop, 254 wasted runs), and after an auth failure the
  CLI deletes the file, the write-back no-ops, and the DB keeps a consumed
  refresh token that re-poisons every retry.

## Solution

One rule: **env is for bootstrap and secrets; the DB is for everything else;
nothing else exists.**

- `config.local.yaml` is deleted. Operational knobs (intervals, caps,
  timeouts, UI thresholds) move into `config_globals`, editable in the UI,
  hot-reloaded where safe. Paths and ports become env vars with container
  defaults. The `config-import` CLI is deleted.
- The encryption key auto-generates at first boot into the data volume
  (0600), an env override wins, and a key that cannot decrypt existing rows
  fails the boot loudly instead of 503ing at runtime.
- Schema changes ship as versioned migrations applied by a small runner:
  `BEGIN IMMEDIATE` + `busy_timeout` (container overlap becomes a wait, not
  a crash), a file backup of the DB before pending migrations apply, one
  commit. The daemon is the only migrator.
- Provider credentials (github/linear/claude/codex) live only in
  `oauth_connections`, managed on the Connections page. Agent runs consume
  them through per-run private config dirs materialized from the DB and torn
  down afterwards; the daemon proactively refreshes tokens (serialized) so
  runs never rotate refresh tokens themselves; the write-back survives as a
  compare-and-set safety net. An auth failure parks the issue with a clear
  reason and flips the Connections card to `expired` — never a retry loop,
  never a deleted row.
- Auth0 remains the mandatory operator gate.

The end state for a new operator: `docker compose up`, open the UI, log in
via Auth0, connect four providers, add a binding — no SSH, no config files,
no manual migration steps, ever.

## User Stories

1. As an operator, I want to install Symphony with `docker compose up` and
   an `.env` containing only Auth0 settings, so that first boot needs no
   other manual provisioning.
2. As an operator, I want the encryption key generated automatically on
   first boot, so that I never hand-craft key material.
3. As an operator, I want to override the encryption key via env when I keep
   secrets in a manager, so that the auto-generated file is optional.
4. As an operator, I want the daemon to refuse to boot loudly when the key
   cannot decrypt existing credential rows, so that a lost key is a boot
   error with instructions, not silent runtime 503s.
5. As an operator, I want to see the key fingerprint (never the key) in the
   UI, so that I can verify which key an instance runs.
6. As an operator, I want the daemon to start cleanly with zero bindings and
   zero connections, so that a fresh install lands in an empty-state UI
   instead of a boot refusal.
7. As an operator, I want to connect GitHub, Linear, Claude, and Codex from
   the Connections page, so that every credential enters the system through
   the UI.
8. As an operator, I want to disconnect a provider and know nothing else
   still holds that credential, so that revocation is real.
9. As an operator, I want Test on a Connections card to reflect the live
   token state, so that I can diagnose auth without reading logs.
10. As an operator, I want to create, edit, and delete bindings entirely in
    the UI, so that repo/team topology never requires SSH.
11. As an operator, I want to edit operational knobs (poll intervals, caps,
    timeouts, UI thresholds) in the UI, so that tuning does not require a
    file edit.
12. As an operator, I want knobs that genuinely need a restart clearly
    flagged in the UI, so that I know when a change is pending a restart.
13. As an operator, I want upgrades to apply schema migrations automatically
    at boot, so that deploying a new image is the whole upgrade procedure.
14. As an operator, I want the DB file backed up automatically before
    pending migrations run, so that a broken upgrade is recoverable by
    copying one file back.
15. As an operator, I want two overlapping containers during a deploy to
    serialize on the DB instead of crashing, so that Coolify's recreate
    choreography cannot brick the daemon.
16. As an operator, I want a run that hits an authentication failure to park
    the issue with a human-readable reason, so that I act once instead of
    watching a retry loop burn money.
17. As an operator, I want an auth failure to flip the provider's
    Connections card to `expired`, so that the fix (reconnect) is obvious
    and discoverable where I'd look first.
18. As an operator, I want every credential and config write stamped with
    who/what wrote it, so that I can audit how the instance got into its
    current state.
19. As the daemon, I want to be the sole schema migrator, so that UI pools
    and one-off containers never race DDL.
20. As the daemon, I want to refresh provider tokens proactively and
    serialized before dispatching runs, so that runs never perform refresh
    rotation themselves.
21. As an agent run, I want my own private credential directory materialized
    from the DB at spawn and torn down at exit, so that concurrent runs
    cannot race each other's credentials.
22. As an agent run, I want to inherit no ambient provider credentials from
    the daemon's environment or shared volumes, so that the DB is provably
    the only credential source.
23. As a concurrent pair of agent runs, we want token refresh owned by the
    daemon, so that one-shot refresh-token rotation cannot invalidate one of
    us mid-run.
24. As the daemon, I want the post-run write-back to be compare-and-set, so
    that a stale run result cannot clobber a reconnect that happened
    mid-run.
25. As the daemon, I want write-back to never delete or blank a connection
    row, so that a failed run cannot destroy the stored credential.
26. As an operator, I want Codex connected through a device-auth flow in the
    UI, so that Codex needs no container-side browser or port bridging.
27. As a teammate on the Auth0 allowlist, I want the entire UI gated, so
    that config and credentials are never exposed unauthenticated.
28. As an operator, I want config export from the UI, so that I can snapshot
    topology before destructive maintenance.
29. As a future contributor, I want every test to construct its DB through
    the migration runner, so that migrations are exercised by the whole
    suite for free.
30. As an operator, I want the deploy runbook to be "merge, redeploy,
    done", so that prod never again needs one-off containers or manual SQL.

## Implementation Decisions

Numbered decisions from the grilling session, in dependency order:

1. **Product shape: self-hosted, instance-per-team.** Single-tenant
   instances; SQLite stays; no tenancy layer. Multi-tenant SaaS explicitly
   rejected (revisit triggers: SaaS demand, a second writer process, HA
   requirements).
2. **YAML dies.** The config model assembles from env + DB only. Env carries
   bootstrap and secrets: Auth0 triple, optional webhook secrets, optional
   encryption-key override, paths/ports with container defaults. Everything
   operational is DB-owned and UI-editable. YAML parsing, the
   `config.local.yaml` mount, and the `config-import` CLI are deleted, not
   deprecated (clean slate).
3. **Encryption key auto-provisioning.** First boot generates the key into
   the data volume next to the DB, mode 0600. `SYMPHONY_ENCRYPTION_KEY` env
   overrides. Boot fails with a re-auth instruction when encrypted rows
   exist but the key cannot decrypt them. The key never logs; the UI shows a
   fingerprint only.
4. **Versioned migrations, hand-rolled runner.** A `schema_version` table
   and an ordered migrations directory (SQL files; a Python escape hatch for
   data moves). The runner takes `BEGIN IMMEDIATE` with a generous
   `busy_timeout`, copies the DB file aside before applying anything, applies
   pending migrations in order, commits once. It runs inside the DB connect
   path, so the daemon migrates at boot and every test exercises the runner
   by construction. Migration 001 is the full fresh schema; the legacy
   idempotent `executescript` + column-add `_migrate()` are deleted.
   External tools (alembic) rejected: without ORM models autogenerate offers
   nothing, and SQLite ALTER limits make it ceremony over the same
   hand-written SQL.
5. **Postgres deferred, ported cheaply later if ever.** Migrations are plain
   SQL without new SQLite-isms; all SQL stays inside the DAO layer, which
   remains the single porting boundary.
6. **Agent credentials: DB sole source, per-run materialization.** The
   per-run private-dir pattern (already used for git/gh/linear material)
   extends to agent CLIs: each claude run gets a private `CLAUDE_CONFIG_DIR`
   (codex: its HOME-equivalent) written from the decrypted
   `oauth_connections` row and torn down with the run. Shared HOME auth
   volumes stop being a credential source. The post-run write-back reads the
   per-run dir, not a global path.
7. **Central proactive refresh.** The daemon owns token refresh, serialized
   per provider, triggered when expiry falls within the maximum run
   wall-clock. Runs receive tokens fresh enough to never refresh themselves,
   which removes one-shot refresh-token rotation races by construction.
   Refresh goes through the existing OAuth provider abstraction (standard
   refresh grant, as Linear does today); an isolated one-shot CLI invocation
   is the documented plan B if a provider's token endpoint resists.
8. **Write-back becomes compare-and-set.** Kept as a safety net for any
   CLI-side refresh that slips through. It verifies the row still matches
   what the run started from before upserting (absorbs SYM-207), and it
   never deletes or blanks a row.
9. **Auth-failure semantics.** A run failing on authentication parks the
   issue as an operator wait with the reason, flips the connection row to
   `expired`, and surfaces it on the Connections card. No automatic retry
   of auth failures.
10. **Env credential fallbacks die.** With creds UI-only, the resolver's
    env/volume fallback paths and the ambient-env scrubbing concern
    (SYM-206) are deleted rather than patched. Runs receive only
    materialized credentials.
11. **Codex connect reshaped, not rebuilt.** SYM-201's device-auth login
    driver and pending-login registry survive as the connect front door;
    only the storage/consumption side changes to decisions 6–9.
12. **Auth0 stays mandatory.** Install docs cover creating the Auth0 app;
    built-in auth rejected for now.
13. **Model access control needs nothing new.** The roles matrix (agent,
    model, effort per role, per-binding overrides) already lives in the UI;
    connections cover account access. Budgets/model allowlists deferred.

## Testing Decisions

A good test drives external behavior through an existing seam and asserts
observable outcomes (HTTP responses, run env contents, DB rows, parked
waits, boot errors) — never internal call sequences. All six seams already
exist; this epic adds none:

1. **DB connect → migration runner.** Every test that opens a tmp DB runs
   the migrations, so schema correctness is suite-wide and free. Dedicated
   runner tests cover: pending-migration application order, backup file
   creation, crash-mid-migration recovery (backup restore), concurrent-open
   contention (busy wait, not error), version table integrity. Prior art:
   every existing DB test constructs via the connect path.
2. **`create_app` HTTP seam with fakes.** Connections cards, config CRUD,
   globals editing, key-fingerprint endpoint, empty-state behavior. Prior
   art: the claude OAuth router tests with a faked login factory and tmp DB.
3. **Orchestrator with a fake runner and tmp paths.** Materialization
   (private dir exists during run, gone after; env points at it), write-back
   CAS outcomes, auth-fail parking + row flip. Prior art: the existing
   orchestrator write-back tests.
4. **Login-process protocol fakes** for claude and codex connect flows.
   Prior art: existing pending-registry tests.
5. **OAuth provider + respx** for central refresh: near-expiry token
   triggers one serialized refresh; far-expiry does not; refresh failure
   falls to `expired` + park. Prior art: Linear's in-place refresh tests.
6. **Pure materialization function** (credentials in → dir + env out) tested
   directly. Prior art: existing `materialize_credentials` tests.

Boot-behavior matrix for key provisioning (no key + no rows → generate;
no key + encrypted rows → loud fail; env override → env wins; file present →
reuse) runs at the app/daemon boot seam.

## Out of Scope

- Postgres (deferred; triggers recorded in decision 5).
- Multi-tenant SaaS, RBAC, per-tenant encryption.
- Budget caps, spend dashboards, model allowlists (roles matrix suffices).
- Multi-account per provider (`oauth_connections` keeps one row per
  provider; new work must simply not cement the limit deeper).
- Built-in operator auth / generic OIDC (Auth0 stays).
- Data migration from existing instances: no YAML import, no env→DB
  credential import, no run-history preservation (clean slate).
- GitHub/Linear redirect-OAuth flows themselves (2/7, 3/7) — unchanged
  except fallback removal.
- Dashboard/lane behavior beyond auth-fail parking.

## Further Notes

- **Cutover runbook (this instance):** export config snapshot from the UI;
  stop daemon; remove the DB volume (fresh start) and the now-unused
  claude/codex auth volumes; deploy; connect four providers on the
  Connections page; recreate the six bindings in the UI; unpause.
- **Supersedes:** SYM-201 (reshaped per decision 11), SYM-206 and SYM-207
  (absorbed by decisions 10 and 8) — cancel with references to this spec.
- **Phasing** (strict `blockedBy` chain, one slice at a time, per the
  serialize-stacked-refactors rule): migration runner + baseline → key
  auto-provisioning → per-run claude materialization → central refresh →
  CAS write-back + auth-fail parking → codex connect on the new model →
  knobs to `config_globals` + UI → env-fallback removal → YAML removal.
- The 2026-07-18 incident memory (schema lock recovery, one-off-container
  discipline, Coolify recreate-on-stop behavior) documents why decisions 4
  and 15 look the way they do.
