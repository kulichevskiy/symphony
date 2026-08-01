# Symphony verification kit V1

The kit runs reproducible live E2E experiments against two pinned Symphony system versions. A
system version is one Git commit plus one complete JSON profile. V1 uses EventDesk: a fixed seed
application and six sequential, complete Linear tickets.

## What one experiment does

1. Resolve each requested Git ref to a full commit SHA and snapshot both profiles and the harness.
2. Queue one smoke trial, then `A1, B1, A2, B2, A3, B3`.
3. For each trial, create a private `kulichevskiy/EXP-…` repository from the same EventDesk seed.
4. Create six uniquely titled BENCH issues with blocking relations and the stable routing label
   bound to that trial repository inside the isolated candidate DB.
5. Run that candidate's own `bench seed`, then keep one normal `symphony` daemon alive for the whole trial while the harness polls status and safety metrics.
6. Wait for all six issues to finish through implementation, local review, GitHub CI, remote Codex
   review, and merge. Stop on Needs Input or a safety cap.
7. Clone final `main`; run hidden product checks, all documented regression checks, and independent
   read-only Spec and Standards reviews.
8. Store raw receipts and render A/B means for completion, quality, time, tokens, agent launches,
   remote review rounds, and remote Codex findings by severity.

Only one experiment runs at a time. Submitted experiments remain queued. A worker restart cancels
active executor commands and marks the interrupted experiment failed instead of silently resuming a
partly mutated trial. Trial repositories and Linear issues are retained for inspection.

## Coolify deployment

The bench is a separate Coolify application with four long-running services and one init job:

- `init`: one-shot ownership setup for the shared named volumes;
- `worker`: control API, queue, SQLite state, OAuth snapshot/write-back;
- `executor`: candidate processes and graders on an isolated network and run volume;
- `connections`: the existing authenticated Connections UI, with no repository binding;
- `caddy`: the only public service.

Install a fresh [bench.env.example](bench.env.example) as `/opt/symphony-bench/.env` on the
Coolify host. It must be readable by container uid 1000 and must not reuse production's encryption
key or database. Put the same independently generated `SYMPHONY_BENCH_EXECUTOR_TOKEN` in both that
file and the local deploy environment.

Create the resource first so Coolify assigns a domain:

```bash
export COOLIFY_API_TOKEN=…
export SYMPHONY_BENCH_EXECUTOR_TOKEN=…
export COOLIFY_PREPARE_ONLY=1
scripts/deploy-bench-coolify.sh
```

The script sends API requests through the configured SSH host to Coolify's loopback API; it never
sends the bearer token over public HTTP. Add the printed origin to Auth0 and to the GitHub and Linear
OAuth callback allowlists, finish `/opt/symphony-bench/.env`, then deploy:

```bash
unset COOLIFY_PREPARE_ONLY
scripts/deploy-bench-coolify.sh
```

Open the generated domain, sign in, and connect GitHub, Linear, Codex, and every agent provider used
by the selected profile. The GitHub credential needs permission to create private repositories and
read/write PRs; the kit also configures branch protection when the plan supports it. The Linear
credential needs issue, state, and relation access to the BENCH team. Pre-create the stable
`symphony-bench` team label once and
set its id/name in the bench environment; trials do not need label-management permission.

GitHub Free does not expose branch protection for private personal repositories. In that exact
plan-limited case the kit keeps the repository private and relies on Symphony's own green-CI and
remote-review merge gates; other branch-protection failures remain fatal.

## A–A validation

Submit the same current revision and default profile on both sides:

```bash
export SYMPHONY_BENCH_URL=https://generated-bench-domain
export SYMPHONY_BENCH_TOKEN=…
uv run symphony verify submit --candidate-a main --candidate-b main --repetitions 3
uv run symphony verify status EXP-…
uv run symphony verify report EXP-… --output bench-report.md
```

The submit response is the immutable experiment id. Status and reports require the bearer token.
The report includes the smoke receipt but excludes it from A/B aggregate means.
