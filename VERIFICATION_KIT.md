# Symphony verification kit V1

The kit compares two pinned Symphony system versions in a real Linear → worktree → local review →
GitHub CI → remote review → merge flow. A system version is one exact Git SHA, one complete JSON
profile, and the exact executor-toolchain receipt measured while its image is built.

V1 uses Feedback Inbox: one deliberately incomplete FastAPI/React seed and two complete dependent
Linear tickets. The backend ticket builds the SQLite feedback API. The frontend ticket builds the
operator inbox after the backend finishes.

## Lifecycle

1. Resolve A and B to full SHAs. Snapshot both profiles and every harness input: seed, two-ticket
   campaign, private reference implementation, hidden backend/frontend checks, manifest,
   regression commands, and Spec/Standards review prompts. Store a checksum.
2. Before creating the experiment, run a cheap grader preflight on both isolated Coolify lanes.
   Each lane must discover exactly 9 backend and 7 frontend hidden checks, pass 16/16 on the private
   reference, and produce the fixed 1/9 + 1/7 result on the incomplete seed. Missing dependencies,
   imports, result files, malformed JUnit/JSON, errors, skips, or wrong counts are
   `infrastructure_failed`; they are never recorded as product quality.
3. Run `A1 + B1`, then `A2 + B2`, then `A3 + B3`. Each pair starts concurrently on dedicated
   `bench-a` and `bench-b` executors. There is no S0 trial.
4. For each trial, create a private `kulichevskiy/EXP-…-{A|B}N` repository from the same seed and a
   human-readable Linear Project named `Feedback Inbox V1 · YYYY-MM-DD · <experiment suffix>`.
   Create the two uniquely titled BENCH issues in that Project with frontend blocked by backend.
5. Seed the candidate database with webhooks disabled, then keep one normal `symphony` daemon alive
   while the harness polls issue state and safety metrics.
6. Wait for both tickets to finish through implementation, local review, CI, remote Codex review,
   and merge. Stop on Needs Input or a safety cap.
7. Clone final `main`; inject the private backend and frontend checks; verify their exact manifest;
   run repository regression checks; then run independent read-only Spec and Standards reviews.
8. Persist `A1.md`, `B1.md`, and so on immediately after each trial. Each receipt contains status,
   duration, raw/cache/effective tokens, cost, local/remote review rounds and findings by severity,
   regression and hidden-check results, errors, and links. A durable `/notifications` outbox keeps
   the same Markdown until the chat acknowledges it.
9. Persist `FINAL.md` when the experiment completes or fails. Aggregate only matched completed A/B
   repetitions. Archive every GitHub trial repository after its receipt is collected.

The worker, not candidate code, reads candidate SQLite in read-only mode and computes comparable
metrics. Private reference/hidden controls never enter Git or either container image: the deploy
script uploads an ignored local bundle to `/opt/symphony-bench/controls-current`, mounted read-only
only by the worker. Frozen controls and archived receipts then live under `/data/db/bench-private`;
executors see only their active lane volume. A restart cancels active executor commands and marks
interrupted work failed. A deploy that changes the execution engine invalidates an already queued
frozen harness instead of silently mixing versions.

## Coolify deployment

The existing Coolify application has one init job and five services:

- `init`: ownership setup for named volumes;
- `worker`: public control API, queue, SQLite state, OAuth snapshot/write-back, reports;
- `bench-a` and `bench-b`: isolated candidate/grader executors with separate run volumes;
- `connections`: authenticated Connections UI without a repository binding;
- `caddy`: the only public service.

The worker mounts `/data/bench-a`, `/data/bench-b`, `/data/db`, and the host-only controls bundle.
Each executor mounts only its own lane. To keep hidden preflight inputs away from active candidates,
submit returns HTTP 409 while another experiment is queued or running. Do not create a separate
Coolify application per candidate.

Install [bench.env.example](bench.env.example) as `/opt/symphony-bench/.env` on the Coolify host.
It must be readable by container uid 1000 and must not reuse production's database or encryption
key. Use the same independently generated `SYMPHONY_BENCH_EXECUTOR_TOKEN` in the file and local
deploy environment.

```bash
export COOLIFY_API_TOKEN=…
export SYMPHONY_BENCH_EXECUTOR_TOKEN=…
scripts/deploy-bench-coolify.sh
```

The deploy script talks to Coolify's loopback API over SSH. Open the generated domain and connect
GitHub, Linear, Codex, and every agent provider used by the chosen profile. GitHub needs private
repository and PR access. Linear needs issue, Project, state, and relation access to BENCH. Keep the
stable `symphony-bench` label id/name in the environment.

The ignored local directories `src/symphony/bench/assets/feedback_inbox_reference/` and
`src/symphony/bench/assets/hidden/feedback_inbox/` are required to deploy. The script verifies and
uploads them before asking Coolify to build. They must never be added to Git.

GitHub Free cannot protect branches in private personal repositories. In that exact plan-limited
case the kit keeps the repository private and uses Symphony's green-CI and remote-review merge
gates. Other protection failures remain fatal.

## A–A validation

```bash
export SYMPHONY_BENCH_URL=https://generated-bench-domain
export SYMPHONY_BENCH_TOKEN=…
uv run symphony verify submit --candidate-a <exact-sha> --candidate-b <same-exact-sha> --repetitions 3
uv run symphony verify status EXP-…
uv run symphony verify report EXP-… --output bench-report.md
```

Submit returns an immutable experiment id only after both grader preflights pass. Status, report,
and notification endpoints require the bearer token.
