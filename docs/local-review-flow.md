# Local-reviewer flow

A way to shorten the Review stage from hours to minutes by running the
reviewer **locally** in the same workspace as the implementer, instead
of round-tripping through the GitHub `@codex` bot for every iteration.

> **Status: shipped, default off.**
> Set `review_strategy: hybrid` (or `local`) on a binding to opt in.
> The default `remote` preserves today's `@codex review` behaviour.

**At a glance** — what to read first if you're new to this doc:

- *Why* — [The bottleneck](#the-bottleneck)
- *How* — [Proposal](#proposal), [Modes](#modes),
  [Gate semantics](#gate-semantics)
- *Operator workflow* — [Dry-run before flipping to
  `local`](#dry-run-before-flipping-to-local),
  [Configure a binding](#config), [Operator escape hatch:
  `$skip-local-review`](#operator-escape-hatch-skip-local-review),
  [Telemetry](#telemetry-symphony-runs-local-review-stats)
- *Safety properties* — [Cost accounting](#cost-accounting),
  [Iteration caps](#iteration-caps-local-vs-remote),
  [Risks and mitigations](#risks-and-mitigations)
- *Empirical validation* — [Marker contract: confirmed on both
  agents](#marker-contract-confirmed-on-both-agents),
  [CLI quirks](#cli-quirks-discovered-in-smoke-testing)

## The bottleneck

Today the Review stage looks roughly like:

```
implement → commit → push → open PR → post "@codex review" comment
   ↓
   wait for remote Codex bot to fetch + review + comment   (3–10 min)
   ↓
   orchestrator polls PR every poll_interval_secs=60s      (avg 30s lag)
   ↓
   classify (review_classifier.py) → if CHANGES_REQUESTED
   ↓
   dispatch local fix-run (claude/codex on workspace)      (1–5 min)
   ↓
   push → goto wait
```

Per round, the *non-fix* overhead is roughly:

| Step | Wall time |
| --- | --- |
| push + PR comment | 5–15 s |
| remote Codex bot review | 3–10 min |
| poll lag | 0–60 s |

That is ~5–11 minutes of overhead **before any local fix work happens**, on
every round. Real-world traces show this stage going 5+ hours with 30
rounds. The fix-run itself is rarely the slow part — the wait on the
GitHub-hosted reviewer is.

## Proposal

Add a `local_review` step that runs **between** implement (or fix) and
push:

```
implement → commit → LOCAL_REVIEW                  (loop inside workspace)
   ↳ APPROVED            → push → PR → optional final remote @codex → merge
   ↳ CHANGES_REQUESTED   → review_fix → commit → LOCAL_REVIEW (loop)
   ↳ cap-hit             → push → PR → Needs Approval (operator)
```

The reviewer runs against the workspace's branch via `git diff origin/<base>`
without leaving the box. No network wait, no 60s polling interval.

### Why this works

- The slowest signal in the current loop (Codex bot reviewing the diff) is a
  *function of the diff*, not a function of GitHub. Anything that can run
  that function locally — `codex exec review --base <base>` already does
  exactly this — collapses the wall time to model latency only.
- The existing pipeline already trusts a local agent to write the code;
  trusting a local agent to *read* it is the same threat model.
- The final remote `@codex review` can be kept as a single defense-in-depth
  pass before merge ("hybrid" mode), so we do not lose the second-pair-of-
  eyes property that motivated the current design.

### Reviewer choice

The strongest signal comes from a reviewer that is **different from the
implementer**. We default to:

| `binding.agent` (implement) | `reviewer_agent` (review) |
| --- | --- |
| `claude` | `codex` |
| `codex` | `claude` |

The operator can override per binding (e.g. `reviewer_agent: codex`,
`reviewer_model: gpt-5-codex`).

### Modes

Add `review_strategy` to the binding:

- `remote` — current behavior. Default during rollout for back-compat.
- `local` — only local reviewer runs; PR is opened and merged without
  consulting the remote bot. Suitable for low-risk repos.
- `hybrid` — local reviewer drives the iteration loop; once it approves,
  open the PR, post a single `@codex review`, and gate merge on its
  verdict. This is the recommended target state: fast iteration with
  one independent final check.

## Pieces

Four pure pipeline modules + an orchestrator helper, in order of
dependency:

### `pipeline/local_review.py` (pure)

- `local_review_prompt(...)` — review-instructions prompt that asks
  the reviewer to emit a single structured verdict.
- `build_local_review_command(*, agent, prompt, base_branch, codex_model,
  last_message_path)` — argv for `codex exec --sandbox read-only --json`
  or `claude --print --output-format stream-json`. `base_branch` is
  threaded into the prompt body, not forwarded as a flag (see [CLI
  quirks](#cli-quirks-discovered-in-smoke-testing)).
- `parse_local_review_output(*, agent, stdout, head_sha,
  last_message_file) → LocalVerdict` — `{kind: APPROVED |
  CHANGES_REQUESTED | UNPARSEABLE, findings, trigger_signature}`.
  Signature is keyed on `head_sha + sha256(findings)` so the existing
  `should_dispatch_fix_run` dedup gate works across fix-runs.

### `pipeline/local_review_loop.py` (pure)

The iteration policy — how many rounds, when to dedup, when to
escalate — lives here behind injected async callbacks so it can be
unit-tested without a Runner. Outcomes:

- `APPROVED` → push, open PR; in `local` mode skip the `@codex` ping.
- `EXHAUSTED` → push, escalate to Needs Approval. The final fix-run
  is applied even though it's unverified — the unverified-fixed
  branch state is a better handoff than the known-broken one.
- `STUCK_LOOP` → reviewer fixated on the same trigger twice in a row
  (same `head_sha + findings` signature). Escalate immediately.
- `COST_CAP_BREACHED` → `prior_cost + session_cost >= cap`. Mid-loop
  abort. See [Cost accounting](#cost-accounting).
- `SKIPPED` → operator posted `$skip-local-review`. See
  [escape hatch](#operator-escape-hatch-skip-local-review).
- `REVIEWER_FAILED` → reviewer process died or emitted no verdict
  marker. Treat as escalation; never silently approve.
- `FIX_RUN_FAILED` → fix-run subprocess died.

### `pipeline/local_review_io.py` (adapter)

`collect_runner_output(runner, spec, *, usage_handler=None)` drives the
`Runner` protocol to completion, returns a single string of stdout +
terminal-kind. The optional `usage_handler` is called on every parsed
`Usage` event so a `UsageCostEstimator` can bill the subprocess
against the issue's cost cap.

### `pipeline/local_review_session.py` (integration)

`run_local_review_session(...)` ties prompt + command + IO adapter +
loop together behind the orchestrator's existing `Runner` and the
caller's `head_sha_provider`. Owns the per-iteration callback
plumbing (`should_skip`, `on_iteration`, `report_active_run_id`) and
the cost-estimator lifetimes (one per reviewer, one per fixer,
shared across iterations so codex's cumulative-token invariant
holds).

### Orchestrator wire-in

`poll.py::Orchestrator._run_local_review_phase` is the entry point
`_dispatch_one` calls between Implement-success and push when
`binding.review_strategy != "remote"`. It:

1. Resolves `base_branch`, reviewer agent, model, caps, prior cost.
2. Creates a `stage='local_review'` runs row (for audit + cost
   tracking).
3. Posts the "starting" Linear comment.
4. Invokes `run_local_review_session(...)`.
5. Finalizes the runs row with cost + mapped status in a `finally`.
6. Returns the `LoopResult` for the gate function to decide whether
   to ping `@codex`.

### Config

Per-binding (full set of local-review knobs):

```yaml
- linear_team_key: ENG
  github_repo: org/api-svc
  agent: claude
  review_strategy: hybrid              # remote | hybrid | local
  reviewer_agent: codex                # default: opposite of `agent`
  reviewer_codex_model: gpt-5.1-codex  # default: inherit `codex_model`
  local_review_iteration_cap: 4        # default: inherit global (6)
  post_local_review_pr_summary: true   # default: inherit global (true)
```

Top-level config defaults:

```yaml
local_review_iteration_cap: 6
post_local_review_pr_summary: true
```

Global default `review_strategy: remote` preserves today's behaviour
on every binding that doesn't opt in.

## Expected wins

- Eliminate the 3–10 min remote-bot wait per round → most of the wall
  clock disappears.
- Eliminate the 60s `poll_interval_secs` lag per round → events are
  subprocess exits, immediate.
- Per-round budget shrinks from ~10 min to ~1–3 min (model latency only).
- 30-round runs that today take 5h could complete in ~30–60 min, or
  finish in far fewer rounds because the reviewer can be retriggered
  without waiting on a queue.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Local reviewer "approves its own work" if it shares context with implementer | Different agent family by default (`claude` ⟷ `codex`). The reviewer is a fresh subprocess; no chat history is shared. |
| Reviewer hallucinates a finding → wastes a fix-run | `should_dispatch_fix_run` dedup is already in place; same identical trigger does not retry. Cap still applies. |
| Loses the auditable PR-comment trail | Hybrid mode keeps the final `@codex review` PR comment. We also persist local-reviewer verdicts in the existing `runs` table so the timeline survives. |
| Reviewer cost explodes on a huge diff | Reuse `cost_guard` budget per issue; the existing `cost_cap_per_issue_usd` already gates this. |

## Marker contract: confirmed on both agents

Real-CLI smoke runs against a scratch repo with a planted divide-by-zero
bug, prompted against the production `local_review_prompt`:

- **codex** (`codex exec --sandbox read-only --json -o file PROMPT`):
  emitted `<<<VERDICT:CHANGES_REQUESTED>>>` after a `## Findings` block
  citing `add.py:4-6` and noting missing tests. Parser extracted
  `kind`, `findings`, and `trigger_signature` cleanly.
- **claude** (`claude --print --output-format stream-json --verbose PROMPT`):
  same marker, three findings (the bug, missing tests, plus a flagged
  "# Bug: no zero check" code smell). Stream-json terminal `result`
  event picked up by the parser without changes.

Neither agent needed initial prompt tuning. Both followed the verdict-
marker contract on the first attempt.

### Prompt tightening (iter 16)

Re-smoking after the iter-16 prompt revision showed measurable improvement
on the same divide-by-zero scratch repo:

| Aspect            | Before                 | After (iter 16)              |
| ----------------- | ---------------------- | ---------------------------- |
| Finding count     | 3 (bug + tests + smell)| 1 focused finding            |
| Location citation | `a.py:4-6` (range)     | `add.py:6` (single line)     |
| Findings shape    | free-form bullets      | `path:line - what/fix`       |
| Narration of ref  | "I used main...HEAD"   | (silent fallback)            |

The "junior engineer" framing in the prompt produces tighter findings:
the reviewer writes only what the fix-run agent needs, not a complete
PR review. That keeps the next iteration focused and converges faster.

## CLI quirks discovered in smoke testing

The codex CLI as of v0.130 imposes two constraints worth knowing:

1. **`codex exec review --base <BRANCH>` is mutually exclusive with `[PROMPT]`.**
   The review subcommand's intent is "pick a diff scope (`--base`,
   `--commit`, or `--uncommitted`) OR provide custom review
   instructions — not both." We pass our own prompt, so `--base` is
   not forwarded as a flag; the base branch is threaded into the
   prompt body where the agent runs `git diff origin/<base>...HEAD`
   itself.
2. **`codex exec review` ignores custom output schemas.** It imposes
   its own `[Pn] title — path:line — body` review-comment format and
   the verdict marker contract is dropped. We use plain `codex exec
   --sandbox read-only` instead — same model, full prompt control,
   verdict marker preserved.

The `read-only` sandbox is non-negotiable: the reviewer must not write
the working tree (a fix-run would be ambiguous with a "modify during
review" hallucination). The verdict prompt also says "Do NOT modify
any files" as defense in depth.

If `binding.reviewer_codex_model` resolves to a model the operator's
ChatGPT account does not support (e.g. `gpt-5.1-codex` on a free
account), codex 0.130 returns HTTP 400 with `invalid_request_error`.
The local-review path treats this as `REVIEWER_FAILED` and falls back
to the remote `@codex` bot — no dead end — but the binding should be
updated to a supported model to actually get the local-review speed
win.

## Heartbeat: per-iteration Linear comments

A local-review pass can run 5–15 minutes end-to-end across multiple
review/fix iterations. Without visibility, that feels broken to a
watching operator. The orchestrator posts:

- One **starting** comment when the local-review phase begins,
  showing strategy, reviewer agent, and cap.
- One **per-iteration** comment after each verdict is parsed
  ("`Local review iter 0: changes_requested`") with the first finding
  line and the running cost.
- One **final** outcome comment with the total cost and last findings
  (already shipped in iteration 7).

The per-iteration comments fire via an `on_iteration` async callback
that the loop invokes right after `parse_local_review_output`,
*before* the cap / skip / fix-dispatch checks. That ensures the
operator sees the last verdict even when the loop is about to exit on
cap breach, stuck loop, or skip — the heartbeat is the audit trail.

Callback exceptions are swallowed inside the loop: a flaky Linear post
must not kill the local-review pipeline. The orchestrator's helper
already logs the LinearError, so swallowing is safe.

## Operator escape hatch: `$skip-local-review`

An operator can interrupt an in-flight local-review by posting
`$skip-local-review` on the Linear issue. Behavior:

- The orchestrator sets a per-issue flag; the next iteration boundary
  in `run_local_review_loop` sees it and returns
  `LoopOutcome.SKIPPED`. The in-flight subprocess (if any) finishes
  naturally — bounded by `stall_secs`, not by `cap × stall_secs`.
- `SKIPPED` falls back to the remote `@codex review` via the gate
  function, so the issue still gets a verdict; nothing dead-ends.
- The slash is a no-op when no local-review is in flight; we post a
  `command_rejected("$skip-local-review", "no active local-review phase")`
  comment so the operator sees the rejection rather than silence.
- The slash is *eligible* only while the local-review phase is
  running: the orchestrator keeps the implement run_id in
  `_active_run_ids` for the duration so `_poll_slash_commands` picks
  up the comment.

**Mid-subprocess kill (landed in iteration 10).** When the slash
arrives while a reviewer or fixer is actively running, the orchestrator
calls `self._runner.kill(active_run_id)` on the in-flight subprocess
and the loop exits within seconds instead of waiting up to
`stall_secs`. The session reports its active run_id to the orchestrator
via a `report_active_run_id` callback bracketed around every reviewer
and fixer call (set before, cleared after via `try/finally` — even on
runner exceptions). The skip flag is set *before* the kill so the loop
classifies the resulting failed-subprocess terminal as `SKIPPED`
rather than `REVIEWER_FAILED` / `FIX_RUN_FAILED` — the audit trail
shows operator intent, not "the reviewer crashed."

## GitHub PR summary on approval

When the local-review APPROVES, the orchestrator posts a short summary
comment to the GitHub PR thread so reviewers visiting GitHub (not just
Linear) see the verdict trail:

```
**Symphony local reviewer (codex) approved this PR.**

- iterations: 3
- cost: $0.1823
- strategy: `local`
```

The intent is not to replace human review — it's to give a human
reviewer enough context to decide "I trust this and will skim" vs.
"I'll review carefully." The comment fires only on `APPROVED` outcomes
(failures already have the `@codex review` fallback ping as audit
trail).

Disabling: `post_local_review_pr_summary: false` in the top-level
config turns it off globally. Each `RepoBinding` can override with its
own `post_local_review_pr_summary: true | false` — useful when one
repo's PR thread is already wired to a GitHub-side dashboard and the
extra comment is noise, while sibling repos want every verdict
surfaced. Per-binding override wins over the global value either way.

## Iteration caps: local vs. remote

`review_iteration_cap` and `local_review_iteration_cap` are now
separate. Defaults:

| Knob                          | Default | Why                                       |
| ----------------------------- | ------- | ----------------------------------------- |
| `review_iteration_cap`        | 12      | Remote `@codex` loop tolerates many rounds (each ~30 s + bot wait) |
| `local_review_iteration_cap`  | 6       | Local loop converges fast or it's stuck; high caps just burn cost |

Per-binding overrides are supported via
`RepoBinding.local_review_iteration_cap`. `None` falls back to the
global default. The Pydantic validator enforces `ge=1` — a 0/negative
cap would never enter the loop and is almost certainly a typo.

Tuning advice:

- Drop to `2–3` on bindings where you've configured
  `review_strategy: local` and want fast fallback to the remote bot
  when the local pass is stuck.
- Raise to `8–10` on bindings with `review_strategy: hybrid` where
  the local pass is just an optimization on top of the remote bot
  and you can afford more rounds before giving up.
- The cost cap still applies — `local_review_iteration_cap × stall_secs
  × max-spend-per-iteration` is the upper bound on local-review work
  per issue, regardless of cap.

## Dry-run before flipping to `local`

Before changing a binding's `review_strategy` from `remote` to `local`,
sanity-check the reviewer on a real workspace of yours:

```
symphony local-review-dry-run \
  --workspace ~/code/api-svc \
  --base main \
  --reviewer codex \
  --title "Fix oauth refresh bug" \
  --body "Tokens expire 30s early; refresh window is wrong." \
  --label bug
```

What it does:

- Reads `git diff origin/main...HEAD` (silent fallback to
  `main...HEAD`) in the named workspace.
- Builds the production prompt with the supplied issue context.
- Invokes the reviewer agent the same way `_dispatch_one` does
  (`codex exec --sandbox read-only` or `claude --print`).
- Prints the verdict (`approved` / `changes_requested` / `unparseable`)
  and the parsed findings.

What it does NOT do: write to SQLite, post Linear comments, create a
`runs` row, or touch GitHub. It's read-only against the workspace, so
operators can run it freely against any branch — including PRs already
merged, to spot-check what the reviewer *would have* said.

Omit `--reviewer-model` to use the operator's account default model;
specifying a model the account doesn't support produces an HTTP 400
from codex (iter 5 lesson). Use `--reviewer claude` to swap reviewers.

## Telemetry: `symphony runs local-review-stats`

Once a few local-review sessions have run, operators can answer "is
this actually saving time?" without writing SQL:

```
$ symphony runs local-review-stats --db ~/symphony/state.sqlite
completed (APPROVED):    14
interrupted (SKIPPED):   1
failed (other):          3
running (in-flight):     0
approval rate:           77.8%
total cost:              $3.4521
avg cost per session:    $0.1918
avg duration per session: 92.4s
```

- `approval rate` = `completed / (completed + interrupted + failed)`
  — the share of sessions where the local pass converged. Higher means
  the local reviewer is doing real work; lower means most reviews fall
  back to the remote bot anyway, and `review_strategy: hybrid` might
  be a safer default for this binding.
- `avg duration` is the *whole local-review phase* — multiple
  reviewer/fixer iterations summed. Compare against the historical
  remote-bot wall time (5–10 min per round × up to 30 rounds) to see
  the actual speedup.
- `running` is in-flight at query time; `cost_for_issue` already
  includes those rows' `cost_usd` (which is $0 until finalization).

Implementation: pure SQL aggregation in `db.runs.local_review_stats`,
filtered to `stage='local_review'`. Computes counts by status, total
cost, avg cost (over finished rows), avg duration via
`strftime('%s', ended_at) - strftime('%s', started_at)`, and approval
rate. Implement/review/merge rows are filtered out — they don't
pollute the local-review numbers.

## Postmortem: `symphony runs local-review-trace`

When a specific issue's local-review didn't behave as expected, drill
in to its history:

```
$ symphony runs local-review-trace ENG-123 --db ~/symphony/state.sqlite
local-review runs for ENG-123 (3 total):
started_at                       status        cost      duration  id
2026-05-14T15:30:00+00:00        completed     $0.1812     60.0s   3f8b…
2026-05-14T14:11:00+00:00        failed        $0.5000    120.0s   a91c…
2026-05-14T11:02:00+00:00        interrupted   $0.0500     20.0s   77d2…
```

Newest first. `failed` rows include the bookkeeping (cost burned,
how long the loop went) before the operator-visible Linear comment
trail. Run-row IDs link back to `symphony runs show <id>` for the
full row including PID and ended_at timestamp. `interrupted` means
`$skip-local-review` from Linear fired during that session.

## Run-history persistence

The local-review phase persists a single `runs` row with `stage='local_review'`
across each session, regardless of how many reviewer/fixer iterations
run inside. Status mapping:

| `LoopOutcome`        | `runs.status`  |
| -------------------- | -------------- |
| `APPROVED`           | `completed`    |
| `SKIPPED`            | `interrupted`  |
| every other outcome  | `failed`       |
| session raised       | `failed`       |

`cost_usd` on the row is `LoopResult.total_cost_usd` (reviewer + fixer
subprocess costs summed across iterations). That makes the local-review
cost participate in:

- `db.runs.cost_for_issue(...)` — future stages (review, merge) and
  re-dispatches see the full historical cost.
- `cost_cap_per_issue_usd` / per-binding `cost_cap_usd` — once the
  local-review cost is persisted, downstream stages see the running
  total correctly.
- Operator history queries — `history_for_issue` shows
  implement → local_review → review → merge as separate rows.

The `prior_cost_usd` passed *into* the local-review session reads
`cost_for_issue` *before* the new row is created, so the in-loop cap
check uses the implement cost alone — the just-created row would
contribute $0 and just add noise.

## Cost accounting

The reviewer and fixer subprocesses count against the same per-issue
cost cap (`cost_cap_per_issue_usd` / per-binding `cost_cap_usd`) as the
Implement stage. `run_local_review_session` accepts `prior_cost_usd`
(the issue's running total) and `cost_cap_usd` (the effective cap),
checks both after every reviewer/fixer subprocess, and short-circuits
with `LoopOutcome.COST_CAP_BREACHED` the moment `prior + session_total
>= cap`. The cap check sits *between* reviewer parse and fixer
dispatch, so the loop never pays for a fix-run when the reviewer alone
already tipped the budget.

`UsageCostEstimator` (formerly the private `_UsageCostEstimator` in
`poll.py`, now public in `pipeline/cost_guard.py`) is shared across all
reviewer subprocess calls (and likewise across all fixer calls) so the
codex cumulative-token invariant holds — successive iterations only
charge for *new* tokens, not for the whole turn history.

A `COST_CAP_BREACHED` outcome falls back to the remote `@codex` review
the same way other non-APPROVED outcomes do. The remote bot is hosted
by OpenAI and does not consume the per-issue cap, so it's a free
safety net once the local pass has spent budget.

## Gate semantics

The decision of whether to ping the remote `@codex` bot after opening
the PR is encoded in [`_should_post_codex_review`](../src/symphony/orchestrator/poll.py):

| `review_strategy` | local outcome           | post `@codex review`? |
| ----------------- | ----------------------- | --------------------- |
| `remote`          | (no local pass)         | yes                   |
| `hybrid`          | any                     | yes (defense in depth) |
| `local`           | `APPROVED`              | **no** (local is authoritative) |
| `local`           | `EXHAUSTED`             | yes (safety net)      |
| `local`           | `STUCK_LOOP`            | yes (safety net)      |
| `local`           | `COST_CAP_BREACHED`     | yes (safety net)      |
| `local`           | `SKIPPED` (operator)    | yes (safety net)      |
| `local`           | `FIX_RUN_FAILED`        | yes (safety net)      |
| `local`           | `REVIEWER_FAILED`       | yes (safety net)      |
| `local`           | None (exception)        | yes (safety net)      |

`local` mode is best read as "trust the local reviewer when it
converges; fall back to the bot when it doesn't." That's the cheapest
way to deliver the speed win without losing the second-pair-of-eyes
guarantee on the cases where the local pass already gave up.

## Rollout

**All engineering work is landed; default behaviour is unchanged.**
What an operator does to enable it for a real binding:

1. Run [`local-review-dry-run`](#dry-run-before-flipping-to-local)
   against a representative workspace + branch + issue context. Eyeball
   the verdict and findings.
2. In `config.yaml`, set `review_strategy: hybrid` on that one binding.
   `hybrid` keeps the `@codex review` safety net; you only lose the
   bot when promoting to `local` later.
3. Reload symphonyd (or wait for the next deploy).
4. After a handful of issues run through, check
   [`symphony runs local-review-stats`](#telemetry-symphony-runs-local-review-stats)
   for approval rate, average cost, average duration.
5. If the numbers look good and the Linear / PR threads aren't noisy,
   promote `hybrid → local` to drop the remote bot from the steady-state
   loop.

Rollback at any point is a config edit: set `review_strategy: remote`
and reload. No data migration; the runs history rows from the local
phase remain queryable.
