# Local-review feature: handoff

One-page entry point after the 20-iteration Ralph research loop. If
you're returning to this in three months, start here, then jump to
[`local-review-flow.md`](local-review-flow.md) for the detailed design
and operator workflows.

## The original question

> Current flow: local agent implements the issue, creates a PR, then
> waits for the PR review by another agent and goes back and forth
> until the PR is reviewed and approved. I noticed that this process
> can take a very long time — 5+ hours and up to 30 rounds of review.
> Alternative approach: use another local agent for the review.

## What the research found

The wall-clock bottleneck in the existing flow is **not** the local
fix-run latency. It's the round-trip to the GitHub-hosted `@codex`
bot for each review round:

| Step                       | Wall time per round |
| -------------------------- | ------------------- |
| local push + PR comment    | 5–15 s              |
| **remote codex bot review**| **3–10 min**        |
| poll-cycle pickup lag      | 0–60 s              |
| local fix-run subprocess   | 1–5 min             |

30 rounds × ~10 min of *overhead* per round explains the 5-hour
ceiling. The local fix-run itself is rarely the slow part.

## What shipped

A drop-in `local_review` phase between Implement-success and
push-create-PR that runs the same fix/review loop **in-workspace**,
no GitHub round-trip per round.

### Code surface (5 modules + orchestrator wire-in)

| File                                                 | Role                                |
| ---------------------------------------------------- | ----------------------------------- |
| `src/symphony/pipeline/local_review.py`              | Pure: prompt, argv, parser          |
| `src/symphony/pipeline/local_review_loop.py`         | Pure: iteration policy + outcomes   |
| `src/symphony/pipeline/local_review_io.py`           | Adapter: Runner protocol → string   |
| `src/symphony/pipeline/local_review_session.py`      | Integration: ties all four together |
| `src/symphony/pipeline/cost_guard.py`                | `UsageCostEstimator` made public    |
| `src/symphony/orchestrator/poll.py::_run_local_review_phase` | Wire-in seam                |

### Operator surface

| Tool / signal                       | Iteration | What it does                              |
| ----------------------------------- | --------- | ----------------------------------------- |
| `review_strategy: remote\|hybrid\|local` config | 2 | Per-binding opt-in                        |
| `reviewer_agent` config             | 2         | Override the reviewer family              |
| `symphony local-review-dry-run`     | 17        | Pre-flight without touching DB/GitHub     |
| Linear "starting" + per-iter heartbeats | 9     | Real-time visibility                      |
| `$skip-local-review` slash + mid-subprocess kill | 8, 10 | Operator escape hatch                |
| GitHub PR summary comment on APPROVED | 14      | Audit trail for human reviewers           |
| `symphony runs local-review-stats`  | 12        | Aggregate approval rate / cost / duration |
| `local_review` rows in `runs` table | 11        | Per-issue audit + cost participation      |
| Cost cap enforcement                | 7         | Mid-loop abort if `prior + session ≥ cap` |
| `local_review_iteration_cap` config | 13        | Separate from remote `review_iteration_cap`|

### Safety properties

- **Default off**: `review_strategy: remote` (the original behaviour)
  remains the default. Every test in the suite that doesn't explicitly
  opt in to `local`/`hybrid` runs the original flow.
- **Always-on fallback**: every non-APPROVED outcome (`EXHAUSTED`,
  `STUCK_LOOP`, `REVIEWER_FAILED`, `FIX_RUN_FAILED`, `COST_CAP_BREACHED`,
  `SKIPPED`) falls through to the remote `@codex` bot. The local pass
  is an optimization, never a single point of failure.
- **Cost cap participates**: local-review subprocess cost feeds
  `UsageCostEstimator` and trips the same `cost_cap_per_issue_usd` /
  per-binding `cost_cap_usd` as Implement.
- **Read-only sandbox**: codex reviewer runs with `--sandbox read-only`.
  Cannot modify the working tree.
- **Mid-subprocess kill**: `$skip-local-review` from Linear interrupts
  the in-flight reviewer/fixer within ~1 s, not after `stall_secs`.

## Expected wins (analytic; not yet validated against production traffic)

Per-round budget:
- **Before**: ~10 min/round of round-trip overhead + 1–5 min fix-run.
- **After**: ~1–3 min of model latency only.

A 30-round, 5-hour issue could complete in ~30–60 min, or finish in
far fewer rounds (no queue-latency cost amortization means rounds are
"cheap" and the loop converges to a real answer faster).

## Empirical validation

Real-CLI smoke tests against scratch repos with a planted bug:

- **codex** (iter 5, iter 16) — emitted the `<<<VERDICT:...>>>` marker
  cleanly, located the bug at `add.py:6`, included a fix recipe.
- **claude** (iter 6) — same marker contract, three findings on the
  same diff. Token cost ~$0.28 for one review of a tiny diff.

Two production-relevant CLI quirks found during smoke runs (would
have crashed the wire-in if not caught):

1. `codex exec review --base X [PROMPT]` rejects the combination
   (mutex flags in codex 0.130). Fix: drop `--base`, thread the base
   branch into the prompt body.
2. `codex exec review` ignores custom output schemas, dropping the
   verdict marker. Fix: use plain `codex exec --sandbox read-only`.

See `docs/local-review-flow.md§CLI quirks` for full details.

## What's deferred (not engineering blockers)

- **Production validation against real bindings.** No way to validate
  the actual wall-time savings without operator-driven enablement on
  a real binding. The dry-run command lets operators de-risk before
  flipping the flag.
- **`local-review-trace <issue>` CLI.** Postmortem tool that prints
  the run-history rows for a specific issue. Nice-to-have; the Linear
  comment thread already holds the same data in a different form.
- **Prompt tuning with production data.** The smoke-evidence-driven
  prompt (iter 16) is solid on synthetic bugs. Real prompts will
  surface real edge cases. The dry-run command + telemetry give the
  feedback loop.
- **Auto-promotion `hybrid → local`.** Currently a manual config
  change after operators are satisfied with hybrid telemetry.

## Numbers

| Metric                           | Start | End   |
| -------------------------------- | ----- | ----- |
| Test count                       | 350   | 445   |
| New tests                        | —     | +95   |
| `pipeline/` modules added        | 0     | 4     |
| CLI subcommands added            | 0     | 2     |
| Slash commands added             | 0     | 1     |
| Config knobs added (top-level + binding) | 0 | 6 |
| `LoopOutcome` variants           | —     | 7     |
| `mypy --strict` errors           | 0     | 0     |
| `ruff` errors                    | 0     | 0     |

## Reload-the-context cheatsheet

If you're picking this up cold:

1. Read [`local-review-flow.md`](local-review-flow.md) §Bottleneck +
   §Proposal + §Modes (≈ 100 lines).
2. Look at [`local_review_session.py`](../src/symphony/pipeline/local_review_session.py)
   — the integration layer is the single entry point.
3. Search `_run_local_review_phase` in `poll.py` — that's the
   orchestrator-side wire-in.
4. Run `symphony local-review-dry-run --workspace . --reviewer codex`
   on any local branch to see live output.
