# Incident: pipeline stall, 2026-05-26

Document version: 2026-05-26, written from live state of `state.sqlite`, `logs/`, Linear, and `git log` on `main`.

## TL;DR

Two tickets are parked at `implement_failed` and the operator's `$retry` does nothing. There is **no** new bug here — the cascade is the intersection of two already-known issues compounding each other:

1. **Stall-timeout false positive on long subprocess commands** (new, unfiled): Symphony's runner watchdog treats only **lines on the codex subprocess's stdout/stderr** as activity. When the codex agent issues a single long-running shell command (broad `rg`, `pnpm i`, `gh api`, `pytest --collect-only`, etc.) that runs longer than `stall_timeout_secs` (600s) without printing, `activity` is never set → watchdog kills the run with `stall_timeout`, the run is logged as `implement failed` with **$0 cost** and zero useful artifacts.
2. **SYM-33 — operator slash commands silently dropped** (known, open): the deployed daemon's `LINEAR_API_KEY` is owned by the same Linear user who operates the system, so the operator's `$retry` comments come back with `isMe=true` and are filtered by `slash.parse`. The poll loop advances `comment_cursors.last_seen_at` via the self-authored catch-up branch, never inserts a `comment_events` row, never dispatches a handler. The `$retry` is lost.

The two compound: a single broad ripgrep on a fresh ticket parks it on `implement_failed`, and the operator's natural recovery — `$retry` — is invisible to the daemon. From the operator's vantage point the pipeline looks "stuck": tickets sit in **In Review** with `implement_failed` waits and no further activity.

Only two tickets are actually stuck (VIB-182, SYM-33). Everything else from this work block (VIB-170…VIB-181, SYM-31, SYM-32) merged successfully. The user's read of "all tickets stuck" reflects that the two most recent tickets, including the meta-ticket about this very bug, are both frozen.

## Current state (as of 2026-05-26T16:00Z)

| Ticket | Linear status | DB stage | DB status | Operator wait | Operator action attempted | Result |
|--------|---------------|----------|-----------|----------------|----------------------------|--------|
| VIB-182 | In Review | implement | failed | `implement_failed` since 11:51:26Z | `$retry` at 12:56Z + `$retry` at 15:42Z | both lost — SYM-33 |
| SYM-33 | In Review | implement | failed | `implement_failed` since 15:37:44Z | none yet | — |

Diagnostic evidence (queried directly from `state.sqlite`):

* `operator_waits` has exactly two rows, both `kind=implement_failed`:
  * `439e8078-db67-42e8-879f-c11dc2a19a24` (VIB-182), created 2026-05-26T11:51:26Z, run `828482fd-...`.
  * `7c16c71d-c951-4f01-ae49-6a1fb3f05c83` (SYM-33), created 2026-05-26T15:37:44Z, run `7af4c277-...`.
* `comment_events` contains **zero** rows for either issue ID — confirming SYM-33 fingerprint (cursor advanced, no event recorded).
* `comment_cursors.last_seen_at` for VIB-182 = `2026-05-26T15:42:35.811Z` = exact timestamp of operator's second `$retry`. Cursor `last_seen_ids` contains that comment's UUID. So the daemon **saw** the comment and silently advanced past it.
* Both run logs (`logs/828482fd-...log` and `logs/7af4c277-...log`) end mid-stream with no `turn.completed`/`turn.failed` event, consistent with watchdog termination of the codex subprocess.
* Linear auto-posted "Implement stage failed — pipeline halted" comment for both runs with `Error: runner ended with stall_timeout`.

The Todo lane is empty for VIB and SYM (verified via Linear), so no other ticket is queued. Daemon process is alive (PID 87743, started 2026-05-25T16:00Z, post-#133 — already has the surface-LinearError fix).

## How we got here

### Failure 1 — VIB-182 implement, 2026-05-26T11:34Z → 11:51Z

The codex agent received the workshop-duration prompt and started exploring. After completing 17 short commands, it issued:

```
rg -n "workshops|recording_url|vimeo|Workshop" supabase/seed.sql app lib -g '*.sql' -g '*.ts' -g '*.tsx'
```

This rg over a large monorepo with broad alternation took longer than 10 minutes to return. While `rg` was running, the codex parent was blocked waiting for the tool result and emitted **zero lines** on its stdout. The local runner's watchdog (`src/symphony/agent/runners/local.py:78-97`) measures activity solely from `stream.readline()` calls returning data, so `activity.set()` was never called → `asyncio.wait_for(activity.wait(), timeout=600s)` fired → `_terminate_process_group(proc.pid)` killed codex (and `rg` with it). Run terminated with `stall_timeout` → orchestrator parked the issue at `implement_failed`.

The Linear activity-digest comment at 11:50:46Z literally says "Running commands: `rg ...` (15m 0s)" — i.e. the watchdog already killed at 11:44 but the activity digest reflected its last live snapshot before death. Cost was $0 because codex never closed out a turn → no token accounting.

### Failure 2 — SYM-33 implement, 2026-05-26T15:27Z → 15:37Z

Identical shape. Codex on the SYM-33 (which exists to fix the SYM-33 bug, ironic) issued:

```
rg -n "author_is_me|LinearComment|from_node|comments_since|_poll_slash_comments|command_rejected|slash\\.parse|operator_waits|needs_approval|approve" src tests docs CLAUDE.md
```

That alternation matches `approve` everywhere in the codebase (docs, prompts, prior poll.py logic, every operator_waits reference) — rg ran longer than 10 minutes → same stall-timeout kill → same `implement_failed` park. The run log stops mid item_7 with no terminal event.

### Recovery failure — VIB-182 `$retry`

Operator posted `$retry` twice on VIB-182. Both arrived through Linear's GraphQL with `viewer.isMe=true` because **symphony's LINEAR_API_KEY belongs to the same Linear user** (Alexey Kulichevskiy). `LinearComment.from_node` (`src/symphony/linear/client.py:95`) copies `isMe` into `author_is_me=True`. `slash.parse` skips author-is-me comments (`src/symphony/linear/slash.py:86`, `if c.author_is_me: continue`). The poll loop's self-authored catch-up branch advances `comment_cursors.last_seen_at` to the operator's `createdAt` and appends the comment ID to `last_seen_ids` without ever inserting into `comment_events` or dispatching a handler.

This is **SYM-33** verbatim, filed today (2026-05-26 15:25Z) but not yet fixed.

## Root causes, ordered

1. **Stall-timeout heartbeat tied to wrong event source** (no Linear issue yet; filing required).

   `activity.set()` fires only when the codex parent writes a stdout/stderr line. The agent's loop is "LLM thinks → emit `item.started` → spawn tool → wait for tool → emit `item.completed` → LLM thinks → …". Between `item.started` and `item.completed` there is a tool subprocess running and **no codex output**. If that tool subprocess runs longer than `stall_timeout_secs`, we kill a healthy run.

   This is not a "rg is slow" problem. The same shape will hit any tool execution that legitimately runs ≥600s: `pnpm install` on a cold worktree, `pytest -k integration`, `playwright test`, `gh api graphql` with pagination, large `git diff`, etc. The watchdog is supposed to catch deadlocked codex processes, but it catches "long innocent subprocess" with the same blunt hammer.

   Why this surfaced now: the project added a `supabase/seed.sql + app + lib` triplet, and the agent — perhaps influenced by SKILL.md guidance to "explore first" — issues broad searches early. On smaller repos the same call returns in seconds.

2. **SYM-33 — per-credential identity conflation** (open, filed today).

   See the SYM-33 description in Linear and the [[symphonyd-known-bugs]] memory entry. Operator-authored slash commands are filtered as if they were symphony-authored, because Linear's `isMe` is per-user not per-credential and symphony shares the operator's Linear identity. Cursor advances, `comment_events` stays empty, command silently lost. There is no fix-and-recovery path inside symphony for this — the operator has to act out-of-band (SQL surgery + manual rebase/push + Linear state edit).

3. **(Aggravating) implement_failed wait offers no auto-recovery on plain stall.**

   Today, every stall-timeout in implement parks on `operator_waits.implement_failed` and waits for human input. We have no automatic "retry once on stall" budget. Combined with #1 above, transient long-subprocess events become operator pages. A bounded auto-retry (e.g. one free retry on `stall_timeout`-only failures, with a `stall_retry_count` column) would absorb most flaps before they become incidents — without re-enabling the original infinite-retry mode that #50/#59 explicitly closed.

## Status of fixes (updated 2026-05-26, post-implementation)

Steps 1 and 2 are **implemented locally** and ready for restart-and-observe:

* **Step 1 (subprocess-aware watchdog)** — done. `RunnerSpec.command_secs` (default 1800s) + `Config.command_timeout_secs: int = 1800`. The watchdog in `src/symphony/agent/runners/local.py` is now a polling loop that parses the codex JSON stream: while a `command_execution` item is in flight it measures the deadline against `command_secs`, otherwise against `stall_secs`. Regression tests in `tests/test_runner_local.py` (`..._does_not_stall_while_command_in_flight`, `..._stalls_when_command_exceeds_command_secs`).
* **Step 2 (SYM-33 sentinel)** — done. Every symphony comment now carries `SYMPHONY_COMMENT_SENTINEL` (injected in `Linear.post_comment`); `LinearComment.from_node` derives `author_is_me` from the sentinel, not `isMe`. Operator `$retry` (no sentinel) is no longer dropped. Tests in `tests/test_linear_client.py`.
* Steps 3 (bounded auto-retry) and 4 (prompt tightening) are **not yet implemented** — optional follow-ups.

**Consequence for unsticking:** after restart on the new code, the operator's `$retry` on VIB-182 / SYM-33 should re-dispatch the implement run directly (the `implement_failed` waits are restored on startup, and the fresh `$retry` is now visible). Step 5's SQL surgery becomes a fallback, not the primary path.

Note: `tests/test_merge_stage.py::test_merge_agent_new_commit_requires_fresh_review_before_merge` fails on clean `main` (stale `pr_view` mock missing `include_status_checks`, from SYM-31 #131) — pre-existing, unrelated to this incident.

## Plan to fix the pipeline

Each step below is something that changes pipeline behaviour, **not** something that touches a single stuck ticket. The two stuck tickets get unblocked as a fallout of step 1 (after restart) + a one-time SQL fixup (step 5, last).

### Step 1 — Heartbeat that tracks subprocess activity (P0, fixes future stalls)

**Goal:** Watchdog should consider a run "alive" if either:
* the codex parent has emitted a stdout/stderr line in the last `stall_timeout_secs`, **OR**
* the codex parent's process group has at least one non-codex descendant in `R`/`S` state that has been making syscall progress.

Two viable implementations, simplest first:

* **(1a) Treat `item.started` of `type=command_execution` as a stall extension.**
  When we see an `item.started` JSON event for a `command_execution`, push the watchdog deadline out to `now + command_execution_max_secs` (suggested default: 1800s). When the matching `item.completed` arrives, restore the deadline to `now + stall_timeout_secs`. This requires the runner to parse the JSON stream rather than just count lines — but we already do JSON parsing downstream in `activity.py`; we can route the event through the runner.
  * Pros: zero process-tree fiddling, deterministic, observable in logs.
  * Cons: trusts codex to actually emit `item.started`. If codex itself hangs without emitting, we still want a timeout — keep the unconditional outer cap of e.g. 2× `command_execution_max_secs`.
  * Where: `src/symphony/agent/runners/local.py:60-99` (watchdog loop), plus a minimal JSON peek in `pump()`.

* **(1b) Process-group liveness probe.**
  In the watchdog, before declaring stall, walk `psutil.Process(proc.pid).children(recursive=True)` and require **all** descendants to be either `zombie` or absent before killing. If any descendant is `running`/`sleeping`/`disk-sleep`, extend the deadline.
  * Pros: catches every "innocent long subprocess" case, including third-party tools that don't go through codex events.
  * Cons: adds a `psutil` dependency, slightly more code to test.
  * Where: same place as 1a.

**Recommended:** ship (1a) first because it's the smallest change and covers the actual incident shape. Add (1b) later as a backstop.

Add config knob: `command_execution_max_secs: int = 1800` in `src/symphony/config.py`; thread through to `RunnerSpec`/`LocalRunner`.

Regression test: a fake codex that prints `item.started` then sleeps 12 minutes then prints `item.completed`. Today fails with stall_timeout; after fix completes normally.

### Step 2 — Land SYM-33 fix (P0, fixes operator recovery)

Without this, no operator slash command works while symphony shares the operator's Linear identity. Three implementation paths in the SYM-33 issue; the cheapest:

* **Add a hidden sentinel to every symphony-posted comment body** (e.g. trailing HTML comment `<!-- symphony:agent v1 -->` or a zero-width-joiner-prefixed marker). Detect symphony-authorship by sentinel presence in body instead of `isMe`. Fully backwards-compatible: existing operator comments won't have the sentinel and will be treated as operator input correctly; symphony's own future digests will be detected as self.
* Where: `src/symphony/linear/templates.py` (inject the sentinel into every templated post), `src/symphony/linear/client.py:95` (`LinearComment.from_node` — flip `author_is_me` derivation to "body contains sentinel OR canonical bot UUID match"), `src/symphony/linear/slash.py:86` (still skip on `author_is_me`).
* On startup, replay the last N hours of `comment_cursors` to backfill any sentinel-less operator comments we already advanced past — or simpler, accept that the cursor-advance for old operator comments is permanent and require step 5 (one-time SQL fixup) for the two currently parked issues.

Regression test in `tests/test_slash_polling.py`: fake Linear comment with `isMe=true` and no sentinel → operator path; `isMe=true` and sentinel present → symphony path.

### Step 3 — Bounded auto-retry on stall-timeout (P1, prevents 1-of-N flake from becoming a page)

After step 1 the false-positive rate should drop sharply, but the underlying watchdog must still kill genuinely deadlocked codex runs. Currently every kill becomes an `implement_failed` operator wait. Soften that for stall-only failures:

* Add column `runs.stall_retries INTEGER NOT NULL DEFAULT 0` (migration in `src/symphony/db/schema.sql`).
* In `_handle_run_terminal` (the path that builds the `implement_failed` wait), if `terminal_kind == "stall_timeout"` and `stall_retries < binding.implement_stall_retry_cap` (default: 1), instead of parking, dispatch a fresh implement run with `stall_retries = previous + 1`. Park as `implement_failed` only after the cap is exhausted.
* Surface the retry as a Linear "auto-retry after stall" comment so the operator sees it.
* Where: `src/symphony/orchestrator/poll.py` near `_track_implement_failed_wait`.

Regression test: pytest in `tests/test_implement_e2e.py` — first run ends with `stall_timeout`, expect re-dispatch; second run also stalls, expect park.

### Step 4 — Tighten the agent prompt to avoid broad ripgrep (P2, defence in depth)

The implement prompt today is silent about subprocess-runtime cost. Add a one-liner in `src/symphony/agent/prompt.py` (or whichever file actually composes the implement prompt — verify before editing) along the lines of:

> "Tool calls must return within 5 minutes. For codebase search, prefer narrow ripgrep over a single subtree (e.g. `rg pattern app/admin/cohorts/`); avoid broad alternations over the whole repo. If you need a wide search, run it inside `timeout 60s` and accept that it may be truncated."

This is defence-in-depth: even after step 1 makes the runtime safe, narrow searches are cheaper, more deterministic, and produce smaller activity-digest comments. Doesn't replace step 1.

### Step 5 — Unstick VIB-182 and SYM-33 (one-time, last)

Until step 2 ships, the only way to re-arm these two tickets is out-of-band SQL + Linear edit. **Do this only after step 1 has landed and the daemon has been restarted**, otherwise the next implement attempt will hit the same stall.

```bash
# 1. Backup
cp state.sqlite state.sqlite.bak.$(date +%Y%m%d-%H%M%S)

# 2. Clear the parked waits
sqlite3 state.sqlite "DELETE FROM operator_waits WHERE issue_id IN (
  '439e8078-db67-42e8-879f-c11dc2a19a24',
  '7c16c71d-c951-4f01-ae49-6a1fb3f05c83'
);"

# 3. Restart the daemon (drops the in-memory _implement_failed_run_bindings
#    and _dispatch_run_ids maps so the next poll re-picks up the issue
#    through the normal Todo-lane scan). Verify the new daemon PID and
#    that PID is on a current commit (`git rev-parse HEAD`).
```

Then move both Linear issues back to **Todo** (their `linear_states.ready` state). Reconciliation on the next poll cycle picks them up. Both tickets will redispatch implement runs cleanly under step 1's heartbeat.

If the operator instead wants to push manual work (e.g. SYM-33 is half-done in `workspaces/kulichevskiy_ssymphonyd/sym-33`), commit + push that branch, open the PR manually, and SQL-inject a `runs(stage=review, status=running, pid=NULL)` row + `issue_prs` row. But step 1 + step 2 + step 5 is the cleaner path.

## Order of operations

1. Implement and merge step 1 (subprocess-aware watchdog). Hard requirement before any restart, otherwise the next dispatch hits the same wall.
2. Implement and merge step 2 (SYM-33 sentinel). Hard requirement before relying on `$retry` again.
3. Implement and merge step 3 (bounded auto-retry). Optional but high-value; eliminates the residual single-flake page.
4. Implement and merge step 4 (prompt tightening). Optional; defence in depth.
5. Restart daemon on the new HEAD. Then SQL fixup (step 5) + Linear state flip for VIB-182 and SYM-33.

Once steps 1+2+5 are done, the pipeline runs unattended again and the operator's `$retry` works as intended.

## Open questions for the operator

* Is the operator OK with bumping `stall_timeout_secs` to e.g. 900s as a tide-over until step 1 lands? It would absorb ~80% of today's incident without code changes, but doesn't fix the underlying class of bug.
* For step 2, is "sentinel in body" preferred over migrating symphony to a dedicated Linear bot user? The bot-user route is cleaner long-term but requires an extra Linear seat + OAuth dance.
* Is `command_execution_max_secs = 1800` (30 min) acceptable as the new outer cap per single tool call?
