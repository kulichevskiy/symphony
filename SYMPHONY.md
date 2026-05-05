# Symphony (Python) — Design Notes

A personal autopilot that watches GitHub Issues labeled `auto`, dispatches Claude Code agents in
git worktrees to implement them, runs them through Codex's GitHub-App review until approved, and
auto-merges. Inspired by [openai/symphony](https://github.com/openai/symphony) — same shape,
different stack.

## Scope

- **Goal:** personal autopilot for one user, one repo per Symphony instance. Not a team service
  (yet).
- **Reference SPEC:** [openai/symphony SPEC.md](https://github.com/openai/symphony/blob/main/SPEC.md).
  We follow the architectural pattern (loader, orchestrator, workspace, agent runner, event-driven
  state machine) but diverge where the SPEC's defaults don't fit personal use.

## End-to-end flow

```
poll GitHub for open issues labeled `auto`
  → resolve dependencies via task lists; pick FIFO from ready set
    → create worktree at $worktree_root/<repo>-<num> on branch auto/<num>
      → render round-1 prompt (issue + repo context + satisfied deps)
        → spawn `claude -p --output-format stream-json --max-turns 50 \
                --model claude-opus-4-7 --permission-mode bypassPermissions \
                --settings .symphony/claude-settings.json`
          → on agent exit: commit, push, open PR with `Closes #N`, post `@codex review`,
            arm `gh pr merge --auto --squash --delete-branch`
            → poll PR reviews; on CHANGES_REQUESTED feed comments to agent via
              `claude --resume <session-id>` (rounds 1–3) or fresh session (rounds 4–10)
              → push, re-nudge `@codex review`, repeat
                → on APPROVED, GitHub fires auto-merge → branch deleted, issue closed via Closes
                  → cleanup: `git worktree remove`, log event
```

## Decisions

### Agent
- **Subprocess to `claude` CLI**, not the Claude Agent SDK. Routes through Claude Code subscription
  (no API billing). `--output-format stream-json` for event parsing, `--resume <id>` for review
  rounds.
- **Model:** `claude-opus-4-7` for all rounds. No sonnet fallback.
- **Permission mode:** `bypassPermissions` (unattended runs).
- **Settings isolation:** Symphony passes `--settings <repo>/.symphony/claude-settings.json` so the
  user's interactive Claude Code config (skills, hooks, CLAUDE.md additions) does not leak into
  agent runs.
- **Max turns:** 50 per round (configurable).

### Issue source
- **GitHub Issues**, single repo per instance, configured in `symphony.toml`.
- **Selection:** label `auto`, state `open`. Skip if `auto/<num>` branch exists locally OR an open
  PR references the issue.
- **Ordering:** FIFO by `createdAt`.
- **Auth:** shell out to `gh` CLI for everything (issues, PRs, reviews, merge, comments). No PAT in
  Symphony.

### Dependencies
- **Declared via GitHub task lists** in issue body (`- [ ] #42`). One GraphQL query yields
  `trackedIssues { state }`.
- **Blocker satisfied** = closed-as-completed.
- **Cycles:** detect; label both members `auto-cycle`, skip.
- **Non-`auto` blockers count too** (you may have a manual issue C blocking auto-eligible A).

### Concurrency
- **asyncio**, single process, single event loop. Concurrency cap **3** (down from SPEC default 10
  to respect Claude subscription rate limits).
- **Rate-limit-aware pause:** detect 429/usage-limit in `claude` result events; set global
  `paused_until`, suspend dispatch until expiry.

### Workspace
- **Git worktrees** under `$worktree_root/<sanitized-repo>-<num>` on branch `auto/<num>`.
- **Sanitization:** keep `[A-Za-z0-9._-]`, replace others with `_`.
- **Reuse on re-dispatch** (don't blow away history).
- **Cleanup:** remove on successful merge; preserve on `auto-stuck` and on manual `symphony cancel`.
- **Startup cleanup:** at boot, remove worktrees whose issue is closed *and* PR is merged.
- **`after_create` hook** only (`<repo>/.symphony/hooks/after_create.sh`, runs with worktree path as
  `$1`). Other SPEC hooks (`before_run`, `after_run`, `before_remove`) skipped in v1.

### Review loop (Codex GH App, separate integration)
- **Detection:** poll `gh api repos/{owner}/{repo}/pulls/{n}/reviews` every 30s while in flight.
- **Verdict parsing:** track `last_reviewed_sha`; a review is fresh if `commit_id == HEAD`. Verdict
  states:
  - `APPROVED` → done (auto-merge will fire via branch protection).
  - `CHANGES_REQUESTED` → re-invoke agent with rendered comments.
  - `COMMENTED` with non-empty body → treat as `CHANGES_REQUESTED`.
- **Trigger:** post `@codex review` PR comment on open and after every push (idempotent).
- **Re-invocation:** rounds 1–3 resume same session; rounds 4–10 fresh session with full diff +
  accumulated comments preamble.
- **Cap:** 10 rounds. Beyond that → label `auto-stuck`, leave PR + worktree, free slot.
- **Codex never reviews:** re-nudge at 10 min; give up at 30 min → label `auto-stuck`.

### Output / merge
- **Symphony writes the PR body** (with `Closes #<num>`). Agent never edits PR metadata.
- **Auto-merge via branch protection.** Symphony arms `gh pr merge --auto --squash --delete-branch`
  on PR open. GitHub fires merge once Codex APPROVED + required CI checks pass. **Preflight refuses
  to start if branch protection isn't configured.**
- **No local pre-push gates** (no symphony-side `pytest`/lint). CI is source of truth; CI failure
  feeds back to agent like a review comment.
- **Git author identity** (per worktree, not global): configurable in `symphony.toml`, defaults to a
  Symphony bot identity so commits are distinguishable in `git log` / blame.
- **Frozen prompt at dispatch.** Mid-flight issue comments are ignored; cancel + re-dispatch to
  re-prompt.

### State
- **In-memory:** authoritative for live state (running set, retry queue, round counters,
  `paused_until`).
- **SQLite append-only event log** at `<repo>/.symphony/events.db`. One table:
  `events(id, ts, issue_number, run_id, kind, payload_json)`. New event kinds = new strings; no
  migrations.
- **Recovery:** on startup, replay events to reconstruct round counters + last reviewed SHA. World
  state (worktrees + GitHub) is the source of truth for "what's open."

### Lifecycle
- **Foreground CLI** for v1 (`symphony run` in a tmux pane). Promote to launchd later if it sticks.
- **Status surface:** structured JSON logs to file + pretty stdout, plus `symphony status` reading
  the event log + live `gh` state. No TUI, no HTTP dashboard.

### Configuration
- **`symphony.toml`** (config) + **`prompts/round1.md.j2`**, **`prompts/review.md.j2`** (Jinja2
  templates). No hot-reload — restart to apply changes.

Sample `symphony.toml`:
```toml
[repo]
path = "/Users/ak/Code/some-project"
default_branch = "main"

[github]
label = "auto"

[git]
author_name = "Symphony"
author_email = "alexey.kulichevskiy+symphony@adjust.com"

[orchestrator]
poll_interval_s = 60
max_concurrent = 3
review_round_cap = 10
codex_renudge_after_min = 10
codex_giveup_after_min = 30

[agent]
model = "claude-opus-4-7"
max_turns = 50

[paths]
worktree_root = "/Users/ak/Code/symphony-worktrees"
prompts_dir = "./prompts"
```

## Layout

```
symphony/
  pyproject.toml              # uv, Python ≥ 3.11
  README.md
  SYMPHONY.md                 # this file
  symphony/
    __init__.py
    __main__.py               # python -m symphony
    cli.py                    # typer: init, preflight, run, status, logs, cancel, gc
    config.py                 # TOML loader + dataclasses
    orchestrator.py           # asyncio loop, dispatch, retry, rate-limit pause
    github.py                 # `gh` wrapper: issues, deps (GraphQL), PRs, reviews, merge
    agent.py                  # `claude` subprocess: spawn, parse JSONL, capture session_id
    reviewer.py               # poll PR reviews, parse verdict, render review prompt
    workspace.py              # worktree create/reuse/remove, sanitization, after_create hook
    prompts.py                # Jinja2 rendering
    events.py                 # SQLite append + replay
    logging_setup.py          # structlog
    state.py                  # in-memory runtime state
    types.py                  # Issue, Run, ReviewRound dataclasses
  prompts/
    round1.md.j2
    review.md.j2
  tests/
    test_dispatch.py          # dep resolution, FIFO, cycle detection
    test_workspace.py         # sanitization, reuse
    test_events.py            # log + replay
    test_reviewer.py          # verdict parsing fixtures
```

## Dependencies

```
jinja2
structlog
typer
# stdlib: tomllib, sqlite3, asyncio, subprocess, pathlib
# subprocess shells: gh, git, claude
```

## CLI

```
symphony init        # write symphony.toml + prompts/, mkdir .symphony
symphony preflight   # check: gh auth, claude auth, branch protection, Codex App, worktree_root
symphony run         # main loop (foreground)
symphony status      # snapshot of in-flight + recent runs
symphony logs        # tail JSON log; --issue N to filter
symphony cancel <N>  # stop loop for issue N, leave artifacts, label auto-canceled
symphony gc          # remove auto-stuck worktrees older than N days (opt-in)
```

## Round-1 prompt skeleton

```
You are working on issue #{{ issue.number }}: {{ issue.title }}

{{ issue.body }}

{% if issue.comments %}
## Issue thread
{% for c in issue.comments %}
> {{ c.author }}: {{ c.body }}
{% endfor %}
{% endif %}

Repository: {{ repo.owner }}/{{ repo.name }}
Branch: auto/{{ issue.number }} (already checked out)
Base: {{ repo.default_branch }}
Working directory: {{ worktree_path }}

{% if satisfied_deps %}
Satisfied dependencies:
{% for d in satisfied_deps %}
- #{{ d.number }} {{ d.title }} (PR {{ d.pr_url or 'merged' }})
{% endfor %}
{% endif %}

Task: implement the change described in the issue.
- Make focused commits.
- Run tests/linters before declaring done.
- Do not push or open a PR — Symphony handles git operations.
- When done, exit cleanly.
```

## Round-N (review) prompt skeleton

```
Codex requested changes on commit {{ sha }}. Address each comment:

{% for c in comments %}
{% if c.path %}[{{ c.path }}{% if c.line %}:{{ c.line }}{% endif %}]{% else %}[general]{% endif %} {{ c.body }}
{% endfor %}

When done, ensure tests pass and commit. Do not push — Symphony will push.
```

## Safety invariants (from SPEC, kept)

1. The agent runs only inside its per-issue worktree path.
2. The worktree path stays inside `worktree_root` (validated post-sanitization).
3. The worktree key is sanitized to `[A-Za-z0-9._-]`.
4. Hooks are fully trusted (you write them, they live in your repo).
5. Secrets via `$VAR` indirection in TOML (e.g. `api_key = "$GITHUB_TOKEN"`); values never logged.

## Failure modes and behavior

| Failure | Behavior |
|---|---|
| Agent crash mid-run | Mark run failed, exponential backoff retry (10s × 2^attempt, cap 5 min) |
| Agent stalls (no event for `stall_timeout`, default 5 min) | Kill subprocess, treat as crash |
| Agent exits clean but `git diff` empty | Log; treat as failed run, retry |
| `gh` API error | Log, skip tick, keep workers running, retry next poll |
| Codex never reviews | Re-nudge at 10 min; give up at 30 min → `auto-stuck` |
| Codex requests changes 10× | `auto-stuck`; leave PR + worktree |
| CI fails | Treated as `CHANGES_REQUESTED`; failing logs piped to agent as review comments |
| Subscription rate-limited | Global `paused_until`; suspend dispatch until expiry |
| Cycle in dependencies | Both members labeled `auto-cycle`, skipped |
| Branch `auto/<num>` exists, no PR | Reuse worktree + branch (resumed run) |
| Open PR exists for issue | Skip dispatch; reviewer loop continues if Symphony was running for it |

## Implementation order

### M0 — Spikes (half-day, blocking)

Validate the four risky assumptions before writing milestone code:

1. **Codex GH App posts formal `APPROVED`/`CHANGES_REQUESTED` reviews** (not just PR comments). Test
   on a throwaway PR; confirm via `gh api .../reviews`.
2. **`@codex review` PR comment triggers Codex.** If not, find the actual trigger or rely on
   auto-only.
3. **`claude --resume <id>`** preserves context cross-invocation. Two-line round-trip test.
4. **Branch protection + `gh pr merge --auto`** actually fires after Codex APPROVED + green CI.

If any spike fails, revisit the relevant decision before writing code.

### M1 — Agent runner spike (~1 day)
`python -m symphony.agent run --prompt ... --workdir ...` — subprocess wrapper around `claude`,
JSONL parsing, session_id capture. No GitHub, no orchestrator.

### M2 — Single-issue happy path (~2 days)
`symphony run-once <num>`: fetch issue → worktree → agent → commit → push → open PR with
`Closes #N` → arm auto-merge → exit. No review loop.

### M3 — Review loop (~2 days)
Poll Codex review verdict, feed comments back via `--resume`, cap 10 rounds, re-nudge timers,
`auto-stuck` on exhaustion.

### M4 — Orchestrator + concurrency + dependencies (~3 days)
`symphony run` long-running. Asyncio loop, FIFO with task-list dep resolution, cap 3, retry queue,
rate-limit pause.

### M5 — Event log + status CLI (~1 day)
SQLite events, `symphony status`, `symphony logs`.

### M6 — Polish (~1–2 days)
`symphony preflight`, `after_create` hook, `symphony gc`, `symphony cancel`, structured logging
file output.

Total ~10 working days for a working v1.

## Out of scope (v1)

- Multiple repos per instance.
- Daemon / launchd integration.
- TUI or HTTP dashboard.
- Hot config reload.
- Hooks beyond `after_create`.
- Multi-host / SSH worker extension.
- Pluggable trackers (Linear, Jira) — single GitHub Issues adapter only.
- Pluggable agents (Codex, Aider) — single `claude` adapter only.
- Cost / token reporting beyond raw event log.
