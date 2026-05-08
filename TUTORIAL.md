# Symphony — Tutorial

How to use this thing in practice: from zero to an autopilot that closes
GitHub issues for you.

Symphony is a personal autopilot. It watches issues labeled `auto`, runs a
Claude Code agent in a dedicated git worktree, drives the resulting PR through
the Codex GitHub App until it's approved, and merges. One instance = one
repository.

---

## 0. Prerequisites

- Python ≥ 3.11 and [`uv`](https://docs.astral.sh/uv/)
- `gh` CLI, authenticated (`gh auth login`)
- `claude` CLI (Claude Code), authenticated — Symphony talks to your Claude
  Code subscription, not the API
- `git` ≥ 2.20 (worktrees are required)
- The target repository — cloned locally and present on GitHub
- The Codex GitHub App
  ([chatgpt-codex-connector](https://github.com/apps/chatgpt-codex-connector))
  installed on the target repository

Symphony does not install anything into the target repository — it only writes
config files and creates worktrees.

---

## 1. Installing Symphony

```bash
git clone https://github.com/kulichevskiy/symphony.git ~/Code/symphony
cd ~/Code/symphony
uv sync
```

From there you can either invoke it via `uv run symphony …` from this
directory, or put it on your `PATH`:

```bash
uv tool install --editable .
symphony --version
```

---

## 2. Preparing the target repository

Switch to the repo you want Symphony to drive:

```bash
cd /path/to/your/project
symphony init
```

This creates:

- `symphony.toml` — config
- `prompts/round1.md.j2` — round-1 prompt template (defaults to "issue body
  + repo context")
- `prompts/review.md.j2` — review-round prompt template (Codex feedback fed
  back to the agent)
- `.symphony/` — runtime directory (worktrees, event log); added to
  `.gitignore`

Open `symphony.toml` and adjust:

```toml
[repo]
path = "."                          # path to the repo (absolute is fine)
default_branch = "main"

[github]
label = "auto"                      # the label that marks an issue for the autopilot

[git]
author_name = "Symphony"
author_email = "you+symphony@example.com"  # commits will be signed with this identity

[orchestrator]
poll_interval_s = 60                # GitHub poll cadence
max_concurrent = 3                  # parallel issues (≤ 3 to stay within Claude rate limits)
review_round_cap = 10               # max review rounds before flipping to auto-stuck
codex_renudge_after_min = 10        # if Codex is silent, post @codex review again
codex_giveup_after_min = 30         # if it's still silent, give up → auto-stuck

[agent]
model = "claude-opus-4-7"
max_turns = 50

[paths]
worktree_root = ".symphony/worktrees"   # one worktree per issue lives here
prompts_dir = "./prompts"
```

### Labels and branch protection

The target repo must have these labels (Symphony checks during preflight):

- `auto` — marks an issue for the autopilot
- `auto-stuck` — applied automatically if the loop runs out of rounds or
  Codex stays silent
- `auto-cycle` — applied automatically when a dependency cycle is detected
- `auto-canceled` — applied by `symphony cancel`

Create them in one shot:

```bash
gh label create auto --color 0E8A16
gh label create auto-stuck --color B60205
gh label create auto-cycle --color B60205
gh label create auto-canceled --color C5DEF5
```

Branch protection on `main`: **recommended** to require at least one status
check (any CI). Preflight reports this as a warning, not a hard failure, since
branch protection rules require a paid GitHub plan on private repos.
Do **not** require approving reviewers — Codex never posts `APPROVED` reviews,
so Symphony does the merge itself.

---

## 3. Preflight — sanity-check the setup

```bash
symphony preflight
```

What it checks:

- `gh auth status` is green
- `claude --version` and a tiny `claude -p ok --max-turns 1` round-trip work
- `worktree_root` exists and is writable
- The default branch has at least one required status check
- The Codex GitHub App is installed on the repo (warn, not fatal — sometimes
  the token simply can't see installations)
- All required labels exist

If anything is red, fix it and re-run. Once everything is green, you're ready.

---

## 4. Shake it down on one issue: `run-once`

Don't jump straight to `run`. Pick one safe issue, label it `auto`:

```bash
gh issue edit 42 --add-label auto
```

Then run exactly one iteration:

```bash
symphony run-once 42
```

What happens:

1. A worktree is created at `.symphony/worktrees/<repo>-42` on branch
   `auto/42`.
2. If `.symphony/hooks/after_create.sh` exists, it runs with the worktree
   path as `$1`. Useful for `uv sync`, `pnpm install`, and similar setup.
3. The round-1 prompt is rendered from `prompts/round1.md.j2` (issue body +
   context).
4. `claude -p --output-format stream-json --max-turns 50 --model
   claude-opus-4-7 --permission-mode bypassPermissions` runs in the
   worktree.
5. When the agent exits, Symphony commits, pushes, and opens a PR with
   `Closes #42`. Codex auto-reviews on PR open.
6. Every 30s Symphony polls the PR for a review from Codex and a `+1`
   reaction on the PR by `chatgpt-codex-connector[bot]`.
7. If Codex requests changes, Symphony renders the round-2 prompt from
   `prompts/review.md.j2` and feeds it to the agent via
   `claude --resume <session>` (rounds 1–3) or a fresh session with the full
   diff (rounds 4–10).
8. Once Codex 👍 the PR and all required CI is green, `gh pr merge --squash
   --delete-branch` fires. The issue closes via `Closes`. The worktree is
   removed.

Exit codes:

- `0` — PR was merged
- `2` — `auto-stuck` or agent error (PR and worktree are left for inspection)
- `1` — issue never reached dispatch (skip reason logged)

---

## 5. Full autopilot: `symphony run`

Once `run-once` has worked at least once without surprises:

```bash
symphony run
```

Long-running process. What it does:

- Polls `gh issue list --label auto --state open` every `poll_interval_s`.
- Resolves dependencies via task-list checkboxes in the issue body
  (`- [ ] #41`). Only issues with all blockers closed are eligible.
- Picks FIFO by `createdAt`, up to `max_concurrent` in parallel.
- If Claude returns 429 / usage-limit, sets a global pause until
  `paused_until` and stops dispatching new work.
- If a dependency cycle is detected, both members get `auto-cycle` and are
  skipped.
- Runs preflight on startup; exits if it fails.

Run it inside `tmux` / `screen` / a dedicated terminal tab. There is no
daemon / launchd integration in v1.

`Ctrl+C` shuts down cooperatively, lets in-flight rounds finish, and exits.
Worktrees and PRs are not touched.

---

## 6. Observability and control

### What's running right now

```bash
symphony status
```

Shows:
- in-flight runs (issue, current round, elapsed time, last reviewed SHA,
  last verdict)
- terminal runs over the last 24h (approved / stuck / failed)

### Event stream

```bash
symphony logs                  # last 100 events
symphony logs --issue 42       # only events for issue #42
symphony logs --follow         # tail -f the event log
```

JSON lines. The source of truth is the SQLite database at
`<repo>/.symphony/events.db` (append-only, no migrations).

### Cancel

```bash
symphony cancel 42
```

Cooperatively shuts down the work for issue 42 and applies the `auto-canceled`
label. The worktree and PR are left intact — clean them up by hand.

### Garbage collection

```bash
symphony gc                # candidates: auto-stuck worktrees older than 14 days
symphony gc --days 7
```

Asks for confirmation, then removes the worktree and the local branch. The PR
on GitHub is left alone.

---

## 7. Gotchas

**The prompt is frozen at dispatch.** Comments added to the issue after the
agent starts are ignored. To inject extra context, `symphony cancel`, edit the
issue, then re-apply the `auto` label.

**Settings isolation.** Symphony passes `--settings .symphony/claude-settings.json`
to the agent. Your personal hooks and skills from `~/.claude/` do **not** apply
inside agent runs — by design, so local config doesn't break reproducibility.
If you need setup, drop commands into `.symphony/hooks/after_create.sh`.

**`after_create` is the only hook in v1.** Receives the worktree path as `$1`.
It's trusted (you write it). There is no `before_run` / `after_run`.

**Dependencies are task-list only.** `- [ ] #41` in the issue body. Not in
comments, not in the title. A blocker is satisfied only when it's closed
*as completed* — closed-as-not-planned does not count.

**CI failure = changes requested.** Failing logs are piped to the agent as if
they were review comments. Green CI is mandatory for merge.

**Codex silent?** After `codex_renudge_after_min` (10 min), Symphony posts
`@codex review`. After `codex_giveup_after_min` (30 min), the issue is flipped
to `auto-stuck`.

**Re-dispatch.** If branch `auto/<num>` already exists with no PR, Symphony
reuses the worktree (resumed run). If a PR is open, dispatch is skipped but
the review loop keeps spinning for it.

**Secrets.** In `symphony.toml` you can write `api_key = "$GITHUB_TOKEN"` —
Symphony resolves the value from the environment and **never logs it**.

---

## 8. A typical workflow

```bash
# 1. Open the issue with a clear definition of done and any dependencies
gh issue create --title "Add /version flag" --body "...task list..."

# 2. Mark it auto
gh issue edit 137 --add-label auto

# 3. If Symphony is already running, just wait.
#    Otherwise, shake it down on a single issue:
symphony run-once 137

# 4. Watch progress
symphony status
symphony logs --issue 137 --follow

# 5. If it gets stuck → diagnose, edit the issue, re-apply the label
symphony cancel 137
gh issue edit 137 --remove-label auto-canceled --add-label auto
```

---

## 9. What Symphony does NOT do (v1)

- Multiple repos in one instance — not supported
- Daemon / launchd integration — keep it in tmux
- TUI / HTTP dashboard — there's `status` and `logs`
- Hot config reload — restart to apply changes
- Hooks beyond `after_create` — none
- Linear / Jira / Codex / Aider — only GitHub Issues + Claude Code

For design details and trade-offs, read `SYMPHONY.md`.
