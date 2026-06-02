# symphony

`symphony` is a headless, Linear-native orchestrator for AI coding agents. It
watches your Linear teams for labelled issues, runs a local coding agent
(`claude` or `codex`) inside a per-issue git workspace, opens a GitHub pull
request, drives the review/fix/merge loop, and reports every step back to Linear
— no human in the terminal required.

What you see from the outside:

```text
Linear issue (ready state + label)
  → agent edits code in a private git workspace
  → GitHub PR opened
  → review / fix loop (CI + @codex review)
  → PR merged
  → Linear issue moved to Done
```

You drive it entirely from Linear: move an issue into the pickup state, and
watch comments and state transitions appear as the work progresses.

---

## Prerequisites

| Tool / access | Check |
| --- | --- |
| Python 3.12+ | `python --version` |
| [`uv`](https://docs.astral.sh/uv/) | `uv --version` |
| `git` | `git --version` |
| GitHub CLI `gh` | `gh --version` |
| One agent CLI: [`claude`](https://docs.claude.com/en/docs/claude-code) **or** [`codex`](https://github.com/openai/codex) **0.136+** | `claude --version` / `codex --version` |
| A Linear API key (`lin_api_…`) | from Linear → Settings → API |
| Write access to the GitHub repo(s) you target | `gh auth status` |

---

## Install

```bash
git clone git@github.com:kulichevskiy/symphony.git
cd symphony
uv sync                      # installs all dependencies into .venv

# Authenticate GitHub (the daemon pushes branches, opens and merges PRs):
gh auth login --hostname github.com --git-protocol ssh --scopes repo,workflow
gh auth status

# Confirm your agent CLI is installed and logged in:
claude --version             # or: codex --version
```

---

## Configure

Configuration is split in two: **secrets** go in `.env`, **topology and
behavior** go in a YAML file.

### 1. Secrets — `.env`

```bash
cp .env.example .env
$EDITOR .env
```

```bash
LINEAR_API_KEY=lin_api_paste_here

# Optional. Set this to enable the loopback webhook receiver (faster pickup
# than polling). Generate once: openssl rand -hex 32
LINEAR_WEBHOOK_SECRET=

# Optional. If unset, the daemon falls back to `gh auth token`.
# GH_TOKEN=
```

### 2. Topology — `config.local.yaml`

```bash
cp examples/config.yaml config.local.yaml
$EDITOR config.local.yaml
```

A minimal working binding:

```yaml
poll_interval_secs: 60
global_max_concurrent: 4
workspace_root: ~/symphony/workspaces
log_root: ~/symphony/logs
db_path: ~/symphony/state.sqlite

repos:
  - linear_team_key: YOUR_TEAM_KEY     # e.g. ENG
    github_repo: owner/repo            # where PRs are opened
    agent: claude                      # claude | codex
    issue_label: symphony              # only issues with this label are picked up
    branch_prefix: symphony
    max_concurrent: 2
    runner: local
    linear_states:
      ready: Todo                      # SOURCE state — no default, you MUST set it
      in_progress: In Progress
      code_review: In Review
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
```

Notes:

- **State names must match your Linear workflow exactly.** `ready` has no
  default — every binding must declare which state issues are picked up from.
  `preflight` (below) verifies team keys and state names before anything runs.
- The full, line-commented reference for every field (cost caps, review
  strategy, multiple bindings, webhook tuning, the optional dependency-wait
  lane) lives in **[`examples/config.yaml`](examples/config.yaml)**.
- Using `agent: codex`? Add `codex_model` to the binding (e.g.
  `codex_model: gpt-5.1-codex`). `preflight` will set up and verify the Codex
  permissions profile it needs for unattended commits — no manual TOML editing.
  **Requires Codex CLI 0.136+:** the profile grants workspace + `.git` writes
  via the `:workspace_roots` filesystem token. Older builds used `:project_roots`,
  which current Codex silently ignores — leaving the workspace read-only so the
  agent can't commit. `preflight` rewrites a stale `:project_roots` profile
  automatically; re-run it after upgrading Codex.
- Using the remote `@codex review` bot? Install the **Codex GitHub App** on the
  target repo.

---

## Run

```bash
# 1. Validate config, Linear auth, team keys and state names. Always run first.
uv run symphony preflight --config config.local.yaml

# 2. Single poll tick — a safe smoke test. Picks up at most a tick's worth of
#    work, waits for scheduled tasks, then exits.
uv run symphony --config config.local.yaml --once

# 3. Run the daemon. It stays resident, polling Linear and driving issues
#    through to merge. Stop with Ctrl-C.
uv run symphony --config config.local.yaml
```

If `preflight` fails, fix `.env` / YAML before starting the daemon.

---

## Using it

### Start work on an issue

1. Add the configured `issue_label` to a Linear issue in a configured team.
2. Move it to the `ready` state (e.g. `Todo`).
3. Wait for the next poll tick (or webhook). The daemon comments "implement
   starting", moves the issue to `in_progress`, opens a PR, and takes it from
   there.

### Watch progress

**Linear is the main screen.** State changes and comments (start, activity,
review feedback, cost warnings, merge) tell the whole story.

Local inspection of the daemon's own state:

```bash
uv run symphony runs ls --db ~/symphony/state.sqlite
uv run symphony runs show <run_id> --db ~/symphony/state.sqlite
tail -f ~/symphony/logs/<run_id>.log
```

#### Optional: web dashboard

The daemon can serve a small status UI at `http://localhost:8787/ui/`. It is
**not** built by default (Node + [`pnpm`](https://pnpm.io/) required):

```bash
cd frontend
pnpm install
pnpm build          # writes frontend/dist/, which the daemon then serves
```

Restart the daemon and open `http://localhost:8787/ui/`.

### Operator commands

Steer a run by leaving a **top-level Linear comment** that starts with `$`
(not `/`):

| Command | Effect |
| --- | --- |
| `$stop` | Stop the active runner or review monitor |
| `$retry` | Retry in a supported wait context |
| `$approve` (or `👍`) | Approve / resume in a supported wait context |
| `$reject` | Reject a parked run and move it to the blocked state |
| `$skip-review` | Bypass the review verdict and merge directly |

Commands are context-specific (they act on whatever the issue is currently
waiting on). Free-form comments are **not** steering — only `$`-prefixed ones.

---

## Troubleshooting

**Issue wasn't picked up.** Check that it's in the `ready` state, has the
`issue_label`, and belongs to a configured `linear_team_key`. Re-run
`preflight`. Make sure concurrency caps (`max_concurrent`, `global_max_concurrent`)
aren't full.

**Changed the config but nothing changed.** Config is read once at startup.
Stop the process (Ctrl-C) and start it again.

**`preflight` fails.** Fix `.env` or the YAML it complains about before starting
the daemon — don't run with a bad config.

**Codex runs halt with "review fix-run completed without advancing" /
"Operation not permitted" on `.git`.** The Codex permissions profile is stale
or Codex is too old. Current Codex (0.136+) drops the legacy `:project_roots`
token the profile used to rely on, leaving the workspace read-only so the agent
edits files but can't commit. Upgrade Codex (`codex --version` ≥ 0.136), then
re-run `uv run symphony preflight` to rewrite the `symphony-git` profile to the
`:workspace_roots` token. No daemon restart is needed — Codex re-reads
`~/.codex/config.toml` on each run.

**Agent process seems hung.** It's killed automatically after
`stall_timeout_secs` of no output. Inspect with
`uv run symphony runs show <run_id> --db ~/symphony/state.sqlite` and the run
log, or send `$stop` in Linear.
