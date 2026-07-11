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
  → optional local review / fix loop (local_code_review)
  → GitHub PR opened
  → CI and optional remote @codex review / fix loop
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

`.env` is also the home for any secret a binding's `env:` mapping references
(see below) — for example a Supabase access token for schema work:

```bash
# Personal access token from https://supabase.com/dashboard/account/tokens
MASHA2_SUPABASE_ACCESS_TOKEN=sbp_...
MASHA2_SUPABASE_PROJECT_REF=abcdefghijklmnop
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
    agent: claude                      # builder: implement + local-review fixes
    # reviewer_agent: codex            # local reviewer; default is opposite agent
    local_review: false                # in-workspace pre-PR reviewer loop
    remote_review: true                # @codex GitHub-bot PR review
    issue_label: symphony              # only issues with this label are picked up
    branch_prefix: symphony
    max_concurrent: 2
    runner: local
    linear_states:
      ready: Todo                      # SOURCE state — no default, you MUST set it
      in_progress: In Progress
      local_code_review: Local Code Review
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
  booleans, multiple bindings, webhook tuning, the optional dependency-wait
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

### Headless agents: MCP allowlist and per-binding env

Agents run headless — their prompts ban interactive auth flows (OAuth URLs,
browser logins, device codes); an agent that hits one stops and reports
`SYMPHONY_BLOCKED: <what the operator must authorize and where>` instead of
hanging the run. Two binding-level knobs keep agents on non-interactive paths:

- **`mcp_servers:`** — the MCP allowlist. Claude agents are spawned with
  `--strict-mcp-config` and an MCP config generated from this mapping, so they
  only see servers the binding explicitly grants. The default is **none**:
  user-level MCP servers (including OAuth-only ones like Supabase's) are
  invisible. Only grant servers that authenticate headlessly.
- **`env:`** — extra environment variables injected into the binding's agent
  processes. Values name keys in `.env`; secrets never live in the YAML, and a
  key missing at startup fails config load.

Example — make Supabase schema tasks autonomously completable via the
`supabase` CLI (`supabase db push`, `supabase gen types typescript`) instead of
the OAuth-only Supabase MCP server:

1. Create a personal access token at
   <https://supabase.com/dashboard/account/tokens>.
2. Put it in symphony's `.env`, e.g. `MASHA2_SUPABASE_ACCESS_TOKEN=sbp_...`
   (plus `MASHA2_SUPABASE_PROJECT_REF=<project-ref>`).
3. Reference it from the binding:

```yaml
    env:
      SUPABASE_ACCESS_TOKEN: MASHA2_SUPABASE_ACCESS_TOKEN
      SUPABASE_PROJECT_REF: MASHA2_SUPABASE_PROJECT_REF
```

The agent's `supabase` CLI picks up `SUPABASE_ACCESS_TOKEN` and never needs a
browser.

### Review configuration

Review is controlled per binding with two booleans:

| `local_review` | `remote_review` | Behavior |
| --- | --- | --- |
| `false` | `true` | Remote-only default: open the PR and run `@codex review`. |
| `true` | `false` | Local-only: run the in-workspace reviewer before PR/CI/merge. |
| `true` | `true` | Sequential: local reviewer first, then PR and `@codex review`. |
| `false` | `false` | No review: implement, open PR, pass CI, merge. |

`agent` is the builder: it handles implementation and any fix rounds requested
by the local reviewer. `reviewer_agent` selects that local reviewer; if omitted,
it defaults to the opposite agent family from `agent`.

The remote reviewer is the `@codex` GitHub bot. It uses the PR `code_review`
lane, while local review uses the `local_code_review` lane
(`linear_states.local_code_review`).

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

### Run with Docker Compose (daemon + Caddy)

A containerized stack runs the same daemon behind a [Caddy](https://caddyserver.com/)
reverse proxy that terminates HTTPS locally. The image bundles the full agent
toolchain (`claude`, `codex`, `gh`, `git`, `uv`, `node`); named volumes persist
all CLI auth and daemon state, so nothing sensitive is baked into the image.

```bash
cp .env.example .env                        # fill in secrets
cp examples/config.docker.yaml config.local.yaml
$EDITOR config.local.yaml                   # set team keys / repos

# One-time: log the CLIs into their (persisted) named volumes. All three use
# headless flows (device code / paste-back code), so nothing needs a reachable
# localhost port — these work unchanged in the container and on a remote VPS
# where the browser is on a different machine.
docker compose run --rm --entrypoint claude symphony auth login          # opens a URL, paste the code back
docker compose run --rm --entrypoint codex symphony login --device-auth  # prints a URL + one-time code
docker compose run --rm --entrypoint gh symphony auth login --git-protocol https --web  # github.com/login/device

docker compose up -d
```

> `codex login` **must** use `--device-auth`. The default flow starts an OAuth
> callback server on `localhost:1455`; inside the container (which owns no
> published port — Caddy only exposes 443/80) the browser redirect can't reach
> it, so login silently fails. `--device-auth` needs no inbound port.
>
> If your OpenAI org **disables device-code auth** (the one-time code is
> rejected by admin policy), use `./scripts/codex-login-docker.sh` instead: it
> runs the default browser flow in a one-off container with a port bridge
> (codex binds its callback to the container's loopback, which plain
> `-p 1455:1455` can't reach — the script forwards published traffic to it).
> On a remote VPS, run the same script through an SSH tunnel — the browser's
> `localhost:1455` redirect then lands on the VPS:
>
> ```bash
> ssh -L 1455:localhost:1455 <vps>   # then, on the VPS:
> ./scripts/codex-login-docker.sh
> ```
>
> Alternatives for headless setups: an API key
> (`codex login --with-api-key`) or copying a working `~/.codex/auth.json`
> into the `codex_auth` volume.

The UI is then at `https://localhost/ui/` (Caddy's internal cert — accept the
warning or trust the CA once), and the `/linear/webhook` + `/github/webhook`
receivers are reachable through the same origin. The daemon's http surface
stays bound to `127.0.0.1` inside the shared network namespace; only Caddy is
published. This is the foundation for the VPS move — the same stack runs there
with a public hostname swapped into the `Caddyfile`.

---

## Using it

### Start work on an issue

1. Add the configured `issue_label` to a Linear issue in a configured team.
2. Move it to the `ready` state (e.g. `Todo`).
3. Wait for the next poll tick (or webhook). The daemon comments "implement
   starting", moves the issue to `in_progress`, runs the optional
   `local_review` lane in `local_code_review`, opens a PR, and takes it
   through CI and optional remote review from there.

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

The web dashboard renders these same commands as per-issue buttons, enabled
only when valid for the issue's current status.

### Global pause / resume

The dashboard header has a **Pause** toggle — a daemon-level kill-switch. When
paused, the dispatch loop starts **no new runs** for Ready issues (via either
the poll scan or a webhook); in-flight runs and their review/merge/acceptance
follow-ups continue untouched. **Resume** restores normal dispatch. The toggle
is also exposed as an auth-gated `GET`/`POST /api/pause` endpoint.

The flag is in-memory: **a daemon restart clears it back to running.**

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
