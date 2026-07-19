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

Configuration comes from exactly two places: **`.env`** for bootstrap secrets,
and the **web UI** for everything operational (provider connections, bindings,
the roles matrix, runtime knobs). There is no config file.

### 1. Secrets and bootstrap — `.env`

```bash
cp .env.example .env
$EDITOR .env
```

```bash
# Auth0 gates the UI/API (mandatory for any exposed deployment).
AUTH0_DOMAIN=your-tenant.auth0.com
AUTH0_CLIENT_ID=...
AUTH0_ALLOWED_EMAILS=you@example.com

# Encryption key for stored provider credentials. Optional — auto-generated
# into the data volume (0600) on first boot if unset. Set it to pin the key.
# SYMPHONY_ENCRYPTION_KEY=

# Optional. Set to enable the loopback webhook receiver (faster than polling).
# Generate once: openssl rand -hex 32
# LINEAR_WEBHOOK_SECRET=
# GITHUB_WEBHOOK_SECRET=

# Optional path overrides (defaults: ~/symphony/{state.sqlite,logs,workspaces}).
# The container sets these to /data/* — see docker-compose.yml.
# SYMPHONY_DB_PATH=~/symphony/state.sqlite
# SYMPHONY_LOG_ROOT=~/symphony/logs
# SYMPHONY_WORKSPACE_ROOT=~/symphony/workspaces
```

`.env` is also the home for any secret a binding's `env:` mapping references
(see below) — for example a Supabase access token for schema work:

```bash
# Personal access token from https://supabase.com/dashboard/account/tokens
MASHA2_SUPABASE_ACCESS_TOKEN=sbp_...
MASHA2_SUPABASE_PROJECT_REF=abcdefghijklmnop
```

### 2. Providers and topology — the web UI

Everything operational lives in the DB and is edited in the UI — no file to
copy or hand-edit:

- **Connections page** — connect **GitHub**, **Linear**, **Claude**, and
  **Codex**. Provider and agent credentials are stored encrypted in the DB;
  agent runs consume them through per-run credential dirs. (GitHub may
  alternatively authenticate via `gh`/`GH_TOKEN` — see the Docker section.)
- **Config page** — add **bindings** (Linear team key → GitHub repo, issue
  label, branch prefix, per-lane Linear state names, review booleans,
  concurrency, `agent`/`reviewer_agent`), edit the **roles matrix**
  (agent/model/effort per role, with per-binding overrides), and tune
  **runtime knobs** (poll/reconcile intervals, caps, timeouts).

A fresh install boots with **zero bindings**; you add them in the UI after
connecting providers. `preflight` (below) verifies each binding's team keys and
Linear state names.

Notes:

- **State names must match your Linear workflow exactly.** Every binding must
  declare which state issues are picked up from (`ready`); there is no default.
- Using `agent: codex` on a binding? Set its `codex_model` (e.g.
  `gpt-5.1-codex`). `preflight` sets up and verifies the Codex permissions
  profile needed for unattended commits — no manual TOML editing.
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
- **`env`** — extra environment variables injected into the binding's agent
  processes. Values name keys in `.env`; the secret itself is never stored on
  the binding, and a key missing at startup fails config assembly.

Example — make Supabase schema tasks autonomously completable via the
`supabase` CLI (`supabase db push`, `supabase gen types typescript`) instead of
the OAuth-only Supabase MCP server:

1. Create a personal access token at
   <https://supabase.com/dashboard/account/tokens>.
2. Put it in symphony's `.env`, e.g. `MASHA2_SUPABASE_ACCESS_TOKEN=sbp_...`
   (plus `MASHA2_SUPABASE_PROJECT_REF=<project-ref>`).
3. Reference it from the binding's `env` mapping (Config page → binding →
   advanced), keying each agent-visible variable to its `.env` name:

```json
"env": {
  "SUPABASE_ACCESS_TOKEN": "MASHA2_SUPABASE_ACCESS_TOKEN",
  "SUPABASE_PROJECT_REF": "MASHA2_SUPABASE_PROJECT_REF"
}
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
# 1. Validate provider auth, team keys and state names for the bindings in the
#    DB. Always run first (after connecting providers + adding bindings).
uv run symphony preflight

# 2. Single poll tick — a safe smoke test. Picks up at most a tick's worth of
#    work, waits for scheduled tasks, then exits.
uv run symphony --once

# 3. Run the daemon. It stays resident, polling Linear and driving issues
#    through to merge. Stop with Ctrl-C.
uv run symphony
```

Config assembles from `.env` + the DB — there is no config-file argument. The
daemon serves the UI, so the first run of a fresh install is: start the daemon,
open the UI, log in, connect providers, add a binding. If `preflight` fails,
fix `.env` or the binding in the UI before starting the daemon.

### Run with Docker Compose (daemon + Caddy)

A containerized stack runs the same daemon behind a [Caddy](https://caddyserver.com/)
reverse proxy that terminates HTTPS locally. The image bundles the full agent
toolchain (`claude`, `codex`, `gh`, `git`, `uv`, `node`); named volumes persist
all CLI auth and daemon state, so nothing sensitive is baked into the image.

```bash
cp .env.example .env                        # fill in secrets (Auth0 triple + optional webhook secrets)
docker compose up -d
```

Then open `https://localhost/ui/` (Caddy's internal cert — accept the warning
or trust the CA once), log in via Auth0, and on the **Connections** page
connect GitHub, Linear, Claude, and Codex. Add your bindings on the **Config**
page. Provider and agent credentials live encrypted in the DB (a named volume)
— nothing is baked into the image and no CLI login step is needed.

> **GitHub via `gh` (optional).** Instead of connecting GitHub in the UI you
> can authenticate the `gh` CLI into its persisted volume — the env/volume
> fallback for GitHub still works:
>
> ```bash
> docker compose run --rm --entrypoint gh symphony auth login --git-protocol https --web
> ```

The `/linear/webhook` + `/github/webhook` receivers are reachable through the
same origin. The daemon's http surface stays bound to `127.0.0.1` inside the
shared network namespace; only Caddy is published. This is the foundation for
the VPS move — the same stack runs there with a public hostname swapped into
the `Caddyfile`.

### Deploy on Coolify

Use `docker-compose.coolify.yml` (compose-file path in the resource settings).
It keeps the same caddy⇄daemon topology but drops host port publishing —
Coolify's proxy terminates TLS on the public domain and forwards to caddy:80.
The file's header comment is the deployment guide: the file mount for `.env`
(gitignored, so absent from the clone — and secrets must NOT go through
Coolify's env-vars UI) and the "Connect To Predefined Network" toggle. After
the first deploy, log in and connect the four providers on the UI Connections
page, then point the Linear/GitHub webhooks and the Auth0
callback/logout/origin URLs at the new domain.

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

The web dashboard is the second screen. Served at `/ui/` behind the Auth0
gate, it shows the active issues, streams live agent output, exposes per-issue
command buttons, and offers a global pause. See
[Optional: web dashboard](#optional-web-dashboard) below to build it.

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
