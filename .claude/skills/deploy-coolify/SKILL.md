---
name: deploy-coolify
description: "Deploy the Symphony stack from scratch onto a Coolify VPS — drive the Coolify API end-to-end (deploy key, app, domain, operator files, DNS, Auth0, CLI logins, smoke test, live flow test). Use when the user wants to deploy/redeploy Symphony to Coolify or a new VPS, migrate the stack to another server, or debug a broken Coolify deployment. Battle-tested runbook: every step below was verified live (2026-07-11, symphony.vibecamp.ru) and every warning is a failure that actually happened."
---

# Deploy Symphony on Coolify

Drive the whole deployment yourself via the Coolify REST API + ssh. The
operator only does the steps that require their browser (OAuth logins,
Auth0 dashboard) or that move secrets / write as root on the server —
prepare exact commands/scripts for those and wait.

## Inputs to collect first

Ask for whatever is missing; verify each credential immediately after
receiving it (call the API, don't trust):

| Input | Notes |
|---|---|
| VPS with Coolify | panel usually at `http://<ip>:8000` (302 = alive) |
| `COOLIFY_API_KEY` | panel → Keys & Tokens → API tokens; verify `GET /api/v1/version` |
| ssh access | `ssh <user>@<host>`; check passwordless `sudo -n true` |
| Domain + DNS control | e.g. Gcore zone; A-record `<sub>` → VPS IP overrides a wildcard |
| Auth0 tenant | SPA app client_id; operator edits Application URIs |
| `.env` + `config.local.yaml` | from the current working deployment |

API call pattern. The panel speaks plain HTTP on the IP — do NOT send
the bearer token (or, worse, the deploy private key) over it from
outside. Either tunnel (`ssh -L 8000:localhost:8000 <host>`, then call
`http://127.0.0.1:8000`) or, when sshd blocks forwarding
("administratively prohibited"), run curl ON the host over ssh:

```bash
# pipe the token over stdin: a single-quoted $TOK would expand (empty) on
# the REMOTE shell, and a locally-expanded one would show up in remote ps
printf '%s' "$COOLIFY_API_KEY" | ssh <host> \
  'read -r TOK; curl -s -H "Authorization: Bearer $TOK" \
     http://127.0.0.1:8000/api/v1/<path>'
```

## Phase 1 — Coolify resource

1. **Repo access — deploy key.** Generate `ssh-keygen -t ed25519` in the
   scratchpad; add the public half to GitHub
   (`gh api repos/<owner>/<repo>/keys -f title=coolify-symphony -f key="$(cat <pub-file>)" -F read_only=true` — note `-f key=@path` would send the literal string, not the file);
   register the private half: `POST /security/keys {name, private_key}` → keep `uuid`.
2. **Project.** `GET /projects`; create if absent.
3. **Application.** `POST /applications/private-deploy-key` with
   `project_uuid`, `server_uuid` (from `GET /servers`),
   `environment_name: production`, `private_key_uuid`,
   `git_repository: git@github.com:<owner>/<repo>.git`, `git_branch: main`,
   `build_pack: dockercompose`,
   `docker_compose_location: "/docker-compose.coolify.yml"`
   (**leading slash required** — 422 without it), `instant_deploy: false`.
4. **Domain on the caddy service.**
   `PATCH /applications/{uuid}` with
   `docker_compose_domains: [{"name": "caddy", "domain": "https://<sub>.<domain>"}]`
   (array of objects; a plain map is rejected).
5. **"Connect To Predefined Network" must stay OFF** (resource settings).
   Coolify would inject its network into every service, which conflicts
   with `network_mode: service:caddy` on the symphony service. The compose
   already puts caddy on the external `coolify` network for Traefik.

## Phase 2 — operator files on the host (`/opt/symphony/`)

`config.local.yaml` + `.env` are **absolute host binds**, never
Coolify-managed mounts:

> ⚠️ Coolify re-parses the compose on EVERY deploy/restart and resets its
> managed file mounts (relative or `content:`) to the compose placeholder
> content. A real `.env` uploaded into a storage record was clobbered by a
> single restart — verified live. The Caddyfile alone stays `content:`-
> managed because its placeholder IS its real static config.

Prepare a script for the operator (secrets + root writes are theirs to
run; put it in the scratchpad and hand over the path):

```bash
# Build locally first: the config gets /data paths
#   (workspace_root: /data/workspaces, log_root: /data/logs,
#    db_path: /data/db/state.sqlite)
# and .env drops panel-only tokens (GCORE_API_KEY, COOLIFY_API_KEY lines).
scp /tmp/vps-config.yaml /tmp/vps.env "<host>:/tmp/"
ssh "<host>" '
  sudo install -d -m 755 /opt/symphony
  sudo install -m 400 -o 1000 -g 1000 /tmp/vps-config.yaml /opt/symphony/config.local.yaml
  sudo install -m 400 -o 1000 -g 1000 /tmp/vps.env         /opt/symphony/.env
  rm /tmp/vps-config.yaml /tmp/vps.env
'
```

Both files get mode 400: `config.local.yaml` can carry per-binding
`webhook_secret`s, so it is as sensitive as `.env`.

> ⚠️ **uid 1000, not root.** The container user is `symphony` (uid 1000);
> a root-owned 600 `.env` boots into `PermissionError` — happened.
> ⚠️ `.env` is a dotfile — verify with `ls -la`, not `ls -l` (lost 10
> minutes to that).

The compose sets `SYMPHONY_REQUIRE_AUTH0=1`: the daemon fails closed
before reconcile if the `AUTH0_*` triple is missing from `.env` — an
empty/placeholder env produces a crash-loop, not an open API. That's by
design; fix the file, don't remove the flag.

## Phase 3 — DNS + first deploy

1. A-record `<sub>` → VPS IP (Gcore: overrides the `*`→vercel wildcard
   for that name only). Verify: `dig +short <sub>.<domain> @<zone-ns>`.
2. Deploy: `GET /deploy?uuid=<app-uuid>` → response is
   `{"deployments": [{"deployment_uuid": …}]}` (nested in the array, not
   top-level). Poll `GET /deployments/{deployment_uuid}` (`status`, `logs` is a JSON
   string of `{output}` entries) until `finished`/`failed`. First build
   ≈ 5–15 min (full agent toolchain); later ones hit cache.
3. Traefik issues the LE cert automatically once DNS resolves.

Smoke (all through the public domain):

```
GET  /api/issues        → 401 {"detail":"missing bearer token"}   (Auth0 gate)
POST /linear/webhook    → 401 {"detail":"invalid signature"}       (own HMAC — outside the gate; 404 is EXPECTED when LINEAR_WEBHOOK_SECRET is unset — the receiver isn't registered)
GET  /ui/               → 200
TLS                     → valid LE cert
GET  /api/auth-config   → {"enabled":true,...}
```

502 = daemon down (check container logs over ssh), 503 = containers being
recreated mid-deploy, 404 on 80/443 = Traefik up but no route (domain not
attached).

## Phase 4 — Auth0

Operator adds to the SPA app (Application URIs, comma-appended, then
**Save Changes**):

- Allowed Callback URLs: `https://<sub>.<domain>/ui/` — **trailing slash
  and `/ui/` path are significant** (the SPA sends `${origin}/ui/`)
- Allowed Logout URLs: same
- Allowed Web Origins: `https://<sub>.<domain>` (no path)

"Callback URL mismatch" after this = exact-string diff between the error
page's `redirect_uri=` param and the saved entry.

## Phase 5 — CLI logins into the named volumes

Find real names first: `docker volume ls | grep -E 'claude|codex|gh'` and
image from `docker ps` (Coolify prefixes both with the app uuid). All
three logins are one-time; volumes survive redeploys.

- **claude** — paste-back flow, no port:
  `ssh -t <host> 'sudo docker run --rm -it -v <claude-vol>:/home/symphony/.claude --entrypoint claude <image> auth login'`
  Credentials land in `.claude/.credentials.json` INSIDE the volume; the
  later warning about missing `~/.claude.json` is cosmetic (that file is
  outside the volume and regenerates). Verify with a live call:
  `--entrypoint claude <image> -p "Reply with exactly one word: ok"`.
- **gh** — device flow: `… --entrypoint gh <image> auth login --git-protocol https --web`.
- **codex** — layered fallbacks, in order:
  1. `codex login --device-auth` — unless the OpenAI org admin disabled
     device codes (rejected one-time code).
  2. Port bridge + ssh tunnel: `ssh -L 1455:localhost:1455 <host>`, then
     run `scripts/codex-login-docker.sh` semantics on the host (node
     forwarder `0.0.0.0:14550 → 127.0.0.1:1455`, publish
     `127.0.0.1:1455:14550`). codex binds its callback to container
     loopback, so plain `-p 1455:1455` gives "empty reply".
  3. **sshd blocks forwarding** ("administratively prohibited")? Run the
     bridge container anyway, open the printed auth URL in the local
     browser, let the redirect to `localhost:1455` fail, then deliver the
     browser's full callback URL manually on the VPS:
     `curl 'http://127.0.0.1:1455/auth/callback?code=…&state=…'` → 302 =
     accepted. Code is single-use, ~10 min TTL; state must match the live
     container.
  Verify: `--entrypoint codex <image> login status` → "Logged in using ChatGPT".

The daemon self-provisions the codex `symphony-git` permissions profile
at boot, and claude builder runs carry an explicit `--allowedTools`
(PR #297) — no manual permission setup in the volumes.

## Phase 6 — cutover + live test

1. ⚠️ **One daemon only.** Stop any other stack polling the same Linear
   teams (`docker compose down` on the old machine) — separate SQLite
   DBs mean no shared dedup → double dispatch. The VPS DB starts fresh;
   that's safe (source of truth = Linear/GitHub), only local history is
   lost.
2. Full-flow test: create a tiny real issue (label `symphony`, state
   Todo, use the ticket template with "Where to verify"). The daemon
   polls every 60 s. Watch `docker logs` for
   `dispatching <ID> …` → `move_issue … In Progress → Local Code Review
   → In Review → Done`, and the dashboard for the live stream. A
   docs-only ticket completes in ~5 min.
3. Optionally re-point Linear/GitHub webhooks to
   `https://<sub>.<domain>/{linear,github}/webhook` (polling covers the
   gap) and set `TELEGRAM_*` in `/opt/symphony/.env` + restart.

## Troubleshooting index (each of these actually happened)

| Symptom | Cause → fix |
|---|---|
| deploy fails `not a directory` mounting a file | short-form bind to a gitignored/absent file: docker created a dir. Use Coolify `content:` mounts for static files, `/opt/…` host binds for operator files |
| daemon: `ValidationError … input_value=None` | config file mounted empty (placeholder/dir) — check `/opt/symphony` contents & the generated compose binds |
| daemon: `PermissionError: '.env'` | file owned root:600 — `chown 1000:1000`, mode 400; container keeps the old inode until recreated |
| real file content reverts after restart | it was a Coolify-managed mount — move to `/opt/symphony` |
| 422 `docker_compose_location format invalid` | add the leading slash |
| 422 `docker compose domains must be an array` | `[{"name": svc, "domain": url}]` |
| storage API PATCH updates DB but not disk | only record *creation* writes the file; don't rely on storages for operator data |
| Traefik 504s intermittently | caddy on two networks → two IPs; keep it on the `coolify` network only |
| codex login redirect dead-ends | see Phase 5 fallback chain |
