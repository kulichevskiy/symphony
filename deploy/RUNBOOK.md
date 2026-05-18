# VPS Deployment Runbook

This runbook deploys `symphonyd` on an Ubuntu 24.04 VPS with the webhook receiver bound to `127.0.0.1` and exposed only through Cloudflare Tunnel.

Do not start this deployment until the operator has already demonstrated the local Implement -> Review -> Merge -> Done happy path end-to-end. That check is manual by design and is the gate for issue 14.

## 1. Provision the VPS

Run as `root` on a fresh Ubuntu 24.04 host:

```bash
apt-get update
apt-get install -y ca-certificates curl git gnupg jq nodejs npm openssh-client python3 rsync sqlite3 sudo wget
adduser --disabled-password --gecos "" symphony
usermod -aG sudo symphony
install -d -o symphony -g symphony -m 0755 /opt/symphonyd
```

Install `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh
uv --version
```

Install GitHub CLI:

```bash
(type -p wget >/dev/null || (apt-get update && apt-get install wget -y)) \
  && mkdir -p -m 755 /etc/apt/keyrings \
  && out="$(mktemp)" \
  && wget -nv -O "$out" https://cli.github.com/packages/githubcli-archive-keyring.gpg \
  && cat "$out" > /etc/apt/keyrings/githubcli-archive-keyring.gpg \
  && chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
  && mkdir -p -m 755 /etc/apt/sources.list.d \
  && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list \
  && apt-get update \
  && apt-get install -y gh
gh --version
```

Install Cloudflare Tunnel:

```bash
mkdir -p --mode=0755 /usr/share/keyrings
curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg > /usr/share/keyrings/cloudflare-main.gpg
echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main" > /etc/apt/sources.list.d/cloudflared.list
apt-get update
apt-get install -y cloudflared
cloudflared --version
```

Install Claude Code and Codex CLI as the `symphony` user:

```bash
sudo -iu symphony
npm config set prefix "$HOME/.local"
echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.profile"
export PATH="$HOME/.local/bin:$PATH"
npm install -g pnpm @anthropic-ai/claude-code @openai/codex
claude --version
codex --version
pnpm --version
exit
```

## 2. Install symphonyd

Run from the operator workstation, from the repo root. Replace `root@symphonyd.example.org` with the VPS SSH target:

```bash
export VPS=root@symphonyd.example.org
test -f .env || cp .env.example .env
openssl rand -hex 32
${EDITOR:-vi} .env
rsync -a --delete --exclude .git --exclude .venv --exclude .env --exclude frontend/node_modules --exclude frontend/dist ./ "$VPS:/opt/symphonyd/"
scp .env "$VPS:/tmp/symphonyd.env"
ssh "$VPS" 'install -o symphony -g symphony -m 0600 /tmp/symphonyd.env /opt/symphonyd/.env && rm -f /tmp/symphonyd.env && chown -R symphony:symphony /opt/symphonyd'
```

Put generated hex values in `.env` as `LINEAR_WEBHOOK_SECRET` and
`GITHUB_WEBHOOK_SECRET`. Use the Linear value when configuring the Linear
webhook signing secret, and the GitHub value when configuring GitHub repository
webhooks. A repo can override the global GitHub secret with
`repos[].webhook_secret` in `/opt/symphonyd/config.yaml`.

Run on the VPS as `root`:

```bash
sudo -iu symphony
cd /opt/symphonyd
uv sync
cd frontend
pnpm install
pnpm build
cd ..
cp examples/config.yaml config.yaml
exit
```

Edit `/opt/symphonyd/config.yaml`:

```bash
nano /opt/symphonyd/config.yaml
```

Set:

- `webhook_host: 127.0.0.1`
- `webhook_port: 8787`
- `reconcile_interval_secs: 300`, `reconcile_max_per_tick: 50`,
  `reconcile_max_actions_per_tick: 10`, and `reconcile_backoff_secs: 600`
  unless you need a quieter or more aggressive external-truth audit cadence.
- `workspace_root`, `log_root`, and `db_path` to directories writable by `symphony`.
- Each `repos[].github_repo` to a watched GitHub repo.
- Leave `repos[].webhook_enabled: true` for watched repos that should accept
  GitHub webhook events; set it to `false` for repos that should keep polling
  only.
- Leave `repos[].reconcile_enabled: true` for repos whose operator waits and
  open PRs should be included in the background external-observation scan.
- If `GITHUB_WEBHOOK_SECRET` is unset, every repo with
  `repos[].webhook_enabled: true` must define `repos[].webhook_secret`.
- Each `repos[].linear_team_key`, `issue_label`, and `linear_states` entry to match the Linear workspace.

Keep `SYMPHONY_RECONCILE_DRYRUN` unset for observe-only rows with
`action_taken='observed'`. Set `SYMPHONY_RECONCILE_DRYRUN=1` during a dry-run
window when drift rows should use `action_taken='would_clear'` without mutating
SQLite. Set `SYMPHONY_RECONCILE_DRYRUN=0` only after the dry-run audit is clean;
that enables monotonic auto-clear for `merge_zombie`, `pr_locally_merged`, and
`pr_closed_no_merge`. `linear_state_done` is noted in the timeline but leaves
the operator wait intact.

Auto-clear rollout:

1. Verify dry-run history: `SELECT drift_kind, COUNT(*) FROM external_observations WHERE drift_kind IS NOT NULL GROUP BY drift_kind;`
2. Spot-check at least 5 `would_clear` rows per drift kind and confirm each would have been correct.
3. Set `SYMPHONY_RECONCILE_DRYRUN=0` and restart `symphonyd.service`.
4. Watch `action_taken='cleared'` rows for 24 hours and confirm zero false positives.
5. If anything looks wrong, set `SYMPHONY_RECONCILE_AUTOCLEAR_DISABLED=1` and restart; this returns the reconciler to observe-only behavior.

## 3. Authenticate headless tools

Run as the `symphony` user:

```bash
sudo -iu symphony
gh auth login --hostname github.com --git-protocol ssh --scopes repo,workflow
gh auth status
claude --print "hello"
codex --version
exit
```

Configure the Codex permissions profile that Symphony uses for unattended
`implement` and `review_fix` runs. `symphony preflight` creates this block in
`~/.codex/config.toml` if no permissions profiles exist yet. If an operator has
already customized Codex permissions without defining `symphony-git`, preflight
fails and asks for the profile to be added manually instead of rewriting custom
TOML:

```toml
[permissions.symphony-git.filesystem]
":root" = "read"
"/tmp" = "write"

[permissions.symphony-git.filesystem.":project_roots"]
"." = "write"
".git" = "write"
".agents" = "read"
".codex" = "read"

[permissions.symphony-git.network]
enabled = false
```

The runner deliberately does not pass `--sandbox workspace-write`. In current
Codex versions that sandbox mode keeps `.git` read-only even when extra config
or writable roots are provided, so agents can edit files but fail to create the
commit that Symphony needs before opening a PR. The named profile is the
permissions boundary for Symphony-managed worktrees and explicitly grants
`.git` writes inside the project root.

After the profile exists, smoke-test the argv shape that Symphony uses:

```bash
codex exec --json \
  --config 'default_permissions="symphony-git"' \
  --config 'approval_policy="never"' \
  --model gpt-5.1-codex \
  "say hello"
```

Install the Codex GitHub app on every watched repo listed in `/opt/symphonyd/config.yaml`. The review stage posts `@codex review`, and that bot can only inspect repositories where the app is installed.

Run the built-in preflight:

```bash
sudo -iu symphony
cd /opt/symphonyd
.venv/bin/symphony preflight --config /opt/symphonyd/config.yaml
exit
```

## 4. Enable symphonyd

Run as `root`:

```bash
cp /opt/symphonyd/deploy/systemd/symphonyd.service /etc/systemd/system/symphonyd.service
cp /opt/symphonyd/deploy/systemd/symphonyd-maintenance.service /etc/systemd/system/symphonyd-maintenance.service
cp /opt/symphonyd/deploy/systemd/symphonyd-maintenance.timer /etc/systemd/system/symphonyd-maintenance.timer
systemctl daemon-reload
systemctl enable --now symphonyd.service
systemctl enable --now symphonyd-maintenance.timer
systemctl status symphonyd.service --no-pager
systemctl list-timers symphonyd-maintenance.timer --no-pager
journalctl -u symphonyd.service -f
```

The unit must show a webhook listener on `127.0.0.1:8787` when `LINEAR_WEBHOOK_SECRET` is set. It must never listen on `0.0.0.0`.

The maintenance timer runs daily. It reads `db_path` and `log_root` from `/opt/symphonyd/config.yaml`, creates online-safe SQLite backups next to the DB using `sqlite3 .backup`, keeps the 7 newest backups by default, and prunes `*.log` files older than 14 days by default. Override the defaults by editing `SYMPHONYD_BACKUP_KEEP` and `SYMPHONYD_LOG_RETENTION_DAYS` in `/etc/systemd/system/symphonyd-maintenance.service`, then run `systemctl daemon-reload`.

SQLite now runs in WAL mode. Expect `state.sqlite-wal` and `state.sqlite-shm`
companion files next to `state.sqlite` while the daemon is running. For
backup/restore, keep using `sqlite3 .backup` or stop the daemon before copying
the database files directly so the WAL contents are not split from the main DB.

To run maintenance immediately:

```bash
systemctl start symphonyd-maintenance.service
journalctl -u symphonyd-maintenance.service -n 50 --no-pager
```

## 5. Set up Cloudflare Tunnel

Create a tunnel as the `symphony` user:

```bash
sudo -iu symphony
cloudflared tunnel login
cloudflared tunnel create symphonyd
cloudflared tunnel list
```

Copy the tunnel UUID from `cloudflared tunnel list`, then install the sample config. The UUID must replace both the top-level `tunnel` value and the credentials filename placeholder:

```bash
install -d -m 0700 "$HOME/.cloudflared"
cp /opt/symphonyd/deploy/cloudflared/config.yaml "$HOME/.cloudflared/config.yml"
sed -i 's/<TUNNEL_ID>/PASTE_TUNNEL_UUID_HERE/g' "$HOME/.cloudflared/config.yml"
sed -i 's/symphonyd.example.com/symphonyd.example.org/g' "$HOME/.cloudflared/config.yml"
cloudflared tunnel route dns symphonyd symphonyd.example.org
cloudflared tunnel ingress validate --config "$HOME/.cloudflared/config.yml"
exit
```

Install the tunnel service:

```bash
cloudflared --config /home/symphony/.cloudflared/config.yml service install
systemctl enable --now cloudflared.service
systemctl status cloudflared.service --no-pager
```

Configure Linear to deliver webhooks to:

```text
https://symphonyd.example.org/linear/webhook
```

Use the same signing secret as `LINEAR_WEBHOOK_SECRET`.

Configure each watched GitHub repository under **Settings -> Webhooks**:

- Payload URL: `https://symphonyd.example.org/github/webhook`
- Content type: `application/json`
- Secret: the matching `repos[].webhook_secret` when set, otherwise
  `GITHUB_WEBHOOK_SECRET`
- Events: select **Pull requests** and **Issue comments** only

The receiver accepts `pull_request` actions `closed`, `merged`, and `reopened`,
and `issue_comment` action `created`. Other GitHub events return 200 without
triggering the reconciler hook.

## 6. Smoke test with Linear

Keep logs open:

```bash
journalctl -u symphonyd.service -f
```

Then:

1. In Linear, create or pick a ticket for a configured team.
2. Add the configured `issue_label` if the binding requires one.
3. Drag the ticket into the configured `ready` state.
4. Confirm the logs show a webhook delivery followed by a single dispatch.
5. Confirm the ticket moves into the configured in-progress state.
6. Confirm the watched repo receives the implementation branch and PR.
7. Confirm the PR receives an `@codex review` comment and advances through Review -> Merge -> Done.

If the webhook path is down, the poll loop remains a fallback. A successful webhook delivery should not double-fire on the next poll tick.

## 7. Operations

Common commands:

```bash
systemctl restart symphonyd.service
systemctl restart cloudflared.service
systemctl start symphonyd-maintenance.service
journalctl -u symphonyd.service -n 200 --no-pager
journalctl -u cloudflared.service -n 200 --no-pager
journalctl -u symphonyd-maintenance.service -n 50 --no-pager
sudo -iu symphony -- sh -lc 'cd /opt/symphonyd && .venv/bin/symphony --config /opt/symphonyd/config.yaml --once'
```

To update from the operator workstation:

```bash
export VPS=root@symphonyd.example.org
ssh "$VPS" 'systemctl stop symphonyd.service'
rsync -a --delete --exclude .git --exclude .venv --exclude .env --exclude frontend/node_modules --exclude frontend/dist ./ "$VPS:/opt/symphonyd/"
ssh "$VPS" 'chown -R symphony:symphony /opt/symphonyd && sudo -iu symphony -- sh -lc "cd /opt/symphonyd && uv sync && cd frontend && pnpm install && pnpm build" && systemctl start symphonyd.service'
```
