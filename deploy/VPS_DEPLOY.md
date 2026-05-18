# Symphony VPS deploy

Пошаговая инструкция для деплоя `symphonyd` на Ubuntu 24.04 VPS и запуска через
`systemd`. Webhook слушает только `127.0.0.1:8787`, наружу он публикуется через
Cloudflare Tunnel.

Перед продакшен-деплоем желательно уже проверить локальный happy path:
`Implement -> Review -> Merge -> Done`.

## 1. Подготовить VPS

Зайти на свежий VPS под `root`:

```bash
ssh root@YOUR_VPS
```

Установить базовые пакеты и создать отдельного пользователя:

```bash
apt-get update
apt-get install -y ca-certificates curl git gnupg jq nodejs npm openssh-client python3 rsync sqlite3 sudo wget

adduser --disabled-password --gecos "" symphony
usermod -aG sudo symphony
install -d -o symphony -g symphony -m 0755 /opt/symphonyd
```

Установить `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh
uv --version
```

Установить GitHub CLI:

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

Установить Cloudflare Tunnel:

```bash
mkdir -p --mode=0755 /usr/share/keyrings
curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg > /usr/share/keyrings/cloudflare-main.gpg
echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main" > /etc/apt/sources.list.d/cloudflared.list
apt-get update
apt-get install -y cloudflared
cloudflared --version
```

## 2. Установить агентские CLI

Команды выполнять на VPS под пользователем `symphony`:

```bash
sudo -iu symphony

npm config set prefix "$HOME/.local"
echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.profile"
export PATH="$HOME/.local/bin:$PATH"

npm install -g @anthropic-ai/claude-code @openai/codex
claude --version
codex --version

exit
```

## 3. Подготовить `.env` и залить код

На локальной машине, из корня репозитория `symphonyd`:

```bash
cd /Users/ak/Code/symphonyd
export VPS=root@YOUR_VPS

test -f .env || cp .env.example .env
openssl rand -hex 32
$EDITOR .env
```

В `.env` должны быть минимум:

```bash
LINEAR_API_KEY=lin_api_...
LINEAR_WEBHOOK_SECRET=<hex_from_openssl_rand>
```

`LINEAR_WEBHOOK_SECRET` должен совпадать с signing secret в настройках Linear
webhook.

Залить код и секреты на VPS:

```bash
rsync -a --delete --exclude .git --exclude .venv --exclude .env ./ "$VPS:/opt/symphonyd/"
scp .env "$VPS:/tmp/symphonyd.env"
ssh "$VPS" 'install -o symphony -g symphony -m 0600 /tmp/symphonyd.env /opt/symphonyd/.env && rm -f /tmp/symphonyd.env && chown -R symphony:symphony /opt/symphonyd'
```

## 4. Создать `config.yaml`

На VPS:

```bash
ssh root@YOUR_VPS
sudo -iu symphony
cd /opt/symphonyd

uv sync
cp examples/config.yaml config.yaml
mkdir -p ~/symphony/workspaces ~/symphony/logs
nano config.yaml

exit
```

В `config.yaml` проверить:

```yaml
workspace_root: ~/symphony/workspaces
log_root: ~/symphony/logs
db_path: ~/symphony/state.sqlite
webhook_host: 127.0.0.1
webhook_port: 8787
```

Для каждого `repos[]` настроить:

- `linear_team_key`: ключ Linear team, например `ENG`.
- `github_repo`: GitHub repo в формате `owner/repo`.
- `agent`: `claude` или `codex`.
- `issue_label`: Linear label, по которому symphonyd подбирает тикеты.
- `linear_states.ready`: состояние, из которого symphonyd берет задачи.
- `linear_states.in_progress`, `needs_approval`, `blocked`, `done`: реальные
  названия workflow states в Linear.

Пример binding:

```yaml
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    agent: codex
    codex_model: gpt-5.1-codex
    issue_label: symphony
    branch_prefix: symphony
    max_concurrent: 2
    runner: local
    linear_states:
      ready: Todo
      in_progress: In Progress
      needs_approval: Needs Approval
      blocked: Blocked
      done: Done
```

## 5. Авторизовать headless-инструменты

На VPS под `symphony`:

```bash
sudo -iu symphony

gh auth login --hostname github.com --git-protocol ssh --scopes repo,workflow
gh auth status

claude --print "hello"
codex --version

exit
```

Также нужно установить Codex GitHub App на каждый репозиторий из
`/opt/symphonyd/config.yaml`. Review stage постит `@codex review`, и бот сможет
работать только в репозиториях, где app установлен.

Если хотя бы один binding может запускать локальный Codex CLI, `symphony
preflight` создаст профиль Codex `symphony-git` в `~/.codex/config.toml`, если
других permissions-профилей еще нет. Если permissions уже настроены вручную без
`symphony-git`, preflight попросит добавить профиль руками, не переписывая
пользовательский TOML. Binding-и, которые используют только Claude и remote
`@codex review`, этот локальный профиль не требуют. После этого можно проверить
тот же режим, который daemon использует для unattended `implement` и
`review_fix`:

```bash
codex exec --json \
  --config 'default_permissions="symphony-git"' \
  --config 'approval_policy="never"' \
  --model gpt-5.1-codex \
  "say hello"
```

## 6. Прогнать preflight

```bash
sudo -iu symphony
cd /opt/symphonyd
.venv/bin/symphony preflight --config /opt/symphonyd/config.yaml
exit
```

Если preflight не проходит, не запускать daemon. Сначала исправить
`LINEAR_API_KEY`, `linear_team_key` или названия Linear states в `config.yaml`.

## 7. Включить `systemd`

На VPS под `root`:

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

В логах должно быть видно, что webhook receiver слушает `127.0.0.1:8787`.
Он не должен слушать `0.0.0.0`.

## 8. Настроить Cloudflare Tunnel

Создать tunnel под пользователем `symphony`:

```bash
sudo -iu symphony

cloudflared tunnel login
cloudflared tunnel create symphonyd
cloudflared tunnel list
```

Скопировать tunnel UUID из `cloudflared tunnel list`, затем:

```bash
install -d -m 0700 "$HOME/.cloudflared"
cp /opt/symphonyd/deploy/cloudflared/config.yaml "$HOME/.cloudflared/config.yml"
nano "$HOME/.cloudflared/config.yml"
```

В `~/.cloudflared/config.yml` заменить `<TUNNEL_ID>` и hostname:

```yaml
tunnel: <TUNNEL_ID>
credentials-file: /home/symphony/.cloudflared/<TUNNEL_ID>.json

ingress:
  - hostname: symphonyd.example.org
    service: http://127.0.0.1:8787
  - service: http_status:404
```

Проверить и привязать DNS:

```bash
cloudflared tunnel route dns symphonyd symphonyd.example.org
cloudflared tunnel ingress validate --config "$HOME/.cloudflared/config.yml"
exit
```

Установить tunnel service:

```bash
cloudflared --config /home/symphony/.cloudflared/config.yml service install
systemctl enable --now cloudflared.service
systemctl status cloudflared.service --no-pager
```

В Linear webhook настроить URL:

```text
https://symphonyd.example.org/linear/webhook
```

Signing secret в Linear должен быть тем же значением, что
`LINEAR_WEBHOOK_SECRET`.

## 9. Smoke test

Открыть логи:

```bash
journalctl -u symphonyd.service -f
```

В Linear:

1. Создать или выбрать ticket в настроенной team.
2. Добавить configured `issue_label`, если binding его требует.
3. Перевести ticket в configured `ready` state.
4. Проверить в логах webhook delivery и single dispatch.
5. Проверить, что ticket перешел в `in_progress`.
6. Проверить, что в GitHub появился branch и PR.
7. Проверить, что PR получил `@codex review`.
8. Дождаться перехода `Review -> Merge -> Done`.

Если webhook временно не работает, poll loop остается fallback. Успешный webhook
delivery не должен запускать дубль на следующем poll tick.

## 10. Операции

Рестарт daemon:

```bash
systemctl restart symphonyd.service
```

Рестарт Cloudflare Tunnel:

```bash
systemctl restart cloudflared.service
```

Логи:

```bash
journalctl -u symphonyd.service -n 200 --no-pager
journalctl -u cloudflared.service -n 200 --no-pager
journalctl -u symphonyd-maintenance.service -n 50 --no-pager
```

Один poll tick вручную:

```bash
sudo -iu symphony -- sh -lc 'cd /opt/symphonyd && .venv/bin/symphony --config /opt/symphonyd/config.yaml --once'
```

Запустить maintenance вручную:

```bash
systemctl start symphonyd-maintenance.service
journalctl -u symphonyd-maintenance.service -n 50 --no-pager
```

Обновить код с локальной машины:

```bash
cd /Users/ak/Code/symphonyd
export VPS=root@YOUR_VPS

ssh "$VPS" 'systemctl stop symphonyd.service'
rsync -a --delete --exclude .git --exclude .venv --exclude .env ./ "$VPS:/opt/symphonyd/"
ssh "$VPS" 'chown -R symphony:symphony /opt/symphonyd && sudo -iu symphony -- sh -lc "cd /opt/symphonyd && uv sync" && systemctl start symphonyd.service'
```

Важно: `config.yaml` читается один раз при старте. После любого изменения
конфига нужен:

```bash
systemctl restart symphonyd.service
```
