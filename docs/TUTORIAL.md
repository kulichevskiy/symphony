# symphonyd Tutorial

Этот документ - учебный manual по `symphonyd`: как объяснять систему другим,
что у нее под капотом, как ее установить, как ей пользоваться и как развивать
кодовую базу без случайных поломок.

Используйте его не как справочник "прочитать подряд", а как сценарий обучения:
сначала дать человеку mental model, потом показать один happy path, затем
провести через типовые сбои и только после этого отправлять в код.

Главная фраза для первого объяснения:

> `symphonyd` - это headless orchestrator. Он смотрит на Linear, берет
> подходящие issue, запускает локального AI-агента в отдельном GitHub
> workspace, открывает PR, ведет Review/Fix/Merge loop и оставляет оператору
> видимые receipts в Linear.

## 0. Как пользоваться этим мануалом

Если вы учите operator, installer или developer, не давайте всем одинаковую
глубину. У каждого другая задача:

| Роль | Что человек должен уметь после обучения | Главные разделы |
| --- | --- | --- |
| Operator | Запускать работу из Linear, понимать прогресс, останавливать и диагностировать stuck issue | 1, 2, 4, 6, 7, 10 |
| Installer | Поставить систему локально или на VPS, настроить секреты, config, webhook и сервис | 2, 3, 5, 7, 10 |
| Developer | Менять lifecycle, команды, runner, review/merge logic и не ломать recovery | 1, 3, 4, 7, 8, 9 |

Минимальный результат первого занятия: ученик может нарисовать поток
`Linear -> SQLite -> workspace -> agent -> GitHub PR -> review -> merge -> Linear`
и объяснить, какой слой является источником правды в каждой точке.

Проверяйте понимание не вопросом "прочитал ли код", а практическими кейсами:
"issue в Linear выглядит зависшим", "PR уже merged, но Linear не Done",
"поменяли config, но поведение не изменилось", "Codex оставил review comment".
Хороший ответ всегда включает Linear, GitHub и SQLite вместе.

## 1. Как учить других

### 1.1. Цель обучения

После первого занятия человек должен уметь объяснить:

- из какого Linear state система берет задачи;
- как Linear issue маршрутизируется в конкретный GitHub repository;
- зачем нужны одновременно Linear, GitHub и SQLite;
- что происходит на стадиях `implement`, `review`, `review_fix` и `merge`;
- чем локальный agent CLI отличается от GitHub-бота `@codex review`;
- какие команды оператор может оставить в Linear comments;
- какие файлы менять для config, intake, runner, review, merge, persistence и
  deployment.

Не начинайте обучение с чтения `poll.py`. Сначала дайте человеку карту
системы, затем один happy path, и только после этого открывайте код.

### 1.2. 10-минутная версия

Объясняйте так:

1. Пользователь готовит Linear issue: нужная team, нужный workflow state,
   нужный label.
2. `symphonyd` видит issue через poll или webhook.
3. Оркестратор создает durable run в SQLite, двигает issue в `in_progress`,
   берет per-issue workspace и запускает agent CLI.
4. Агент меняет код и делает commit, но не push.
5. Оркестратор пушит branch, открывает PR и постит `@codex review`.
6. Issue переходит в review lane, а review monitor начинает читать CI,
   review comments, reactions и mergeability.
7. Если CI или review feedback требуют исправления, оркестратор запускает
   `review_fix` agent run, пушит fix и снова постит `@codex review`.
8. Когда review approved и PR mergeable, merge stage делает финальную проверку,
   вызывает `gh pr merge`, затем двигает Linear issue в `done`.
9. Если что-то требует человека, система паркует issue в configured review или
   blocked state и ждет Linear command.

### 1.3. Happy path на доске

```text
Linear issue in ready state + optional label
  -> poll/webhook schedules issue
  -> SQLite: runs(stage=implement, status=running)
  -> Linear: move to in_progress
  -> workspace: clone or reuse per-issue branch
  -> LocalRunner starts claude/codex CLI
  -> agent commits locally
  -> orchestrator pushes branch
  -> GitHub PR created
  -> PR comment: @codex review
  -> Linear: move to needs_approval / In Review
  -> SQLite: runs(stage=review, status=running)
  -> review monitor handles CI/review feedback
  -> SQLite: runs(stage=merge, status=running)
  -> gh pr merge
  -> Linear: move to done
  -> workspace cleanup
```

### 1.4. Три источника правды

Когда кто-то говорит "issue завис", нельзя смотреть только на Linear UI.
У системы три слоя:

| Layer | Что знает | Для чего нужен |
| --- | --- | --- |
| Linear | Issue state, labels, comments, operator commands | Human-facing workflow |
| SQLite | Runs, PR mapping, review state, comment cursors, waits | Durable memory of daemon |
| GitHub | Branch, PR, reviews, checks, reactions, merge state | Code-review and merge truth |

Практическое правило: сначала проверить Linear card, затем GitHub PR, затем
SQLite rows. Один слой часто выглядит "зависшим", хотя другой слой уже ушел
дальше.

### 1.5. Где показывать код новичку

Покажите эти файлы в таком порядке:

1. `examples/config.yaml` - как Linear teams связаны с GitHub repos.
2. `src/symphony/config.py` - модель config и env/YAML loading.
3. `src/symphony/cli.py` - entrypoint, `preflight`, `dispatch`, `runs`.
4. `src/symphony/db/schema.sql` - durable state.
5. `src/symphony/orchestrator/poll.py` - главный stateful orchestration loop.
6. `src/symphony/workspace.py` - per-issue git workspaces.
7. `src/symphony/agent/runners/local.py` - subprocess runner.
8. `src/symphony/github/client.py` - wrapper over `gh`.
9. `src/symphony/linear/client.py` and `src/symphony/linear/slash.py` -
   Linear GraphQL and operator commands.
10. `src/symphony/pipeline/review_classifier.py` - pure review decision logic.

После этого можно читать `docs/linear-issue-lifecycle.md`: это более глубокая
карта lifecycle, edge cases и design inventory. Некоторые пункты в ней могут
быть уже закрыты текущим кодом, поэтому перед планированием работы всегда
сверяйте выводы с `poll.py`, `review_classifier.py`, tests и свежим `git diff`.

### 1.6. Как провести обучение

Удобный формат - не лекция по файлам, а последовательность из четырех проходов:

| Проход | Цель | Что показывать |
| --- | --- | --- |
| Карта | Дать mental model | Linear -> SQLite -> workspace -> GitHub -> Linear |
| Happy path | Показать нормальный поток | Один issue от ready state до PR/review/merge |
| Failure path | Научить не паниковать | Stuck review, failing CI, cost cap, merged PR without Done |
| Code path | Связать поведение с кодом | `config.py`, `cli.py`, `poll.py`, `review_classifier.py`, DB schema |

Для первого занятия держите фокус на вопросе "кто сейчас владеет issue?".
Владельцем может быть оператор в Linear, daemon, local agent, GitHub CI,
Codex reviewer или GitHub merge queue. Если ученик умеет определить владельца
по Linear/GitHub/SQLite, он уже понимает систему на практическом уровне.

### 1.7. План занятия на 90 минут

```text
00-10  Проблема: зачем нужен symphonyd и какие ручные шаги он автоматизирует.
10-25  Архитектура: пять плоскостей - Linear, GitHub, SQLite, workspace, agent CLI.
25-40  Config: как Linear team/state/label маршрутизируются в GitHub repo.
40-55  Happy path: Implement -> Review -> Review fix -> Merge -> Done.
55-70  Операции: runs ls/show, logs, gh pr view/checks, SQLite inspection.
70-80  Commands: $stop, $retry, $approve, $reject, $skip-review; где они работают.
80-90  Development map: какие файлы и тесты менять для типовых задач.
```

Для 30-минутного onboarding оставьте только первые четыре блока и один
короткий troubleshooting пример: "Linear says In Review, что проверяем?".

Для half-day workshop добавьте hands-on:

1. Поднять local config на sandbox Linear team/repo.
2. Запустить `preflight`.
3. Сделать `--once`.
4. Запустить ручной `dispatch`.
5. Найти run в SQLite и лог по `run_id`.
6. Смоделировать `$stop` или review failure на тестовом issue.

### 1.8. Три разные аудитории

Не всем нужно одинаковое объяснение.

| Аудитория | Что важно | Что можно отложить |
| --- | --- | --- |
| Operator | Как запускать, смотреть прогресс, останавливать, возобновлять | Внутренности prompt builders |
| Installer | VPS, `.env`, `config.yaml`, `systemd`, Cloudflare Tunnel, auth | Review classifier details |
| Developer | State machine, DB schema, side effects, tests, failure modes | VPS hardening details |

Если человек будет только пользоваться системой, не грузите его `poll.py`.
Если человек будет менять систему, он обязан понимать, что `poll.py` - это
место, где большинство внешних side effects сшиты с durable state.

### 1.9. Live-demo script

Хорошая демонстрация должна показывать не только успех, но и наблюдаемость.

Перед demo:

```bash
uv sync
uv run symphony preflight --config config.local.yaml
gh auth status
codex --version
```

Во время demo держите рядом четыре окна:

```bash
uv run symphony --config config.local.yaml
tail -f logs/*.log
uv run symphony runs ls --db state.sqlite
gh pr view <number> --repo owner/repo --comments
```

Объясняйте вслух каждую границу:

- "Linear state changed, теперь daemon может claim issue."
- "Run row появился до запуска agent, чтобы restart не потерял работу."
- "Agent делает commit локально, но push/PR/review делает orchestrator."
- "GitHub review bot и local Codex CLI - разные исполнители."
- "Если UI выглядит stuck, мы не угадываем; проверяем Linear, GitHub, SQLite."

### 1.10. Что не надо обещать ученикам

Не учите будущие behavior contracts как уже готовые возможности.

- Free-form Linear comments сейчас не являются steering.
- Config не reloadится без restart процесса.
- Один Linear state вроде `In Review` может означать разные внутренние
  состояния.
- Не каждая ошибка чинится командой; иногда нужен inspection SQLite/GitHub.
- SQLite нельзя воспринимать как disposable cache: это durable память daemon.

## 2. Что делает система

`symphonyd` связывает Linear teams с GitHub repositories. Один binding в config
говорит:

- какую Linear team смотреть;
- какой GitHub repository обслуживать;
- какой agent CLI использовать: `claude` или `codex`;
- какой Linear label нужен для pickup, если `issue_label` задан;
- из какого Linear state брать issue;
- куда двигать issue во время implement, review, blocked и done;
- сколько задач можно вести параллельно;
- какой branch prefix и merge strategy использовать.

Минимальная модель:

```text
Linear = control plane
GitHub = code-review and merge plane
SQLite = durable orchestration memory
Local workspace = execution plane
Agent CLI = code-changing worker
```

The orchestrator owns side effects: Linear moves/comments, PR creation, review
triggering, merge attempts, SQLite rows and workspace lifecycle.

The agent owns code edits inside the checked-out branch. It should commit
locally, but it should not push, create PRs or merge.

## 3. Что под капотом

### 3.1. Process and CLI

Entrypoint из `pyproject.toml`:

```bash
symphony = "symphony.cli:main"
```

Основные команды:

```bash
uv run symphony --config config.local.yaml
uv run symphony --config config.local.yaml --once
uv run symphony preflight --config config.local.yaml
uv run symphony dispatch ADJ-123 --config config.local.yaml
uv run symphony runs ls --db state.sqlite
uv run symphony runs show <run_id> --db state.sqlite
```

`--once` делает один poll tick, дожидается scheduled tasks и выходит. Это
удобный smoke test.

Config читается при старте процесса. Если поменяли `.env`, YAML config,
model, paths, state names, caps или bindings, нужен restart процесса.

### 3.2. Config layers

Config специально разделен на два слоя:

- `.env` - secrets: `LINEAR_API_KEY`, `LINEAR_WEBHOOK_SECRET`, optional
  `GH_TOKEN`;
- YAML - topology and behavior: repos, paths, Linear states, caps, agent,
  branch prefix, merge strategy.

Минимальный local config:

```yaml
poll_interval_secs: 60
global_max_concurrent: 4
workspace_root: ./workspaces
log_root: ./logs
db_path: ./state.sqlite
webhook_host: 127.0.0.1
webhook_port: 8787

repos:
  - linear_team_key: ADJ
    github_repo: adjust/creative-management-suite
    agent: codex
    codex_model: gpt-5.1-codex
    issue_label: symphony
    branch_prefix: symphony
    max_concurrent: 2
    runner: local
    linear_states:
      ready: Todo
      in_progress: In Progress
      needs_approval: In Review
      blocked: In Review
      done: Done

review_iteration_cap: 12
cost_cap_per_issue_usd: 100.0
cost_warning_pct: 75
stall_timeout_secs: 300
```

Important config fields:

| Field | Meaning |
| --- | --- |
| `linear_team_key` | Linear team key such as `ADJ` |
| `github_repo` | GitHub repo in `owner/repo` format |
| `agent` | `claude` or `codex` for local code-changing runs |
| `codex_model` | Required for Codex CLI pricing and command building |
| `issue_label` | Optional Linear label gate; if omitted, binding can catch all team issues |
| `linear_states.ready` | Source state for automatic pickup |
| `linear_states.needs_approval` | Review/operator-wait lane, often named `In Review` |
| `merge_strategy` | Strategy passed to `gh pr merge`, default `squash` |
| `allow_auto_merge` | Whether to call `gh pr merge --auto` |
| `cost_cap_usd` | Per-binding override; `0` disables cap for that binding |

State names must exactly match Linear workflow names. `preflight` exists mainly
to catch wrong team keys and state names before the daemon starts doing work.

### 3.3. Intake: poll and webhook

There are two intake paths:

- poll loop scans configured Linear teams for issues in `linear_states.ready`;
- webhook receives Linear events at `POST /linear/webhook` when
  `LINEAR_WEBHOOK_SECRET` is set.

Webhook is a latency optimization. It does not create a second behavior model.
Both paths converge into the same scheduling logic.

Safety behavior:

- Linear webhook signature is verified with HMAC.
- Webhook timestamps must be fresh.
- Delivery IDs are deduped in `webhook_deliveries`.
- Before a scheduled issue runs, the daemon re-checks team, state and label.
- SQLite plus in-memory scheduled sets prevent duplicate dispatch.

### 3.4. SQLite persistence

SQLite is not a cache; it is the daemon's memory. Without it, restart,
dedupe, review continuation, cost caps and operator waits become unreliable.

Schema lives in `src/symphony/db/schema.sql`.

Key tables:

| Table | Purpose |
| --- | --- |
| `issues` | Cached Linear issue identity, title and team |
| `runs` | Every `implement`, `review`, `review_fix` and `merge` run |
| `issue_prs` | Linear issue to GitHub PR mapping |
| `review_state` | Current PR number, iteration count and trigger signature |
| `comment_cursors` | Linear comment polling cursor |
| `comment_events` | Dedup of handled Linear comments |
| `webhook_deliveries` | Dedup of webhook deliveries |
| `issue_cost_marks` | Once-per-issue cost warning state |
| `activity_comment_marks` | Rate limiting for Codex activity digests |
| `operator_waits` | Durable waits that survive daemon restart |

Useful inspection:

```bash
uv run symphony runs ls --db state.sqlite
uv run symphony runs show <run_id> --db state.sqlite
sqlite3 state.sqlite '.tables'
sqlite3 state.sqlite 'select id, issue_id, stage, status, started_at, ended_at from runs order by started_at desc limit 20;'
sqlite3 state.sqlite 'select issue_id, github_repo, pr_number, pr_url, merged_at from issue_prs order by created_at desc limit 20;'
sqlite3 state.sqlite 'select * from review_state;'
sqlite3 state.sqlite 'select * from operator_waits;'
```

### 3.5. Workspaces

Every issue gets a private git clone:

```text
{workspace_root}/{repo_safe}/{issue_identifier_lower}/
```

Example:

```text
./workspaces/adjust_screative-management-suite/adj-123/
```

The workspace persists across Implement, Review fix-runs and Merge. This saves
clone time and keeps branch context available for retries.

Workspace behavior:

- clone if missing;
- fetch if present;
- switch to local branch if it exists;
- track remote branch if it exists;
- create branch from current HEAD otherwise;
- wipe interrupted non-git residue before reclone;
- cleanup after terminal merge finalization;
- TTL sweep stale issue dirs.

Relevant file: `src/symphony/workspace.py`.

### 3.6. Runner and agent process

Current runner implementation is local subprocess execution:

- `src/symphony/agent/runner.py` defines the protocol.
- `src/symphony/agent/runners/local.py` implements `LocalRunner`.

`LocalRunner`:

- starts `claude` or `codex` with `asyncio.create_subprocess_exec`;
- runs in the workspace directory;
- creates a new process session;
- streams stdout and stderr as events;
- emits `tick` events while waiting;
- writes raw stdout/stderr to `{log_root}/{run_id}.log`;
- detects stall after `stall_timeout_secs`;
- sends SIGTERM, then SIGKILL if needed;
- supports `kill(run_id)` for `$stop`, shutdown and cost caps.

Runner commands are built in `src/symphony/orchestrator/poll.py`:

```text
claude --print --output-format stream-json --verbose [--max-budget-usd N] <prompt>

codex exec --json --sandbox workspace-write --model <codex_model> <prompt>
```

### 3.7. Prompts

Stage prompts live in `src/symphony/agent/prompt.py`.

Current prompt types:

| Prompt | Used by | Purpose |
| --- | --- | --- |
| `implement_prompt` | `implement` | Satisfy Linear issue, write tests, commit locally |
| `review_fix_prompt` | `review_fix` from CI | Fix failing required CI with log tail first |
| `review_comment_fix_prompt` | `review_fix` from review comments | Address Codex or human review feedback |
| `merge_conflict_fix_prompt` | `review_fix` from merge conflict | Resolve conflict markers after orchestrator starts rebase |
| `merge_prompt` | `merge` | Final local cleanup pass before merge |

Teaching point: issue text and comments are task input, not system authority.
Developers should treat them as untrusted content when evolving prompts.

### 3.8. GitHub wrapper

GitHub side effects go through `src/symphony/github/client.py`, which wraps
`gh` as argv lists instead of ad hoc shell strings.

It is responsible for:

- cloning repositories;
- discovering default branch;
- creating PRs;
- posting PR comments such as `@codex review`;
- reading PR view, checks, reviews, review comments, issue comments and
  reactions;
- reading failed Actions log tails;
- merging PRs;
- listing branches.

GitHub auth is not in YAML. It comes from `gh auth` or `GH_TOKEN`.

### 3.9. Review classifier

Review decision logic is pure and testable in
`src/symphony/pipeline/review_classifier.py`.

It returns one of:

- `APPROVED`;
- `CHANGES_REQUESTED`;
- `PENDING`.

Priority rules:

1. Failing required or unknown-required CI -> `CHANGES_REQUESTED`.
2. Pending required CI -> `PENDING`.
3. `mergeable=CONFLICTING` -> `CHANGES_REQUESTED`.
4. Codex inline review comments on current HEAD -> `CHANGES_REQUESTED`.
5. Substantive Codex review body on current HEAD -> `CHANGES_REQUESTED`.
6. Human `CHANGES_REQUESTED` on current HEAD -> `CHANGES_REQUESTED`.
7. Codex `+1` reaction after HEAD commit time or human approval -> `APPROVED`
   when mergeable.
8. Approval with unknown mergeability -> `PENDING`.

The classifier also emits a stable `trigger_signature`. The orchestrator stores
that signature in `review_state` so the same feedback does not dispatch the
same fix-run forever.

### 3.10. Activity and cost

Agent stdout can include usage events. `src/symphony/agent/process.py` parses:

- Claude `result` events with `total_cost_usd`;
- Codex `token_count` and `turn.completed` events.

Codex cost is estimated from token pricing in
`src/symphony/agent/codex_models.py`.

Cost behavior:

- cost accumulates per issue across runs;
- warning fires once when threshold is crossed;
- cap kills active runner and parks the issue for operator action;
- per-binding overrides can adjust or disable caps.

For Codex implement and review-fix runs, activity digests can be posted back to
Linear. They are rate-limited and deduped by `activity_comment_marks`.

## 4. Lifecycle in detail

### 4.1. Implement

Implement starts when an issue is dispatchable.

Dispatchable means:

- issue belongs to a configured `linear_team_key`;
- issue is in `linear_states.ready`;
- if `issue_label` is set, issue has that label;
- global and per-binding concurrency caps have room;
- no active or already completed run blocks duplicate dispatch.

Implement sequence:

1. Upsert `issues`.
2. Create `runs(stage='implement', status='running')`.
3. Post "Implement starting" Linear comment.
4. Move Linear issue to `linear_states.in_progress`.
5. Acquire workspace.
6. Run agent CLI.
7. Track logs, activity and cost.
8. On clean exit, push branch.
9. Create PR.
10. Post Linear stage transition comment.
11. Mark implement run `completed`.
12. Start Review stage.

If the runner fails, push fails or PR creation fails, the run is marked failed
and the issue is reset or parked according to stage behavior.

### 4.2. Review

Review starts after PR creation.

Start behavior:

- parse PR number from PR URL;
- upsert `review_state`;
- upsert `issue_prs`;
- post `@codex review` on the PR;
- move Linear issue to `linear_states.needs_approval`;
- create `runs(stage='review', status='running')`.

The name `needs_approval` is config-level vocabulary. In some workspaces it is
actually the visible `In Review` lane, not a human-only approval lane. Teach
people to inspect config before assuming ownership.

Review monitor behavior:

- reads required checks;
- reads PR view and mergeability;
- when CI is clean, also reads reviews, review comments and reactions;
- classifies current state with `review_classifier`;
- posts a one-time Linear "Codex reviewed - no issues" notice if Codex has an
  LGTM issue comment;
- dispatches fix-runs for failing CI, review comments and merge conflicts;
- dedupes repeated triggers by signature;
- increments review iteration count;
- parks the issue when `review_iteration_cap` is reached.

Review-fix behavior:

- create `runs(stage='review_fix', status='running')`;
- run the configured local agent CLI, not the GitHub review bot;
- push the fix branch;
- post Linear "Fix pushed" comment when available;
- post a fresh `@codex review` on the PR;
- let the review monitor classify the new head again.

### 4.3. Merge conflict handling

If review classification sees `mergeable=CONFLICTING`, the review monitor can
dispatch a merge-conflict fix-run.

The orchestrator:

- resolves base branch from config or GitHub default branch;
- fetches remote;
- starts a rebase;
- collects conflicted files;
- asks the agent to edit only conflict markers;
- stages files and continues the rebase;
- pushes the result;
- re-triggers `@codex review`.

This is still a review-stage fix-run, not the final merge stage.

### 4.4. Merge

Merge candidates come from unmerged `issue_prs` rows.

Before scheduling merge, the orchestrator:

- revalidates Linear issue state and label;
- checks there is no active conflicting run;
- reads GitHub PR state;
- finalizes if the PR was already externally merged or closed;
- runs the review classifier again;
- schedules merge only when verdict is approved and mergeable.

Merge sequence:

1. Create `runs(stage='merge', status='running')`.
2. Acquire workspace.
3. Sync workspace to remote branch.
4. Run merge prompt for final local cleanup.
5. Push branch.
6. Call `gh pr merge` using configured strategy and auto-merge setting.
7. Poll/verify merge finalization.
8. Move Linear issue to `done`.
9. Mark `issue_prs.merged_at`.
10. Mark merge run `done`.
11. Cleanup workspace.

If merge cannot complete, the system parks the run as `needs_approval` and
stores an `operator_wait` of kind `merge`.

### 4.5. Restart and recovery

At startup the daemon runs reconcile logic before normal orchestration. The
purpose is to avoid pretending a dead process is still active.

Important recovery concepts:

- live process state is not durable;
- SQLite rows are durable;
- active subprocess PIDs can die while rows still say `running`;
- review monitors can be resurrected for open PRs;
- operator waits survive restart via `operator_waits`;
- config is not hot-reloaded.

## 5. Installation

### 5.1. Local prerequisites

Install:

- Python 3.12;
- `uv`;
- `git`;
- GitHub CLI `gh`;
- at least one agent CLI: `codex` or `claude`;
- access to Linear API;
- GitHub permissions for configured repositories.

Authenticate GitHub:

```bash
gh auth login --hostname github.com --git-protocol ssh --scopes repo,workflow
gh auth status
```

Check Codex:

```bash
codex --version
codex exec --json --sandbox workspace-write --model gpt-5.1-codex "say hello"
```

Check Claude if using `agent: claude`:

```bash
claude --version
claude --print "hello"
```

Install Codex GitHub App on repositories where Review stage should use
`@codex review`.

### 5.2. Local setup

From repo root:

```bash
cd /Users/ak/Code/symphonyd
uv sync
cp .env.example .env
$EDITOR .env
cp examples/config.yaml config.local.yaml
$EDITOR config.local.yaml
mkdir -p workspaces logs
```

`.env` minimum:

```bash
LINEAR_API_KEY=lin_api_...
LINEAR_WEBHOOK_SECRET=
```

If you want local webhook, generate a secret:

```bash
openssl rand -hex 32
```

Put the same secret into `.env` and Linear webhook settings. The local endpoint
is:

```text
http://127.0.0.1:8787/linear/webhook
```

For a public Linear webhook, expose it with a tunnel. Production deployment in
this repo uses Cloudflare Tunnel and keeps the FastAPI server bound only to
`127.0.0.1`.

### 5.3. Preflight

Run:

```bash
uv run symphony preflight --config config.local.yaml
```

Preflight checks:

- `LINEAR_API_KEY` exists;
- Linear auth works;
- configured teams are visible;
- configured Linear state names exist.

If preflight fails, do not start the daemon. Fix `.env` or YAML first.

### 5.4. Smoke test

Run one poll tick:

```bash
uv run symphony --config config.local.yaml --once
```

Use this for safe validation after config changes. It does not stay resident.

### 5.5. Long-running local daemon

```bash
uv run symphony --config config.local.yaml
```

Leave it running in a terminal. Stop with Ctrl-C.

When webhook secret is configured, logs should show the receiver listening on:

```text
127.0.0.1:8787
```

### 5.6. VPS deployment

Use the dedicated deployment docs:

- `deploy/VPS_DEPLOY.md` - Russian step-by-step VPS guide;
- `deploy/RUNBOOK.md` - English operational runbook.

Production shape:

- Ubuntu 24.04 VPS;
- dedicated `symphony` user;
- code in `/opt/symphonyd`;
- config in `/opt/symphonyd/config.yaml`;
- secrets in `/opt/symphonyd/.env`;
- `systemd` service for daemon;
- `systemd` timer for maintenance backups and log pruning;
- webhook bound to `127.0.0.1:8787`;
- Cloudflare Tunnel exposes `/linear/webhook`;
- `gh`, `claude` and `codex` authenticated under the `symphony` user.

## 6. How to use it

### 6.1. Automatic pickup from Linear

To start normal work:

1. Create or choose a Linear issue in a configured team.
2. Add the configured `issue_label`, if the binding has one.
3. Move the issue to `linear_states.ready`.
4. Wait for webhook or next poll tick.

Expected visible behavior:

- Linear comment says implement is starting;
- issue moves to `in_progress`;
- local logs appear under `log_root`;
- branch appears in GitHub;
- PR is opened;
- PR receives `@codex review`;
- issue moves to review lane;
- review/fix/merge comments appear as needed;
- issue eventually moves to `done`.

### 6.2. Manual dispatch

Manual dispatch launches an issue regardless of current state:

```bash
uv run symphony dispatch ADJ-123 --config config.local.yaml
```

Use it for intentional per-issue restart or debugging routing.

Manual dispatch still resolves binding by team and label. If one team fans out
to multiple repos, labels matter.

### 6.3. Watch progress

Linear is the main operator UI. Watch:

- state changes;
- starting comments;
- activity digest comments;
- review feedback comments;
- cost warning/cap comments;
- final merge comment.

Local inspection:

```bash
tail -f logs/*.log
uv run symphony runs ls --db state.sqlite
uv run symphony runs show <run_id> --db state.sqlite
```

GitHub inspection:

```bash
gh pr view <number> --repo owner/repo --json state,merged,mergedAt,mergeable,headRefOid,url
gh pr checks <number> --repo owner/repo --required
gh pr view <number> --repo owner/repo --comments
```

### 6.4. Operator commands in Linear

Commands are top-level Linear comments. They start with `$`, not `/`.
Mirrored GitHub comments are ignored by the Linear command parser.

| Command | Meaning |
| --- | --- |
| `$stop` | Stop active runner or active review monitor when supported |
| `$retry` | Retry in supported operator-wait contexts |
| `$approve` | Approve/resume in supported operator-wait contexts |
| `$reject` | Reject a supported parked run and move to blocked state |
| `$skip-review` | Cancel active review monitor and dispatch merge directly |
| `👍`, `:+1:`, `:+1` | Parsed as `$approve` |

Supported contexts:

| Context | Behavior |
| --- | --- |
| Active `implement`, `review_fix` or `merge` runner | `$stop` kills runner |
| Active review monitor | `$stop` cancels monitor |
| Active review monitor with PR | `$skip-review` bypasses review verdict and schedules merge |
| Cost-cap wait | `$approve`/`$retry` move issue back to ready; `$reject`/`$stop` block |
| Review-failed wait | `$approve`/`$retry` restarts review monitor; `$reject`/`$stop` block |
| Merge needs-approval wait | `$approve`/`$retry` re-dispatch merge; `$reject`/`$stop` block |

Do not teach free-form Linear comments as steering yet. The parser ignores
free-form text today even if some older templates mention it.

### 6.5. Restart vocabulary

People often say "restart" and mean different things:

| Type | Meaning | Typical action |
| --- | --- | --- |
| Daemon restart | Reload process and config | Ctrl-C/start again or `systemctl restart symphonyd.service` |
| Per-issue dev restart | Run one issue again | `uv run symphony dispatch ADJ-123 --config config.local.yaml` |
| Full local reset | Clear demo/local orchestration state | Back up DB/workspaces/logs, then intentionally clear them |

Config changes need daemon restart. A stuck single issue usually needs DB/GitHub
inspection and maybe `dispatch`, not a process restart.

## 7. Troubleshooting

### 7.1. Issue was not picked up

Check:

- Is issue in `linear_states.ready`?
- Does issue have `issue_label`?
- Is `linear_team_key` correct?
- Does `preflight` see team and states?
- Are concurrency caps full?
- Is there already a running or completed run for this issue?
- Did webhook deliver but get deduped?

Commands:

```bash
uv run symphony preflight --config config.local.yaml
uv run symphony runs ls --db state.sqlite
sqlite3 state.sqlite 'select issue_id, stage, status from runs order by started_at desc limit 20;'
```

### 7.2. Issue is stuck in review

Do not assume review is the blocker. Check all three surfaces:

1. Linear issue state and latest comments.
2. GitHub PR state, checks, reviews, comments, reactions and mergeability.
3. SQLite rows in `runs`, `issue_prs`, `review_state`, `operator_waits`.

Useful commands:

```bash
gh pr view <number> --repo owner/repo --json state,merged,mergedAt,mergeable,headRefOid,url
gh pr checks <number> --repo owner/repo --required
sqlite3 state.sqlite 'select id, issue_id, stage, status, started_at, ended_at from runs order by started_at desc limit 30;'
sqlite3 state.sqlite 'select * from review_state;'
sqlite3 state.sqlite 'select * from operator_waits;'
```

Common meanings of "stuck in review":

- review monitor is still polling;
- required CI is pending;
- required CI failed and fix-run is queued or running;
- Codex or human review feedback is being handled;
- merge conflict fix-run is needed;
- iteration cap parked the issue;
- PR is approved but merge stage is waiting for capacity;
- PR already merged but finalization lagged;
- daemon is not running or cannot reach GitHub/Linear.

### 7.3. PR is merged but Linear is not Done

This is usually merge finalization lag, not review failure.

Check:

```bash
gh pr view <number> --repo owner/repo --json merged,mergedAt,state,url
sqlite3 state.sqlite 'select issue_id, github_repo, pr_number, merged_at from issue_prs order by created_at desc limit 20;'
sqlite3 state.sqlite 'select id, issue_id, stage, status from runs order by started_at desc limit 20;'
```

If GitHub says merged but `issue_prs.merged_at` is empty and Linear is not
`done`, the finalization path has not completed.

### 7.4. Config changed but behavior did not

Expected. Config is loaded once at startup.

Local:

```bash
# stop current process with Ctrl-C, then:
uv run symphony --config config.local.yaml
```

systemd:

```bash
systemctl restart symphonyd.service
```

### 7.5. Agent process hangs

`LocalRunner` has `stall_timeout_secs`. If there is no output for that period,
it terminates the process group.

Inspect:

```bash
uv run symphony runs show <run_id> --db state.sqlite
less logs/<run_id>.log
```

Operator action:

```text
$stop
```

### 7.6. Webhook does not work

Webhook starts only when `LINEAR_WEBHOOK_SECRET` is set.

Check:

- daemon logs show listener on `127.0.0.1:8787`;
- tunnel points to `/linear/webhook`;
- Linear webhook uses the same signing secret;
- payload timestamp is fresh;
- `webhook_deliveries` does not show unexpected pending duplicates;
- poll fallback still works.

### 7.7. `gh pr checks --required` returns no required checks

The GitHub wrapper treats "no checks reported" and "no required checks
reported" as an empty required-check set. If this fails again, inspect
`src/symphony/github/client.py` and `tests/test_github_client.py`.

### 7.8. Linear state looks ambiguous

In some configs, both `needs_approval` and `blocked` can point to a visible
state named `In Review`. That means the lane name alone does not tell you
whether review is active, human approval is needed or merge finalization lagged.

Use SQLite and GitHub to decide.

## 8. How to develop the system

### 8.1. Reading path for new developers

Read in this order:

1. `examples/config.yaml`
2. `src/symphony/config.py`
3. `src/symphony/cli.py`
4. `src/symphony/db/schema.sql`
5. `src/symphony/orchestrator/poll.py`
6. `src/symphony/workspace.py`
7. `src/symphony/agent/runner.py`
8. `src/symphony/agent/runners/local.py`
9. `src/symphony/pipeline/review_classifier.py`
10. `src/symphony/linear/slash.py`
11. `tests/test_review_stage.py`
12. `tests/test_merge_stage.py`
13. `tests/test_slash.py`

Then read `docs/linear-issue-lifecycle.md` for broader lifecycle design. Treat
it as design inventory, not as automatically-current truth; verify every gap
against the current code and tests.

### 8.2. Change map

| If changing... | Start here | Tests |
| --- | --- | --- |
| CLI commands | `src/symphony/cli.py` | `tests/test_cli_runs.py`, `tests/test_preflight.py` |
| Config fields | `src/symphony/config.py` | `tests/test_config.py` |
| Linear API | `src/symphony/linear/client.py`, `queries.py` | `tests/test_linear_client.py` |
| Webhook | `src/symphony/webhook.py` | `tests/test_webhook.py` |
| Dispatch/poll loop | `src/symphony/orchestrator/poll.py` | `tests/test_scheduler.py`, `tests/test_poll_dedupe.py`, `tests/test_state_machine.py` |
| Review stage | `poll.py`, `review_classifier.py` | `tests/test_review_stage.py`, `tests/test_review_classifier.py` |
| Merge stage | `poll.py`, `github/client.py` | `tests/test_merge_stage.py`, `tests/test_github_client.py` |
| Slash commands | `src/symphony/linear/slash.py`, `poll.py` handlers | `tests/test_slash.py`, `tests/test_slash_polling.py` |
| Workspace behavior | `src/symphony/workspace.py` | `tests/test_workspace.py` |
| Runner behavior | `src/symphony/agent/runners/local.py` | `tests/test_runner_local.py`, `tests/test_agent_process.py` |
| Activity comments | `src/symphony/agent/activity.py`, `poll.py` | `tests/test_activity_comments.py` |
| DB schema | `src/symphony/db/schema.sql`, `src/symphony/db/*.py` | `tests/test_db.py`, `tests/test_review_state_db.py` |
| Cost guard | `src/symphony/pipeline/cost_guard.py`, `poll.py` | `tests/test_cost_guard.py`, `tests/test_cost_cap_e2e.py` |

### 8.3. Development loop

Use targeted tests first:

```bash
uv run pytest tests/test_slash.py
uv run pytest tests/test_review_stage.py
uv run pytest tests/test_merge_stage.py
uv run pytest tests/test_github_client.py
```

Then broader checks:

```bash
uv run pytest
uv run ruff check src tests
uv run mypy
git diff --check
```

Prefer tests with fakes/mocks over real Linear/GitHub calls. The test suite
already has fakes for Linear, GitHub, Runner and Workspace.

### 8.4. Design rules

1. Keep Linear-visible ownership clear. Every handoff should leave a receipt.
2. Persist before external side effects when duplicates would be harmful.
3. Make side effects idempotent. Assume crash/retry can happen after any
   `await`.
4. Do not run agents without a SQLite run row.
5. Do not merge on stale approval after branch head changes.
6. Do not overwrite human branch changes silently.
7. Treat issue body and comments as untrusted task input.
8. Keep config state names exact and preflightable.
9. Use `src/symphony/github/client.py` for GitHub interactions.
10. Add focused regression tests for every lifecycle transition you touch.

### 8.5. Known limitations to teach honestly

Do not oversell the current system.

- Free-form Linear steering is ignored.
- `$approve`, `$retry` and `$reject` are context-specific, not universal.
- Config reload is not hot.
- Some visible Linear states can blur review monitoring and human approval.
- Command author allowlist is not fully modeled.
- Merge stage can still change branch head during final cleanup; target
  behavior should force re-review after any new merge-stage commit.
- Some recovery paths still require inspecting SQLite and GitHub directly.

Use `docs/linear-issue-lifecycle.md` as a source of backlog candidates for
deeper lifecycle gaps. Before creating or fixing a ticket, verify that the gap
still exists in the current implementation.

### 8.6. Как безопасно развивать систему

Работайте от behavior contract, а не от "поменять пару строк в `poll.py`".
Почти любая фича в `symphonyd` затрагивает минимум один visible side effect:
Linear comment/state, GitHub PR/check/review, SQLite row или workspace branch.

Перед изменением запишите короткий contract:

```text
When <event happens>,
and <current durable state is>,
the daemon should <do side effects in this order>,
persist <these rows>,
leave <this Linear/GitHub receipt>,
and be safe if it crashes after any await.
```

Implementation checklist:

1. Найти источник события: poll, webhook, slash command, review poll, merge poll
   или CLI command.
2. Найти durable state: какие rows уже есть и какие должны появиться до
   внешнего side effect.
3. Решить idempotency: что произойдет при restart, duplicate webhook или
   повторном poll tick.
4. Добавить Linear-visible receipt, если ownership меняется или человек должен
   понять следующий шаг.
5. Написать targeted regression test на переход state machine.
6. Прогнать narrow tests, затем repo-level checks.

### 8.7. Типовые изменения и минимальный scope

| Change | Scope that is usually enough |
| --- | --- |
| New Linear command | Parser test, handler in `poll.py`, template, slash polling test |
| New review rule | Pure classifier test first, then orchestrator test if it dispatches work |
| New config option | Pydantic model, example config, preflight if externally validated |
| New activity signal | Agent process parser, activity formatter, DB mark if dedupe is needed |
| New runner backend | Implement `Runner`, keep `RunnerEvent` contract, add process/lifecycle tests |
| Merge behavior change | Merge-stage tests plus GitHub client fake expectations |
| DB schema change | `schema.sql`, DAO helper, migration/backfill story if prod DBs already exist |

Keep the first PR narrow. In this codebase, broad lifecycle fixes tend to cross
dispatch, review, merge, slash commands and templates at once; that makes
review hard and increases the chance of a hidden restart bug.

### 8.8. Review checklist for code changes

Before calling a change done, ask:

- Does this change leave a Linear or GitHub receipt for humans?
- Is the relevant SQLite state written before the side effect that needs dedupe?
- Can the same webhook/comment/poll tick arrive twice without double dispatch?
- What happens if the process dies after each awaited GitHub/Linear/SQLite call?
- Does `--once` still behave predictably?
- Does a dirty existing workspace behave correctly?
- Are stale PR reviews or stale approvals ignored after a new head commit?
- Are secrets kept out of logs and activity comments?
- Did the test prove the lifecycle transition, not just a helper function?

### 8.9. Documentation rule

Update docs when you change operator-visible behavior:

- New install/deploy step -> `deploy/VPS_DEPLOY.md` and/or `deploy/RUNBOOK.md`.
- New lifecycle behavior or edge case -> `docs/linear-issue-lifecycle.md`.
- New way to teach, operate or develop the system -> this file.
- New command or changed command semantics -> this file and relevant templates.

## 9. Teaching exercises

Use these prompts to check understanding.

### 9.1. Beginner questions

- What makes a Linear issue dispatchable?
- Why does the system have both webhook and poll?
- Why is SQLite required?
- What is the difference between `codex exec` and `@codex review`?
- What does `--once` do?
- What happens when config changes?

### 9.2. Operator scenarios

Ask the learner what they would inspect:

- Linear says `In Review`, but GitHub PR is merged.
- PR has failing required CI.
- PR has Codex inline review comments.
- Issue does not start after moving to `Todo`.
- Agent process is still running but logs are quiet.
- Cost cap was reached and user wants to continue.
- Operator wants to bypass review for one issue.

Good answers should mention Linear, GitHub and SQLite together.

### 9.3. Developer scenarios

Ask which files and tests they would touch:

- Add a new Linear command.
- Add a new config field.
- Change Codex model pricing.
- Change review classifier rules.
- Add a new runner backend.
- Improve merge finalization.
- Add a new Linear comment template.

If they can answer without opening `poll.py`, they have the right mental model.
Then open `poll.py` and trace one real issue end to end.

## 10. One-page cheat sheet

### Run locally

```bash
uv sync
uv run symphony preflight --config config.local.yaml
uv run symphony --config config.local.yaml --once
uv run symphony --config config.local.yaml
```

### Dispatch one issue

```bash
uv run symphony dispatch ADJ-123 --config config.local.yaml
```

### Inspect runs

```bash
uv run symphony runs ls --db state.sqlite
uv run symphony runs show <run_id> --db state.sqlite
tail -f logs/<run_id>.log
```

### Inspect PR

```bash
gh pr view <number> --repo owner/repo --json state,merged,mergedAt,mergeable,headRefOid,url
gh pr checks <number> --repo owner/repo --required
```

### Linear commands

```text
$stop
$retry
$approve
$reject
$skip-review
👍
```

### High-blast-radius files

```text
src/symphony/orchestrator/poll.py
src/symphony/pipeline/review_classifier.py
src/symphony/db/schema.sql
src/symphony/github/client.py
src/symphony/workspace.py
src/symphony/agent/runners/local.py
```

### Best first debugging move

```text
Do not guess from one UI. Check Linear + GitHub + SQLite.
```
