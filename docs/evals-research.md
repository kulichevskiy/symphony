# Где и какие evals полезны в symphonyd

_Дата: 2026-05-27. Автор: research-сессия по коду + истории тикетов SYM-* и инцидентам._

## Что такое eval и чем он отличается от теста (термины)

В этом документе:

- **Unit-тест** — проверяет, что функция на _придуманном_ входе даёт ожидаемый
  выход. Вход синтетический, цель — зафиксировать контракт.
- **Eval** — прогон системы (или её части) на **корпусе реальных входов** с
  заранее размеченным ожидаемым результатом, где нас интересует **агрегатная
  метрика** (precision/recall, доля ошибок), а не один кейс. Корпус растёт из
  продакшена: каждый пойманный баг добавляет строку.
- **Assertion-eval** — корпус + детерминированная проверка (выход совпал с
  меткой). Подходит для чистых функций-классификаторов.
- **LLM-judge eval** — выход агента (текст, diff, вердикт) оценивает либо
  человек-разметчик, либо модель-судья. Нужен там, где «правильный ответ»
  не строка, а суждение о качестве.

Ключевое отличие от текущих `tests/` symphonyd: там ~55 файлов с unit- и
e2e-тестами на синтетике. Это хорошо ловит регрессии контракта, но **не ловит
дрейф качества решений** — а именно на нём горели самые дорогие инциденты
(SYM-28: плохой PR замержился в main; #54: тикет завис навсегда).

---

## TL;DR — приоритеты

| # | Поверхность | Тип eval | Что ловит | Почему сейчас |
|---|-------------|----------|-----------|---------------|
| **1** | `review_classifier` (9 правил вердикта) | Assertion-corpus | SYM-28, #54, #90 — неверный merge/висяк из-за хрупких эвристик над Codex-выводом | **P0.** Один баг уже замержил регрессию в main |
| **2** | Локальный ревьюер-агент (качество вердикта) | LLM-judge | Пропуск реальных багов / ложные блокировки — вся ценность symphonyd держится на этом гейте | **P0.** Ядро продукта, сейчас 0 evals |
| **3** | `acceptance_classifier` + acceptance-агент | Assertion + LLM-judge | Ложный `pass` (немерженное «готово»), ложный `infra_error`, неверный quick-skip-trivial (SYM-22) | **P1** |
| **4** | `extract_acceptance_criteria` (парсер Linear-описаний) | Golden-corpus | Потеря/мусор в критериях → ревью проверяет не то | **P1** |
| **5** | `slash.parse` + authorship-детект | Assertion-corpus | SYM-33, #59, #50 — потерянные операторские команды | **P1** |
| **6** | Merge-stage агент (scope discipline) | Safety/assertion | SYM-29/SYM-30 — агент правит source мимо ревью | **P2** |
| **7** | Implement-агент (end-to-end качество) | LLM-judge / smoke | Дрейф качества реализации | **P2**, дорого |

---

## Откуда берутся данные для корпусов

У symphonyd уникально хорошая ситуация: **весь продакшн уже логируется в
структуру**, из которой собирается размеченный датасет почти даром.

- `state.sqlite` — таблицы `runs`, `review_state`, `issue_prs`,
  `operator_waits`, `comment_events`, `comment_cursors`. История каждого решения.
- **Linear** — описания тикетов (вход для критериев), операторские комментарии
  (вход для slash-парсера), вердикты-комментарии симфонии (выход).
- **GitHub** через `gh` — PR snapshot'ы (`reviews`, `comments`, `statusCheckRollup`,
  `mergeable`), тела Codex-ревью (вход для `review_classifier`).
- `workspaces/*/<slug>/.../*.last.txt` и stream-json транскрипты —
  выход ревьюера и acceptance-агента.
- **Тикеты SYM-* сами по себе** — это уже размеченный набор «здесь система
  ошиблась + как должно было быть». Каждый закрытый баг = золотой кейс.

Рекомендация: завести `evals/` рядом с `tests/`, с подпапками-корпусами в JSONL,
и фикстурами, снятыми с реальных инцидентов (анонимизировать тела при необходимости).

---

## 1. `review_classifier` — корпус реальных PR-снимков → вердикт

**Файл:** [src/symphony/pipeline/review_classifier.py](../src/symphony/pipeline/review_classifier.py)

Чистая функция `(comments, ci, snapshot) → Verdict` с девятью правилами в
приоритетном порядке. Это **самый дорогой по последствиям и самый хрупкий**
участок: он держится на эвристиках, подогнанных под наблюдаемый вывод Codex.

### Почему именно eval, а не unit-тесты

`tests/test_review_classifier.py` уже есть, но он на синтетике. Проблема в том,
что правила настроены на _фактический формат_ Codex-ревью, который **меняется на
стороне OpenAI вне нашего контроля**:

- `CODEX_BOILERPLATE_THRESHOLD = 750` ([review_classifier.py:48](../src/symphony/pipeline/review_classifier.py#L48))
  — магическое число: «пустые» тела ревью ~621 символ, порог 750 с запасом.
  Это подгонка под наблюдение. Если Codex поменяет boilerplate — порог поедет, и
  unit-тест на синтетике этого **не заметит**, потому что синтетика тоже подогнана
  под старый формат.
- `CODEX_NO_ISSUES_MARKER = "any major issues"` — строковый матч по телу.
- Логика «свежее одобрение supersede'ит старые inline-комментарии»
  ([:300-304](../src/symphony/pipeline/review_classifier.py#L300)) — тонкая
  работа со временем/SHA, источник #54.

### Что подтверждает история

- **SYM-28** (Urgent, замержило регрессию в `main`): `codex_review_has_approval_emoji`
  возвращал True, потому что Codex стал класть «👍» в boilerplate-блок _каждого_
  ревью. Субстантивный P1 был проглочен, PR авто-замержился. Фикс — порог 750.
  **Eval поймал бы это сразу:** в корпусе появилось бы реальное тело Codex-ревью с
  P1 + boilerplate-👍, метка `CHANGES_REQUESTED`, а классификатор выдал бы `APPROVED`.
- **#54**: top-level «no major issues» не перебивал устаревшие inline-комментарии
  на том же SHA → тикет вис навсегда.
- **#90**: `interrupted` review-row не воскрешался (соседняя поверхность, но тот
  же класс «состояние PR → решение»).

### Дизайн eval

```
evals/review_classifier/cases.jsonl
  каждая строка: { id, source_issue: "VIB-116", snapshot, ci, comments, expected_verdict, expected_rule, note }
```

- Снять снимки реальных PR через `gh pr view --json` на момент инцидента
  (VIB-116/PR#229 для SYM-28, VIB-22/PR#123 для #54, и happy-path кейсы).
- Метрики: **доля верных вердиктов**, отдельно **false-APPROVE rate** (самая
  дорогая ошибка — пропуск плохого PR в merge) и **false-CHANGES rate** (висяк).
- Гейт в CI: false-APPROVE должен быть 0 на корпусе известных регрессий.
- **Корпус Codex-boilerplate**: отдельно копить реальные тела ревью Codex, чтобы
  при смене формата на стороне OpenAI порог/маркеры ловились мгновенно. Это
  страховка ровно от того класса, что породил SYM-28.

Это самый дешёвый высокоэффективный eval: функция чистая, разметка почти
бесплатна (вердикт по факту known), а одна пойманная ошибка = несмерженная
регрессия в чужом `main`.

---

## 2. Локальный ревьюер-агент — LLM-judge на качество вердикта

**Файлы:** [src/symphony/pipeline/local_review.py](../src/symphony/pipeline/local_review.py)
(`local_review_prompt`), [local_review_session.py](../src/symphony/pipeline/local_review_session.py).

Это **сердце продукта**. `review_classifier` (eval #1) — это парсер _удалённого_
Codex-ревью; а здесь symphonyd сам запускает агента-ревьюера (`codex`/`claude`),
который читает `git diff origin/<base>...HEAD` и выдаёт
`<<<VERDICT:APPROVED>>>` / `<<<VERDICT:CHANGES_REQUESTED>>>`.

Вся гарантия «плохой код не пройдёт» держится на качестве этого суждения. Сейчас
на него **ноль evals** — только тесты парсинга маркера.

### Что эвалить

Корпус `(issue, diff) → ожидаемый вердикт + ключевые findings`:

1. **Recall на дефектах** (главное): набор diff'ов с _намеренно посаженными_
   багами (взять из реальных rejected-PR + синтетические регрессии в стиле SYM-28:
   удаление авто-провижининга админа). Метрика — доля, где ревьюер сказал
   `CHANGES_REQUESTED` и в findings есть нужный файл/строка.
2. **Precision / отсутствие ложных блокировок**: корпус заведомо хороших,
   замерженных-без-правок diff'ов → ревьюер должен `APPROVE`. Высокий false-CHANGES
   жжёт деньги на fix-run'ах и упирается в iteration cap (12) → `needs_approval`.
3. **Качество findings**: feed для фиксера. В промпте явно требуются `path:line`
   и «одно предложение что не так + одно как чинить»
   ([local_review.py:110-118](../src/symphony/pipeline/local_review.py#L110)).
   LLM-judge оценивает: цитата конкретна? фикс actionable? Расплывчатые findings
   → расплывчатые фиксы → петли.
4. **Independence-гипотеза**: `default_reviewer_agent` ставит ревьюера из
   _противоположного_ семейства к имплементеру
   ([:180-192](../src/symphony/pipeline/local_review.py#L180)) — «чтобы не делили
   слепые зоны». Это **проверяемая гипотеза**: eval может сравнить recall дефектов
   у codex-ревьюит-claude vs claude-ревьюит-claude и подтвердить/опровергнуть, что
   кросс-семейная пара реально ловит больше.

### Тип

LLM-judge + ручная разметка стартового сидового набора (20-40 кейсов из реальных
review-итераций в `workspaces/*/review-*.last.txt`). Это дороже #1, но это и есть
тот eval, ради которого вообще стоит заводить evals: он измеряет основную ценность.

---

## 3. `acceptance_classifier` + acceptance-агент

**Файл:** [src/symphony/pipeline/acceptance_classifier.py](../src/symphony/pipeline/acceptance_classifier.py)

Две раздельные поверхности.

### 3a. Парсер транскрипта → вердикт (assertion-eval)

`acceptance_classifier` читает Claude stream-json, достаёт финальное сообщение,
footer (`pass|reject|infra_error`) и артефакты. Хрупкое место — **детект
infra_error по keyword-матчу** ([:468-507](../src/symphony/pipeline/acceptance_classifier.py#L468)):

```python
("playwright" in text and ("timeout" in text or "timed out" in text))
or ("npm install" in text and ("hang" in text or "hung" in text))
or "dev server failed" in text
or "preview 404" in text
```

Это ровно тот класс эвристик, что ломается тихо. SYM-18 ввёл трёхисходную модель
именно чтобы infra-ошибки не путались с продуктовым reject — но **корректность
самой классификации никто не измеряет на реальных транскриптах**.

- Корпус: реальные stream-json транскрипты acceptance-ранов (pass / reject /
  таймаут Playwright / 404 preview / cost-cap breach из SYM-19).
- Метрика: confusion matrix по трём классам. Особо опасно: `infra_error`,
  ошибочно прочитанный как `reject` (лишний acceptance_fix), и наоборот.

### 3b. Acceptance-агент — качество вердикта (LLM-judge)

- **Quick-skip-trivial (SYM-22)**: агент решает «есть ли вообще user-visible
  поведение для проверки» и при «нет» — авто-pass. **Ложный trivial-skip = немерженное
  непроверенным.** Это прямой риск: корпус из (тривиальные: rename/dep-bump/docs)
  vs (нетривиальные: фичи с UI) → доля верной классификации. False-trivial rate —
  ключевая safety-метрика.
- **Соответствие критериям**: при `mode: dev`/`preview` агент проверяет каждый
  критерий визуально и репортит per-criterion pass/fail. Eval: совпадает ли его
  per-criterion вердикт с человеческой разметкой на наборе реальных PR.

### 4. `extract_acceptance_criteria` — golden-corpus на парсер описаний

Та же функция в [acceptance_classifier.py:198](../src/symphony/pipeline/acceptance_classifier.py#L198)
— нетривиальный markdown-парсер (ATX и setext заголовки, вложенные списки,
lazy-continuation, секции non-criteria). От него зависит, **что именно** будет
проверять ревью/acceptance. Потеря критерия = непроверенное требование; мусорный
критерий = шум в промпте.

- Golden-corpus: реальные Linear-описания (`mcp__linear-server__get_issue` по всем
  SYM-* и VIB-*) → ожидаемый список `{name, predicate}`.
- Assertion-eval, разметка одноразовая, регрессии ловятся навсегда.

---

## 5. `slash.parse` + детект авторства — корпус операторских команд

**Файл:** [src/symphony/linear/slash.py](../src/symphony/linear/slash.py)

История тикетов показывает **целый класс** дорогих багов «операторская команда
тихо потеряна»: SYM-33, SYM-32, #104, #59, #50. Часть из них — баги
state-machine (routing), но **корневой для SYM-33 — именно поверхность парсера**:

```python
if c.author_is_me:   # slash.py:86
    continue
```

`author_is_me` приходит из Linear `isMe`, который **per-user, а не per-credential**
→ когда токен симфонии и оператор — один Linear-юзер, все `$approve/$retry`
оператора отфильтровываются. Чинилось через sentinel-маркер в телах симфонии.

### Eval

- Корпус `(LinearComment{body, author_is_me, external_thread_type, sentinel}) →
  expected SlashIntent | None`. Включить: обычные команды, thumbs-up, команды в
  code-fence (`_command_text`), `$approved`→APPROVE, **и authorship-кейсы**:
  оператор==симфония-юзер с/без sentinel.
- Это assertion-eval; ценность — зафиксировать, что **sentinel-логика не
  регрессирует** обратно к `isMe` (SYM-33 был «латентен неделями»).
- Замечание: SYM-32/#104/#59 — про _routing после_ парсинга (silent drop в
  обработчике, не в `parse`). Их лучше закрывать **scenario/property-тестами на
  state-machine**, а не классическим eval; но «после провала handler'а обязан
  запостить `command_rejected` И не двигать курсор» — отличный инвариант для
  property-based проверки.

---

## 6. Merge-stage агент — safety-eval на дисциплину scope

**Файл:** [src/symphony/agent/prompt.py:268](../src/symphony/agent/prompt.py#L268) (`merge_prompt`)

SYM-29 (Urgent) и SYM-30: merge-агент мог коммитить «small final fix» в source/tests
_после_ ревью, и `_merge_approved_pr` пушил это и мержил **без ре-валидации
вердикта против нового HEAD** → непроверенный код в main. Промпт после SYM-30
сужен до «только lockfiles/generated/changelog, source/tests не трогать»
([:289-300](../src/symphony/agent/prompt.py#L289)).

### Eval

- Safety-eval: набор PR, где merge-агент _провоцируется_ внести source-правку
  (например, в diff есть очевидный недочёт). Метрика: **доля ранов, где агент
  отредактировал запрещённый файл** (должна быть 0) и **доля, где корректно вышел
  без коммита, отдав на human adjudication**.
- Это инструкция-following eval: проверяет, что промпт-ограждение реально держит
  агента, а не только что бэкенд-гейт (SYM-29) ловит последствие.

---

## 7. Implement-агент — end-to-end smoke (дорого, P2)

**Файл:** [prompt.py:16](../src/symphony/agent/prompt.py#L16) (`implement_prompt`)

Открытая генерация → честный eval дорогой. Прагматичный вариант: маленький
**golden-набор тикетов** (5-10), для которых известен «хороший» PR, и метрика
«сгенерированный PR проходит локальное ревью (#2) + CI». По сути это композитный
e2e-eval поверх #1-#2. Заводить **последним**, когда #1-#2 стабильны — иначе
неясно, чей провал измеряется.

---

## Что НЕ стоит превращать в eval

Чтобы не распылять усилия — часть болей symphonyd **не eval-формы**, это
инфра/CLI-контракт/гонки:

- **#126** (broken `--config` argv убил все раны): урок не «нужен eval», а
  «unit-тест валидировал `tomllib.loads`, а не реальный codex CLI». Лечится
  contract-тестом против настоящего бинаря, не корпусом.
- **#93 / SYM-26** (codex sandbox `.git`, MEMORY.md `applies_to`): конфигурация
  окружения. Максимум — preflight-проверка, не eval.
- **stall_timeout false-positive** (инцидент 2026-05-26): watchdog считал
  активностью только stdout. Это поведение раннера → unit/integration-тест
  (`tests/test_runner_local.py` уже есть), не eval.
- **SYM-2/#96, #97, SYM-3, #90, #104** — гонки воскрешения/реконсиляции
  состояния. Это property/scenario-тесты на state-machine, не датасет-evals.

Линия раздела простая: **eval — там, где «правильность» зависит от поведения
модели или от формата чужого AI-вывода (Codex), который дрейфует.** Гонки,
argv-контракты и конфиги — это детерминированный код, для них обычные тесты.

---

## Предлагаемая последовательность

1. **Неделя 1 — `review_classifier` corpus (eval #1).** Дешевле всего, ловит
   доказанно-дорогой класс (SYM-28/#54). Снять снимки PR, зафиксировать
   false-APPROVE=0 как CI-гейт. Параллельно начать копить корпус Codex-boilerplate.
2. **Неделя 1-2 — `extract_acceptance_criteria` golden (eval #4) и `slash.parse`
   corpus (eval #5).** Оба assertion, разметка одноразовая, регрессии навсегда.
3. **Неделя 2-3 — acceptance transcript-classifier (eval #3a).** Корпус
   транскриптов из `workspaces/`.
4. **Неделя 3+ — LLM-judge на локального ревьюера (eval #2).** Главный по
   ценности, но требует сидовой ручной разметки и судьи. Сюда же —
   independence-эксперимент кросс-семейной пары.
5. **Позже — quick-skip-trivial safety (#3b), merge-scope safety (#6),
   implement smoke (#7).**

### Инфраструктура

- `evals/` параллельно `tests/`; корпуса в JSONL; раннер на `pytest` с
  marker `@pytest.mark.eval`, чтобы отделять от быстрых unit-тестов.
- Гейты в CI только для **assertion-evals с регрессионным смыслом**
  (false-APPROVE=0). LLM-judge-evals — на ручной/ночной прогон с трендом метрики,
  не блокирующий гейт (иначе флак).
- **Процесс**: каждый новый SYM-* баг класса «модель/Codex ошиблась» обязан
  добавить строку в соответствующий корпус. Тикеты SYM-* — это уже готовая
  разметка; нужно лишь систематически переносить их в `evals/`.

---

## Сводка: связь инцидент → eval

| Инцидент | Класс | Покрывающий eval |
|----------|-------|------------------|
| SYM-28 (boilerplate 👍 → merge регрессии) | Дрейф формата Codex | #1 review_classifier + Codex-corpus |
| #54 (inline-shadow, висяк) | Логика supersede | #1 review_classifier |
| SYM-22 (quick-skip) — риск ложного skip | Суждение агента | #3b acceptance LLM-judge |
| SYM-18/#19 (infra vs reject) | Keyword-эвристика | #3a transcript-classifier |
| SYM-33 (isMe per-user) | Authorship-детект | #5 slash corpus |
| SYM-29/30 (merge-агент правит source) | Instruction-following | #6 merge safety |
| #126, #93, SYM-26, stall_timeout | Инфра/CLI/гонки | _не eval_ — обычные тесты |
