# symphonyd Production Reliability Audit

Date: 2026-05-14

Scope: local `symphonyd` production history in `state.sqlite`, run logs under
`logs/`, prior incident notes, and the current orchestration code paths for
autonomous Linear issue execution.

## Executive Summary

The local daemon processed 20 Linear issues and opened 19 tracked PRs. All 19
tracked PRs eventually reached a recorded `merged_at`, but the path was not
production-reliable: the database shows 436 failed implement runs, 54 failed
review runs, 7 failed review-fix runs, 9 interrupted runs, and 5 merge approval
waits. The highest-impact failures are not simple code-style defects; they are
state-machine and signal-freshness failures that caused repeated work, stale
review loops, or visible Linear state lag.

The priority fixes in this patch address:

- Implement failures now park in durable `operator_waits` instead of being put
  straight back into the ready lane.
- Operator waits for failed implement runs survive restart and are resumed only
  by `$retry` or `$approve`.
- Ready-state scans skip issues already represented by an active/operator-wait
  run, even if Linear still reports the issue as ready.
- Review and merge candidates are polled before new ready issues, so finishing
  existing work is prioritized over starting fresh work.
- Review-fix work reserves normal dispatch capacity, preventing new implement
  runs from starving reviewer feedback.
- Codex "no major issues" PR comments and thumbs-up style signals are treated
  as approval signals when fresh for the current head.
- Stale Codex comments and stale LGTM comments before the current head commit
  are ignored.
- Deduped red CI no longer masks later fresh review feedback on the same PR.
- Merge conflict recovery syncs the workspace to the remote branch first and
  reports `git status --short` when rebase exits with no unresolved paths.
- Merge finalization records Done and clears waits before workspace cleanup, so
  cleanup failure does not roll a completed issue back into review.

## Evidence Sources

- SQLite run inventory:
  `sqlite3 state.sqlite "select stage,status,count(*) from runs group by stage,status"`
- SQLite PR inventory:
  `sqlite3 state.sqlite "select identifier, pr_url, created_at, merged_at from issue_prs ..."`
- SQLite operator waits:
  `sqlite3 state.sqlite "select ... from operator_waits ..."` returned no active waits at audit time.
- Representative run logs:
  `logs/f0093d7e-c797-412f-b287-0589d2d4b4af.log` for the VIB-1 misrouting loop,
  `logs/1f020a47-29e4-44cc-b542-62544fa180c5.log` for VIB-11 merge/finalization lag.
- Prior incident notes in memory for ADJ-3/VIB-11 merge finalization lag and LP-5 stale inline review churn.

## Run Inventory

Status counts from `state.sqlite`:

| Stage | Status | Count |
| --- | ---: | ---: |
| implement | completed | 19 |
| implement | failed | 436 |
| review | completed | 24 |
| review | failed | 54 |
| review_fix | completed | 112 |
| review_fix | failed | 7 |
| review_fix | interrupted | 7 |
| merge | done | 19 |
| merge | interrupted | 2 |
| merge | needs_approval | 5 |

Tracked PRs: 19. All 19 have `merged_at` recorded. Slowest PRs by local
create-to-merge time were ADJ-1 at 1318.5 minutes, ADJ-2 at 631.0 minutes,
LP-7 at 421.3 minutes, LP-5 at 404.0 minutes, and VIB-9 at 353.9 minutes.

## Incident Inventory

| Incident cluster | Evidence | Root cause | Fix status |
| --- | --- | --- | --- |
| Implement retry storm | VIB-1 had 406 failed implement runs from 2026-05-11T17:21:03Z to 2026-05-13T00:20:05Z. | Failed implement runs were moved back to the ready lane, so a bad or misrouted issue could be picked up repeatedly. | Fixed by `KIND_IMPLEMENT_FAILED`, durable operator waits, scan suppression, and `$retry` resume. |
| Misrouted/low-quality issue execution | VIB-1 title was "add emoji to the comment"; a sampled log shows the agent in `vibecamp-org/vibecamp` while checking for symphonyd/Linear comment behavior. | The daemon contained the blast but did not validate whether the issue scope matched the bound repo before running. | Partially fixed by parking failed runs. Routing-quality preflight remains open. |
| Stale review-fix churn | ADJ-1 had 38 completed review-fix runs plus 22 failed review monitor runs; LP-5 had 29 completed review-fix runs and long review duration. | Old Codex inline comments and LGTM-like comments were treated as fresh because review classification lacked current head commit time. | Fixed by fetching `head_committed_at` and using it in review classification and LGTM notification. |
| Approval signal missed | Some Codex approvals appeared as top-level PR issue comments containing "no major issues" or thumbs-up text rather than GitHub approval reviews. | The classifier only considered reviews/reactions in some paths. | Fixed by normalizing fresh Codex no-issues issue comments into approval-like reactions. |
| Red CI hid new review feedback | Review monitor skipped review endpoints whenever CI was red. If the red-CI signature had already dispatched a fix, later review comments could be missed. | CI preemption was too absolute after dedupe. | Fixed by checking review signals when the red-CI signature is already deduped. |
| Existing work starved by new work | Review-fix and merge work could be delayed by new implementation dispatches. | Merge candidates were polled after ready scans, and review-fix dispatch used a separate pool without reserving normal capacity. | Fixed by polling merge candidates before scans and making review-fix runs reserve normal capacity. |
| Merge finalization lag | VIB-11 and ADJ-3 looked stuck in Review even when GitHub state indicated merge/finalization was the real blocker. | Linear/GitHub/SQLite could fall behind after merge or cleanup errors. | Fixed by finalizing merged PRs before review classification and by marking Done/clearing waits before cleanup. |
| Merge conflict recovery ambiguity | Rebase could fail with no unresolved paths, leaving operators with a blank "conflict" failure. | Dirty workspace or non-content rebase failures were not diagnosed. | Fixed by syncing workspace to remote first and including `git status --short` in failure comments. |
| Slash-command misses | Operators can paste commands as inline code/fenced code, or type `$approved`; thumbs-up is also used as approval. | Parser expected a narrower command shape. | Fixed by accepting markdown-wrapped commands, `$approved`, and standalone thumbs-up approval. |
| Restart recovery gap for failed implement | Review/merge waits survived restart, but failed implement runs did not have a durable operator wait. | `operator_waits` had cost/review/merge kinds only. | Fixed by persisting `implement_failed` waits and restoring them on startup. |

## Workflow Semantics Change

Implement failure behavior changed intentionally.

Before: after an implement-stage failure, symphonyd marked the run failed and
moved the Linear issue back to its prior state. For ready-lane issues this
usually meant `Todo`, so the next scan could dispatch the same issue again.

After: after an implement-stage failure, symphonyd marks the run failed, moves
the issue to `needs_approval` when available, otherwise `blocked` when
available, otherwise the prior state, records an `operator_waits` row with kind
`implement_failed`, and posts a Linear comment telling the operator to use
`$retry`/`$approve` or `$reject`/`$stop`.

Migration note: no schema migration is required because `operator_waits.kind`
is a string. Existing rows keep their old kinds. Operators should expect failed
implement issues to stay out of the ready lane until explicitly retried.

## Code Changes

- `src/symphony/db/operator_waits.py`
  - Added `KIND_IMPLEMENT_FAILED`.
- `src/symphony/orchestrator/poll.py`
  - Restores and routes failed-implement waits.
  - Handles `$retry`/`$approve` by moving the issue back to ready and clearing
    the wait.
  - Handles `$reject`/`$stop` by moving the issue to blocked and clearing the
    wait.
  - Suppresses ready scans for issues already present in `_dispatch_run_ids`.
  - Polls merge candidates before scanning new ready work.
  - Gives review-fix work priority by reserving normal dispatch capacity.
  - Uses current head commit time for stale review/LGTM filtering.
  - Treats fresh Codex no-issues PR issue comments as approval signals.
  - Avoids re-triggering `@codex review` when fresh approval is already present.
  - Makes merge finalization durable before cleanup.
- `src/symphony/linear/slash.py`
  - Accepts `$approved`, inline/fenced-code commands, and standalone thumbs-up.
- Tests added/updated in:
  - `tests/test_implement_e2e.py`
  - `tests/test_review_stage.py`
  - `tests/test_merge_stage.py`
  - `tests/test_slash.py`
  - `tests/test_review_classifier.py`

## Validation

Regression command:

```bash
uv run pytest tests/test_implement_e2e.py tests/test_cost_cap_e2e.py tests/test_review_classifier.py tests/test_slash.py tests/test_review_stage.py tests/test_merge_stage.py tests/test_poll_dedupe.py tests/test_reconcile.py
```

Result: 164 passed. Pytest emitted two existing mock cleanup warnings from
test code using unawaited `AsyncMock` objects.

Full unit suite:

```bash
uv run pytest
```

Result: 331 passed. Pytest emitted five existing mock cleanup warnings from
test code using unawaited `AsyncMock` objects.

Lint:

```bash
uv run ruff check src tests
```

Result: all checks passed.

Source typing:

```bash
uv run mypy src
```

Result: success, no issues in 41 source files.

Full test typing:

```bash
uv run mypy src tests
```

Result: failed on pre-existing test typing debt, including untyped pytest
fixture parameters and method assignment ignores in tests. The source tree
remains clean under `uv run mypy src`.

## Remaining Gaps

- Routing-quality preflight is still weak. Parking contains VIB-1-style loops,
  but it does not prove that an issue belongs to the configured GitHub repo
  before dispatch.
- Real live validation after these fixes still needs one fresh issue or a safe
  dry-run harness that can exercise implement -> review -> merge/finalize ->
  Done without risking unrelated ready-lane tickets.
- The test suite still has mock cleanup warnings and existing mypy debt in
  tests. These do not block the fixed source paths but should be cleaned up.
- Poll efficiency remains mostly cadence-based. The dual webhook plus poll
  model is safer than webhook-only, but the audit did not implement adaptive
  polling or webhook health backoff.
