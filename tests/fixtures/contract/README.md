# Contract-test golden fixtures

Recorded real GitHub/Linear payloads that pin the harness fakes
(`tests/harness/fakes.py`) against reality. See
`tests/test_fake_contracts.py`: each contract feeds **both** the recorded real
payload here and the fake's output through the **same real parsing path** and
asserts the resulting domain objects agree. A fake whose shape or values drift
goes red.

| Fixture | Surface | Real parsing path |
|---|---|---|
| `github_pr_view.json` | `gh pr view <n> --json …` | `GitHub.pr_view` → poll merge-gate classifiers |
| `github_pr_checks.json` | `gh pr checks <n> --required --json …` | `GitHub.pr_checks` → `PRChecks` |
| `github_pr_webhook.json` | `pull_request` (closed/merged) webhook delivery | `github.webhook._parse_event` |
| `linear_issues_in_state.json` | `issues(...)` GraphQL `data` | `LinearTracker.issues_in_state` → `LinearIssue.from_node` |
| `linear_comments_since.json` | `issue.comments(...)` GraphQL `data` | `LinearTracker.comments_since` → `LinearComment.from_node` |
| `linear_comment_webhook.json` | `Comment`/`create` webhook delivery | `linear.client.comment_from_webhook_payload` |

## Regenerating

One command (set only the env vars for the surfaces you want to refresh):

```bash
GH_REPO=owner/repo PR=1234 HOOK_ID=555000111 \
LINEAR_API_KEY=lin_xxx ISSUE=SYM-42 \
LINEAR_COMMENT_DELIVERY=/path/to/saved-delivery.json \
scripts/capture-fixtures.sh
```

Then review the diff and run `uv run pytest tests/test_fake_contracts.py`.

`pr_view` / `pr_checks` and the Linear issue/comment reads capture live. The two
webhook deliveries need a configured hook (`HOOK_ID`) or a saved inbound
delivery body (`LINEAR_COMMENT_DELIVERY`); when those aren't available the
committed copies are real-shaped payloads refreshed manually from a captured
delivery. IDs/titles/SHAs are sanitized to a synthetic `acme/widgets` + `SYM`
project — the contracts compare *domain semantics*, not literal identifiers, so
sanitizing is safe.

## Deferred (YAGNI)

Not built until drift actually bites: an opt-in `--record` pytest mode that
re-captures goldens during a test run, and a periodic sandbox-CI drift check
that diffs fresh captures against these goldens on a schedule.
