#!/usr/bin/env bash
# Regenerate the contract-test golden fixtures from REAL GitHub/Linear payloads.
#
# These goldens (tests/fixtures/contract/) pin the harness fakes against
# reality — see tests/test_fake_contracts.py. Refreshing a fixture is "run this
# against a real PR/issue", never hand-editing JSON.
#
# Usage:
#   GH_REPO=owner/repo PR=1234 FAILING_PR=5678 HOOK_ID=555000111 \
#   LINEAR_API_KEY=lin_xxx ISSUE=SYM-42 \
#   LINEAR_COMMENT_DELIVERY=/path/to/saved-delivery.json \
#   scripts/capture-fixtures.sh
#
# Captures (only the surfaces whose env vars are set are refreshed):
#   GH_REPO + PR          -> github_pr_view.json, github_pr_checks_passing.json
#                            (PR must be open, mergeable, all required checks green)
#   GH_REPO + FAILING_PR  -> github_pr_checks.json
#                            (PR must have >=1 failing required check; gh exits 1)
#   GH_REPO + HOOK_ID     -> github_pr_webhook.json (latest pull_request delivery)
#   LINEAR_API_KEY+ISSUE  -> linear_issues_in_state.json, linear_comments_since.json
#                            (ISSUE must have >=1 comment)
#   LINEAR_COMMENT_DELIVERY (path to a saved webhook body) -> linear_comment_webhook.json
#
# All captured payloads are sanitized to a synthetic acme/widgets + SYM project
# before being written to the committed fixture — no real org names, user names,
# or SHAs appear in the repository.
#
# Requires: gh (authenticated), jq, curl.
#
# Webhook deliveries are not always reproducible on demand (they need a
# configured hook / a real inbound delivery). When HOOK_ID / a saved delivery
# body are not available, those two goldens are left untouched — the committed
# copies are real-shaped and refreshed manually from a captured delivery.

set -euo pipefail

cd "$(dirname "$0")/.."
OUT="tests/fixtures/contract"
mkdir -p "$OUT"

PR_VIEW_FIELDS="number,title,state,url,headRefName,headRefOid,baseRefName,mergeable,mergeStateStatus,isDraft,mergedAt,statusCheckRollup"

# ---------------------------------------------------------------------------
# github_pr_view.json + github_pr_checks_passing.json
# Use a PR that is open, mergeable, and has all required checks green.
# ---------------------------------------------------------------------------
if [[ -n "${GH_REPO:-}" && -n "${PR:-}" ]]; then
  echo "capturing github_pr_view.json from $GH_REPO#$PR"
  _tmp=$(mktemp)
  if gh pr view "$PR" --repo "$GH_REPO" --json "$PR_VIEW_FIELDS" \
      | jq '
        .url         = ("https://github.com/acme/widgets/pull/" + (.number | tostring)) |
        .title       = "Fix flaky merge gate" |
        .headRefName = "symphony/sym-42" |
        .headRefOid  = "9f3a1c2e5b7d8a0f1234567890abcdef12345678" |
        .baseRefName = "main" |
        .statusCheckRollup |= (if . then map(
          {__typename, name: "ci", status, conclusion, state}
        ) else . end)
      ' > "$_tmp"; then
    # Reject if any statusCheckRollup entry is non-green: a pending/failing
    # optional status would corrupt the "fully passing PR" contract fixture.
    # CheckRun entries have a `status` field; treat anything other than
    # COMPLETED as pending even when `conclusion` is null.
    if jq -e '
      .statusCheckRollup // [] | all(
        (
          (.conclusion == "SUCCESS" or .conclusion == null) and
          (.state == "SUCCESS" or .state == null)
        ) and
        (.__typename != "CheckRun" or .status == "COMPLETED")
      )
    ' "$_tmp" > /dev/null 2>&1; then
      mv "$_tmp" "$OUT/github_pr_view.json"
    else
      rm -f "$_tmp"
      echo "warning: PR has non-green statusCheckRollup entries (pending/failing optional checks?); existing golden unchanged" >&2
    fi
  else
    rm -f "$_tmp"
    echo "warning: gh pr view failed; existing golden unchanged" >&2
  fi

  echo "capturing github_pr_checks_passing.json from $GH_REPO#$PR"
  # gh pr checks exits 0 only when all required checks pass.
  _tmp=$(mktemp)
  if gh pr checks "$PR" --repo "$GH_REPO" --required --json name,state,bucket,link \
      | jq 'map(.link = "https://github.com/acme/widgets/actions/runs/9876543210/job/12345678901")' \
      > "$_tmp"; then
    # Reject empty array: a PR with no required checks exits 0 with [] but would
    # overwrite the fixture with a payload that fails the contract.
    if jq -e 'type == "array" and length > 0' "$_tmp" > /dev/null 2>&1; then
      mv "$_tmp" "$OUT/github_pr_checks_passing.json"
    else
      rm -f "$_tmp"
      echo "warning: gh pr checks returned empty array (no required checks on PR?); existing golden unchanged" >&2
    fi
  else
    rm -f "$_tmp"
    echo "warning: gh pr checks did not exit 0 (PR not fully green?); existing golden unchanged" >&2
  fi
fi

# ---------------------------------------------------------------------------
# github_pr_checks.json
# Use a PR with >=1 failing required check (gh pr checks exits 1).
# Exit 8 (checks pending) is NOT a failing state — never overwrite this golden
# with pending output.
# ---------------------------------------------------------------------------
if [[ -n "${GH_REPO:-}" && -n "${FAILING_PR:-}" ]]; then
  echo "capturing github_pr_checks.json from $GH_REPO#$FAILING_PR"
  _tmp=$(mktemp)
  if gh pr checks "$FAILING_PR" --repo "$GH_REPO" --required --json name,state,bucket,link \
      | jq '
        # Normalise to the single synthetic failing entry the contract fake models:
        # name="ci", state="FAILURE", bucket="fail". This makes the fixture
        # independent of the real check name/count while preserving the
        # semantics the merge-gate tests on (any_failed=true, all_passed=false).
        # Output [] if no failing entry is found; the length guard below rejects it.
        map(select(.bucket == "fail" or .state == "FAILURE")) |
        if length > 0 then [first | {
          name: "ci",
          state: "FAILURE",
          bucket: "fail",
          link: "https://github.com/acme/widgets/actions/runs/9876543210/job/12345678901"
        }] else [] end
      ' \
      > "$_tmp"; then
    # Exit 0 = all checks passed — wrong PR, golden unchanged.
    rm -f "$_tmp"
    echo "warning: gh pr checks exited 0 (no failing checks on FAILING_PR); existing golden unchanged" >&2
  else
    _gh_exit="${PIPESTATUS[0]}"
    case "$_gh_exit" in
      1) # Validate output is a non-empty JSON array — not an error message from a PR
         # with no required checks or an unrelated gh failure.
         if jq -e 'type == "array" and length > 0' "$_tmp" > /dev/null 2>&1; then
           mv "$_tmp" "$OUT/github_pr_checks.json"
         else
           rm -f "$_tmp"
           echo "warning: gh pr checks exit-1 output is not a non-empty check array (no required checks on FAILING_PR?); existing golden unchanged" >&2
         fi ;;
      8) rm -f "$_tmp"
         echo "warning: gh pr checks exited 8 (checks pending, not failing); existing golden unchanged" >&2 ;;
      *) rm -f "$_tmp"
         echo "warning: gh pr checks failed (exit $_gh_exit); existing golden unchanged" >&2 ;;
    esac
  fi
fi

if [[ -n "${GH_REPO:-}" && -n "${HOOK_ID:-}" ]]; then
  echo "capturing github_pr_webhook.json from $GH_REPO hook $HOOK_ID"
  # Pick the most-recent closed pull_request delivery; the per-delivery fetch
  # then verifies pull_request.merged == true so a closed-not-merged event
  # never becomes the golden.
  DELIVERY_ID=$(gh api "repos/$GH_REPO/hooks/$HOOK_ID/deliveries" \
    --jq 'map(select(.event == "pull_request" and .action == "closed")) | .[0].id' 2>&1) || {
    echo "warning: gh api hook deliveries failed (wrong HOOK_ID or insufficient permissions?); existing golden unchanged" >&2
    DELIVERY_ID=""
  }
  if [[ -z "$DELIVERY_ID" || "$DELIVERY_ID" == "null" ]]; then
    echo "warning: no closed pull_request delivery found for hook $HOOK_ID; existing golden unchanged" >&2
  else
    _tmp=$(mktemp)
    # Whitelist only the fields the contract needs; reject deliveries where the
    # PR was closed without merging (merged == true is required).
    if gh api "repos/$GH_REPO/hooks/$HOOK_ID/deliveries/$DELIVERY_ID" \
        --jq '.request.payload' \
        | jq '
          if .pull_request.merged != true then
            error("delivery is a closed-without-merge PR; point HOOK_ID at a hook that received a merged-PR event")
          else . end |
          {
            action: .action,
            number: .pull_request.number,
            pull_request: {
              number: .pull_request.number,
              state: .pull_request.state,
              merged: .pull_request.merged,
              merged_at: .pull_request.merged_at,
              merged_by: (if .pull_request.merged_by then {"login": "alex"} else null end),
              head: {"ref": "symphony/sym-42", "sha": "9f3a1c2e5b7d8a0f1234567890abcdef12345678"},
              base: {"ref": "main"}
            },
            repository: {"full_name": "acme/widgets"},
            sender: {"login": "alex"}
          }
        ' > "$_tmp"; then
      mv "$_tmp" "$OUT/github_pr_webhook.json"
    else
      rm -f "$_tmp"
      echo "warning: github_pr_webhook capture failed; existing golden unchanged" >&2
    fi
  fi
fi

if [[ -n "${LINEAR_API_KEY:-}" && -n "${ISSUE:-}" ]]; then
  echo "capturing linear_issues_in_state.json + linear_comments_since.json for $ISSUE"
  # Mirror queries.ISSUES_IN_STATE_NO_LABEL / ISSUE_COMMENTS_SINCE node shapes.
  ISSUE_Q=$(cat <<'GQL'
query($id: String!) {
  issue(id: $id) {
    id identifier title description url
    state { id name type }
    team { key }
    labels { nodes { name } }
    updatedAt
    relations { nodes { type relatedIssue { id identifier state { type } archivedAt } } pageInfo { hasNextPage endCursor } }
    inverseRelations { nodes { type issue { id identifier state { type } archivedAt } } pageInfo { hasNextPage endCursor } }
  }
}
GQL
)
  _tmp=$(mktemp)
  if curl -fsS https://api.linear.app/graphql \
    -H "Authorization: $LINEAR_API_KEY" -H "Content-Type: application/json" \
    -d "$(jq -nc --arg q "$ISSUE_Q" --arg id "$ISSUE" '{query:$q, variables:{id:$id}}')" \
    | jq '
      {issues: {nodes: [.data.issue]}} |
      .issues.nodes[0].id          = "8a1f0c2e-1b3d-4e5f-9a7b-0c1d2e3f4a5b" |
      .issues.nodes[0].identifier  = "SYM-42" |
      .issues.nodes[0].title       = "Fix flaky merge gate" |
      .issues.nodes[0].description = "The merge gate occasionally races with auto-merge." |
      .issues.nodes[0].url         = "https://linear.app/acme/issue/SYM-42/fix-flaky-merge-gate" |
      .issues.nodes[0].state.id    = "state-ready-uuid" |
      .issues.nodes[0].state.name  = "Ready" |
      .issues.nodes[0].state.type  = "unstarted" |
      .issues.nodes[0].team.key    = "SYM" |
      .issues.nodes[0].labels.nodes = [{"name": "symphony"}] |
      .issues.nodes[0].updatedAt    = "2026-06-20T10:00:00.000Z" |
      .issues.nodes[0].relations         = {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}} |
      .issues.nodes[0].inverseRelations  = {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}}
    ' > "$_tmp"; then
    mv "$_tmp" "$OUT/linear_issues_in_state.json"
  else
    rm -f "$_tmp"
    echo "warning: linear_issues_in_state capture failed; existing golden unchanged" >&2
  fi

  # Mirror ISSUE_COMMENTS_SINCE: first: 50, $after filter, $cursor pagination.
  # Captures only the first page (sufficient to exercise the parse path).
  # Sanitized to a single synthetic comment so the test's hardcoded SimComment
  # seed stays valid after regeneration from any real issue.
  COMMENTS_Q=$(cat <<'GQL'
query IssueComments($id: String!, $after: DateTimeOrDuration!, $cursor: String) {
  issue(id: $id) {
    comments(first: 50, after: $cursor, filter: { createdAt: { gte: $after } }, orderBy: createdAt) {
      pageInfo { hasNextPage endCursor }
      nodes { id body createdAt user { name isMe } externalThread { type } }
    }
  }
}
GQL
)
  _tmp=$(mktemp)
  if curl -fsS https://api.linear.app/graphql \
    -H "Authorization: $LINEAR_API_KEY" -H "Content-Type: application/json" \
    -d "$(jq -nc --arg q "$COMMENTS_Q" --arg id "$ISSUE" \
      '{query:$q, variables:{id:$id, after:"2000-01-01T00:00:00Z"}}')" \
    | jq '
      {issue: .data.issue} |
      if (.issue.comments.nodes | length) == 0
      then error("issue has no comments; pick an issue with >=1 comment")
      else . end |
      .issue.comments.nodes        = [.issue.comments.nodes[0]] |
      .issue.comments.pageInfo.hasNextPage = false |
      .issue.comments.pageInfo.endCursor   = null |
      .issue.comments.nodes[0].id          = "c1a2b3c4-d5e6-4f70-8192-a3b4c5d6e7f8" |
      .issue.comments.nodes[0].body        = "$approve" |
      .issue.comments.nodes[0].createdAt   = "2026-06-20T15:00:00.000Z" |
      .issue.comments.nodes[0].user.name   = "Alex" |
      .issue.comments.nodes[0].user.isMe   = false |
      .issue.comments.nodes[0].externalThread = null
    ' > "$_tmp"; then
    mv "$_tmp" "$OUT/linear_comments_since.json"
  else
    rm -f "$_tmp"
    echo "warning: linear_comments_since capture failed; existing golden unchanged" >&2
  fi
fi

if [[ -n "${LINEAR_COMMENT_DELIVERY:-}" && -f "${LINEAR_COMMENT_DELIVERY:-}" ]]; then
  echo "capturing linear_comment_webhook.json from $LINEAR_COMMENT_DELIVERY"
  _tmp=$(mktemp)
  if jq '
    # Whitelist only the fields the contract tests act on so no extra fields
    # from a real delivery leak into the committed fixture.
    {
      type:   .type,
      action: .action,
      data: {
        id:             "c1a2b3c4-d5e6-4f70-8192-a3b4c5d6e7f8",
        body:           "$approve",
        issueId:        "8a1f0c2e-1b3d-4e5f-9a7b-0c1d2e3f4a5b",
        externalThread: null
      },
      actor: (if .actor then {"id": "user-uuid", "name": "Alex"} else null end)
    }
  ' "$LINEAR_COMMENT_DELIVERY" > "$_tmp"; then
    mv "$_tmp" "$OUT/linear_comment_webhook.json"
  else
    rm -f "$_tmp"
    echo "warning: linear_comment_webhook capture failed; existing golden unchanged" >&2
  fi
fi

echo "done. review the diff, then run: uv run pytest tests/test_fake_contracts.py"
