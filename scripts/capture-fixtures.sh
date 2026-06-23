#!/usr/bin/env bash
# Regenerate the contract-test golden fixtures from REAL GitHub/Linear payloads.
#
# These goldens (tests/fixtures/contract/) pin the harness fakes against
# reality — see tests/test_fake_contracts.py. Refreshing a fixture is "run this
# against a real PR/issue", never hand-editing JSON.
#
# Usage:
#   GH_REPO=owner/repo PR=1234 \
#   LINEAR_API_KEY=lin_xxx ISSUE=SYM-42 \
#   scripts/capture-fixtures.sh
#
# Captures (only the surfaces whose env vars are set are refreshed):
#   GH_REPO + PR        -> github_pr_view.json, github_pr_checks.json
#   GH_REPO + HOOK_ID   -> github_pr_webhook.json (latest pull_request delivery)
#   LINEAR_API_KEY+ISSUE-> linear_issues_in_state.json, linear_comments_since.json
#   LINEAR_COMMENT_DELIVERY (path to a saved webhook body) -> linear_comment_webhook.json
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

if [[ -n "${GH_REPO:-}" && -n "${PR:-}" ]]; then
  echo "capturing github_pr_view.json from $GH_REPO#$PR"
  gh pr view "$PR" --repo "$GH_REPO" --json "$PR_VIEW_FIELDS" | jq . > "$OUT/github_pr_view.json"

  echo "capturing github_pr_checks.json from $GH_REPO#$PR"
  # `--required` mirrors the merge gate; exit 8 means a check is failing (valid JSON).
  # Capture to a temp file so a broken run never clobbers the committed golden.
  _tmp=$(mktemp)
  if gh pr checks "$PR" --repo "$GH_REPO" --required --json name,state,bucket,link \
      | jq . > "$_tmp"; then
    mv "$_tmp" "$OUT/github_pr_checks.json"
  else
    _gh_exit="${PIPESTATUS[0]}"
    if [[ "$_gh_exit" -eq 8 ]]; then
      mv "$_tmp" "$OUT/github_pr_checks.json"
    else
      rm -f "$_tmp"
      echo "warning: gh pr checks failed (exit $_gh_exit); existing golden unchanged" >&2
    fi
  fi
fi

if [[ -n "${GH_REPO:-}" && -n "${HOOK_ID:-}" ]]; then
  echo "capturing github_pr_webhook.json from $GH_REPO hook $HOOK_ID"
  DELIVERY_ID=$(gh api "repos/$GH_REPO/hooks/$HOOK_ID/deliveries" \
    --jq 'map(select(.event == "pull_request")) | .[0].id')
  gh api "repos/$GH_REPO/hooks/$HOOK_ID/deliveries/$DELIVERY_ID" \
    --jq '.request.payload' | jq . > "$OUT/github_pr_webhook.json"
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
  curl -fsS https://api.linear.app/graphql \
    -H "Authorization: $LINEAR_API_KEY" -H "Content-Type: application/json" \
    -d "$(jq -nc --arg q "$ISSUE_Q" --arg id "$ISSUE" '{query:$q, variables:{id:$id}}')" \
    | jq '{issues: {nodes: [.data.issue]}}' > "$OUT/linear_issues_in_state.json"

  COMMENTS_Q=$(cat <<'GQL'
query($id: String!) {
  issue(id: $id) {
    comments(first: 1) {
      pageInfo { hasNextPage endCursor }
      nodes { id body createdAt user { name isMe } externalThread { type } }
    }
  }
}
GQL
)
  curl -fsS https://api.linear.app/graphql \
    -H "Authorization: $LINEAR_API_KEY" -H "Content-Type: application/json" \
    -d "$(jq -nc --arg q "$COMMENTS_Q" --arg id "$ISSUE" '{query:$q, variables:{id:$id}}')" \
    | jq '{issue: .data.issue}' > "$OUT/linear_comments_since.json"
fi

if [[ -n "${LINEAR_COMMENT_DELIVERY:-}" && -f "${LINEAR_COMMENT_DELIVERY:-}" ]]; then
  echo "capturing linear_comment_webhook.json from $LINEAR_COMMENT_DELIVERY"
  jq . "$LINEAR_COMMENT_DELIVERY" > "$OUT/linear_comment_webhook.json"
fi

echo "done. review the diff, then run: uv run pytest tests/test_fake_contracts.py"
