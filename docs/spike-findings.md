# M0 Spike Findings

**Summary:** The design shifts. Spike 3 (`claude --resume`) passed cleanly. Spike 2 (`@codex review` trigger) is confirmed by Codex's own published behavior. Spikes 1 and 4 invalidate SYMPHONY.md's auto-merge strategy: the Codex GitHub App never posts formal `APPROVED` / `CHANGES_REQUESTED` reviews — every Codex review carries `state == "COMMENTED"`. Approval is signaled out-of-band via a 👍 reaction (`+1`) on the PR by `chatgpt-codex-connector[bot]`. Because GitHub branch-protection "required reviewers" only counts `APPROVED` reviews, Codex cannot serve as the gating approver. SYMPHONY.md is being adjusted in a follow-up commit on this branch: verdict parsing now keys on reaction-vs-comment-with-body, and auto-merge moves from `gh pr merge --auto` (waiting on a non-existent Codex `APPROVED`) to Symphony firing `gh pr merge --squash --delete-branch` itself once it observes Codex 👍 + green required CI checks. Downstream issues #3 (review loop) and #4 (single-issue happy path) are affected.

## Methodology

Spike 1, 2, and 4 were validated by querying public GitHub PRs that have the OpenAI Codex GitHub App installed (`chatgpt-codex-connector[bot]`), rather than installing the app on a throwaway repo. The Codex GH App's behavior is repo-independent and the available public corpus (15+ PRs across 6 repos surveyed) is consistent and sufficient. Spike 3 was executed locally against the user's `claude` CLI v2.1.129 with model `claude-opus-4-7[1m]`. Raw payloads are committed under `docs/spike-evidence/`.

---

## Spike 1 — Codex review verdict shape: **DESIGN INVALIDATED**

**Question:** Does the Codex GH App post formal `APPROVED` / `CHANGES_REQUESTED` reviews?

**Answer:** No. **All Codex reviews are `state == "COMMENTED"`**, regardless of whether suggestions exist. Approval is signaled by a `+1` reaction on the PR (issue) added by `chatgpt-codex-connector[bot]`.

### Evidence

Sample of `gh api repos/{owner}/{repo}/pulls/{n}/reviews` filtered to `chatgpt-codex-connector[bot]`:

| Repo / PR | state | body |
|---|---|---|
| JoeyTeng/codex-background-task-handler#8 | `COMMENTED` | "Here are some automated review suggestions…" |
| gaejabong/codex-analytics-review-repro#6 | `COMMENTED` | "Here are some automated review suggestions…" |
| guaguastandup/zotero-pdf2zh#301 (×7 commits reviewed) | `COMMENTED` | "Here are some automated review suggestions…" |
| guaguastandup/zotero-pdf2zh#295 (×7 commits reviewed) | `COMMENTED` | "Here are some automated review suggestions…" |

`APPROVED` and `CHANGES_REQUESTED` from `chatgpt-codex-connector[bot]` were **never** observed (n = 15+ Codex reviews surveyed).

The Codex review body explicitly documents the verdict mechanism inline in every review:

> **If Codex has suggestions, it will comment; otherwise it will react with 👍.**

Reaction evidence on PRs where Codex had no further suggestions:

```jsonc
// gh api repos/JoeyTeng/codex-background-task-handler/issues/36/reactions
[{"content": "+1", "user": {"login": "chatgpt-codex-connector[bot]"}}]

// gh api repos/guaguastandup/zotero-pdf2zh/issues/301/reactions
[{"content": "+1", "user": {"login": "chatgpt-codex-connector[bot]", "type": "Bot"}}]
```

Raw payload: `docs/spike-evidence/codex-review-sample.json`, `docs/spike-evidence/codex-pr-reactions.json`.

### Implication for SYMPHONY.md

The "Review loop / Verdict parsing" section's mapping from review `state` to verdict is wrong. Replace with:

- **Approved:** there exists a `+1` reaction on the PR from `chatgpt-codex-connector[bot]` whose `created_at` is after the HEAD commit's `committer.date` and *no* fresh `COMMENTED` review with non-empty body covers the HEAD commit.
- **Changes requested:** the latest Codex review whose `commit_id == HEAD` has `state == "COMMENTED"` and a non-empty body (the inline About-Codex `<details>` boilerplate is always present, so a "non-empty body" must be measured *with the boilerplate stripped or above a length floor*; a body length of < 600 chars is empirically near-pure boilerplate).
- **Pending:** Codex hasn't yet acted on HEAD (no review on the HEAD commit_id and no fresh 👍).

The PR-level `+1` reaction does not include a `commit_id`, so timing-based association with the HEAD commit is required. A safer alternative: detect "Codex re-reviewed without suggestions" by observing the React event in the PR timeline (`gh api repos/{o}/{r}/issues/{n}/timeline`), which carries timestamps and orderings.

---

## Spike 2 — `@codex review` trigger syntax: **CONFIRMED**

**Question:** Does posting `@codex review` as a PR comment trigger a fresh Codex review?

**Answer:** Yes. Codex documents the trigger inline in every review it posts:

> Reviews are triggered when you
> - Open a pull request for review
> - Mark a draft as ready
> - Comment "@codex review".

(Source: every Codex review body — see `docs/spike-evidence/codex-review-sample.json`.)

This is canonical, owner-published behavior. No design change needed.

### Implication for SYMPHONY.md

No change. The "Trigger: post `@codex review` PR comment on open and after every push (idempotent)" decision stands.

Note: opening a PR or marking it ready already triggers a review automatically, so Symphony's own initial `@codex review` comment is redundant on PR open and only strictly necessary as a re-nudge after subsequent pushes. Keeping it on open is harmless (idempotent).

---

## Spike 3 — `claude --resume` context preservation: **CONFIRMED**

**Question:** Does `claude --resume <session_id>` preserve conversation context across invocations?

**Answer:** Yes. Round-trip test passed.

### Procedure

```sh
# Round 1 — establish context
claude -p --output-format json --max-turns 1 \
  "Remember the secret number is 42. Just acknowledge in one short sentence."
# → captured session_id = 1e2e28ee-7f4b-40ee-8780-964ef51d7da7
# → result: "Got it — secret number 42 noted."

# Round 2 — resume and recall
claude --resume 1e2e28ee-7f4b-40ee-8780-964ef51d7da7 \
  -p --output-format json --max-turns 1 \
  "What was the secret number I just told you? Answer with just the number."
# → result: "42"
# → same session_id returned
# → cache_read_input_tokens: 27840 (vs cache_creation_input_tokens: 42) — confirms full context replay
```

Environment: `claude` v2.1.129, model `claude-opus-4-7[1m]`, working dir `/tmp/symphony-spike3` (clean — no project memory or CLAUDE.md interference).

Raw payloads: `docs/spike-evidence/claude-resume-round1.json`, `claude-resume-round2.json`.

### Implication for SYMPHONY.md

No change. The "rounds 1–3 resume same session, rounds 4–10 fresh session" decision stands.

Operational note: the result event's `session_id` field (top-level, not nested) is the authoritative source for capture. Symphony's agent runner should persist this from the final `result` event of round 1.

---

## Spike 4 — Branch protection + `gh pr merge --auto`: **DESIGN INVALIDATED (cascades from Spike 1)**

**Question:** Does configuring branch protection (Codex review + 1 CI check required) plus arming `gh pr merge --auto` actually fire the merge once both conditions are met?

**Answer:** The mechanical question is moot — **Codex review can never satisfy a "required approving reviewers" branch protection rule** because Codex never submits a review with `state == "APPROVED"` (Spike 1). GitHub's branch protection counts only `APPROVED` reviews toward the required-reviewer threshold; `COMMENTED` reviews do not count, regardless of body content. (`gh pr merge --auto` itself is well-documented: it queues a merge that fires when all required checks + approving reviews are green; the documentation is not in dispute. The premise of routing Codex's signal *through* branch protection is.)

**Direct test not run.** Setting up a throwaway repo with the Codex GH App installed requires manual UI authorization (the app's installation flow is interactive and tied to a GitHub account / org). Given Spike 1's result already invalidates the design, validating the merge mechanism end-to-end against a setup that we know can't approve adds no information. If the design switches to "Symphony fires the merge directly" (see below), spike 4 becomes a different question — and the answer for that one is trivial: `gh pr merge --squash --delete-branch` from a user who has merge rights, against a PR that meets all branch-protection requirements, just merges.

### Implication for SYMPHONY.md

The "Output / merge" section needs to change. Two viable replacements:

**Option A — Symphony fires merge directly (recommended):**
- Drop "required approving reviewers" from branch protection requirements.
- Keep "required status checks" (CI must be green).
- Symphony, on detecting Codex 👍 + green required checks, runs `gh pr merge <n> --squash --delete-branch` itself.
- Removes one safety layer (no GitHub-enforced "Codex approved" gate), but adds none we didn't already have — Symphony was always going to be the one interpreting the verdict.
- Preflight check shifts from "branch protection requires approving reviews" to "branch protection requires status checks" (and a check that `gh` auth has merge rights).

**Option B — Symphony posts the `APPROVED` review itself:**
- Symphony, on Codex 👍, submits an `APPROVED` review using `gh pr review <n> --approve` under the user's identity.
- `gh pr merge --auto` (already armed) fires.
- Pro: keeps GitHub as the merge actor; preserves the merge-via-branch-protection invariant.
- Con: requires that the PR author identity differ from the approver identity (branch protection forbids self-approval by default). Since the issue-implementer agent commits under the Symphony bot identity but the user's `gh` auth is `kulichevskiy`, this may work — *if* Symphony's bot commits are not authored as `kulichevskiy`. Worth verifying before committing to this option.

Option A is recommended. It collapses two indirections (Symphony → GitHub branch protection → merge) into one (Symphony → merge), which is also closer to the actual control flow Symphony already needs.

---

## Affected downstream issues

- **#3 — Review loop:** must implement reaction-based verdict polling (poll PR `+1` reactions from `chatgpt-codex-connector[bot]` AND latest Codex review on HEAD commit) instead of the simple review-state mapping.
- **#4 — Single-issue happy path:** must change the merge step from `gh pr merge --auto --squash --delete-branch` (armed at PR-open time) to `gh pr merge --squash --delete-branch` fired by Symphony after verdict resolves. The PR-open code path no longer arms auto-merge.
- **#5 — Preflight:** branch-protection check should require status checks but **not** required-approving-reviewers (since no party — Codex or Symphony itself, under sensible auth — will be posting `APPROVED` reliably).

## What's not yet validated

- Codex behavior on draft PRs and on subsequent `@codex review` re-nudges within the same PR (assumed to behave as documented; not directly observed across multiple nudges in our sample).
- Latency between commit push and Codex's first review (anecdotally <2 min in samples; not measured rigorously).
- Whether Codex re-runs after `@codex address that feedback` is materially different from `@codex review`. Out of scope for M0.
