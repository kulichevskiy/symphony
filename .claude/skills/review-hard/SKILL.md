---
name: review-hard
description: Strict, adversarial LOCAL code review of the current diff — no suppression, reads full context, fans out lensed finders on Opus, then verifies each finding by trying to refute it. Use as the local reviewer before pushing or opening a PR, when asked to "review this hard / properly / before I merge", or whenever a thorough review is wanted instead of the soft `code-review`. This is the project's default local code reviewer.
argument-hint: "[--quick] [base ref, default: main] | [PR number]"
---

# review-hard

You are running a **strict, adversarial** code review of the **local uncommitted + committed-but-unpushed changes** on the current branch. This is deliberately more въедливый than `code-review`: the goal is to catch what a soft reviewer misses, not to keep the comment count low.

The skill argument (if any) is optional. Strip a leading `--quick` flag first, then interpret the rest:
- empty → review the diff of the current branch against `main`
- a git ref (e.g. `origin/main`, `HEAD~3`) → diff against that ref
- a number → treat as a PR and review `gh pr diff <N>` instead

**Effort mode** (from the `--quick` flag and the diff size):
- **`--quick`, or a diff under ~150 changed lines / ≤3 files** → run **4 finders** (correctness, intent, contracts, conventions+a11y — fold error-handling into correctness) + the verify pass.
- **Full (default for larger diffs)** → run **all 6 finders** + verify.
- State which mode you picked and why in one line before fanning out, so the cost is visible.

## Operating principles (read these — they are the whole point)

1. **No self-censorship during finding.** Finders list *everything* suspicious, including uncertain items. Do NOT instruct finders to "avoid nitpicks", "do a shallow scan", or "ignore likely false positives". Filtering happens later, as a separate step.
2. **Read the surrounding context, not just the `+/-` lines.** For every changed hunk, read the whole function, its callers, and the relevant tests. Most missed bugs live in the interaction between changed and unchanged code.
3. **Check intent, not just syntax.** Pull the linked issue/PRD and verify the change actually does what it was supposed to — including the cases it forgot.
4. **Adversarial stance.** Assume a bug exists. A senior reviewer would request changes — find the reason. An empty "looks good" is only allowed after a genuine attempt to break it.
5. **Confidence is shown, not used to silence.** Report uncertain findings flagged as such. Do not drop a finding just because it scored below a threshold.

## Steps

### 1. Establish scope and intent
- Get the diff:
  - PR mode: `gh pr diff <N>` and `gh pr view <N> --json title,body`
  - Local mode: `git diff <base>...HEAD` **and** `git diff` (unstaged) **and** `git diff --staged` — review all uncommitted + unpushed work.
- Find the linked issue. Branch is usually `claude/issue-<N>`; if so, fetch it:
  `gh api repos/kulichevskiy/SymphonyMac/issues/<N> --jq '{title, body}'` (adjust repo if needed).
- Read the root `CLAUDE.md` and any `CLAUDE.md` in touched directories. Treat these as hard rules.
- Write a 2–3 sentence summary of *what the change is supposed to do*. Keep it; the verify step grades against it.

### 2. Fan out — parallel finders, each on Opus, each with a distinct lens
Launch these as **parallel Agent calls (model: opus)** (4 or 6 of them per the effort mode above). Each finder reads the diff **plus surrounding context** and returns a list of findings (`file:line`, what, why it's wrong, how to repro/trigger, confidence 0–100). Tell each finder explicitly:
- Do not pre-filter, list everything.
- **Tag every finding `INTRODUCED` or `PRE-EXISTING`**: `INTRODUCED` = this diff created or newly triggers it; `PRE-EXISTING` = the problem already lived in the code/an inherited pattern (e.g. copied from a sibling component, an unchanged helper, a repo-wide convention) and this diff merely touches nearby. When unsure, check `git blame`/the base version of the line. This tag is mandatory.

- **Correctness & edge cases** — empty/null/overflow inputs, off-by-one, wrong branch, missed early-return, state left inconsistent on the error path.
- **Concurrency & ordering** — races, shared mutable state, await/lock gaps, assumptions about call order, partial writes.
- **Error handling & failure modes** — swallowed exceptions, errors logged-and-continued, missing rollback, silent fallbacks that hide bugs. (See the `feedback`/memory note on never hiding behind silent fallbacks.)
- **Intent vs implementation** — does the diff actually satisfy the issue/PRD from step 1? What case did it forget? Any requirement only half-done?
- **Contracts & callers** — every caller of a changed signature/behavior updated? Return-type/None-handling drift? Tests still asserting the real contract (not a stale value)?
- **Config / no-hardcode & conventions** — `uv` only, agent/LLM chosen from config never hardcoded, anything that should come from `binding.agent` etc. Plus CLAUDE.md rules.

### 3. Adversarial verify — try to REFUTE each finding
For each finding from step 2, launch a parallel Agent (model: opus) whose job is to **prove the finding wrong**: read the actual code path, check whether it really triggers, and **confirm or correct the finder's `INTRODUCED`/`PRE-EXISTING` tag** by checking the base version (`git show <base>:<file>` or `git blame`) — finders often mislabel an inherited pattern as introduced. Also check whether a guard elsewhere already handles it. Return: `confirmed` | `refuted` | `uncertain`, the verified `INTRODUCED`/`PRE-EXISTING` tag, and the evidence (code excerpt + reasoning). Dedupe findings that are the same root cause.

### 4. Report — grouped by severity, nothing silently dropped
Do not apply an 80-confidence cutoff. **Lead with what *this diff* broke** — `INTRODUCED` findings come first within each severity group, `PRE-EXISTING` ones after, visibly tagged. The reader's first question is always "what did my change break?", so answer that before inherited/repo-wide issues. Group as:

```
## review-hard — <branch or PR>  [<mode: quick|full>]

What this change is meant to do: <2-3 sentences>

### 🔴 Blocking — confirmed, will bite in practice
- [INTRODUCED] `file:line` — <issue>. Why: <repro/trigger>. Fix: <concrete suggestion>.
- [PRE-EXISTING] `file:line` — <issue>. (only if genuinely blocking)

### 🟡 Likely — confirmed but lower impact
- [INTRODUCED] ...
- [PRE-EXISTING] ...

### ⚪ Uncertain — couldn't confirm or refute, worth a human look
- ...  (these are the ones a soft review would have hidden; tag each too)

### Pre-existing / inherited (not caused by this diff — context, don't block on these)
- <one line each> — what it is and why it predates the change.

### Refuted (for transparency)
- <one line each> — why it's not actually a problem.

### Verdict
Request changes / Approve-with-nits / Approve — with the single most important reason. Base the verdict on `INTRODUCED` findings; pre-existing issues inform but do not gate.
```

Rules:
- Cite every finding with `file:line` and a short code excerpt.
- For uncertain findings, say exactly what you couldn't verify and what would settle it.
- If you genuinely find nothing after an adversarial pass, say so — but list what you actively tried to break.
- Do **not** post anything to GitHub. This is a local review; output to the terminal only. (Posting to a PR / `@codex` stays the existing separate flow.)
