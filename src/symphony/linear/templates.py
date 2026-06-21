"""Markdown templates for Symphony's outbound Linear comments.

Templates live in code (not external files) so renames during refactor stay
type-checked. Mirrors §29 of `docs/linear-integration-research.md` with one
adjustment for the Python port: Linear is the *only* UI here, so the
"awaiting approval" template tells users to act in Linear directly rather
than referring back to a Symphony app.

Format constraints (carried over from the prior research):
- One leading emoji per body so phone push notifications carry a glanceable signal.
- Hard cap each body at 4 KB. Caller must truncate large fields (e.g. log tails).
"""

from __future__ import annotations

from dataclasses import dataclass

from ..tokens import effective_tokens

# Linear silently rejects very long comment bodies. 4 KB is the documented
# soft cap; truncating in the caller keeps dispatch deterministic when a
# `failed` comment's log-tail field would otherwise blow past it.
COMMENT_BYTE_LIMIT = 4096

_TRUNCATION_SUFFIX = "\n\n…[truncated]"
SYMPHONY_COMMENT_MARKER = "<!-- symphony:comment -->"
_SYMPHONY_COMMENT_SUFFIX = f"\n\n{SYMPHONY_COMMENT_MARKER}"


def is_symphony_comment(body: str) -> bool:
    return SYMPHONY_COMMENT_MARKER in body


def mark_symphony_comment(body: str, *, limit: int = COMMENT_BYTE_LIMIT) -> str:
    if is_symphony_comment(body):
        return body
    marker_size = len(_SYMPHONY_COMMENT_SUFFIX.encode("utf-8"))
    if limit <= marker_size:
        return _SYMPHONY_COMMENT_SUFFIX.encode("utf-8")[:limit].decode(
            "utf-8", errors="ignore"
        )
    return truncate_body(body, limit=limit - marker_size) + _SYMPHONY_COMMENT_SUFFIX


def truncate_body(body: str, *, limit: int = COMMENT_BYTE_LIMIT) -> str:
    """Cap a comment body at `limit` UTF-8 bytes, appending an ellipsis
    marker when truncation occurs. The marker itself is included inside
    the limit so the returned string is always within budget."""
    encoded = body.encode("utf-8")
    if len(encoded) <= limit:
        return body
    if limit <= 0:
        return ""
    suffix = _TRUNCATION_SUFFIX
    suffix_encoded = suffix.encode("utf-8")
    if len(suffix_encoded) >= limit:
        return suffix_encoded[:limit].decode("utf-8", errors="ignore")
    head_budget = limit - len(suffix_encoded)
    head = encoded[:head_budget].decode("utf-8", errors="ignore")
    return head + suffix


@dataclass
class CommentVars:
    stage: str  # implement|review|merge
    repo: str
    issue: int
    pr_url: str = "(no PR yet)"
    run_id: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0
    review_iter: int = 0
    trigger: str = ""
    error: str = ""
    last_log: str = ""
    next_stage: str = ""
    linear_identifier: str = ""
    commit_url: str = ""
    auto_retry: bool = False


def token_block(v: CommentVars) -> str:
    """Render the per-issue token breakdown shown in outbound comments.

    Replaces the former dollar `cost` line: input / output / cache write /
    cache read deltas as a 4-way inline split, so operators can gauge spend
    without a pricing model baked into the comment. The trailing `eff`
    figure is the weighted *effective* token total — the unit the per-issue
    budget gates on — so an operator deciding `$approve` sees the same number
    as the ceiling.
    """
    eff = effective_tokens(
        v.input_tokens,
        v.output_tokens,
        v.cache_write_tokens,
        v.cache_read_tokens,
    )
    return (
        f"Tokens: in {v.input_tokens} · out {v.output_tokens} · "
        f"cache w {v.cache_write_tokens} / r {v.cache_read_tokens} · "
        f"eff {eff:,.0f}"
    )


def run_started(v: CommentVars) -> str:
    return (
        f"🚀 **Implement starting** on `{v.repo}#{v.issue}`\n\n"
        f"- Run ID: `{v.run_id}`\n"
        f"- Workspace cloned, agent dispatched.\n"
    )


def moved_to_waiting(v: CommentVars, blockers: list[str]) -> str:
    blocker_list = ", ".join(f"`{identifier}`" for identifier in blockers)
    target = v.linear_identifier or f"{v.repo}#{v.issue}"
    return (
        f"🟠 **Moved to {v.next_stage} — dependency blocked**\n\n"
        f"Symphony did not start `{target}` because Linear says it "
        f"is blocked by: {blocker_list}.\n\n"
        "Return this issue to the ready lane manually after the blocker closes. "
        "Automatic return-to-ready is planned for the next dependency slice.\n"
    )


def stage_done(v: CommentVars) -> str:
    return (
        f"✓ **{v.stage.title()} → {v.next_stage.title()}**\n\n"
        f"- PR: {v.pr_url}\n"
        f"- {token_block(v)}\n"
        f"- Run ID: `{v.run_id}`\n"
    )


def awaiting_approval(v: CommentVars) -> str:
    body = (
        f"🟡 **Awaiting approval — {v.stage} → next stage**\n\n"
        f"Symphony has paused on `{v.repo}#{v.issue}` after **{v.stage}**.\n\n"
        f"- PR: {v.pr_url}\n"
        f"- {token_block(v)}\n"
        f"- Run ID: `{v.run_id}`\n\n"
    )
    if v.error:
        body += f"- Error: `{v.error}`\n\n"
    body += (
        "Reply on this issue with:\n"
        "- `$approve` — advance to the next stage\n"
        "- `$reject` — stop the pipeline here\n"
        "- Free-form text — queued as steering for the next stage's prompt\n"
    )
    return body


def stuck_loop_escape(v: CommentVars) -> str:
    return (
        f"🟠 **Stuck-loop escape — review iteration cap reached**\n\n"
        f"Symphony has parked `{v.repo}#{v.issue}` because:\n\n"
        f"- Review iterations: **{v.review_iter}** (cap reached)\n"
        f"- Last trigger: **{v.trigger}**\n"
        f"- {token_block(v)}\n\n"
        f"PR: {v.pr_url}\n\n"
        f"Reply with `$approve` to force-advance, `$reject` to stop, or "
        f"free-form steering for one more attempt.\n"
    )


def budget_exceeded(
    v: CommentVars,
    *,
    used_effective: float,
    ceiling: float,
    breakdown: list[tuple[str, float]],
) -> str:
    """Per-issue token budget tripped — parked for a human decision.

    Shows effective tokens used vs the ceiling (`per_issue_token_budget +
    granted_token_budget`) with a per-stage breakdown. The unit is effective
    tokens (input + output + cache_write*1.25 + cache_read*0.1), not dollars.
    The live agent is never killed; the next run was simply not dispatched.
    """
    lines = "".join(
        f"  - {stage}: {round(value):,}\n"
        for stage, value in sorted(breakdown, key=lambda kv: kv[1], reverse=True)
    )
    return (
        f"🟡 **Token budget exceeded — parked for approval**\n\n"
        f"Symphony has paused `{v.repo}#{v.issue}`: this issue's cumulative "
        f"effective tokens crossed its per-issue budget.\n\n"
        f"- Effective tokens used: **{round(used_effective):,}** "
        f"(ceiling **{round(ceiling):,}**)\n"
        f"- Unit: effective tokens "
        f"(in + out + cache_write×1.25 + cache_read×0.1), not dollars\n"
        f"- Per-stage breakdown:\n{lines}"
        f"- Run ID: `{v.run_id}`\n"
        f"- PR: {v.pr_url}\n\n"
        f"Reply with `$approve` (or 👍) to grant another budget window and "
        f"resume, or `$reject` to stop here.\n"
    )


def acceptance_blocked(v: CommentVars) -> str:
    detail = f"\n\nLast infra error: {v.error}\n" if v.error else "\n"
    return (
        f"🟠 **Acceptance blocked — infrastructure error**\n\n"
        f"Symphony paused `{v.repo}#{v.issue}` after acceptance infra retries "
        f"were exhausted.\n\n"
        f"- Run ID: `{v.run_id}`\n"
        f"- PR: {v.pr_url}\n"
        f"{detail}\n"
        "Reply with `$retry-acceptance` to run Acceptance again. "
        "Reply with `$skip-acceptance` to merge despite the blocked "
        "Acceptance stage.\n"
    )


def acceptance_retry_requested(v: CommentVars) -> str:
    return (
        f"↻ **Acceptance retry requested**\n\n"
        f"`$retry-acceptance` received for `{v.repo}#{v.issue}`.\n\n"
        f"- Run ID: `{v.run_id}`\n"
        f"- PR: {v.pr_url}\n\n"
        "Cleared Acceptance state; the next poll will run Acceptance again.\n"
    )


def acceptance_skipped(v: CommentVars) -> str:
    return (
        f"⏭️ **Acceptance skipped — advancing to merge**\n\n"
        f"`$skip-acceptance` received on `{v.repo}#{v.issue}`.\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Run ID: `{v.run_id}`\n\n"
        "Dispatching merge now.\n"
    )


def review_stopped(v: CommentVars) -> str:
    return (
        f"⏸️ **Review monitor stopped — pipeline paused**\n\n"
        f"Symphony stopped review monitoring for `{v.repo}#{v.issue}`.\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Run ID: `{v.run_id}`\n\n"
        "Reply with `$retry` or `$approve` to resume review monitoring. "
        "Reply with `$reject` or `$stop` to leave it halted.\n"
    )


def failed(v: CommentVars) -> str:
    body = (
        f"🔴 **{v.stage.title()} stage failed — pipeline halted**\n\n"
        f"`{v.repo}#{v.issue}` failed at **{v.stage}**.\n\n"
        f"- Error: `{v.error}`\n"
        f"- PR: {v.pr_url}\n"
        f"- Run ID: `{v.run_id}`\n"
        f"- {token_block(v)}\n"
    )
    if v.last_log:
        body += f"\nLast log lines:\n\n```\n{v.last_log}\n```\n"
    if v.auto_retry:
        body += "\nWill auto-retry shortly.\n"
    return body


def implement_blocked(v: CommentVars) -> str:
    """Human-action handoff for a blocked Implement run.

    `v.error` carries the agent's `SYMPHONY_BLOCKED` reason verbatim — the
    OAuth URL, the secret to provide, etc. It is reproduced unmodified so the
    operator can act on the precise ask, then resume with `$retry`.
    """
    return (
        f"🔒 **Implement blocked — waiting on a human action**\n\n"
        f"Symphony paused `{v.repo}#{v.issue}` after **implement**: the agent "
        f"committed what it could but cannot finish without a human action.\n\n"
        f"- Run ID: `{v.run_id}`\n"
        f"- {token_block(v)}\n\n"
        f"**Blocked on (verbatim from the agent):**\n\n"
        f"```\n{v.error}\n```\n\n"
        "**What to do:** perform the action above, then reply `$retry` on this "
        "issue. Put any tokens, URLs, or instructions the agent needs in that "
        "same comment — its text is handed to the fresh run. Your prior work in "
        "the workspace is preserved (uncommitted changes are kept).\n"
    )


def implement_already_satisfied(v: CommentVars, *, delivered_ref: str) -> str:
    """No-op-done close for an Implement run whose scope already landed.

    The agent verified every acceptance criterion was already satisfied in the
    current tree (nothing to commit) and named the delivering commit/PR via
    `SYMPHONY_ALREADY_DONE`. The orchestrator verified that commit is an
    ancestor of HEAD before moving the issue to Done — `delivered_ref` is
    reproduced verbatim so the close is auditable.
    """
    return (
        f"✅ **Already delivered — closing as done**\n\n"
        f"Symphony found every acceptance criterion for `{v.repo}#{v.issue}` "
        f"already satisfied in the current tree, so **implement** made no "
        f"commit.\n\n"
        f"- Delivered by: `{delivered_ref}`\n"
        f"- Run ID: `{v.run_id}`\n"
        f"- {token_block(v)}\n\n"
        "No PR was opened (nothing to push). Moved to Done. Reopen and reply "
        "`$retry` if this close was wrong.\n"
    )


def resumed(v: CommentVars) -> str:
    return f"✅ Resumed — advancing `{v.repo}#{v.issue}` to **{v.next_stage}**\n"


def review_retry_requested(v: CommentVars) -> str:
    return (
        f"✅ **Review retry requested** on `{v.repo}#{v.issue}`\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Run ID: `{v.run_id}`\n"
        "- Re-posted `@codex review`; the next review signal will be treated as fresh.\n"
    )


def reviewing_feedback(v: CommentVars) -> str:
    return (
        f"👀 **Reviewer feedback detected** on `{v.repo}#{v.issue}` "
        f"(review iteration {v.review_iter})\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Trigger: `{v.trigger}`\n"
        f"- Dispatching fix-run…\n"
    )


def fixing_merge_conflict(v: CommentVars) -> str:
    return (
        f"🔀 **Merge conflict detected** on `{v.repo}#{v.issue}` "
        f"(review iteration {v.review_iter})\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Rebasing onto base branch and resolving conflicts…\n"
    )


def fix_pushed(v: CommentVars) -> str:
    commit_line = (
        f"- Commit: [{v.commit_url.split('/')[-1][:8]}]({v.commit_url})\n"
        if v.commit_url
        else ""
    )
    return (
        f"📤 **Fix pushed** for `{v.repo}#{v.issue}`\n\n"
        f"- PR: {v.pr_url}\n"
        f"{commit_line}"
        f"- Re-triggered `@codex review` — waiting for re-review.\n"
        f"- {token_block(v)}\n"
    )


def command_rejected(slash: str, reason: str) -> str:
    return f"🚫 `{slash}` ignored: {reason}\n"


def codex_lgtm(v: CommentVars) -> str:
    return (
        f"✅ **Codex reviewed — no issues** on `{v.repo}#{v.issue}`\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Codex found no blocking issues.\n"
    )


def skip_review_forced(v: CommentVars) -> str:
    return (
        f"⏭️ **Review skipped — advancing to merge**\n\n"
        f"`$skip-review` received on `{v.repo}#{v.issue}`.\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Run ID: `{v.run_id}`\n\n"
        f"Review stage cancelled. Dispatching merge now.\n"
    )


def acceptance_rejected(v: CommentVars) -> str:
    return (
        f"🟠 **Acceptance rejected — pipeline paused**\n\n"
        f"Acceptance still rejected `{v.repo}#{v.issue}` after the "
        "acceptance fix attempt.\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Run ID: `{v.run_id}`\n\n"
        "Reply with `$skip-acceptance` to dispatch merge anyway, or "
        "`$retry-acceptance` to reset acceptance state and run acceptance again.\n"
    )


def skip_acceptance_forced(v: CommentVars) -> str:
    return (
        f"⏭️ **Acceptance skipped — advancing to merge**\n\n"
        f"`$skip-acceptance` received on `{v.repo}#{v.issue}`.\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Run ID: `{v.run_id}`\n\n"
        "Acceptance gate cleared. Dispatching merge now.\n"
    )


def retry_acceptance_requested(v: CommentVars) -> str:
    return (
        f"✅ **Acceptance retry requested** on `{v.repo}#{v.issue}`\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Run ID: `{v.run_id}`\n\n"
        "`$retry-acceptance` cleared the acceptance state. Dispatching a fresh "
        "acceptance run now.\n"
    )
