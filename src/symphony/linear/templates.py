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

# Linear silently rejects very long comment bodies. 4 KB is the documented
# soft cap; truncating in the caller keeps dispatch deterministic when a
# `failed` comment's log-tail field would otherwise blow past it.
COMMENT_BYTE_LIMIT = 4096

_TRUNCATION_SUFFIX = "\n\n…[truncated]"


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
    cost: str = "$0.00"
    review_iter: int = 0
    trigger: str = ""
    error: str = ""
    last_log: str = ""
    next_stage: str = ""
    linear_identifier: str = ""
    pct: int = 0
    commit_url: str = ""
    auto_retry: bool = False


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
        f"- Cost so far: **{v.cost}**\n"
        f"- Run ID: `{v.run_id}`\n"
    )


def awaiting_approval(v: CommentVars) -> str:
    body = (
        f"🟡 **Awaiting approval — {v.stage} → next stage**\n\n"
        f"Symphony has paused on `{v.repo}#{v.issue}` after **{v.stage}**.\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Cost so far: **{v.cost}**\n"
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
        f"- Cumulative cost: **{v.cost}**\n\n"
        f"PR: {v.pr_url}\n\n"
        f"Reply with `$approve` to force-advance, `$reject` to stop, or "
        f"free-form steering for one more attempt.\n"
    )


def cost_cap_reached(v: CommentVars) -> str:
    return (
        f"🟠 **Cost cap reached — pipeline paused**\n\n"
        f"Symphony paused `{v.repo}#{v.issue}` because cumulative agent "
        f"cost reached **{v.cost}** during **{v.stage}**.\n\n"
        f"- Run ID: `{v.run_id}`\n"
        f"- PR: {v.pr_url}\n\n"
        f"After raising the cap, reply with `$approve` or `$retry` to requeue. "
        f"Reply with `$reject` or `$stop` to leave the issue halted.\n"
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
        f"- Cost: **{v.cost}**\n"
    )
    if v.last_log:
        body += f"\nLast log lines:\n\n```\n{v.last_log}\n```\n"
    if v.auto_retry:
        body += "\nWill auto-retry shortly.\n"
    return body


def cost_warning(v: CommentVars) -> str:
    return (
        f"💸 Cost notice — `{v.repo}#{v.issue}` has used **{v.cost}** "
        f"({v.pct}% of cap)\n\n"
        f"Next stuck-loop escape will fire if cost reaches the cap. PR: {v.pr_url}\n"
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
        f"- Cost so far: **{v.cost}**\n"
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
