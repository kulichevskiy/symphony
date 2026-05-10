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
    pct: int = 0


def run_started(v: CommentVars) -> str:
    return (
        f"▶ **Implement starting** on `{v.repo}#{v.issue}`\n\n"
        f"- Run ID: `{v.run_id}`\n"
        f"- Workspace cloned, agent dispatched.\n"
    )


def stage_done(v: CommentVars) -> str:
    return (
        f"✓ **{v.stage.title()} → {v.next_stage.title()}**\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Cost so far: **{v.cost}**\n"
        f"- Run ID: `{v.run_id}`\n"
    )


def awaiting_approval(v: CommentVars) -> str:
    return (
        f"🟡 **Awaiting approval — {v.stage} → next stage**\n\n"
        f"Symphony has paused on `{v.repo}#{v.issue}` after **{v.stage}**.\n\n"
        f"- PR: {v.pr_url}\n"
        f"- Cost so far: **{v.cost}**\n"
        f"- Run ID: `{v.run_id}`\n\n"
        f"Reply on this issue with:\n"
        f"- `/approve` — advance to the next stage\n"
        f"- `/reject` — stop the pipeline here\n"
        f"- Free-form text — queued as steering for the next stage's prompt\n"
    )


def stuck_loop_escape(v: CommentVars) -> str:
    return (
        f"🟠 **Stuck-loop escape — review iteration cap reached**\n\n"
        f"Symphony has parked `{v.repo}#{v.issue}` because:\n\n"
        f"- Review iterations: **{v.review_iter}** (cap reached)\n"
        f"- Last trigger: **{v.trigger}**\n"
        f"- Cumulative cost: **{v.cost}**\n\n"
        f"PR: {v.pr_url}\n\n"
        f"Reply with `/approve` to force-advance, `/reject` to stop, or "
        f"free-form steering for one more attempt.\n"
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
    body += "\nReply `/retry` in this thread to dispatch again.\n"
    return body


def cost_warning(v: CommentVars) -> str:
    return (
        f"💸 Cost notice — `{v.repo}#{v.issue}` has used **{v.cost}** "
        f"({v.pct}% of cap)\n\n"
        f"Next stuck-loop escape will fire if cost reaches the cap. PR: {v.pr_url}\n"
    )


def resumed(v: CommentVars) -> str:
    return f"✅ Resumed — advancing `{v.repo}#{v.issue}` to **{v.next_stage}**\n"


def command_rejected(slash: str, reason: str) -> str:
    return f"🚫 `{slash}` ignored: {reason}\n"
