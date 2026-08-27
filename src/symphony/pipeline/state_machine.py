"""Pure transition decisions for the pipeline state machine.

Given the current stage and the terminal runner event, return the next
run status, the Linear state to move the issue to (if any), and whether
the pipeline halts here.

The transition decision only classifies the runner result. The orchestrator
owns side effects such as moving Linear between workflow states, opening PRs,
and starting Review/Merge monitor rows.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

RunStatus = Literal["running", "completed", "failed"]


@dataclass(frozen=True)
class Transition:
    next_run_status: RunStatus
    next_linear_state: str | None
    halt: bool


# --- Completion gate ---------------------------------------------------------
#
# Exit code 0 alone does not mean the Implement stage succeeded: an agent that
# politely ends its turn blocked on a human action (e.g. "please authorize this
# OAuth URL", MCH-14) also exits 0. The agent must therefore end its final
# message with a machine-readable marker, and the orchestrator additionally
# checks that HEAD advanced over the branch base. When neither signal is
# present we fall back to a cheap classifier of the final message.

SYMPHONY_DONE_MARKER = "SYMPHONY_DONE"
SYMPHONY_BLOCKED_PREFIX = "SYMPHONY_BLOCKED:"
SYMPHONY_ALREADY_DONE_PREFIX = "SYMPHONY_ALREADY_DONE:"

ImplementOutcome = Literal["completed", "blocked", "failed", "already_satisfied"]
ClassifierVerdict = Literal["done", "blocked", "ambiguous"]
MarkerKind = Literal["done", "blocked", "already_done"]

_DONE_MARKER_RE = re.compile(rf"(?m)^[ \t>*-]*{SYMPHONY_DONE_MARKER}\s*$")
_BLOCKED_MARKER_RE = re.compile(rf"(?m)^[ \t>*-]*{SYMPHONY_BLOCKED_PREFIX}\s*(?P<reason>.*?)\s*$")
_ALREADY_DONE_MARKER_RE = re.compile(
    rf"(?m)^[ \t>*-]*{SYMPHONY_ALREADY_DONE_PREFIX}\s*(?P<ref>.*?)\s*$"
)

# Phrases an agent uses when it is stuck waiting on a human (the bug being
# fixed). Lower-cased substring match on the final message.
_BLOCKED_SIGNALS: tuple[str, ...] = (
    "i need you to",
    "i need your",
    "need you to authorize",
    "needs your authorization",
    "requires your authorization",
    "please authorize",
    "please provide",
    "please paste",
    "please share",
    "please grant",
    "please run",
    "please log in",
    "please sign in",
    "authorize the",
    "waiting for you",
    "waiting for your",
    "give me access",
    "grant access",
    "log in to",
    "sign in to",
    "manual intervention",
    "human intervention",
    "cannot proceed",
    "can't proceed",
    "unable to proceed",
    "your approval",
    "your credentials",
    "oauth",
)
# Phrases that signal genuine completion. Only consulted in the no-marker /
# no-commit fallback, and only when no blocked signal is present.
_DONE_SIGNALS: tuple[str, ...] = (
    "i've completed",
    "i have completed",
    "completed the",
    "successfully implemented",
    "implementation is complete",
    "changes are complete",
    "all tests pass",
    "already satisfied",
    "nothing to change",
    "no changes needed",
)


@dataclass(frozen=True)
class CompletionMarker:
    kind: MarkerKind | None
    blocked_reason: str = ""
    already_done_ref: str = ""


@dataclass(frozen=True)
class ImplementCompletion:
    outcome: ImplementOutcome
    blocked_reason: str = ""
    already_satisfied_ref: str = ""


def parse_completion_marker(final_message: str) -> CompletionMarker:
    """Read the `SYMPHONY_DONE` / `SYMPHONY_BLOCKED: <reason>` /
    `SYMPHONY_ALREADY_DONE: <ref>` final-line marker.

    The agent may quote the contract from the prompt earlier in its message, so
    the *last* marker is the operative one (mirrors the local-review verdict
    parsing). The blocked reason / already-done ref is captured verbatim from
    the prefix to the end of the line.

    `SYMPHONY_ALREADY_DONE: <ref>` is the no-op-done signal: the agent verified
    the scope was already delivered elsewhere and produced no commit. It is a
    distinct marker (not inferred from `SYMPHONY_DONE` + clean tree) so a plain
    done-without-commits stays a failure.
    """
    done_matches = list(_DONE_MARKER_RE.finditer(final_message))
    blocked_matches = list(_BLOCKED_MARKER_RE.finditer(final_message))
    already_matches = list(_ALREADY_DONE_MARKER_RE.finditer(final_message))
    last_done = done_matches[-1].start() if done_matches else -1
    last_blocked = blocked_matches[-1].start() if blocked_matches else -1
    last_already = already_matches[-1].start() if already_matches else -1
    best = max(last_done, last_blocked, last_already)
    if best < 0:
        return CompletionMarker(kind=None)
    if last_blocked == best:
        reason = blocked_matches[-1].group("reason").strip()
        return CompletionMarker(kind="blocked", blocked_reason=reason)
    if last_already == best:
        ref = already_matches[-1].group("ref").strip()
        return CompletionMarker(kind="already_done", already_done_ref=ref)
    return CompletionMarker(kind="done")


def classify_blocked_final_message(message: str) -> tuple[ClassifierVerdict, str]:
    """Cheap classifier of an unmarked final message (pattern mirrors the
    acceptance classifier: parse the agent's final text, no extra LLM round
    trip). Returns ``(verdict, human_action_ask)``.

    Blocked detection wins over done detection: an agent that did some work but
    then stalled on a human action is blocked, not done.
    """
    text = message.casefold()
    if any(signal in text for signal in _BLOCKED_SIGNALS):
        return "blocked", message.strip()
    if any(signal in text for signal in _DONE_SIGNALS):
        return "done", ""
    return "ambiguous", ""


def classify_implement_completion(
    *,
    final_message: str,
    head_advanced: bool,
    branch_ahead_of_base: bool = False,
    tree_clean: bool = False,
    classifier: Callable[[str], tuple[ClassifierVerdict, str]] = (classify_blocked_final_message),
) -> ImplementCompletion:
    """Classify an rc=0 Implement run into completed / blocked / failed.

    * ``SYMPHONY_BLOCKED: …`` -> ``blocked`` with the reason verbatim.
    * ``SYMPHONY_ALREADY_DONE: <ref>`` without commits -> ``already_satisfied``
      with the ref verbatim (scope already delivered elsewhere; no-op done).
      With a HEAD advance it is a normal ``completed`` (commits are ground
      truth). This is a distinct marker so a plain ``SYMPHONY_DONE`` without
      commits stays ``failed`` — the no-op guard is not weakened.
    * ``SYMPHONY_DONE`` + HEAD advanced -> ``completed`` (the happy path).
    * ``SYMPHONY_DONE`` + HEAD-not-advanced + ``branch_ahead_of_base`` +
      ``tree_clean`` -> ``completed``. Converges a killed-then-redispatched
      run: the conservative re-run re-confirms work already committed on the
      branch (ahead of base, nothing uncommitted) but makes no *new* commit.
      The no-op guard is not weakened — branch-not-ahead or a dirty tree still
      falls through to ``failed``.
    * ``SYMPHONY_DONE`` without commits and not ahead (or dirty) -> ``failed``
      (claimed done, produced nothing to push).
    * No marker but HEAD advanced -> ``completed`` (commits are ground truth).
    * No marker AND no commits -> run ``classifier`` on the final message; only
      a ``blocked`` ask is actionable. A ``done`` verdict here cannot mean
      completed (no commits to push), so done/ambiguous -> ``failed``.

    The classifier fallback runs *only* in the last case (marker missing and
    HEAD did not advance).
    """
    marker = parse_completion_marker(final_message)
    if marker.kind == "blocked":
        return ImplementCompletion(outcome="blocked", blocked_reason=marker.blocked_reason)
    if marker.kind == "already_done":
        # Ground truth wins: a real HEAD advance means the agent actually
        # committed, so this is a normal completion regardless of the claim.
        if head_advanced:
            return ImplementCompletion(outcome="completed")
        return ImplementCompletion(
            outcome="already_satisfied", already_satisfied_ref=marker.already_done_ref
        )
    if marker.kind == "done":
        if head_advanced:
            return ImplementCompletion(outcome="completed")
        if branch_ahead_of_base and tree_clean:
            return ImplementCompletion(outcome="completed")
        return ImplementCompletion(outcome="failed")
    if head_advanced:
        return ImplementCompletion(outcome="completed")
    verdict, ask = classifier(final_message)
    if verdict == "blocked":
        return ImplementCompletion(outcome="blocked", blocked_reason=ask)
    # No commits exist: a "done" verdict cannot mean completed (rc=0 without
    # commits and without SYMPHONY_DONE never classifies as completed). Only a
    # blocked ask is actionable here; everything else is a failure.
    return ImplementCompletion(outcome="failed")


def on_runner_event(*, stage: str, event_kind: str, returncode: int | None) -> Transition:
    if event_kind == "exit" and returncode == 0:
        if stage == "implement":
            return Transition(next_run_status="completed", next_linear_state=None, halt=True)
        return Transition(next_run_status="completed", next_linear_state=None, halt=True)
    return Transition(next_run_status="failed", next_linear_state=None, halt=True)


def _final_event_termination(final_kind: str | None, returncode: int | None) -> str | None:
    if final_kind in {"stall_timeout", "wall_clock_timeout", "spawn_failed"}:
        return final_kind
    if final_kind == "exit" and returncode not in (None, 0):
        return "agent_nonzero_exit"
    return None


def _is_orphaned_termination(status: str, text: str) -> bool:
    return status == "interrupted" and any(
        signal in text
        for signal in ("host restarted", "startup reconcile", "pidless", "orphan", "pid ")
    )


def _is_cancelled_termination(text: str) -> bool:
    return any(
        signal in text for signal in ("cancelled", "canceled", "skipped", "$stop", "operator stop")
    )


def _is_validation_failure(text: str) -> bool:
    return any(
        signal in text
        for signal in (
            "without advancing",
            "validation",
            "acceptance rejected",
            "failed criteria",
            "infra_error",
            "infra error",
        )
    )


def _is_tracker_error(text: str) -> bool:
    return any(
        signal in text
        for signal in (
            "move_issue",
            "post_comment",
            "comment failed",
            "team_states",
            "lookup_issue",
            "tracker",
            "linear",
            "pr_create",
            "repo_default_branch",
            "github",
            "no pr number",
            "no longer matches",
        )
    )


def _text_termination_kind(*, status: str, text: str) -> str | None:
    if "superseded" in text or "stale merge" in text:
        return "superseded"
    if _is_orphaned_termination(status, text):
        return "orphaned"
    if _is_cancelled_termination(text):
        return "cancelled"
    if "rebase" in text:
        return "rebase_failed"
    if any(signal in text for signal in ("push failed", "force-push", "git push")):
        return "push_failed"
    if _is_validation_failure(text):
        return "validation_failed"
    if _is_tracker_error(text):
        return "tracker_error"
    return None


def classify_termination(
    *,
    status: str,
    final_kind: str | None = None,
    returncode: int | None = None,
    exc: BaseException | str | None = None,
    reason: str | None = None,
) -> tuple[str, str]:
    """Classify why a run reached a non-success terminal status.

    This is telemetry only: it must not change the state-machine transition
    decision made by `on_runner_event`.
    """
    if status in {"completed", "done"}:
        return "", ""

    detail = _termination_detail(
        status=status,
        final_kind=final_kind,
        returncode=returncode,
        exc=exc,
        reason=reason,
    )
    text = " ".join(
        part
        for part in (
            status,
            final_kind or "",
            str(returncode) if returncode is not None else "",
            reason or "",
            str(exc) if exc is not None else "",
        )
        if part
    ).casefold()

    if event_kind := _final_event_termination(final_kind, returncode):
        return event_kind, detail
    if status == "needs_approval":
        return "awaiting_human_merge", detail
    if text_kind := _text_termination_kind(status=status, text=text):
        return text_kind, detail
    if exc is not None:
        return "execution_error", detail
    return "unknown", detail


def _termination_detail(
    *,
    status: str,
    final_kind: str | None,
    returncode: int | None,
    exc: BaseException | str | None,
    reason: str | None,
) -> str:
    if (
        final_kind == "exit"
        and returncode is not None
        and (not reason or reason == "runner ended with exit")
    ):
        return f"runner exited with return code {returncode}"
    if reason:
        return reason
    if exc is not None:
        if isinstance(exc, BaseException):
            text = str(exc)
            return text or type(exc).__name__
        return str(exc)
    if final_kind:
        return f"runner ended with {final_kind}"
    return status
