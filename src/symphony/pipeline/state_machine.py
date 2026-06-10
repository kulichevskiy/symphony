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

ImplementOutcome = Literal["completed", "blocked", "failed"]
ClassifierVerdict = Literal["done", "blocked", "ambiguous"]
MarkerKind = Literal["done", "blocked"]

_DONE_MARKER_RE = re.compile(rf"(?m)^[ \t>*-]*{SYMPHONY_DONE_MARKER}\s*$")
_BLOCKED_MARKER_RE = re.compile(
    rf"(?m)^[ \t>*-]*{SYMPHONY_BLOCKED_PREFIX}\s*(?P<reason>.*?)\s*$"
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


@dataclass(frozen=True)
class ImplementCompletion:
    outcome: ImplementOutcome
    blocked_reason: str = ""


def parse_completion_marker(final_message: str) -> CompletionMarker:
    """Read the `SYMPHONY_DONE` / `SYMPHONY_BLOCKED: <reason>` final-line marker.

    The agent may quote the contract from the prompt earlier in its message, so
    the *last* marker is the operative one (mirrors the local-review verdict
    parsing). The blocked reason is captured verbatim from the prefix to the
    end of the message.
    """
    done_matches = list(_DONE_MARKER_RE.finditer(final_message))
    blocked_matches = list(_BLOCKED_MARKER_RE.finditer(final_message))
    last_done = done_matches[-1].start() if done_matches else -1
    last_blocked = blocked_matches[-1].start() if blocked_matches else -1
    if last_blocked >= 0 and last_blocked > last_done:
        reason = blocked_matches[-1].group("reason").strip()
        return CompletionMarker(kind="blocked", blocked_reason=reason)
    if last_done >= 0:
        return CompletionMarker(kind="done")
    return CompletionMarker(kind=None)


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
    classifier: Callable[[str], tuple[ClassifierVerdict, str]] = (
        classify_blocked_final_message
    ),
) -> ImplementCompletion:
    """Classify an rc=0 Implement run into completed / blocked / failed.

    * ``SYMPHONY_BLOCKED: …`` -> ``blocked`` with the reason verbatim.
    * ``SYMPHONY_DONE`` + HEAD advanced -> ``completed`` (the happy path).
    * ``SYMPHONY_DONE`` without commits -> ``failed`` (claimed done, produced
      nothing to push).
    * No marker but HEAD advanced -> ``completed`` (commits are ground truth).
    * No marker AND no commits -> run ``classifier`` on the final message to
      decide done vs blocked; still ambiguous -> ``failed``.

    The classifier fallback runs *only* in the last case (marker missing and
    HEAD did not advance).
    """
    marker = parse_completion_marker(final_message)
    if marker.kind == "blocked":
        return ImplementCompletion(outcome="blocked", blocked_reason=marker.blocked_reason)
    if marker.kind == "done":
        if head_advanced:
            return ImplementCompletion(outcome="completed")
        return ImplementCompletion(outcome="failed")
    if head_advanced:
        return ImplementCompletion(outcome="completed")
    verdict, ask = classifier(final_message)
    if verdict == "blocked":
        return ImplementCompletion(outcome="blocked", blocked_reason=ask)
    if verdict == "done":
        return ImplementCompletion(outcome="completed")
    return ImplementCompletion(outcome="failed")


def on_runner_event(
    *, stage: str, event_kind: str, returncode: int | None
) -> Transition:
    if event_kind == "exit" and returncode == 0:
        if stage == "implement":
            return Transition(
                next_run_status="completed", next_linear_state=None, halt=True
            )
        return Transition(
            next_run_status="completed", next_linear_state=None, halt=True
        )
    return Transition(next_run_status="failed", next_linear_state=None, halt=True)


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

    if final_kind == "stall_timeout":
        return "stall_timeout", detail
    if final_kind == "spawn_failed":
        return "spawn_failed", detail
    if final_kind == "exit" and returncode not in (None, 0):
        return "agent_nonzero_exit", detail
    if status == "needs_approval":
        return "awaiting_human_merge", detail
    if "superseded" in text or "stale merge" in text:
        return "superseded", detail
    if (
        status == "interrupted"
        and (
            "host restarted" in text
            or "startup reconcile" in text
            or "pidless" in text
            or "orphan" in text
            or "pid " in text
        )
    ):
        return "orphaned", detail
    if (
        "cancelled" in text
        or "canceled" in text
        or "skipped" in text
        or "$stop" in text
        or "operator stop" in text
    ):
        return "cancelled", detail
    if "rebase" in text:
        return "rebase_failed", detail
    if "push failed" in text or "force-push" in text or "git push" in text:
        return "push_failed", detail
    if (
        "without advancing" in text
        or "validation" in text
        or "acceptance rejected" in text
        or "failed criteria" in text
        or "infra_error" in text
        or "infra error" in text
    ):
        return "validation_failed", detail
    if (
        "move_issue" in text
        or "post_comment" in text
        or "comment failed" in text
        or "team_states" in text
        or "lookup_issue" in text
        or "tracker" in text
        or "linear" in text
        or "pr_create" in text
        or "repo_default_branch" in text
        or "github" in text
        or "no pr number" in text
        or "no longer matches" in text
    ):
        return "tracker_error", detail
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
    if final_kind == "exit" and returncode is not None and (
        not reason or reason == "runner ended with exit"
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
