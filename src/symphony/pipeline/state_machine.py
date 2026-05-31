"""Pure transition decisions for the pipeline state machine.

Given the current stage and the terminal runner event, return the next
run status, the Linear state to move the issue to (if any), and whether
the pipeline halts here.

The transition decision only classifies the runner result. The orchestrator
owns side effects such as moving Linear between workflow states, opening PRs,
and starting Review/Merge monitor rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

RunStatus = Literal["running", "completed", "failed"]


@dataclass(frozen=True)
class Transition:
    next_run_status: RunStatus
    next_linear_state: str | None
    halt: bool


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
    cap_breached: bool = False,
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

    if (
        cap_breached
        or final_kind == "cost_cap"
        or "cost cap" in text
        or "cost_cap" in text
    ):
        return "cost_cap", detail
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
    if final_kind == "cost_cap":
        return "cost cap reached"
    if final_kind:
        return f"runner ended with {final_kind}"
    return status
