"""Pure orchestration of the local-review iteration loop.

`poll.py` owns subprocesses, git, GitHub, and Linear. This module owns
the *policy* of the local-review loop: how many rounds, how to dedup
identical verdicts, when to escalate. Keeping the policy here behind an
async callback contract makes it unit-testable without a runner, a
workspace, or a fake `gh`.

Contract
--------
Callers inject two async callbacks:

- `reviewer(prompt) -> ReviewerOutput` — runs the reviewer agent in the
  workspace and returns its stdout, an optional `last_message_file`
  payload, and the HEAD SHA the reviewer saw.
- `fixer(findings) -> FixerOutput` — runs a fix-run that produces a new
  commit addressing `findings`. Returns whether the fix-run succeeded.

The loop:

  for i in range(cap):
      out = await reviewer(prompt)
      if reviewer failed              → retry once, then reviewer_failed
      verdict = parse(out)
      if UNPARSEABLE                  → retry once, then reviewer_failed
      if APPROVED                     → approved
      if findings identical to prev   → stuck_loop  (dedup gate)
      fix_ok = await fixer(findings)
      if not fix_ok                   → fix_run_failed
  → exhausted (cap hit)

`approved` / `exhausted` / `stuck_loop` are operationally meaningful for
the caller: approved → push and merge; exhausted → push but escalate to
Needs Approval so an operator can intervene; stuck_loop → same as
exhausted but with a clearer telemetry signal.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

from .local_review import (
    LocalVerdict,
    LocalVerdictKind,
    ReviewerAgent,
    parse_local_review_output,
)


class LoopOutcome(StrEnum):
    APPROVED = "approved"
    EXHAUSTED = "exhausted"
    REVIEWER_FAILED = "reviewer_failed"
    FIX_RUN_FAILED = "fix_run_failed"
    FIX_RUN_BLOCKED = "fix_run_blocked"
    STUCK_LOOP = "stuck_loop"


@dataclass(frozen=True)
class ReviewerOutput:
    stdout: str
    head_sha: str
    last_message_file: str | None = None
    ok: bool = True
    error: str | None = None
    # A human-readable error pulled from a `turn.failed`/`error` event in the
    # reviewer's stream (e.g. an API 4xx). The reviewer process can exit 0 with
    # only such an event and no verdict; surfacing this lets the loop report the
    # real cause instead of a generic "no verdict marker".
    agent_error: str | None = None
    cost_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0


@dataclass(frozen=True)
class FixerOutput:
    ok: bool
    error: str | None = None
    # SYM-107: a fix-run that exits 0 but politely stalls on a human action
    # (SYM-101 `SYMPHONY_BLOCKED` contract) sets `blocked` so the loop halts
    # and routes to the operator-wait path instead of re-reviewing / pushing.
    # `blocked` is independent of `ok`: a blocked run still exited 0.
    blocked: bool = False
    blocked_reason: str = ""
    cost_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0


@dataclass(frozen=True)
class LoopResult:
    outcome: LoopOutcome
    iterations: int
    verdicts: tuple[LocalVerdict, ...]
    error: str | None = None
    # Sum of reviewer+fixer subprocess costs across every iteration.
    # Recorded on the issue's `runs.cost_usd` for the audit trail; it no
    # longer gates the loop.
    total_cost_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0

    @property
    def last_verdict(self) -> LocalVerdict | None:
        return self.verdicts[-1] if self.verdicts else None


ReviewerCallable = Callable[[int], Awaitable[ReviewerOutput]]
FixerCallable = Callable[[int, LocalVerdict], Awaitable[FixerOutput]]


REVIEWER_FAILURE_RETRIES = 1

# Fired after each reviewer's verdict is parsed but before any fix-run is
# dispatched. Lets the orchestrator post a heartbeat Linear comment so a
# 5-minute local-review doesn't look dead to a watching operator. The
# callback is async because realistic implementations (post a comment,
# log a metric) want to await; sync callbacks can simply not await.
IterationCallback = Callable[[int, LocalVerdict, float], Awaitable[None]]


async def run_local_review_loop(
    *,
    reviewer_agent: ReviewerAgent,
    reviewer: ReviewerCallable,
    fixer: FixerCallable,
    cap: int,
    on_iteration: IterationCallback | None = None,
) -> LoopResult:
    """Drive the review/fix iteration until approved, exhausted, or stuck.

    `reviewer(iteration)` and `fixer(iteration, verdict)` receive the
    0-based iteration index so callers can log telemetry. The verdict is
    forwarded so the fixer can use `verdict.findings` as the trigger
    text for `review_comment_fix_prompt`.

    `cap` must be at least 1 — a zero or negative cap returns `EXHAUSTED`
    immediately with no work, which is almost certainly a configuration
    bug worth surfacing rather than silently approving.
    """
    if cap < 1:
        return LoopResult(
            outcome=LoopOutcome.EXHAUSTED,
            iterations=0,
            verdicts=(),
            error="cap must be >= 1",
        )

    verdicts: list[LocalVerdict] = []
    prev_findings_signature = ""
    total_cost = 0.0
    total_input_tokens = 0
    total_output_tokens = 0
    total_cache_write_tokens = 0
    total_cache_read_tokens = 0

    def _record_usage(out: ReviewerOutput | FixerOutput) -> None:
        nonlocal total_cost
        nonlocal total_input_tokens
        nonlocal total_output_tokens
        nonlocal total_cache_write_tokens
        nonlocal total_cache_read_tokens
        total_cost += out.cost_usd
        total_input_tokens += out.input_tokens
        total_output_tokens += out.output_tokens
        total_cache_write_tokens += out.cache_write_tokens
        total_cache_read_tokens += out.cache_read_tokens

    def _result(
        *,
        outcome: LoopOutcome,
        iterations: int,
        error: str | None = None,
    ) -> LoopResult:
        return LoopResult(
            outcome=outcome,
            iterations=iterations,
            verdicts=tuple(verdicts),
            error=error,
            total_cost_usd=total_cost,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            cache_write_tokens=total_cache_write_tokens,
            cache_read_tokens=total_cache_read_tokens,
        )

    for i in range(cap):
        verdict: LocalVerdict | None = None
        reviewer_error: str | None = None
        for attempt in range(REVIEWER_FAILURE_RETRIES + 1):
            out = await reviewer(i)
            _record_usage(out)
            if not out.ok:
                reviewer_error = out.error or "reviewer failed"
                if attempt < REVIEWER_FAILURE_RETRIES:
                    continue
                return _result(
                    outcome=LoopOutcome.REVIEWER_FAILED,
                    iterations=i + 1,
                    error=reviewer_error,
                )
            parsed = parse_local_review_output(
                agent=reviewer_agent,
                stdout=out.stdout,
                head_sha=out.head_sha,
                last_message_file=out.last_message_file,
            )
            if (
                parsed.kind == LocalVerdictKind.UNPARSEABLE
                and attempt < REVIEWER_FAILURE_RETRIES
            ):
                continue
            verdict = parsed
            break

        if verdict is None:
            return _result(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=i + 1,
                error=reviewer_error or out.agent_error or "reviewer failed",
            )
        verdicts.append(verdict)

        # Heartbeat: fire the callback once per iteration so the
        # orchestrator can post a Linear comment ("iteration N:
        # changes_requested"). Done after the parse so the callback sees
        # the verdict, but before the fix check so the signal reaches the
        # operator even when the loop is about to exit.
        if on_iteration is not None:
            try:
                await on_iteration(i, verdict, total_cost)
            except Exception:  # noqa: BLE001
                # The loop must not die because of a heartbeat side
                # effect (Linear flake, etc.). Swallow + continue.
                pass

        if verdict.kind == LocalVerdictKind.APPROVED:
            return _result(
                outcome=LoopOutcome.APPROVED,
                iterations=i + 1,
            )
        if verdict.kind == LocalVerdictKind.UNPARSEABLE:
            return _result(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=i + 1,
                error=out.agent_error or "reviewer emitted no verdict marker",
            )

        # CHANGES_REQUESTED — gate on the merged-findings digest before paying
        # for another fix-run. Same unresolved findings twice in a row is the
        # local non-convergence signal even when the fix-run advanced HEAD.
        findings_signature = (
            verdict.findings_signature or verdict.trigger_signature
        )
        if findings_signature == prev_findings_signature:
            return _result(
                outcome=LoopOutcome.STUCK_LOOP,
                iterations=i + 1,
                error="reviewer produced the same findings twice in a row",
            )
        prev_findings_signature = findings_signature

        fix = await fixer(i, verdict)
        _record_usage(fix)
        # A blocked fix-run halts the loop before the next review pass: the
        # branch is waiting on a human action, so re-reviewing or pushing is
        # pointless. Checked before `ok` because a blocked run exited 0.
        if fix.blocked:
            return _result(
                outcome=LoopOutcome.FIX_RUN_BLOCKED,
                iterations=i + 1,
                error=fix.blocked_reason or "fix-run blocked on a human action",
            )
        if not fix.ok:
            return _result(
                outcome=LoopOutcome.FIX_RUN_FAILED,
                iterations=i + 1,
                error=fix.error or "fix-run failed",
            )

    return _result(
        outcome=LoopOutcome.EXHAUSTED,
        iterations=cap,
    )


__all__ = [
    "FixerCallable",
    "FixerOutput",
    "LoopOutcome",
    "LoopResult",
    "ReviewerCallable",
    "ReviewerOutput",
    "run_local_review_loop",
]
