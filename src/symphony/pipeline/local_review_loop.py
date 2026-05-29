"""Pure orchestration of the local-review iteration loop.

`poll.py` owns subprocesses, git, GitHub, and Linear. This module owns
the *policy* of the local-review loop: how many rounds, how to dedup
identical verdicts, when to escalate. Keeping the policy here behind an
async callback contract makes it unit-testable without a runner, a
workspace, or a fake `gh`.

An optional `skip_event` callback gives the orchestrator a non-blocking
escape hatch: when the operator posts `$skip-local-review` on the
Linear issue, the slash handler sets the event, and the loop exits
with `SKIPPED` at the next iteration boundary. The in-flight subprocess
is allowed to finish naturally (bounded by `stall_secs`); mid-
subprocess kill is a separate concern owned by the orchestrator.

Contract
--------
Callers inject two async callbacks:

- `reviewer(prompt) -> ReviewerOutput` — runs the reviewer agent in the
  workspace and returns its stdout, an optional `last_message_file`
  payload, and the HEAD SHA the reviewer saw. The head SHA must reflect
  the commit being reviewed (so signatures change after each fix-run).
- `fixer(findings) -> FixerOutput` — runs a fix-run that produces a new
  commit addressing `findings`. Returns whether the fix-run succeeded.

The loop:

  for i in range(cap):
      out = await reviewer(prompt)
      if reviewer failed              → retry once, then reviewer_failed
      verdict = parse(out)
      if UNPARSEABLE                  → retry once, then reviewer_failed
      if APPROVED                     → approved
      if verdict identical to prev    → stuck_loop  (dedup gate)
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
    STUCK_LOOP = "stuck_loop"
    COST_CAP_BREACHED = "cost_cap_breached"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class ReviewerOutput:
    stdout: str
    head_sha: str
    last_message_file: str | None = None
    ok: bool = True
    error: str | None = None
    cost_usd: float = 0.0


@dataclass(frozen=True)
class FixerOutput:
    ok: bool
    error: str | None = None
    cost_usd: float = 0.0


@dataclass(frozen=True)
class LoopResult:
    outcome: LoopOutcome
    iterations: int
    verdicts: tuple[LocalVerdict, ...]
    error: str | None = None
    # Sum of reviewer+fixer subprocess costs across every iteration.
    # Callers feed this into the issue's cumulative cost so the local
    # loop participates in the same cap-breach logic as Implement.
    total_cost_usd: float = 0.0

    @property
    def last_verdict(self) -> LocalVerdict | None:
        return self.verdicts[-1] if self.verdicts else None


ReviewerCallable = Callable[[int], Awaitable[ReviewerOutput]]
FixerCallable = Callable[[int, LocalVerdict], Awaitable[FixerOutput]]


REVIEWER_FAILURE_RETRIES = 1
SkipPredicate = Callable[[], bool]

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
    cost_cap_usd: float = 0.0,
    prior_cost_usd: float = 0.0,
    should_skip: SkipPredicate | None = None,
    on_iteration: IterationCallback | None = None,
) -> LoopResult:
    """Drive the review/fix iteration until approved, capped, or stuck.

    `reviewer(iteration)` and `fixer(iteration, verdict)` receive the
    0-based iteration index so callers can log telemetry. The verdict is
    forwarded so the fixer can use `verdict.findings` as the trigger
    text for `review_comment_fix_prompt`.

    `cap` must be at least 1 — a zero or negative cap returns `EXHAUSTED`
    immediately with no work, which is almost certainly a configuration
    bug worth surfacing rather than silently approving.

    `cost_cap_usd` (when > 0) bounds the cumulative cost of the local-
    review session in *combination with* `prior_cost_usd`, which is the
    issue's cost so far (Implement + any earlier work). The loop checks
    after each subprocess call; the first one that pushes
    `prior + this_session_total >= cap` returns `COST_CAP_BREACHED`. A
    `cost_cap_usd` of `0` (or unset) means uncapped — same convention
    as `cost_guard.evaluate_cost`.
    """
    if cap < 1:
        return LoopResult(
            outcome=LoopOutcome.EXHAUSTED,
            iterations=0,
            verdicts=(),
            error="cap must be >= 1",
        )

    verdicts: list[LocalVerdict] = []
    prev_signature = ""
    total_cost = 0.0

    def _cap_breached() -> bool:
        return cost_cap_usd > 0 and (prior_cost_usd + total_cost) >= cost_cap_usd

    def _skip() -> bool:
        return should_skip is not None and should_skip()

    def _skipped_result(iterations: int) -> LoopResult:
        return LoopResult(
            outcome=LoopOutcome.SKIPPED,
            iterations=iterations,
            verdicts=tuple(verdicts),
            error="operator requested $skip-local-review",
            total_cost_usd=total_cost,
        )

    def _cost_cap_breached_result(iterations: int, *, phase: str) -> LoopResult:
        return LoopResult(
            outcome=LoopOutcome.COST_CAP_BREACHED,
            iterations=iterations,
            verdicts=tuple(verdicts),
            error=(
                f"cost cap ${cost_cap_usd:.2f} reached {phase} "
                f"(prior=${prior_cost_usd:.4f}, session=${total_cost:.4f})"
            ),
            total_cost_usd=total_cost,
        )

    for i in range(cap):
        # Skip-event check at iteration boundaries. An operator who posts
        # `$skip-local-review` mid-loop will see at most one extra
        # subprocess complete before the loop exits — bounded by
        # `stall_secs`, not by `cap × stall_secs`.
        if _skip():
            return _skipped_result(i)
        verdict: LocalVerdict | None = None
        reviewer_error: str | None = None
        for attempt in range(REVIEWER_FAILURE_RETRIES + 1):
            out = await reviewer(i)
            total_cost += out.cost_usd
            # If the orchestrator killed the reviewer mid-subprocess via
            # `$skip-local-review`, the runner will emit a failure terminal.
            # Prefer SKIPPED over REVIEWER_FAILED so the audit trail reflects
            # operator intent, not "the reviewer crashed".
            if _skip():
                return _skipped_result(i + 1)
            if not out.ok:
                reviewer_error = out.error or "reviewer failed"
                if _cap_breached():
                    return _cost_cap_breached_result(
                        i + 1, phase="during local review"
                    )
                if attempt < REVIEWER_FAILURE_RETRIES:
                    continue
                return LoopResult(
                    outcome=LoopOutcome.REVIEWER_FAILED,
                    iterations=i + 1,
                    verdicts=tuple(verdicts),
                    error=reviewer_error,
                    total_cost_usd=total_cost,
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
                and not _cap_breached()
            ):
                continue
            verdict = parsed
            break

        if verdict is None:
            return LoopResult(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=i + 1,
                verdicts=tuple(verdicts),
                error=reviewer_error or "reviewer failed",
                total_cost_usd=total_cost,
            )
        verdicts.append(verdict)

        # Heartbeat: fire the callback once per iteration so the
        # orchestrator can post a Linear comment ("iteration N:
        # changes_requested"). Done after the parse so the callback sees
        # the verdict, but before the cap/skip/fix checks so the signal
        # reaches the operator even when the loop is about to exit.
        if on_iteration is not None:
            try:
                await on_iteration(i, verdict, total_cost)
            except Exception:  # noqa: BLE001
                # The loop must not die because of a heartbeat side
                # effect (Linear flake, etc.). Swallow + continue.
                pass

        # Cap check sits *between* reviewer parse and fixer dispatch: if
        # the reviewer alone tipped us over, surface the breach without
        # also paying for a fix-run. Doing it after the verdict parse
        # lets callers see *what* the reviewer found before we aborted.
        if _cap_breached():
            return _cost_cap_breached_result(
                i + 1, phase="during local review"
            )

        if verdict.kind == LocalVerdictKind.APPROVED:
            return LoopResult(
                outcome=LoopOutcome.APPROVED,
                iterations=i + 1,
                verdicts=tuple(verdicts),
                total_cost_usd=total_cost,
            )
        if verdict.kind == LocalVerdictKind.UNPARSEABLE:
            return LoopResult(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=i + 1,
                verdicts=tuple(verdicts),
                error="reviewer emitted no verdict marker",
                total_cost_usd=total_cost,
            )

        # CHANGES_REQUESTED — gate on the dedup signature before paying
        # for another fix-run. Same trigger twice in a row is the
        # stuck-loop pattern the broader pipeline already avoids in the
        # remote case (see review_classifier.should_dispatch_fix_run).
        if verdict.trigger_signature == prev_signature:
            return LoopResult(
                outcome=LoopOutcome.STUCK_LOOP,
                iterations=i + 1,
                verdicts=tuple(verdicts),
                error="reviewer produced the same trigger twice in a row",
                total_cost_usd=total_cost,
            )
        prev_signature = verdict.trigger_signature

        fix = await fixer(i, verdict)
        total_cost += fix.cost_usd
        # Same priority as the reviewer side: an operator-killed fix-run
        # surfaces as SKIPPED, not FIX_RUN_FAILED.
        if _skip():
            return _skipped_result(i + 1)
        if not fix.ok:
            return LoopResult(
                outcome=LoopOutcome.FIX_RUN_FAILED,
                iterations=i + 1,
                verdicts=tuple(verdicts),
                error=fix.error or "fix-run failed",
                total_cost_usd=total_cost,
            )
        if _cap_breached():
            return _cost_cap_breached_result(i + 1, phase="after fix-run")

    return LoopResult(
        outcome=LoopOutcome.EXHAUSTED,
        iterations=cap,
        verdicts=tuple(verdicts),
        total_cost_usd=total_cost,
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
