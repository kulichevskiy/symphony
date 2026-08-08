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

The loop reviews, fixes, and then always verifies the final permitted fix.
`cap` limits change-driving fixer turns; the closure review is read-only and
does not consume another fix allowance. Earlier confirmed findings are carried
into later fixer prompts as regression obligations.

`approved` / `exhausted` / `stuck_loop` are operationally meaningful for
the caller: approved → push and merge; exhausted → push but escalate to
Needs Approval so an operator can intervene; stuck_loop → same as
exhausted but with a clearer telemetry signal.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from enum import StrEnum

from .local_review import (
    LocalVerdict,
    LocalVerdictKind,
    ReviewerAgent,
    StreamApiError,
    is_auth_api_error,
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
    # The same error as a typed signal when it is a provider API error
    # (`api_error.transient` distinguishes a retryable 5xx/429 from a
    # deterministic 4xx). `agent_error` carries its message for operators;
    # `api_error` is the gate downstream retry logic reads.
    api_error: StreamApiError | None = None
    # Which agent produced `api_error` ("claude"/"codex"). A two-pass review
    # can run the verifier on a different provider than the finder, so the
    # session tags the pass that actually failed here; the loop threads it into
    # `LoopResult.api_error_agent` so an expiry hits the failing provider rather
    # than always the session's `reviewer_agent`. None → the caller falls back
    # to the reviewer agent (the single-pass / common case).
    api_error_agent: str | None = None
    # Additional (error, agent) pairs when MORE THAN ONE provider failed in the
    # same output — a two-pass review where the finder and the verifier each hit
    # their own auth failure. The single `api_error` slot can only name one, but
    # every failing connection needs its own re-validate/expire, so the rest ride
    # here (SYM-218 review).
    extra_api_errors: tuple[tuple[StreamApiError, str], ...] = ()
    # Providers that completed a pass in THIS output with no error of their own
    # — a later clean pass proves that credential works, so any earlier failure
    # held against it is stale and must not expire a healthy connection
    # (SYM-218 review).
    healthy_agents: tuple[str, ...] = ()
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
    api_error: StreamApiError | None = None
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
    # The typed API error from the last reviewer turn that produced no verdict,
    # when one was present in the stream. Callers gate retry logic on
    # `api_error.transient`; `error` carries the human-readable message.
    api_error: StreamApiError | None = None
    # Which agent produced `api_error` ("claude"/"codex"), so a caller flags
    # only the failing provider in a mixed reviewer/fixer config (Config v2 6/9).
    api_error_agent: str | None = None
    # Further (error, agent) pairs when more than one provider failed — the
    # caller must act on each, not just `api_error_agent` (SYM-218 review).
    extra_api_errors: tuple[tuple[StreamApiError, str], ...] = ()
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


class _AuthErrorLedger:
    """The unresolved provider error per provider — at most one each.

    A provider has exactly one connection, so it has at most one outstanding
    failure. Recording is keyed by provider (a later failure for the same
    provider replaces its earlier one; a different provider's never overwrites
    it), and a pass that proves a provider healthy removes its entry. Those two
    operations are the only way the set changes, which makes "an error was
    silently dropped, duplicated, or outlived its own fix" unrepresentable
    rather than something every assignment site has to remember (SYM-218).
    """

    def __init__(self) -> None:
        self._errors: dict[str, StreamApiError] = {}

    def record(self, error: StreamApiError | None, agent: str | None) -> None:
        if error is not None and agent:
            self._errors[str(agent)] = error

    def record_many(self, pairs: tuple[tuple[StreamApiError, str], ...]) -> None:
        for error, agent in pairs:
            self.record(error, agent)

    def clear(self, *agents: str) -> None:
        for agent in agents:
            self._errors.pop(str(agent), None)

    def resolve(
        self, prefer_agent: str | None = None
    ) -> tuple[StreamApiError | None, str | None, tuple[tuple[StreamApiError, str], ...]]:
        """The primary error (the caller's provider first, then any auth
        failure, since that is what needs a re-validate) plus the rest."""
        if not self._errors:
            return None, None, ()
        ordered = sorted(
            self._errors.items(),
            key=lambda item: (item[0] != prefer_agent, not is_auth_api_error(item[1])),
        )
        (primary_agent, primary_error), *rest = ordered
        return primary_error, primary_agent, tuple((error, agent) for agent, error in rest)


async def run_local_review_loop(
    *,
    reviewer_agent: ReviewerAgent,
    # Which agent runs the fixer turns — tags `api_error_agent` on a fix-run
    # failure so callers flag only the failing provider. Defaults to the
    # reviewer's agent for the common single-agent config.
    fixer_agent: str = "",
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

    `cap` is the maximum number of fixer turns. One closure review runs after
    the final permitted fix. A zero or negative cap returns `EXHAUSTED`
    immediately with no work, which is almost certainly a configuration bug
    worth surfacing rather than silently approving.
    """
    if cap < 1:
        return LoopResult(
            outcome=LoopOutcome.EXHAUSTED,
            iterations=0,
            verdicts=(),
            error="cap must be >= 1",
        )

    verdicts: list[LocalVerdict] = []
    # Outlives every iteration: a failure is only resolved by proof that its
    # provider works, not by the loop moving on.
    ledger = _AuthErrorLedger()
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
        prefer_agent: str | None = None,
    ) -> LoopResult:
        api_error, api_error_agent, extras = ledger.resolve(prefer_agent)
        return LoopResult(
            outcome=outcome,
            iterations=iterations,
            verdicts=tuple(verdicts),
            error=error,
            api_error=api_error,
            api_error_agent=api_error_agent,
            extra_api_errors=extras,
            total_cost_usd=total_cost,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            cache_write_tokens=total_cache_write_tokens,
            cache_read_tokens=total_cache_read_tokens,
        )

    i = 0
    fixes_used = 0
    prior_findings: list[str] = []
    while True:
        verdict: LocalVerdict | None = None
        reviewer_error: str | None = None
        # The real stream error (an API/config failure) can surface on one
        # attempt but not a later one; retain the last non-empty one so an
        # unparseable final attempt still reports the real cause rather than
        # the generic marker message.
        stream_error: str | None = None
        last_failed_agent: str | None = None
        reviewer_retries_used = 0
        severity_retries_used = 0
        while True:
            out = await reviewer(i)
            _record_usage(out)
            if out.agent_error:
                stream_error = out.agent_error
            # Record first, then clear: an output never reports a provider as
            # both failed and healthy, so order only matters for readability.
            if out.api_error is not None:
                last_failed_agent = out.api_error_agent or str(reviewer_agent)
                ledger.record(out.api_error, last_failed_agent)
            ledger.record_many(out.extra_api_errors)
            ledger.clear(*out.healthy_agents)
            if not out.ok:
                reviewer_error = out.error or "reviewer failed"
                if reviewer_retries_used < REVIEWER_FAILURE_RETRIES:
                    reviewer_retries_used += 1
                    continue
                return _result(
                    outcome=LoopOutcome.REVIEWER_FAILED,
                    iterations=i + 1,
                    error=reviewer_error,
                    prefer_agent=last_failed_agent,
                )
            parsed = parse_local_review_output(
                agent=reviewer_agent,
                stdout=out.stdout,
                head_sha=out.head_sha,
                last_message_file=out.last_message_file,
            )
            if parsed.kind == LocalVerdictKind.UNPARSEABLE:
                if reviewer_retries_used < REVIEWER_FAILURE_RETRIES:
                    reviewer_retries_used += 1
                    continue
            elif (
                parsed.severity_inferred
                and severity_retries_used < REVIEWER_FAILURE_RETRIES
            ):
                severity_retries_used += 1
                continue
            verdict = parsed
            break

        if verdict is None:
            return _result(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=i + 1,
                error=reviewer_error or stream_error or "reviewer failed",
                prefer_agent=last_failed_agent,
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
            # A verifier that approves cleanly can still follow a finder
            # that hit a typed auth failure (401) but still emitted usable
            # findings — surface (and expire) that provider rather than
            # dropping it just because the loop is otherwise happy.
            return _result(
                outcome=LoopOutcome.APPROVED,
                iterations=i + 1,
                prefer_agent=last_failed_agent,
            )
        if verdict.kind == LocalVerdictKind.UNPARSEABLE:
            return _result(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=i + 1,
                error=stream_error or "reviewer emitted no verdict marker",
                prefer_agent=last_failed_agent,
            )

        # CHANGES_REQUESTED — gate on the merged-findings digest before paying
        # for another fix-run. Same unresolved findings twice in a row is the
        # local non-convergence signal even when the fix-run advanced HEAD.
        findings_signature = verdict.findings_signature or verdict.trigger_signature
        if findings_signature == prev_findings_signature:
            return _result(
                outcome=LoopOutcome.STUCK_LOOP,
                iterations=i + 1,
                error="reviewer produced the same findings twice in a row",
                prefer_agent=last_failed_agent,
            )
        prev_findings_signature = findings_signature

        # The cap limits change-driving fix turns. After the final permitted
        # fix we still run one read-only review; only an unresolved verdict on
        # that closure review is EXHAUSTED.
        if fixes_used >= cap:
            return _result(
                outcome=LoopOutcome.EXHAUSTED,
                iterations=i + 1,
                prefer_agent=last_failed_agent,
            )

        # Preserve earlier confirmed findings as regression obligations. A
        # fixer that sees only the newest item can solve it by reintroducing a
        # defect fixed one round earlier (the production BENCH-59 failure).
        current_findings = verdict.findings.strip()
        if prior_findings:
            fixer_findings = (
                "# Current findings\n\n"
                f"{current_findings}\n\n"
                "# Regression obligations from earlier review rounds\n\n"
                + "\n\n".join(prior_findings)
            )
            fixer_verdict = replace(verdict, findings=fixer_findings)
        else:
            fixer_verdict = verdict

        fix = await fixer(i, fixer_verdict)
        _record_usage(fix)
        # Whatever the fixer reports goes on the ledger; a clean run clears its
        # provider, since completing proves that credential works.
        ledger.record(fix.api_error, fixer_agent)
        if fix.api_error is None and fix.ok:
            ledger.clear(fixer_agent)
        # A blocked fix-run halts the loop before the next review pass: the
        # branch is waiting on a human action, so re-reviewing or pushing is
        # pointless. Checked before `ok` because a blocked run exited 0.
        if fix.blocked:
            return _result(
                outcome=LoopOutcome.FIX_RUN_BLOCKED,
                iterations=i + 1,
                error=fix.blocked_reason or "fix-run blocked on a human action",
                prefer_agent=fixer_agent if fix.api_error else last_failed_agent,
            )
        if not fix.ok:
            return _result(
                outcome=LoopOutcome.FIX_RUN_FAILED,
                iterations=i + 1,
                error=fix.error or "fix-run failed",
                prefer_agent=fixer_agent if fix.api_error else last_failed_agent,
            )
        if current_findings:
            prior_findings.append(current_findings)
        fixes_used += 1
        i += 1


__all__ = [
    "FixerCallable",
    "FixerOutput",
    "LoopOutcome",
    "LoopResult",
    "ReviewerCallable",
    "ReviewerOutput",
    "run_local_review_loop",
]
