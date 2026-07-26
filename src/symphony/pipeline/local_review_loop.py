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


def _dedupe_api_errors(
    errors: tuple[tuple[StreamApiError, str], ...],
    *,
    primary: StreamApiError | None,
    primary_agent: str | None,
) -> tuple[tuple[StreamApiError, str], ...]:
    """Drop repeats and anything the primary slot already names, so a provider
    is never flagged twice for the same failure."""
    seen: set[str] = set()
    out: list[tuple[StreamApiError, str]] = []
    for error, agent in errors:
        # Keyed by PROVIDER: each provider has one connection needing one
        # re-validate. Flagging it twice would trigger a second refresh, which
        # is exactly the rotation the daemon serializes to avoid (SYM-218 review).
        if primary is not None and (error is primary or agent == primary_agent):
            continue
        if agent in seen:
            continue
        seen.add(agent)
        out.append((error, agent))
    return tuple(out)


def _providers_named(
    primary_agent: str | None, extras: tuple[tuple[StreamApiError, str], ...]
) -> set[str]:
    """Every provider an output speaks for — those it did NOT name were not
    exercised by it, so its silence says nothing about them."""
    named = {agent for _, agent in extras}
    if primary_agent:
        named.add(primary_agent)
    return named


def _prefer_auth_error(
    first: tuple[StreamApiError | None, str | None],
    second: tuple[StreamApiError | None, str | None],
) -> tuple[StreamApiError | None, str | None]:
    """`first` wins unless only `second` is an auth failure: a credential that
    needs re-validating outranks a plain API error even when both belong to the
    same provider (SYM-218 review)."""
    first_error, _ = first
    second_error, _ = second
    if (
        second_error is not None
        and is_auth_api_error(second_error)
        and (first_error is None or not is_auth_api_error(first_error))
    ):
        return second
    return first if first_error is not None else second


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
    # An auth failure that must outlive the iteration that saw it: a fixer that
    # commits and *then* fails on auth is not an iteration-local event — the
    # next reviewer may well approve, and the result would otherwise carry no
    # error at all, leaving that credential un-revalidated (SYM-218 review).
    pending_api_error: StreamApiError | None = None
    pending_api_error_agent: str | None = None
    pending_extra_api_errors: tuple[tuple[StreamApiError, str], ...] = ()
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
        api_error: StreamApiError | None = None,
        api_error_agent: str | None = None,
        extra_api_errors: tuple[tuple[StreamApiError, str], ...] = (),
    ) -> LoopResult:
        # Anything carried from an earlier iteration still needs acting on, so
        # it fills the slot when this outcome has no error of its own.
        # Falls back to the current iteration's reviewer extras (function-scoped,
        # assigned at the top of each iteration) when the caller passes none.
        extras = extra_api_errors or stream_extra_api_errors
        if api_error is None and pending_api_error is not None:
            api_error = pending_api_error
            api_error_agent = pending_api_error_agent
            extras = (*extras, *pending_extra_api_errors)
        elif pending_api_error is not None:
            # A later error took the slot. The earlier primary rides along when
            # it names a DIFFERENT connection; its extras ride along regardless,
            # since they name providers of their own and the repeated-primary
            # case must not swallow them (SYM-218 review).
            if (
                pending_api_error is not api_error
                and pending_api_error_agent
                and pending_api_error_agent != api_error_agent
            ):
                extras = (*extras, (pending_api_error, pending_api_error_agent))
            extras = (*extras, *pending_extra_api_errors)
        extras = _dedupe_api_errors(extras, primary=api_error, primary_agent=api_error_agent)
        return LoopResult(
            outcome=outcome,
            iterations=iterations,
            verdicts=tuple(verdicts),
            error=error,
            api_error=api_error,
            api_error_agent=api_error_agent,
            # Only meaningful alongside a primary error; a result with no
            # api_error has nothing extra to act on either.
            extra_api_errors=(extras if api_error is not None else ()),
            total_cost_usd=total_cost,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            cache_write_tokens=total_cache_write_tokens,
            cache_read_tokens=total_cache_read_tokens,
        )

    for i in range(cap):
        verdict: LocalVerdict | None = None
        reviewer_error: str | None = None
        # The real stream error (an API/config failure) can surface on one
        # attempt but not a later one; retain the last non-empty one so an
        # unparseable final attempt still reports the real cause rather than
        # the generic marker message.
        stream_error: str | None = None
        stream_api_error: StreamApiError | None = None
        # Which provider owns `stream_api_error`. The output attributes it
        # (verifier pass may differ from the finder); fall back to the session's
        # reviewer agent when it doesn't (single-pass / common case).
        stream_api_error_agent: str | None = None
        # Secondary (error, agent) pairs from the same output — see
        # ReviewerOutput.extra_api_errors.
        stream_extra_api_errors: tuple[tuple[StreamApiError, str], ...] = ()
        for attempt in range(REVIEWER_FAILURE_RETRIES + 1):
            out = await reviewer(i)
            _record_usage(out)
            if out.agent_error:
                stream_error = out.agent_error
            if out.api_error is not None:
                stream_api_error = out.api_error
                stream_api_error_agent = out.api_error_agent or str(reviewer_agent)
                stream_extra_api_errors = out.extra_api_errors
            if not out.ok:
                reviewer_error = out.error or "reviewer failed"
                if attempt < REVIEWER_FAILURE_RETRIES:
                    continue
                return _result(
                    outcome=LoopOutcome.REVIEWER_FAILED,
                    iterations=i + 1,
                    error=reviewer_error,
                    api_error=stream_api_error,
                    api_error_agent=(stream_api_error_agent if stream_api_error else None),
                )
            # This attempt recovered, so only ITS errors are still live: a
            # previous attempt's 401 must not ride along on a successful retry,
            # or the lifecycle expires a connection that just proved healthy.
            # (An error the same output carries — e.g. a finder 401 merged with
            # a clean verifier — is preserved, since it comes from `out`.)
            parsed = parse_local_review_output(
                agent=reviewer_agent,
                stdout=out.stdout,
                head_sha=out.head_sha,
                last_message_file=out.last_message_file,
            )
            if parsed.kind == LocalVerdictKind.UNPARSEABLE and attempt < REVIEWER_FAILURE_RETRIES:
                continue
            # Only a parseable verdict means this attempt truly recovered, so
            # only then do earlier attempts' errors stop being live. The FINAL
            # attempt can also be unparseable (no `continue` left to take), and
            # clearing there would drop the first attempt's typed transient/auth
            # signal from the REVIEWER_FAILED result (SYM-218 review).
            if parsed.kind != LocalVerdictKind.UNPARSEABLE:
                stream_api_error = out.api_error
                stream_api_error_agent = (
                    (out.api_error_agent or str(reviewer_agent))
                    if out.api_error is not None
                    else None
                )
                # Providers this attempt never spoke for keep their earlier
                # failures: a retry that fails in the finder says nothing about
                # a verifier that never ran (SYM-218 review).
                touched = _providers_named(
                    out.api_error_agent or str(reviewer_agent) if out.api_error else None,
                    out.extra_api_errors,
                )
                untouched = tuple(
                    (error, agent)
                    for error, agent in (
                        *stream_extra_api_errors,
                        *(
                            ((stream_api_error, stream_api_error_agent),)
                            if stream_api_error is not None and stream_api_error_agent
                            else ()
                        ),
                    )
                    if agent not in touched
                )
                stream_extra_api_errors = (
                    (*out.extra_api_errors, *untouched) if out.api_error is not None else untouched
                )
            elif out.api_error is not None:
                # An unparseable final attempt with its own error still reports
                # that error rather than the earlier attempt's.
                stream_api_error = out.api_error
                stream_api_error_agent = out.api_error_agent or str(reviewer_agent)
                stream_extra_api_errors = out.extra_api_errors
            if out.healthy_agents:
                healthy = set(out.healthy_agents)
                if stream_api_error_agent in healthy:
                    stream_api_error = None
                    stream_api_error_agent = None
                stream_extra_api_errors = tuple(
                    (e, a) for e, a in stream_extra_api_errors if a not in healthy
                )
                if pending_api_error_agent in healthy:
                    pending_api_error = None
                    pending_api_error_agent = None
                pending_extra_api_errors = tuple(
                    (e, a) for e, a in pending_extra_api_errors if a not in healthy
                )
            verdict = parsed
            break

        if verdict is None:
            return _result(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=i + 1,
                error=reviewer_error or stream_error or "reviewer failed",
                api_error=stream_api_error,
                api_error_agent=(stream_api_error_agent if stream_api_error else None),
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
                api_error=stream_api_error,
                api_error_agent=(stream_api_error_agent if stream_api_error else None),
            )
        if verdict.kind == LocalVerdictKind.UNPARSEABLE:
            return _result(
                outcome=LoopOutcome.REVIEWER_FAILED,
                iterations=i + 1,
                error=stream_error or "reviewer emitted no verdict marker",
                api_error=stream_api_error,
                api_error_agent=(stream_api_error_agent if stream_api_error else None),
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
                api_error=stream_api_error,
                api_error_agent=(stream_api_error_agent if stream_api_error else None),
            )
        prev_findings_signature = findings_signature

        # The reviewer's auth failure must outlive this iteration: a successful
        # fixer followed by a clean approval next round would otherwise drop it,
        # and so would a blocked fixer returning immediately (SYM-218 review).
        if stream_api_error is not None:
            pending_api_error = stream_api_error
            pending_api_error_agent = stream_api_error_agent
            pending_extra_api_errors = stream_extra_api_errors

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
            _fix_failure_error, _fix_failure_agent = _prefer_auth_error(
                (fix.api_error, fixer_agent if fix.api_error else None),
                (stream_api_error, stream_api_error_agent if stream_api_error else None),
            )
            return _result(
                outcome=LoopOutcome.FIX_RUN_FAILED,
                iterations=i + 1,
                error=fix.error or "fix-run failed",
                # A fixer that failed without its own provider error must not
                # erase the reviewer's surviving auth failure — a LoopResult
                # exists, so the lifecycle's shared-log fallback won't run and
                # that connection would stay connected (SYM-218 review).
                api_error=_fix_failure_error,
                api_error_agent=_fix_failure_agent,
            )
        if fix.api_error is not None:
            # A fixer that committed and *then* failed on auth is still an auth
            # failure the daemon must act on: carry it so the eventual result
            # (APPROVED, EXHAUSTED, ...) surfaces the provider (SYM-218 review).
            # A pending error from another provider is demoted to an extra
            # rather than overwritten — both connections need action.
            if (
                pending_api_error is not None
                and pending_api_error_agent
                and pending_api_error_agent != fixer_agent
            ):
                pending_extra_api_errors = (
                    *pending_extra_api_errors,
                    (pending_api_error, pending_api_error_agent),
                )
            pending_api_error = fix.api_error
            pending_api_error_agent = fixer_agent

    return _result(
        outcome=LoopOutcome.EXHAUSTED,
        iterations=cap,
        # `stream_api_error`/`stream_api_error_agent` from the final
        # iteration's reviewer call are still bound here (for-loop bodies
        # don't scope in Python) — surface a lingering auth failure even
        # when the cap was hit rather than the reviewer failing outright.
        api_error=stream_api_error,
        api_error_agent=(stream_api_error_agent if stream_api_error else None),
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
