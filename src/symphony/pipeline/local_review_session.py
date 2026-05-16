"""End-to-end local-review session: build callbacks, run the loop.

This is the single entry point the orchestrator calls once the
Implement stage succeeds and `review_strategy != "remote"`. It wires
together the four building blocks:

  prompt + command (local_review.py)
       ↓
  Runner -> stdout string (local_review_io.py)
       ↓
  verdict parsing (local_review.py)
       ↓
  policy / iteration (local_review_loop.py)

Caller responsibilities outside this module:

- Provide a `Runner` and the absolute `workspace_path` of an
  already-cloned and -checked-out branch.
- Provide a `head_sha_provider` callback so the SHA reads use the
  caller's existing helper (the orchestrator already has
  `_workspace_head_sha`).
- After the session returns:
  - `APPROVED`            push and proceed (skip `@codex` if `local` mode,
                          post `@codex review` once if `hybrid`).
  - `EXHAUSTED|STUCK_LOOP|FIX_RUN_FAILED`
                          push and escalate to `needs_approval`. The
                          branch has the best-effort fix already.
  - `REVIEWER_FAILED`     orchestrator decides: optionally fall back to
                          the remote `@codex review` flow so the issue
                          isn't dead-ended by a reviewer crash.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Literal

from ..agent.codex_models import DEFAULT_CODEX_MODEL
from ..agent.prompt import review_comment_fix_prompt
from ..agent.runner import Runner, RunnerSpec
from .cost_guard import UsageCostEstimator
from .local_review import (
    LocalVerdict,
    ReviewerAgent,
    build_local_review_command,
    local_review_prompt,
)
from .local_review_io import collect_runner_output
from .local_review_loop import (
    FixerOutput,
    IterationCallback,
    LoopResult,
    ReviewerOutput,
    SkipPredicate,
    run_local_review_loop,
)

ImplementerAgent = Literal["claude", "codex"]

HeadShaProvider = Callable[[Path], Awaitable[str]]

# Notified with the active subprocess run_id before each reviewer / fixer
# call, and with `None` once that subprocess returns. The orchestrator
# uses this to track the kill target for `$skip-local-review` — calling
# `runner.kill(run_id)` from the slash handler interrupts a long-running
# reviewer immediately instead of waiting for it to finish naturally.
ActiveRunIdReporter = Callable[[str | None], Awaitable[None]]

# Run IDs must survive becoming git refs / log filenames. The orchestrator
# already uses UUIDs, but if a caller passes something weirder we still
# want clean derived IDs.
_RUN_ID_SAFE_RE = re.compile(r"[^a-zA-Z0-9_.\-]")


def _safe_run_id(parent_run_id: str, suffix: str) -> str:
    base = _RUN_ID_SAFE_RE.sub("-", parent_run_id) or "run"
    return f"{base}-{suffix}"


def _build_fix_command(
    *,
    agent: ImplementerAgent,
    codex_model: str,
    prompt: str,
) -> list[str]:
    """Mirror `build_fix_runner_command` without importing from orchestrator.

    Kept inline to avoid a circular import: the orchestrator imports
    this module, and we don't want this module to depend on the
    orchestrator's command-builders.
    """
    if agent == "claude":
        return [
            "claude",
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            prompt,
        ]
    if agent == "codex":
        return [
            "codex",
            "exec",
            "--json",
            "--sandbox",
            "workspace-write",
            "--model",
            codex_model,
            prompt,
        ]
    raise ValueError(f"unknown implementer agent {agent!r}")


async def run_local_review_session(
    *,
    runner: Runner,
    workspace_path: Path,
    base_branch: str,
    parent_run_id: str,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    implementer_agent: ImplementerAgent,
    implementer_codex_model: str,
    reviewer_agent: ReviewerAgent,
    reviewer_codex_model: str,
    cap: int,
    stall_secs: int,
    last_message_dir: Path,
    head_sha_provider: HeadShaProvider,
    cost_cap_usd: float = 0.0,
    prior_cost_usd: float = 0.0,
    should_skip: SkipPredicate | None = None,
    on_iteration: IterationCallback | None = None,
    report_active_run_id: ActiveRunIdReporter | None = None,
) -> LoopResult:
    """Run the review→fix loop in-workspace; return the loop's outcome.

    `last_message_dir` is where the reviewer's `-o <file>` payloads go
    so each iteration's final agent message is recoverable. The
    directory is created on demand.
    """
    # Same sync-mkdir pattern as `_run_stage_command` in poll.py.
    # Directory creation is microseconds; pushing it to a thread would
    # add overhead with no real benefit.
    last_message_dir.mkdir(parents=True, exist_ok=True)  # noqa: ASYNC240

    review_prompt = local_review_prompt(
        issue_title=issue_title,
        issue_body=issue_body,
        labels=labels,
        base_branch=base_branch,
    )

    # One estimator per agent — codex sums token deltas across calls,
    # so it must persist across iterations. Sharing the reviewer
    # estimator across all reviewer subprocess calls (and likewise for
    # the fixer) keeps the cumulative-token invariant intact.
    reviewer_estimator = UsageCostEstimator(
        agent=reviewer_agent,
        codex_model=reviewer_codex_model or DEFAULT_CODEX_MODEL,
    )
    fixer_estimator = UsageCostEstimator(
        agent=implementer_agent,
        codex_model=implementer_codex_model,
    )

    async def _reviewer(iteration: int) -> ReviewerOutput:
        head_sha = await head_sha_provider(workspace_path)
        last_message_path = last_message_dir / f"review-{iteration}.last.txt"
        # Clear any previous iteration's leftover so a partial run
        # doesn't smuggle a stale "approved" into the next pass.
        if last_message_path.exists():
            try:
                last_message_path.unlink()
            except OSError:
                pass
        command = build_local_review_command(
            agent=reviewer_agent,
            prompt=review_prompt,
            base_branch=base_branch,
            codex_model=reviewer_codex_model or DEFAULT_CODEX_MODEL,
            last_message_path=(
                str(last_message_path) if reviewer_agent == "codex" else None
            ),
        )
        spec = RunnerSpec(
            run_id=_safe_run_id(parent_run_id, f"rev-{iteration}"),
            workspace_path=workspace_path,
            command=command,
            stall_secs=stall_secs,
            stage="local_review",
        )
        cost_before = reviewer_estimator.total_cost_usd
        if report_active_run_id is not None:
            await report_active_run_id(spec.run_id)
        try:
            collected = await collect_runner_output(
                runner, spec, usage_handler=reviewer_estimator.delta
            )
        finally:
            if report_active_run_id is not None:
                await report_active_run_id(None)
        cost_delta = reviewer_estimator.total_cost_usd - cost_before

        last_message_text: str | None = None
        if last_message_path.exists():
            try:
                last_message_text = last_message_path.read_text(
                    encoding="utf-8", errors="replace"
                )
            except OSError:
                last_message_text = None

        if collected.terminal_kind == "spawn_failed":
            return ReviewerOutput(
                stdout=collected.stdout,
                head_sha=head_sha,
                last_message_file=last_message_text,
                ok=False,
                error=f"spawn_failed: {collected.spawn_error or 'unknown'}",
                cost_usd=cost_delta,
            )
        if collected.stall_timeout:
            return ReviewerOutput(
                stdout=collected.stdout,
                head_sha=head_sha,
                last_message_file=last_message_text,
                ok=False,
                error="reviewer stalled",
                cost_usd=cost_delta,
            )
        # A non-zero exit is *not* automatically failure — the reviewer
        # may still have emitted a usable agent_message before crashing.
        # The verdict parser decides; if the marker is missing the loop
        # treats it as REVIEWER_FAILED.
        return ReviewerOutput(
            stdout=collected.stdout,
            head_sha=head_sha,
            last_message_file=last_message_text,
            ok=True,
            cost_usd=cost_delta,
        )

    async def _fixer(iteration: int, verdict: LocalVerdict) -> FixerOutput:
        prompt = review_comment_fix_prompt(
            issue_title=issue_title,
            issue_body=issue_body,
            labels=labels,
            trigger=verdict.findings,
        )
        command = _build_fix_command(
            agent=implementer_agent,
            codex_model=implementer_codex_model,
            prompt=prompt,
        )
        spec = RunnerSpec(
            run_id=_safe_run_id(parent_run_id, f"fix-{iteration}"),
            workspace_path=workspace_path,
            command=command,
            stall_secs=stall_secs,
            stage="local_review_fix",
        )
        cost_before = fixer_estimator.total_cost_usd
        if report_active_run_id is not None:
            await report_active_run_id(spec.run_id)
        try:
            collected = await collect_runner_output(
                runner, spec, usage_handler=fixer_estimator.delta
            )
        finally:
            if report_active_run_id is not None:
                await report_active_run_id(None)
        cost_delta = fixer_estimator.total_cost_usd - cost_before
        if collected.terminal_kind == "spawn_failed":
            return FixerOutput(
                ok=False,
                error=f"spawn_failed: {collected.spawn_error or 'unknown'}",
                cost_usd=cost_delta,
            )
        if collected.stall_timeout:
            return FixerOutput(
                ok=False, error="fix-run stalled", cost_usd=cost_delta
            )
        if not collected.ok_exit:
            return FixerOutput(
                ok=False,
                error=f"fix-run exited rc={collected.returncode}",
                cost_usd=cost_delta,
            )
        return FixerOutput(ok=True, cost_usd=cost_delta)

    return await run_local_review_loop(
        reviewer_agent=reviewer_agent,
        reviewer=_reviewer,
        fixer=_fixer,
        cap=cap,
        cost_cap_usd=cost_cap_usd,
        prior_cost_usd=prior_cost_usd,
        should_skip=should_skip,
        on_iteration=on_iteration,
    )


__all__ = [
    "HeadShaProvider",
    "ImplementerAgent",
    "run_local_review_session",
]
