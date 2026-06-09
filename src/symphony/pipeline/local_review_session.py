"""End-to-end local-review session: build callbacks, run the loop.

This is the single entry point the orchestrator calls once the
Implement stage succeeds and `local_review` is enabled. It wires
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
  - `APPROVED`            push and proceed (skip `@codex` when `remote_review`
                          is false, post `@codex review` once when true).
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
from dataclasses import replace
from pathlib import Path
from typing import Literal

from ..agent.codex_cli import build_codex_workspace_write_command
from ..agent.codex_models import DEFAULT_CODEX_MODEL
from ..agent.prompt import review_comment_fix_prompt
from ..agent.runner import Runner, RunnerSpec
from .cost_guard import UsageCostEstimator
from .local_review import (
    DiffSize,
    LocalVerdict,
    ReviewerAgent,
    build_local_review_command,
    extract_last_agent_message,
    is_small_diff,
    local_review_finder_prompt,
    local_review_prompt,
    local_review_verifier_prompt,
)
from .local_review_io import CollectedRunnerOutput, collect_runner_output
from .local_review_loop import (
    FixerOutput,
    IterationCallback,
    LoopResult,
    ReviewerOutput,
    run_local_review_loop,
)

ImplementerAgent = Literal["claude", "codex"]

HeadShaProvider = Callable[[Path], Awaitable[str]]

# Measures the current branch's diff so `_reviewer` can pick single- vs
# two-pass. The orchestrator implements this with `git diff --numstat` +
# `parse_diff_numstat`; tests inject a fake. When None, the session can't
# size the diff and stays single-pass (cheaper, back-compat default).
DiffSizeProvider = Callable[[Path], Awaitable[DiffSize]]

# Scrubs the working tree after the pass-2 verifier returns, before verdict
# parsing and before the fixer. The orchestrator implements this with
# `git checkout -- . && git clean -fd` so the verifier's throwaway tests /
# mutations never reach the diff the fixer sees. When None, no scrub runs
# (back-compat: only relevant once pass 2 has Tier B write access).
WorkspaceScrubber = Callable[[Path], Awaitable[None]]

# Run IDs must survive becoming git refs / log filenames. The orchestrator
# already uses UUIDs, but if a caller passes something weirder we still
# want clean derived IDs.
_RUN_ID_SAFE_RE = re.compile(r"[^a-zA-Z0-9_.\-]")


def _safe_run_id(parent_run_id: str, suffix: str) -> str:
    base = _RUN_ID_SAFE_RE.sub("-", parent_run_id) or "run"
    return f"{base}-{suffix}"


def _persist_runner_transcript(
    log_dir: Path, stem: str, collected: CollectedRunnerOutput
) -> None:
    (log_dir / f"{stem}.out.log").write_text(
        collected.stdout, encoding="utf-8", errors="replace"
    )
    (log_dir / f"{stem}.err.log").write_text(
        collected.stderr, encoding="utf-8", errors="replace"
    )


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
            # Headless MCP policy: the fixer sees no MCP servers at all.
            # (The binding's allowlist is not threaded into this session;
            # the empty strict config matches the policy default of none.)
            "--strict-mcp-config",
            prompt,
        ]
    if agent == "codex":
        return build_codex_workspace_write_command(
            prompt=prompt,
            codex_model=codex_model,
        )
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
    command_secs: int = 1800,
    last_message_dir: Path,
    head_sha_provider: HeadShaProvider,
    diff_size_provider: DiffSizeProvider | None = None,
    workspace_scrubber: WorkspaceScrubber | None = None,
    on_iteration: IterationCallback | None = None,
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
    finder_prompt = local_review_finder_prompt(
        issue_title=issue_title,
        issue_body=issue_body,
        labels=labels,
        base_branch=base_branch,
    )

    # One estimator per agent — codex sums token deltas across calls,
    # so it must persist across iterations. Sharing the reviewer
    # estimator across all reviewer subprocess calls (and likewise for
    # the fixer) keeps the cumulative-token invariant intact. The pass-2
    # verifier runs in the implementer's family, so it needs its own
    # estimator: feeding its (separate) codex token stream through the
    # reviewer's estimator would corrupt the cumulative-max bookkeeping.
    reviewer_estimator = UsageCostEstimator(
        agent=reviewer_agent,
        codex_model=reviewer_codex_model or DEFAULT_CODEX_MODEL,
    )
    verifier_estimator = UsageCostEstimator(
        agent=implementer_agent,
        codex_model=implementer_codex_model,
    )
    fixer_estimator = UsageCostEstimator(
        agent=implementer_agent,
        codex_model=implementer_codex_model,
    )

    async def _run_reviewer_pass(
        *,
        agent: ReviewerAgent,
        codex_model: str,
        prompt: str,
        stem: str,
        run_suffix: str,
        estimator: UsageCostEstimator,
        head_sha: str,
        pass_two: bool = False,
    ) -> ReviewerOutput:
        """Run one reviewer subprocess and price its usage.

        `stem` names the transcript / last-message files; `run_suffix`
        names the RunnerSpec id. Both single-pass and the two finder/
        verifier passes route through here so cost accounting, transcript
        persistence, and failure handling stay identical. `pass_two` grants
        the Tier B exec/write surface (verifier only); pass 1 and the
        single-pass fallback stay read-only.
        """
        last_message_path = last_message_dir / f"{stem}.last.txt"
        # Clear any previous iteration's leftover so a partial run
        # doesn't smuggle a stale "approved" into the next pass.
        if last_message_path.exists():
            try:
                last_message_path.unlink()
            except OSError:
                pass
        command = build_local_review_command(
            agent=agent,
            prompt=prompt,
            base_branch=base_branch,
            codex_model=codex_model or DEFAULT_CODEX_MODEL,
            last_message_path=(
                str(last_message_path) if agent == "codex" else None
            ),
            pass_two=pass_two,
        )
        spec = RunnerSpec(
            run_id=_safe_run_id(parent_run_id, run_suffix),
            workspace_path=workspace_path,
            command=command,
            stall_secs=stall_secs,
            command_secs=command_secs,
            stage="local_review",
        )
        cost_before = estimator.total_cost_usd
        input_before = estimator.total_input_tokens
        output_before = estimator.total_output_tokens
        cache_write_before = estimator.total_cache_write_tokens
        cache_read_before = estimator.total_cache_read_tokens
        collected = await collect_runner_output(
            runner, spec, usage_handler=estimator.delta
        )
        _persist_runner_transcript(last_message_dir, stem, collected)
        cost_delta = estimator.total_cost_usd - cost_before
        input_delta = estimator.total_input_tokens - input_before
        output_delta = estimator.total_output_tokens - output_before
        cache_write_delta = (
            estimator.total_cache_write_tokens - cache_write_before
        )
        cache_read_delta = (
            estimator.total_cache_read_tokens - cache_read_before
        )

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
                input_tokens=input_delta,
                output_tokens=output_delta,
                cache_write_tokens=cache_write_delta,
                cache_read_tokens=cache_read_delta,
            )
        if collected.stall_timeout:
            return ReviewerOutput(
                stdout=collected.stdout,
                head_sha=head_sha,
                last_message_file=last_message_text,
                ok=False,
                error="reviewer stalled",
                cost_usd=cost_delta,
                input_tokens=input_delta,
                output_tokens=output_delta,
                cache_write_tokens=cache_write_delta,
                cache_read_tokens=cache_read_delta,
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
            input_tokens=input_delta,
            output_tokens=output_delta,
            cache_write_tokens=cache_write_delta,
            cache_read_tokens=cache_read_delta,
        )

    async def _reviewer(iteration: int) -> ReviewerOutput:
        head_sha = await head_sha_provider(workspace_path)

        # Small diffs collapse to a single direct review to save the
        # second subprocess. Without a provider we can't size the diff,
        # so default to single-pass (cheaper, back-compat).
        small = True
        if diff_size_provider is not None:
            small = is_small_diff(await diff_size_provider(workspace_path))
        if small:
            return await _run_reviewer_pass(
                agent=reviewer_agent,
                codex_model=reviewer_codex_model,
                prompt=review_prompt,
                stem=f"review-{iteration}",
                run_suffix=f"rev-{iteration}",
                estimator=reviewer_estimator,
                head_sha=head_sha,
            )

        # Pass 1 — finder, opposite the implementer's family. Lists every
        # suspicion, emits no verdict marker; its findings feed pass 2.
        finder_out = await _run_reviewer_pass(
            agent=reviewer_agent,
            codex_model=reviewer_codex_model,
            prompt=finder_prompt,
            stem=f"review-{iteration}-find",
            run_suffix=f"rev-{iteration}-find",
            estimator=reviewer_estimator,
            head_sha=head_sha,
        )
        if not finder_out.ok:
            # Propagate the finder failure (with its cost) to the loop;
            # no point paying for a verifier with nothing to verify.
            return finder_out

        pass_one_findings = extract_last_agent_message(
            agent=reviewer_agent,
            stdout=finder_out.stdout,
            last_message_file=finder_out.last_message_file,
        )
        verifier_prompt = local_review_verifier_prompt(
            issue_title=issue_title,
            issue_body=issue_body,
            labels=labels,
            base_branch=base_branch,
            pass_one_findings=pass_one_findings,
        )
        # Pass 2 — adversarial verifier, the implementer's family (model
        # diversity vs pass 1). Refutes/confirms pass-1 findings, adds
        # misses, and emits the single marker the loop parses.
        verifier_out = await _run_reviewer_pass(
            agent=implementer_agent,
            codex_model=implementer_codex_model,
            prompt=verifier_prompt,
            stem=f"review-{iteration}-verify",
            run_suffix=f"rev-{iteration}-verify",
            estimator=verifier_estimator,
            head_sha=head_sha,
            pass_two=True,
        )
        # Pass 2 had Tier B write access: scrub the working tree before the
        # verdict is parsed and before the fixer runs, so the verifier's
        # throwaway tests / scratch edits never reach the diff the fixer
        # sees. The verifier's final message is already captured below from
        # stdout / the last-message file (outside the workspace), so the
        # scrub can't lose the verdict.
        if workspace_scrubber is not None:
            await workspace_scrubber(workspace_path)
        # Merge: the loop parses pass-2's verdict (survivors + new
        # findings already merged in the verifier's message); fold in
        # pass-1's usage so the loop sees the full two-pass spend.
        #
        # The loop parses with the session's `reviewer_agent`, but pass 2
        # ran in the implementer's (possibly different) family. Pin the
        # verifier's final message as `last_message_file` so the parser
        # reads it verbatim regardless of which agent's JSONL the stdout
        # is in (the parser prefers `last_message_file`).
        verifier_message = extract_last_agent_message(
            agent=implementer_agent,
            stdout=verifier_out.stdout,
            last_message_file=verifier_out.last_message_file,
        )
        return replace(
            verifier_out,
            last_message_file=verifier_message,
            cost_usd=verifier_out.cost_usd + finder_out.cost_usd,
            input_tokens=verifier_out.input_tokens + finder_out.input_tokens,
            output_tokens=(
                verifier_out.output_tokens + finder_out.output_tokens
            ),
            cache_write_tokens=(
                verifier_out.cache_write_tokens + finder_out.cache_write_tokens
            ),
            cache_read_tokens=(
                verifier_out.cache_read_tokens + finder_out.cache_read_tokens
            ),
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
            command_secs=command_secs,
            stage="local_review_fix",
        )
        cost_before = fixer_estimator.total_cost_usd
        input_before = fixer_estimator.total_input_tokens
        output_before = fixer_estimator.total_output_tokens
        cache_write_before = fixer_estimator.total_cache_write_tokens
        cache_read_before = fixer_estimator.total_cache_read_tokens
        collected = await collect_runner_output(
            runner, spec, usage_handler=fixer_estimator.delta
        )
        _persist_runner_transcript(
            last_message_dir, f"fix-{iteration}", collected
        )
        cost_delta = fixer_estimator.total_cost_usd - cost_before
        input_delta = fixer_estimator.total_input_tokens - input_before
        output_delta = fixer_estimator.total_output_tokens - output_before
        cache_write_delta = (
            fixer_estimator.total_cache_write_tokens - cache_write_before
        )
        cache_read_delta = fixer_estimator.total_cache_read_tokens - cache_read_before
        if collected.terminal_kind == "spawn_failed":
            return FixerOutput(
                ok=False,
                error=f"spawn_failed: {collected.spawn_error or 'unknown'}",
                cost_usd=cost_delta,
                input_tokens=input_delta,
                output_tokens=output_delta,
                cache_write_tokens=cache_write_delta,
                cache_read_tokens=cache_read_delta,
            )
        if collected.stall_timeout:
            return FixerOutput(
                ok=False,
                error="fix-run stalled",
                cost_usd=cost_delta,
                input_tokens=input_delta,
                output_tokens=output_delta,
                cache_write_tokens=cache_write_delta,
                cache_read_tokens=cache_read_delta,
            )
        if not collected.ok_exit:
            return FixerOutput(
                ok=False,
                error=f"fix-run exited rc={collected.returncode}",
                cost_usd=cost_delta,
                input_tokens=input_delta,
                output_tokens=output_delta,
                cache_write_tokens=cache_write_delta,
                cache_read_tokens=cache_read_delta,
            )
        return FixerOutput(
            ok=True,
            cost_usd=cost_delta,
            input_tokens=input_delta,
            output_tokens=output_delta,
            cache_write_tokens=cache_write_delta,
            cache_read_tokens=cache_read_delta,
        )

    return await run_local_review_loop(
        reviewer_agent=reviewer_agent,
        reviewer=_reviewer,
        fixer=_fixer,
        cap=cap,
        on_iteration=on_iteration,
    )


__all__ = [
    "HeadShaProvider",
    "ImplementerAgent",
    "WorkspaceScrubber",
    "run_local_review_session",
]
