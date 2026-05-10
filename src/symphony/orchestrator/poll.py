"""The always-running poll loop.

Walking-skeleton scope (iteration 3): scan each configured Linear team for
issues in the "ready" state with the configured label, post a "would
dispatch" comment, then mark the issue in-memory so we don't re-comment.

Real dispatch (workspace clone, agent spawn, GitHub PR creation, stage
transitions) lands in iteration 4+. The structure of this loop is the
v1 shape; only the body of `_dispatch_one` changes.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime

from ..config import Config, RepoBinding
from ..linear.client import Linear, LinearError, LinearIssue
from ..linear.templates import CommentVars, run_started

log = logging.getLogger(__name__)


class Orchestrator:
    """Owns the poll loop and the in-memory dispatch ledger.

    `_dispatched` is the throwaway in-memory analog of the SQLite `runs`
    table; iteration 4 replaces it with `db.runs`.
    """

    def __init__(self, config: Config, linear: Linear) -> None:
        self.config = config
        self.linear = linear
        self._shutdown = asyncio.Event()
        # (linear_uuid -> first-dispatch RFC3339 timestamp). Used in lieu of
        # SQLite for the walking skeleton so a re-poll doesn't re-comment.
        self._dispatched: dict[str, str] = {}
        # Cache of (team_key -> {state_name: state_uuid}). Re-fetched on
        # startup; never mutated at runtime.
        self._states: dict[str, dict[str, str]] = {}

    async def warmup(self) -> None:
        """One-time startup work: cache team workflow states, validate auth."""
        viewer_keys = await self.linear.viewer_team_keys()
        log.info("linear viewer sees teams: %s", viewer_keys)
        for binding in self.config.repos:
            if binding.linear_team_key not in viewer_keys:
                log.warning(
                    "team %s configured but not visible to API key — "
                    "the binding will produce no work",
                    binding.linear_team_key,
                )
                continue
            self._states[binding.linear_team_key] = await self.linear.team_states(
                binding.linear_team_key
            )

    async def shutdown(self) -> None:
        self._shutdown.set()

    async def run(self) -> None:
        """The single long-lived task. Cancellation-safe."""
        await self.warmup()
        log.info("orchestrator entering poll loop (interval=%ds)", self.config.poll_interval_secs)
        while not self._shutdown.is_set():
            try:
                await self._tick()
            except Exception:  # noqa: BLE001 — must not kill the loop
                log.exception("poll cycle failed")
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=self.config.poll_interval_secs
                )
            except TimeoutError:
                pass

    async def _tick(self) -> None:
        for binding in self.config.repos:
            await self._scan_binding(binding)

    async def _scan_binding(self, binding: RepoBinding) -> None:
        ready_state = binding.linear_states.ready
        try:
            issues = await self.linear.issues_in_state(
                binding.linear_team_key, ready_state, binding.issue_label
            )
        except LinearError as e:
            log.warning("scan failed for %s: %s", binding.linear_team_key, e)
            return
        log.info(
            "scan %s: %d issue(s) in %s%s",
            binding.linear_team_key,
            len(issues),
            ready_state,
            f" with label '{binding.issue_label}'" if binding.issue_label else "",
        )
        for issue in issues:
            if issue.id in self._dispatched:
                continue
            await self._dispatch_one(binding, issue)

    async def _dispatch_one(self, binding: RepoBinding, issue: LinearIssue) -> None:
        """Walking-skeleton: just announce and mark dispatched.

        Iteration 4 will:
        - Clone the GitHub repo to `workspace_root / binding.repo_safe / issue.identifier`.
        - Build a stage-1 prompt from `agent.prompts.implement(issue, binding)`.
        - Spawn the runner; stream events into the DB and Linear.
        - Move the Linear issue to `binding.linear_states.in_progress`.
        """
        run_id = str(uuid.uuid4())
        log.info(
            "would dispatch %s (%s) -> %s [run_id=%s]",
            issue.identifier,
            issue.title,
            binding.github_repo,
            run_id,
        )
        body = run_started(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,  # GitHub issue number not yet bound in v0
                run_id=run_id,
            )
        )
        try:
            await self.linear.post_comment(issue.id, body)
        except LinearError as e:
            log.warning("could not announce dispatch on %s: %s", issue.identifier, e)
            return
        self._dispatched[issue.id] = datetime.now(UTC).isoformat()
