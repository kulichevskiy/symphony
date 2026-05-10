"""The always-running poll loop.

Walking-skeleton scope (iteration 3): scan each configured Linear team for
issues in the "ready" state with the configured label, post a "would
dispatch" comment, then record a `runs` row so we don't re-comment.

Real dispatch (workspace clone, agent spawn, GitHub PR creation, stage
transitions) lands in iteration 4+. The structure of this loop is the
v1 shape; only the body of `_dispatch_one` changes.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime

import aiosqlite

from .. import db
from ..config import Config, RepoBinding
from ..linear.client import Linear, LinearError, LinearIssue
from ..linear.templates import CommentVars, run_started

log = logging.getLogger(__name__)


class Orchestrator:
    """Owns the poll loop. Dedupe is a SQLite query over the `runs` table."""

    def __init__(self, config: Config, linear: Linear, conn: aiosqlite.Connection) -> None:
        self.config = config
        self.linear = linear
        self._conn = conn
        self._shutdown = asyncio.Event()
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
            if await db.runs.has_active(self._conn, issue.id):
                continue
            await self._dispatch_one(binding, issue)

    async def _dispatch_one(self, binding: RepoBinding, issue: LinearIssue) -> str | None:
        """Walking-skeleton: record a `runs` row, then announce.

        Returns the `run_id` of the row that was written, or `None` if the
        atomic dedupe found a pre-existing live run and skipped the insert
        (a concurrent dispatcher won the race; do not announce again). On
        success the row's final status is `running`; if the announce
        raised, it's `failed` and callers that need to surface that
        distinction (e.g. the CLI `dispatch` command) can re-read the row.

        Persisting first is what makes the SQLite-backed dedupe correct: if
        the host crashed (or the DB write threw) *after* a successful
        `post_comment`, the next poll would see no active run and post a
        second "starting" comment. Writing the row first closes that
        window. The insert itself uses `create_if_no_active` rather than a
        separate `has_active` check + unconditional insert, so the dedupe
        is atomic against a racing poll loop or a second manual dispatch.
        If the announce itself fails, we flip the row to `failed` so the
        next tick can retry without the dedupe suppressing it.

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
        await db.issues.upsert(
            self._conn,
            id=issue.id,
            identifier=issue.identifier,
            title=issue.title,
            team_key=issue.team_key,
        )
        now = datetime.now(UTC).isoformat()
        inserted = await db.runs.create_if_no_active(
            self._conn,
            id=run_id,
            issue_id=issue.id,
            stage="implement",
            status="running",
            pid=None,  # iteration 4 fills this in once a real subprocess is spawned
            started_at=now,
        )
        if not inserted:
            log.info(
                "skipping dispatch for %s: another dispatcher won the race",
                issue.identifier,
            )
            return None
        try:
            await self.linear.post_comment(issue.id, body)
        except LinearError as e:
            log.warning("could not announce dispatch on %s: %s", issue.identifier, e)
            await db.runs.update_status(
                self._conn, run_id, "failed", ended_at=datetime.now(UTC).isoformat()
            )
        return run_id
