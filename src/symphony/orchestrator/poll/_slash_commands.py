"""`_SlashCommandsMixin` — the slash-command domain of the poll loop (SYM-145).

Owns the slash-command surface: the web-command queue, the Linear comment-cursor
poll, the `$intent` dispatcher (`_handle_slash_intent`), and every per-state
`_handle_*_slash_intent` handler. It extends `_OrchestratorBase` so it sees the
shared state + foundation methods; the concrete `Orchestrator` (in `__init__.py`)
inherits this mixin.

The cross-domain methods this layer calls (`_stop_review_monitor`,
`_schedule_merge`, `_deliver_implement_run`, ...) still live on `Orchestrator`;
they are declared under `TYPE_CHECKING` below so mypy resolves them without a
runtime stub.

Pure structural extraction: method bodies are byte-for-byte unchanged from the
pre-split `Orchestrator`.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ... import db
from ...config import RepoBinding
from ...github.client import GitHubError
from ...linear import slash
from ...linear.client import LinearError
from ...linear.slash import SlashIntent, SlashKind
from ...linear.templates import (
    CommentVars,
    acceptance_retry_requested,
    acceptance_skipped,
    command_rejected,
    resumed,
    retry_acceptance_requested,
    skip_acceptance_forced,
    truncate_body,
)
from ...pipeline import controls
from ...tracker import Comment as LinearComment
from ...tracker import Issue as LinearIssue
from ._base import (
    SlashHandlerFailure as SlashHandlerFailure,
)
from ._base import (
    _ImplementHandoff,
    _OrchestratorBase,
    _PendingDelivery,
)
from ._helpers import (
    _needs_human_approval_label_present,
    _parse_rfc3339,
    _pr_url_for_state,
)

log = logging.getLogger(__name__)


MANUAL_MERGE_PARKED_RUN_PREFIX = "manual-merge-parked:"
# Synthetic comment id `_apply_web_command` stamps on a UI-issued command.
WEB_COMMAND_COMMENT_PREFIX = "web-"


def _manual_merge_parked_run_id(pr: db.issue_prs.IssuePR) -> str:
    return f"{MANUAL_MERGE_PARKED_RUN_PREFIX}{pr.issue_id}:{pr.github_repo}:{pr.pr_number}"


class _SlashCommandsMixin(_OrchestratorBase):
    """Slash-command domain of the poll loop; `Orchestrator` extends it."""

    if TYPE_CHECKING:
        # Sibling-domain methods provided by the concrete `Orchestrator`.
        def _binding_for_pr(self, candidate: db.issue_prs.IssuePR) -> RepoBinding | None: ...

        async def _binding_for_review_issue_id(
            self, issue_id: str, *, state: db.review_state.ReviewState
        ) -> RepoBinding | None: ...

        async def _blocked_reason_for_run(self, run_id: str) -> str: ...

        async def _clear_operator_wait(
            self, issue_id: str, run_id: str, *, commit: bool = True
        ) -> None: ...

        async def _reopen_operator_wait(self, wait: db.operator_waits.OperatorWait) -> None: ...

        async def _clear_stage_park(
            self,
            issue_id: str,
            run_id: str,
            *,
            kind: str,
            reason: str | None = None,
            outcome: controls.AttemptOutcome = ...,
        ) -> None: ...

        async def _clear_review_rearm_retry(self, run_id: str) -> None: ...

        async def _deliver_implement_run(
            self, *, ctx: _PendingDelivery, base_branch: str | None = None
        ) -> str: ...

        async def _handle_active_review_retry_intent(
            self, issue_id: str, run_id: str, intent: SlashIntent
        ) -> None: ...

        async def _handle_merge_needs_approval_slash_intent(
            self, issue_id: str, run_id: str, intent: SlashIntent
        ) -> None: ...

        async def _handle_review_failed_slash_intent(
            self, issue_id: str, run_id: str, intent: SlashIntent
        ) -> None: ...

        async def _handle_skip_review_intent(self, issue_id: str, run_id: str) -> None: ...

        async def _open_merge_wait_for_human_approval_label(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            pr_url: str,
        ) -> None: ...

        async def _resolve_pending_delivery(
            self,
            issue_id: str,
            run_id: str,
            binding: RepoBinding,
            intent: SlashIntent,
        ) -> _PendingDelivery | None: ...

        async def _restore_operator_wait_binding(
            self,
            issue_id: str,
            run_id: str,
            intent: SlashIntent,
            *,
            expected_kinds: tuple[str, ...],
        ) -> RepoBinding | None: ...

        async def _restore_operator_waits(self) -> None: ...

        async def _resume_review_monitor(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            issue_id: str,
            tracker_issue_id: str,
            run_id: str,
        ) -> None: ...

        def _schedule_acceptance(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            pr_number: int,
            pr_url: str,
            pr_head_sha: str,
        ) -> asyncio.Task[None]: ...

        def _schedule_merge(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            pr_number: int,
            pr_url: str,
            approved_head_sha: str = "",
            skip_review: bool = False,
            on_started: Callable[[str], Awaitable[None]] | None = None,
            storage_issue_id: str | None = None,
        ) -> asyncio.Task[None]: ...

        async def _stop_review_monitor(self, issue_id: str, run_id: str) -> None: ...

        async def _terminate_deliver_failed_review_monitors(
            self, issue_id: str, *, detail: str
        ) -> None: ...

    def enqueue_web_command(self, issue_id: str, kind: SlashKind) -> str:
        """Submit an operator command from the web UI.

        Returns a command id and wakes the poll loop so the command is drained
        and applied on the loop's next turn. Validation of issue existence and
        command name happens in the HTTP handler; run eligibility is resolved
        at drain time (mirrors the Linear slash-comment path).
        """
        command_id = uuid.uuid4().hex
        self._web_commands.put_nowait((issue_id, kind, command_id))
        self._wake.set()
        return command_id

    async def _drain_web_commands(self) -> None:
        while True:
            try:
                issue_id, kind, command_id = self._web_commands.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await self._apply_web_command(issue_id, kind, command_id)
            except Exception:  # noqa: BLE001 — a bad command must not kill the loop
                log.exception("web command failed (issue=%s kind=%s)", issue_id, kind)

    async def _apply_web_command(self, issue_id: str, kind: SlashKind, command_id: str) -> None:
        run_id = await self._web_command_run_id(issue_id)
        if run_id is None:
            log.warning(
                "web command $%s for issue %s has no eligible run; dropping",
                kind.value,
                issue_id,
            )
            return
        intent = SlashIntent(
            kind=kind,
            comment_id=f"{WEB_COMMAND_COMMENT_PREFIX}{command_id}",
            created_at=self._now().isoformat(),
        )
        await self._handle_slash_intent(issue_id, run_id, intent)

    async def _web_command_run_id(self, issue_id: str) -> str | None:
        run_id = self._dispatch_run_ids.get(issue_id) or self._review_poll_issue_ids.get(issue_id)
        if run_id is None:
            for iid, rid in await self._parked_manual_merge_slash_pairs():
                if iid == issue_id:
                    run_id = rid
                    break
        if run_id is None or not self._slash_command_run_eligible(run_id):
            return None
        return run_id

    def _slash_command_run_eligible(self, run_id: str) -> bool:
        return (
            run_id in self._active_run_ids
            or run_id in self._operator_wait_run_ids
            or run_id in self._review_poll_run_ids
            or run_id.startswith(MANUAL_MERGE_PARKED_RUN_PREFIX)
        )

    async def _parked_manual_merge_slash_pairs(self) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        for pr in await db.issue_prs.list_merge_candidates(self._conn):
            if pr.parked_at is None:
                continue
            if self._binding_for_pr(pr) is None:
                log.warning(
                    "cannot watch parked manual-merge PR %s#%d: no binding",
                    pr.github_repo,
                    pr.pr_number,
                )
                continue
            pairs.append((pr.issue_id, _manual_merge_parked_run_id(pr)))
        return pairs

    async def _parked_manual_merge_run_id_for_issue(self, issue_id: str) -> str | None:
        pr = await db.issue_prs.get_for_issue(self._conn, issue_id=issue_id)
        if pr is None or pr.merged_at is not None or pr.parked_at is None:
            return None
        if self._binding_for_pr(pr) is None:
            return None
        return _manual_merge_parked_run_id(pr)

    async def _poll_slash_commands(self) -> None:
        """For each active run, fetch new comments and dispatch slash intents.

        The cursor (`(timestamp, ids_at_timestamp)`) lives in `comment_cursors`.
        We query with `gte` and drop any comment whose ID is in the cursor's
        boundary set, which both (a) avoids re-firing handled commands across
        restarts and (b) avoids losing comments tied at the boundary timestamp.
        """
        await self._restore_operator_waits()
        active_pairs = list(self._dispatch_run_ids.items())
        # Also include issues in active review polling that have no active fix run.
        dispatch_issue_ids = {iid for iid, _ in active_pairs}
        active_pairs += [
            (iid, run_id)
            for iid, run_id in self._review_poll_issue_ids.items()
            if iid not in dispatch_issue_ids
        ]
        pairs: list[tuple[str, str]] = []
        paired_issue_ids: set[str] = set()
        parked_pairs = await self._parked_manual_merge_slash_pairs()
        for issue_id, run_id in [*active_pairs, *parked_pairs]:
            if not self._slash_command_run_eligible(run_id):
                continue
            if issue_id in paired_issue_ids:
                continue
            paired_issue_ids.add(issue_id)
            pairs.append((issue_id, run_id))
        for issue_id, run_id in pairs:
            try:
                after, seen_ids = await self._resolve_comment_cursor(issue_id, run_id)
            except Exception:  # noqa: BLE001 — keep loop alive
                log.exception("failed to resolve cursor for issue %s", issue_id)
                continue
            tracker_issue_id, tracker_ctx = await self._tracker_identity_for_issue(issue_id)
            try:
                comments = await self.tracker(tracker_ctx).comments_since(tracker_issue_id, after)
            except LinearError as e:
                log.warning("comments_since failed for %s: %s", issue_id, e)
                continue
            latest_self_authored: LinearComment | None = None
            for comment in comments:
                if comment.id in seen_ids:
                    continue
                try:
                    await self._handle_unseen_slash_comment(issue_id, run_id, comment)
                except SlashHandlerFailure:
                    # Handler failed mid-transition; the rejection was posted
                    # and the comment was deliberately NOT marked seen. Stop
                    # iterating later comments for this issue so the failed
                    # command stays first-in-line on the next poll tick —
                    # otherwise a later comment could advance the cursor past
                    # the failed one, recreating the silent-drop behavior.
                    # Crucially, the self-authored cursor catch-up below is
                    # also skipped — we must not advance past any failed
                    # command.
                    latest_self_authored = None
                    break
                if comment.author_is_me:
                    # `_handle_unseen_slash_comment` deliberately did not
                    # advance the cursor for self-authored comments (so a
                    # `command_rejected` posted mid-failure can't strand the
                    # failed original). Now that the loop has reached the
                    # end without a `SlashHandlerFailure`, every prior comment
                    # in the batch was either handled or safely skipped — so
                    # we can advance the cursor past the latest self-authored
                    # one. Without this, `comments_since` would re-fetch the
                    # same bot-authored comments on every tick, growing
                    # unboundedly over long outages.
                    latest_self_authored = comment
            if latest_self_authored is not None:
                await self._advance_comment_cursor(
                    issue_id,
                    latest_self_authored.created_at,
                    {latest_self_authored.id},
                )

    async def _handle_unseen_slash_comment(
        self, issue_id: str, run_id: str, comment: LinearComment
    ) -> bool:
        """Process a single slash comment under the comment-event lock.

        Returns True when the comment was handled and persisted (marked seen
        + cursor advanced). Returns False when the comment was already seen
        (duplicate) OR carries no actionable intent for us (self-authored or
        externally mirrored — `slash.parse` filters those out). Raises
        `SlashHandlerFailure` when the handler failed mid-transition (e.g.
        `linear.move_issue` upstream error); in that case a rejection
        comment has been posted and the comment is intentionally NOT marked
        seen, so the caller MUST stop processing later comments for this
        issue (otherwise their cursor advance would leave the failed
        comment stranded).
        """
        # Self-authored comments (e.g. the `command_rejected` we post after
        # a `SlashHandlerFailure`) MUST NOT advance the cursor — otherwise a
        # rejection posted *after* a failed slash command would push the
        # cursor past the still-unprocessed original, permanently stranding
        # it. Skip without marking or advancing; the next poll's
        # `comments_since` will return it again, and we'll cheaply skip it
        # again until the failed command is retried and the cursor catches
        # up naturally. Non-self-authored comments (operator chatter,
        # external-thread mirrors, etc.) keep their pre-existing behavior of
        # marking-seen + advancing the cursor so the watermark moves
        # forward.
        if comment.author_is_me:
            return False
        async with self._comment_event_lock:
            if await db.comment_events.seen(self._conn, comment.id):
                return False
            try:
                await self._handle_slash_comments(issue_id, run_id, [comment])
            except SlashHandlerFailure as exc:
                log.warning(
                    "slash handler failed for comment %s on issue %s: %s",
                    comment.id,
                    issue_id,
                    exc.reason,
                )
                await self._post_command_rejected(issue_id, exc.slash_text, exc.reason)
                raise
            await db.comment_events.mark(
                self._conn,
                issue_id=issue_id,
                comment_id=comment.id,
                seen_at=comment.created_at,
            )
        await self._advance_comment_cursor(issue_id, comment.created_at, {comment.id})
        return True

    async def _handle_slash_comments(
        self, issue_id: str, run_id: str, comments: list[LinearComment]
    ) -> None:
        for intent in slash.parse(comments):
            await self._handle_slash_intent(issue_id, run_id, intent)

    async def _advance_comment_cursor(
        self, issue_id: str, latest: str, latest_ids: set[str]
    ) -> None:
        try:
            stored = await db.comment_cursors.get(self._conn, issue_id)
            if stored is not None:
                stored_at, stored_ids = stored
                stored_dt = _parse_rfc3339(stored_at)
                latest_dt = _parse_rfc3339(latest)
                if stored_dt > latest_dt:
                    return
                if stored_dt == latest_dt:
                    latest_ids |= set(stored_ids)
            await db.comment_cursors.set(self._conn, issue_id, latest, latest_ids)
        except Exception:  # noqa: BLE001
            log.exception("failed to persist comment cursor for %s", issue_id)

    async def _resolve_comment_cursor(
        self, issue_id: str, run_id: str
    ) -> tuple[datetime, set[str]]:
        """Resolve the cursor, clamped to the current run's `started_at`.

        Without the clamp, a stale slash comment posted between two runs on
        the same issue would still be `> stored_cursor` when the next run
        starts, and the first poll tick could immediately kill it even though
        the command was not intended for it.
        """
        if run_id.startswith(MANUAL_MERGE_PARKED_RUN_PREFIX):
            run_started = await self._manual_merge_parked_started_at(issue_id)
        else:
            run_started = await self._review_cap_wait_started_at(run_id) or (
                await self._run_started_at(run_id)
            )
        stored = await db.comment_cursors.get(self._conn, issue_id)
        if stored is None:
            return run_started, set()
        stored_at, stored_ids = stored
        stored_dt = _parse_rfc3339(stored_at)
        if stored_dt < run_started:
            return run_started, set()
        return stored_dt, set(stored_ids)

    async def _review_cap_wait_started_at(self, run_id: str) -> datetime | None:
        """Floor for a review-cap park: the park time, not the reused review run's start.

        A review-cap wait reuses the review run's own id (`run.id`), whose
        `started_at` is when that review iteration began — long before the
        park happened. Clamping to it would let comments from that entire
        review round (potentially the whole cap-hitting cycle) count as
        "new" on every poll. Clamp to the operator_wait's `created_at` (set
        at park time) instead (SYM-114 review).
        """
        wait = await db.operator_waits.get_by_run_id(self._conn, run_id)
        if wait is None or wait.kind != db.operator_waits.KIND_REVIEW_CAP:
            return None
        try:
            return _parse_rfc3339(wait.created_at)
        except ValueError:
            return None

    async def _run_started_at(self, run_id: str) -> datetime:
        cur = await self._conn.execute("SELECT started_at FROM runs WHERE id = ?", (run_id,))
        row = await cur.fetchone()
        if row is not None and row[0]:
            return _parse_rfc3339(row[0])
        return datetime(1970, 1, 1, tzinfo=UTC)

    async def _manual_merge_parked_started_at(self, issue_id: str) -> datetime:
        pr = await db.issue_prs.get_for_issue(self._conn, issue_id=issue_id)
        if pr is None or not pr.parked_at:
            return datetime(1970, 1, 1, tzinfo=UTC)
        try:
            return _parse_rfc3339(pr.parked_at)
        except ValueError:
            log.warning(
                "invalid parked_at timestamp for manual-merge PR %s#%d: %r",
                pr.github_repo,
                pr.pr_number,
                pr.parked_at,
            )
            return datetime(1970, 1, 1, tzinfo=UTC)

    async def _handle_slash_intent(self, issue_id: str, run_id: str, intent: SlashIntent) -> None:
        if run_id.startswith(MANUAL_MERGE_PARKED_RUN_PREFIX):
            await self._handle_parked_manual_merge_slash_intent(issue_id, intent)
            return
        if (
            run_id in self._operator_wait_run_ids
            and await db.operator_waits.get_by_run_id(self._conn, run_id) is None
        ):
            # The in-memory dicts below are only refreshed by `_restore_operator_waits`
            # at the top of a poll tick, so a wait cleared out-of-band mid-tick (e.g. by
            # the background reconciler auto-clearing a canceled issue) can leave a
            # stale run_id routable here even though its DB row is already gone. Evict
            # it and reject the command instead of acting on a wait that no longer
            # exists.
            await self._clear_operator_wait(issue_id, run_id)
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                "operator wait is no longer active",
            )
            return
        if run_id in self._implement_failed_run_bindings:
            await self._handle_implement_failed_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._implement_blocked_run_bindings:
            await self._handle_implement_blocked_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._deliver_failed_run_bindings:
            await self._handle_deliver_failed_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._review_failed_run_bindings:
            await self._handle_review_failed_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._merge_needs_approval_bindings:
            await self._handle_merge_needs_approval_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._acceptance_rejected_run_bindings:
            await self._handle_acceptance_rejected_slash_intent(issue_id, run_id, intent)
            return
        if run_id in self._budget_exceeded_run_bindings:
            await self._handle_budget_exceeded_slash_intent(issue_id, run_id, intent)
            return
        if await self._handle_persisted_wait_slash_intent(issue_id, run_id, intent):
            return
        if run_id in self._operator_wait_run_ids:
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                "operator wait is no longer active",
            )
            return
        if intent.kind is SlashKind.RETRY:
            monitor_run_id = self._review_poll_issue_ids.get(issue_id)
            if monitor_run_id is not None and monitor_run_id in self._review_poll_run_ids:
                await self._handle_active_review_retry_intent(issue_id, monitor_run_id, intent)
                return
        if intent.kind is SlashKind.STOP:
            monitor_run_id = self._review_poll_issue_ids.get(issue_id)
            if monitor_run_id is not None and monitor_run_id in self._review_poll_run_ids:
                await self._stop_review_monitor(issue_id, monitor_run_id)
                return
            log.info(
                "$stop received for run %s (issue %s) — terminating runner",
                run_id,
                issue_id,
            )
            try:
                await self._runner.kill(run_id)
            except Exception:  # noqa: BLE001
                log.exception("runner.kill failed for run %s", run_id)
                raise
            return
        if intent.kind is SlashKind.SKIP_REVIEW:
            await self._handle_skip_review_intent(issue_id, run_id)
            return
        if intent.kind in (
            SlashKind.RETRY_ACCEPTANCE,
            SlashKind.SKIP_ACCEPTANCE,
        ):
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                "no blocked acceptance wait is active",
            )
            return
        log.info(
            "slash %s received for run %s (handler not implemented in this slice)",
            intent.kind,
            run_id,
        )
        if intent.kind is SlashKind.RETRY:
            await self._post_command_rejected(
                issue_id,
                "$retry",
                "no active retry handler for the current run state",
            )

    async def _handle_persisted_wait_slash_intent(
        self,
        issue_id: str,
        run_id: str,
        intent: SlashIntent,
    ) -> bool:
        wait = await db.operator_waits.get_by_run_id(self._conn, run_id)
        if wait is None:
            return False
        if wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED:
            await self._handle_implement_failed_slash_intent(issue_id, run_id, intent)
        elif wait.kind == db.operator_waits.KIND_IMPLEMENT_BLOCKED:
            await self._handle_implement_blocked_slash_intent(issue_id, run_id, intent)
        elif wait.kind == db.operator_waits.KIND_DELIVER_FAILED:
            await self._handle_deliver_failed_slash_intent(issue_id, run_id, intent)
        elif wait.kind in (
            db.operator_waits.KIND_REVIEW_FAILED,
            db.operator_waits.KIND_REVIEW_STOPPED,
        ):
            await self._handle_review_failed_slash_intent(issue_id, run_id, intent)
        elif wait.kind in (
            db.operator_waits.KIND_MERGE,
            db.operator_waits.KIND_REVIEW_CAP,
        ):
            await self._handle_merge_needs_approval_slash_intent(issue_id, run_id, intent)
        elif wait.kind == db.operator_waits.KIND_ACCEPTANCE_BLOCKED:
            await self._handle_acceptance_blocked_slash_intent(issue_id, run_id, intent)
        elif wait.kind == db.operator_waits.KIND_ACCEPTANCE_REJECTED:
            await self._handle_acceptance_rejected_slash_intent(issue_id, run_id, intent)
        elif wait.kind == db.operator_waits.KIND_BUDGET_EXCEEDED:
            await self._handle_budget_exceeded_slash_intent(issue_id, run_id, intent)
        else:
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                f"unsupported operator wait kind: {wait.kind}",
            )
        return True

    @staticmethod
    def _slash_text(intent: SlashIntent) -> str:
        return f"${intent.kind.value}"

    async def _post_command_rejected(self, issue_id: str, slash_text: str, reason: str) -> None:
        tracker_issue_id, tracker_ctx = await self._tracker_identity_for_issue(issue_id)
        tracker = self.tracker(tracker_ctx)
        try:
            await tracker.post_comment(
                tracker_issue_id, truncate_body(command_rejected(slash_text, reason))
            )
        except LinearError as e:
            log.warning(
                "could not post %s rejection for %s: %s",
                slash_text,
                issue_id,
                e,
            )

    async def _accept_control_action(
        self,
        issue_id: str,
        action: controls.ControlAction,
        intent: SlashIntent,
        *,
        fingerprint: str | None = None,
    ) -> controls.ActionResult | None:
        """Put one operator command through the pipeline-control module.

        Returns the accepted result — the transition is committed, so the
        caller's side effects run behind a durable decision — or None when the
        action is rejected (not available in the current state, or this exact
        command was already applied), having told the operator why.

        Every stage's Retry and every validation stage's Skip comes through
        here, so which command an operator may run is decided once, from the
        durable state, rather than by whichever per-stage handler the command
        happened to route to (SYM-245).
        """
        result = await controls.apply(
            self._conn,
            issue_id,
            action,
            actor=self._control_actor(intent),
            action_id=intent.comment_id,
            at=self._now().isoformat(),
            fingerprint=fingerprint,
        )
        if result.accepted:
            return result
        log.info(
            "pipeline control rejected %s for issue %s: %s",
            action.value,
            issue_id,
            result.rejection,
        )
        await self._post_command_rejected(
            issue_id,
            self._slash_text(intent),
            result.rejection or f"{action.value} is not available right now",
        )
        return None

    @staticmethod
    def _control_actor(intent: SlashIntent) -> str:
        """Who asked: the comment author when known, else a synthetic
        `origin:id` (web-button commands carry a synthetic `web-` comment id
        and no author)."""
        if intent.author:
            return intent.author
        if intent.comment_id.startswith(WEB_COMMAND_COMMENT_PREFIX):
            return f"web:{intent.comment_id.removeprefix(WEB_COMMAND_COMMENT_PREFIX)}"
        return f"tracker:{intent.comment_id}"

    async def _handle_parked_manual_merge_slash_intent(
        self,
        issue_id: str,
        intent: SlashIntent,
        *,
        binding: RepoBinding | None = None,
        pr: db.issue_prs.IssuePR | None = None,
    ) -> None:
        if intent.kind is not SlashKind.APPROVE:
            log.info(
                "slash %s for parked manual-merge issue %s ignored",
                intent.kind,
                issue_id,
            )
            return
        if pr is None:
            pr = await db.issue_prs.get_for_issue(self._conn, issue_id=issue_id)
        if pr is None or pr.merged_at is not None or pr.parked_at is None:
            await self._post_command_rejected(
                issue_id,
                "$approve",
                "manual-merge parking marker is no longer active",
            )
            return
        if binding is None:
            binding = self._binding_for_pr(pr)
        if binding is None:
            await self._post_command_rejected(
                issue_id,
                "$approve",
                "no repository binding found for parked manual merge",
            )
            return
        tracker = self.tracker(binding)
        try:
            gh = await self._gh_client()
            await gh.pr_merge(
                pr.pr_number,
                strategy=binding.merge_strategy,
                auto=False,
                repo=binding.github_repo,
            )
        except GitHubError as e:
            log.warning(
                "manual merge failed for parked PR %s#%d on %s: %s",
                binding.github_repo,
                pr.pr_number,
                pr.identifier,
                e,
            )
            body = (
                f"manual merge failed for {pr.pr_url}: {e}\n\n"
                "The issue remains parked; reply with `$approve` to try again."
            )
            try:
                await tracker.post_comment(issue_id, truncate_body(body))
            except LinearError as comment_error:
                log.warning(
                    "could not post manual merge failure for %s: %s",
                    pr.identifier,
                    comment_error,
                )

    async def _handle_implement_failed_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._implement_failed_run_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(db.operator_waits.KIND_IMPLEMENT_FAILED,),
            )
            if binding is None:
                return

        tracker_issue_id, _ = await self._tracker_identity_for_issue(issue_id)
        tracker = self.tracker(binding)
        states = await self._states_for_binding(binding)
        if intent.kind in (SlashKind.APPROVE, SlashKind.RETRY):
            ready_id = states.get(binding.linear_states.ready)
            if ready_id is None:
                log.warning(
                    "could not retry failed implement run %s: missing ready state %r",
                    run_id,
                    binding.linear_states.ready,
                )
                return
            # The control module decides — and durably records — whether this
            # retry happens, before anything outside the DB moves (SYM-244).
            # A command that is no longer valid, or one already applied, stops
            # here and can never start a second implement attempt. `$approve`
            # on this park is the same transition, so it goes through the same
            # gate. The workspace and its committed checkpoint are untouched:
            # the fresh attempt picks them up on the next dispatch; process and
            # session state are deliberately not resumed.
            accepted = await self._accept_control_action(
                issue_id, controls.ControlAction.RETRY, intent
            )
            if accepted is None:
                return
            try:
                await tracker.move_issue(tracker_issue_id, ready_id)
            except LinearError as e:
                # Nothing carried the transition out; release it so the next
                # poll tick can re-deliver this same command.
                await controls.release(self._conn, accepted, at=self._now().isoformat())
                log.warning("could not move %s to ready for retry: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not move issue to ready state for retry: {e}",
                ) from e
            except BaseException:
                # Any other failure (malformed payload, cancellation, daemon
                # death) also means the park isn't safely resumed; release it
                # the same way so the park stays retryable, then propagate.
                await controls.release(self._conn, accepted, at=self._now().isoformat())
                raise
            # The issue has already moved to ready, but the transition is only
            # truly un-releasable once `_clear_operator_wait` below commits:
            # until then the park is still open and dispatch is still blocked
            # by `_dispatch_run_ids` (set when the park was created), so a
            # failure anywhere before that call still needs to release the
            # transition for the next poll tick to re-deliver the same
            # `$retry`. `_clear_operator_wait` itself pops `_dispatch_run_ids`/
            # `_operator_wait_run_ids`/`_implement_failed_run_bindings` before
            # its own DB delete, so a failure inside it leaves the in-memory
            # gate already open with the wait row still present (SYM-244
            # review) — its `except` below re-arms that tracking before
            # releasing so the gate stays shut until the wait is actually gone.
            body = resumed(
                CommentVars(
                    stage="implement",
                    repo=binding.github_repo,
                    issue=0,
                    run_id=run_id,
                    next_stage=binding.linear_states.ready,
                )
            )
            try:
                await tracker.post_comment(tracker_issue_id, truncate_body(body))
            except LinearError as e:
                log.warning("implement retry comment failed for issue %s: %s", issue_id, e)
            except BaseException:
                await controls.release(self._conn, accepted, at=self._now().isoformat())
                raise
            try:
                await self._clear_operator_wait(issue_id, run_id)
            except BaseException:
                # `_clear_operator_wait` already popped the dispatch gate for
                # this run before its DB delete failed; re-arm it so a
                # webhook-driven dispatch can't slip in and start a second
                # implement attempt before the released park is restored.
                self._dispatch_run_ids[issue_id] = run_id
                self._operator_wait_run_ids.add(run_id)
                self._implement_failed_run_bindings[run_id] = binding
                await controls.release(self._conn, accepted, at=self._now().isoformat())
                raise
            return

        if intent.kind in (SlashKind.REJECT, SlashKind.STOP):
            blocked_id = states.get(binding.linear_states.blocked)
            if blocked_id is None:
                log.warning(
                    "could not stop failed implement run %s: missing blocked state %r",
                    run_id,
                    binding.linear_states.blocked,
                )
                try:
                    await tracker.post_comment(
                        tracker_issue_id,
                        truncate_body(
                            command_rejected(
                                f"${intent.kind}",
                                "missing blocked state; keeping issue parked",
                            )
                        ),
                    )
                except LinearError as e:
                    log.warning(
                        "implement stop rejection comment failed for %s: %s",
                        issue_id,
                        e,
                    )
                return
            try:
                await tracker.move_issue(tracker_issue_id, blocked_id)
            except LinearError as e:
                log.warning("could not move %s to blocked: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not move issue to blocked state: {e}",
                ) from e
            # The park is going away with no retry behind it: settle the
            # control row to a terminal, non-retryable outcome in the same
            # transaction as the wait delete, so `controls.snapshot` never
            # keeps advertising Retry for a blocked issue with nothing parked
            # to retry (SYM-244 review).
            await self._clear_stage_park(
                issue_id,
                run_id,
                kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            )
            return

        log.info(
            "slash %s received for failed implement run %s (ignored)",
            intent.kind,
            run_id,
        )

    async def _handle_implement_blocked_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._implement_blocked_run_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(db.operator_waits.KIND_IMPLEMENT_BLOCKED,),
            )
            if binding is None:
                return

        tracker_issue_id, _ = await self._tracker_identity_for_issue(issue_id)
        if intent.kind in (SlashKind.APPROVE, SlashKind.RETRY):
            await self._resume_blocked_implement(
                issue_id, run_id, intent, binding, tracker_issue_id
            )
            return

        if intent.kind in (SlashKind.REJECT, SlashKind.STOP):
            await self._stop_blocked_implement(issue_id, run_id, intent, binding, tracker_issue_id)
            return

        log.info(
            "slash %s received for blocked implement run %s (ignored)",
            intent.kind,
            run_id,
        )

    async def _resume_blocked_implement(
        self,
        issue_id: str,
        run_id: str,
        intent: SlashIntent,
        binding: RepoBinding,
        tracker_issue_id: str,
    ) -> None:
        tracker = self.tracker(binding)
        states = await self._states_for_binding(binding)
        ready_id = states.get(binding.linear_states.ready)
        if ready_id is None:
            log.warning(
                "could not resume blocked implement run %s: missing ready state %r",
                run_id,
                binding.linear_states.ready,
            )
            return
        # Same canonical Retry the implement-failed park uses: the decision is
        # recorded durably before anything outside the DB moves, so a stale or
        # re-delivered command can never start a second attempt (SYM-245).
        accepted = await self._accept_control_action(issue_id, controls.ControlAction.RETRY, intent)
        if accepted is None:
            return
        # The verbatim blocked reason survives as handoff context, but the
        # operator's command text does not: Retry carries no instruction
        # payload. The fresh attempt re-reads the issue from the tracker
        # instead, so whatever the operator wants it to know belongs in the
        # description or a comment (SYM-245).
        self._implement_handoffs[issue_id] = _ImplementHandoff(
            blocked_reason=await self._blocked_reason_for_run(run_id),
        )
        try:
            await tracker.move_issue(tracker_issue_id, ready_id)
        except LinearError as e:
            self._implement_handoffs.pop(issue_id, None)
            await controls.release(self._conn, accepted, at=self._now().isoformat())
            log.warning("could not move %s to ready for resume: %s", issue_id, e)
            raise SlashHandlerFailure(
                slash_text=self._slash_text(intent),
                reason=f"could not move issue to ready state for resume: {e}",
            ) from e
        except BaseException:
            # Any other failure (malformed payload, cancellation, daemon death)
            # also leaves the park unresumed; release so the next poll tick can
            # re-deliver this same command.
            self._implement_handoffs.pop(issue_id, None)
            await controls.release(self._conn, accepted, at=self._now().isoformat())
            raise
        body = resumed(
            CommentVars(
                stage="implement",
                repo=binding.github_repo,
                issue=0,
                run_id=run_id,
                next_stage=binding.linear_states.ready,
            )
        )
        try:
            await tracker.post_comment(tracker_issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("implement resume comment failed for issue %s: %s", issue_id, e)
        try:
            await self._clear_operator_wait(issue_id, run_id)
        except BaseException:
            # `_clear_operator_wait` already popped the dispatch gate and this
            # run's binding before its own DB delete failed; re-arm both so a
            # webhook-driven dispatch can't slip in and start a second
            # implement attempt before the released park is restored (mirrors
            # the implement-failed retry above).
            self._dispatch_run_ids[issue_id] = run_id
            self._operator_wait_run_ids.add(run_id)
            self._implement_blocked_run_bindings[run_id] = binding
            await controls.release(self._conn, accepted, at=self._now().isoformat())
            raise

    async def _stop_blocked_implement(
        self,
        issue_id: str,
        run_id: str,
        intent: SlashIntent,
        binding: RepoBinding,
        tracker_issue_id: str,
    ) -> None:
        tracker = self.tracker(binding)
        states = await self._states_for_binding(binding)
        blocked_id = states.get(binding.linear_states.blocked)
        if blocked_id is None:
            log.warning(
                "could not stop blocked implement run %s: missing blocked state %r",
                run_id,
                binding.linear_states.blocked,
            )
            try:
                await tracker.post_comment(
                    tracker_issue_id,
                    truncate_body(
                        command_rejected(
                            f"${intent.kind}",
                            "missing blocked state; keeping issue parked",
                        )
                    ),
                )
            except LinearError as e:
                log.warning(
                    "implement stop rejection comment failed for %s: %s",
                    issue_id,
                    e,
                )
            return
        try:
            await tracker.move_issue(tracker_issue_id, blocked_id)
        except LinearError as e:
            log.warning("could not move %s to blocked: %s", issue_id, e)
            raise SlashHandlerFailure(
                slash_text=self._slash_text(intent),
                reason=f"could not move issue to blocked state: {e}",
            ) from e
        await self._clear_stage_park(
            issue_id,
            run_id,
            kind=db.operator_waits.KIND_IMPLEMENT_BLOCKED,
        )

    async def _handle_acceptance_blocked_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = await self._restore_operator_wait_binding(
            issue_id,
            run_id,
            intent,
            expected_kinds=(db.operator_waits.KIND_ACCEPTANCE_BLOCKED,),
        )
        if binding is None:
            return

        tracker_issue_id, _ = await self._tracker_identity_for_issue(issue_id)
        tracker = self.tracker(binding)
        state = await db.acceptance_state.get(self._conn, issue_id)
        if intent.kind is SlashKind.RETRY_ACCEPTANCE:
            pr_url = state.pr_url or (
                f"https://github.com/{binding.github_repo}/pull/{state.pr_number}"
                if state.pr_number is not None
                else "(no PR yet)"
            )
            states = await self._states_for_binding(binding)
            active_state_names = (
                binding.linear_states.needs_approval,
                binding.linear_states.in_acceptance,
                binding.linear_states.in_progress,
            )
            target_state_name = next(
                (name for name in dict.fromkeys(active_state_names) if states.get(name)),
                None,
            )
            if target_state_name is None:
                log.warning(
                    "could not retry blocked acceptance run %s: missing active state",
                    run_id,
                )
                await self._post_command_rejected(
                    issue_id,
                    self._slash_text(intent),
                    "missing active Linear state; keeping acceptance blocked",
                )
                return
            target_state_id = states[target_state_name]
            accepted = await self._accept_control_action(
                issue_id, controls.ControlAction.RETRY, intent
            )
            if accepted is None:
                return
            try:
                await tracker.move_issue(tracker_issue_id, target_state_id)
            except LinearError as e:
                await controls.release(self._conn, accepted, at=self._now().isoformat())
                log.warning(
                    "could not move %s to %s for acceptance retry: %s",
                    issue_id,
                    target_state_name,
                    e,
                )
                await self._post_command_rejected(
                    issue_id,
                    self._slash_text(intent),
                    "could not move issue to an active Linear state; keeping acceptance blocked",
                )
                return
            try:
                await db.acceptance_state.reset(self._conn, issue_id)
                await self._clear_operator_wait(issue_id, run_id)
            except BaseException:
                # `_clear_operator_wait` pops the dispatch gate for this run
                # before its own DB delete can fail; re-arm it so a
                # webhook-driven dispatch can't slip in before the released
                # park is restored (mirrors the implement-failed retry above).
                self._dispatch_run_ids[issue_id] = run_id
                self._operator_wait_run_ids.add(run_id)
                await controls.release(self._conn, accepted, at=self._now().isoformat())
                raise
            body = acceptance_retry_requested(
                CommentVars(
                    stage="acceptance",
                    repo=binding.github_repo,
                    issue=state.pr_number or 0,
                    pr_url=pr_url,
                    run_id=run_id,
                )
            )
            try:
                await tracker.post_comment(tracker_issue_id, truncate_body(body))
            except LinearError as e:
                log.warning("acceptance retry comment failed for %s: %s", issue_id, e)
            return

        if intent.kind is SlashKind.SKIP_ACCEPTANCE:
            if state.pr_number is None:
                await self._post_command_rejected(
                    issue_id,
                    self._slash_text(intent),
                    "no PR found for blocked acceptance",
                )
                return
            try:
                issue = await tracker.lookup_issue(tracker_issue_id)
            except LinearError as e:
                log.warning("could not look up %s for skip-acceptance: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not look up issue for skip-acceptance: {e}",
                ) from e

            # Acceptance is a validation stage, so Skip is on offer — scoped to
            # the PR head it approves, so a later push requires acceptance again
            # (SYM-245).
            # Captured before the accept/clear below can touch the row, so a
            # release can put the exact same durable wait back (SYM-245 review).
            wait = await db.operator_waits.get_by_run_id(self._conn, run_id)
            accepted = await self._accept_control_action(
                issue_id,
                controls.ControlAction.SKIP,
                intent,
                fingerprint=state.pr_head_sha or None,
            )
            if accepted is None:
                return
            try:
                await db.acceptance_state.record_verdict(
                    self._conn,
                    issue_id,
                    verdict="pass",
                    artifacts_url=state.last_artifacts_url,
                )
                await self._clear_operator_wait(issue_id, run_id)
                if _needs_human_approval_label_present(issue):
                    await self._open_merge_wait_for_human_approval_label(
                        binding=binding,
                        issue=issue,
                        pr_url=state.pr_url,
                    )
                else:
                    # Reserve while holding `config_write_lock` so the drain
                    # guard's `scheduled_slots` sample can't miss this
                    # reservation (SYM-193 review; see `_review_fix_dispatch_slot`
                    # in `_dispatch.py`).
                    async with self._config_write_lock:
                        self._schedule_merge(
                            binding=binding,
                            issue=issue,
                            pr_number=state.pr_number,
                            pr_url=state.pr_url,
                        )
            except BaseException:
                # Nothing carried the accepted Skip through to a scheduled
                # merge; release it so the next poll tick can re-deliver the
                # same `$skip-acceptance` instead of leaving a `SKIPPED` park
                # that offers neither Retry nor Skip (SYM-245 review). Re-arm
                # the dispatch gate and re-insert the durable wait row too —
                # `_clear_operator_wait` already dropped both, and this park
                # has no dedicated binding dict of its own, so
                # `_restore_operator_wait_binding`'s next lookup depends
                # entirely on the durable row being back (SYM-245 review).
                self._dispatch_run_ids[issue_id] = run_id
                self._operator_wait_run_ids.add(run_id)
                if wait is not None:
                    await self._reopen_operator_wait(wait)
                await controls.release(self._conn, accepted, at=self._now().isoformat())
                raise
            body = acceptance_skipped(
                CommentVars(
                    stage="acceptance",
                    repo=binding.github_repo,
                    issue=state.pr_number,
                    pr_url=state.pr_url,
                    run_id=run_id,
                    next_stage="merge",
                )
            )
            try:
                await tracker.post_comment(tracker_issue_id, truncate_body(body))
            except LinearError as e:
                log.warning("acceptance skip comment failed for %s: %s", issue_id, e)
            return

        await self._post_command_rejected(
            issue_id,
            self._slash_text(intent),
            "acceptance is blocked; use $retry-acceptance or $skip-acceptance",
        )

    async def _handle_budget_exceeded_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._budget_exceeded_run_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(db.operator_waits.KIND_BUDGET_EXCEEDED,),
            )
            if binding is None:
                return

        tracker_issue_id, _ = await self._tracker_identity_for_issue(issue_id)
        tracker = self.tracker(binding)
        states = await self._states_for_binding(binding)
        if intent.kind in (SlashKind.APPROVE, SlashKind.RETRY):
            # Grant one more budget window (repeatable). Raising the ceiling
            # before re-dispatch keeps the guard from immediately re-parking.
            budget = binding.resolved_per_issue_token_budget(self.config.per_issue_token_budget)
            if budget is not None:
                await db.issues.add_granted_token_budget(self._conn, issue_id, budget)
            # If the parked boundary already has an open PR (any review/merge
            # boundary), re-dispatch the review monitor directly. Routing
            # through the ready scan would hit `_blocking_existing_pr` /
            # `_park_already_has_pr`, which bounces an open-PR issue to In
            # Progress — review never resumes and the granted window is wasted.
            pr = await db.issue_prs.get(
                self._conn,
                issue_id=issue_id,
                github_repo=binding.github_repo,
            )
            if pr is not None and pr.merged_at is None:
                # Look up the issue BEFORE clearing the wait (in the helper) so
                # a lookup failure leaves the wait intact for the next tick.
                try:
                    issue = await tracker.lookup_issue(tracker_issue_id)
                except LinearError as e:
                    log.warning("could not look up %s for resume: %s", issue_id, e)
                    raise SlashHandlerFailure(
                        slash_text=self._slash_text(intent),
                        reason=f"could not look up issue for resume: {e}",
                    ) from e
                await self._resume_review_monitor(
                    binding=binding,
                    issue=issue,
                    issue_id=issue_id,
                    tracker_issue_id=tracker_issue_id,
                    run_id=run_id,
                )
                return
            # No open PR yet (implement boundary): the ready scan dispatches
            # implement — the existing-PR guard is a no-op without a PR.
            ready_id = states.get(binding.linear_states.ready)
            if ready_id is None:
                log.warning(
                    "could not resume budget-parked run %s: missing ready state %r",
                    run_id,
                    binding.linear_states.ready,
                )
                return
            try:
                await tracker.move_issue(tracker_issue_id, ready_id)
            except LinearError as e:
                log.warning("could not move %s to ready for resume: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not move issue to ready state for resume: {e}",
                ) from e
            body = resumed(
                CommentVars(
                    stage="implement",
                    repo=binding.github_repo,
                    issue=0,
                    run_id=run_id,
                    next_stage=binding.linear_states.ready,
                )
            )
            try:
                await tracker.post_comment(tracker_issue_id, truncate_body(body))
            except LinearError as e:
                log.warning("budget resume comment failed for %s: %s", issue_id, e)
            await self._clear_operator_wait(issue_id, run_id)
            return

        if intent.kind in (SlashKind.REJECT, SlashKind.STOP):
            blocked_id = states.get(binding.linear_states.blocked)
            if blocked_id is None:
                log.warning(
                    "could not block budget-parked run %s: missing blocked state %r",
                    run_id,
                    binding.linear_states.blocked,
                )
                await self._post_command_rejected(
                    issue_id,
                    self._slash_text(intent),
                    "missing blocked state; keeping issue parked",
                )
                return
            try:
                await tracker.move_issue(tracker_issue_id, blocked_id)
            except LinearError as e:
                log.warning("could not move %s to blocked: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not move issue to blocked state: {e}",
                ) from e
            await self._clear_operator_wait(issue_id, run_id)
            return

        log.info(
            "slash %s received for budget-parked run %s (ignored)",
            intent.kind,
            run_id,
        )

    async def _handle_acceptance_rejected_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._acceptance_rejected_run_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(db.operator_waits.KIND_ACCEPTANCE_REJECTED,),
            )
            if binding is None:
                return
        tracker = self.tracker(binding)

        if intent.kind not in (
            SlashKind.SKIP_ACCEPTANCE,
            SlashKind.RETRY_ACCEPTANCE,
        ):
            log.info(
                "slash %s for acceptance-rejected run %s ignored",
                intent.kind,
                run_id,
            )
            return

        state = await db.acceptance_state.get(self._conn, issue_id)
        if state.pr_number is None:
            await self._post_command_rejected(
                issue_id,
                self._slash_text(intent),
                "no PR found for acceptance state",
            )
            return

        try:
            issue = await tracker.lookup_issue(issue_id)
        except LinearError as e:
            log.warning("could not look up %s for acceptance slash: %s", issue_id, e)
            raise SlashHandlerFailure(
                slash_text=self._slash_text(intent),
                reason=f"could not look up issue for acceptance slash: {e}",
            ) from e

        pr_number = state.pr_number
        pr_url = _pr_url_for_state(
            repo=binding.github_repo,
            pr_number=pr_number,
            pr_url=state.pr_url,
        )

        if intent.kind is SlashKind.SKIP_ACCEPTANCE:
            # See the acceptance-blocked branch: one canonical, head-scoped Skip
            # for the acceptance stage, whichever park it is answered from.
            # Captured before the accept/clear below can touch the row, so a
            # release can put the exact same durable wait back (SYM-245 review).
            wait = await db.operator_waits.get_by_run_id(self._conn, run_id)
            accepted = await self._accept_control_action(
                issue_id,
                controls.ControlAction.SKIP,
                intent,
                fingerprint=state.pr_head_sha or None,
            )
            if accepted is None:
                return
            try:
                await db.acceptance_state.record_verdict(
                    self._conn,
                    issue_id,
                    verdict="pass",
                    artifacts_url=state.last_artifacts_url,
                )
                await self._clear_operator_wait(issue_id, run_id)
                if _needs_human_approval_label_present(issue):
                    await self._open_merge_wait_for_human_approval_label(
                        binding=binding,
                        issue=issue,
                        pr_url=pr_url,
                    )
                else:
                    # See the sibling `SKIP_ACCEPTANCE` branch above for why this
                    # reservation is made under `config_write_lock` (SYM-193
                    # review).
                    async with self._config_write_lock:
                        self._schedule_merge(
                            binding=binding,
                            issue=issue,
                            pr_number=pr_number,
                            pr_url=pr_url,
                        )
            except BaseException:
                # `_clear_operator_wait` pops `_dispatch_run_ids`/
                # `_operator_wait_run_ids`/`_acceptance_rejected_run_bindings`
                # before its own DB delete can fail; re-arm them, and
                # re-insert the durable wait row too — the in-memory re-arm
                # alone leaves the stale-wait guard and
                # `_restore_operator_wait_binding` with no row to authorize a
                # re-delivered command against (SYM-245 review), so a
                # webhook-driven dispatch can't slip in before the released
                # park is restored.
                self._dispatch_run_ids[issue_id] = run_id
                self._operator_wait_run_ids.add(run_id)
                self._acceptance_rejected_run_bindings[run_id] = binding
                if wait is not None:
                    await self._reopen_operator_wait(wait)
                await controls.release(self._conn, accepted, at=self._now().isoformat())
                raise
            body = skip_acceptance_forced(
                CommentVars(
                    stage="acceptance",
                    repo=binding.github_repo,
                    issue=pr_number,
                    pr_url=pr_url,
                    run_id=run_id,
                    next_stage="merge",
                )
            )
            try:
                await tracker.post_comment(issue_id, truncate_body(body))
            except LinearError as e:
                log.warning("skip-acceptance comment failed for %s: %s", issue_id, e)
            return

        # Captured before the accept/clear below can touch the row, so a
        # release can put the exact same durable wait back (SYM-245 review).
        wait = await db.operator_waits.get_by_run_id(self._conn, run_id)
        accepted = await self._accept_control_action(
            issue_id, controls.ControlAction.RETRY, intent
        )
        if accepted is None:
            return
        try:
            await self._clear_operator_wait(issue_id, run_id)
            await db.acceptance_state.reset(self._conn, issue_id)
            # See the `SKIP_ACCEPTANCE` branch above for why this reservation is
            # made under `config_write_lock` (SYM-193 review).
            async with self._config_write_lock:
                self._schedule_acceptance(
                    binding=binding,
                    issue=issue,
                    pr_number=pr_number,
                    pr_url=pr_url,
                    pr_head_sha=state.pr_head_sha,
                )
        except BaseException:
            # `_clear_operator_wait` pops `_dispatch_run_ids`/
            # `_operator_wait_run_ids`/`_acceptance_rejected_run_bindings`
            # before its own DB delete can fail; re-arm them, and re-insert
            # the durable wait row too — the in-memory re-arm alone leaves the
            # stale-wait guard and `_restore_operator_wait_binding` with no
            # row to authorize a re-delivered command against (SYM-245
            # review), so a webhook-driven dispatch can't slip in before the
            # released park is restored.
            self._dispatch_run_ids[issue_id] = run_id
            self._operator_wait_run_ids.add(run_id)
            self._acceptance_rejected_run_bindings[run_id] = binding
            if wait is not None:
                await self._reopen_operator_wait(wait)
            await controls.release(self._conn, accepted, at=self._now().isoformat())
            raise
        body = retry_acceptance_requested(
            CommentVars(
                stage="acceptance",
                repo=binding.github_repo,
                issue=pr_number,
                pr_url=pr_url,
                run_id=run_id,
                next_stage="acceptance",
            )
        )
        try:
            await tracker.post_comment(issue_id, truncate_body(body))
        except LinearError as e:
            log.warning("retry-acceptance comment failed for %s: %s", issue_id, e)

    async def _handle_deliver_failed_slash_intent(
        self, issue_id: str, run_id: str, intent: SlashIntent
    ) -> None:
        binding = self._deliver_failed_run_bindings.get(run_id)
        if binding is None:
            binding = await self._restore_operator_wait_binding(
                issue_id,
                run_id,
                intent,
                expected_kinds=(db.operator_waits.KIND_DELIVER_FAILED,),
            )
            if binding is None:
                return

        if intent.kind in (SlashKind.REJECT, SlashKind.STOP):
            states = await self._states_for_binding(binding)
            blocked_id = states.get(binding.linear_states.blocked)
            tracker_issue_id, _ = await self._tracker_identity_for_issue(issue_id)
            tracker = self.tracker(binding)
            if blocked_id is None:
                try:
                    await tracker.post_comment(
                        tracker_issue_id,
                        truncate_body(
                            command_rejected(
                                f"${intent.kind}",
                                "missing blocked state; keeping issue parked",
                            )
                        ),
                    )
                except LinearError as e:
                    log.warning(
                        "deliver_failed stop rejection comment failed for %s: %s",
                        issue_id,
                        e,
                    )
                return
            try:
                await tracker.move_issue(tracker_issue_id, blocked_id)
            except LinearError as e:
                log.warning("could not move %s to blocked: %s", issue_id, e)
                raise SlashHandlerFailure(
                    slash_text=self._slash_text(intent),
                    reason=f"could not move issue to blocked state: {e}",
                ) from e
            await self._terminate_deliver_failed_review_monitors(
                issue_id,
                detail=f"${intent.kind} halted deliver_failed delivery wait",
            )
            self._pending_deliveries.pop(run_id, None)
            await self._clear_stage_park(
                issue_id,
                run_id,
                kind=db.operator_waits.KIND_DELIVER_FAILED,
            )
            return

        if intent.kind not in (SlashKind.APPROVE, SlashKind.RETRY):
            log.info(
                "slash %s received for deliver_failed run %s (ignored)",
                intent.kind,
                run_id,
            )
            return

        ctx = await self._resolve_pending_delivery(issue_id, run_id, binding, intent)
        if ctx is None:
            return
        # Delivery is a mandatory stage: Retry is the only canonical action for
        # it, and it goes through the same durable gate as every other stage's
        # (SYM-245).
        accepted = await self._accept_control_action(issue_id, controls.ControlAction.RETRY, intent)
        if accepted is None:
            return
        # The accepted transition already committed the control row to
        # `pending`, dropping Retry from `allowed_actions` — the operator wait
        # row alone no longer gives the issue a retryable home. Each of
        # `_deliver_implement_run`'s own `_park_deliver_failed` handlers
        # re-parks the stage as `failed` on the failure it catches, but an
        # exception escaping all of them needs this release, or the row is
        # stuck `pending` with nothing able to answer it until a daemon
        # restart's `reconcile_interrupted_retries` (SYM-245 review).
        self._pending_deliveries.pop(run_id, None)
        try:
            try:
                await self._deliver_implement_run(ctx=ctx)
            except BaseException:
                await controls.release(self._conn, accepted, at=self._now().isoformat())
                raise
            run = await db.runs.get_with_issue(self._conn, run_id)
            if run is not None and run.run.status in db.runs.SUCCESS_STATUSES:
                await self._clear_operator_wait(issue_id, run_id)
        finally:
            if ctx.retry_workspace_acquired:
                self._workspace.release(ctx.binding, ctx.issue)
