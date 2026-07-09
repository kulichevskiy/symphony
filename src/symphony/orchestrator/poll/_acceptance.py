"""`_AcceptanceMixin` — the acceptance-stage domain of the poll loop (SYM-148).

Pure structural extraction from the pre-split `Orchestrator`: every acceptance
method (run/dispatch/post-comment/upload/track/handle/schedule helpers) plus the
acceptance-only module-level helpers move here verbatim. `Orchestrator` (in
`__init__.py`) inherits this mixin, which itself inherits `_OrchestratorBase`
for the shared state + foundation methods.

Method bodies are byte-for-byte unchanged. Cross-domain methods the acceptance
flow calls (merge/slash/dispatch infrastructure) stay on `Orchestrator`; they
are declared here under `TYPE_CHECKING` only so `mypy --strict` resolves the
`self.<method>` calls while the real implementations are inherited at runtime.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, replace
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from ... import db
from ...agent.prompt import acceptance_fix_prompt
from ...agent.runners.acceptance import quick_skip_trivial_acceptance, run_acceptance
from ...config import RepoBinding
from ...github.client import GitHubError
from ...linear.client import LinearError
from ...linear.slash import SlashIntent
from ...linear.templates import (
    CommentVars,
    acceptance_blocked,
    acceptance_rejected,
    truncate_body,
)
from ...notify import EVENT_OPERATOR_WAIT
from ...pipeline.acceptance_classifier import (
    AcceptanceScreenshot,
    AcceptanceVerdict,
    ExtractedCriterion,
    extract_acceptance_criteria,
    format_acceptance_criteria_comment,
    format_acceptance_verdict_comment,
)
from ...pipeline.cost_guard import UsageDelta
from ...pipeline.preview_resolver import (
    PreviewResolutionError,
    render_preview_url,
    resolve_preview_url,
)
from ...pipeline.state_machine import on_runner_event
from ...pipeline.taste_guide import load_taste_guide
from ...tracker import (
    Issue as LinearIssue,
)
from ._base import _binding_key, _infra_retry_backoff_secs, _OrchestratorBase
from ._git import (
    _git_fetch_branch,
    _git_status_short,
    _workspace_head_sha,
    _workspace_ref_sha,
)
from ._helpers import (
    _acceptance_degrade_note,
    _add_run_usage,
    _needs_human_approval_label_present,
    _parse_rfc3339,
    _termination_kwargs,
    build_fix_runner_command,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ...agent.codex_models import DEFAULT_CODEX_MODEL
    from ._base import BindingKey

log = logging.getLogger(__name__)

_CODE_ONLY_ACCEPTANCE_MODE = "code_only"
ACCEPTANCE_INFRA_RETRY_LIMIT = 2
ACCEPTANCE_FIX_ITERATION_CAP = 1


class _AcceptancePrDiffUnavailable(RuntimeError):
    pass


def _with_acceptance_degrade_note(
    verdict: AcceptanceVerdict, degrade_note: str | None
) -> AcceptanceVerdict:
    if not degrade_note:
        return verdict
    details = verdict.details.strip()
    if details.startswith(degrade_note):
        return verdict
    combined = degrade_note if not details else f"{degrade_note}\n\n{details}"
    return replace(verdict, details=combined)


def _acceptance_criterion_names(criteria: list[ExtractedCriterion]) -> list[str]:
    return [item["name"] for item in criteria if item["name"].strip()]


def _acceptance_criterion_predicates(criteria: list[ExtractedCriterion]) -> list[str]:
    return [item["predicate"] for item in criteria if item["predicate"].strip()]


def _replace_acceptance_criteria_labels(
    *,
    verdict: AcceptanceVerdict,
    criteria_names: list[str],
    criteria_predicates: list[str],
) -> AcceptanceVerdict:
    labels = dict(zip(criteria_predicates, criteria_names, strict=False))
    criterion_results = tuple(
        replace(
            item,
            criterion=labels.get(item.criterion, item.criterion),
        )
        for item in verdict.criterion_results
    )
    screenshots = tuple(
        replace(
            item,
            label=labels.get(item.label, item.label),
        )
        for item in verdict.screenshots
    )
    return replace(
        verdict,
        criteria=criteria_names,
        criterion_results=criterion_results,
        screenshots=screenshots,
    )


def _acceptance_artifact_path(workspace_path: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = workspace_path / path
    try:
        resolved = path.resolve(strict=False)
        workspace = workspace_path.resolve(strict=False)
    except RuntimeError as e:
        raise OSError(f"acceptance artifact path cannot be resolved: {raw_path}") from e
    try:
        resolved.relative_to(workspace)
    except ValueError as e:
        raise OSError(f"acceptance artifact path escapes workspace: {raw_path}") from e
    return resolved


@dataclass(frozen=True)
class _AcceptancePrep:
    """Inputs computed by `_begin_acceptance_run` and consumed by the verdict step."""

    effective_mode: str
    degrade_note: str | None
    preview_url: str
    preview_resolution_error: str
    criteria_names: list[str]
    criteria_predicates: list[str]


class _AcceptanceMixin(_OrchestratorBase):
    if TYPE_CHECKING:
        # Cross-domain methods inherited from `Orchestrator` at runtime; declared
        # here so `mypy --strict` resolves the acceptance flow's `self.<m>` calls.
        async def _clear_operator_wait(self, issue_id: str, run_id: str) -> None: ...
        async def _complete_review_monitors_for_merge(self, issue: LinearIssue) -> None: ...
        def _dispatch_task_done(
            self, task: asyncio.Task[None], issue_id: str, binding_key: BindingKey
        ) -> None: ...
        async def _fail_run(
            self,
            run_id: str,
            reason: str,
            *,
            final_kind: str | None = None,
            returncode: int | None = None,
            exc: BaseException | str | None = None,
            termination_kind: str | None = None,
            termination_detail: str | None = None,
        ) -> None: ...
        async def _merge_approved_pr(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            pr_number: int,
            pr_url: str,
            approved_head_sha: str = "",
            skip_review: bool = False,
            on_started: Callable[[str], Awaitable[None]] | None = None,
        ) -> str | None: ...
        async def _open_merge_wait_for_human_approval_label(
            self,
            *,
            binding: RepoBinding,
            issue: LinearIssue,
            pr_url: str,
        ) -> None: ...
        async def _post_command_rejected(
            self, issue_id: str, slash_text: str, reason: str
        ) -> None: ...
        async def _refresh_issue_for_acceptance_merge_handoff(
            self, binding: RepoBinding, issue: LinearIssue
        ) -> LinearIssue: ...
        async def _refresh_merge_candidate(
            self,
            binding: RepoBinding,
            issue: LinearIssue,
        ) -> LinearIssue | None: ...
        def _reserve_scheduled_slot(self, *, issue_id: str, binding_key: BindingKey) -> None: ...
        async def _restore_operator_wait_binding(
            self,
            issue_id: str,
            run_id: str,
            intent: SlashIntent,
            *,
            expected_kinds: tuple[str, ...],
        ) -> RepoBinding | None: ...
        async def _run_runner(
            self,
            *,
            run_id: str,
            workspace_path: Path,
            command: list[str],
            stage: str,
            agent: str,
            binding: RepoBinding,
            issue: LinearIssue,
            codex_model: str = DEFAULT_CODEX_MODEL,
            activity_stage: str | None = None,
            prior_total: float = 0.0,
            clear_pid_on_finish: bool = False,
        ) -> tuple[UsageDelta, str, int | None]: ...
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
        ) -> asyncio.Task[None]: ...
        @staticmethod
        def _slash_text(intent: SlashIntent) -> str: ...

    async def _track_acceptance_blocked_wait(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        run_id: str,
        verdict: AcceptanceVerdict,
    ) -> None:
        states: dict[str, str] = {}
        try:
            states = await self._states_for_binding(binding)
        except LinearError as e:
            log.warning(
                "could not load states while parking acceptance-blocked %s: %s",
                issue.identifier,
                e,
            )
        target_id = states.get(binding.linear_states.needs_approval) or states.get(
            binding.linear_states.blocked
        )
        tracker = self.tracker(binding)
        if target_id is not None:
            try:
                await tracker.move_issue(issue.id, target_id)
            except LinearError as e:
                log.warning(
                    "could not park acceptance-blocked %s: %s",
                    issue.identifier,
                    e,
                )

        body = acceptance_blocked(
            CommentVars(
                stage="acceptance",
                repo=binding.github_repo,
                issue=pr_number,
                pr_url=await self._acceptance_pr_url(issue.id),
                run_id=run_id,
                error=verdict.details,
            )
        )
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning("acceptance blocked comment failed on %s: %s", issue.identifier, e)

        self._dispatch_run_ids[issue.id] = run_id
        self._operator_wait_run_ids.add(run_id)
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue.id,
            run_id=run_id,
            kind=db.operator_waits.KIND_ACCEPTANCE_BLOCKED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )
        await self._notify_attention(
            event=EVENT_OPERATOR_WAIT,
            issue_identifier=issue.identifier,
            issue_url=issue.url,
            dedupe_key=f"operator_wait:{run_id}",
            detail=verdict.details,
        )

    async def _track_acceptance_rejected_wait(
        self, issue_id: str, run_id: str, binding: RepoBinding
    ) -> None:
        self._dispatch_run_ids[issue_id] = run_id
        self._operator_wait_run_ids.add(run_id)
        self._acceptance_rejected_run_bindings[run_id] = binding
        await db.operator_waits.upsert(
            self._conn,
            issue_id=issue_id,
            run_id=run_id,
            kind=db.operator_waits.KIND_ACCEPTANCE_REJECTED,
            linear_team_key=binding.linear_team_key,
            github_repo=binding.github_repo,
            issue_label=binding.issue_label or "",
            created_at=self._now().isoformat(),
            provider=binding.provider,
            tracker_provider=binding.tracker_provider,
            tracker_site=binding.tracker_site,
        )

    async def _acceptance_pr_url(self, issue_id: str) -> str:
        state = await db.acceptance_state.get(self._conn, issue_id)
        if state.pr_url:
            return state.pr_url
        if state.pr_number is not None:
            return f"#{state.pr_number}"
        return "(no PR yet)"

    async def _acceptance_passed_for_candidate(
        self,
        candidate: db.issue_prs.IssuePR,
        binding: RepoBinding,
        pr_head_sha: str,
    ) -> bool:
        if not pr_head_sha:
            return False
        state = await db.acceptance_state.get(self._conn, candidate.issue_id)
        return (
            state.pr_number == candidate.pr_number
            and state.pr_url == candidate.pr_url
            and state.pr_head_sha == pr_head_sha
            and state.mode == binding.acceptance.mode
            and state.last_verdict == "pass"
        )

    async def _acceptance_infra_retry_backoff_active(self, issue_id: str) -> bool:
        state = await db.acceptance_state.get(self._conn, issue_id)
        if state.last_verdict != "infra_error" or state.infra_retries <= 0:
            return False
        latest = await db.runs.latest_for_issue_stage(
            self._conn,
            issue_id=issue_id,
            stage="acceptance",
        )
        if latest is None or latest.ended_at is None:
            return False
        try:
            ended_at = _parse_rfc3339(latest.ended_at)
        except ValueError:
            return False
        retry_count = min(state.infra_retries, ACCEPTANCE_INFRA_RETRY_LIMIT)
        backoff_secs = _infra_retry_backoff_secs(retry_count)
        return self._now() < ended_at + timedelta(seconds=backoff_secs)

    def _schedule_acceptance(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
    ) -> asyncio.Task[None]:
        binding_key = _binding_key(binding)
        self._reserve_scheduled_slot(issue_id=issue.id, binding_key=binding_key)
        task = asyncio.create_task(
            self._acceptance_with_limits(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                pr_head_sha=pr_head_sha,
            )
        )
        self._dispatch_tasks.add(task)
        task.add_done_callback(
            partial(
                self._dispatch_task_done,
                issue_id=issue.id,
                binding_key=binding_key,
            )
        )
        return task

    async def _acceptance_with_limits(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
    ) -> None:
        key = _binding_key(binding)
        binding_sem = self._binding_dispatch_sems.setdefault(
            key,
            asyncio.Semaphore(max(binding.max_concurrent, 1)),
        )
        try:
            async with self._global_dispatch_sem:
                async with binding_sem:
                    current = await self._refresh_merge_candidate(binding, issue)
                    if current is None:
                        return
                    await self._run_acceptance_stage(
                        binding=binding,
                        issue=current,
                        pr_number=pr_number,
                        pr_url=pr_url,
                        pr_head_sha=pr_head_sha,
                    )
        except asyncio.CancelledError:
            run_id = self._dispatch_run_ids.get(issue.id)
            if run_id is not None:
                await self._fail_run(run_id, "acceptance cancelled")
            raise

    def _acceptance_preview_url(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
    ) -> str:
        if binding.acceptance.mode == "dev" and binding.acceptance.dev_port:
            return f"http://127.0.0.1:{binding.acceptance.dev_port}"
        pattern = binding.acceptance.preview_url_pattern
        if not pattern:
            return ""
        try:
            return render_preview_url(
                acceptance=binding.acceptance,
                issue_identifier=issue.identifier,
                issue_id=issue.id,
                pr_number=pr_number,
                pr_url=pr_url,
            )
        except PreviewResolutionError as e:
            log.warning(
                "could not render acceptance preview URL for %s from %r: %s",
                issue.identifier,
                pattern,
                e,
            )
            return ""

    async def _acceptance_pr_diff(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
    ) -> str:
        try:
            return await self._gh.pr_diff(pr_number, repo=binding.github_repo)
        except GitHubError as e:
            log.warning(
                "could not fetch acceptance PR diff for %s#%d on %s: %s",
                binding.github_repo,
                pr_number,
                issue.identifier,
                e,
            )
            raise _AcceptancePrDiffUnavailable(
                f"Could not fetch PR diff for {binding.github_repo}#{pr_number}: {e}"
            ) from e

    async def _post_acceptance_verdict_comment(
        self,
        *,
        binding: RepoBinding | None = None,
        issue: LinearIssue,
        pr_url: str,
        verdict: AcceptanceVerdict,
    ) -> str:
        tracker = (
            self.tracker(binding)
            if binding is not None
            else await self._tracker_for_issue_id(issue.id)
        )
        try:
            body = format_acceptance_verdict_comment(
                verdict=verdict,
                pr_url=pr_url,
            )
            comment_id = await tracker.post_comment(issue.id, truncate_body(body))
            if comment_id:
                return f"{issue.url}#comment-{comment_id}"
        except LinearError as e:
            log.warning(
                "acceptance verdict comment failed on %s: %s",
                issue.identifier,
                e,
            )
        return ""

    async def _post_acceptance_criteria_comment(
        self,
        *,
        binding: RepoBinding | None = None,
        issue: LinearIssue,
        criteria: list[ExtractedCriterion],
    ) -> None:
        tracker = (
            self.tracker(binding)
            if binding is not None
            else await self._tracker_for_issue_id(issue.id)
        )
        try:
            body = format_acceptance_criteria_comment(criteria)
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "acceptance criteria comment failed on %s: %s",
                issue.identifier,
                e,
            )

    async def _upload_acceptance_screenshots(
        self,
        *,
        binding: RepoBinding | None = None,
        issue: LinearIssue,
        workspace_path: Path,
        verdict: AcceptanceVerdict,
    ) -> AcceptanceVerdict:
        if verdict.kind not in {"pass", "reject"} or not verdict.screenshots:
            return verdict

        tracker = (
            self.tracker(binding)
            if binding is not None
            else await self._tracker_for_issue_id(issue.id)
        )
        uploaded_by_path: dict[str, str] = {}
        uploaded_screenshots: list[AcceptanceScreenshot] = []
        for screenshot in verdict.screenshots:
            try:
                path = _acceptance_artifact_path(workspace_path, screenshot.path)
                url = await tracker.upload_issue_attachment(
                    issue_uuid=issue.id,
                    path=path,
                    title=f"Acceptance screenshot: {screenshot.label}",
                )
            except (LinearError, OSError, httpx.HTTPError) as e:
                return replace(
                    verdict,
                    kind="infra_error",
                    hero_screenshot_url="",
                    screenshots=(),
                    criterion_results=(),
                    details=f"acceptance screenshot upload failed: {e}",
                )
            uploaded_by_path[screenshot.path] = url
            uploaded_screenshots.append(replace(screenshot, url=url))

        criterion_results = tuple(
            replace(
                result,
                screenshot_url=uploaded_by_path.get(
                    result.screenshot_path,
                    result.screenshot_url,
                ),
            )
            for result in verdict.criterion_results
        )
        hero_url = next(
            (item.url for item in uploaded_screenshots if item.kind == "hero" and item.url),
            verdict.hero_screenshot_url,
        )
        return replace(
            verdict,
            hero_screenshot_url=hero_url,
            screenshots=tuple(uploaded_screenshots),
            criterion_results=criterion_results,
        )

    async def _run_acceptance_stage(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
        reset_iteration: bool = True,
    ) -> str | None:
        run_id = str(uuid.uuid4())
        inserted = await db.runs.create_if_no_active(
            self._conn,
            id=run_id,
            issue_id=issue.id,
            stage="acceptance",
            status="running",
            pid=None,
            started_at=self._now().isoformat(),
            ignored_stage="review",
        )
        if not inserted:
            return None

        self._dispatch_run_ids[issue.id] = run_id
        try:
            prep = await self._begin_acceptance_run(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                pr_head_sha=pr_head_sha,
                reset_iteration=reset_iteration,
            )
            verdict = await self._compute_acceptance_verdict(
                run_id=run_id,
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                prep=prep,
            )

            verdict_usage = verdict.usage
            if verdict_usage.has_usage():
                verdict_usage = UsageDelta(
                    cost_usd=verdict.cost,
                    input_tokens=verdict_usage.input_tokens,
                    output_tokens=verdict_usage.output_tokens,
                    cache_write_tokens=verdict_usage.cache_write_tokens,
                    cache_read_tokens=verdict_usage.cache_read_tokens,
                )
            elif verdict.cost > 0:
                verdict_usage = UsageDelta(cost_usd=verdict.cost)
            if verdict_usage.has_usage():
                await _add_run_usage(self._conn, run_id, verdict_usage)

            comment_url = await self._post_acceptance_verdict_comment(
                binding=binding,
                issue=issue,
                pr_url=pr_url,
                verdict=verdict,
            )
            await db.acceptance_state.record_verdict(
                self._conn,
                issue.id,
                verdict=verdict.kind,
                artifacts_url=comment_url or verdict.hero_screenshot_url,
                preview_url=verdict.preview_url,
            )

            return await self._finalize_acceptance_verdict(
                run_id=run_id,
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                pr_head_sha=pr_head_sha,
                verdict=verdict,
            )
        except Exception as e:
            log.exception("acceptance stage failed for %s", issue.identifier)
            await db.runs.update_status(
                self._conn,
                run_id,
                "failed",
                ended_at=self._now().isoformat(),
                **_termination_kwargs(
                    status="failed",
                    exc=e,
                    reason=f"acceptance stage failed: {e}",
                ),
            )
            return run_id
        finally:
            if (
                self._dispatch_run_ids.get(issue.id) == run_id
                and run_id not in self._operator_wait_run_ids
            ):
                self._dispatch_run_ids.pop(issue.id, None)

    async def _begin_acceptance_run(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
        reset_iteration: bool,
    ) -> _AcceptancePrep:
        """Complete review monitors, resolve mode/preview/criteria, and open the
        acceptance state + comments before the verdict is computed."""
        await self._complete_review_monitors_for_merge(issue)
        degrade_note = (
            _acceptance_degrade_note(issue.description)
            if binding.acceptance.mode != _CODE_ONLY_ACCEPTANCE_MODE
            else None
        )
        effective_mode = _CODE_ONLY_ACCEPTANCE_MODE if degrade_note else binding.acceptance.mode
        if degrade_note:
            log.info("%s for %s", degrade_note, issue.identifier)
        preview_url = ""
        preview_resolution_error = ""
        if not degrade_note:
            if effective_mode == "preview" and binding.acceptance.preview_url_pattern:
                try:
                    preview_url = render_preview_url(
                        acceptance=binding.acceptance,
                        issue_identifier=issue.identifier,
                        issue_id=issue.id,
                        pr_number=pr_number,
                        pr_url=pr_url,
                    )
                except PreviewResolutionError as e:
                    preview_resolution_error = str(e)
                    preview_url = e.url
            else:
                preview_url = self._acceptance_preview_url(
                    binding=binding,
                    issue=issue,
                    pr_number=pr_number,
                    pr_url=pr_url,
                )
        extracted_criteria = extract_acceptance_criteria(issue.description)
        criteria_names = _acceptance_criterion_names(extracted_criteria)
        criteria_predicates = _acceptance_criterion_predicates(extracted_criteria)
        await db.acceptance_state.begin_acceptance(
            self._conn,
            issue.id,
            pr_number=pr_number,
            pr_url=pr_url,
            pr_head_sha=pr_head_sha,
            mode=binding.acceptance.mode,
            preview_url=preview_url,
            extracted_criteria=json.dumps(extracted_criteria),
            reset_iteration=reset_iteration,
        )
        await self._post_acceptance_criteria_comment(
            binding=binding,
            issue=issue,
            criteria=extracted_criteria,
        )
        await self._move_issue_to_acceptance_state(binding=binding, issue=issue)
        return _AcceptancePrep(
            effective_mode=effective_mode,
            degrade_note=degrade_note,
            preview_url=preview_url,
            preview_resolution_error=preview_resolution_error,
            criteria_names=criteria_names,
            criteria_predicates=criteria_predicates,
        )

    async def _compute_acceptance_verdict(
        self,
        *,
        run_id: str,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        prep: _AcceptancePrep,
    ) -> AcceptanceVerdict:
        """Resolve the acceptance verdict for the prepared run (pass-through /
        preview-resolution / quick-skip / real runner), with the degrade note
        applied."""
        effective_mode = prep.effective_mode
        preview_url = prep.preview_url
        criteria_names = prep.criteria_names
        criteria_predicates = prep.criteria_predicates
        verdict: AcceptanceVerdict | None = None
        if effective_mode not in {_CODE_ONLY_ACCEPTANCE_MODE, "dev", "preview"}:
            verdict = AcceptanceVerdict(
                kind="pass",
                criteria=criteria_names,
                cost=0.0,
                hero_screenshot_url="",
                details=(
                    f"Acceptance mode {binding.acceptance.mode!r} is configured, "
                    "but this mode does not have a real runner in this slice. "
                    "Preserving pass-through acceptance behavior until that "
                    "mode's runner is implemented."
                ),
            )
        elif prep.preview_resolution_error:
            verdict = AcceptanceVerdict(
                kind="infra_error",
                criteria=criteria_names,
                cost=0.0,
                hero_screenshot_url="",
                details=prep.preview_resolution_error,
                preview_url=preview_url,
            )
        elif effective_mode == "preview":
            try:
                preview_url = await resolve_preview_url(
                    acceptance=binding.acceptance,
                    pr_number=pr_number,
                    issue_identifier=issue.identifier,
                    issue_id=issue.id,
                    pr_url=pr_url,
                )
            except PreviewResolutionError as e:
                verdict = AcceptanceVerdict(
                    kind="infra_error",
                    criteria=criteria_names,
                    cost=0.0,
                    hero_screenshot_url="",
                    details=str(e),
                    preview_url=e.url,
                )

        if verdict is None:
            try:
                pr_diff_summary = await self._acceptance_pr_diff(
                    binding=binding,
                    issue=issue,
                    pr_number=pr_number,
                )
            except _AcceptancePrDiffUnavailable as e:
                verdict = AcceptanceVerdict(
                    kind="infra_error",
                    criteria=criteria_names,
                    cost=0.0,
                    hero_screenshot_url="",
                    details=str(e),
                )
            else:
                quick_skip = (
                    quick_skip_trivial_acceptance(
                        linear_description=issue.description,
                        pr_diff_summary=pr_diff_summary,
                        criteria=criteria_names,
                    )
                    if effective_mode == _CODE_ONLY_ACCEPTANCE_MODE
                    else None
                )
                if quick_skip is not None:
                    verdict = quick_skip
                else:
                    workspace_path = await self._workspace.acquire(binding, issue)
                    try:
                        verdict = await run_acceptance(
                            runner=self._runner,
                            run_id=run_id,
                            workspace_path=workspace_path,
                            mode=effective_mode,
                            linear_description=issue.description,
                            pr_diff_summary=pr_diff_summary,
                            taste_guide=load_taste_guide(
                                binding_taste_guide=binding.acceptance.taste_guide,
                            ),
                            criteria=criteria_predicates,
                            stall_secs=binding.acceptance.time_cap_minutes * 60,
                            preview_url=preview_url,
                            dev_command=binding.acceptance.dev_command,
                            dev_port=binding.acceptance.dev_port,
                        )
                        verdict = _replace_acceptance_criteria_labels(
                            verdict=verdict,
                            criteria_names=criteria_names,
                            criteria_predicates=criteria_predicates,
                        )
                        if effective_mode in {"dev", "preview"}:
                            verdict = await self._upload_acceptance_screenshots(
                                binding=binding,
                                issue=issue,
                                workspace_path=workspace_path,
                                verdict=verdict,
                            )
                    finally:
                        self._workspace.release(binding, issue)

        return _with_acceptance_degrade_note(verdict, prep.degrade_note)

    async def _finalize_acceptance_verdict(
        self,
        *,
        run_id: str,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
        verdict: AcceptanceVerdict,
    ) -> str:
        """Record the run outcome and drive the next step: merge on pass, retry/
        block on infra_error, fix-run/operator-wait on rejection."""
        ended_at = self._now().isoformat()
        if verdict.kind == "pass":
            await db.runs.update_status(
                self._conn,
                run_id,
                "completed",
                ended_at=ended_at,
            )
            if self._dispatch_run_ids.get(issue.id) == run_id:
                self._dispatch_run_ids.pop(issue.id, None)
            merge_issue = await self._refresh_issue_for_acceptance_merge_handoff(binding, issue)
            if _needs_human_approval_label_present(merge_issue):
                await self._open_merge_wait_for_human_approval_label(
                    binding=binding,
                    issue=merge_issue,
                    pr_url=pr_url,
                )
            else:
                await self._merge_approved_pr(
                    binding=binding,
                    issue=merge_issue,
                    pr_number=pr_number,
                    pr_url=pr_url,
                    approved_head_sha=pr_head_sha,
                )
            return run_id

        if verdict.kind == "infra_error":
            state = await db.acceptance_state.get(self._conn, issue.id)
            if state.infra_retries >= ACCEPTANCE_INFRA_RETRY_LIMIT:
                await db.runs.update_status(
                    self._conn,
                    run_id,
                    "failed",
                    ended_at=ended_at,
                    **_termination_kwargs(
                        status="failed",
                        reason=f"acceptance infra_error: {verdict.details}",
                    ),
                )
                await self._track_acceptance_blocked_wait(
                    binding=binding,
                    issue=issue,
                    pr_number=pr_number,
                    run_id=run_id,
                    verdict=verdict,
                )
                return run_id
            await db.acceptance_state.bump_infra_retries(self._conn, issue.id)
            await db.runs.update_status(
                self._conn,
                run_id,
                "failed",
                ended_at=ended_at,
                **_termination_kwargs(
                    status="failed",
                    reason=f"acceptance infra_error: {verdict.details}",
                ),
            )
            return run_id

        state = await db.acceptance_state.get(self._conn, issue.id)
        await db.runs.update_status(
            self._conn,
            run_id,
            "failed",
            ended_at=ended_at,
            **_termination_kwargs(
                status="failed",
                reason=f"acceptance rejected: {verdict.details}",
            ),
        )
        if state.iteration < ACCEPTANCE_FIX_ITERATION_CAP:
            dispatched = await self._dispatch_acceptance_fix_run(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                pr_head_sha=pr_head_sha,
                verdict=verdict,
            )
            if dispatched:
                return run_id
            log.warning(
                "acceptance fix-run did not advance %s; opening operator wait",
                issue.identifier,
            )

        await self._track_acceptance_rejected_wait(issue.id, run_id, binding)
        await self._notify_attention(
            event=EVENT_OPERATOR_WAIT,
            issue_identifier=issue.identifier,
            issue_url=issue.url,
            dedupe_key=f"operator_wait:{run_id}",
            detail=verdict.details,
        )
        body = acceptance_rejected(
            CommentVars(
                stage="acceptance",
                repo=binding.github_repo,
                issue=pr_number,
                pr_url=pr_url,
                run_id=run_id,
            )
        )
        tracker = self.tracker(binding)
        try:
            await tracker.post_comment(issue.id, truncate_body(body))
        except LinearError as e:
            log.warning(
                "acceptance rejected wait comment failed on %s: %s",
                issue.identifier,
                e,
            )
        return run_id

    async def _dispatch_acceptance_fix_run(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        pr_number: int,
        pr_url: str,
        pr_head_sha: str,
        verdict: AcceptanceVerdict,
    ) -> bool:
        await db.acceptance_state.bump_iteration(self._conn, issue.id)
        prompt = acceptance_fix_prompt(
            issue_title=issue.title,
            issue_body=issue.description,
            labels=list(issue.labels),
            acceptance_verdict=format_acceptance_verdict_comment(
                verdict=verdict,
                pr_url=pr_url,
            ),
        )

        try:
            workspace_path = await self._workspace.acquire(binding, issue)
        except Exception:  # noqa: BLE001
            log.exception("workspace acquire failed for acceptance fix-run %s", issue.identifier)
            return False

        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
        try:
            try:
                await _git_fetch_branch(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "could not fetch acceptance fix-run remote HEAD for %s: %s",
                    branch,
                    e,
                )
                return False
            start_sha = await _workspace_ref_sha(workspace_path, f"origin/{branch}")
            if not start_sha:
                start_sha = pr_head_sha
            if not start_sha:
                log.warning(
                    "could not read acceptance fix-run remote HEAD for %s",
                    branch,
                )
                return False

            fix_run_id = str(uuid.uuid4())
            await db.runs.create(
                self._conn,
                id=fix_run_id,
                issue_id=issue.id,
                stage="acceptance_fix",
                status="running",
                pid=None,
                started_at=self._now().isoformat(),
            )
            self._dispatch_run_ids[issue.id] = fix_run_id

            try:
                prior_total = await db.runs.cost_for_issue(self._conn, issue.id)
                (
                    usage_delta,
                    final_kind,
                    final_returncode,
                ) = await self._run_acceptance_fix_agent(
                    binding=binding,
                    issue=issue,
                    run_id=fix_run_id,
                    workspace_path=workspace_path,
                    prompt=prompt,
                    prior_total=prior_total,
                )
            except Exception as e:  # noqa: BLE001
                log.exception("acceptance fix-run execution failed for %s", issue.identifier)
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        exc=e,
                        reason=f"acceptance fix-run execution failed: {e}",
                    ),
                )
                return False
            finally:
                if self._dispatch_run_ids.get(issue.id) == fix_run_id:
                    self._dispatch_run_ids.pop(issue.id, None)

            await _add_run_usage(self._conn, fix_run_id, usage_delta)

            transition = on_runner_event(
                stage="acceptance_fix",
                event_kind=final_kind,
                returncode=final_returncode,
            )
            if transition.next_run_status != "completed":
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    transition.next_run_status,
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status=transition.next_run_status,
                        final_kind=final_kind,
                        returncode=final_returncode,
                        reason=f"acceptance fix-run ended with {final_kind}",
                    ),
                )
                return False

            pushed_sha = await _workspace_head_sha(workspace_path)
            if not pushed_sha or pushed_sha == start_sha:
                short_sha = (pushed_sha or start_sha)[:12] or "(unknown)"
                status_short = await _git_status_short(workspace_path)
                log.warning(
                    "acceptance fix-run completed without advancing %s; "
                    "HEAD stayed at %s; status=%s",
                    branch,
                    short_sha,
                    status_short,
                )
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        reason=(
                            "acceptance fix-run completed without advancing "
                            f"{branch}; HEAD stayed at {short_sha}; status={status_short}"
                        ),
                    ),
                )
                return False

            try:
                await self._push_fn(workspace_path, branch)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "git push failed for acceptance fix-run %s: %s",
                    issue.identifier,
                    e,
                )
                await db.runs.update_status(
                    self._conn,
                    fix_run_id,
                    "failed",
                    ended_at=self._now().isoformat(),
                    **_termination_kwargs(
                        status="failed",
                        exc=e,
                        reason=f"push failed: {e}",
                    ),
                )
                return False

            await db.runs.update_status(
                self._conn,
                fix_run_id,
                "completed",
                ended_at=self._now().isoformat(),
            )
            await self._run_acceptance_stage(
                binding=binding,
                issue=issue,
                pr_number=pr_number,
                pr_url=pr_url,
                pr_head_sha=pushed_sha,
                reset_iteration=False,
            )
            return True
        finally:
            self._workspace.release(binding, issue)

    async def _move_issue_to_acceptance_state(
        self, *, binding: RepoBinding, issue: LinearIssue
    ) -> None:
        try:
            states = await self._states_for_binding(binding)
            acceptance_state_id = states.get(binding.linear_states.in_acceptance)
        except LinearError as e:
            log.warning(
                "could not load states while moving %s to acceptance: %s",
                issue.identifier,
                e,
            )
            return
        if acceptance_state_id is None:
            log.warning(
                "missing Linear acceptance state %r for %s",
                binding.linear_states.in_acceptance,
                issue.identifier,
            )
            return
        try:
            await self.tracker(binding).move_issue(issue.id, acceptance_state_id)
        except LinearError as e:
            log.warning(
                "could not move %s to acceptance state %r: %s",
                issue.identifier,
                binding.linear_states.in_acceptance,
                e,
            )

    async def _run_acceptance_fix_agent(
        self,
        *,
        binding: RepoBinding,
        issue: LinearIssue,
        run_id: str,
        workspace_path: Path,
        prompt: str,
        prior_total: float,
    ) -> tuple[UsageDelta, str, int | None]:
        command = build_fix_runner_command(
            binding.agent,
            prompt,
            codex_model=binding.codex_model,
            workspace_path=workspace_path,
            mcp_servers=binding.mcp_servers,
        )
        return await self._run_runner(
            run_id=run_id,
            workspace_path=workspace_path,
            command=command,
            stage="acceptance_fix",
            agent=binding.agent,
            codex_model=binding.codex_model,
            binding=binding,
            issue=issue,
            activity_stage="acceptance_fix",
            prior_total=prior_total,
        )
