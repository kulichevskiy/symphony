"""Guards the poll/ package layout (SYM-143, SYM-144, SYM-149, SYM-145).

Free module-level functions live in `_git.py` (git/workspace primitives) and
`_helpers.py` (cross-cutting + domain-shaped pure helpers). `poll/__init__.py`
re-exports the whole surface by explicit name so existing imports keep working.

`_OrchestratorBase` (SYM-144) owns the `__init__`, all in-memory state, and the
foundation methods (tracker/binding/state-resolve) that every domain calls;
`Orchestrator` inherits it.

`_DispatchMixin` (SYM-149) owns the dispatch domain (scan/schedule/capacity/
slot/sem + the park guards); it extends `_OrchestratorBase` and `Orchestrator`
inherits it.

`_SlashCommandsMixin` (SYM-145) owns the slash-command domain (web-commands /
comment-cursor poll / `$intent` dispatch + the per-state handlers); it extends
`_OrchestratorBase` and `Orchestrator` inherits it.

`_MergeMixin` (SYM-147) owns the merge domain — merge-candidate polling, merge
execution + fix-runs, manual-merge park/revival, merge-wait reconciliation —
plus the merge-exclusive free functions. `Orchestrator` inherits it.
"""

from symphony.orchestrator import poll
from symphony.orchestrator.poll import _base, _dispatch, _git, _helpers, _merge, _slash_commands
from symphony.orchestrator.poll._base import _OrchestratorBase
from symphony.orchestrator.poll._dispatch import _DispatchMixin
from symphony.orchestrator.poll._merge import _MergeMixin
from symphony.orchestrator.poll._slash_commands import _SlashCommandsMixin

# Git/workspace primitives that must live in `_git.py`.
_GIT_FUNCS = [
    "_default_push",
    "_default_force_push",
    "_sync_workspace_to_remote",
    "_git_fetch",
    "_git_fetch_branch",
    "_git_status_short",
    "_git_rebase",
    "_git_abort_rebase",
    "_git_conflicted_files",
    "_git_add_and_continue_rebase",
    "_workspace_head_sha",
    "_workspace_ref_sha",
    "_workspace_ref_is_ancestor",
    "_workspace_ref_landed_in_base",
    "_workspace_commits_ahead",
    "_workspace_diff_size",
    "_branch_ahead_of_base",
    "_workspace_scrub",
    "_workspace_dirty_files",
]

# Cross-cutting + domain-shaped pure helpers that must live in `_helpers.py`.
_HELPER_FUNCS = [
    "_sum_usage",
    "_parse_rfc3339",
    "_parse_optional_datetime",
    "build_pr_title",
    "build_pr_body",
    "build_runner_command",
    "build_fix_runner_command",
    "build_merge_runner_command",
    "pr_number_from_url",
    "_github_commit_url",
    "_pr_url_for_state",
    "_pr_view_is_merged",
    "_pr_view_is_closed",
    "_pr_view_has_merge_conflict",
    "_pr_view_skips_required_check_fix",
    "_pr_view_is_clean_mergeable",
    "_pr_base_ref_from_view",
    "_status_rollup_nodes",
    "_status_check_identity",
    "_status_check_names",
    "_status_check_sha",
    "_status_check_failed",
    "_status_check_succeeded",
    "_status_check_run_id",
    "_no_signal_head_check_state",
    "_required_check_detail",
    "_required_check_trigger_signature",
    "_acceptance_has_where_to_verify",
    "_normalize_acceptance_section_heading",
    "_acceptance_degrade_note",
    "_needs_human_approval_label_present",
]


# Foundation methods that must live on `_OrchestratorBase` (SYM-144):
# the __init__, tracker/binding/state-resolve methods every domain calls.
_BASE_METHODS = [
    "__init__",
    "_now",
    "tracker",
    "_stored_tracker_identity_for_issue",
    "_storage_issue_ids_for_tracker_issue",
    "_storage_issue_id_for_tracker_issue",
    "_stored_tracker_context_for_issue",
    "_tracker_context_for_issue",
    "_tracker_identity_for_issue",
    "_tracker_for_issue_id",
    "_configured_tracker_contexts",
    "_lookup_webhook_issue",
    "_states_for_binding",
    "_binding_for_issue",
    "_binding_for_review",
]

# Foundation module-level names relocated to `_base.py` (SYM-144, SYM-149),
# re-exported from `__init__.py` by explicit name so existing imports keep working.
_BASE_NAMES = [
    "_tracker_context_for_binding",
    "_state_cache_key",
    "_register_configured_trackers",
    "_PendingDelivery",
    "_ImplementHandoff",
    "PushFn",
    "BindingKey",
    "_binding_key",
]

# Dispatch-domain methods that must live on `_DispatchMixin` (SYM-149):
# scan/schedule/capacity/slot/sem logic plus the park guards.
_DISPATCH_METHODS = [
    "_scan_binding",
    "_auto_unblock_waiting",
    "_dispatch_capacity",
    "_scheduled_slot_count",
    "_reserve_scheduled_slot",
    "_release_scheduled_slot",
    "_review_fix_dispatch_slot",
    "_schedule_ready_issue",
    "_blocking_existing_pr",
    "_park_already_has_pr",
    "_park_blocked_by_deps",
    "_ready_binding_for_issue",
    "_schedule_dispatch",
    "_dispatch_with_limits",
    "_mark_cancelled_dispatch",
    "_refresh_dispatch_candidate",
    "_dispatch_task_done",
]


# Slash-command-domain methods that must live on `_SlashCommandsMixin` (SYM-145):
# web-commands, the comment-cursor poll, the `$intent` dispatcher, and the
# per-state `_handle_*_slash_intent` handlers.
_SLASH_METHODS = [
    "enqueue_web_command",
    "_drain_web_commands",
    "_apply_web_command",
    "_web_command_run_id",
    "_slash_command_run_eligible",
    "_poll_slash_commands",
    "_handle_unseen_slash_comment",
    "_handle_slash_comments",
    "_advance_comment_cursor",
    "_resolve_comment_cursor",
    "_run_started_at",
    "_handle_slash_intent",
    "_slash_text",
    "_post_command_rejected",
    "_handle_active_review_retry_intent",
    "_handle_implement_failed_slash_intent",
    "_handle_implement_blocked_slash_intent",
    "_handle_acceptance_blocked_slash_intent",
    "_handle_budget_exceeded_slash_intent",
    "_handle_review_failed_slash_intent",
    "_handle_acceptance_rejected_slash_intent",
    "_handle_skip_review_intent",
    "_handle_deliver_failed_slash_intent",
]

# Slash-domain module-level names relocated to `_slash_commands.py` (SYM-145),
# re-exported from `__init__.py` by explicit name so existing imports keep working.
_SLASH_NAMES = [
    "SlashHandlerFailure",
    "MANUAL_MERGE_PARKED_RUN_PREFIX",
    "_manual_merge_parked_run_id",
]


# Merge-domain methods that must live on `_MergeMixin` (SYM-147).
_MERGE_METHODS = [
    "_run_auto_recoverable_merge_wait_reconciler",
    "_parked_manual_merge_slash_pairs",
    "_parked_manual_merge_run_id_for_issue",
    "_manual_merge_parked_started_at",
    "_handle_parked_manual_merge_slash_intent",
    "_reconcile_orphaned_merge_runs",
    "_reconcile_auto_recoverable_merge_waits",
    "_reconcile_auto_recoverable_merge_wait",
    "_repo_view_for_merge_wait_reconcile",
    "_schedule_reconciled_merge_conflict_rebase_fix",
    "_merge_wait_reconcile_task_done",
    "_complete_review_monitors_for_merge",
    "_interrupt_stale_merge_needs_approval_for_state",
    "_resolve_pr_base_ref",
    "_required_check_failures_for_view",
    "_merge_required_check_fix_should_dispatch",
    "_merge_required_check_action_log_tail",
    "_mark_merge_required_check_fix_needs_approval",
    "_merge_required_check_terminal_run",
    "_dispatch_merge_required_check_fix_if_allowed",
    "_dispatch_merge_required_check_fix_run",
    "_run_required_check_fix_agent",
    "_mark_merge_conflict_fix_needs_approval",
    "_dispatch_merge_conflict_rebase_fix_run",
    "_schedule_parked_manual_merge_revival_for_issue_event",
    "_schedule_parked_manual_merge_revival_if_requested",
    "_parked_manual_merge_transition_matches",
    "_schedule_parked_manual_merge_revival",
    "_parked_manual_merge_revival_task_done",
    "_dispatch_merge_conflict_fix_run",
    "_handle_merge_needs_approval_slash_intent",
    "_parked_closed_unmerged_pr_for_event",
    "_reconcile_parked_closed_unmerged_pr_event",
    "_mark_parked_closed_unmerged_pr_done",
    "_reconcile_merged_issues_linear_state",
    "_refresh_issue_for_acceptance_merge_handoff",
    "_open_merge_wait_for_human_approval_label",
    "_park_pr_for_manual_merge",
    "_poll_merge_candidates",
    "_schedule_merge_conflict_rebase_fix",
    "_schedule_merge_required_check_fix",
    "_schedule_merge",
    "_merge_with_limits",
    "_refresh_merge_candidate",
    "_maybe_post_codex_lgtm",
    "_review_verdict_for_pr",
    "_poll_submitted_merge",
    "_finalize_pr_if_closed",
    "_merge_approved_pr",
    "_mark_merge_done_if_merged",
    "_mark_merge_done",
    "_mark_merge_needs_approval",
    "_run_merge_agent",
]

# Merge-exclusive free functions co-located into `_merge.py` (SYM-147).
_MERGE_FUNCS = [
    "_abort_rebase_safely",
    "_manual_merge_parked_run_id",
    "_merge_issue_matches_binding",
    "_review_check_from_github",
]


def test_orchestrator_inherits_base() -> None:
    assert issubclass(poll.Orchestrator, _OrchestratorBase)
    assert poll.Orchestrator is not _OrchestratorBase


def test_orchestrator_inherits_dispatch_mixin() -> None:
    assert issubclass(poll.Orchestrator, _DispatchMixin)
    assert issubclass(_DispatchMixin, _OrchestratorBase)
    assert poll.Orchestrator is not _DispatchMixin


def test_dispatch_methods_defined_on_mixin() -> None:
    for name in _DISPATCH_METHODS:
        member = getattr(poll.Orchestrator, name)
        owner = (
            member.fget.__qualname__
            if isinstance(member, property)
            else member.__qualname__
        )
        assert owner.startswith("_DispatchMixin."), name


def test_dispatch_mixin_module() -> None:
    assert _dispatch.__name__.endswith("poll._dispatch")


def test_orchestrator_inherits_slash_mixin() -> None:
    assert issubclass(poll.Orchestrator, _SlashCommandsMixin)
    assert issubclass(_SlashCommandsMixin, _OrchestratorBase)
    assert poll.Orchestrator is not _SlashCommandsMixin


def test_slash_methods_defined_on_mixin() -> None:
    for name in _SLASH_METHODS:
        member = getattr(poll.Orchestrator, name)
        owner = (
            member.fget.__qualname__
            if isinstance(member, property)
            else member.__qualname__
        )
        assert owner.startswith("_SlashCommandsMixin."), name


def test_slash_mixin_module() -> None:
    assert _slash_commands.__name__.endswith("poll._slash_commands")


def test_slash_names_relocated_and_reexported_by_identity() -> None:
    for name in _SLASH_NAMES:
        assert getattr(poll, name) is getattr(_slash_commands, name), name


def test_orchestrator_inherits_merge_mixin() -> None:
    assert issubclass(poll.Orchestrator, _MergeMixin)
    assert poll.Orchestrator is not _MergeMixin


def test_merge_mixin_extends_base() -> None:
    assert issubclass(_MergeMixin, _OrchestratorBase)


def test_merge_methods_defined_on_mixin() -> None:
    for name in _MERGE_METHODS:
        member = getattr(poll.Orchestrator, name)
        owner = member.fget.__qualname__ if isinstance(member, property) else member.__qualname__
        assert owner.startswith("_MergeMixin."), name


def test_merge_free_functions_live_in_merge_module() -> None:
    for name in _MERGE_FUNCS:
        fn = getattr(_merge, name)
        assert fn.__module__.endswith("poll._merge"), name


def test_foundation_methods_defined_on_base() -> None:
    for name in _BASE_METHODS:
        member = getattr(poll.Orchestrator, name)
        owner = member.fget.__qualname__ if isinstance(member, property) else member.__qualname__
        assert owner.startswith("_OrchestratorBase."), name


def test_linear_property_defined_on_base() -> None:
    prop = poll.Orchestrator.linear
    assert isinstance(prop, property)
    assert prop.fget.__qualname__.startswith("_OrchestratorBase.")


def test_base_names_relocated_and_reexported_by_identity() -> None:
    for name in _BASE_NAMES:
        assert getattr(poll, name) is getattr(_base, name), name


def test_git_functions_live_in_git_module() -> None:
    for name in _GIT_FUNCS:
        fn = getattr(_git, name)
        assert fn.__module__.endswith("poll._git"), name


def test_helper_functions_live_in_helpers_module() -> None:
    for name in _HELPER_FUNCS:
        fn = getattr(_helpers, name)
        assert fn.__module__.endswith("poll._helpers"), name


def test_init_reexports_moved_functions_by_identity() -> None:
    for name in _GIT_FUNCS:
        assert getattr(poll, name) is getattr(_git, name), name
    for name in _HELPER_FUNCS:
        assert getattr(poll, name) is getattr(_helpers, name), name
