"""Guards the poll/ package layout (SYM-143, SYM-144).

Free module-level functions live in `_git.py` (git/workspace primitives) and
`_helpers.py` (cross-cutting + domain-shaped pure helpers). `poll/__init__.py`
re-exports the whole surface by explicit name so existing imports keep working.

`_OrchestratorBase` (SYM-144) owns the `__init__`, all in-memory state, and the
foundation methods (tracker/binding/state-resolve) that every domain calls;
`Orchestrator` inherits it.
"""

from symphony.orchestrator import poll
from symphony.orchestrator.poll import _base, _git, _helpers
from symphony.orchestrator.poll._base import _OrchestratorBase

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

# Foundation module-level names relocated to `_base.py` (SYM-144), re-exported
# from `__init__.py` by explicit name so existing imports keep working.
_BASE_NAMES = [
    "_tracker_context_for_binding",
    "_state_cache_key",
    "_register_configured_trackers",
    "_PendingDelivery",
    "_ImplementHandoff",
    "PushFn",
    "BindingKey",
]


def test_orchestrator_inherits_base() -> None:
    assert issubclass(poll.Orchestrator, _OrchestratorBase)
    assert poll.Orchestrator is not _OrchestratorBase


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
