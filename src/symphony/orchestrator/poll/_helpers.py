"""Cross-cutting + domain-shaped pure helpers for the poll loop (SYM-143).

Pure move out of ``poll/__init__.py`` — bodies are unchanged. Holds generic
utilities (usage/time/command builders) plus the ``pr_view`` / ``status_check``
/ ``required_check`` predicate families, co-located here until a dedicated
domain module exists. Re-exported by the package ``__init__``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from ...agent.codex_cli import build_codex_workspace_write_command
from ...agent.codex_models import DEFAULT_CODEX_MODEL
from ...pipeline.cost_guard import UsageDelta
from ...tracker import Issue as LinearIssue

_ACCEPTANCE_MISSING_WHERE_TO_VERIFY_NOTE = (
    "Acceptance: degraded to code-only — no `Where to verify` in ticket description"
)


def _sum_usage(left: UsageDelta, right: UsageDelta) -> UsageDelta:
    return UsageDelta(
        cost_usd=left.cost_usd + right.cost_usd,
        input_tokens=left.input_tokens + right.input_tokens,
        output_tokens=left.output_tokens + right.output_tokens,
        cache_write_tokens=left.cache_write_tokens + right.cache_write_tokens,
        cache_read_tokens=left.cache_read_tokens + right.cache_read_tokens,
    )


def _acceptance_has_where_to_verify(description: str) -> bool:
    for raw_line in description.splitlines():
        heading = _normalize_acceptance_section_heading(raw_line)
        if heading == "where to verify" or heading.startswith("where to verify:"):
            return True
    return False


def _normalize_acceptance_section_heading(line: str) -> str:
    line = line.strip()
    line = re.sub(r"^#{1,6}\s*", "", line)
    line = re.sub(r"\s+#{1,6}\s*$", "", line)
    line = line.strip(" *_`")
    return re.sub(r"\s+", " ", line).casefold()


def _acceptance_degrade_note(description: str) -> str | None:
    if _acceptance_has_where_to_verify(description):
        return None
    return _ACCEPTANCE_MISSING_WHERE_TO_VERIFY_NOTE


def _parse_optional_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return _parse_rfc3339(raw)
    except ValueError:
        return None


def build_pr_title(issue: LinearIssue) -> str:
    return f"[{issue.identifier}] {issue.title}"


def build_pr_body(issue: LinearIssue) -> str:
    """The Linear URL goes through `gh pr_create`'s `linear_url` argument
    (which appends `Relates to ...`), so the body itself is empty by
    default. Returning the URL here keeps the format pinned in tests."""
    return f"Relates to {issue.url}"


def build_runner_command(
    agent: str,
    prompt: str,
    *,
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    effort: str | None = None,
    workspace_path: Path | None = None,
    mcp_servers: Mapping[str, Any] | None = None,
) -> list[str]:
    """Per-runner argv for the Implement stage prompt.

    `mcp_servers` is the binding's MCP allowlist. Claude spawns always run
    `--strict-mcp-config` so the agent only sees servers the binding
    explicitly grants — none by default. Codex MCP wiring lives in its own
    config.toml and is unaffected.

    `claude_model` is the resolved `implement` role's Claude model: set →
    `--model <alias>`, unset → no flag (CLI default). It is ignored for codex.

    `effort` is the resolved role's reasoning effort: for claude it becomes a
    dedicated `--effort <level>` flag; for codex it becomes
    `--config model_reasoning_effort="<v>"`. Unset → no flag (CLI default).
    """
    if agent == "claude":
        command = [
            "claude",
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            "--strict-mcp-config",
        ]
        if mcp_servers:
            command.extend(
                ["--mcp-config", json.dumps({"mcpServers": dict(mcp_servers)})]
            )
        if claude_model is not None:
            command.extend(["--model", claude_model])
        if effort is not None:
            command.extend(["--effort", effort])
        command.append(prompt)
        return command
    if agent == "codex":
        if workspace_path is None:
            raise ValueError("workspace_path is required for codex write runs")
        return build_codex_workspace_write_command(
            prompt=prompt,
            codex_model=codex_model,
            effort=effort,
        )
    raise ValueError(f"unknown agent {agent!r}")


def build_fix_runner_command(
    agent: str,
    prompt: str,
    *,
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    workspace_path: Path | None = None,
    mcp_servers: Mapping[str, Any] | None = None,
) -> list[str]:
    """argv for a Review-stage fix-run.

    Fix-runs go through the binding's CLI (claude or codex), NOT through
    the GitHub `@codex review` bot. The bot is only consulted via PR
    comments; the binding's `agent` field is what drives code changes
    in response to its feedback.

    `claude_model` is the resolved `fix` role's Claude model: set →
    `--model <alias>`, unset → no flag (CLI default). It is ignored for codex.
    """
    return build_runner_command(
        agent,
        prompt,
        codex_model=codex_model,
        claude_model=claude_model,
        workspace_path=workspace_path,
        mcp_servers=mcp_servers,
    )


def build_merge_runner_command(
    agent: str,
    prompt: str,
    *,
    codex_model: str = DEFAULT_CODEX_MODEL,
    workspace_path: Path | None = None,
    mcp_servers: Mapping[str, Any] | None = None,
) -> list[str]:
    """argv for the Merge-stage final local pass."""
    return build_runner_command(
        agent,
        prompt,
        codex_model=codex_model,
        workspace_path=workspace_path,
        mcp_servers=mcp_servers,
    )


_PR_URL_RE = re.compile(r"/pull/(\d+)")


def pr_number_from_url(url: str) -> int | None:
    """Extract the PR number from a `gh pr create` URL.

    `gh pr create` prints `https://github.com/OWNER/REPO/pull/<N>` on
    success (sometimes with trailing whitespace). The Review-stage poll
    needs that `<N>` to post `@codex review` and to fetch the snapshot.
    """
    if not url:
        return None
    m = _PR_URL_RE.search(url.strip())
    if m is None:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _parse_rfc3339(s: str) -> datetime:
    """Linear timestamps end in `Z`; Python's `fromisoformat` accepts the
    `+00:00` form. Normalize before parsing."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _pr_view_is_merged(view: dict[str, object]) -> bool:
    return (
        bool(view.get("mergedAt"))
        or bool(view.get("merged"))
        or str(view.get("state") or "").upper() == "MERGED"
    )


def _pr_view_is_closed(view: dict[str, object]) -> bool:
    return str(view.get("state") or "").upper() == "CLOSED"


def _pr_view_has_merge_conflict(view: dict[str, object]) -> bool:
    mergeable = str(view.get("mergeable") or "").upper()
    merge_state = str(
        view.get("mergeStateStatus") or view.get("merge_state_status") or ""
    ).upper()
    return mergeable == "CONFLICTING" or merge_state == "DIRTY"


def _pr_view_skips_required_check_fix(view: dict[str, object]) -> bool:
    mergeable = str(view.get("mergeable") or "").upper()
    merge_state = str(
        view.get("mergeStateStatus") or view.get("merge_state_status") or ""
    ).upper()
    return (
        mergeable == "CONFLICTING"
        or merge_state in {"BEHIND", "CONFLICTING", "DIRTY"}
    )


def _pr_view_is_clean_mergeable(view: dict[str, object]) -> bool:
    mergeable = str(view.get("mergeable") or "").upper()
    merge_state = str(
        view.get("mergeStateStatus") or view.get("merge_state_status") or ""
    ).upper()
    return mergeable == "MERGEABLE" and merge_state == "CLEAN"


def _pr_base_ref_from_view(view: dict[str, object]) -> str | None:
    raw = view.get("baseRefName") or view.get("base_ref_name") or view.get("baseRef")
    if raw is None:
        return None
    base_ref = str(raw).strip()
    return base_ref or None


_REQUIRED_CHECK_FAILURE_STATES = {
    "FAILURE",
    "FAILED",
    "ERROR",
    "CANCELLED",
    "CANCELED",
    "TIMED_OUT",
    "ACTION_REQUIRED",
    "STARTUP_FAILURE",
    "STALE",
}


def _status_rollup_nodes(raw: object) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [entry for entry in raw if isinstance(entry, dict)]
    if not isinstance(raw, dict):
        return []
    nodes = raw.get("nodes")
    if isinstance(nodes, list):
        return [entry for entry in nodes if isinstance(entry, dict)]
    edges = raw.get("edges")
    if isinstance(edges, list):
        return [
            edge["node"]
            for edge in edges
            if isinstance(edge, dict) and isinstance(edge.get("node"), dict)
        ]
    contexts = raw.get("contexts")
    if isinstance(contexts, list):
        return [entry for entry in contexts if isinstance(entry, dict)]
    return []


def _status_check_identity(check: Mapping[str, object]) -> str:
    return (
        str(check.get("context") or "").strip()
        or str(check.get("name") or "").strip()
        or str(check.get("workflowName") or "").strip()
        or "(unnamed)"
    )


def _status_check_names(check: Mapping[str, object]) -> set[str]:
    names: set[str] = set()
    for key in ("context", "name", "workflowName"):
        value = str(check.get(key) or "").strip()
        if value:
            names.add(value)
    return names


def _status_check_sha(check: Mapping[str, object]) -> str:
    for key in ("sha", "commitOid", "commit_oid"):
        value = str(check.get(key) or "").strip()
        if value:
            return value
    commit = check.get("commit")
    if isinstance(commit, Mapping):
        return str(commit.get("oid") or commit.get("sha") or "").strip()
    return ""


def _status_check_failed(check: Mapping[str, object]) -> bool:
    state = str(
        check.get("state") or check.get("status") or check.get("__typename") or ""
    ).upper()
    conclusion = str(check.get("conclusion") or "").upper()
    return (
        state in _REQUIRED_CHECK_FAILURE_STATES
        or conclusion in _REQUIRED_CHECK_FAILURE_STATES
    )


# Terminal-success states across both rollup shapes: a `StatusContext` reports
# `state`, a `CheckRun` reports `status`+`conclusion`. SKIPPED/NEUTRAL count as
# non-blocking passes (GitHub treats them as green for branch protection).
_STATUS_CHECK_SUCCESS_STATES = {"SUCCESS", "NEUTRAL", "SKIPPED"}


def _status_check_succeeded(check: Mapping[str, object]) -> bool:
    """True only when *check* has completed successfully (SYM-108).

    A `CheckRun` that has not reached `COMPLETED` is still in flight, so it is
    neither a success nor a failure — the caller treats it as pending.
    """
    if _status_check_failed(check):
        return False
    status = str(check.get("status") or "").upper()
    if status and status != "COMPLETED":
        return False
    conclusion = str(check.get("conclusion") or "").upper()
    if conclusion:
        return conclusion in _STATUS_CHECK_SUCCESS_STATES
    state = str(check.get("state") or "").upper()
    if state:
        return state in _STATUS_CHECK_SUCCESS_STATES
    return False


def _no_signal_head_check_state(view: dict[str, object]) -> str:
    """Classify the CI rollup on the PR head for the no_signal merge gate.

    Returns "green" (≥1 check, all complete and successful), "failed" (≥1
    check failed), "pending" (≥1 check, none failed but some still running),
    or "none" (no check reports on the head). SYM-108: a clean no_signal
    bypass merges only on "green"; "none" needs a verify_cmd/opt-in; "pending"
    keeps polling; "failed" defers to the review/required-check fix path.
    """
    head_sha = str(view.get("headRefOid") or "")
    nodes: list[dict[str, Any]] = []
    for check in _status_rollup_nodes(view.get("statusCheckRollup")):
        check_sha = _status_check_sha(check)
        if check_sha and head_sha and check_sha != head_sha:
            continue
        nodes.append(check)
    if not nodes:
        return "none"
    if any(_status_check_failed(check) for check in nodes):
        return "failed"
    if all(_status_check_succeeded(check) for check in nodes):
        return "green"
    return "pending"


def _required_check_detail(check: Mapping[str, object]) -> dict[str, object]:
    detail: dict[str, object] = {}
    for key in (
        "__typename",
        "name",
        "context",
        "workflowName",
        "state",
        "status",
        "conclusion",
        "targetUrl",
        "detailsUrl",
        "description",
    ):
        value = check.get(key)
        if value is not None:
            detail[key] = value
    run_id = _status_check_run_id(check)
    if run_id:
        detail["runId"] = run_id
    return detail


def _status_check_run_id(check: Mapping[str, object]) -> str:
    for key in ("runId", "run_id"):
        value = str(check.get(key) or "").strip()
        if value:
            return value
    workflow_run = check.get("workflowRun")
    if isinstance(workflow_run, Mapping):
        for key in ("databaseId", "database_id", "id"):
            value = str(workflow_run.get(key) or "").strip()
            if value:
                return value
    for key in ("detailsUrl", "targetUrl"):
        url = str(check.get(key) or "")
        match = re.search(r"/actions/runs/([^/?#]+)", url)
        if match is not None:
            return match.group(1)
    for key in ("databaseId", "database_id"):
        value = str(check.get(key) or "").strip()
        if value:
            return value
    return ""


def _required_check_trigger_signature(
    *,
    head_sha: str,
    failing_checks: list[dict[str, object]],
) -> str:
    contexts = sorted(_status_check_identity(check) for check in failing_checks)
    contexts_hash = hashlib.sha256("\n".join(contexts).encode("utf-8")).hexdigest()[:12]
    return f"required_check_failure:{head_sha}:{contexts_hash}"


def _github_commit_url(repo: str, sha: str) -> str:
    """Return a browser commit URL for *sha* in [HOST/]OWNER/REPO."""
    if not sha:
        return ""
    parts = repo.split("/")
    if len(parts) == 3:
        host, owner, name = parts
    elif len(parts) == 2:
        host = "github.com"
        owner, name = parts
    else:
        return ""
    return f"https://{host}/{owner}/{name}/commit/{sha}"


def _pr_url_for_state(
    *, repo: str, pr_number: int | None, pr_url: str
) -> str:
    if pr_url:
        return pr_url
    if pr_number is not None:
        return f"https://github.com/{repo}/pull/{pr_number}"
    return "(no PR)"
