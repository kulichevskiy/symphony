"""Thin wrappers around the ``gh`` CLI for issues, PRs, and dependency state.

Symphony shells out to ``gh`` for everything GitHub — no PAT in the process,
no octokit dependency. Each function takes ``repo_path`` so ``gh`` resolves
the correct repository from the local git remote.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

ISSUE_FIELDS = "number,title,body,comments,labels,createdAt"

# ``trackedIssues`` returns issues referenced as task-list items in the parent
# issue body. ``closedByPullRequestsReferences`` gives the PR (if any) that
# closed each one — handy for rendering "satisfied dependencies" in the prompt.
_TRACKED_ISSUES_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $name) {
    issue(number: $number) {
      trackedIssues(first: 100, after: $after) {
        nodes {
          number
          title
          state
          stateReason
          closedByPullRequestsReferences(first: 1, includeClosedPrs: true) {
            nodes { url }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
}
"""


class GithubError(Exception):
    """Raised when a ``gh`` invocation fails or returns unexpected JSON."""


@dataclass(frozen=True)
class IssueComment:
    author: str
    body: str


@dataclass(frozen=True)
class Issue:
    number: int
    title: str
    body: str
    labels: list[str]
    comments: list[IssueComment]
    created_at: str = ""


@dataclass(frozen=True)
class TrackedIssue:
    number: int
    title: str
    state: str
    state_reason: str | None
    pr_url: str | None


@dataclass(frozen=True)
class PR:
    number: int
    url: str


@dataclass(frozen=True)
class Review:
    """A pull-request review submission. ``state`` is the GitHub review state
    (``APPROVED``, ``CHANGES_REQUESTED``, ``COMMENTED``). ``commit_sha`` is the
    HEAD the reviewer was looking at — used to ignore stale reviews."""

    id: int
    user_login: str
    state: str
    body: str
    commit_sha: str
    submitted_at: str


@dataclass(frozen=True)
class ReviewComment:
    """An inline (line-level) review comment on a PR diff."""

    id: int
    user_login: str
    path: str
    line: int | None
    body: str
    commit_sha: str
    created_at: str
    review_id: int = 0


@dataclass(frozen=True)
class Reaction:
    """A reaction on the PR's underlying issue (Codex's ``+1`` lives here)."""

    user_login: str
    content: str
    created_at: str


@dataclass(frozen=True)
class CheckRun:
    """One CI check run or status context on the PR's HEAD commit."""

    name: str
    status: str
    conclusion: str | None
    details_url: str | None
    app_id: int | None = None
    required: bool | None = True


@dataclass(frozen=True)
class RequiredStatusCheck:
    context: str
    app_id: int | None = None


def _run_gh(
    args: list[str],
    *,
    cwd: Path | None = None,
    allowed_exit_codes: set[int] | tuple[int, ...] = (0,),
) -> str:
    """Run ``gh`` with the given args and return stdout.

    Raises :class:`GithubError` on unexpected exit; stderr is included in the
    message so callers can show actionable failures without re-running.
    """
    try:
        res = subprocess.run(
            ["gh", *args],
            cwd=str(cwd) if cwd else None,
            check=False,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise GithubError(
            f"gh {' '.join(args)} failed (exit {e.returncode}): {e.stderr.strip()}"
        ) from e
    if res.returncode not in allowed_exit_codes:
        raise GithubError(
            f"gh {' '.join(args)} failed (exit {res.returncode}): {res.stderr.strip()}"
        )
    return res.stdout


def _parse_json(stdout: str, *, context: str) -> Any:
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as e:
        raise GithubError(f"could not parse JSON from {context}: {e}") from e


def _flatten_paginated_list(data: Any, *, context: str) -> list[Any]:
    """Flatten ``gh api --paginate --slurp`` output for list endpoints."""
    if not isinstance(data, list):
        raise GithubError(f"unexpected paginated JSON from {context}: {data!r}")
    if not data:
        return []
    if all(isinstance(page, list) for page in data):
        return [item for page in data for item in page]
    if all(isinstance(item, dict) for item in data):
        return data
    raise GithubError(f"unexpected paginated JSON from {context}: {data!r}")


def _flatten_paginated_object_list(data: Any, *, key: str, context: str) -> list[Any]:
    """Flatten ``gh api --paginate --slurp`` output for object-list endpoints."""
    if isinstance(data, dict):
        pages = [data]
    elif isinstance(data, list):
        pages = data
    else:
        raise GithubError(f"unexpected paginated JSON from {context}: {data!r}")

    flattened: list[Any] = []
    for page in pages:
        if not isinstance(page, dict):
            raise GithubError(f"unexpected paginated JSON from {context}: {data!r}")
        items = page.get(key, [])
        if not isinstance(items, list):
            raise GithubError(f"unexpected {key!r} payload from {context}: {page!r}")
        flattened.extend(items)
    return flattened


def _api_paginated_list(endpoint: str, *, repo_path: Path, context: str) -> list[Any]:
    out = _run_gh(["api", endpoint, "--paginate", "--slurp"], cwd=repo_path)
    data = _parse_json(out, context=context)
    return _flatten_paginated_list(data, context=context)


def _api_paginated_object_list(
    endpoint: str, *, repo_path: Path, key: str, context: str
) -> list[Any]:
    out = _run_gh(["api", endpoint, "--paginate", "--slurp"], cwd=repo_path)
    data = _parse_json(out, context=context)
    return _flatten_paginated_object_list(data, key=key, context=context)


def view_issue(number: int, *, repo_path: Path) -> Issue:
    out = _run_gh(
        ["issue", "view", str(number), "--json", ISSUE_FIELDS],
        cwd=repo_path,
    )
    data = _parse_json(out, context=f"gh issue view {number}")
    # `author` is nullable in the GitHub API (deleted accounts), so a plain
    # `c.get("author", {}).get(...)` chain crashes when the key exists with
    # value `None`. Treat missing-or-null author as an empty login.
    comments = [
        IssueComment(
            author=(c.get("author") or {}).get("login", ""),
            body=c.get("body", ""),
        )
        for c in data.get("comments", [])
    ]
    labels = [lbl.get("name", "") for lbl in data.get("labels", [])]
    return Issue(
        number=int(data["number"]),
        title=data.get("title", ""),
        body=data.get("body", ""),
        labels=labels,
        comments=comments,
        created_at=data.get("createdAt", ""),
    )


def list_open_issues_with_label(
    label: str, *, repo_path: Path, limit: int = 1000
) -> list[Issue]:
    """All open issues carrying ``label``, with the same shape as ``view_issue``.

    Used by the orchestrator's poll loop to find candidates. ``createdAt``
    drives the FIFO selection so the oldest ready issue dispatches first.

    ``gh issue list --limit`` is a fetch cap on the most-recent items: with
    the previous default of 100, an ``auto`` backlog larger than that would
    silently starve older issues (the FIFO sort only applied within the
    truncated subset). The default is now high enough to cover any realistic
    personal-autopilot backlog; bump explicitly if you really do have more.
    """
    out = _run_gh(
        [
            "issue",
            "list",
            "--label",
            label,
            "--state",
            "open",
            "--limit",
            str(limit),
            "--json",
            ISSUE_FIELDS,
        ],
        cwd=repo_path,
    )
    data = _parse_json(out, context=f"gh issue list --label {label}")
    issues: list[Issue] = []
    for d in data:
        comments = [
            IssueComment(
                author=(c.get("author") or {}).get("login", ""),
                body=c.get("body", ""),
            )
            for c in d.get("comments", [])
        ]
        labels = [lbl.get("name", "") for lbl in d.get("labels", [])]
        issues.append(
            Issue(
                number=int(d["number"]),
                title=d.get("title", ""),
                body=d.get("body", ""),
                labels=labels,
                comments=comments,
                created_at=d.get("createdAt", ""),
            )
        )
    return issues


def get_issue_state(number: int, *, repo_path: Path) -> str:
    """Return the issue's state — ``"OPEN"`` or ``"CLOSED"``.

    Used by the GC paths to decide whether an orphan worktree's underlying
    issue is still in flight or done. Raises :class:`GithubError` on any
    other shape so callers can log + skip rather than guess.
    """
    out = _run_gh(
        ["issue", "view", str(number), "--json", "state"],
        cwd=repo_path,
    )
    data = _parse_json(out, context=f"gh issue view {number} (state)")
    state = str(data.get("state") or "")
    if state not in {"OPEN", "CLOSED"}:
        raise GithubError(f"unexpected issue state for #{number}: {state!r}")
    return state


def find_pr_for_branch(
    branch: str,
    *,
    repo_path: Path,
    base_branch: str | None = None,
    expected_owner: str | None = None,
) -> tuple[PR, str] | None:
    """Return ``(PR, state)`` for the most-recent PR whose head ref is
    ``branch`` in any state, or ``None`` if no PR ever existed.

    ``state`` is GitHub's PR state (``"OPEN"``, ``"CLOSED"``, ``"MERGED"``).
    Used by GC to tell ``"merged PR"`` from ``"PR closed without merge"`` from
    ``"never opened a PR"``. Disambiguates by ``base_branch`` and
    ``expected_owner`` like :func:`find_open_pr_for_branch`.
    """
    args = [
        "pr",
        "list",
        "--head",
        branch,
        "--state",
        "all",
        "--json",
        "number,url,state,baseRefName,headRepositoryOwner",
    ]
    if base_branch:
        args += ["--base", base_branch]
    out = _run_gh(args, cwd=repo_path)
    data = _parse_json(out, context=f"gh pr list --head {branch} --state all")
    best: tuple[PR, str] | None = None
    best_number = -1
    for entry in data:
        if base_branch and entry.get("baseRefName") != base_branch:
            continue
        if expected_owner:
            owner = (entry.get("headRepositoryOwner") or {}).get("login", "")
            if owner != expected_owner:
                continue
        pr_number = int(entry["number"])
        if pr_number <= best_number:
            continue
        best = (
            PR(number=pr_number, url=entry.get("url", "")),
            str(entry.get("state") or ""),
        )
        best_number = pr_number
    return best


def name_with_owner(repo_path: Path) -> tuple[str, str]:
    """Resolve the GitHub ``(owner, name)`` for the repo at ``repo_path``."""
    out = _run_gh(["repo", "view", "--json", "nameWithOwner"], cwd=repo_path)
    data = _parse_json(out, context="gh repo view")
    nwo = data.get("nameWithOwner", "")
    if "/" not in nwo:
        raise GithubError(f"unexpected nameWithOwner: {nwo!r}")
    owner, name = nwo.split("/", 1)
    return owner, name


# Backwards-compat private alias used elsewhere in this module.
_name_with_owner = name_with_owner


def tracked_issues(number: int, *, repo_path: Path) -> list[TrackedIssue]:
    owner, name = _name_with_owner(repo_path)
    results: list[TrackedIssue] = []
    after: str | None = None
    while True:
        args = [
            "api",
            "graphql",
            "-F",
            f"owner={owner}",
            "-F",
            f"name={name}",
            "-F",
            f"number={number}",
        ]
        if after is not None:
            args += ["-F", f"after={after}"]
        args += ["-f", f"query={_TRACKED_ISSUES_QUERY}"]

        out = _run_gh(args, cwd=repo_path)
        data = _parse_json(out, context="gh api graphql trackedIssues")
        try:
            tracked = data["data"]["repository"]["issue"]["trackedIssues"]
            nodes = tracked["nodes"]
        except (KeyError, TypeError) as e:
            raise GithubError(f"unexpected GraphQL response shape: {data!r}") from e

        for n in nodes:
            prs = n.get("closedByPullRequestsReferences", {}).get("nodes", [])
            pr_url = prs[0]["url"] if prs else None
            results.append(
                TrackedIssue(
                    number=int(n["number"]),
                    title=n.get("title", ""),
                    state=n.get("state", ""),
                    state_reason=n.get("stateReason"),
                    pr_url=pr_url,
                )
            )

        page_info = tracked.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            return results
        after = page_info.get("endCursor")
        if after is None:
            raise GithubError(
                f"trackedIssues pageInfo.hasNextPage without endCursor: {data!r}"
            )


def find_open_pr_for_branch(
    branch: str,
    *,
    repo_path: Path,
    base_branch: str | None = None,
    expected_owner: str | None = None,
) -> PR | None:
    """Return the open PR whose head ref is ``branch``, if any.

    Used by ``run_once`` to make re-dispatch idempotent: a second run on the
    same issue should reuse the existing PR rather than failing on
    ``gh pr create``'s duplicate-PR error.

    ``--head`` filters by branch name only — multiple open PRs (e.g. from
    forks) can match the same head ref name. Callers should pass
    ``base_branch`` and ``expected_owner`` so this picks the PR for *this*
    repo+base, not a stranger's same-named branch.
    """
    args = [
        "pr",
        "list",
        "--head",
        branch,
        "--state",
        "open",
        "--json",
        "number,url,baseRefName,headRepositoryOwner",
    ]
    if base_branch:
        args += ["--base", base_branch]
    out = _run_gh(args, cwd=repo_path)
    data = _parse_json(out, context=f"gh pr list --head {branch}")
    for entry in data:
        if base_branch and entry.get("baseRefName") != base_branch:
            continue
        if expected_owner:
            owner = (entry.get("headRepositoryOwner") or {}).get("login", "")
            if owner != expected_owner:
                continue
        return PR(number=int(entry["number"]), url=entry.get("url", ""))
    return None


def open_pr(
    *,
    repo_path: Path,
    head: str,
    base: str,
    title: str,
    body: str,
) -> PR:
    """Create a PR from ``head`` into ``base`` and return its number + url.

    ``gh pr create`` prints the PR URL to stdout; we re-fetch via ``gh pr view``
    on that URL to get a structured number, avoiding URL parsing.
    """
    create_out = _run_gh(
        [
            "pr",
            "create",
            "--head",
            head,
            "--base",
            base,
            "--title",
            title,
            "--body",
            body,
        ],
        cwd=repo_path,
    )
    url = create_out.strip().splitlines()[-1].strip() if create_out.strip() else ""
    if not url:
        raise GithubError("gh pr create returned no URL")
    out = _run_gh(["pr", "view", url, "--json", "number,url"], cwd=repo_path)
    data = _parse_json(out, context="gh pr view")
    return PR(number=int(data["number"]), url=data.get("url", url))


def comment_pr(*, repo_path: Path, pr_number: int, body: str) -> None:
    _run_gh(
        ["pr", "comment", str(pr_number), "--body", body],
        cwd=repo_path,
    )


def arm_auto_merge(
    *,
    repo_path: Path,
    pr_number: int,
    method: str = "squash",
) -> None:
    flag = {"squash": "--squash", "merge": "--merge", "rebase": "--rebase"}[method]
    _run_gh(
        ["pr", "merge", str(pr_number), "--auto", flag, "--delete-branch"],
        cwd=repo_path,
    )


def merge_pr(
    *,
    repo_path: Path,
    pr_number: int,
    method: str = "squash",
    match_head_commit: str | None = None,
    match_head_sha: str | None = None,
) -> None:
    """Merge a PR after Symphony's review loop reaches an approved verdict."""
    flag = {"squash": "--squash", "merge": "--merge", "rebase": "--rebase"}[method]
    args = ["pr", "merge", str(pr_number), flag, "--delete-branch"]
    match_head = match_head_commit or match_head_sha
    if match_head:
        args += ["--match-head-commit", match_head]
    _run_gh(
        args,
        cwd=repo_path,
    )


def get_pr_head_sha(pr_number: int, *, repo_path: Path) -> str:
    out = _run_gh(
        ["pr", "view", str(pr_number), "--json", "headRefOid"], cwd=repo_path
    )
    data = _parse_json(out, context=f"gh pr view {pr_number}")
    sha = data.get("headRefOid", "")
    if not sha:
        raise GithubError(f"PR #{pr_number} has no headRefOid")
    return sha


def _as_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _required_status_checks(
    *, owner: str, name: str, base_branch: str, repo_path: Path
) -> tuple[RequiredStatusCheck, ...] | None:
    if not base_branch:
        return None
    encoded_branch = quote(base_branch, safe="")
    try:
        out = _run_gh(
            [
                "api",
                f"repos/{owner}/{name}/branches/{encoded_branch}/protection/required_status_checks",
            ],
            cwd=repo_path,
        )
    except GithubError as e:
        message = str(e)
        if "403" in message or "404" in message:
            return None
        raise
    data = _parse_json(
        out,
        context=f"required status checks for branch {base_branch}",
    )
    required: list[RequiredStatusCheck] = []
    check_contexts: set[str] = set()
    for check in data.get("checks") or []:
        if not isinstance(check, dict):
            continue
        context = check.get("context")
        if context:
            check_contexts.add(str(context))
            required.append(
                RequiredStatusCheck(
                    context=str(context),
                    app_id=_as_int_or_none(check.get("app_id")),
                )
            )
    required.extend(
        RequiredStatusCheck(context=str(context))
        for context in data.get("contexts") or []
        if context and str(context) not in check_contexts
    )
    return tuple(required)


def _matches_required_check(
    name: str,
    app_id: int | None,
    required_checks: tuple[RequiredStatusCheck, ...],
) -> bool:
    for required in required_checks:
        if required.context != name:
            continue
        if required.app_id in (None, -1) or required.app_id == app_id:
            return True
    return False


def _is_required_check(
    name: str,
    app_id: int | None,
    required_checks: tuple[RequiredStatusCheck, ...] | None,
) -> bool | None:
    if required_checks is None:
        return None
    return _matches_required_check(name, app_id, required_checks)


def _check_run_timestamp(check_run: dict[str, Any]) -> str:
    return str(
        check_run.get("completed_at")
        or check_run.get("started_at")
        or check_run.get("created_at")
        or check_run.get("updated_at")
        or ""
    )


def is_pr_merged(pr_number: int, *, repo_path: Path) -> bool:
    out = _run_gh(
        ["pr", "view", str(pr_number), "--json", "state,mergedAt"],
        cwd=repo_path,
    )
    data = _parse_json(out, context=f"gh pr view {pr_number}")
    return bool(data.get("mergedAt")) or data.get("state") == "MERGED"


def list_pr_reviews(pr_number: int, *, repo_path: Path) -> list[Review]:
    """All review submissions on a PR, oldest first."""
    owner, name = _name_with_owner(repo_path)
    data = _api_paginated_list(
        f"repos/{owner}/{name}/pulls/{pr_number}/reviews",
        repo_path=repo_path,
        context=f"reviews for PR {pr_number}",
    )
    return [
        Review(
            id=int(r.get("id", 0)),
            user_login=(r.get("user") or {}).get("login", ""),
            state=r.get("state", ""),
            body=r.get("body") or "",
            commit_sha=r.get("commit_id", ""),
            submitted_at=r.get("submitted_at", ""),
        )
        for r in data
    ]


def list_pr_review_comments(pr_number: int, *, repo_path: Path) -> list[ReviewComment]:
    """All inline (line-level) review comments on a PR's diff."""
    owner, name = _name_with_owner(repo_path)
    data = _api_paginated_list(
        f"repos/{owner}/{name}/pulls/{pr_number}/comments",
        repo_path=repo_path,
        context=f"review comments for PR {pr_number}",
    )
    return [
        ReviewComment(
            id=int(c.get("id", 0)),
            user_login=(c.get("user") or {}).get("login", ""),
            path=c.get("path", ""),
            line=c.get("line"),
            body=c.get("body") or "",
            commit_sha=c.get("commit_id", ""),
            created_at=c.get("created_at", ""),
            review_id=int(c.get("pull_request_review_id", 0) or 0),
        )
        for c in data
    ]


def list_pr_reactions(pr_number: int, *, repo_path: Path) -> list[Reaction]:
    """Reactions on the PR's underlying issue. Codex's approval ``+1`` lives here."""
    owner, name = _name_with_owner(repo_path)
    data = _api_paginated_list(
        f"repos/{owner}/{name}/issues/{pr_number}/reactions",
        repo_path=repo_path,
        context=f"reactions for PR {pr_number}",
    )
    return [
        Reaction(
            user_login=(r.get("user") or {}).get("login", ""),
            content=r.get("content", ""),
            created_at=r.get("created_at", ""),
        )
        for r in data
    ]


def list_pr_checks(
    pr_number: int, *, repo_path: Path, head_sha: str | None = None
) -> list[CheckRun]:
    """CI check runs and status contexts on the PR's HEAD commit."""
    owner, name = _name_with_owner(repo_path)
    pr_out = _run_gh(
        ["pr", "view", str(pr_number), "--json", "headRefOid,baseRefName"],
        cwd=repo_path,
    )
    pr_data = _parse_json(pr_out, context=f"gh pr view {pr_number}")
    sha = head_sha or pr_data.get("headRefOid", "")
    if not sha:
        raise GithubError(f"PR #{pr_number} has no headRefOid")
    required_checks = _required_status_checks(
        owner=owner,
        name=name,
        base_branch=str(pr_data.get("baseRefName") or ""),
        repo_path=repo_path,
    )
    check_runs_out = _run_gh(
        [
            "api",
            f"repos/{owner}/{name}/commits/{sha}/check-runs?per_page=100",
            "--paginate",
            "--slurp",
        ],
        cwd=repo_path,
    )
    check_run_pages = _parse_json(
        check_runs_out, context=f"check runs for PR {pr_number}"
    )
    status_out = _run_gh(
        [
            "api",
            f"repos/{owner}/{name}/commits/{sha}/status?per_page=100",
            "--paginate",
            "--slurp",
        ],
        cwd=repo_path,
    )
    status_pages = _parse_json(status_out, context=f"status contexts for PR {pr_number}")

    latest_check_runs: dict[tuple[str, int | None], tuple[str, int, CheckRun]] = {}
    check_index = 0
    for page in check_run_pages:
        for c in page.get("check_runs", []):
            check_name = str(c.get("name", ""))
            app_id = _as_int_or_none((c.get("app") or {}).get("id"))
            check = CheckRun(
                name=check_name,
                status=c.get("status", ""),
                conclusion=c.get("conclusion") or None,
                details_url=c.get("details_url") or None,
                app_id=app_id,
                required=_is_required_check(
                    check_name,
                    app_id,
                    required_checks,
                ),
            )
            key = (check_name, app_id)
            timestamp = _check_run_timestamp(c)
            current = latest_check_runs.get(key)
            if current is None or (timestamp, check_index) > (current[0], current[1]):
                latest_check_runs[key] = (timestamp, check_index, check)
            check_index += 1
    checks = [entry[2] for entry in latest_check_runs.values()]
    latest_statuses: dict[str, Any] = {}
    for page in status_pages:
        for status in page.get("statuses", []):
            context = str(status.get("context") or "")
            current = latest_statuses.get(context)
            timestamp = str(status.get("created_at") or status.get("updated_at") or "")
            current_timestamp = (
                str(current.get("created_at") or current.get("updated_at") or "")
                if current is not None
                else ""
            )
            if current is None or timestamp > current_timestamp:
                latest_statuses[context] = status
    for status in latest_statuses.values():
        state = str(status.get("state") or "").lower()
        if state == "success":
            check_status, conclusion = "completed", "success"
        elif state in {"error", "failure"}:
            check_status, conclusion = "completed", "failure"
        else:
            check_status, conclusion = "in_progress", None
        checks.append(
            CheckRun(
                name=str(status.get("context") or ""),
                status=check_status,
                conclusion=conclusion,
                details_url=status.get("target_url") or None,
                required=_is_required_check(
                    str(status.get("context") or ""),
                    None,
                    required_checks,
                ),
            )
        )
    return checks


def label_issue(number: int, label: str, *, repo_path: Path) -> None:
    """Add ``label`` to the issue (or PR) with the given number. Idempotent."""
    _run_gh(
        ["issue", "edit", str(number), "--add-label", label], cwd=repo_path
    )


def get_commit_committed_at(sha: str, *, repo_path: Path) -> str:
    """ISO timestamp for ``sha``'s committer date — used to gate Codex's
    ``+1`` reaction (only count reactions newer than the commit they
    presumably refer to)."""
    owner, name = _name_with_owner(repo_path)
    out = _run_gh(
        ["api", f"repos/{owner}/{name}/commits/{sha}"], cwd=repo_path
    )
    data = _parse_json(out, context=f"commit {sha}")
    try:
        return data["commit"]["committer"]["date"]
    except (KeyError, TypeError) as e:
        raise GithubError(f"missing committer.date for {sha}") from e
