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

ISSUE_FIELDS = "number,title,body,comments,labels,createdAt"

# ``trackedIssues`` returns issues referenced as task-list items in the parent
# issue body. ``closedByPullRequestsReferences`` gives the PR (if any) that
# closed each one — handy for rendering "satisfied dependencies" in the prompt.
_TRACKED_ISSUES_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    issue(number: $number) {
      trackedIssues(first: 50) {
        nodes {
          number
          title
          state
          stateReason
          closedByPullRequestsReferences(first: 1, includeClosedPrs: true) {
            nodes { url }
          }
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


@dataclass(frozen=True)
class Reaction:
    """A reaction on the PR's underlying issue (Codex's ``+1`` lives here)."""

    user_login: str
    content: str
    created_at: str


@dataclass(frozen=True)
class CheckRun:
    """One CI check run on the PR's HEAD commit."""

    name: str
    status: str
    conclusion: str | None
    details_url: str | None


def _run_gh(args: list[str], *, cwd: Path | None = None) -> str:
    """Run ``gh`` with the given args and return stdout.

    Raises :class:`GithubError` on non-zero exit; stderr is included in the
    message so callers can show actionable failures without re-running.
    """
    try:
        res = subprocess.run(
            ["gh", *args],
            cwd=str(cwd) if cwd else None,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise GithubError(
            f"gh {' '.join(args)} failed (exit {e.returncode}): {e.stderr.strip()}"
        ) from e
    return res.stdout


def _parse_json(stdout: str, *, context: str) -> Any:
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as e:
        raise GithubError(f"could not parse JSON from {context}: {e}") from e


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
    out = _run_gh(
        [
            "api",
            "graphql",
            "-F",
            f"owner={owner}",
            "-F",
            f"name={name}",
            "-F",
            f"number={number}",
            "-f",
            f"query={_TRACKED_ISSUES_QUERY}",
        ],
        cwd=repo_path,
    )
    data = _parse_json(out, context="gh api graphql trackedIssues")
    try:
        nodes = data["data"]["repository"]["issue"]["trackedIssues"]["nodes"]
    except (KeyError, TypeError) as e:
        raise GithubError(f"unexpected GraphQL response shape: {data!r}") from e

    results: list[TrackedIssue] = []
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
    return results


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


def get_pr_head_sha(pr_number: int, *, repo_path: Path) -> str:
    out = _run_gh(
        ["pr", "view", str(pr_number), "--json", "headRefOid"], cwd=repo_path
    )
    data = _parse_json(out, context=f"gh pr view {pr_number}")
    sha = data.get("headRefOid", "")
    if not sha:
        raise GithubError(f"PR #{pr_number} has no headRefOid")
    return sha


def list_pr_reviews(pr_number: int, *, repo_path: Path) -> list[Review]:
    """All review submissions on a PR, oldest first."""
    owner, name = _name_with_owner(repo_path)
    out = _run_gh(
        ["api", f"repos/{owner}/{name}/pulls/{pr_number}/reviews", "--paginate"],
        cwd=repo_path,
    )
    data = _parse_json(out, context=f"reviews for PR {pr_number}")
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
    out = _run_gh(
        ["api", f"repos/{owner}/{name}/pulls/{pr_number}/comments", "--paginate"],
        cwd=repo_path,
    )
    data = _parse_json(out, context=f"review comments for PR {pr_number}")
    return [
        ReviewComment(
            id=int(c.get("id", 0)),
            user_login=(c.get("user") or {}).get("login", ""),
            path=c.get("path", ""),
            line=c.get("line"),
            body=c.get("body") or "",
            commit_sha=c.get("commit_id", ""),
            created_at=c.get("created_at", ""),
        )
        for c in data
    ]


def list_pr_reactions(pr_number: int, *, repo_path: Path) -> list[Reaction]:
    """Reactions on the PR's underlying issue. Codex's approval ``+1`` lives here."""
    owner, name = _name_with_owner(repo_path)
    out = _run_gh(
        ["api", f"repos/{owner}/{name}/issues/{pr_number}/reactions", "--paginate"],
        cwd=repo_path,
    )
    data = _parse_json(out, context=f"reactions for PR {pr_number}")
    return [
        Reaction(
            user_login=(r.get("user") or {}).get("login", ""),
            content=r.get("content", ""),
            created_at=r.get("created_at", ""),
        )
        for r in data
    ]


def list_pr_checks(pr_number: int, *, repo_path: Path) -> list[CheckRun]:
    """CI check runs on the PR's HEAD commit (`gh pr checks`)."""
    out = _run_gh(
        [
            "pr",
            "checks",
            str(pr_number),
            "--json",
            "name,status,conclusion,detailsUrl",
        ],
        cwd=repo_path,
    )
    data = _parse_json(out, context=f"checks for PR {pr_number}")
    return [
        CheckRun(
            name=c.get("name", ""),
            status=c.get("status", ""),
            conclusion=c.get("conclusion") or None,
            details_url=c.get("detailsUrl") or None,
        )
        for c in data
    ]


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
