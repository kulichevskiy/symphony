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

ISSUE_FIELDS = "number,title,body,comments,labels"

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
    )


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


def find_open_pr_for_branch(branch: str, *, repo_path: Path) -> PR | None:
    """Return the open PR whose head ref is ``branch``, if any.

    Used by ``run_once`` to make re-dispatch idempotent: a second run on the
    same issue should reuse the existing PR rather than failing on
    ``gh pr create``'s duplicate-PR error.
    """
    out = _run_gh(
        [
            "pr",
            "list",
            "--head",
            branch,
            "--state",
            "open",
            "--json",
            "number,url",
        ],
        cwd=repo_path,
    )
    data = _parse_json(out, context=f"gh pr list --head {branch}")
    if not data:
        return None
    first = data[0]
    return PR(number=int(first["number"]), url=first.get("url", ""))


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
