"""Tests for symphony.github — `gh` CLI wrappers.

We monkeypatch the module-level ``_run_gh`` so tests don't shell out.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from symphony import github as gh_mod
from symphony.github import (
    GithubError,
    Issue,
    IssueComment,
    PR,
    TrackedIssue,
    arm_auto_merge,
    comment_pr,
    find_open_pr_for_branch,
    open_pr,
    tracked_issues,
    view_issue,
)


def _stub(responses: dict[tuple[str, ...], str]):
    """Make a `_run_gh` stub keyed by argv tuple prefix.

    The matched key is the longest prefix of args that appears in `responses`.
    Returns stdout for that key.
    """

    calls: list[list[str]] = []

    def _fake(args, *, cwd=None):  # type: ignore[no-untyped-def]
        calls.append(list(args))
        for length in range(len(args), 0, -1):
            key = tuple(args[:length])
            if key in responses:
                return responses[key]
        raise AssertionError(f"unexpected gh call: {args}")

    _fake.calls = calls  # type: ignore[attr-defined]
    return _fake


def test_view_issue_parses_gh_json(monkeypatch, tmp_path):
    payload = {
        "number": 3,
        "title": "Single-issue happy path",
        "body": "do the thing",
        "labels": [{"name": "auto"}, {"name": "p1"}],
        "comments": [
            {"author": {"login": "ak"}, "body": "hello"},
            {"author": {"login": "bot"}, "body": "ping"},
        ],
    }
    fake = _stub(
        {
            ("issue", "view", "3"): json.dumps(payload),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    issue = view_issue(3, repo_path=tmp_path)

    assert isinstance(issue, Issue)
    assert issue.number == 3
    assert issue.title == "Single-issue happy path"
    assert issue.body == "do the thing"
    assert issue.labels == ["auto", "p1"]
    assert issue.comments == [
        IssueComment(author="ak", body="hello"),
        IssueComment(author="bot", body="ping"),
    ]
    # cwd was forwarded so gh resolves the right repo
    assert fake.calls[0][:3] == ["issue", "view", "3"]
    assert "--json" in fake.calls[0]


def test_view_issue_handles_null_comment_author(monkeypatch, tmp_path):
    """Regression: GitHub returns ``"author": null`` for deleted accounts.

    A naive ``c.get("author", {}).get("login")`` crashes because ``.get``
    returns the existing ``None``, not the default. The wrapper must keep
    parsing and emit an empty-string login.
    """
    payload = {
        "number": 7,
        "title": "x",
        "body": "",
        "labels": [],
        "comments": [
            {"author": None, "body": "left in the open"},
            {"body": "no author key at all"},
        ],
    }
    monkeypatch.setattr(
        gh_mod, "_run_gh", _stub({("issue", "view", "7"): json.dumps(payload)})
    )
    issue = view_issue(7, repo_path=tmp_path)
    assert issue.comments == [
        IssueComment(author="", body="left in the open"),
        IssueComment(author="", body="no author key at all"),
    ]


def test_view_issue_handles_missing_optional_fields(monkeypatch, tmp_path):
    payload = {"number": 9, "title": "x", "body": "", "labels": [], "comments": []}
    monkeypatch.setattr(gh_mod, "_run_gh", _stub({("issue", "view", "9"): json.dumps(payload)}))
    issue = view_issue(9, repo_path=tmp_path)
    assert issue.body == ""
    assert issue.labels == []
    assert issue.comments == []


def test_tracked_issues_parses_graphql(monkeypatch, tmp_path):
    repo_payload = {"nameWithOwner": "owner/name"}
    gql_payload = {
        "data": {
            "repository": {
                "issue": {
                    "trackedIssues": {
                        "nodes": [
                            {
                                "number": 1,
                                "title": "scaffold",
                                "state": "CLOSED",
                                "stateReason": "COMPLETED",
                                "closedByPullRequestsReferences": {
                                    "nodes": [{"url": "https://x/pr/8"}]
                                },
                            },
                            {
                                "number": 2,
                                "title": "spike",
                                "state": "CLOSED",
                                "stateReason": "COMPLETED",
                                "closedByPullRequestsReferences": {"nodes": []},
                            },
                            {
                                "number": 7,
                                "title": "open",
                                "state": "OPEN",
                                "stateReason": None,
                                "closedByPullRequestsReferences": {"nodes": []},
                            },
                        ]
                    }
                }
            }
        }
    }
    monkeypatch.setattr(
        gh_mod,
        "_run_gh",
        _stub(
            {
                ("repo", "view"): json.dumps(repo_payload),
                ("api", "graphql"): json.dumps(gql_payload),
            }
        ),
    )

    results = tracked_issues(3, repo_path=tmp_path)
    assert results == [
        TrackedIssue(
            number=1, title="scaffold", state="CLOSED", state_reason="COMPLETED",
            pr_url="https://x/pr/8",
        ),
        TrackedIssue(
            number=2, title="spike", state="CLOSED", state_reason="COMPLETED",
            pr_url=None,
        ),
        TrackedIssue(
            number=7, title="open", state="OPEN", state_reason=None, pr_url=None,
        ),
    ]


def test_open_pr_invokes_gh_create_and_returns_pr(monkeypatch, tmp_path):
    fake = _stub(
        {
            ("pr", "create"): "https://github.com/owner/name/pull/12\n",
            ("pr", "view"): json.dumps({"number": 12, "url": "https://github.com/owner/name/pull/12"}),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    pr = open_pr(
        repo_path=tmp_path,
        head="auto/3",
        base="main",
        title="Single-issue happy path",
        body="Closes #3\n",
    )
    assert pr.number == 12
    assert pr.url == "https://github.com/owner/name/pull/12"

    create_call = next(c for c in fake.calls if c[:2] == ["pr", "create"])
    assert "--head" in create_call and "auto/3" in create_call
    assert "--base" in create_call and "main" in create_call
    assert "--title" in create_call
    assert "--body" in create_call


def test_comment_pr_calls_gh_pr_comment(monkeypatch, tmp_path):
    fake = _stub({("pr", "comment", "12"): ""})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    comment_pr(repo_path=tmp_path, pr_number=12, body="@codex review")
    call = fake.calls[0]
    assert call[:3] == ["pr", "comment", "12"]
    assert "--body" in call
    assert "@codex review" in call


def test_find_open_pr_for_branch_returns_pr(monkeypatch, tmp_path):
    fake = _stub(
        {
            ("pr", "list"): json.dumps(
                [{"number": 12, "url": "https://github.com/o/r/pull/12"}]
            ),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    pr = find_open_pr_for_branch("auto/3", repo_path=tmp_path)
    assert pr == PR(number=12, url="https://github.com/o/r/pull/12")
    call = fake.calls[0]
    assert "--head" in call and "auto/3" in call
    assert "--state" in call and "open" in call


def test_find_open_pr_for_branch_returns_none_when_no_pr(monkeypatch, tmp_path):
    fake = _stub({("pr", "list"): "[]"})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    assert find_open_pr_for_branch("auto/99", repo_path=tmp_path) is None


def test_arm_auto_merge_calls_gh_pr_merge(monkeypatch, tmp_path):
    fake = _stub({("pr", "merge", "12"): ""})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    arm_auto_merge(repo_path=tmp_path, pr_number=12)
    call = fake.calls[0]
    assert call[:3] == ["pr", "merge", "12"]
    assert "--auto" in call
    assert "--squash" in call
    assert "--delete-branch" in call


def test_run_gh_raises_github_error_on_failure(monkeypatch, tmp_path):
    """If gh exits non-zero, the wrapper raises GithubError with stderr."""
    import subprocess as _sp

    def _boom(*args, **kwargs):
        raise _sp.CalledProcessError(
            returncode=1, cmd=args, output="", stderr="bad request"
        )

    monkeypatch.setattr(gh_mod.subprocess, "run", _boom)
    with pytest.raises(GithubError) as exc:
        gh_mod._run_gh(["issue", "view", "1"], cwd=tmp_path)
    assert "bad request" in str(exc.value)
