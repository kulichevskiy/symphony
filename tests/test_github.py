"""Tests for symphony.github — `gh` CLI wrappers.

We monkeypatch the module-level ``_run_gh`` so tests don't shell out.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from symphony import github as gh_mod
from symphony.github import (
    CheckRun,
    GithubError,
    Issue,
    IssueComment,
    PR,
    Reaction,
    Review,
    ReviewComment,
    TrackedIssue,
    arm_auto_merge,
    comment_pr,
    find_open_pr_for_branch,
    get_commit_committed_at,
    get_pr_head_sha,
    is_pr_merged,
    label_issue,
    list_pr_checks,
    list_pr_reactions,
    list_pr_review_comments,
    list_pr_reviews,
    merge_pr,
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

    kwargs_seen: list[dict[str, object]] = []

    def _fake(args, *, cwd=None, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(list(args))
        kwargs_seen.append(dict(kwargs))
        for length in range(len(args), 0, -1):
            key = tuple(args[:length])
            if key in responses:
                return responses[key]
        raise AssertionError(f"unexpected gh call: {args}")

    _fake.calls = calls  # type: ignore[attr-defined]
    _fake.kwargs_seen = kwargs_seen  # type: ignore[attr-defined]
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


def test_view_issue_includes_created_at(monkeypatch, tmp_path):
    payload = {
        "number": 5,
        "title": "x",
        "body": "y",
        "labels": [],
        "comments": [],
        "createdAt": "2026-05-01T00:00:00Z",
    }
    monkeypatch.setattr(gh_mod, "_run_gh", _stub({("issue", "view", "5"): json.dumps(payload)}))
    issue = view_issue(5, repo_path=tmp_path)
    assert issue.created_at == "2026-05-01T00:00:00Z"


def test_list_open_issues_with_label_parses_json(monkeypatch, tmp_path):
    from symphony.github import list_open_issues_with_label

    payload = [
        {
            "number": 1,
            "title": "older",
            "body": "...",
            "labels": [{"name": "auto"}],
            "comments": [],
            "createdAt": "2026-04-01T00:00:00Z",
        },
        {
            "number": 2,
            "title": "newer",
            "body": "...",
            "labels": [{"name": "auto"}, {"name": "p2"}],
            "comments": [{"author": {"login": "ak"}, "body": "hi"}],
            "createdAt": "2026-05-01T00:00:00Z",
        },
    ]
    fake = _stub({("issue", "list"): json.dumps(payload)})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    issues = list_open_issues_with_label("auto", repo_path=tmp_path)
    assert [i.number for i in issues] == [1, 2]
    assert issues[0].created_at == "2026-04-01T00:00:00Z"
    assert issues[1].labels == ["auto", "p2"]
    assert issues[1].comments[0].author == "ak"
    # Default limit must be high enough to cover a realistic auto backlog —
    # `gh issue list --limit` is a fetch cap, so a too-small default
    # silently starves older items past the cap.
    [argv] = fake.calls
    limit_arg = argv[argv.index("--limit") + 1]
    assert int(limit_arg) >= 1000


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


def test_tracked_issues_paginates_graphql(monkeypatch, tmp_path):
    repo_payload = {"nameWithOwner": "owner/name"}
    pages = [
        {
            "data": {
                "repository": {
                    "issue": {
                        "trackedIssues": {
                            "nodes": [
                                {
                                    "number": 1,
                                    "title": "first page",
                                    "state": "CLOSED",
                                    "stateReason": "COMPLETED",
                                    "closedByPullRequestsReferences": {"nodes": []},
                                }
                            ],
                            "pageInfo": {
                                "hasNextPage": True,
                                "endCursor": "cursor-1",
                            },
                        }
                    }
                }
            }
        },
        {
            "data": {
                "repository": {
                    "issue": {
                        "trackedIssues": {
                            "nodes": [
                                {
                                    "number": 101,
                                    "title": "second page",
                                    "state": "OPEN",
                                    "stateReason": None,
                                    "closedByPullRequestsReferences": {"nodes": []},
                                }
                            ],
                            "pageInfo": {
                                "hasNextPage": False,
                                "endCursor": None,
                            },
                        }
                    }
                }
            }
        },
    ]
    calls: list[list[str]] = []

    def fake_run_gh(args, *, cwd=None):  # type: ignore[no-untyped-def]
        calls.append(list(args))
        if args[:2] == ["repo", "view"]:
            return json.dumps(repo_payload)
        if args[:2] == ["api", "graphql"]:
            return json.dumps(pages.pop(0))
        raise AssertionError(f"unexpected gh call: {args}")

    monkeypatch.setattr(gh_mod, "_run_gh", fake_run_gh)

    results = tracked_issues(3, repo_path=tmp_path)

    assert [r.number for r in results] == [1, 101]
    graphql_calls = [c for c in calls if c[:2] == ["api", "graphql"]]
    assert len(graphql_calls) == 2
    assert not any(a.startswith("after=") for a in graphql_calls[0])
    assert "after=cursor-1" in graphql_calls[1]


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
                [
                    {
                        "number": 12,
                        "url": "https://github.com/o/r/pull/12",
                        "baseRefName": "main",
                        "headRepositoryOwner": {"login": "o"},
                    }
                ]
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


def test_find_open_pr_for_branch_skips_fork_prs(monkeypatch, tmp_path):
    """Regression: `gh pr list --head <branch>` matches by branch name only,
    so a fork can have an open PR with the same head ref name. Must not
    return a PR whose head-repo owner differs from `expected_owner`.
    """
    fake = _stub(
        {
            ("pr", "list"): json.dumps(
                [
                    {
                        "number": 33,
                        "url": "https://github.com/stranger/r/pull/33",
                        "baseRefName": "main",
                        "headRepositoryOwner": {"login": "stranger"},
                    },
                    {
                        "number": 12,
                        "url": "https://github.com/o/r/pull/12",
                        "baseRefName": "main",
                        "headRepositoryOwner": {"login": "o"},
                    },
                ]
            ),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    pr = find_open_pr_for_branch(
        "auto/3", repo_path=tmp_path, base_branch="main", expected_owner="o"
    )
    assert pr == PR(number=12, url="https://github.com/o/r/pull/12")


def test_find_open_pr_for_branch_skips_wrong_base(monkeypatch, tmp_path):
    fake = _stub(
        {
            ("pr", "list"): json.dumps(
                [
                    {
                        "number": 12,
                        "url": "https://github.com/o/r/pull/12",
                        "baseRefName": "develop",
                        "headRepositoryOwner": {"login": "o"},
                    }
                ]
            ),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    pr = find_open_pr_for_branch(
        "auto/3", repo_path=tmp_path, base_branch="main", expected_owner="o"
    )
    assert pr is None


def test_arm_auto_merge_calls_gh_pr_merge(monkeypatch, tmp_path):
    fake = _stub({("pr", "merge", "12"): ""})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    arm_auto_merge(repo_path=tmp_path, pr_number=12)
    call = fake.calls[0]
    assert call[:3] == ["pr", "merge", "12"]
    assert "--auto" in call
    assert "--squash" in call
    assert "--delete-branch" in call


def test_merge_pr_matches_reviewed_head(monkeypatch, tmp_path):
    fake = _stub({("pr", "merge", "12"): ""})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    merge_pr(repo_path=tmp_path, pr_number=12, match_head_commit="abc123")
    assert fake.calls[0] == [
        "pr",
        "merge",
        "12",
        "--squash",
        "--delete-branch",
        "--match-head-commit",
        "abc123",
    ]


def test_get_pr_head_sha(monkeypatch, tmp_path):
    fake = _stub({("pr", "view", "10"): json.dumps({"headRefOid": "abc123"})})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    assert get_pr_head_sha(10, repo_path=tmp_path) == "abc123"


def test_is_pr_merged(monkeypatch, tmp_path):
    fake = _stub(
        {
            ("pr", "view", "10"): json.dumps(
                {"state": "MERGED", "mergedAt": "2026-05-06T18:00:00Z"}
            )
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    assert is_pr_merged(10, repo_path=tmp_path) is True
    assert fake.calls == [
        ["pr", "view", "10", "--json", "state,mergedAt"],
    ]


def test_list_pr_reviews_parses_payload(monkeypatch, tmp_path):
    payload = [
        {
            "id": 1,
            "user": {"login": "alice"},
            "state": "APPROVED",
            "body": "lgtm",
            "commit_id": "sha1",
            "submitted_at": "2026-05-06T07:00:00Z",
        },
        {
            "id": 2,
            "user": {"login": "chatgpt-codex-connector[bot]"},
            "state": "COMMENTED",
            "body": None,
            "commit_id": "sha2",
            "submitted_at": "2026-05-06T07:30:00Z",
        },
    ]
    # ``gh api --paginate --slurp`` returns a JSON array of pages; each page
    # is itself an array. Simulate two pages to lock in the flatten path.
    fake = _stub(
        {
            ("repo", "view"): json.dumps({"nameWithOwner": "o/r"}),
            ("api",): json.dumps([payload[:1], payload[1:]]),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    reviews = list_pr_reviews(10, repo_path=tmp_path)
    assert reviews == [
        Review(id=1, user_login="alice", state="APPROVED", body="lgtm", commit_sha="sha1", submitted_at="2026-05-06T07:00:00Z"),
        Review(id=2, user_login="chatgpt-codex-connector[bot]", state="COMMENTED", body="", commit_sha="sha2", submitted_at="2026-05-06T07:30:00Z"),
    ]
    api_call = next(c for c in fake.calls if c[0] == "api")
    assert "--paginate" in api_call and "--slurp" in api_call


def test_list_pr_review_comments(monkeypatch, tmp_path):
    payload = [
        {
            "id": 7,
            "user": {"login": "chatgpt-codex-connector[bot]"},
            "path": "src/x.py",
            "line": 42,
            "body": "fix this",
            "commit_id": "shaH",
            "created_at": "2026-05-06T07:30:00Z",
            "pull_request_review_id": 99,
        }
    ]
    fake = _stub(
        {
            ("repo", "view"): json.dumps({"nameWithOwner": "o/r"}),
            ("api",): json.dumps([payload]),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    cs = list_pr_review_comments(10, repo_path=tmp_path)
    assert cs == [
        ReviewComment(
            id=7,
            user_login="chatgpt-codex-connector[bot]",
            path="src/x.py",
            line=42,
            body="fix this",
            commit_sha="shaH",
            created_at="2026-05-06T07:30:00Z",
            review_id=99,
        )
    ]
    api_call = next(c for c in fake.calls if c[0] == "api")
    assert "--paginate" in api_call and "--slurp" in api_call


def test_list_pr_reactions(monkeypatch, tmp_path):
    payload = [
        {"user": {"login": "chatgpt-codex-connector[bot]"}, "content": "+1", "created_at": "2026-05-06T08:00:00Z"},
        {"user": None, "content": "eyes", "created_at": "2026-05-06T07:00:00Z"},
    ]
    fake = _stub(
        {
            ("repo", "view"): json.dumps({"nameWithOwner": "o/r"}),
            ("api",): json.dumps([payload]),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    reactions = list_pr_reactions(10, repo_path=tmp_path)
    assert reactions == [
        Reaction(user_login="chatgpt-codex-connector[bot]", content="+1", created_at="2026-05-06T08:00:00Z"),
        Reaction(user_login="", content="eyes", created_at="2026-05-06T07:00:00Z"),
    ]
    api_call = next(c for c in fake.calls if c[0] == "api")
    assert "--paginate" in api_call and "--slurp" in api_call


def test_list_pr_checks(monkeypatch, tmp_path):
    page1 = [
        {
            "name": "build",
            "status": "completed",
            "conclusion": "success",
            "details_url": "https://ci/build-wrong-app",
            "app": {"id": 999},
        },
        {
            "name": "build",
            "status": "completed",
            "conclusion": "success",
            "details_url": "https://ci/build",
            "app": {"id": 123},
        },
    ]
    page2 = [
        {"name": "test", "status": "completed", "conclusion": "failure", "details_url": "https://ci/test"},
        {"name": "lint", "status": "in_progress", "conclusion": None, "details_url": None},
    ]
    status_page1 = [
        {
            "context": "deploy",
            "state": "failure",
            "target_url": "https://ci/deploy-old",
            "created_at": "2026-05-06T07:00:00Z",
        },
        {
            "context": "deploy",
            "state": "success",
            "target_url": "https://ci/deploy",
            "created_at": "2026-05-06T07:05:00Z",
        },
        {
            "context": "external",
            "state": "pending",
            "target_url": None,
            "created_at": "2026-05-06T07:10:00Z",
        },
    ]
    status_page2 = [
        {
            "context": "legacy",
            "state": "error",
            "target_url": "https://ci/legacy",
            "created_at": "2026-05-06T07:15:00Z",
        },
    ]
    fake = _stub(
        {
            ("repo", "view"): json.dumps({"nameWithOwner": "o/r"}),
            ("pr", "view", "10"): json.dumps(
                {"headRefOid": "abc123", "baseRefName": "auto/5"}
            ),
            (
                "api",
                "repos/o/r/branches/auto%2F5/protection/required_status_checks",
            ): json.dumps(
                {
                    "contexts": ["build", "test", "legacy"],
                    "checks": [{"context": "build", "app_id": 123}],
                }
            ),
            ("api", "repos/o/r/commits/abc123/check-runs?per_page=100"): json.dumps(
                [{"check_runs": page1}, {"check_runs": page2}]
            ),
            ("api", "repos/o/r/commits/abc123/status?per_page=100"): json.dumps(
                [{"statuses": status_page1}, {"statuses": status_page2}]
            ),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    checks = list_pr_checks(10, repo_path=tmp_path)
    assert len(checks) == 7
    assert checks[0] == CheckRun(
        name="build",
        status="completed",
        conclusion="success",
        details_url="https://ci/build-wrong-app",
        app_id=999,
        required=False,
    )
    assert checks[1] == CheckRun(
        name="build",
        status="completed",
        conclusion="success",
        details_url="https://ci/build",
        app_id=123,
        required=True,
    )
    assert checks[2] == CheckRun(
        name="test",
        status="completed",
        conclusion="failure",
        details_url="https://ci/test",
        required=True,
    )
    assert checks[3].conclusion is None
    assert checks[3].required is False
    assert checks[4] == CheckRun(
        name="deploy",
        status="completed",
        conclusion="success",
        details_url="https://ci/deploy",
        required=False,
    )
    assert checks[5] == CheckRun(
        name="external",
        status="in_progress",
        conclusion=None,
        details_url=None,
        required=False,
    )
    assert checks[6] == CheckRun(
        name="legacy",
        status="completed",
        conclusion="failure",
        details_url="https://ci/legacy",
        required=True,
    )
    assert fake.calls == [
        ["repo", "view", "--json", "nameWithOwner"],
        ["pr", "view", "10", "--json", "headRefOid,baseRefName"],
        [
            "api",
            "repos/o/r/branches/auto%2F5/protection/required_status_checks",
        ],
        [
            "api",
            "repos/o/r/commits/abc123/check-runs?per_page=100",
            "--paginate",
            "--slurp",
        ],
        [
            "api",
            "repos/o/r/commits/abc123/status?per_page=100",
            "--paginate",
            "--slurp",
        ],
    ]


def test_list_pr_checks_uses_latest_check_run_attempt_per_app(monkeypatch, tmp_path):
    check_runs = [
        {
            "name": "build",
            "status": "completed",
            "conclusion": "failure",
            "details_url": "https://ci/build-old",
            "app": {"id": 123},
            "completed_at": "2026-05-06T07:00:00Z",
        },
        {
            "name": "build",
            "status": "completed",
            "conclusion": "success",
            "details_url": "https://ci/build-new",
            "app": {"id": 123},
            "completed_at": "2026-05-06T07:05:00Z",
        },
        {
            "name": "build",
            "status": "completed",
            "conclusion": "failure",
            "details_url": "https://ci/build-other-app",
            "app": {"id": 999},
            "completed_at": "2026-05-06T07:10:00Z",
        },
    ]
    fake = _stub(
        {
            ("repo", "view"): json.dumps({"nameWithOwner": "o/r"}),
            ("pr", "view", "10"): json.dumps(
                {"headRefOid": "abc123", "baseRefName": "main"}
            ),
            (
                "api",
                "repos/o/r/branches/main/protection/required_status_checks",
            ): json.dumps(
                {
                    "contexts": [],
                    "checks": [{"context": "build", "app_id": 123}],
                }
            ),
            ("api", "repos/o/r/commits/abc123/check-runs?per_page=100"): json.dumps(
                [{"check_runs": check_runs}]
            ),
            ("api", "repos/o/r/commits/abc123/status?per_page=100"): json.dumps(
                [{"statuses": []}]
            ),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)

    checks = list_pr_checks(10, repo_path=tmp_path)

    assert checks == [
        CheckRun(
            name="build",
            status="completed",
            conclusion="success",
            details_url="https://ci/build-new",
            app_id=123,
            required=True,
        ),
        CheckRun(
            name="build",
            status="completed",
            conclusion="failure",
            details_url="https://ci/build-other-app",
            app_id=999,
            required=False,
        ),
    ]


def test_list_pr_checks_marks_forbidden_required_context_lookup_as_unknown(
    monkeypatch, tmp_path
):
    calls: list[list[str]] = []

    def fake(args, *, cwd=None):  # type: ignore[no-untyped-def]
        calls.append(list(args))
        if args[:2] == ["repo", "view"]:
            return json.dumps({"nameWithOwner": "o/r"})
        if args[:3] == ["pr", "view", "10"]:
            return json.dumps({"headRefOid": "abc123", "baseRefName": "main"})
        if args[:2] == [
            "api",
            "repos/o/r/branches/main/protection/required_status_checks",
        ]:
            raise GithubError("gh api failed: HTTP 403")
        if args[:2] == ["api", "repos/o/r/commits/abc123/check-runs?per_page=100"]:
            return json.dumps(
                [
                    {
                        "check_runs": [
                            {
                                "name": "build",
                                "status": "in_progress",
                                "conclusion": None,
                                "details_url": None,
                            }
                        ]
                    }
                ]
            )
        if args[:2] == ["api", "repos/o/r/commits/abc123/status?per_page=100"]:
            return json.dumps([{"statuses": []}])
        raise AssertionError(f"unexpected gh call: {args}")

    monkeypatch.setattr(gh_mod, "_run_gh", fake)

    checks = list_pr_checks(10, repo_path=tmp_path)

    assert checks == [
        CheckRun(
            name="build",
            status="in_progress",
            conclusion=None,
            details_url=None,
            required=None,
        )
    ]
    assert calls[2] == [
        "api",
        "repos/o/r/branches/main/protection/required_status_checks",
    ]


def test_label_issue_calls_gh(monkeypatch, tmp_path):
    fake = _stub({("issue", "edit", "10"): ""})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    label_issue(10, "auto-stuck", repo_path=tmp_path)
    call = fake.calls[0]
    assert call[:3] == ["issue", "edit", "10"]
    assert "--add-label" in call and "auto-stuck" in call


def test_get_commit_committed_at(monkeypatch, tmp_path):
    fake = _stub(
        {
            ("repo", "view"): json.dumps({"nameWithOwner": "o/r"}),
            ("api",): json.dumps(
                {"commit": {"committer": {"date": "2026-05-06T07:00:00Z"}}}
            ),
        }
    )
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    assert get_commit_committed_at("abc", repo_path=tmp_path) == "2026-05-06T07:00:00Z"


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


def test_run_gh_allows_configured_nonzero_exit(monkeypatch, tmp_path):
    """`gh pr checks` exits 8 while checks are pending; callers can allow it."""
    import subprocess as _sp

    def _pending(*args, **kwargs):
        return _sp.CompletedProcess(args=args, returncode=8, stdout="[]", stderr="pending")

    monkeypatch.setattr(gh_mod.subprocess, "run", _pending)
    assert (
        gh_mod._run_gh(
            ["pr", "checks", "10"],
            cwd=tmp_path,
            allowed_exit_codes={0, 8},
        )
        == "[]"
    )


def test_get_issue_state_open(monkeypatch, tmp_path):
    from symphony.github import get_issue_state

    fake = _stub({("issue", "view", "7"): json.dumps({"state": "OPEN"})})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    assert get_issue_state(7, repo_path=tmp_path) == "OPEN"


def test_get_issue_state_closed(monkeypatch, tmp_path):
    from symphony.github import get_issue_state

    fake = _stub({("issue", "view", "7"): json.dumps({"state": "CLOSED"})})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    assert get_issue_state(7, repo_path=tmp_path) == "CLOSED"


def test_get_issue_state_unexpected_raises(monkeypatch, tmp_path):
    from symphony.github import get_issue_state

    fake = _stub({("issue", "view", "7"): json.dumps({"state": "WAT"})})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    with pytest.raises(GithubError):
        get_issue_state(7, repo_path=tmp_path)


def test_find_pr_for_branch_returns_none_when_empty(monkeypatch, tmp_path):
    from symphony.github import find_pr_for_branch

    fake = _stub({("pr", "list", "--head", "auto/42"): "[]"})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    assert find_pr_for_branch("auto/42", repo_path=tmp_path) is None


def test_find_pr_for_branch_picks_latest(monkeypatch, tmp_path):
    from symphony.github import find_pr_for_branch

    payload = [
        {
            "number": 11,
            "url": "https://gh/x/11",
            "state": "MERGED",
            "baseRefName": "main",
            "headRepositoryOwner": {"login": "ak"},
        },
        {
            "number": 14,
            "url": "https://gh/x/14",
            "state": "CLOSED",
            "baseRefName": "main",
            "headRepositoryOwner": {"login": "ak"},
        },
    ]
    fake = _stub({("pr", "list", "--head", "auto/42"): json.dumps(payload)})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    pr, state = find_pr_for_branch(
        "auto/42",
        repo_path=tmp_path,
        base_branch="main",
        expected_owner="ak",
    )
    assert pr.number == 14
    assert state == "CLOSED"


def test_find_pr_for_branch_filters_owner(monkeypatch, tmp_path):
    from symphony.github import find_pr_for_branch

    payload = [
        {
            "number": 22,
            "url": "https://gh/x/22",
            "state": "MERGED",
            "baseRefName": "main",
            "headRepositoryOwner": {"login": "stranger"},
        },
    ]
    fake = _stub({("pr", "list", "--head", "auto/42"): json.dumps(payload)})
    monkeypatch.setattr(gh_mod, "_run_gh", fake)
    assert (
        find_pr_for_branch(
            "auto/42",
            repo_path=tmp_path,
            base_branch="main",
            expected_owner="ak",
        )
        is None
    )
