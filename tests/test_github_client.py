"""Tests for `symphony.github.client.GitHub` — the gh CLI wrapper.

We exercise the wrapper against a fake `gh` shim placed earlier on `PATH`.
The shim is a tiny Python script that records its argv and emits canned
output based on argv patterns. This is faster + more predictable than
mocking `asyncio.subprocess` and catches real argv-construction bugs.

Covers:
- argv shape for each method (no shell injection — list-form invocation)
- `pr_create` body always carries `Relates to <linear-url>` when supplied
- `pr_checks` parses JSON output into a typed `PRChecks` object
- `pr_merge(strategy="squash")` enables auto-merge with the right flag
- non-zero exit raises `GitHubError`
- JSON parse failure raises `GitHubError`
- `GH_TOKEN` override is forwarded into the subprocess env
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from symphony.github.client import (
    CheckRun,
    GitHub,
    GitHubError,
    PRChecks,
    _is_auto_merge_disabled_error,
)


def _make_fake_gh(
    tmp_path: Path, responses: dict[str, list[object]]
) -> tuple[Path, Path]:
    """Write a fake `gh` to a temp dir and return (bin_dir, calls_log).

    `responses` maps a substring of the joined argv to `[exit_code, stdout]`.
    The first matching pattern wins; default is exit 0 with empty stdout.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    cfg = bin_dir / "responses.json"
    cfg.write_text(json.dumps(responses))
    calls = bin_dir / "calls.log"
    shim = bin_dir / "gh"
    shim.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "argv = sys.argv[1:]\n"
        "tok = os.environ.get('GH_TOKEN', '')\n"
        "etok = os.environ.get('GH_ENTERPRISE_TOKEN', '')\n"
        "rec = {'argv': argv, 'env_GH_TOKEN': tok, 'env_GH_ENTERPRISE_TOKEN': etok}\n"
        f"with open({str(calls)!r}, 'a') as f:\n"
        "    f.write(json.dumps(rec) + '\\n')\n"
        f"with open({str(cfg)!r}) as f:\n"
        "    responses = json.load(f)\n"
        "joined = ' '.join(argv)\n"
        "for pattern, spec in responses.items():\n"
        "    if pattern in joined:\n"
        "        code, out = spec[0], spec[1]\n"
        "        sys.stdout.write(out)\n"
        "        sys.exit(code)\n"
        "sys.exit(0)\n"
    )
    shim.chmod(0o755)
    return bin_dir, calls


def _calls(log: Path) -> list[dict[str, object]]:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]


@pytest.fixture
def fake_gh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    def _build(responses: dict[str, list[object]]) -> Path:
        bin_dir, log = _make_fake_gh(tmp_path, responses)
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
        return log

    return _build


# ---- pr_create ------------------------------------------------------


async def test_repo_default_branch_reads_repo_view(fake_gh) -> None:  # type: ignore[no-untyped-def]
    payload = json.dumps({"defaultBranchRef": {"name": "trunk"}})
    log = fake_gh({"repo view org/r": [0, payload]})
    gh = GitHub()
    assert await gh.repo_default_branch("org/r") == "trunk"
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv == ["repo", "view", "org/r", "--json", "defaultBranchRef"]


async def test_pr_create_appends_linear_url_when_provided(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"pr create": [0, "https://github.com/org/r/pull/42\n"]})
    gh = GitHub()
    url = await gh.pr_create(
        title="t",
        body="my body",
        base="main",
        head="feat/x",
        repo="org/r",
        linear_url="https://linear.app/team/issue/ENG-1",
    )
    assert url == "https://github.com/org/r/pull/42"
    calls = _calls(log)
    assert len(calls) == 1
    argv = calls[0]["argv"]
    assert isinstance(argv, list)
    # argv must be list-form (no shell). title/body passed via flags.
    assert argv[0] == "pr"
    assert argv[1] == "create"
    assert "--title" in argv and argv[argv.index("--title") + 1] == "t"
    assert "--base" in argv and argv[argv.index("--base") + 1] == "main"
    assert "--head" in argv and argv[argv.index("--head") + 1] == "feat/x"
    assert "--repo" in argv and argv[argv.index("--repo") + 1] == "org/r"
    body = argv[argv.index("--body") + 1]
    assert "my body" in body
    assert "Relates to https://linear.app/team/issue/ENG-1" in body


async def test_pr_create_omits_relates_when_no_linear_url(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"pr create": [0, "https://github.com/org/r/pull/1\n"]})
    gh = GitHub()
    await gh.pr_create(title="t", body="b", base="main", head="x", repo="org/r")
    body = _calls(log)[0]["argv"]
    assert isinstance(body, list)
    payload = body[body.index("--body") + 1]
    assert "Relates to" not in str(payload)


async def test_pr_create_omits_base_when_not_provided(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"pr create": [0, "https://github.com/org/r/pull/1\n"]})
    gh = GitHub()
    await gh.pr_create(title="t", body="b", head="x", repo="org/r")
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert "--base" not in argv
    assert "--head" in argv and argv[argv.index("--head") + 1] == "x"


async def test_pr_external_snapshot_reads_rollup_and_review_comments(fake_gh) -> None:  # type: ignore[no-untyped-def]
    view = json.dumps(
        {
            "number": 42,
            "state": "MERGED",
            "url": "https://github.com/org/r/pull/42",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "mergedAt": "2026-05-17T11:00:00Z",
            "mergedBy": {"login": "octo"},
            "statusCheckRollup": [
                {"state": "SUCCESS"},
                {"state": "COMPLETED", "conclusion": "FAILURE"},
                {"state": "PENDING"},
            ],
        }
    )
    comments = json.dumps(
        [
            {
                "id": 1,
                "body": "older",
                "created_at": "2026-05-17T10:00:00Z",
                "updated_at": "2026-05-17T10:00:00Z",
                "html_url": "https://github.com/org/r/pull/42#discussion_r1",
                "user": {"login": "reviewer-a"},
            },
            {
                "id": 2,
                "body": "newer",
                "created_at": "2026-05-17T10:05:00Z",
                "updated_at": "2026-05-17T10:06:00Z",
                "html_url": "https://github.com/org/r/pull/42#discussion_r2",
                "user": {"login": "reviewer-b"},
            },
        ]
    )
    log = fake_gh(
        {
            "pr view 42": [0, view],
            "repos/org/r/pulls/42/comments": [0, comments],
        }
    )
    gh = GitHub()

    snapshot = await gh.pr_external_snapshot(42, repo="org/r")

    assert snapshot["state"] == "MERGED"
    assert snapshot["merged_by"] == "octo"
    assert snapshot["check_summary"] == {
        "passing": 1,
        "failing": 1,
        "pending": 1,
        "total": 3,
    }
    assert [comment["comment_id"] for comment in snapshot["comments"]] == [2, 1]
    calls = _calls(log)
    assert calls[0]["argv"][:3] == ["pr", "view", "42"]
    assert "statusCheckRollup" in calls[0]["argv"][-1]
    assert calls[1]["argv"] == [
        "api",
        "repos/org/r/pulls/42/comments?per_page=5&sort=updated&direction=desc",
    ]


async def test_pr_external_snapshot_keeps_metadata_when_recent_comments_fail(fake_gh) -> None:  # type: ignore[no-untyped-def]
    view = json.dumps(
        {
            "number": 42,
            "state": "OPEN",
            "url": "https://github.com/org/r/pull/42",
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "mergedAt": None,
            "mergedBy": None,
            "statusCheckRollup": [{"state": "SUCCESS"}],
        }
    )
    log = fake_gh(
        {
            "pr view 42": [0, view],
            "repos/org/r/pulls/42/comments": [1, "missing scope"],
        }
    )
    gh = GitHub()

    snapshot = await gh.pr_external_snapshot(42, repo="org/r")

    assert snapshot["state"] == "OPEN"
    assert snapshot["check_summary"] == {
        "passing": 1,
        "failing": 0,
        "pending": 0,
        "total": 1,
    }
    assert snapshot["comments"] == []
    assert "missing scope" in snapshot["comments_error"]
    calls = _calls(log)
    assert len(calls) == 2


# ---- pr_checks ------------------------------------------------------


async def test_pr_checks_parses_json_into_typed_object(fake_gh) -> None:  # type: ignore[no-untyped-def]
    payload = json.dumps(
        [
            {"name": "build", "state": "SUCCESS", "bucket": "pass", "link": "u1"},
            {"name": "test", "state": "FAILURE", "bucket": "fail", "link": "u2"},
            {"name": "lint", "state": "PENDING", "bucket": "pending", "link": None},
        ]
    )
    log = fake_gh({"pr checks": [0, payload]})
    gh = GitHub()
    result = await gh.pr_checks(42, repo="org/r")
    assert isinstance(result, PRChecks)
    assert [r.name for r in result.runs] == ["build", "test", "lint"]
    assert result.any_failed is True
    assert result.all_passed is False
    assert result.pending is True
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[:3] == ["pr", "checks", "42"]
    assert "--json" in argv
    assert "--required" in argv


async def test_pr_checks_raises_on_json_parse_failure(fake_gh) -> None:  # type: ignore[no-untyped-def]
    fake_gh({"pr checks": [0, "not json at all"]})
    gh = GitHub()
    with pytest.raises(GitHubError):
        await gh.pr_checks(7, repo="org/r")


async def test_pr_checks_accepts_exit_code_8_pending(fake_gh) -> None:  # type: ignore[no-untyped-def]
    # `gh pr checks` exits 8 when checks are still pending but still emits JSON.
    payload = json.dumps(
        [{"name": "build", "state": "PENDING", "bucket": "pending", "link": None}]
    )
    fake_gh({"pr checks": [8, payload]})
    gh = GitHub()
    result = await gh.pr_checks(11, repo="org/r")
    assert result.pending is True
    assert result.all_passed is False
    assert result.any_failed is False


async def test_pr_checks_raises_on_other_non_zero_exit(fake_gh) -> None:  # type: ignore[no-untyped-def]
    fake_gh({"pr checks": [1, "boom"]})
    gh = GitHub()
    with pytest.raises(GitHubError):
        await gh.pr_checks(12, repo="org/r")


async def test_all_passed_treats_skipping_as_pass(fake_gh) -> None:  # type: ignore[no-untyped-def]
    # Path-conditional workflows produce `skipping` runs; PR is still mergeable.
    payload = json.dumps(
        [
            {"name": "build", "state": "SUCCESS", "bucket": "pass", "link": None},
            {"name": "lint", "state": "SKIPPED", "bucket": "skipping", "link": None},
        ]
    )
    fake_gh({"pr checks": [0, payload]})
    gh = GitHub()
    result = await gh.pr_checks(13, repo="org/r")
    assert result.all_passed is True
    assert result.any_failed is False
    assert result.pending is False


async def test_all_passed_true_when_no_checks_configured(fake_gh) -> None:  # type: ignore[no-untyped-def]
    # Repos with no required checks return an empty array. Nothing is failing
    # or pending, so merge gating must not stall on this case.
    fake_gh({"pr checks": [0, "[]"]})
    gh = GitHub()
    result = await gh.pr_checks(14, repo="org/r")
    assert result.runs == []
    assert result.all_passed is True
    assert result.any_failed is False
    assert result.pending is False


async def test_pr_checks_treats_no_checks_reported_as_empty(fake_gh) -> None:  # type: ignore[no-untyped-def]
    fake_gh({"pr checks": [1, "no checks reported on the 'feature' branch"]})
    gh = GitHub()
    result = await gh.pr_checks(15, repo="org/r")
    assert result.runs == []
    assert result.all_passed is True
    assert result.any_failed is False
    assert result.pending is False


async def test_pr_checks_treats_no_required_checks_reported_as_empty(fake_gh) -> None:  # type: ignore[no-untyped-def]
    fake_gh({"pr checks": [1, "no required checks reported on the 'feature' branch"]})
    gh = GitHub()
    result = await gh.pr_checks(16, repo="org/r")
    assert result.runs == []
    assert result.all_passed is True
    assert result.any_failed is False
    assert result.pending is False


async def test_check_log_tail_fetches_run_log_from_check_link(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"run view": [0, "line 1\nline 2\n"]})
    gh = GitHub()
    tail = await gh.check_log_tail(
        CheckRun(
            name="unit",
            state="FAILURE",
            bucket="fail",
            link="https://github.com/org/r/actions/runs/123/jobs/456",
        ),
        repo="org/r",
    )
    assert tail == "line 1\nline 2\n"
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[:3] == ["run", "view", "123"]
    assert "--job" not in argv
    assert "--repo" in argv and argv[argv.index("--repo") + 1] == "org/r"
    assert "--log-failed" in argv


# ---- pr_merge -------------------------------------------------------


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("GraphQL: enablePullRequestAutoMerge must be true", True),
        ("GraphQL: enablePullRequestAutoMerge=false", True),
        ("Auto merge is not allowed for this repository", True),
        (
            "GraphQL: Resource not accessible by integration (enablePullRequestAutoMerge)",
            False,
        ),
        ("GraphQL: Base branch was modified", False),
        ("gh pr merge 99 exited 1: something else failed", False),
    ],
)
def test_auto_merge_disabled_error_classifier(message: str, expected: bool) -> None:
    assert _is_auto_merge_disabled_error(message) is expected


async def test_pr_merge_squash_with_auto(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"pr merge": [0, ""]})
    gh = GitHub()
    await gh.pr_merge(99, strategy="squash", auto=True, repo="org/r")
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[:3] == ["pr", "merge", "99"]
    assert "--squash" in argv
    assert "--auto" in argv
    assert "--repo" in argv and argv[argv.index("--repo") + 1] == "org/r"


async def test_pr_merge_omits_auto_unless_requested(fake_gh) -> None:  # type: ignore[no-untyped-def]
    # `--auto` requires repo-level auto-merge; opt-in only.
    log = fake_gh({"pr merge": [0, ""]})
    gh = GitHub()
    await gh.pr_merge(99, strategy="merge", repo="org/r")
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert "--auto" not in argv
    assert "--merge" in argv


async def test_pr_merge_degrades_auto_merge_disabled_to_sync_retry(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh(
        {
            "pr merge 99 --squash --repo org/r --auto": [
                1,
                "GraphQL: enablePullRequestAutoMerge must be true\n",
            ],
            "pr merge 99 --squash --repo org/r": [0, ""],
        }
    )
    gh = GitHub()

    await gh.pr_merge(99, strategy="squash", auto=True, repo="org/r")

    calls = _calls(log)
    assert len(calls) == 2
    first = calls[0]["argv"]
    second = calls[1]["argv"]
    assert isinstance(first, list)
    assert isinstance(second, list)
    assert "--auto" in first
    assert "--auto" not in second


async def test_pr_merge_sync_retry_error_wins_and_degraded_state_sticks(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh(
        {
            "pr merge 99 --merge --repo org/r --auto": [
                1,
                "GraphQL: Auto merge is not allowed for this repository\n",
            ],
            "pr merge 99 --merge --repo org/r": [
                1,
                "GraphQL: Base branch was modified\n",
            ],
        }
    )
    gh = GitHub()

    with pytest.raises(GitHubError, match="Base branch was modified") as exc:
        await gh.pr_merge(99, strategy="merge", auto=True, repo="org/r")
    assert "Auto merge is not allowed" not in str(exc.value)

    with pytest.raises(GitHubError, match="Base branch was modified"):
        await gh.pr_merge(99, strategy="merge", auto=True, repo="org/r")

    calls = _calls(log)
    assert len(calls) == 3
    first = calls[0]["argv"]
    second = calls[1]["argv"]
    third = calls[2]["argv"]
    assert isinstance(first, list)
    assert isinstance(second, list)
    assert isinstance(third, list)
    assert "--auto" in first
    assert "--auto" not in second
    assert "--auto" not in third


# ---- non-zero exit + GH_TOKEN env -----------------------------------


async def test_non_zero_exit_raises_github_error(fake_gh) -> None:  # type: ignore[no-untyped-def]
    fake_gh({"pr view": [1, "boom\n"]})
    gh = GitHub()
    with pytest.raises(GitHubError):
        await gh.pr_view(5, repo="org/r")


async def test_gh_token_override_forwarded_to_subprocess_env(fake_gh) -> None:  # type: ignore[no-untyped-def]
    # Both env vars are set so gh picks the right one per target host
    # (GH_TOKEN for github.com, GH_ENTERPRISE_TOKEN for GHES).
    log = fake_gh({"pr view": [0, '{"number": 5}']})
    gh = GitHub(token="ghs_test_token")
    await gh.pr_view(5, repo="org/r")
    call = _calls(log)[0]
    assert call["env_GH_TOKEN"] == "ghs_test_token"
    assert call["env_GH_ENTERPRISE_TOKEN"] == "ghs_test_token"
    argv = call["argv"]
    assert isinstance(argv, list)
    fields = str(argv[argv.index("--json") + 1])
    assert "mergedAt" in fields
    assert "merged," not in fields


# ---- head_sha + branch_list + repo_clone + pr_comment + pr_close ----


async def test_head_sha_returns_pr_head_oid(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"pr view": [0, "deadbeefcafe\n"]})
    gh = GitHub()
    sha = await gh.head_sha(42, repo="org/r")
    assert sha == "deadbeefcafe"
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert "headRefOid" in " ".join(str(a) for a in argv)


async def test_branch_list_returns_names(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"api": [0, "main\nfeat/x\nfeat/y\n"]})
    gh = GitHub()
    branches = await gh.branch_list("org/r")
    assert branches == ["main", "feat/x", "feat/y"]
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[0] == "api"
    assert any("repos/org/r/branches" in str(a) for a in argv)
    assert "--hostname" not in argv


async def test_branch_list_handles_host_qualified_repo(fake_gh) -> None:  # type: ignore[no-untyped-def]
    # `[HOST/]OWNER/REPO` -> --hostname HOST, path repos/OWNER/REPO/branches.
    log = fake_gh({"api": [0, "main\n"]})
    gh = GitHub()
    branches = await gh.branch_list("github.example.com/org/r")
    assert branches == ["main"]
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert "--hostname" in argv
    assert argv[argv.index("--hostname") + 1] == "github.example.com"
    assert any("repos/org/r/branches" in str(a) for a in argv)
    assert not any("github.example.com/org/r" in str(a) for a in argv)


async def test_branch_list_rejects_malformed_repo(fake_gh) -> None:  # type: ignore[no-untyped-def]
    fake_gh({"api": [0, ""]})
    gh = GitHub()
    with pytest.raises(GitHubError):
        await gh.branch_list("just-one-segment")


async def test_repo_clone_invokes_gh_repo_clone(fake_gh, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"repo clone": [0, ""]})
    gh = GitHub()
    dest = tmp_path / "ws"
    await gh.repo_clone("org/r", dest)
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[:2] == ["repo", "clone"]
    assert "org/r" in argv
    assert str(dest) in argv


async def test_pr_comment_passes_body(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"pr comment": [0, ""]})
    gh = GitHub()
    await gh.pr_comment(7, "hello world", repo="org/r")
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[:3] == ["pr", "comment", "7"]
    assert "--body" in argv
    assert argv[argv.index("--body") + 1] == "hello world"


async def test_pr_close_invokes_pr_close(fake_gh) -> None:  # type: ignore[no-untyped-def]
    log = fake_gh({"pr close": [0, ""]})
    gh = GitHub()
    await gh.pr_close(8, repo="org/r")
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[:3] == ["pr", "close", "8"]


async def test_pr_reviews_fetches_all_pages(fake_gh) -> None:  # type: ignore[no-untyped-def]
    payload = json.dumps(
        [
            [{"id": 1, "state": "COMMENTED"}],
            [{"id": 2, "state": "APPROVED"}],
        ]
    )
    log = fake_gh({"pulls/7/reviews": [0, payload]})
    gh = GitHub()
    result = await gh.pr_reviews(7, repo="org/r")
    assert [entry["id"] for entry in result] == [1, 2]
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[:3] == ["api", "--paginate", "--slurp"]
    assert "repos/org/r/pulls/7/reviews" in argv


async def test_pr_review_comments_fetches_all_pages(fake_gh) -> None:  # type: ignore[no-untyped-def]
    payload = json.dumps([[{"id": 1}], [{"id": 2}]])
    log = fake_gh({"pulls/7/comments": [0, payload]})
    gh = GitHub()
    result = await gh.pr_review_comments(7, repo="org/r")
    assert [entry["id"] for entry in result] == [1, 2]
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[:3] == ["api", "--paginate", "--slurp"]
    assert "repos/org/r/pulls/7/comments" in argv


async def test_pr_reactions_fetches_all_pages(fake_gh) -> None:  # type: ignore[no-untyped-def]
    payload = json.dumps([[{"id": 1, "content": "eyes"}], [{"id": 2, "content": "+1"}]])
    log = fake_gh({"issues/7/reactions": [0, payload]})
    gh = GitHub()
    result = await gh.pr_reactions(7, repo="org/r")
    assert [entry["id"] for entry in result] == [1, 2]
    argv = _calls(log)[0]["argv"]
    assert isinstance(argv, list)
    assert argv[:3] == ["api", "--paginate", "--slurp"]
    assert "repos/org/r/issues/7/reactions" in argv
    assert "Accept: application/vnd.github+json" in argv
