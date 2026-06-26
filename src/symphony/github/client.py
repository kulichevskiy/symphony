"""Async wrapper over the local `gh` CLI.

All argv is built in list form and handed to `asyncio.create_subprocess_exec`
— never a shell — so caller-supplied strings (titles, bodies, branch names)
cannot inject. The single chokepoint also keeps the `gh` argv surface in one
file when the CLI inevitably renames flags.

`gh auth` is the auth source; setting `GH_TOKEN` overrides it for the
spawned subprocess only (so the orchestrator's own ambient `gh auth login`
isn't disturbed).

No `gh issue *` operations: Symphony reads issues from Linear and only
writes to GitHub via PRs.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable

MergeStrategy = Literal["squash", "merge", "rebase"]

_ACTIONS_RUN_RE = re.compile(r"/actions/runs/(\d+)")
DEFAULT_LOG_TAIL_BYTES = 12_000
_AUTO_MERGE_DISABLED_MARKERS = (
    "enablepullrequestautomerge must be true",
    "enablepullrequestautomerge=false",
    "auto merge is not allowed for this repository",
)
_MERGE_CONFLICT_MARKERS = (
    "merge conflict",
    "merge conflicts",
    "conflict between",
)


class GitHubError(RuntimeError):
    """Raised on non-zero exit, spawn failure, or unparseable JSON."""


def _is_auto_merge_disabled_error(error: object) -> bool:
    message = str(error).casefold()
    return any(marker in message for marker in _AUTO_MERGE_DISABLED_MARKERS)


def _is_merge_conflict_error(error: object) -> bool:
    message = str(error).casefold()
    return any(marker in message for marker in _MERGE_CONFLICT_MARKERS)


@dataclass
class CheckRun:
    name: str
    state: str  # SUCCESS|FAILURE|PENDING|...
    bucket: str  # pass|fail|pending|skipping|cancel
    link: str | None = None


@dataclass
class PRChecks:
    runs: list[CheckRun] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        # `skipping` is non-blocking (path-conditional workflows), so it counts
        # as passing alongside `pass`. Empty runs (no configured checks) is
        # also passing — there is nothing failing or pending to block on.
        return all(r.bucket in ("pass", "skipping") for r in self.runs)

    @property
    def any_failed(self) -> bool:
        return any(r.bucket in ("fail", "cancel") for r in self.runs)

    @property
    def pending(self) -> bool:
        return any(r.bucket == "pending" for r in self.runs)


@runtime_checkable
class GitHubClient(Protocol):
    """The GitHub surface the orchestrator and reconciler call on `gh`.

    Both the real `GitHub` client and the test `FakeGitHub` satisfy this
    structurally, so a renamed/re-signatured method on the real client is
    caught against the fake instead of drifting silently. Methods the
    orchestrator never calls (e.g. `pr_create`, `branch_list`) are
    intentionally omitted.
    """

    async def repo_clone(self, repo: str, dest: Path) -> None: ...

    async def repo_default_branch(self, repo: str) -> str: ...

    async def open_pr_for_head(
        self, *, head: str, repo: str | None = None
    ) -> dict[str, Any] | None: ...

    async def ensure_pr(
        self,
        *,
        title: str,
        body: str,
        base: str | None = None,
        head: str,
        repo: str | None = None,
        linear_url: str | None = None,
        draft: bool = False,
    ) -> str: ...

    async def pr_view(
        self,
        pr: int | str,
        *,
        repo: str | None = None,
        include_status_checks: bool = False,
    ) -> dict[str, Any]: ...

    async def pr_comment(self, pr: int | str, body: str, *, repo: str | None = None) -> None: ...

    async def pr_diff(self, pr: int | str, *, repo: str | None = None) -> str: ...

    async def pr_checks(self, pr: int | str, *, repo: str | None = None) -> PRChecks: ...

    async def pr_review_comments(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]: ...

    async def pr_reviews(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]: ...

    async def pr_issue_comments(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]: ...

    async def pr_reactions(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]: ...

    async def commit_committed_at(self, repo: str, sha: str) -> str: ...

    async def check_log_tail(
        self,
        check: CheckRun,
        *,
        repo: str | None = None,
        max_bytes: int = DEFAULT_LOG_TAIL_BYTES,
    ) -> str: ...

    async def run_failed_log_tail(
        self,
        run_id: int | str,
        *,
        repo: str | None = None,
        max_bytes: int = DEFAULT_LOG_TAIL_BYTES,
    ) -> str: ...

    async def pr_merge(
        self,
        pr: int | str,
        *,
        strategy: MergeStrategy,
        auto: bool = False,
        repo: str | None = None,
    ) -> None: ...


class GitHub:
    """Thin async wrapper. One instance per orchestrator process."""

    def __init__(
        self,
        *,
        gh_path: str = "gh",
        token: str | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        self._gh = gh_path
        self._token = token
        self._extra_env = dict(env or {})
        self._auto_merge_disabled_repos: set[str] = set()

    # ---- low-level ----

    async def _run(
        self,
        argv: list[str],
        *,
        cwd: Path | None = None,
        allow_exit_codes: tuple[int, ...] = (0,),
    ) -> str:
        stdout, stderr, returncode = await self._run_capture(argv, cwd=cwd)
        if returncode not in allow_exit_codes:
            raise GitHubError(
                f"gh {' '.join(argv)} exited {returncode}: {stderr.strip() or stdout.strip()}"
            )
        return stdout

    async def _run_capture(
        self,
        argv: list[str],
        *,
        cwd: Path | None = None,
    ) -> tuple[str, str, int]:
        env = {**os.environ, **self._extra_env}
        if self._token is not None:
            # `gh` splits auth by host: GH_TOKEN for github.com / *.ghe.com,
            # GH_ENTERPRISE_TOKEN for GHES. We don't know the target host up
            # front, so set both — gh reads the one matching the call.
            env["GH_TOKEN"] = self._token
            env["GH_ENTERPRISE_TOKEN"] = self._token
        try:
            proc = await asyncio.create_subprocess_exec(
                self._gh,
                *argv,
                cwd=str(cwd) if cwd is not None else None,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
            )
        except (OSError, FileNotFoundError) as e:
            raise GitHubError(f"failed to spawn gh: {type(e).__name__}: {e}") from e
        stdout_b, stderr_b = await proc.communicate()
        stdout = stdout_b.decode(errors="replace")
        stderr = stderr_b.decode(errors="replace")
        return stdout, stderr, proc.returncode if proc.returncode is not None else -1

    async def _run_json(
        self,
        argv: list[str],
        *,
        cwd: Path | None = None,
        allow_exit_codes: tuple[int, ...] = (0,),
    ) -> Any:
        out = await self._run(argv, cwd=cwd, allow_exit_codes=allow_exit_codes)
        try:
            return json.loads(out)
        except json.JSONDecodeError as e:
            raise GitHubError(
                f"could not parse gh output as JSON: {e}; output={out[:200]!r}"
            ) from e

    @staticmethod
    def _repo_args(repo: str | None) -> list[str]:
        return ["--repo", repo] if repo else []

    @staticmethod
    def _api_repo(repo: str) -> tuple[list[str], str]:
        parts = repo.split("/")
        if len(parts) == 3:
            host, owner, name = parts
            return ["--hostname", host], f"{owner}/{name}"
        if len(parts) == 2:
            return [], repo
        raise GitHubError(f"invalid repo {repo!r} (expected [HOST/]OWNER/REPO)")

    # ---- high-level ----

    async def repo_clone(self, repo: str, dest: Path) -> None:
        await self._run(["repo", "clone", repo, str(dest)])

    async def repo_default_branch(self, repo: str) -> str:
        result = await self._run_json(["repo", "view", repo, "--json", "defaultBranchRef"])
        if not isinstance(result, dict):
            raise GitHubError(f"repo view: expected object, got {type(result).__name__}")
        default_ref = result.get("defaultBranchRef")
        if not isinstance(default_ref, dict) or not default_ref.get("name"):
            raise GitHubError(f"repo view: missing default branch for {repo}")
        return str(default_ref["name"])

    async def repo_view(self, repo: str) -> dict[str, Any]:
        host_args, owner_repo = self._api_repo(repo)
        result = await self._run_json(["api", *host_args, f"repos/{owner_repo}"])
        if not isinstance(result, dict):
            raise GitHubError(f"repo view: expected object, got {type(result).__name__}")
        payload = dict(result)
        if "allow_auto_merge" in payload:
            payload.setdefault(
                "enablePullRequestAutoMerge",
                payload.get("allow_auto_merge"),
            )
        return payload

    async def pr_create(
        self,
        *,
        title: str,
        body: str,
        base: str | None = None,
        head: str,
        repo: str | None = None,
        linear_url: str | None = None,
        draft: bool = False,
    ) -> str:
        """Create a PR; return the PR URL printed by `gh`."""
        full_body = body
        if linear_url:
            sep = "\n\n" if full_body and not full_body.endswith("\n\n") else ""
            full_body = f"{full_body}{sep}Relates to {linear_url}"
        argv = [
            "pr",
            "create",
            "--title",
            title,
            "--body",
            full_body,
        ]
        if base:
            argv.extend(["--base", base])
        argv.extend(
            [
                "--head",
                head,
                *self._repo_args(repo),
            ]
        )
        if draft:
            argv.append("--draft")
        out = await self._run(argv)
        return out.strip()

    async def open_pr_for_head(
        self, *, head: str, repo: str | None = None
    ) -> dict[str, Any] | None:
        """Return `{number, url}` of the open PR for `head`, or None.

        Lists by head branch (`gh pr list --head <head> --state open`) so a
        PR that was opened for the branch but never recorded locally can still
        be discovered.
        """
        result = await self._run_json(
            [
                "pr",
                "list",
                "--head",
                head,
                "--state",
                "open",
                *self._repo_args(repo),
                "--json",
                "number,url",
            ]
        )
        if not isinstance(result, list):
            raise GitHubError(f"pr list: expected array, got {type(result).__name__}")
        for entry in result:
            if isinstance(entry, dict) and entry.get("url"):
                return {"number": int(entry["number"]), "url": str(entry["url"])}
        return None

    async def pr_for_head(self, *, head: str, repo: str | None = None) -> str | None:
        """Return the URL of the open PR for `head`, or None if none exists."""
        pr = await self.open_pr_for_head(head=head, repo=repo)
        return pr["url"] if pr is not None else None

    async def ensure_pr(
        self,
        *,
        title: str,
        body: str,
        base: str | None = None,
        head: str,
        repo: str | None = None,
        linear_url: str | None = None,
        draft: bool = False,
    ) -> str:
        """Get-or-create the PR for `(repo, head)`; return its URL.

        Idempotent: adopts an existing open PR for the head branch instead of
        creating a duplicate, and recovers if `gh pr create` races and fails
        because a PR already exists. This lets every retry or re-dispatch that
        reaches the PR step converge on the one PR for the branch.
        """
        existing = await self.pr_for_head(head=head, repo=repo)
        if existing:
            return existing
        try:
            return await self.pr_create(
                title=title,
                body=body,
                base=base,
                head=head,
                repo=repo,
                linear_url=linear_url,
                draft=draft,
            )
        except GitHubError as e:
            if "a pull request already exists" not in str(e).casefold():
                raise
            recovered = await self.pr_for_head(head=head, repo=repo)
            if recovered:
                return recovered
            raise

    async def pr_view(
        self,
        pr: int | str,
        *,
        repo: str | None = None,
        include_status_checks: bool = False,
    ) -> dict[str, Any]:
        fields = [
            "number",
            "title",
            "state",
            "url",
            "headRefName",
            "headRefOid",
            "baseRefName",
            "mergeable",
            "mergeStateStatus",
            "isDraft",
            "mergedAt",
        ]
        if include_status_checks:
            fields.append("statusCheckRollup")
        argv = [
            "pr",
            "view",
            str(pr),
            *self._repo_args(repo),
            "--json",
            ",".join(fields),
        ]
        result = await self._run_json(argv)
        if not isinstance(result, dict):
            raise GitHubError(f"pr view: expected object, got {type(result).__name__}")
        return result

    async def pr_external_snapshot(self, pr: int | str, *, repo: str) -> dict[str, Any]:
        """Current PR state/checks plus recent review comments for the UI."""
        view = await self._run_json(
            [
                "pr",
                "view",
                str(pr),
                *self._repo_args(repo),
                "--json",
                ",".join(
                    [
                        "number",
                        "state",
                        "url",
                        "mergeable",
                        "mergeStateStatus",
                        "mergedAt",
                        "mergedBy",
                        "statusCheckRollup",
                    ]
                ),
            ]
        )
        if not isinstance(view, dict):
            raise GitHubError(f"pr view: expected object, got {type(view).__name__}")

        comments_error: str | None = None
        try:
            comments = await self.pr_review_comments_recent(pr, repo=repo, limit=5)
        except GitHubError as exc:
            comments = []
            comments_error = str(exc)
        comments.sort(
            key=lambda comment: str(comment.get("updated_at") or comment.get("created_at") or ""),
            reverse=True,
        )
        merged_by = view.get("mergedBy")
        snapshot: dict[str, Any] = {
            "pr_number": view.get("number"),
            "state": view.get("state"),
            "url": view.get("url"),
            "mergeable": view.get("mergeable"),
            "merge_state_status": view.get("mergeStateStatus"),
            "merged_at": view.get("mergedAt"),
            "merged_by": (merged_by.get("login") if isinstance(merged_by, dict) else merged_by),
            "check_summary": _status_check_summary(view.get("statusCheckRollup")),
            "comments": [
                {
                    "author": str((comment.get("user") or {}).get("login") or ""),
                    "ts": comment.get("updated_at") or comment.get("created_at"),
                    "body": comment.get("body") or "",
                    "comment_id": comment.get("id"),
                    "url": comment.get("html_url"),
                }
                for comment in comments[:5]
            ],
        }
        if comments_error is not None:
            snapshot["comments_error"] = comments_error
        return snapshot

    async def pr_comment(self, pr: int | str, body: str, *, repo: str | None = None) -> None:
        argv = ["pr", "comment", str(pr), "--body", body, *self._repo_args(repo)]
        await self._run(argv)

    async def pr_diff(self, pr: int | str, *, repo: str | None = None) -> str:
        return await self._run(["pr", "diff", str(pr), *self._repo_args(repo)])

    async def pr_checks(self, pr: int | str, *, repo: str | None = None) -> PRChecks:
        # `--required` mirrors GitHub's mergeability rule: optional checks
        # should not block merge gating even if they fail.
        argv = [
            "pr",
            "checks",
            str(pr),
            "--required",
            *self._repo_args(repo),
            "--json",
            "name,state,bucket,link",
        ]
        stdout, stderr, returncode = await self._run_capture(argv)
        # `gh pr checks --required` exits 1 in two distinct cases:
        #   a) no checks / no required checks: stderr contains "no checks reported"
        #      → treat as empty required-check list, not a transient failure.
        #   b) one or more required checks are failing: stdout is a valid JSON array.
        # Both exit 1; exit 8 means checks are still pending.
        output = f"{stderr}\n{stdout}".casefold()
        if returncode == 1 and (
            "no checks reported" in output or "no required checks reported" in output
        ):
            return PRChecks()
        if returncode not in (0, 1, 8):
            raise GitHubError(
                f"gh {' '.join(argv)} exited {returncode}: {stderr.strip() or stdout.strip()}"
            )
        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as e:
            raise GitHubError(
                f"could not parse gh output as JSON: {e}; output={stdout[:200]!r}"
            ) from e
        if not isinstance(data, list):
            raise GitHubError(f"pr checks: expected array, got {type(data).__name__}")
        runs: list[CheckRun] = []
        for entry in data:
            if not isinstance(entry, dict):
                raise GitHubError(f"pr checks: malformed entry {entry!r}")
            runs.append(
                CheckRun(
                    name=str(entry.get("name", "")),
                    state=str(entry.get("state", "")),
                    bucket=str(entry.get("bucket", "")),
                    link=entry.get("link") or None,
                )
            )
        return PRChecks(runs=runs)

    async def pr_review_comments(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]:
        host_args, owner_repo = self._api_repo(repo)
        result = await self._run_paginated_list(
            [
                "api",
                *host_args,
                f"repos/{owner_repo}/pulls/{pr}/comments",
            ]
        )
        return result

    async def pr_review_comments_recent(
        self,
        pr: int | str,
        *,
        repo: str,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        host_args, owner_repo = self._api_repo(repo)
        result = await self._run_json(
            [
                "api",
                *host_args,
                f"repos/{owner_repo}/pulls/{pr}/comments"
                f"?per_page={limit}&sort=updated&direction=desc",
            ]
        )
        if not isinstance(result, list):
            raise GitHubError(
                f"recent review comments: expected array, got {type(result).__name__}"
            )
        return [entry for entry in result if isinstance(entry, dict)]

    async def pr_reviews(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]:
        host_args, owner_repo = self._api_repo(repo)
        result = await self._run_paginated_list(
            [
                "api",
                *host_args,
                f"repos/{owner_repo}/pulls/{pr}/reviews",
            ]
        )
        return result

    async def pr_issue_comments(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]:
        """Regular PR comments (issue comments), not inline review comments."""
        host_args, owner_repo = self._api_repo(repo)
        result = await self._run_paginated_list(
            [
                "api",
                *host_args,
                f"repos/{owner_repo}/issues/{pr}/comments",
            ]
        )
        return result

    async def pr_reactions(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]:
        host_args, owner_repo = self._api_repo(repo)
        result = await self._run_paginated_list(
            [
                "api",
                *host_args,
                "-H",
                "Accept: application/vnd.github+json",
                f"repos/{owner_repo}/issues/{pr}/reactions",
            ]
        )
        return result

    async def _run_paginated_list(self, argv: list[str]) -> list[dict[str, Any]]:
        result = await self._run_json(
            [
                argv[0],
                "--paginate",
                "--slurp",
                *argv[1:],
            ]
        )
        if not isinstance(result, list):
            raise GitHubError(f"paginated api: expected array, got {type(result).__name__}")
        flattened: list[dict[str, Any]] = []
        for page in result:
            if isinstance(page, list):
                flattened.extend(entry for entry in page if isinstance(entry, dict))
            elif isinstance(page, dict):
                flattened.append(page)
        return flattened

    async def commit_committed_at(self, repo: str, sha: str) -> str:
        host_args, owner_repo = self._api_repo(repo)
        result = await self._run_json(
            [
                "api",
                *host_args,
                f"repos/{owner_repo}/commits/{sha}",
            ]
        )
        if not isinstance(result, dict):
            raise GitHubError(f"commit view: expected object, got {type(result).__name__}")
        commit = result.get("commit")
        if not isinstance(commit, dict):
            raise GitHubError("commit view: missing commit object")
        committer = commit.get("committer")
        if not isinstance(committer, dict) or not committer.get("date"):
            raise GitHubError("commit view: missing committer date")
        return str(committer["date"])

    async def check_log_tail(
        self,
        check: CheckRun,
        *,
        repo: str | None = None,
        max_bytes: int = DEFAULT_LOG_TAIL_BYTES,
    ) -> str:
        """Return a truncated failed-step log excerpt for a PR check run.

        `gh pr checks --json link` points at an Actions run/job page. Browser
        job URLs do not carry the job database id required by `gh run view --job`,
        so use the parent run id and ask gh for all failed-step logs in that run.
        Checks without an Actions URL simply have no retrievable excerpt.
        """
        if not check.link:
            return ""
        run_match = _ACTIONS_RUN_RE.search(check.link)
        if run_match is None:
            return ""
        return await self.run_failed_log_tail(
            run_match.group(1),
            repo=repo,
            max_bytes=max_bytes,
        )

    async def run_failed_log_tail(
        self,
        run_id: int | str,
        *,
        repo: str | None = None,
        max_bytes: int = DEFAULT_LOG_TAIL_BYTES,
    ) -> str:
        argv = ["run", "view", str(run_id), *self._repo_args(repo), "--log-failed"]
        out = await self._run(argv)
        return _tail_utf8(out, max_bytes=max_bytes)

    async def pr_merge(
        self,
        pr: int | str,
        *,
        strategy: MergeStrategy,
        auto: bool = False,
        repo: str | None = None,
    ) -> None:
        # `strategy` is required because any of merge/squash/rebase can be
        # disabled at the repo level — there's no universally safe default.
        # `--auto` requires repo-level auto-merge to be enabled, so callers
        # opt in explicitly when they want merge-on-green.
        argv = ["pr", "merge", str(pr), f"--{strategy}", *self._repo_args(repo)]
        use_auto = auto and (repo is None or repo not in self._auto_merge_disabled_repos)
        if use_auto:
            argv.append("--auto")
        try:
            await self._run(argv)
        except GitHubError as e:
            if not use_auto or not _is_auto_merge_disabled_error(e):
                raise
            if repo is not None:
                self._auto_merge_disabled_repos.add(repo)
            retry_argv = [
                "pr",
                "merge",
                str(pr),
                f"--{strategy}",
                *self._repo_args(repo),
            ]
            await self._run(retry_argv)

    async def pr_close(self, pr: int | str, *, repo: str | None = None) -> None:
        await self._run(["pr", "close", str(pr), *self._repo_args(repo)])

    async def branch_list(self, repo: str) -> list[str]:
        """Remote branches via `gh api`. Paginated.

        Accepts `[HOST/]OWNER/REPO` like the rest of the wrapper. The host
        portion (if present) is forwarded via `--hostname` so GHES and other
        non-default hosts work; only `OWNER/REPO` is interpolated into the
        API path.
        """
        host_args, owner_repo = self._api_repo(repo)
        argv = [
            "api",
            *host_args,
            "--paginate",
            f"repos/{owner_repo}/branches",
            "--jq",
            ".[].name",
        ]
        out = await self._run(argv)
        return [line for line in out.splitlines() if line]

    async def head_sha(self, pr: int | str, *, repo: str | None = None) -> str:
        """Head commit SHA of the given PR."""
        argv = [
            "pr",
            "view",
            str(pr),
            *self._repo_args(repo),
            "--json",
            "headRefOid",
            "-q",
            ".headRefOid",
        ]
        out = await self._run(argv)
        return out.strip()


def _tail_utf8(text: str, *, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    if max_bytes <= 0:
        return ""
    suffix = b"\n...[truncated]\n"
    if len(suffix) >= max_bytes:
        return suffix[:max_bytes].decode("utf-8", errors="ignore")
    tail = encoded[-(max_bytes - len(suffix)) :].decode("utf-8", errors="ignore")
    return suffix.decode("utf-8") + tail


def _status_check_summary(raw: object) -> dict[str, object]:
    checks = _status_check_nodes(raw)
    passing = 0
    failing = 0
    pending = 0
    details: list[dict[str, object]] = []
    for check in checks:
        state = str(
            check.get("state") or check.get("status") or check.get("__typename") or ""
        ).upper()
        conclusion = str(check.get("conclusion") or "").upper()
        if conclusion in {
            "FAILURE",
            "FAILED",
            "ERROR",
            "CANCELLED",
            "CANCELED",
            "TIMED_OUT",
            "ACTION_REQUIRED",
            "STARTUP_FAILURE",
            "STALE",
        }:
            failing += 1
        elif conclusion in {"SUCCESS", "NEUTRAL", "SKIPPED"}:
            passing += 1
        elif state in {"SUCCESS", "PASS", "PASSED", "NEUTRAL", "SKIPPED"}:
            passing += 1
        elif state in {
            "FAILURE",
            "FAILED",
            "ERROR",
            "CANCELLED",
            "CANCELED",
            "TIMED_OUT",
            "ACTION_REQUIRED",
            "STARTUP_FAILURE",
            "STALE",
        }:
            failing += 1
        else:
            pending += 1
        details.append(_status_check_detail(check))
    return {
        "passing": passing,
        "failing": failing,
        "pending": pending,
        "total": len(checks),
        "checks": details,
    }


def _status_check_detail(check: dict[str, Any]) -> dict[str, object]:
    detail: dict[str, object] = {}
    for key in (
        "__typename",
        "name",
        "context",
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
    return detail


def _status_check_nodes(raw: object) -> list[dict[str, Any]]:
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
