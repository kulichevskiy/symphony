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
from typing import Any, Literal

MergeStrategy = Literal["squash", "merge", "rebase"]

_ACTIONS_RUN_RE = re.compile(r"/actions/runs/(\d+)")
_ACTIONS_JOB_RE = re.compile(r"/jobs?/(\d+)")
DEFAULT_LOG_TAIL_BYTES = 12_000


class GitHubError(RuntimeError):
    """Raised on non-zero exit, spawn failure, or unparseable JSON."""


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

    # ---- low-level ----

    async def _run(
        self,
        argv: list[str],
        *,
        cwd: Path | None = None,
        allow_exit_codes: tuple[int, ...] = (0,),
    ) -> str:
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
        if proc.returncode not in allow_exit_codes:
            raise GitHubError(
                f"gh {' '.join(argv)} exited {proc.returncode}: {stderr.strip() or stdout.strip()}"
            )
        return stdout

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

    # ---- high-level ----

    async def repo_clone(self, repo: str, dest: Path) -> None:
        await self._run(["repo", "clone", repo, str(dest)])

    async def repo_default_branch(self, repo: str) -> str:
        result = await self._run_json(
            ["repo", "view", repo, "--json", "defaultBranchRef"]
        )
        if not isinstance(result, dict):
            raise GitHubError(
                f"repo view: expected object, got {type(result).__name__}"
            )
        default_ref = result.get("defaultBranchRef")
        if not isinstance(default_ref, dict) or not default_ref.get("name"):
            raise GitHubError(f"repo view: missing default branch for {repo}")
        return str(default_ref["name"])

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

    async def pr_view(self, pr: int | str, *, repo: str | None = None) -> dict[str, Any]:
        argv = [
            "pr",
            "view",
            str(pr),
            *self._repo_args(repo),
            "--json",
            "number,title,state,url,headRefName,headRefOid,mergeable,isDraft",
        ]
        result = await self._run_json(argv)
        if not isinstance(result, dict):
            raise GitHubError(f"pr view: expected object, got {type(result).__name__}")
        return result

    async def pr_comment(self, pr: int | str, body: str, *, repo: str | None = None) -> None:
        argv = ["pr", "comment", str(pr), "--body", body, *self._repo_args(repo)]
        await self._run(argv)

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
        # gh exits 8 when checks are still pending but still emits valid JSON.
        data = await self._run_json(argv, allow_exit_codes=(0, 8))
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

    async def check_log_tail(
        self,
        check: CheckRun,
        *,
        repo: str | None = None,
        max_bytes: int = DEFAULT_LOG_TAIL_BYTES,
    ) -> str:
        """Return a truncated failed-step log excerpt for a PR check run.

        `gh pr checks --json link` points at an Actions run/job page. Prefer the
        job-specific log when the URL includes a job id; otherwise fall back to
        the whole run's failed-step log. Checks without an Actions URL simply
        have no retrievable excerpt.
        """
        if not check.link:
            return ""
        job_match = _ACTIONS_JOB_RE.search(check.link)
        run_match = _ACTIONS_RUN_RE.search(check.link)
        if job_match is not None:
            argv = [
                "run",
                "view",
                *self._repo_args(repo),
                "--job",
                job_match.group(1),
                "--log-failed",
            ]
        elif run_match is not None:
            argv = [
                "run",
                "view",
                run_match.group(1),
                *self._repo_args(repo),
                "--log-failed",
            ]
        else:
            return ""
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
        if auto:
            argv.append("--auto")
        await self._run(argv)

    async def pr_close(self, pr: int | str, *, repo: str | None = None) -> None:
        await self._run(["pr", "close", str(pr), *self._repo_args(repo)])

    async def branch_list(self, repo: str) -> list[str]:
        """Remote branches via `gh api`. Paginated.

        Accepts `[HOST/]OWNER/REPO` like the rest of the wrapper. The host
        portion (if present) is forwarded via `--hostname` so GHES and other
        non-default hosts work; only `OWNER/REPO` is interpolated into the
        API path.
        """
        parts = repo.split("/")
        if len(parts) == 3:
            host, owner, name = parts
            host_args = ["--hostname", host]
            owner_repo = f"{owner}/{name}"
        elif len(parts) == 2:
            host_args = []
            owner_repo = repo
        else:
            raise GitHubError(
                f"branch_list: invalid repo {repo!r} (expected [HOST/]OWNER/REPO)"
            )
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
