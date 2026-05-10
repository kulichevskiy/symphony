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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

MergeStrategy = Literal["squash", "merge", "rebase"]


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
        return bool(self.runs) and all(r.bucket == "pass" for r in self.runs)

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
        check: bool = True,
    ) -> str:
        env = {**os.environ, **self._extra_env}
        if self._token is not None:
            env["GH_TOKEN"] = self._token
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
        if check and proc.returncode != 0:
            raise GitHubError(
                f"gh {' '.join(argv)} exited {proc.returncode}: {stderr.strip() or stdout.strip()}"
            )
        return stdout

    async def _run_json(self, argv: list[str], *, cwd: Path | None = None) -> Any:
        out = await self._run(argv, cwd=cwd)
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

    async def pr_create(
        self,
        *,
        title: str,
        body: str,
        base: str,
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
            "--base",
            base,
            "--head",
            head,
            *self._repo_args(repo),
        ]
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
        argv = [
            "pr",
            "checks",
            str(pr),
            *self._repo_args(repo),
            "--json",
            "name,state,bucket,link",
        ]
        data = await self._run_json(argv)
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

    async def pr_merge(
        self,
        pr: int | str,
        *,
        strategy: MergeStrategy = "squash",
        auto: bool = True,
        repo: str | None = None,
    ) -> None:
        argv = ["pr", "merge", str(pr), f"--{strategy}", *self._repo_args(repo)]
        if auto:
            argv.append("--auto")
        await self._run(argv)

    async def pr_close(self, pr: int | str, *, repo: str | None = None) -> None:
        await self._run(["pr", "close", str(pr), *self._repo_args(repo)])

    async def branch_list(self, repo: str) -> list[str]:
        """Remote branches via `gh api`. Paginated."""
        argv = [
            "api",
            "--paginate",
            f"repos/{repo}/branches",
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
