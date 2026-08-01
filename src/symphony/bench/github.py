from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class CommandError(RuntimeError):
    pass


class Commands(Protocol):
    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str: ...


class SubprocessCommands:
    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str:
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd,
            env={
                **{
                    key: value
                    for key, value in os.environ.items()
                    if not key.startswith("SYMPHONY_")
                },
                **(env or {}),
            },
            stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        try:
            stdout, stderr = await process.communicate(
                stdin.encode("utf-8") if stdin is not None else None
            )
        except asyncio.CancelledError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
            raise
        if process.returncode != 0:
            detail = (
                stderr.decode(errors="replace").strip() or stdout.decode(errors="replace").strip()
            )
            raise CommandError(f"{' '.join(argv)} exited {process.returncode}: {detail}")
        return stdout.decode(errors="replace")


@dataclass(frozen=True)
class GitHubRepository:
    slug: str
    url: str


class GitHubSandbox:
    def __init__(
        self,
        *,
        owner: str,
        token: str,
        commands: Commands | None = None,
    ) -> None:
        self._owner = owner
        self._token = token
        self._commands = commands or SubprocessCommands()

    async def create_repository(self, *, name: str, source: Path) -> GitHubRepository:
        slug = f"{self._owner}/{name}"
        token_env = {"GH_TOKEN": self._token}
        await self._commands.run(["git", "init", "--initial-branch=main"], cwd=source)
        await self._commands.run(["git", "add", "."], cwd=source)
        try:
            await self._commands.run(
                ["git", "commit", "-m", "Seed EventDesk benchmark"], cwd=source
            )
        except CommandError as exc:
            if "nothing to commit" not in str(exc):
                raise
        try:
            await self._commands.run(["gh", "api", f"repos/{slug}"], cwd=source, env=token_env)
        except CommandError as exc:
            if "404" not in str(exc):
                raise
            await self._commands.run(
                ["gh", "repo", "create", slug, "--private"],
                cwd=source,
                env=token_env,
            )
        try:
            await self._commands.run(["git", "remote", "remove", "origin"], cwd=source)
        except CommandError as exc:
            if "No such remote" not in str(exc):
                raise
        await self._commands.run(
            ["git", "remote", "add", "origin", f"https://github.com/{slug}.git"], cwd=source
        )
        await self._commands.run(
            ["git", "push", "--set-upstream", "origin", "main"],
            cwd=source,
            env=token_env,
        )
        await self._commands.run(
            ["gh", "repo", "edit", slug, "--enable-auto-merge"],
            cwd=source,
            env=token_env,
        )
        protection = json.dumps(
            {
                "required_status_checks": {
                    "strict": True,
                    "contexts": ["backend", "frontend"],
                },
                "enforce_admins": False,
                "required_pull_request_reviews": None,
                "restrictions": None,
                "allow_force_pushes": False,
                "allow_deletions": False,
            },
            separators=(",", ":"),
        )
        await self._commands.run(
            [
                "gh",
                "api",
                "--method",
                "PUT",
                f"repos/{slug}/branches/main/protection",
                "--input",
                "-",
            ],
            cwd=source,
            env=token_env,
            stdin=protection,
        )
        return GitHubRepository(slug=slug, url=f"https://github.com/{slug}")

    async def review_metrics(self, *, repository_slug: str) -> dict[str, int]:
        """Count actual remote Codex findings; keep rounds as a separate DB metric."""
        comments: list[object] = []
        for endpoint in (
            f"repos/{repository_slug}/pulls/comments?per_page=100",
            f"repos/{repository_slug}/issues/comments?per_page=100",
        ):
            raw = await self._commands.run(
                ["gh", "api", "--paginate", "--slurp", endpoint],
                cwd=Path.cwd(),
                env={"GH_TOKEN": self._token},
            )
            try:
                page = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RuntimeError("GitHub returned invalid review metrics JSON") from exc
            if not isinstance(page, list):
                raise RuntimeError("GitHub returned non-list review comments")
            for batch in page:
                if not isinstance(batch, list):
                    raise RuntimeError("GitHub returned an invalid review comment page")
                comments.extend(batch)

        counts = {priority: 0 for priority in range(4)}
        for raw_comment in comments:
            if not isinstance(raw_comment, dict):
                continue
            raw_user = raw_comment.get("user")
            login = raw_user.get("login", "") if isinstance(raw_user, dict) else ""
            if not isinstance(login, str) or not re.search(r"(?:codex|chatgpt)", login, re.I):
                continue
            body = raw_comment.get("body")
            if not isinstance(body, str):
                continue
            match = re.search(r"\[P([0-3])\]", body, re.I)
            if match is not None:
                counts[int(match.group(1))] += 1
        return {
            "remote_review_comments": sum(counts.values()),
            **{f"remote_review_p{priority}": counts[priority] for priority in range(4)},
        }
