"""Per-issue persistent workspace clones.

Each issue gets a private clone at `{root}/{repo_safe}/{issue_id_lower}/`.
The clone persists across stages (Implement / Review fix-runs / Merge) so
fix-runs don't pay the clone cost on every retry. Stale clones are swept
on mtime — the simplest signal that nothing is running against the dir.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from .config import RepoBinding
from .linear.client import LinearIssue

log = logging.getLogger(__name__)

CloneFn = Callable[[str, Path], Awaitable[None]]
DEFAULT_TTL_SECS = 7 * 24 * 3600
DEFAULT_SWEEP_INTERVAL_SECS = 6 * 3600


class WorkspaceError(RuntimeError):
    """Raised when a git operation against a workspace fails."""


class Workspace:
    """Manages per-issue clones rooted at `root`.

    A single instance is shared across the orchestrator process. Cloning
    is delegated to `clone_fn` (typically `GitHub.repo_clone`) so tests
    can substitute a local source path without going through `gh`.
    """

    def __init__(
        self,
        *,
        root: Path,
        clone_fn: CloneFn,
        ttl_secs: int = DEFAULT_TTL_SECS,
    ) -> None:
        self._root = root
        self._clone_fn = clone_fn
        self._ttl_secs = ttl_secs

    @staticmethod
    def repo_safe(github_repo: str) -> str:
        # `org/repo` would otherwise nest into a subdirectory and conflict
        # with case-folded filesystems if the repo name is reused under
        # multiple orgs. Double underscore keeps the mapping reversible.
        return github_repo.replace("/", "__")

    def path_for(self, binding: RepoBinding, issue: LinearIssue) -> Path:
        return self._root / self.repo_safe(binding.github_repo) / issue.identifier.lower()

    async def acquire(self, binding: RepoBinding, issue: LinearIssue) -> Path:
        """Idempotent: clone if missing, fetch if present, then check out branch."""
        path = self.path_for(binding, issue)
        branch = f"{binding.branch_prefix}/{issue.identifier.lower()}"
        if (path / ".git").exists():
            await self._git(path, "fetch", "origin")
        else:
            if path.exists():
                # Residue from an interrupted clone — git clone refuses
                # non-empty destinations, so wipe before retrying.
                await asyncio.to_thread(shutil.rmtree, path)
            path.parent.mkdir(parents=True, exist_ok=True)
            await self._clone_fn(binding.github_repo, path)
        await self._ensure_branch(path, branch)
        # Bump mtime so an in-flight stage isn't swept mid-run.
        path.touch(exist_ok=True)
        return path

    async def cleanup(self, issue: LinearIssue) -> None:
        """Remove the workspace dir for `issue` from every repo namespace."""
        if not self._root.exists():
            return
        issue_id = issue.identifier.lower()
        for repo_dir in self._root.iterdir():
            if not repo_dir.is_dir():
                continue
            candidate = repo_dir / issue_id
            if candidate.exists():
                await asyncio.to_thread(shutil.rmtree, candidate)

    async def sweep_ttl(self, *, now: float | None = None) -> None:
        """Remove issue dirs whose mtime is older than `ttl_secs`."""
        if not self._root.exists():
            return
        threshold = (now if now is not None else time.time()) - self._ttl_secs
        for repo_dir in self._root.iterdir():
            if not repo_dir.is_dir():
                continue
            for issue_dir in repo_dir.iterdir():
                if not issue_dir.is_dir():
                    continue
                try:
                    mtime = issue_dir.stat().st_mtime
                except FileNotFoundError:
                    continue
                if mtime < threshold:
                    log.info("ttl sweep: removing stale workspace %s", issue_dir)
                    await asyncio.to_thread(shutil.rmtree, issue_dir, ignore_errors=True)

    async def run_sweeper(
        self, *, interval_secs: int = DEFAULT_SWEEP_INTERVAL_SECS
    ) -> None:
        """Sweep at startup, then every `interval_secs`. Cancellation-safe."""
        while True:
            try:
                await self.sweep_ttl()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — must not kill the loop
                log.exception("ttl sweep failed")
            await asyncio.sleep(interval_secs)

    async def _ensure_branch(self, path: Path, branch: str) -> None:
        # Prefer an existing local branch (preserves agent commits made
        # during a prior fix-run). Otherwise, track origin if it has the
        # branch. Otherwise, create from current HEAD.
        if await self._git_ok(path, "rev-parse", "--verify", branch):
            await self._git(path, "switch", branch)
            return
        if await self._git_ok(path, "rev-parse", "--verify", f"origin/{branch}"):
            await self._git(path, "switch", "-c", branch, "--track", f"origin/{branch}")
            return
        await self._git(path, "switch", "-c", branch)

    async def _git(self, cwd: Path, *args: str) -> None:
        proc = await asyncio.create_subprocess_exec(
            "git", *args,
            cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise WorkspaceError(
                f"git {' '.join(args)} in {cwd} exited {proc.returncode}: "
                f"{stderr.decode(errors='replace').strip()}"
            )

    async def _git_ok(self, cwd: Path, *args: str) -> bool:
        proc = await asyncio.create_subprocess_exec(
            "git", *args,
            cwd=str(cwd),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            stdin=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        return proc.returncode == 0
