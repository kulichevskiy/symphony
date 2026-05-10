"""Tests for the per-issue workspace manager.

Each test seeds a tiny local git repo and uses it as the clone source so
we exercise real `git` without touching the network.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest

from symphony.config import LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.workspace import Workspace


async def _run(*args: str, cwd: Path | None = None) -> None:
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(cwd) if cwd else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"{args} failed: {err.decode()}")


async def _make_remote(tmp_path: Path) -> Path:
    remote = tmp_path / "remote_src"
    remote.mkdir()
    await _run("git", "init", "-q", "-b", "main", cwd=remote)
    await _run("git", "config", "user.email", "test@example.com", cwd=remote)
    await _run("git", "config", "user.name", "Tester", cwd=remote)
    (remote / "README.md").write_text("hello\n")
    await _run("git", "add", ".", cwd=remote)
    await _run("git", "commit", "-q", "-m", "init", cwd=remote)
    return remote


def _binding(repo: str = "acme/widgets", branch_prefix: str = "symphony") -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo=repo,
        branch_prefix=branch_prefix,
        linear_states=LinearStates(ready="Backlog"),
    )


def _issue(identifier: str = "ENG-123") -> LinearIssue:
    return LinearIssue(
        id="uuid-" + identifier,
        identifier=identifier,
        title="t",
        description="",
        url="",
        state_id="state",
        state_name="Backlog",
        state_type="unstarted",
        team_key="ENG",
    )


def _make_clone_fn(remote: Path):
    async def clone_fn(repo: str, dest: Path) -> None:
        await _run("git", "clone", "-q", str(remote), str(dest))

    return clone_fn


def test_repo_safe_collapses_slash() -> None:
    assert Workspace.repo_safe("acme/widgets") == "acme__widgets"
    assert Workspace.repo_safe("acme/repo-name") == "acme__repo-name"
    # Deterministic: same input -> same output.
    assert Workspace.repo_safe("a/b") == Workspace.repo_safe("a/b")


@pytest.mark.asyncio
async def test_acquire_clones_and_checks_out_branch(tmp_path: Path) -> None:
    remote = await _make_remote(tmp_path)
    ws = Workspace(root=tmp_path / "ws", clone_fn=_make_clone_fn(remote))

    path = await ws.acquire(_binding(), _issue("ENG-123"))

    assert path == tmp_path / "ws" / "acme__widgets" / "eng-123"
    assert (path / ".git").exists()
    assert (path / "README.md").exists()

    proc = await asyncio.create_subprocess_exec(
        "git", "branch", "--show-current",
        cwd=path, stdout=asyncio.subprocess.PIPE,
    )
    out, _ = await proc.communicate()
    assert out.decode().strip() == "symphony/eng-123"


@pytest.mark.asyncio
async def test_acquire_is_idempotent(tmp_path: Path) -> None:
    remote = await _make_remote(tmp_path)
    calls: list[Path] = []

    async def clone_fn(repo: str, dest: Path) -> None:
        calls.append(dest)
        await _run("git", "clone", "-q", str(remote), str(dest))

    ws = Workspace(root=tmp_path / "ws", clone_fn=clone_fn)
    p1 = await ws.acquire(_binding(), _issue())
    p2 = await ws.acquire(_binding(), _issue())

    assert p1 == p2
    assert len(calls) == 1, "second acquire must not re-clone"


@pytest.mark.asyncio
async def test_acquire_recovers_from_non_git_residue(tmp_path: Path) -> None:
    remote = await _make_remote(tmp_path)
    ws = Workspace(root=tmp_path / "ws", clone_fn=_make_clone_fn(remote))

    # Simulate residue from an interrupted clone: dir exists, no .git.
    residue = ws.path_for(_binding(), _issue("ENG-9"))
    residue.mkdir(parents=True)
    (residue / "stale.txt").write_text("leftover")

    path = await ws.acquire(_binding(), _issue("ENG-9"))
    assert (path / ".git").exists()
    assert not (path / "stale.txt").exists()


@pytest.mark.asyncio
async def test_cleanup_removes_workspace_dir(tmp_path: Path) -> None:
    remote = await _make_remote(tmp_path)
    ws = Workspace(root=tmp_path / "ws", clone_fn=_make_clone_fn(remote))

    path = await ws.acquire(_binding(), _issue("ENG-7"))
    assert path.exists()

    await ws.cleanup(_issue("ENG-7"))
    assert not path.exists()


@pytest.mark.asyncio
async def test_sweep_ttl_removes_stale_keeps_fresh(tmp_path: Path) -> None:
    root = tmp_path / "ws"
    stale = root / "acme__widgets" / "eng-old"
    fresh = root / "acme__widgets" / "eng-new"
    stale.mkdir(parents=True)
    fresh.mkdir(parents=True)

    now = time.time()
    ttl = 7 * 24 * 3600
    os.utime(stale, (now - 30 * 24 * 3600, now - 30 * 24 * 3600))
    os.utime(fresh, (now, now))

    async def clone_fn(repo: str, dest: Path) -> None:
        raise AssertionError("clone should not be called during sweep")

    ws = Workspace(root=root, clone_fn=clone_fn, ttl_secs=ttl)
    await ws.sweep_ttl(now=now)

    assert not stale.exists()
    assert fresh.exists()
