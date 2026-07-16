"""Push-auth header config derives its host from `origin` (OAuth in UI 4/7
review fix): a hardcoded `github.com` doesn't apply to GHE remotes, and
`http.extraHeader` is a no-op against an SSH remote."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from symphony.orchestrator.poll._git import _clear_git_push_auth, _configure_git_push_auth


async def _git(cwd: Path, *args: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "git",
        *args,
        cwd=str(cwd),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    assert proc.returncode == 0, stderr.decode()


async def _get_config(cwd: Path, key: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        "git",
        "config",
        "--local",
        "--get",
        key,
        cwd=str(cwd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode().strip() if proc.returncode == 0 else ""


@pytest.mark.asyncio
async def test_configure_uses_ghe_host_from_origin(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    await _git(repo, "init")
    await _git(repo, "remote", "add", "origin", "https://ghe.example.com/org/repo.git")

    await _configure_git_push_auth(repo, "tok")

    assert await _get_config(repo, "http.https://github.com/.extraheader") == ""
    header = await _get_config(repo, "http.https://ghe.example.com/.extraheader")
    assert "basic" in header.lower()

    await _clear_git_push_auth(repo)
    assert await _get_config(repo, "http.https://ghe.example.com/.extraheader") == ""


@pytest.mark.asyncio
async def test_configure_is_noop_for_ssh_remote(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    await _git(repo, "init")
    await _git(repo, "remote", "add", "origin", "git@github.com:org/repo.git")

    await _configure_git_push_auth(repo, "tok")

    assert await _get_config(repo, "http.https://github.com/.extraheader") == ""


@pytest.mark.asyncio
async def test_configure_defaults_to_github_com_without_origin(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    await _git(repo, "init")

    await _configure_git_push_auth(repo, "tok")

    header = await _get_config(repo, "http.https://github.com/.extraheader")
    assert "basic" in header.lower()

    await _clear_git_push_auth(repo)
    assert await _get_config(repo, "http.https://github.com/.extraheader") == ""
