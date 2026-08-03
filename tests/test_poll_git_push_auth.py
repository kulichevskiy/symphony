"""Push-auth header config derives its host from `origin` (OAuth in UI 4/7
review fix): a hardcoded `github.com` doesn't apply to GHE remotes, and
`http.extraHeader` is a no-op against an SSH remote.

Held in process memory only, never written to `.git/config` (OAuth in UI 4/7
review fix: a daemon kill between configure/clear must not leave the
plaintext token on disk) — assert against the subprocess env `git push`
would actually receive, not the workspace's persisted config file.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from symphony.orchestrator.poll._git import (
    _clear_git_push_auth,
    _configure_git_push_auth,
    _push_auth_subprocess_env,
    _retry_transient_push,
)
from symphony.orchestrator.poll._helpers import _retry_transient_delivery


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


def _configured_headers(cwd: Path) -> dict[str, str]:
    env = _push_auth_subprocess_env(cwd)
    if env is None:
        return {}
    count = int(env.get("GIT_CONFIG_COUNT", "0"))
    return {env[f"GIT_CONFIG_KEY_{i}"]: env[f"GIT_CONFIG_VALUE_{i}"] for i in range(count)}


@pytest.mark.asyncio
async def test_configure_uses_ghe_host_from_origin(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    await _git(repo, "init")
    await _git(repo, "remote", "add", "origin", "https://ghe.example.com/org/repo.git")

    await _configure_git_push_auth(repo, "tok")

    headers = _configured_headers(repo)
    assert "http.https://github.com/.extraheader" not in headers
    header = headers.get("http.https://ghe.example.com/.extraheader", "")
    assert "basic" in header.lower()
    assert await _get_config(repo, "http.https://ghe.example.com/.extraheader") == ""

    await _clear_git_push_auth(repo)
    assert _configured_headers(repo) == {}


@pytest.mark.asyncio
async def test_configure_rewrites_github_ssh_remote_to_https(tmp_path: Path) -> None:
    # An existing workspace cloned over SSH (documented
    # `gh auth login --git-protocol ssh` flow) must still authenticate with a
    # newly connected DB/GH_TOKEN credential: the SSH transport is rewritten to
    # HTTPS (insteadOf) for the child process and the Basic header applied.
    repo = tmp_path / "repo"
    repo.mkdir()
    await _git(repo, "init")
    await _git(repo, "remote", "add", "origin", "git@github.com:org/repo.git")

    await _configure_git_push_auth(repo, "tok")

    configured = _configured_headers(repo)
    assert configured.get("url.https://github.com/.insteadOf") == "git@github.com:"
    header = configured.get("http.https://github.com/.extraheader", "")
    assert "basic" in header.lower()

    await _clear_git_push_auth(repo)
    assert _configured_headers(repo) == {}


@pytest.mark.asyncio
async def test_configure_is_noop_for_ghe_ssh_remote(tmp_path: Path) -> None:
    # A github.com-scoped token is the wrong host's credential for a GHES SSH
    # remote — leave it on its own SSH-key auth.
    repo = tmp_path / "repo"
    repo.mkdir()
    await _git(repo, "init")
    await _git(repo, "remote", "add", "origin", "git@ghe.example.com:org/repo.git")

    await _configure_git_push_auth(repo, "tok")

    assert _configured_headers(repo) == {}


@pytest.mark.asyncio
async def test_configure_defaults_to_github_com_without_origin(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    await _git(repo, "init")

    await _configure_git_push_auth(repo, "tok")

    header = _configured_headers(repo).get("http.https://github.com/.extraheader", "")
    assert "basic" in header.lower()
    assert await _get_config(repo, "http.https://github.com/.extraheader") == ""

    await _clear_git_push_auth(repo)
    assert _configured_headers(repo) == {}


@pytest.mark.asyncio
async def test_transient_push_retries_without_operator_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempts = 0

    async def push(_workspace: Path, _branch: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("The requested URL returned error: 503")

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)

    await _retry_transient_push(push, tmp_path, "branch")

    assert attempts == 2


@pytest.mark.asyncio
async def test_permanent_push_error_does_not_retry(tmp_path: Path) -> None:
    attempts = 0

    async def push(_workspace: Path, _branch: str) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("The requested URL returned error: 401")

    with pytest.raises(RuntimeError, match="401"):
        await _retry_transient_push(push, tmp_path, "branch")

    assert attempts == 1


@pytest.mark.asyncio
async def test_transient_pr_delivery_retries_without_operator_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    async def ensure_pr() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError("gh timed out")
        return "https://github.com/org/repo/pull/1"

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)

    assert await _retry_transient_delivery(ensure_pr) == (
        "https://github.com/org/repo/pull/1"
    )
    assert attempts == 2
