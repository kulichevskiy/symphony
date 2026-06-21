"""Unit tests for the already-satisfied delivery-ref verification helper.

`_workspace_ref_landed_in_base` must accept only commits that actually landed
in the delivery base branch — not every ancestor of HEAD. On a retry after an
earlier failed implement, unpushed commits left on the issue branch are
ancestors of HEAD but were never delivered; treating them as "landed elsewhere"
would falsely auto-close an undelivered issue.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from symphony.orchestrator.poll import (
    _branch_ahead_of_base,
    _workspace_ref_landed_in_base,
)


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    ).stdout.strip()


def _commit(cwd: Path, msg: str) -> str:
    _git(cwd, "commit", "-q", "--allow-empty", "-m", msg)
    return _git(cwd, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> tuple[Path, str, str]:
    """Returns (workspace, landed_base_sha, stranded_ahead_sha)."""
    ws = tmp_path / "ws"
    ws.mkdir()
    _git(ws, "init", "-q")
    _git(ws, "config", "user.email", "t@example.com")
    _git(ws, "config", "user.name", "T")
    # Base branch with one landed commit.
    base_sha = _commit(ws, "landed on trunk")
    _git(ws, "branch", "-m", "trunk")
    # Issue branch with an extra commit ahead of trunk (e.g. left by an earlier
    # failed implement) — an ancestor of HEAD but never delivered.
    _git(ws, "checkout", "-q", "-b", "symphony/eng-1")
    ahead_sha = _commit(ws, "stranded local commit")
    return ws, base_sha, ahead_sha


@pytest.mark.asyncio
async def test_commit_landed_in_base_is_accepted(repo: tuple[Path, str, str]) -> None:
    ws, base_sha, _ = repo
    assert await _workspace_ref_landed_in_base(ws, base_sha, "trunk")


@pytest.mark.asyncio
async def test_ancestor_of_head_not_in_base_is_rejected(
    repo: tuple[Path, str, str],
) -> None:
    # The stranded commit is an ancestor of HEAD but not reachable from trunk,
    # so it must NOT pass as a landed-elsewhere delivery.
    ws, _, ahead_sha = repo
    assert not await _workspace_ref_landed_in_base(ws, ahead_sha, "trunk")


@pytest.mark.asyncio
async def test_unknown_commit_is_rejected(repo: tuple[Path, str, str]) -> None:
    ws, _, _ = repo
    assert not await _workspace_ref_landed_in_base(ws, "deadbeef", "trunk")


@pytest.mark.asyncio
async def test_unset_base_branch_is_rejected(repo: tuple[Path, str, str]) -> None:
    ws, base_sha, _ = repo
    assert not await _workspace_ref_landed_in_base(ws, base_sha, None)


@pytest.mark.asyncio
async def test_branch_ahead_of_base_blocks_already_done_close(
    repo: tuple[Path, str, str],
) -> None:
    # The issue branch carries a committed-but-unpushed commit ahead of trunk
    # (e.g. left by an earlier failed implement). Even if the named delivering
    # commit legitimately landed in trunk, the already-satisfied close must be
    # refused so this work reaches the deliver path instead of being discarded.
    ws, _, _ = repo
    assert await _branch_ahead_of_base(ws, "trunk")


@pytest.mark.asyncio
async def test_branch_at_base_tip_is_not_ahead(repo: tuple[Path, str, str]) -> None:
    # A genuine no-op already-satisfied run made no commit, so HEAD sits at the
    # base tip — the guard must let that close through.
    ws, _, _ = repo
    _git(ws, "checkout", "-q", "-B", "symphony/eng-2", "trunk")
    assert not await _branch_ahead_of_base(ws, "trunk")
