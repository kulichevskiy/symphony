"""Shared test helpers for faking a workspace that the Implement stage's
completion gate inspects.

The gate now requires HEAD to advance ≥1 commit over the branch base before an
rc=0 run is treated as ``completed``. Fake runners call ``advance_head`` so a
simulated successful run leaves a real commit, mirroring an agent that did the
work and committed it.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def advance_head(workspace_path: Path) -> None:
    """Initialise *workspace_path* as a git repo (if needed) and add a commit.

    Idempotent enough for tests: it always creates a fresh commit so HEAD
    moves relative to whatever it was before the call.
    """
    workspace_path.mkdir(parents=True, exist_ok=True)
    git_dir = workspace_path / ".git"
    if not git_dir.exists():
        _git(workspace_path, "init", "-q")
    # Always pin a commit identity on the repo's own config. The workspace may
    # have been initialised elsewhere (e.g. `_init_git_workspace`) which only
    # sets identity via per-process env vars that don't persist here, so without
    # this the commit below fails with exit 128 in CI (no global git identity).
    _git(workspace_path, "config", "user.email", "test@example.com")
    _git(workspace_path, "config", "user.name", "Test")
    marker = workspace_path / "implemented.txt"
    existing = marker.read_text() if marker.exists() else ""
    marker.write_text(existing + "x")
    _git(workspace_path, "add", "-A")
    _git(workspace_path, "commit", "-q", "-m", "agent change", "--allow-empty")


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
