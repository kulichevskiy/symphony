"""Git / workspace subprocess primitives for the poll loop (SYM-143).

Pure move out of ``poll/__init__.py`` — bodies are unchanged. Re-exported by
the package ``__init__`` so existing ``poll.<name>`` references and test
monkeypatches keep resolving.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from ...pipeline.local_review import DiffSize, parse_diff_numstat


async def _default_push(workspace_path: Path, branch: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "git",
        "push",
        "-u",
        "origin",
        branch,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git push failed: {stderr.decode(errors='replace').strip()}"
        )


async def _default_force_push(workspace_path: Path, branch: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "git",
        "push",
        "--force-with-lease",
        "-u",
        "origin",
        branch,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git push --force-with-lease failed: "
            f"{stderr.decode(errors='replace').strip()}"
        )


async def _sync_workspace_to_remote(workspace_path: Path, branch: str) -> None:
    """Fetch and hard-reset the workspace to origin/branch.

    Called before the merge agent so that local commits left behind by
    review-fix runs (which may have diverged from the remote) do not cause
    a non-fast-forward push failure later.
    """
    fetch_proc = await asyncio.create_subprocess_exec(
        "git", "fetch", "origin", branch,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await fetch_proc.communicate()
    if fetch_proc.returncode != 0:
        raise RuntimeError(
            f"git fetch failed: {stderr.decode(errors='replace').strip()}"
        )
    reset_proc = await asyncio.create_subprocess_exec(
        "git", "reset", "--hard", f"origin/{branch}",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await reset_proc.communicate()
    if reset_proc.returncode != 0:
        raise RuntimeError(
            f"git reset --hard failed: {stderr.decode(errors='replace').strip()}"
        )


async def _git_fetch(workspace_path: Path) -> None:
    """Run ``git fetch origin`` in *workspace_path*."""
    proc = await asyncio.create_subprocess_exec(
        "git", "fetch", "origin",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git fetch failed: {stderr.decode(errors='replace').strip()}"
        )


async def _git_fetch_branch(workspace_path: Path, branch: str) -> None:
    """Fetch ``origin/branch`` so remote-head validation has a fresh baseline."""
    proc = await asyncio.create_subprocess_exec(
        "git", "fetch", "origin", branch,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git fetch origin {branch} failed: {stderr.decode(errors='replace').strip()}"
        )


async def _git_status_short(workspace_path: Path) -> str:
    """Return ``git status --short`` output for failure diagnostics."""
    proc = await asyncio.create_subprocess_exec(
        "git", "status", "--short", "--untracked-files=all",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        return f"<git status failed: {stderr.decode(errors='replace').strip()}>"
    return stdout.decode(errors="replace").strip()


async def _git_rebase(workspace_path: Path, upstream: str) -> bool:
    """Run ``git rebase upstream``.

    Returns ``True`` if the rebase completed cleanly (exit code 0), ``False``
    if it stopped due to conflicts.
    """
    proc = await asyncio.create_subprocess_exec(
        "git", "rebase", upstream,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    await proc.communicate()
    return proc.returncode == 0


async def _git_abort_rebase(workspace_path: Path) -> None:
    """Abort an in-progress rebase in *workspace_path*."""
    proc = await asyncio.create_subprocess_exec(
        "git", "rebase", "--abort",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git rebase --abort failed: {stderr.decode(errors='replace').strip()}"
        )


async def _git_conflicted_files(workspace_path: Path) -> list[str]:
    """Return a list of paths with unresolved conflict markers."""
    proc = await asyncio.create_subprocess_exec(
        "git", "diff", "--name-only", "--diff-filter=U",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        stdin=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    return [p for p in stdout.decode().splitlines() if p]


async def _git_add_and_continue_rebase(
    workspace_path: Path, files: list[str]
) -> bool:
    """Stage *files* and run ``git rebase --continue``.

    Returns ``True`` when the rebase completed. Returns ``False`` when Git
    stopped again, which may be a later conflicting commit in a multi-commit
    rebase.
    """
    if files:
        add_proc = await asyncio.create_subprocess_exec(
            "git", "add", "--", *files,
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
        _, stderr = await add_proc.communicate()
        if add_proc.returncode != 0:
            raise RuntimeError(
                f"git add failed: {stderr.decode(errors='replace').strip()}"
            )
    import os  # noqa: PLC0415
    env = {**os.environ, "GIT_EDITOR": "true"}
    cont_proc = await asyncio.create_subprocess_exec(
        "git", "rebase", "--continue",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
        env=env,
    )
    await cont_proc.communicate()
    if cont_proc.returncode == 0:
        return True
    # A non-zero exit is benign when the rebase has already reached its desired
    # end state (SYM-148): a concurrent run may have completed it, so no rebase
    # is in progress and the tree has no unmerged paths. Treat that as success.
    # A genuine unresolved conflict leaves the rebase in progress (or leaves
    # unmerged paths), so it still reports failure.
    if not await _rebase_in_progress(workspace_path):
        if not await _git_conflicted_files(workspace_path):
            if not await _rebase_had_recent_skips(workspace_path):
                return True
    return False


async def _rebase_had_recent_skips(workspace_path: Path) -> bool:
    """True if the most recently completed rebase used --skip for any commit.

    Uses two strategies to cover all known git reflog formats:

    1. Explicit marker: git ≤2.37 writes ``rebase (skip) (finish): …``
       (combined); newer git may write a separate ``rebase (skip): …`` entry
       before the ``(finish)`` entry.
    2. SHA fallback: when no explicit marker exists, compare the HEAD SHA at
       ``(finish)`` with the SHA at the rebase start (``checkout``).  If they
       are identical no commits were applied — all were dropped via --skip.
    """
    proc = await asyncio.create_subprocess_exec(
        "git", "reflog", "HEAD", "--format=%H %gs",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        stdin=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return False
    finish_sha: str | None = None
    for raw in stdout.decode().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        sha, _, subject = raw.partition(" ")
        if not subject.startswith("rebase"):
            if finish_sha is not None:
                break  # left the rebase session without finding a checkout
            continue
        if "(finish)" in subject:
            if "(skip)" in subject:
                return True  # git ≤2.37: combined marker
            finish_sha = sha
        elif finish_sha is not None:
            if "(skip)" in subject:
                return True  # newer git: separate "(skip)" entry
            if "checkout" in subject:
                # start-of-rebase entry; sha == onto SHA
                return finish_sha == sha
    return False


async def _rebase_in_progress(workspace_path: Path) -> bool:
    """True if a rebase is in progress in *workspace_path*.

    Resolves the real git dir via ``git rev-parse --git-path`` so it works for
    worktrees (where ``.git`` is a file, not a directory) as well as plain
    clones, then checks for the ``rebase-merge`` / ``rebase-apply`` state dirs.
    """
    for name in ("rebase-merge", "rebase-apply"):
        proc = await asyncio.create_subprocess_exec(
            "git", "rev-parse", "--git-path", name,
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            stdin=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            continue
        rel = stdout.decode(errors="replace").strip()
        if rel and (workspace_path / rel).exists():
            return True
    return False


async def _workspace_head_sha(workspace_path: Path) -> str:
    """Return the HEAD commit SHA of *workspace_path*, or "" on error."""
    return await _workspace_ref_sha(workspace_path, "HEAD")


async def _workspace_ref_sha(workspace_path: Path, ref: str) -> str:
    """Return the commit SHA for *ref* in *workspace_path*, or "" on error."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "rev-parse", "--verify", ref,
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            return stdout.decode().strip()
    except Exception:  # noqa: BLE001
        pass
    return ""


async def _workspace_ref_is_ancestor(
    workspace_path: Path, ancestor: str, descendant: str = "HEAD"
) -> bool:
    """True iff *ancestor* is a commit reachable from *descendant* (default
    HEAD) in *workspace_path*. False on any error (bad ref, not a repo).

    Wraps ``git merge-base --is-ancestor`` (exit 0 = ancestor, 1 = not,
    128 = bad/unknown commit). Used to verify an already-done claim before
    auto-closing the issue.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "merge-base", "--is-ancestor", ancestor, descendant,
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.communicate()
        return proc.returncode == 0
    except Exception:  # noqa: BLE001
        return False


async def _workspace_ref_landed_in_base(
    workspace_path: Path, ref: str, base_branch: str | None
) -> bool:
    """True iff *ref* is reachable from the delivery base branch tip.

    A genuinely already-delivered commit lives in the base branch's history, so
    "already done elsewhere" is verified against the base, not against HEAD.
    Checking HEAD alone is unsafe: on a retry after an earlier failed implement,
    unpushed commits left on the issue branch are ancestors of HEAD too, so a
    bogus already-done ref naming one of them would falsely pass. Tries
    ``origin/<base>`` first (present after a fresh clone), then the local
    ``<base>`` ref. False if base is unset or neither ref resolves.
    """
    if not base_branch:
        return False
    for descendant in (f"origin/{base_branch}", base_branch):
        if await _workspace_ref_is_ancestor(workspace_path, ref, descendant):
            return True
    return False


async def _workspace_commits_ahead(
    workspace_path: Path, base_branch: str
) -> int | None:
    """Commits on HEAD not in *base_branch*, or None if undeterminable.

    Prefer `origin/<base>` (present after a fresh clone), fall back to the
    local `<base>` ref. Returns None when neither ref resolves so callers can
    degrade gracefully rather than mistake a measurement failure for "empty".
    """
    for ref in (f"origin/{base_branch}..HEAD", f"{base_branch}..HEAD"):
        try:
            proc = await asyncio.create_subprocess_exec(
                "git", "rev-list", "--count", ref,
                cwd=str(workspace_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode == 0:
                return int(stdout.decode().strip() or "0")
        except Exception:  # noqa: BLE001
            pass
    return None


async def _workspace_diff_size(
    workspace_path: Path, base_branch: str
) -> DiffSize:
    """Measure the branch's diff vs *base_branch* via `git diff --numstat`.

    Mirrors the reviewer prompt's ref logic: prefer `origin/<base>...HEAD`,
    fall back to `<base>...HEAD` when origin is absent. On any error,
    report a small diff so the reviewer only escalates to the expensive
    two-pass review when the diff is *provably* large — an unmeasurable
    diff degrades to the cheaper single pass.
    """
    for ref in (f"origin/{base_branch}...HEAD", f"{base_branch}...HEAD"):
        try:
            proc = await asyncio.create_subprocess_exec(
                "git", "diff", "--numstat", ref,
                cwd=str(workspace_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode == 0:
                return parse_diff_numstat(stdout.decode(errors="replace"))
        except Exception:  # noqa: BLE001
            pass
    # Neither ref resolved — treat as small so we don't pay for two passes
    # on a diff we couldn't size.
    return DiffSize(changed_lines=0, changed_files=0)


async def _branch_ahead_of_base(workspace_path: Path, base_branch: str | None) -> bool:
    """True if HEAD has ≥1 commit not in *base_branch* (`git rev-list base..HEAD`).

    Mirrors the diff helper's ref logic: prefer `origin/<base>..HEAD`, fall back
    to `<base>..HEAD` when origin is absent. On any error (or no base), report
    False so the run takes the normal agent path instead of skipping it — a
    branch we can't prove is ahead must not bypass the implementer.
    """
    if not base_branch:
        return False
    for ref in (f"origin/{base_branch}..HEAD", f"{base_branch}..HEAD"):
        try:
            proc = await asyncio.create_subprocess_exec(
                "git", "rev-list", "--count", ref,
                cwd=str(workspace_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode == 0:
                count = stdout.decode().strip()
                return count.isdigit() and int(count) > 0
        except Exception:  # noqa: BLE001
            pass
    return False


async def _workspace_scrub(workspace_path: Path) -> None:
    """Reset the working tree to HEAD and remove untracked files.

    Runs `git checkout -- .` then `git clean -fd` so a pass-2 verifier's
    throwaway tests / scratch edits never reach the diff the fixer sees or
    the branch that gets pushed. Best-effort: failures are swallowed so a
    scrub hiccup never breaks the local-review phase.
    """
    for argv in (
        ["git", "checkout", "--", "."],
        ["git", "clean", "-fd"],
    ):
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                cwd=str(workspace_path),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.communicate()
        except Exception:  # noqa: BLE001
            pass


async def _workspace_dirty_files(workspace_path: Path) -> list[str]:
    """`git status --porcelain` entries for *workspace_path*.

    Returns the raw porcelain lines (status prefix + path). Best-effort
    like the other workspace helpers: if git itself fails (not a repo,
    git missing) the tree can't be inspected and we return [] so the
    gate degrades to today's behavior instead of dead-ending every push.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "status", "--porcelain",
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            stdin=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            return [
                line
                for line in stdout.decode(errors="replace").splitlines()
                if line.strip()
            ]
    except Exception:  # noqa: BLE001
        pass
    return []
