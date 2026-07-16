"""Git / workspace subprocess primitives for the poll loop (SYM-143).

Pure move out of ``poll/__init__.py`` — bodies are unchanged. Re-exported by
the package ``__init__`` so existing ``poll.<name>`` references and test
monkeypatches keep resolving.
"""

from __future__ import annotations

import asyncio
import base64
from pathlib import Path

from ...pipeline.local_review import DiffSize, parse_diff_numstat

_DEFAULT_PUSH_AUTH_HOST = "github.com"


def _push_auth_config_key(host: str) -> str:
    return f"http.https://{host}/.extraheader"


async def _push_auth_host(workspace_path: Path) -> str | None:
    """The host to scope the push-auth header to, derived from `origin`.

    Handles the documented `[HOST/]OWNER/REPO` binding form (e.g. a GHE
    remote like `ghe.example.com/org/repo`), whose `origin` is not
    `github.com` — a hardcoded host would silently fail to authenticate
    those pushes. Falls back to the default host when `origin` can't be
    read (workspace not yet a git repo, no remote configured) so existing
    single-host deployments are unaffected. Returns ``None`` only when
    `origin` is unambiguously an SSH remote (the documented
    `gh auth login --git-protocol ssh` flow, README.md:48) — `http.extraHeader`
    does not apply to that transport, so injecting it would be a no-op.

    Never raises: a workspace dir that doesn't exist yet (or any other
    OS-level failure starting `git`) degrades to the default host, matching
    `_clear_git_push_auth`'s existing best-effort contract.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "git",
            "remote",
            "get-url",
            "origin",
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            stdin=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
    except OSError:
        return _DEFAULT_PUSH_AUTH_HOST
    if proc.returncode != 0:
        return _DEFAULT_PUSH_AUTH_HOST
    url = stdout.decode(errors="replace").strip()
    if url.startswith("https://"):
        return url[len("https://") :].split("/", 1)[0] or _DEFAULT_PUSH_AUTH_HOST
    if url.startswith("git@") or url.startswith("ssh://"):
        return None
    return _DEFAULT_PUSH_AUTH_HOST


async def _configure_git_push_auth(workspace_path: Path, token: str) -> None:
    """Point *workspace_path*'s local (non-global) git config at *token*.

    Scoped to this one workspace's `.git/config`, so a concurrent run pushing
    from a different workspace is unaffected. `x-access-token` is the GitHub
    convention for a token-as-password Basic credential. Paired with
    `_clear_git_push_auth`, called right after the push regardless of outcome.
    A no-op when `origin` is an SSH remote (see `_push_auth_host`).
    """
    host = await _push_auth_host(workspace_path)
    if host is None:
        return
    basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    proc = await asyncio.create_subprocess_exec(
        "git",
        "config",
        "--local",
        _push_auth_config_key(host),
        f"AUTHORIZATION: basic {basic}",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git config push auth failed: {stderr.decode(errors='replace').strip()}"
        )


async def _clear_git_push_auth(workspace_path: Path) -> None:
    """Best-effort removal of the push-auth header set by `_configure_git_push_auth`.

    Called unconditionally before every push (OAuth in UI 4/7 review fix), so
    a workspace that doesn't exist yet (or any other OS-level failure
    starting `git`) must not raise — there is no header to clear either way.
    Clears both the current `origin`-derived host key and the default host
    key unconditionally, so a header written by an older build (hardcoded to
    the default host) or left behind after a binding's host changed is still
    cleaned up.
    """
    host = await _push_auth_host(workspace_path)
    keys = {_push_auth_config_key(_DEFAULT_PUSH_AUTH_HOST)}
    if host is not None:
        keys.add(_push_auth_config_key(host))
    for key in keys:
        try:
            proc = await asyncio.create_subprocess_exec(
                "git",
                "config",
                "--local",
                "--unset-all",
                key,
                cwd=str(workspace_path),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                stdin=asyncio.subprocess.DEVNULL,
            )
            await proc.communicate()
        except OSError:
            pass


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
        raise RuntimeError(f"git push failed: {stderr.decode(errors='replace').strip()}")


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
            f"git push --force-with-lease failed: {stderr.decode(errors='replace').strip()}"
        )


async def _sync_workspace_to_remote(workspace_path: Path, branch: str) -> None:
    """Fetch and hard-reset the workspace to origin/branch.

    Called before the merge agent so that local commits left behind by
    review-fix runs (which may have diverged from the remote) do not cause
    a non-fast-forward push failure later.
    """
    fetch_proc = await asyncio.create_subprocess_exec(
        "git",
        "fetch",
        "origin",
        branch,
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await fetch_proc.communicate()
    if fetch_proc.returncode != 0:
        raise RuntimeError(f"git fetch failed: {stderr.decode(errors='replace').strip()}")
    reset_proc = await asyncio.create_subprocess_exec(
        "git",
        "reset",
        "--hard",
        f"origin/{branch}",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await reset_proc.communicate()
    if reset_proc.returncode != 0:
        raise RuntimeError(f"git reset --hard failed: {stderr.decode(errors='replace').strip()}")


async def _git_fetch(workspace_path: Path) -> None:
    """Run ``git fetch origin`` in *workspace_path*."""
    proc = await asyncio.create_subprocess_exec(
        "git",
        "fetch",
        "origin",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"git fetch failed: {stderr.decode(errors='replace').strip()}")


async def _git_fetch_branch(workspace_path: Path, branch: str) -> None:
    """Fetch ``origin/branch`` so remote-head validation has a fresh baseline."""
    proc = await asyncio.create_subprocess_exec(
        "git",
        "fetch",
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
            f"git fetch origin {branch} failed: {stderr.decode(errors='replace').strip()}"
        )


async def _git_status_short(workspace_path: Path) -> str:
    """Return ``git status --short`` output for failure diagnostics."""
    proc = await asyncio.create_subprocess_exec(
        "git",
        "status",
        "--short",
        "--untracked-files=all",
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
        "git",
        "rebase",
        upstream,
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
        "git",
        "rebase",
        "--abort",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"git rebase --abort failed: {stderr.decode(errors='replace').strip()}")


async def _git_conflicted_files(workspace_path: Path) -> list[str]:
    """Return a list of paths with unresolved conflict markers."""
    proc = await asyncio.create_subprocess_exec(
        "git",
        "diff",
        "--name-only",
        "--diff-filter=U",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        stdin=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    return [p for p in stdout.decode().splitlines() if p]


async def _git_tree_is_clean(workspace_path: Path) -> bool:
    """True if the working tree has no staged, unstaged, or untracked changes."""
    proc = await asyncio.create_subprocess_exec(
        "git",
        "status",
        "--porcelain",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        stdin=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    return proc.returncode == 0 and not stdout.strip()


async def _git_add_and_continue_rebase(workspace_path: Path, files: list[str]) -> bool:
    """Stage *files* and run ``git rebase --continue``.

    Returns ``True`` when the rebase completed, or when no rebase is in
    progress and the tree is clean (already-resolved — treat as benign no-op).
    Returns ``False`` when Git stopped again, which may be a later conflicting
    commit in a multi-commit rebase.
    """
    # Fast-path: if no rebase is in progress and the tree is clean, the rebase
    # was already completed (SYM-148). Return success before staging stale paths.
    if not await _rebase_in_progress(workspace_path):
        if await _git_tree_is_clean(workspace_path):
            return True
    if files:
        add_proc = await asyncio.create_subprocess_exec(
            "git",
            "add",
            "--",
            *files,
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
        _, stderr = await add_proc.communicate()
        if add_proc.returncode != 0:
            raise RuntimeError(f"git add failed: {stderr.decode(errors='replace').strip()}")
    import os  # noqa: PLC0415

    env = {**os.environ, "GIT_EDITOR": "true"}
    cont_proc = await asyncio.create_subprocess_exec(
        "git",
        "rebase",
        "--continue",
        cwd=str(workspace_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
        env=env,
    )
    await cont_proc.communicate()
    if cont_proc.returncode == 0:
        return True
    # A non-zero exit may still be benign: the rebase completed between our
    # pre-check and this call (SYM-148). Require both no rebase in progress
    # AND a fully clean tree — not just absence of unmerged paths — to avoid
    # masking genuine failures with staged-but-non-conflict changes.
    if not await _rebase_in_progress(workspace_path):
        if await _git_tree_is_clean(workspace_path):
            return True
    return False


async def _rebase_in_progress(workspace_path: Path) -> bool:
    """True if a rebase is in progress in *workspace_path*.

    Resolves the real git dir via ``git rev-parse --git-path`` so it works for
    worktrees (where ``.git`` is a file, not a directory) as well as plain
    clones, then checks for the ``rebase-merge`` / ``rebase-apply`` state dirs.
    """
    for name in ("rebase-merge", "rebase-apply"):
        proc = await asyncio.create_subprocess_exec(
            "git",
            "rev-parse",
            "--git-path",
            name,
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
            "git",
            "rev-parse",
            "--verify",
            ref,
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
            "git",
            "merge-base",
            "--is-ancestor",
            ancestor,
            descendant,
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


async def _workspace_commits_ahead(workspace_path: Path, base_branch: str) -> int | None:
    """Commits on HEAD not in *base_branch*, or None if undeterminable.

    Prefer `origin/<base>` (present after a fresh clone), fall back to the
    local `<base>` ref. Returns None when neither ref resolves so callers can
    degrade gracefully rather than mistake a measurement failure for "empty".
    """
    for ref in (f"origin/{base_branch}..HEAD", f"{base_branch}..HEAD"):
        try:
            proc = await asyncio.create_subprocess_exec(
                "git",
                "rev-list",
                "--count",
                ref,
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


async def _workspace_diff_size(workspace_path: Path, base_branch: str) -> DiffSize:
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
                "git",
                "diff",
                "--numstat",
                ref,
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
                "git",
                "rev-list",
                "--count",
                ref,
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


async def _workspace_scrub(workspace_path: Path, target_sha: str) -> None:
    """Reset the branch to *target_sha* and remove untracked files.

    Runs `git reset --hard <target_sha>` then `git clean -fd`. `target_sha` is
    the branch HEAD captured *before* a reviewer/verifier pass ran; resetting to
    it discards not just the pass's throwaway working-tree edits but any commit
    it made — so a reviewer run (now unsandboxed) can never contribute a commit
    to the diff the fixer sees or the branch that gets pushed. Earlier fix
    commits are preserved (they are already part of `target_sha`).

    Falls back to a working-tree-only clean when `target_sha` is empty (e.g. an
    empty repo) so we never hard-reset to nothing. Best-effort: failures are
    swallowed so a scrub hiccup never breaks the local-review phase.
    """
    reset_argv = (
        ["git", "reset", "--hard", target_sha] if target_sha else ["git", "checkout", "--", "."]
    )
    for argv in (
        reset_argv,
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
            "git",
            "status",
            "--porcelain",
            cwd=str(workspace_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            stdin=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            return [line for line in stdout.decode(errors="replace").splitlines() if line.strip()]
    except Exception:  # noqa: BLE001
        pass
    return []
