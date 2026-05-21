# Codex Git Sandbox Research

## 2026-05-21 worktree project-root regression

Post-#93, Symphony runs local Codex write stages with the named
`symphony-git` permissions profile and no `--sandbox workspace-write`.
That profile allows project-root writes plus `.git` writes, but it relied on
the profile-relative `.` entry under `:project_roots`.

The failing implement/review-fix runs were launched with subprocess
`cwd=workspace_path`. In linked worktrees, Codex can resolve the git/common
project root differently from Symphony's disposable worktree path, so
`apply_patch` can reject files that are inside the worktree as outside the
resolved project roots.

The fix is to pin the writable worktree per invocation. For the current
worktree-cwd run, Symphony resolves:

```text
/Users/ak/Code/symphonyd/workspaces/kulichevskiy_ssymphonyd/sym-26
```

and passes this Codex config override:

```text
permissions.symphony-git.filesystem.":project_roots"."/Users/ak/Code/symphonyd/workspaces/kulichevskiy_ssymphonyd/sym-26"="write"
```

At runtime the absolute value is derived from `workspace_path.resolve()`, so a
VIB worktree such as `/Users/ak/Code/symphonyd/workspaces/vibecamp-org_svibecamp/vib-94`
gets its own absolute `:project_roots` write entry instead of relying on `.`.
