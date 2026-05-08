# Symphony

A personal autopilot that watches GitHub Issues labeled `auto`, dispatches
[Claude Code](https://www.anthropic.com/claude-code) agents in git worktrees to
implement them, runs the resulting pull requests through the
[Codex GitHub App](https://github.com/apps/chatgpt-codex-connector) review until
approved, and auto-merges.

This is a Python implementation of the idea behind
[`openai/symphony`](https://github.com/openai/symphony) — same architectural
shape (loader, orchestrator, workspace, agent runner, event-driven state
machine), different stack. Built for one user, one repo per Symphony instance.
Not a team service.

## Status

Position: **personal tool, others welcome to use as-is**. Used by the author on
their own repos, including this one (Symphony self-hosts and writes its own
issues). No release cadence, no semver promise, no support SLA. Issues and PRs
welcome but answered on a best-effort basis.

## How it works

```
poll GitHub for issues labeled `auto`
  → resolve task-list dependencies, pick FIFO from ready set
    → create worktree at $worktree_root/<repo>-<num> on branch auto/<num>
      → spawn `claude` with a rendered round-1 prompt
        → on agent exit: commit, push, open PR with `Closes #N`
          → poll PR for Codex review verdict (👍 reaction = approved)
            → on changes-requested, feed comments back via `claude --resume`
              → on approval + green required CI, `gh pr merge --squash`
```

See [`SYMPHONY.md`](SYMPHONY.md) for the full design notes, decision log, and
failure-mode table.

## Requirements

You will need all of these. Symphony does not install any of them.

- **Python ≥ 3.11** and [`uv`](https://docs.astral.sh/uv/)
- **`gh` CLI**, authenticated (`gh auth login`)
- **`claude` CLI** (Claude Code), authenticated — Symphony shells out to your
  Claude Code subscription rather than the API, so no API billing
- **`git` ≥ 2.20** (worktree support)
- **The Codex GitHub App**
  ([`chatgpt-codex-connector`](https://github.com/apps/chatgpt-codex-connector))
  installed on the target repo. Symphony's review loop is hard-coded to Codex —
  there is no other reviewer adapter
- **Branch protection** on the default branch with at least one required status
  check (CI must be green to merge)

## Quickstart

Install Symphony:

```bash
git clone https://github.com/kulichevskiy/symphony.git ~/Code/symphony
cd ~/Code/symphony
uv sync
uv tool install --editable .
symphony --version
```

Initialize the target repo:

```bash
cd /path/to/your/project
symphony init                  # writes symphony.toml + prompts/
gh label create auto --color 0E8A16
gh label create auto-stuck --color B60205
gh label create auto-cycle --color B60205
gh label create auto-canceled --color C5DEF5
$EDITOR symphony.toml          # set author_email, paths
symphony preflight             # gh auth, claude auth, branch protection, labels
```

Try one issue end-to-end:

```bash
gh issue edit 42 --add-label auto
symphony run-once 42
```

Then run the loop:

```bash
symphony run                   # foreground; keep it in tmux
```

For the full walkthrough — flags, commands, gotchas, recovery — read
[`TUTORIAL.md`](TUTORIAL.md).

## CLI

```
symphony init        write symphony.toml + prompts/, mkdir .symphony
symphony preflight   sanity-check gh auth, claude auth, branch protection, labels
symphony run         main loop (foreground)
symphony run-once N  one-shot dispatch for a single issue
symphony status      snapshot of in-flight + recent runs
symphony logs        tail JSON event log; --issue N to filter
symphony cancel N    stop loop for issue N, leave artifacts, label auto-canceled
symphony gc          remove auto-stuck or orphaned worktrees older than N days
```

## Out of scope (v1)

Multiple repos per instance, daemon/launchd, TUI/HTTP dashboard, hot config
reload, hooks beyond `after_create`, Linear/Jira/Codex/Aider adapters. Read
[`SYMPHONY.md`](SYMPHONY.md) for the full list and rationale.

## Acknowledgements

The design follows [`openai/symphony`](https://github.com/openai/symphony) —
its SPEC laid out the loader/orchestrator/workspace/agent-runner shape and the
event-driven state machine. Symphony swaps in Claude Code as the agent and the
Codex GitHub App as the reviewer, and diverges where the SPEC's defaults
didn't fit personal use (concurrency cap, verdict parsing, merge path). See
[`SYMPHONY.md`](SYMPHONY.md) for the full set of divergences.

## License

MIT — see [`LICENSE`](LICENSE).
