"""Local-only starter project scaffold for ``symphony init``."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

STARTER_CONFIG = """# Starter Symphony configuration. Edit values before running against a real repo.

[repo]
path = "."
default_branch = "main"

[github]
label = "auto"

[git]
author_name = "Symphony"
author_email = "you+symphony@example.com"

[orchestrator]
poll_interval_s = 60
max_concurrent = 3
review_round_cap = 10
codex_renudge_after_min = 10
codex_giveup_after_min = 30

[agent]
model = "claude-opus-4-7"
max_turns = 50

[paths]
worktree_root = ".symphony/worktrees"
prompts_dir = "./prompts"
"""

ROUND1_TEMPLATE = """You are working on issue #{{ issue.number }}: {{ issue.title }}

{{ issue.body }}

{% if issue.comments -%}
## Issue thread
{% for c in issue.comments -%}
> {{ c.author }}: {{ c.body }}
{% endfor %}
{% endif -%}

Repository: {{ repo.owner }}/{{ repo.name }}
Branch: auto/{{ issue.number }} (already checked out)
Base: {{ repo.default_branch }}
Working directory: {{ worktree_path }}

{% if satisfied_deps -%}
Satisfied dependencies:
{% for d in satisfied_deps -%}
- #{{ d.number }} {{ d.title }} (PR {{ d.pr_url or 'merged' }})
{% endfor %}
{% endif -%}

Task: implement the change described in the issue.
- Make focused commits.
- Run tests/linters before declaring done.
- Do not push or open a PR - Symphony handles git operations.
- When done, exit cleanly.
"""

REVIEW_TEMPLATE = """{% if merge_conflict -%}
The PR is approved but cannot be merged: it conflicts with `{{ base_branch }}`.
Resolve the conflicts on commit {{ sha }} so the branch can fast-forward cleanly.

## How to resolve

1. `git fetch origin {{ base_branch }}`
2. `git merge origin/{{ base_branch }}` (do NOT rebase - preserve commit history).
3. Resolve every conflicted file. Read both sides, keep the intent of each
   change, and re-run any tests touched by the conflicting hunks.
4. `git add` the resolved files and complete the merge commit.
5. Confirm `git status` is clean and tests pass.

Do not push - Symphony will push the merge commit and re-request review.
{%- else -%}
Codex requested changes on commit {{ sha }}. Address the feedback below.

{% if review_body -%}
## Reviewer note

{{ review_body }}

{% endif -%}
{% if comments -%}
## Inline comments

{% for c in comments -%}
{% if c.path %}[{{ c.path }}{% if c.line %}:{{ c.line }}{% endif %}]{% else %}[general]{% endif %} {{ c.body }}
{% endfor %}

{% endif -%}
{% if ci_failures -%}
## CI failures

{% for f in ci_failures -%}
- {{ f.name }} (see {{ f.details_url or "PR checks" }})
{% endfor %}

{% endif -%}
When done, ensure tests pass and commit. Do not push - Symphony will push.
{%- endif %}
"""


@dataclass(frozen=True)
class InitAction:
    path: Path
    status: str


def _write_if_missing(path: Path, text: str) -> InitAction:
    if path.exists():
        return InitAction(path=path, status="kept")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return InitAction(path=path, status="created")


def _ensure_gitignore(root: Path) -> InitAction:
    path = root / ".gitignore"
    if path.exists():
        body = path.read_text()
        lines = body.splitlines()
        if ".symphony/" in lines:
            return InitAction(path=path, status="kept")
        suffix = "" if body.endswith("\n") or not body else "\n"
        path.write_text(f"{body}{suffix}.symphony/\n")
        return InitAction(path=path, status="updated")
    path.write_text(".symphony/\n")
    return InitAction(path=path, status="created")


def init_scaffold(root: Path) -> list[InitAction]:
    """Create the local-only starter files. Existing files are preserved."""
    root.mkdir(parents=True, exist_ok=True)
    actions: list[InitAction] = []
    symphony_dir = root / ".symphony"
    symphony_status = "kept" if symphony_dir.exists() else "created"
    (symphony_dir / "worktrees").mkdir(parents=True, exist_ok=True)
    actions.append(InitAction(path=symphony_dir, status=symphony_status))
    actions.append(
        _write_if_missing(root / "symphony.toml", STARTER_CONFIG)
    )
    actions.append(
        _write_if_missing(root / "prompts" / "round1.md.j2", ROUND1_TEMPLATE)
    )
    actions.append(
        _write_if_missing(root / "prompts" / "review.md.j2", REVIEW_TEMPLATE)
    )
    actions.append(_ensure_gitignore(root))
    return actions
