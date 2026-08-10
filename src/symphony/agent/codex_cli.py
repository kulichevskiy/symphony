"""Helpers for building Codex CLI argv.

Codex runs bypass its OS sandbox (`--dangerously-bypass-approvals-and-sandbox`)
because bubblewrap can't initialize nested inside our container; the container is
the isolation boundary. No permissions profile is provisioned as a result.
"""

from __future__ import annotations

import json
from pathlib import Path


def codex_reasoning_effort_config(effort: str) -> str:
    """Serialize one effort value as a single TOML basic string."""
    return f"model_reasoning_effort={json.dumps(effort, ensure_ascii=False)}"


def build_codex_workspace_write_command(
    *,
    prompt: str,
    codex_model: str,
    workspace_path: Path,
    effort: str | None = None,
) -> list[str]:
    """Build `codex exec` argv for agents that must modify and commit.

    Uses `--dangerously-bypass-approvals-and-sandbox`: codex's OS sandbox
    (bubblewrap) can't initialize nested inside our Docker container — every
    run dies at namespace/uid-map/loopback setup ("bwrap: ..."). That flag is
    codex's documented mode for "already-sandboxed environments"; the container
    IS the isolation boundary (non-root, ephemeral workspace clone, per-binding
    env allowlist), so the agent may write anywhere inside it.

    `effort` maps to `--config model_reasoning_effort="<v>"`. Unset → no flag,
    so the Codex CLI default stands.
    """
    command = [
        "codex",
        "exec",
        "--json",
        "--dangerously-bypass-approvals-and-sandbox",
    ]
    command += ["--cd", str(workspace_path)]
    if effort is not None:
        command += ["--config", codex_reasoning_effort_config(effort)]
    command += ["--model", codex_model, prompt]
    return command


__all__ = [
    "build_codex_workspace_write_command",
    "codex_reasoning_effort_config",
]
