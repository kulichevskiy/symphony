"""Helpers for building Codex CLI argv."""

from __future__ import annotations

CODEX_ALLOW_GIT_WRITES_CONFIG = "sandbox_workspace_write.allow_git_writes=true"


def build_codex_workspace_write_command(
    *, prompt: str, codex_model: str
) -> list[str]:
    """Build `codex exec` argv for agents that must modify and commit."""
    return [
        "codex",
        "exec",
        "--json",
        "--sandbox",
        "workspace-write",
        "--config",
        CODEX_ALLOW_GIT_WRITES_CONFIG,
        "--model",
        codex_model,
        prompt,
    ]


__all__ = [
    "CODEX_ALLOW_GIT_WRITES_CONFIG",
    "build_codex_workspace_write_command",
]
