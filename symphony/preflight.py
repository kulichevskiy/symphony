"""Environment checks run before Symphony starts dispatching work."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .config import Config
from .github import GithubError, _run_gh, name_with_owner

REQUIRED_LABELS = ("auto", "auto-stuck", "auto-cycle", "auto-canceled")
CODEX_APP_SLUGS = {"chatgpt-codex-connector", "codex"}


@dataclass(frozen=True)
class PreflightResult:
    name: str
    ok: bool
    message: str


CommandRunner = Callable[[list[str], Path | None], tuple[bool, str]]
GhRunner = Callable[[list[str], Path], str]


def _run_command(args: list[str], cwd: Path | None = None) -> tuple[bool, str]:
    try:
        res = subprocess.run(
            args,
            cwd=str(cwd) if cwd else None,
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        return False, str(e)
    output = (res.stderr or res.stdout).strip()
    return res.returncode == 0, output


def _run_gh_cmd(args: list[str], cwd: Path) -> str:
    return _run_gh(args, cwd=cwd)


def _json_from_gh(
    args: list[str], *, repo_path: Path, gh_runner: GhRunner, context: str
) -> Any:
    out = gh_runner(args, repo_path)
    try:
        return json.loads(out) if out.strip() else {}
    except json.JSONDecodeError as e:
        raise GithubError(f"could not parse JSON from {context}: {e}") from e


def _label_names(payload: Any) -> set[str]:
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        return set()
    if payload and all(isinstance(page, list) for page in payload):
        rows = [item for page in payload for item in page]
    else:
        rows = payload
    return {
        str(item.get("name"))
        for item in rows
        if isinstance(item, dict) and item.get("name")
    }


def _branch_protection_result(
    *,
    cfg: Config,
    owner: str,
    name: str,
    gh_runner: GhRunner,
) -> PreflightResult:
    encoded_branch = quote(cfg.repo.default_branch, safe="")
    try:
        data = _json_from_gh(
            [
                "api",
                f"repos/{owner}/{name}/branches/{encoded_branch}/protection",
            ],
            repo_path=cfg.repo.path,
            gh_runner=gh_runner,
            context=f"branch protection for {cfg.repo.default_branch}",
        )
    except GithubError as e:
        return PreflightResult(
            "branch protection",
            False,
            f"could not read protection for {cfg.repo.default_branch}: {e}",
        )

    required_checks = data.get("required_status_checks") or {}
    check_count = len(required_checks.get("checks") or []) + len(
        required_checks.get("contexts") or []
    )

    if check_count < 1:
        return PreflightResult(
            "branch protection",
            False,
            f"{cfg.repo.default_branch} is missing at least one required CI/status check",
        )
    return PreflightResult(
        "branch protection",
        True,
        f"{cfg.repo.default_branch} requires {check_count} CI/status check(s)",
    )


def _codex_app_result(
    *, owner: str, name: str, repo_path: Path, gh_runner: GhRunner
) -> PreflightResult:
    try:
        data = _json_from_gh(
            ["api", f"/repos/{owner}/{name}/installation"],
            repo_path=repo_path,
            gh_runner=gh_runner,
            context="repository GitHub App installation",
        )
    except GithubError as e:
        return PreflightResult(
            "Codex GitHub App",
            False,
            "could not verify repository GitHub App installation. "
            "This endpoint requires an authenticated GitHub App/JWT token: "
            f"{e}",
        )
    app_slug = str(data.get("app_slug") or "")
    if app_slug and app_slug not in CODEX_APP_SLUGS:
        return PreflightResult(
            "Codex GitHub App",
            False,
            f"repository installation is for unexpected app_slug {app_slug!r}",
        )
    return PreflightResult(
        "Codex GitHub App",
        True,
        "repository Codex GitHub App installation is reachable",
    )


def _labels_result(
    *, owner: str, name: str, repo_path: Path, gh_runner: GhRunner
) -> PreflightResult:
    try:
        payload = _json_from_gh(
            [
                "api",
                f"repos/{owner}/{name}/labels?per_page=100",
                "--paginate",
                "--slurp",
            ],
            repo_path=repo_path,
            gh_runner=gh_runner,
            context="repository labels",
        )
    except GithubError as e:
        return PreflightResult("labels", False, f"could not list labels: {e}")

    names = _label_names(payload)
    missing = [label for label in REQUIRED_LABELS if label not in names]
    if missing:
        return PreflightResult(
            "labels",
            False,
            f"missing required label(s): {', '.join(missing)}",
        )
    return PreflightResult(
        "labels",
        True,
        f"found required label(s): {', '.join(REQUIRED_LABELS)}",
    )


def run_preflight(
    cfg: Config,
    *,
    command_runner: CommandRunner | None = None,
    gh_runner: GhRunner | None = None,
) -> list[PreflightResult]:
    """Run all configured preflight checks and return every result."""
    command_runner = command_runner or _run_command
    gh_runner = gh_runner or _run_gh_cmd
    results: list[PreflightResult] = []

    ok, output = command_runner(["gh", "auth", "status"], None)
    results.append(
        PreflightResult(
            "gh auth",
            ok,
            "gh auth status succeeded" if ok else f"gh auth status failed: {output}",
        )
    )

    version_ok, version_output = command_runner(["claude", "--version"], None)
    prompt_ok, prompt_output = command_runner(
        ["claude", "-p", "ok", "--max-turns", "1"], None
    )
    claude_ok = version_ok and prompt_ok
    if claude_ok:
        claude_message = "claude --version and claude -p ok succeeded"
    else:
        failures = []
        if not version_ok:
            failures.append(f"version failed: {version_output}")
        if not prompt_ok:
            failures.append(f"prompt failed: {prompt_output}")
        claude_message = "; ".join(failures)
    results.append(PreflightResult("claude", claude_ok, claude_message))

    root = cfg.paths.worktree_root
    root_ok = root.is_dir() and os.access(root, os.W_OK)
    if root_ok:
        root_message = f"{root} exists and is writable"
    elif not root.exists():
        root_message = f"{root} does not exist"
    elif not root.is_dir():
        root_message = f"{root} is not a directory"
    else:
        root_message = f"{root} is not writable"
    results.append(PreflightResult("worktree_root", root_ok, root_message))

    try:
        owner, name = name_with_owner(cfg.repo.path)
    except GithubError as e:
        message = f"could not resolve GitHub repo: {e}"
        results.extend(
            [
                PreflightResult("branch protection", False, message),
                PreflightResult("Codex GitHub App", False, message),
                PreflightResult("labels", False, message),
            ]
        )
        return results

    results.append(
        _branch_protection_result(
            cfg=cfg, owner=owner, name=name, gh_runner=gh_runner
        )
    )
    results.append(
        _codex_app_result(
            owner=owner, name=name, repo_path=cfg.repo.path, gh_runner=gh_runner
        )
    )
    results.append(
        _labels_result(
            owner=owner, name=name, repo_path=cfg.repo.path, gh_runner=gh_runner
        )
    )
    return results


def preflight_ok(results: list[PreflightResult]) -> bool:
    return all(result.ok for result in results)


def format_preflight_results(results: list[PreflightResult]) -> str:
    return "\n".join(
        f"{'OK' if result.ok else 'FAIL'} {result.name}: {result.message}"
        for result in results
    )
