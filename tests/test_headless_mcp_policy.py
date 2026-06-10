"""SYM-105 headless MCP policy.

Three guarantees:
1. Implement/fix prompts ban interactive auth flows and give the agent the
   `SYMPHONY_BLOCKED` escape hatch.
2. Claude spawns use a strict MCP allowlist derived from the binding —
   unlisted servers are invisible; the default allowlist is empty.
3. Per-binding `env:` is resolved from symphony's `.env` (secrets never in
   YAML) and injected into the agent process env.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.agent.prompt import (
    acceptance_fix_prompt,
    implement_prompt,
    merge_conflict_fix_prompt,
    merge_conflict_rebase_fix_prompt,
    merge_prompt,
    merge_required_check_fix_prompt,
    review_comment_fix_prompt,
    review_fix_prompt,
)
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator, build_runner_command
from symphony.pipeline.local_review import VERDICT_CHANGES_REQUESTED_MARKER
from symphony.pipeline.local_review_loop import LoopOutcome
from symphony.pipeline.local_review_session import (
    _build_fix_command,
    run_local_review_session,
)

# --- 1. Headless / no-interactive-auth prompt rule -------------------------


def _change_prompts() -> list[str]:
    common = {
        "issue_title": "Apply schema migration",
        "issue_body": "Add the breakdown table.",
        "labels": ["symphony"],
    }
    return [
        implement_prompt(**common),
        review_fix_prompt(**common, trigger="CI red", failing_check_log_tail="boom"),
        review_comment_fix_prompt(**common, trigger="reviewer comment"),
        acceptance_fix_prompt(**common, acceptance_verdict="rejected"),
        merge_conflict_fix_prompt(
            **common, base_branch="main", conflicted_files=["a.py"]
        ),
        merge_conflict_rebase_fix_prompt(**common, pr_number=7, base_ref="main"),
        merge_required_check_fix_prompt(
            **common,
            pr_number=7,
            head_sha="abc123",
            merge_error="required check failing",
            failing_checks=[],
            action_log_tail="",
        ),
        merge_prompt(**common, pr_url="https://github.com/org/repo/pull/7"),
    ]


def test_implement_and_fix_prompts_contain_headless_rule() -> None:
    for prompt in _change_prompts():
        assert "You run headless" in prompt
        assert "OAuth URLs" in prompt
        assert "device codes" in prompt
        assert "SYMPHONY_BLOCKED" in prompt


# --- 2. Strict MCP allowlist on claude spawns -------------------------------


def test_claude_runner_command_is_strict_mcp_with_no_servers_by_default() -> None:
    argv = build_runner_command("claude", "do it")
    assert "--strict-mcp-config" in argv
    assert "--mcp-config" not in argv
    assert argv[-1] == "do it"


def test_claude_runner_command_passes_binding_mcp_allowlist() -> None:
    servers = {"supabase": {"type": "http", "url": "https://mcp.example"}}
    argv = build_runner_command("claude", "do it", mcp_servers=servers)
    assert "--strict-mcp-config" in argv
    config_json = argv[argv.index("--mcp-config") + 1]
    assert json.loads(config_json) == {"mcpServers": servers}
    assert argv[-1] == "do it"


def test_codex_runner_command_ignores_mcp_allowlist(tmp_path: Path) -> None:
    argv = build_runner_command(
        "codex",
        "do it",
        workspace_path=tmp_path,
        mcp_servers={"supabase": {}},
    )
    assert "--strict-mcp-config" not in argv
    assert "--mcp-config" not in argv


def test_local_review_fix_command_is_strict_mcp() -> None:
    argv = _build_fix_command(
        agent="claude", codex_model="gpt-5.1-codex", prompt="fix it"
    )
    assert "--strict-mcp-config" in argv
    assert "--mcp-config" not in argv
    assert argv[-1] == "fix it"


def test_local_review_fix_command_passes_binding_mcp_allowlist() -> None:
    servers = {"supabase": {"type": "http", "url": "https://mcp.example"}}
    argv = _build_fix_command(
        agent="claude",
        codex_model="gpt-5.1-codex",
        prompt="fix it",
        mcp_servers=servers,
    )
    assert "--strict-mcp-config" in argv
    config_json = argv[argv.index("--mcp-config") + 1]
    assert json.loads(config_json) == {"mcpServers": servers}
    assert argv[-1] == "fix it"


# --- 3. Per-binding env, resolved from .env ---------------------------------


def _binding(**overrides: object) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo"),
        **overrides,
    )


def test_binding_env_and_mcp_servers_default_empty() -> None:
    binding = _binding()
    assert binding.env == {}
    assert binding.mcp_servers == {}


def test_resolve_env_replaces_dotenv_key_names_with_values() -> None:
    binding = _binding(env={"SUPABASE_ACCESS_TOKEN": "MASHA2_SUPABASE_TOKEN"})
    binding.resolve_env({"MASHA2_SUPABASE_TOKEN": "sbp-secret"})
    assert binding.env == {"SUPABASE_ACCESS_TOKEN": "sbp-secret"}


def test_resolve_env_missing_key_fails_loudly() -> None:
    binding = _binding(env={"SUPABASE_ACCESS_TOKEN": "MISSING_KEY"})
    with pytest.raises(ValueError, match="MISSING_KEY"):
        binding.resolve_env({})


def _write_config_yaml(path: Path) -> None:
    path.write_text(
        """
repos:
  - linear_team_key: ENG
    github_repo: org/repo
    env:
      SUPABASE_ACCESS_TOKEN: MASHA2_SUPABASE_TOKEN
    linear_states:
      ready: Todo
""",
        encoding="utf-8",
    )


def test_config_load_resolves_binding_env_from_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MASHA2_SUPABASE_TOKEN", raising=False)
    (tmp_path / ".env").write_text("MASHA2_SUPABASE_TOKEN=sbp-from-dotenv\n")
    config_path = tmp_path / "config.yaml"
    _write_config_yaml(config_path)
    cfg = Config.load(config_path)
    assert cfg.repos[0].env == {"SUPABASE_ACCESS_TOKEN": "sbp-from-dotenv"}


def test_config_load_prefers_process_env_over_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("MASHA2_SUPABASE_TOKEN=sbp-from-dotenv\n")
    monkeypatch.setenv("MASHA2_SUPABASE_TOKEN", "sbp-from-process")
    config_path = tmp_path / "config.yaml"
    _write_config_yaml(config_path)
    cfg = Config.load(config_path)
    assert cfg.repos[0].env == {"SUPABASE_ACCESS_TOKEN": "sbp-from-process"}


def test_config_load_fails_on_unresolvable_env_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MASHA2_SUPABASE_TOKEN", raising=False)
    config_path = tmp_path / "config.yaml"
    _write_config_yaml(config_path)
    with pytest.raises(ValueError, match="MASHA2_SUPABASE_TOKEN"):
        Config.load(config_path)


# --- Spawn integration: env + MCP config reach the RunnerSpec ---------------


class _SpecCapturingRunner:
    def __init__(self) -> None:
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec):
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self):
        yield RunnerEvent(kind="started", pid=4242)
        yield RunnerEvent(kind="exit", returncode=0)

    async def kill(self, run_id: str) -> None:
        pass


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Apply schema migration",
        description="Add the breakdown table.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["symphony"],
    )


@pytest.mark.asyncio
async def test_implement_spawn_injects_binding_env_and_strict_mcp(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(
            env={"SUPABASE_ACCESS_TOKEN": "sbp-resolved"},
            mcp_servers={"supabase": {"type": "http", "url": "https://mcp.example"}},
        )
        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.issues_in_state = AsyncMock(return_value=[_issue()])
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.move_issue = AsyncMock()

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()

        gh = MagicMock()
        gh.ensure_pr = AsyncMock(return_value="https://github.com/org/repo/pull/42")
        gh.pr_comment = AsyncMock()
        gh.repo_clone = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="main")

        runner = _SpecCapturingRunner()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {  # noqa: SLF001
            "ENG": {
                "Todo": "state-todo",
                "In Progress": "state-progress",
                "Needs Approval": "state-na",
                "Blocked": "state-bl",
                "Done": "state-done",
            }
        }

        tasks = await orch._scan_binding(cfg.repos[0])  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)

        spec = runner.captured_spec
        assert spec is not None
        assert spec.env == {"SUPABASE_ACCESS_TOKEN": "sbp-resolved"}
        assert "--strict-mcp-config" in spec.command
        config_json = spec.command[spec.command.index("--mcp-config") + 1]
        assert json.loads(config_json) == {
            "mcpServers": {
                "supabase": {"type": "http", "url": "https://mcp.example"}
            }
        }
    finally:
        await conn.close()


class _FixerSpecCapturingRunner:
    """Drives one review→fix iteration without a subprocess.

    The reviewer pass emits a `CHANGES_REQUESTED` verdict (claude `result`
    event) so the loop dispatches a fix-run; the fixer's `RunnerSpec` —
    the masha2 schema-fix path — is then captured for env assertions.
    """

    def __init__(self) -> None:
        self.fixer_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec):
        if spec.stage == "local_review_fix":
            self.fixer_spec = spec
            return self._exit_only(pid=2)
        return self._reviewer_changes_requested()

    async def _reviewer_changes_requested(self):
        message = (
            "## Findings\n\n"
            "- file.py:1 — schema drift; regenerate types.\n\n"
            f"{VERDICT_CHANGES_REQUESTED_MARKER}"
        )
        yield RunnerEvent(kind="started", pid=1)
        yield RunnerEvent(
            kind="stdout",
            line=json.dumps({"type": "result", "result": message}),
        )
        yield RunnerEvent(kind="exit", returncode=0)

    async def _exit_only(self, *, pid: int):
        yield RunnerEvent(kind="started", pid=pid)
        yield RunnerEvent(kind="exit", returncode=0)

    async def kill(self, run_id: str) -> None:
        pass


@pytest.mark.asyncio
async def test_local_review_fixer_spawn_injects_binding_env(
    tmp_path: Path,
) -> None:
    runner = _FixerSpecCapturingRunner()

    async def head_sha(_path: Path) -> str:
        return "deadbeef"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path,
        base_branch="main",
        parent_run_id="run-1",
        issue_title="Apply schema migration",
        issue_body="Add the breakdown table.",
        labels=["symphony"],
        implementer_agent="claude",
        implementer_codex_model="",
        reviewer_agent="claude",
        reviewer_codex_model="",
        cap=1,
        stall_secs=10,
        binding_env={"SUPABASE_ACCESS_TOKEN": "sbp-resolved"},
        mcp_servers={"supabase": {"type": "http", "url": "https://mcp.example"}},
        last_message_dir=tmp_path / "msgs",
        head_sha_provider=head_sha,
    )

    # cap=1: reviewer requests changes → fixer runs once → loop exhausts.
    assert result.outcome is LoopOutcome.EXHAUSTED
    spec = runner.fixer_spec
    assert spec is not None
    assert spec.stage == "local_review_fix"
    # The headline assertion: the binding's resolved secret reaches the
    # fixer process env. Dropping `env=dict(binding_env or {})` fails here.
    assert spec.env == {"SUPABASE_ACCESS_TOKEN": "sbp-resolved"}
    # The binding's MCP allowlist must reach the fixer command end-to-end.
    # Dropping `mcp_servers=mcp_servers` on either forwarding hop fails here.
    assert "--strict-mcp-config" in spec.command
    config_json = spec.command[spec.command.index("--mcp-config") + 1]
    assert json.loads(config_json) == {
        "mcpServers": {"supabase": {"type": "http", "url": "https://mcp.example"}}
    }
