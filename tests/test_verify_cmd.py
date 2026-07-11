"""Per-binding `verify_cmd`: build/test gate in the workspace before push.

A binding may declare `verify_cmd` (e.g. `pnpm build && pnpm test`). After
the last code-mutating stage (local review when enabled, implement
otherwise) and before push, the command runs in the workspace. Red gets
one implementer fix turn seeded with the failing output tail, then a
re-run; still red fails closed: no push, no PR, operator wait with the
failure tail on Linear. No `verify_cmd` → behavior unchanged.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.pipeline.local_review import (
    VERDICT_APPROVED_MARKER,
    VERDICT_CHANGES_REQUESTED_MARKER,
)
from symphony.pipeline.verify import (
    VerifyResult,
    run_verify_command,
    run_verify_session,
)
from symphony.ui.live import parse_stream_events

from ._workspace_helpers import advance_head


class _StagedRunner:
    """Returns scripted events keyed by `RunnerSpec.stage`."""

    def __init__(self, scripts: dict[str, list[list[RunnerEvent]]]) -> None:
        self._scripts = {k: list(v) for k, v in scripts.items()}
        self.captured: list[RunnerSpec] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured.append(spec)
        # Simulate the agent committing its work so the completion gate sees
        # HEAD advance over the branch base and treats the implement run as
        # completed (rc=0 alone no longer suffices).
        if spec.stage == "implement":
            advance_head(spec.workspace_path)
        bucket = self._scripts.get(spec.stage)
        if not bucket:
            raise AssertionError(f"unexpected stage {spec.stage!r}; remaining={self._scripts}")
        events = bucket.pop(0)

        async def gen() -> AsyncIterator[RunnerEvent]:
            for ev in events:
                yield ev

        return gen()

    async def kill(self, run_id: str) -> None:
        pass


# --- config ---------------------------------------------------------------


def test_verify_cmd_defaults_to_none() -> None:
    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo"),
    )
    assert binding.verify_cmd is None


def test_verify_cmd_and_timeout_parse() -> None:
    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        verify_cmd="pnpm build && pnpm test",
        verify_timeout_secs=600,
        linear_states=LinearStates(ready="Todo"),
    )
    assert binding.verify_cmd == "pnpm build && pnpm test"
    assert binding.resolved_verify_timeout_secs(1800) == 600


def test_verify_timeout_falls_back_to_global_default() -> None:
    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        verify_cmd="make check",
        linear_states=LinearStates(ready="Todo"),
    )
    assert binding.resolved_verify_timeout_secs(1800) == 1800


# --- run_verify_command (real subprocess) ----------------------------------


@pytest.mark.asyncio
async def test_run_verify_command_green(tmp_path: Path) -> None:
    ok, output = await run_verify_command(tmp_path, "echo all-good", 30)
    assert ok
    assert "all-good" in output


@pytest.mark.asyncio
async def test_run_verify_command_red_captures_output(tmp_path: Path) -> None:
    ok, output = await run_verify_command(
        tmp_path, "echo boom-stdout; echo boom-stderr >&2; exit 3", 30
    )
    assert not ok
    assert "boom-stdout" in output
    assert "boom-stderr" in output


@pytest.mark.asyncio
async def test_run_verify_command_times_out(tmp_path: Path) -> None:
    ok, output = await run_verify_command(tmp_path, "sleep 30", 1)
    assert not ok
    assert "timed out" in output


# --- run_verify_session ----------------------------------------------------


def _session_kwargs(runner: _StagedRunner, command_runner) -> dict:  # type: ignore[no-untyped-def]
    return {
        "runner": runner,
        "workspace_path": Path("/tmp/ws"),
        "verify_cmd": "pnpm build && pnpm test",
        "timeout_secs": 60,
        "parent_run_id": "run-1",
        "issue_title": "Add auth",
        "issue_body": "OAuth login.",
        "labels": ["feature"],
        "implementer_agent": "claude",
        "implementer_codex_model": "gpt-5.3-codex",
        "stall_secs": 60,
        "command_runner": command_runner,
    }


@pytest.mark.asyncio
async def test_verify_session_green_first_run_skips_fix_turn() -> None:
    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return True, "all green"

    runner = _StagedRunner({})
    result = await run_verify_session(**_session_kwargs(runner, command_runner))
    assert result.ok
    assert not result.fix_attempted
    assert runner.captured == []


@pytest.mark.asyncio
async def test_verify_session_green_first_run_still_writes_fix_log(tmp_path: Path) -> None:
    """A green verify_cmd on the first attempt must still populate
    `fix_log_path`, so the UI never opens a `verify` run to a blank log.

    The note must also be stream-parseable (the endpoint drops plain text)
    and end with a trailing newline (the endpoint buffers an unterminated
    final line forever)."""

    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return True, "all green"

    runner = _StagedRunner({})
    fix_log_path = tmp_path / "verify.log"
    result = await run_verify_session(
        **_session_kwargs(runner, command_runner), fix_log_path=fix_log_path
    )
    assert result.ok
    assert fix_log_path.exists()
    text = fix_log_path.read_text(encoding="utf-8")
    assert text.endswith("\n")
    events = parse_stream_events(text.strip())
    assert events == [
        {"kind": "message", "text": "verify_cmd passed on first attempt; no fix turn was run."}
    ]


@pytest.mark.asyncio
async def test_verify_session_disabled_fix_still_writes_parseable_log(tmp_path: Path) -> None:
    """`allow_fixes=False` (publish-resume gate) failing must still populate
    `fix_log_path` with a stream-parseable note — `hasTailableLog` in the UI
    treats every `verify` run as tailable, so a silently-missing log here
    would open to a permanent "Waiting for output…" spinner."""

    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return False, "red output"

    runner = _StagedRunner({})
    fix_log_path = tmp_path / "verify.log"
    result = await run_verify_session(
        **_session_kwargs(runner, command_runner),
        fix_log_path=fix_log_path,
        allow_fixes=False,
    )
    assert not result.ok
    assert not result.fix_attempted
    assert runner.captured == []
    text = fix_log_path.read_text(encoding="utf-8")
    assert text.endswith("\n")
    events = parse_stream_events(text.strip())
    assert events == [
        {"kind": "message", "text": "verify_cmd failed; fix turn disabled for publish resume."}
    ]


@pytest.mark.asyncio
async def test_verify_session_red_then_fix_then_green() -> None:
    outcomes = [(False, "FAIL src/a.test.ts\nbuild error TS2345"), (True, "ok")]

    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return outcomes.pop(0)

    runner = _StagedRunner(
        {
            "verify_fix": [
                [
                    RunnerEvent(kind="started", pid=1),
                    RunnerEvent(kind="exit", returncode=0),
                ]
            ]
        }
    )
    result = await run_verify_session(**_session_kwargs(runner, command_runner))
    assert result.ok
    assert result.fix_attempted
    assert [s.stage for s in runner.captured] == ["verify_fix"]
    # The fix turn must see the tail of the failing output.
    prompt = runner.captured[0].command[-1]
    assert "build error TS2345" in prompt
    assert "pnpm build && pnpm test" in prompt


@pytest.mark.asyncio
async def test_verify_session_fix_turn_log_ends_with_newline(tmp_path: Path) -> None:
    """`collect_runner_output` joins stdout lines without a trailing
    newline; the stream endpoint buffers an unterminated final line
    forever, so `fix_log_path` must always end with one."""
    outcomes = [(False, "build error TS2345"), (True, "ok")]

    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return outcomes.pop(0)

    runner = _StagedRunner(
        {
            "verify_fix": [
                [
                    RunnerEvent(kind="started", pid=1),
                    RunnerEvent(kind="stdout", line="fixed the build"),
                    RunnerEvent(kind="exit", returncode=0),
                ]
            ]
        }
    )
    fix_log_path = tmp_path / "verify.log"
    result = await run_verify_session(
        **_session_kwargs(runner, command_runner), fix_log_path=fix_log_path
    )
    assert result.ok
    assert fix_log_path.read_text(encoding="utf-8") == "fixed the build\n"


@pytest.mark.asyncio
async def test_verify_session_fix_turn_carries_fix_role_model() -> None:
    """Path 2 (verify-gate fix turn): `fix_claude_model` threads `--model`
    into the fix-turn argv; unset → no `--model`."""
    outcomes = [(False, "build error TS2345"), (True, "ok")]

    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return outcomes.pop(0)

    runner = _StagedRunner(
        {
            "verify_fix": [
                [
                    RunnerEvent(kind="started", pid=1),
                    RunnerEvent(kind="exit", returncode=0),
                ]
            ]
        }
    )
    result = await run_verify_session(
        **_session_kwargs(runner, command_runner),
        fix_claude_model="claude-opus-4-6",
    )
    assert result.ok
    argv = runner.captured[0].command
    assert argv[argv.index("--model") + 1] == "claude-opus-4-6"


@pytest.mark.asyncio
async def test_verify_session_fix_turn_omits_model_when_unset() -> None:
    """Default `None` → no `--model` (CLI default; no behavior change)."""
    outcomes = [(False, "build error TS2345"), (True, "ok")]

    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return outcomes.pop(0)

    runner = _StagedRunner(
        {
            "verify_fix": [
                [
                    RunnerEvent(kind="started", pid=1),
                    RunnerEvent(kind="exit", returncode=0),
                ]
            ]
        }
    )
    result = await run_verify_session(**_session_kwargs(runner, command_runner))
    assert result.ok
    assert "--model" not in runner.captured[0].command


@pytest.mark.asyncio
async def test_verify_session_still_red_after_fix_fails_closed() -> None:
    outcomes = [(False, "first failure"), (False, "still red: TS2345 tail")]

    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return outcomes.pop(0)

    runner = _StagedRunner(
        {
            "verify_fix": [
                [
                    RunnerEvent(kind="started", pid=1),
                    RunnerEvent(kind="exit", returncode=0),
                ]
            ]
        }
    )
    result = await run_verify_session(**_session_kwargs(runner, command_runner))
    assert not result.ok
    assert result.fix_attempted
    assert "still red: TS2345 tail" in result.tail
    assert result.error


@pytest.mark.asyncio
async def test_verify_session_fix_run_spawn_failure_fails_closed() -> None:
    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return False, "red output"

    runner = _StagedRunner({"verify_fix": [[RunnerEvent(kind="spawn_failed", error="no claude")]]})
    result = await run_verify_session(**_session_kwargs(runner, command_runner))
    assert not result.ok
    assert result.fix_attempted
    assert "red output" in result.tail


@pytest.mark.asyncio
async def test_verify_session_fix_run_spawn_failure_writes_parseable_log(tmp_path: Path) -> None:
    """A fix turn that dies before producing any stdout (missing agent
    binary, or a CLI/auth error emitted only on stderr) must still leave a
    stream-parseable note in `fix_log_path` — an empty file yields no
    events and the LiveFeed is stuck on "Waiting for output…" forever."""

    async def command_runner(path: Path, cmd: str, timeout_secs: int) -> tuple[bool, str]:
        return False, "red output"

    runner = _StagedRunner({"verify_fix": [[RunnerEvent(kind="spawn_failed", error="no claude")]]})
    fix_log_path = tmp_path / "verify.log"
    result = await run_verify_session(
        **_session_kwargs(runner, command_runner), fix_log_path=fix_log_path
    )
    assert not result.ok
    text = fix_log_path.read_text(encoding="utf-8")
    assert text.endswith("\n")
    events = parse_stream_events(text.strip())
    assert len(events) == 1
    assert events[0]["kind"] == "message"
    assert "no output" in events[0]["text"]
    assert "no claude" in events[0]["text"]


# --- orchestrator e2e ------------------------------------------------------


def _verify_binding(verify_cmd: str) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        verify_cmd=verify_cmd,
        linear_states=LinearStates(ready="Todo", code_review="In Review"),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add authentication",
        description="Need OAuth login for the dashboard.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["feature"],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "In Review": "state-review",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


def _implement_script() -> list[RunnerEvent]:
    return [
        RunnerEvent(kind="started", pid=4242),
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "result",
                    "subtype": "success",
                    "total_cost_usd": 0.01,
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                }
            ),
        ),
        RunnerEvent(kind="exit", returncode=0),
    ]


def _init_git_workspace(path: Path) -> str:
    """Make *path* a git repo with one clean commit; return its HEAD SHA.

    Lets `_workspace_head_sha` resolve a real SHA so the verify-pass mark is
    recorded against it, and keeps the tree clean so the pre-push dirty-tree
    gate doesn't divert into a fix turn.
    """
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    (path / "f.txt").write_text("x")
    for cmd in (
        ["git", "init", "-q"],
        ["git", "add", "f.txt"],
        ["git", "commit", "-q", "-m", "init"],
    ):
        subprocess.run(cmd, cwd=path, check=True, env={**os.environ, **env}, capture_output=True)
    out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=path, check=True, capture_output=True)
    return out.stdout.decode().strip()


def _workspace_head(path: Path) -> str:
    """Return *path*'s current HEAD SHA (sync, to stay off the async loop)."""
    return (
        subprocess.run(["git", "rev-parse", "HEAD"], cwd=path, check=True, capture_output=True)
        .stdout.decode()
        .strip()
    )


async def _scan_and_wait(orch: Orchestrator, binding: RepoBinding) -> None:
    tasks = await orch._scan_binding(binding)  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)


def _orch_fixtures(tmp_path: Path, binding: RepoBinding):  # type: ignore[no-untyped-def]
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
    gh.repo_default_branch = AsyncMock(return_value="trunk")
    push_fn = AsyncMock()
    return cfg, linear, workspace_path, workspace, gh, push_fn


@pytest.mark.asyncio
async def test_failing_verify_cmd_blocks_push_and_pr(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _verify_binding("echo BUILD_BROKEN_MARKER; exit 1")
        cfg, linear, workspace_path, workspace, gh, push_fn = _orch_fixtures(tmp_path, binding)
        runner = _StagedRunner(
            {
                "implement": [_implement_script()],
                "verify_fix": [
                    [
                        RunnerEvent(kind="started", pid=1),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        # One fix turn was attempted, seeded with the failing tail.
        assert [s.stage for s in runner.captured] == [
            "implement",
            "verify_fix",
        ]
        assert "BUILD_BROKEN_MARKER" in runner.captured[1].command[-1]

        # Fail-closed: no push, no PR, issue blocked, operator wait.
        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-bl" in move_targets

        history = await db.runs.history_for_issue(conn, "iss-1")
        implement_rows = [h for h in history if h.stage == "implement"]
        assert len(implement_rows) == 1
        assert implement_rows[0].status == "failed"
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is not None
        assert wait.kind == db.operator_waits.KIND_IMPLEMENT_FAILED

        # Failure tail reaches Linear.
        posted = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("BUILD_BROKEN_MARKER" in body for body in posted)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_passing_verify_cmd_push_proceeds(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _verify_binding("true")
        cfg, linear, workspace_path, workspace, gh, push_fn = _orch_fixtures(tmp_path, binding)
        # A real git workspace with a clean base commit. The implement run
        # advances HEAD (the completion gate requires it), so the green-gate
        # recording (poll.py:_dispatch_one) keys the verify-pass mark on the
        # *post-implement* head — the same SHA that gets pushed — not this base.
        _init_git_workspace(workspace_path)
        # No `verify_fix` bucket: a green verify must not dispatch a fix turn.
        runner = _StagedRunner({"implement": [_implement_script()]})
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.ensure_pr.assert_awaited_once()
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is None
        # The green gate must have recorded a verify-pass mark for the exact
        # pushed head (the post-implement HEAD); a regression in that recording
        # would fail here while the merge-side tests (which inject the mark)
        # stay green.
        pushed_head = _workspace_head(workspace_path)
        assert await db.issue_prs.has_verify_passed(
            conn,
            issue_id="iss-1",
            github_repo="org/repo",
            head_sha=pushed_head,
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_verify_session_crash_fails_closed(tmp_path: Path) -> None:
    """An exception inside the verify phase must block the push, not skip
    the gate: the gate exists to stop unbuildable code from shipping."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _verify_binding("true")
        cfg, linear, workspace_path, workspace, gh, push_fn = _orch_fixtures(tmp_path, binding)
        runner = _StagedRunner({"implement": [_implement_script()]})
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        async def _exploding_session(**_: object) -> VerifyResult:
            raise RuntimeError("verify machinery blew up")

        # SYM-150: the verify phase moved to `poll._lifecycle`, which is where
        # `run_verify_session` is now looked up.
        from symphony.orchestrator.poll import _lifecycle as lifecycle_mod

        original = lifecycle_mod.run_verify_session
        lifecycle_mod.run_verify_session = _exploding_session  # type: ignore[assignment]
        try:
            await _scan_and_wait(orch, binding)
        finally:
            lifecycle_mod.run_verify_session = original  # type: ignore[assignment]

        push_fn.assert_not_awaited()
        gh.ensure_pr.assert_not_awaited()
        move_targets = [c.args[1] for c in linear.move_issue.await_args_list]
        assert "state-bl" in move_targets
    finally:
        await conn.close()


# --- verify_cmd + local_review together ------------------------------------


def _codex_agent_message(text: str) -> RunnerEvent:
    return RunnerEvent(
        kind="stdout",
        line=json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "i", "type": "agent_message", "text": text},
            }
        ),
    )


def _local_and_verify_binding(verify_cmd: str) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent="claude",
        branch_prefix="symphony",
        local_review=True,
        remote_review=False,
        reviewer_agent="codex",
        verify_cmd=verify_cmd,
        linear_states=LinearStates(
            ready="Todo",
            local_code_review="Local Code Review",
            code_review="Needs Approval",
        ),
    )


def _local_states() -> dict[str, str]:
    states = _states()
    states["Local Code Review"] = "state-local-review"
    return states


@pytest.mark.asyncio
async def test_verify_runs_after_local_review_fix_loop(tmp_path: Path) -> None:
    """Acceptance: verify runs *after* the last code-mutating stage.

    With `local_review` enabled, the gate must fire after the local-review
    fix loop's last mutation — so what's verified is what gets pushed. The
    local reviewer requests one change (→ `local_review_fix`) then approves;
    `verify_cmd` is red on the first run (→ one `verify_fix` turn) and green
    on the re-run, so push proceeds. The captured stage order proves the
    verify gate ran strictly after the local-review fixes.
    """
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        # Red on the first invocation, green once `.sym_verified` exists.
        # The mock fix turn does not really mutate the tree; the sentinel
        # toggle stands in for "a fix landed" so the re-run goes green.
        verify_cmd = (
            "if [ -f .sym_verified ]; then exit 0; "
            "else touch .sym_verified; echo VERIFY_RED_THEN_GREEN; exit 1; fi"
        )
        binding = _local_and_verify_binding(verify_cmd)
        cfg, linear, workspace_path, workspace, gh, push_fn = _orch_fixtures(tmp_path, binding)
        # The implement turn's commit (completion gate) gitignores the verify
        # sentinel so it never trips the pre-push dirty-tree gate.
        (workspace_path / ".gitignore").write_text(".sym_verified\n")

        def _result_line() -> RunnerEvent:
            return RunnerEvent(
                kind="stdout",
                line=json.dumps(
                    {
                        "type": "result",
                        "subtype": "success",
                        "total_cost_usd": 0.05,
                        "usage": {"input_tokens": 10, "output_tokens": 5},
                    }
                ),
            )

        runner = _StagedRunner(
            {
                "implement": [_implement_script()],
                "local_review": [
                    [
                        _codex_agent_message(
                            f"## Findings\n- needs a tweak\n{VERDICT_CHANGES_REQUESTED_MARKER}"
                        ),
                        RunnerEvent(kind="exit", returncode=0),
                    ],
                    [
                        _codex_agent_message(f"ok\n{VERDICT_APPROVED_MARKER}"),
                        RunnerEvent(kind="exit", returncode=0),
                    ],
                ],
                "local_review_fix": [
                    [
                        RunnerEvent(kind="started", pid=2),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
                "verify_fix": [
                    [
                        RunnerEvent(kind="started", pid=3),
                        _result_line(),
                        RunnerEvent(kind="exit", returncode=0),
                    ]
                ],
            }
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _local_states()}  # noqa: SLF001

        await _scan_and_wait(orch, binding)

        stages = [s.stage for s in runner.captured]
        assert stages == [
            "implement",
            "local_review",
            "local_review_fix",
            "local_review",
            "verify_fix",
        ]
        # The verify gate's fix turn runs strictly after the local-review
        # fix loop's last mutation.
        last_mutation = max(
            i for i, s in enumerate(stages) if s in ("local_review", "local_review_fix")
        )
        assert stages.index("verify_fix") > last_mutation

        # Green on the re-run → push proceeds, PR opened.
        push_fn.assert_awaited_once_with(workspace_path, "symphony/eng-1")
        gh.ensure_pr.assert_awaited_once()
        wait = await db.operator_waits.get(conn, "iss-1")
        assert wait is None

        # The verify fix turn's spend is billed to a `stage="verify"` row.
        history = await db.runs.history_for_issue(conn, "iss-1")
        verify_rows = [h for h in history if h.stage == "verify"]
        assert len(verify_rows) == 1
        assert verify_rows[0].status == "completed"
        assert verify_rows[0].cost_usd > 0
    finally:
        await conn.close()
