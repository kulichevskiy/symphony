"""End-to-end local-review session with a fake Runner.

This is the integration test for the loop wiring: scripted reviewer
and fixer subprocesses produce realistic JSONL streams, and the
session must dispatch the correct argv to the Runner each round.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from symphony.agent.codex_cli import (
    CODEX_APPROVAL_POLICY_CONFIG,
    CODEX_DEFAULT_PERMISSIONS_CONFIG,
    codex_project_root_write_config,
)
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.pipeline.local_review import (
    VERDICT_APPROVED_MARKER,
    VERDICT_CHANGES_REQUESTED_MARKER,
)
from symphony.pipeline.local_review_loop import LoopOutcome
from symphony.pipeline.local_review_session import run_local_review_session


class _ScriptedRunner:
    """Returns a different scripted event stream per call, in order."""

    def __init__(self, scripts: list[list[RunnerEvent]]) -> None:
        self._scripts = scripts
        self.specs: list[RunnerSpec] = []
        self.kill_calls: list[str] = []

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.specs.append(spec)
        idx = len(self.specs) - 1
        events = self._scripts[idx]

        async def gen() -> AsyncIterator[RunnerEvent]:
            for ev in events:
                yield ev

        return gen()

    async def kill(self, run_id: str) -> None:
        self.kill_calls.append(run_id)


def _codex_message_stream(text: str) -> list[RunnerEvent]:
    line = json.dumps(
        {
            "type": "item.completed",
            "item": {"id": "i", "type": "agent_message", "text": text},
        }
    )
    return [
        RunnerEvent(kind="stdout", line=line),
        RunnerEvent(kind="exit", returncode=0),
    ]


def _ok_fix_stream() -> list[RunnerEvent]:
    return [
        RunnerEvent(kind="stdout", line='{"type":"turn.completed"}'),
        RunnerEvent(kind="exit", returncode=0),
    ]


@pytest.mark.asyncio
async def test_first_review_approves_and_session_returns_approved(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(f"looks great\n{VERDICT_APPROVED_MARKER}"),
        ]
    )

    sha_counter = {"i": 0}

    async def head_sha(_: Path) -> str:
        sha_counter["i"] += 1
        return f"sha-{sha_counter['i']}"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-abc",
        issue_title="Add OAuth",
        issue_body="Users should sign in via Google.",
        labels=["feature"],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.APPROVED
    assert result.iterations == 1
    # Exactly one runner spawn (the reviewer), no fix-run.
    assert len(runner.specs) == 1
    spec = runner.specs[0]
    assert spec.stage == "local_review"
    assert spec.run_id == "run-abc-rev-0"
    assert spec.command[:2] == ["codex", "exec"]
    assert "--sandbox" in spec.command
    assert spec.command[spec.command.index("--sandbox") + 1] == "read-only"
    # `--base` is not a flag — base branch is threaded into the prompt
    # body (codex 0.130 forbids `--base` with `[PROMPT]`).
    assert "--base" not in spec.command
    prompt_arg = spec.command[-1]
    assert "origin/main" in prompt_arg


@pytest.mark.asyncio
async def test_fix_then_approve_dispatches_fix_run_in_correct_workspace(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- bug in foo.py:10\n{VERDICT_CHANGES_REQUESTED_MARKER}"
            ),
            _ok_fix_stream(),
            _codex_message_stream(f"fixed\n{VERDICT_APPROVED_MARKER}"),
        ]
    )

    head_calls: list[Path] = []

    async def head_sha(ws: Path) -> str:
        head_calls.append(ws)
        return f"sha-{len(head_calls)}"

    workspace = tmp_path / "workspace"
    result = await run_local_review_session(
        runner=runner,
        workspace_path=workspace,
        base_branch="develop",
        parent_run_id="run-xyz",
        issue_title="t",
        issue_body="b",
        labels=["x"],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.APPROVED
    assert result.iterations == 2
    assert len(runner.specs) == 3  # rev-0, fix-0, rev-1

    # Reviewer specs hit the workspace under the right stage.
    assert runner.specs[0].stage == "local_review"
    assert runner.specs[0].workspace_path == workspace
    assert runner.specs[1].stage == "local_review_fix"
    assert runner.specs[1].workspace_path == workspace
    assert runner.specs[2].stage == "local_review"

    # Fix-run argv should be the implementer's CLI, not the reviewer's
    # `exec review` form (those are different code paths and a mistake
    # here would silently send the fix to the wrong tool).
    fix_argv = runner.specs[1].command
    assert fix_argv[0] == "claude"
    assert "--print" in fix_argv
    # Findings text must be forwarded into the fix-run prompt.
    fix_prompt = fix_argv[-1]
    assert "bug in foo.py:10" in fix_prompt


@pytest.mark.asyncio
async def test_codex_fix_run_allows_git_writes(tmp_path: Path) -> None:
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- bug in foo.py:10\n{VERDICT_CHANGES_REQUESTED_MARKER}"
            ),
            _ok_fix_stream(),
            _codex_message_stream(f"fixed\n{VERDICT_APPROVED_MARKER}"),
        ]
    )
    workspace = tmp_path / "ws"
    workspace.mkdir()
    sha_counter = {"i": 0}

    async def head_sha(_: Path) -> str:
        sha_counter["i"] += 1
        return f"sha-{sha_counter['i']}"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=workspace,
        base_branch="origin/main",
        parent_run_id="run-abc",
        issue_title="Add auth",
        issue_body="Users should sign in via Google.",
        labels=["feature"],
        implementer_agent="codex",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.APPROVED
    fix_argv = runner.specs[1].command
    assert "--sandbox" not in fix_argv
    assert "workspace-write" not in fix_argv
    configs = [fix_argv[i + 1] for i, arg in enumerate(fix_argv) if arg == "--config"]
    assert configs == [
        CODEX_DEFAULT_PERMISSIONS_CONFIG,
        CODEX_APPROVAL_POLICY_CONFIG,
        codex_project_root_write_config(workspace),
    ]


@pytest.mark.asyncio
async def test_reviewer_spawn_failure_returns_reviewer_failed(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(
        scripts=[[RunnerEvent(kind="spawn_failed", error="codex not on PATH")]],
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-1",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.error is not None
    assert "spawn_failed" in result.error


@pytest.mark.asyncio
async def test_reviewer_stall_returns_reviewer_failed(tmp_path: Path) -> None:
    runner = _ScriptedRunner(
        scripts=[[RunnerEvent(kind="stall_timeout")]],
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-1",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.error == "reviewer stalled"


@pytest.mark.asyncio
async def test_fix_run_stall_returns_fix_run_failed(tmp_path: Path) -> None:
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- bug\n{VERDICT_CHANGES_REQUESTED_MARKER}"
            ),
            [RunnerEvent(kind="stall_timeout")],
        ],
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-1",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.FIX_RUN_FAILED
    assert result.error == "fix-run stalled"


@pytest.mark.asyncio
async def test_reviewer_prefers_last_message_file_over_stdout(
    tmp_path: Path,
) -> None:
    """When codex writes to `-o <file>`, that text is authoritative."""
    last_dir = tmp_path / "last"
    last_dir.mkdir()

    captured_paths: list[str] = []

    class _Runner:
        def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
            # Discover the -o file path from the command and pre-populate it
            # before the runner "exits" — that's what real codex does.
            assert "-o" in spec.command
            last_path = spec.command[spec.command.index("-o") + 1]
            captured_paths.append(last_path)
            Path(last_path).write_text(
                f"file-source\n{VERDICT_APPROVED_MARKER}", encoding="utf-8"
            )

            async def gen() -> AsyncIterator[RunnerEvent]:
                yield RunnerEvent(
                    kind="stdout", line='{"type":"turn.completed"}'
                )
                yield RunnerEvent(kind="exit", returncode=0)

            return gen()

        async def kill(self, run_id: str) -> None:
            pass

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=_Runner(),
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-1",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=last_dir,
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert captured_paths and captured_paths[0].startswith(str(last_dir))


@pytest.mark.asyncio
async def test_stale_last_message_does_not_smuggle_into_next_iteration(
    tmp_path: Path,
) -> None:
    """A leftover `review-0.last.txt` saying APPROVED must not be reused
    on iteration 1 — each iteration clears its own file first."""
    last_dir = tmp_path / "last"
    last_dir.mkdir()
    # Pre-seed the iteration-0 file with an APPROVED verdict.
    (last_dir / "review-0.last.txt").write_text(
        f"stale-approval\n{VERDICT_APPROVED_MARKER}", encoding="utf-8"
    )

    # First runner call writes a CHANGES_REQUESTED verdict to the file.
    class _Runner:
        def __init__(self) -> None:
            self.specs: list[RunnerSpec] = []

        def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
            self.specs.append(spec)
            if "-o" in spec.command:
                last_path = spec.command[spec.command.index("-o") + 1]
                Path(last_path).write_text(
                    f"## Findings\n- real-bug\n{VERDICT_CHANGES_REQUESTED_MARKER}",
                    encoding="utf-8",
                )

            async def gen() -> AsyncIterator[RunnerEvent]:
                yield RunnerEvent(kind="exit", returncode=0)

            return gen()

        async def kill(self, run_id: str) -> None:
            pass

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=_Runner(),
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-1",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=1,
        stall_secs=300,
        last_message_dir=last_dir,
        head_sha_provider=head_sha,
    )
    # If the stale file had leaked through we'd have gotten APPROVED.
    assert result.outcome == LoopOutcome.EXHAUSTED
    assert result.last_verdict is not None
    assert "real-bug" in result.last_verdict.findings


@pytest.mark.asyncio
async def test_report_active_run_id_brackets_each_subprocess(
    tmp_path: Path,
) -> None:
    """The reporter must be called with the run_id before the runner
    starts and with `None` after it returns — for both reviewer and
    fixer subprocesses. That's the contract the slash handler depends
    on to call `runner.kill(active)` mid-subprocess."""
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- bug\n{VERDICT_CHANGES_REQUESTED_MARKER}"
            ),
            _ok_fix_stream(),
            _codex_message_stream(f"ok\n{VERDICT_APPROVED_MARKER}"),
        ]
    )

    reports: list[str | None] = []

    async def report(run_id: str | None) -> None:
        reports.append(run_id)

    async def head_sha(_: Path) -> str:
        return "sha-1"

    await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-r1",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        report_active_run_id=report,
    )
    # rev-0 → None → fix-0 → None → rev-1 → None
    assert reports == [
        "run-r1-rev-0",
        None,
        "run-r1-fix-0",
        None,
        "run-r1-rev-1",
        None,
    ]


@pytest.mark.asyncio
async def test_report_active_clears_run_id_even_on_runner_error(
    tmp_path: Path,
) -> None:
    """If the runner blows up mid-collection, the reporter must still
    be called with `None` so the orchestrator doesn't keep pointing at
    a dead run_id."""

    class _ExplodingRunner:
        def __init__(self) -> None:
            self.calls = 0

        def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
            self.calls += 1

            async def gen() -> AsyncIterator[RunnerEvent]:
                if False:
                    yield RunnerEvent(kind="exit", returncode=0)
                raise RuntimeError("runner crashed")

            return gen()

        async def kill(self, run_id: str) -> None:
            pass

    runner = _ExplodingRunner()
    reports: list[str | None] = []

    async def report(run_id: str | None) -> None:
        reports.append(run_id)

    async def head_sha(_: Path) -> str:
        return "sha-1"

    with pytest.raises(RuntimeError):
        await run_local_review_session(
            runner=runner,
            workspace_path=tmp_path / "ws",
            base_branch="main",
            parent_run_id="run-r1",
            issue_title="t",
            issue_body="b",
            labels=[],
            implementer_agent="claude",
            implementer_codex_model="gpt-5.1-codex",
            reviewer_agent="codex",
            reviewer_codex_model="gpt-5.1-codex",
            cap=5,
            stall_secs=300,
            last_message_dir=tmp_path / "last",
            head_sha_provider=head_sha,
            report_active_run_id=report,
        )
    # The session entered the reviewer call and reported the run_id,
    # then the finally block cleared it.
    assert reports == ["run-r1-rev-0", None]


@pytest.mark.asyncio
async def test_safe_run_id_strips_unfriendly_chars(tmp_path: Path) -> None:
    runner = _ScriptedRunner(
        scripts=[_codex_message_stream(f"ok\n{VERDICT_APPROVED_MARKER}")],
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="weird id/with chars",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )
    # The derived run_id stays log-filename safe.
    assert runner.specs[0].run_id == "weird-id-with-chars-rev-0"
