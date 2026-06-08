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
)
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.pipeline.local_review import (
    VERDICT_APPROVED_MARKER,
    VERDICT_CHANGES_REQUESTED_MARKER,
    DiffSize,
)
from symphony.pipeline.local_review_loop import LoopOutcome
from symphony.pipeline.local_review_session import run_local_review_session


def _message_stream(agent: str, text: str) -> list[RunnerEvent]:
    """A single-message reviewer stream in the given agent's JSONL form."""
    if agent == "codex":
        line = json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "i", "type": "agent_message", "text": text},
            }
        )
    else:
        line = json.dumps({"type": "result", "result": text})
    return [
        RunnerEvent(kind="stdout", line=line),
        RunnerEvent(kind="exit", returncode=0),
    ]


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


def _review_stream_with_transcript(
    *, agent: str, message: str, prefix: str, stderr: str
) -> tuple[list[RunnerEvent], str, str]:
    if agent == "codex":
        final = json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "i", "type": "agent_message", "text": message},
            }
        )
    else:
        final = json.dumps({"type": "result", "result": message})
    stdout = f"{prefix}\n{final}"
    return (
        [
            RunnerEvent(kind="stdout", line=prefix),
            RunnerEvent(kind="stderr", line=stderr),
            RunnerEvent(kind="stdout", line=final),
            RunnerEvent(kind="exit", returncode=0),
        ],
        stdout,
        stderr,
    )


def _fix_stream_with_transcript() -> tuple[list[RunnerEvent], str, str]:
    stdout = "fixer-started\nfixer-done"
    stderr = "fixer-warning"
    return (
        [
            RunnerEvent(kind="stdout", line="fixer-started"),
            RunnerEvent(kind="stderr", line=stderr),
            RunnerEvent(kind="stdout", line="fixer-done"),
            RunnerEvent(kind="exit", returncode=0),
        ],
        stdout,
        stderr,
    )


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


@pytest.mark.parametrize("reviewer_agent", ["claude", "codex"])
@pytest.mark.parametrize("implementer_agent", ["claude", "codex"])
@pytest.mark.asyncio
async def test_persists_transcripts_for_review_and_fix_iterations(
    tmp_path: Path, reviewer_agent: str, implementer_agent: str
) -> None:
    review_0, review_0_out, review_0_err = _review_stream_with_transcript(
        agent=reviewer_agent,
        message=f"## Findings\n- bug\n{VERDICT_CHANGES_REQUESTED_MARKER}",
        prefix="reviewer-zero-started",
        stderr="reviewer-zero-warning",
    )
    fix_0, fix_0_out, fix_0_err = _fix_stream_with_transcript()
    review_1, review_1_out, review_1_err = _review_stream_with_transcript(
        agent=reviewer_agent,
        message=f"looks good\n{VERDICT_APPROVED_MARKER}",
        prefix="reviewer-one-started",
        stderr="reviewer-one-warning",
    )
    runner = _ScriptedRunner(scripts=[review_0, fix_0, review_1])
    log_dir = tmp_path / "logs" / "local_review" / "run-transcript"

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "workspace",
        base_branch="main",
        parent_run_id="run-transcript",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent=implementer_agent,
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent=reviewer_agent,
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=log_dir,
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.APPROVED
    assert (
        (log_dir / "review-0.out.log").read_text(encoding="utf-8")
        == review_0_out
    )
    assert (
        (log_dir / "review-0.err.log").read_text(encoding="utf-8")
        == review_0_err
    )
    assert (log_dir / "fix-0.out.log").read_text(encoding="utf-8") == fix_0_out
    assert (log_dir / "fix-0.err.log").read_text(encoding="utf-8") == fix_0_err
    assert (
        (log_dir / "review-1.out.log").read_text(encoding="utf-8")
        == review_1_out
    )
    assert (
        (log_dir / "review-1.err.log").read_text(encoding="utf-8")
        == review_1_err
    )


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
    ]


@pytest.mark.asyncio
async def test_reviewer_spawn_failure_returns_reviewer_failed(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(
        scripts=[
            [RunnerEvent(kind="spawn_failed", error="codex not on PATH")],
            [RunnerEvent(kind="spawn_failed", error="codex not on PATH")],
        ],
    )
    log_dir = tmp_path / "last"

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
        last_message_dir=log_dir,
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.error is not None
    assert "spawn_failed" in result.error
    assert (log_dir / "review-0.out.log").read_text(encoding="utf-8") == ""
    assert (log_dir / "review-0.err.log").read_text(encoding="utf-8") == ""
    assert [spec.run_id for spec in runner.specs] == ["run-1-rev-0", "run-1-rev-0"]


@pytest.mark.asyncio
async def test_reviewer_stall_returns_reviewer_failed(tmp_path: Path) -> None:
    runner = _ScriptedRunner(
        scripts=[
            [RunnerEvent(kind="stall_timeout")],
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
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.error == "reviewer stalled"
    assert [spec.run_id for spec in runner.specs] == ["run-1-rev-0", "run-1-rev-0"]


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

    log_dir = tmp_path / "last"
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
        last_message_dir=log_dir,
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.FIX_RUN_FAILED
    assert result.error == "fix-run stalled"
    assert (log_dir / "fix-0.out.log").read_text(encoding="utf-8") == ""
    assert (log_dir / "fix-0.err.log").read_text(encoding="utf-8") == ""


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


@pytest.mark.parametrize(
    "implementer_agent,reviewer_agent",
    [("claude", "codex"), ("codex", "claude")],
)
@pytest.mark.asyncio
async def test_large_diff_runs_two_passes_with_per_pass_families(
    tmp_path: Path, implementer_agent: str, reviewer_agent: str
) -> None:
    """A large diff spawns pass-1 finder (reviewer/opposite family, no
    marker) then pass-2 verifier (implementer family, emits marker)."""
    finder_text = "## Findings\n- suspicion at foo.py:1"
    verifier_text = f"tried to break it, held\n{VERDICT_APPROVED_MARKER}"
    runner = _ScriptedRunner(
        scripts=[
            _message_stream(reviewer_agent, finder_text),
            _message_stream(implementer_agent, verifier_text),
        ]
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    async def diff_size(_: Path) -> DiffSize:
        return DiffSize(changed_lines=500, changed_files=10)

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-2pass",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent=implementer_agent,
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent=reviewer_agent,
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    # Exactly two reviewer subprocesses; pass 2 approved so no fixer.
    assert result.outcome == LoopOutcome.APPROVED
    assert len(runner.specs) == 2
    finder_spec, verifier_spec = runner.specs
    assert finder_spec.stage == "local_review"
    assert verifier_spec.stage == "local_review"
    assert finder_spec.run_id == "run-2pass-rev-0-find"
    assert verifier_spec.run_id == "run-2pass-rev-0-verify"
    # Family per pass: finder = reviewer (opposite implementer), verifier
    # = implementer family.
    assert finder_spec.command[0] == reviewer_agent
    assert verifier_spec.command[0] == implementer_agent
    # Pass-1 findings are injected into the verifier's prompt.
    assert "suspicion at foo.py:1" in verifier_spec.command[-1]


@pytest.mark.asyncio
async def test_two_pass_merged_verdict_is_pass_twos(tmp_path: Path) -> None:
    """The loop receives pass-2's merged findings, not pass-1's raw
    suspicions. Pass 2 requests changes, so the loop dispatches a fixer
    with pass-2's findings as the trigger."""
    finder_text = "## Findings\n- suspicion at foo.py:1"
    verifier_text = (
        f"## Findings\n- confirmed bug at foo.py:1\n"
        f"{VERDICT_CHANGES_REQUESTED_MARKER}"
    )
    runner = _ScriptedRunner(
        scripts=[
            _message_stream("codex", finder_text),  # pass 1 (reviewer)
            _message_stream("claude", verifier_text),  # pass 2 (implementer)
            _ok_fix_stream(),  # fixer dispatched on CHANGES_REQUESTED
        ]
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    async def diff_size(_: Path) -> DiffSize:
        return DiffSize(changed_lines=500, changed_files=10)

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-merge",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=1,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.EXHAUSTED  # cap=1, CHANGES_REQUESTED
    # First two specs are the reviewer passes; third is the fixer.
    assert runner.specs[0].run_id == "run-merge-rev-0-find"
    assert runner.specs[1].run_id == "run-merge-rev-0-verify"
    assert runner.specs[2].stage == "local_review_fix"
    # Merged verdict is pass-2's, not pass-1's.
    assert result.last_verdict is not None
    assert "confirmed bug at foo.py:1" in result.last_verdict.findings
    assert "suspicion at foo.py:1" not in result.last_verdict.findings
    # The fixer trigger is pass-2's findings.
    assert "confirmed bug at foo.py:1" in runner.specs[2].command[-1]


@pytest.mark.parametrize(
    "lines,files,expected_specs",
    [
        (150, 3, 1),  # both at the inclusive boundary → single pass
        (150, 1, 1),
        (10, 3, 1),
        (151, 3, 2),  # one line over → two passes
        (150, 4, 2),  # one file over → two passes
        (151, 4, 2),
        (1000, 9, 2),
    ],
)
@pytest.mark.asyncio
async def test_small_diff_collapses_to_single_pass(
    tmp_path: Path, lines: int, files: int, expected_specs: int
) -> None:
    if expected_specs == 1:
        scripts = [_message_stream("codex", f"ok\n{VERDICT_APPROVED_MARKER}")]
    else:
        scripts = [
            _message_stream("codex", "## Findings\n- s at a.py:1"),
            _message_stream("claude", f"ok\n{VERDICT_APPROVED_MARKER}"),
        ]
    runner = _ScriptedRunner(scripts=scripts)

    async def head_sha(_: Path) -> str:
        return "sha-1"

    async def diff_size(_: Path) -> DiffSize:
        return DiffSize(changed_lines=lines, changed_files=files)

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-thr",
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
        diff_size_provider=diff_size,
    )
    assert result.outcome == LoopOutcome.APPROVED
    assert len(runner.specs) == expected_specs
    if expected_specs == 1:
        # Single pass uses the reviewer family directly.
        assert runner.specs[0].command[0] == "codex"
        assert runner.specs[0].run_id == "run-thr-rev-0"


@pytest.mark.asyncio
async def test_no_diff_size_provider_defaults_to_single_pass(
    tmp_path: Path,
) -> None:
    """Without a measurement callback the session can't size the diff, so
    it stays single-pass (back-compat / cheaper default)."""
    runner = _ScriptedRunner(
        scripts=[_message_stream("codex", f"ok\n{VERDICT_APPROVED_MARKER}")]
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-none",
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
    assert result.outcome == LoopOutcome.APPROVED
    assert len(runner.specs) == 1


@pytest.mark.parametrize(
    "implementer_agent",
    ["claude", "codex"],
)
@pytest.mark.asyncio
async def test_pass_two_verifier_gets_tier_b_command(
    tmp_path: Path, implementer_agent: str
) -> None:
    """The pass-2 verifier (implementer family) runs with Tier B exec/write
    grants; pass-1 finder stays read-only."""
    reviewer_agent = "codex" if implementer_agent == "claude" else "claude"
    finder_text = "## Findings\n- suspicion at foo.py:1"
    verifier_text = f"held\n{VERDICT_APPROVED_MARKER}"
    runner = _ScriptedRunner(
        scripts=[
            _message_stream(reviewer_agent, finder_text),
            _message_stream(implementer_agent, verifier_text),
        ]
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    async def diff_size(_: Path) -> DiffSize:
        return DiffSize(changed_lines=500, changed_files=10)

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-tierb",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent=implementer_agent,
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent=reviewer_agent,
        reviewer_codex_model="gpt-5.1-codex",
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )
    assert result.outcome == LoopOutcome.APPROVED
    finder_argv = runner.specs[0].command
    verifier_argv = runner.specs[1].command

    # Pass 1 (finder, reviewer family) is read-only.
    if reviewer_agent == "codex":
        assert finder_argv[finder_argv.index("--sandbox") + 1] == "read-only"
    else:
        assert "Write" not in finder_argv[finder_argv.index("--tools") + 1]

    # Pass 2 (verifier, implementer family) gets Tier B grants.
    if implementer_agent == "codex":
        assert verifier_argv[verifier_argv.index("--sandbox") + 1] == "workspace-write"
    else:
        assert "Write" in verifier_argv[verifier_argv.index("--tools") + 1]
        assert "uv run pytest" in verifier_argv[verifier_argv.index("--allowedTools") + 1]


@pytest.mark.asyncio
async def test_workspace_scrubbed_after_pass_two_before_fixer(
    tmp_path: Path,
) -> None:
    """A file the verifier writes during pass 2 must be scrubbed before the
    fixer runs, so throwaway tests never reach the diff the fixer sees."""
    workspace = tmp_path / "ws"
    workspace.mkdir()
    throwaway = workspace / "throwaway_test.py"
    events: list[tuple[str, object]] = []

    finder_line = json.dumps(
        {
            "type": "item.completed",
            "item": {"id": "i", "type": "agent_message", "text": "## Findings\n- s"},
        }
    )
    verifier_text = (
        f"## Findings\n- confirmed bug at foo.py:1 (test failed)\n"
        f"{VERDICT_CHANGES_REQUESTED_MARKER}"
    )
    verifier_line = json.dumps({"type": "result", "result": verifier_text})

    class _Runner:
        def __init__(self) -> None:
            self.specs: list[RunnerSpec] = []

        def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
            self.specs.append(spec)
            stage, run_id = spec.stage, spec.run_id

            async def gen() -> AsyncIterator[RunnerEvent]:
                if stage == "local_review" and "verify" in run_id:
                    throwaway.write_text("def test_x():\n    assert False\n")
                    events.append(("verify_wrote", throwaway.exists()))
                    yield RunnerEvent(kind="stdout", line=verifier_line)
                elif stage == "local_review" and "find" in run_id:
                    yield RunnerEvent(kind="stdout", line=finder_line)
                elif stage == "local_review_fix":
                    events.append(("fix_saw_throwaway", throwaway.exists()))
                    yield RunnerEvent(
                        kind="stdout", line='{"type":"turn.completed"}'
                    )
                yield RunnerEvent(kind="exit", returncode=0)

            return gen()

        async def kill(self, run_id: str) -> None:
            pass

    async def head_sha(_: Path) -> str:
        return "sha-1"

    async def diff_size(_: Path) -> DiffSize:
        return DiffSize(changed_lines=500, changed_files=10)

    async def scrubber(ws: Path) -> None:
        events.append(("scrub", ws))
        if throwaway.exists():
            throwaway.unlink()

    result = await run_local_review_session(
        runner=_Runner(),
        workspace_path=workspace,
        base_branch="main",
        parent_run_id="run-scrub",
        issue_title="t",
        issue_body="b",
        labels=[],
        implementer_agent="claude",
        implementer_codex_model="gpt-5.1-codex",
        reviewer_agent="codex",
        reviewer_codex_model="gpt-5.1-codex",
        cap=1,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
        workspace_scrubber=scrubber,
    )

    assert result.outcome == LoopOutcome.EXHAUSTED  # cap=1, CHANGES_REQUESTED
    kinds = [e[0] for e in events]
    assert ("verify_wrote", True) in events
    assert "scrub" in kinds
    # The fixer must have observed a clean tree.
    assert ("fix_saw_throwaway", False) in events
    # Scrub strictly precedes the fixer.
    assert kinds.index("scrub") < kinds.index("fix_saw_throwaway")
    # Pass-2 evidence flows into the fixer trigger verbatim.
    assert result.last_verdict is not None
    assert "test failed" in result.last_verdict.findings


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
