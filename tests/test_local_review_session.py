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

from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.bench.metrics import local_review_metrics
from symphony.config import ResolvedRole
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


def _turn_failed_stream(message: str) -> list[RunnerEvent]:
    """A reviewer stream that exits 0 but emits only a `turn.failed` (e.g. an
    API 4xx) and no agent_message / verdict."""
    return [
        RunnerEvent(kind="stdout", line=json.dumps({"type": "turn.started"})),
        RunnerEvent(
            kind="stdout",
            line=json.dumps({"type": "turn.failed", "error": {"message": message}}),
        ),
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
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
    # codex's nested OS sandbox is bypassed (container is the boundary).
    assert "--dangerously-bypass-approvals-and-sandbox" in spec.command
    assert "--sandbox" not in spec.command
    # `--base` is not a flag — base branch is threaded into the prompt
    # body (codex 0.130 forbids `--base` with `[PROMPT]`).
    assert "--base" not in spec.command
    prompt_arg = spec.command[-1]
    assert "origin/main" in prompt_arg


@pytest.mark.asyncio
async def test_allow_fixes_false_fails_without_local_review_fix_turn(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- [Major] bug in foo.py:10\n{VERDICT_CHANGES_REQUESTED_MARKER}"
            ),
        ]
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-no-fix",
        issue_title="Add OAuth",
        issue_body="Users should sign in via Google.",
        labels=["feature"],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        allow_fixes=False,
    )

    assert result.outcome == LoopOutcome.FIX_RUN_FAILED
    assert "fix turn disabled for publish resume" in (result.error or "")
    assert [spec.stage for spec in runner.specs] == ["local_review"]


@pytest.mark.parametrize("reviewer_agent", ["claude", "codex"])
@pytest.mark.parametrize("implementer_agent", ["claude", "codex"])
@pytest.mark.asyncio
async def test_persists_transcripts_for_review_and_fix_iterations(
    tmp_path: Path, reviewer_agent: str, implementer_agent: str
) -> None:
    review_0, review_0_out, review_0_err = _review_stream_with_transcript(
        agent=reviewer_agent,
        message=f"## Findings\n- [Major] bug\n{VERDICT_CHANGES_REQUESTED_MARKER}",
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
        reviewer_role=ResolvedRole(
            agent=reviewer_agent, model="gpt-5.1-codex" if reviewer_agent == "codex" else None
        ),
        verifier_role=ResolvedRole(
            agent=implementer_agent, model="gpt-5.1-codex" if implementer_agent == "codex" else None
        ),
        fixer_role=ResolvedRole(
            agent=implementer_agent, model="gpt-5.1-codex" if implementer_agent == "codex" else None
        ),
        cap=5,
        stall_secs=300,
        last_message_dir=log_dir,
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.APPROVED
    assert (log_dir / "review-0.out.log").read_text(encoding="utf-8") == review_0_out
    assert (log_dir / "review-0.err.log").read_text(encoding="utf-8") == review_0_err
    assert (log_dir / "fix-0.out.log").read_text(encoding="utf-8") == fix_0_out
    assert (log_dir / "fix-0.err.log").read_text(encoding="utf-8") == fix_0_err
    assert (log_dir / "review-1.out.log").read_text(encoding="utf-8") == review_1_out
    assert (log_dir / "review-1.err.log").read_text(encoding="utf-8") == review_1_err
    if reviewer_agent == "claude":
        assert (log_dir / "review-0.last.txt").read_text(encoding="utf-8") == (
            f"## Findings\n- [Major] bug\n{VERDICT_CHANGES_REQUESTED_MARKER}"
        )
        assert (log_dir / "review-1.last.txt").read_text(encoding="utf-8") == (
            f"looks good\n{VERDICT_APPROVED_MARKER}"
        )
        metrics = local_review_metrics(tmp_path / "logs")
        assert metrics["local_review_rounds"] == 2
        assert metrics["local_review_findings"] == 1
        assert metrics["local_review_major"] == 1


@pytest.mark.asyncio
async def test_fix_then_approve_dispatches_fix_run_in_correct_workspace(
    tmp_path: Path,
) -> None:
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- [Major] bug in foo.py:10\n{VERDICT_CHANGES_REQUESTED_MARKER}"
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
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
async def test_local_review_claude_model_injected_into_reviewer_and_fixer(
    tmp_path: Path,
) -> None:
    """`local_review_claude_model` threads `--model` into the claude
    reviewer argv; the `fix` role's `fix_claude_model` drives the fixer argv
    (path 1). The two are decoupled so the reviewer and fixer can run on
    different models."""
    runner = _ScriptedRunner(
        scripts=[
            _message_stream(
                "claude",
                f"## Findings\n- [Major] bug in foo.py:10\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            ),
            _ok_fix_stream(),
            _message_stream("claude", f"fixed\n{VERDICT_APPROVED_MARKER}"),
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
        issue_title="t",
        issue_body="b",
        labels=["x"],
        reviewer_role=ResolvedRole(agent="claude", model="claude-sonnet-4-6"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude", model="claude-opus-4-6"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.APPROVED
    assert len(runner.specs) == 3  # rev-0, fix-0, rev-1
    for spec in runner.specs:
        argv = spec.command
        assert argv[0] == "claude"
        expected = "claude-opus-4-6" if spec.stage == "local_review_fix" else "claude-sonnet-4-6"
        assert argv[argv.index("--model") + 1] == expected


@pytest.mark.asyncio
async def test_local_review_claude_model_unset_omits_model_flag(
    tmp_path: Path,
) -> None:
    """Default `None` → no `--model`; argv identical to today."""
    runner = _ScriptedRunner(
        scripts=[
            _message_stream(
                "claude",
                f"## Findings\n- [Major] bug\n{VERDICT_CHANGES_REQUESTED_MARKER}",
            ),
            _ok_fix_stream(),
            _message_stream("claude", f"fixed\n{VERDICT_APPROVED_MARKER}"),
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
        issue_title="t",
        issue_body="b",
        labels=["x"],
        reviewer_role=ResolvedRole(agent="claude"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.APPROVED
    for spec in runner.specs:
        assert "--model" not in spec.command


@pytest.mark.asyncio
async def test_codex_fix_run_allows_git_writes(tmp_path: Path) -> None:
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- [Major] bug in foo.py:10\n{VERDICT_CHANGES_REQUESTED_MARKER}"
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        fixer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.APPROVED
    fix_argv = runner.specs[1].command
    assert "--dangerously-bypass-approvals-and-sandbox" in fix_argv
    assert "--sandbox" not in fix_argv
    assert "workspace-write" not in fix_argv
    # Permissions/approval --config knobs are gone (superseded by the bypass).
    assert not any("default_permissions" in a or "approval_policy" in a for a in fix_argv)


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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.error == "reviewer stalled"
    assert [spec.run_id for spec in runner.specs] == ["run-1-rev-0", "run-1-rev-0"]


def _claude_api_error_stream(status: int) -> list[RunnerEvent]:
    """A claude reviewer stream that exits 0 carrying only a transient provider
    API error (synthetic assistant + `is_error`/`api_error_status` result) and
    no verdict marker."""
    text = f'API Error: {status} {{"type":"error"}}'
    return [
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "assistant",
                    "message": {
                        "model": "<synthetic>",
                        "content": [{"type": "text", "text": text}],
                    },
                }
            ),
        ),
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "result",
                    "subtype": "error_during_execution",
                    "is_error": True,
                    "result": text,
                    "api_error_status": status,
                }
            ),
        ),
        RunnerEvent(kind="exit", returncode=0),
    ]


def _claude_json_field_auth_error_stream() -> list[RunnerEvent]:
    """A claude reviewer stream that exits 0 carrying claude's canonical auth
    shape: a terminal `result` event with `is_error: true` and no
    `api_error_status` — neither `classify_stream_api_error` (no status/no
    `API Error:` text) nor `classify_plaintext_auth_error` (the line is JSON,
    which that classifier skips) can see it; only the JSON-field tier can."""
    return [
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "result",
                    "is_error": True,
                    "result": "Invalid API key · Please run /login",
                }
            ),
        ),
        RunnerEvent(kind="exit", returncode=0),
    ]


@pytest.mark.asyncio
async def test_reviewer_json_field_auth_error_surfaces_as_reviewer_failed(
    tmp_path: Path,
) -> None:
    """Regression: claude's canonical auth-failure shape — a terminal
    `result` event with `is_error: true` and no `api_error_status` — must
    be caught by the third (JSON-field) classification tier in
    `_run_reviewer_pass`, not left unclassified so the row stays
    `connected`."""
    runner = _ScriptedRunner(
        scripts=[
            _claude_json_field_auth_error_stream(),
            _claude_json_field_auth_error_stream(),
        ],
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-json-field-401",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="claude"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.api_error is not None and result.api_error.status == 401
    assert result.api_error_agent == "claude"


@pytest.mark.asyncio
async def test_claude_transient_api_error_surfaces_as_reviewer_failed(
    tmp_path: Path,
) -> None:
    """A claude reviewer that exits 0 with only an `API Error: 500` (no verdict)
    surfaces the real message — not the generic "no verdict marker"."""
    runner = _ScriptedRunner(
        scripts=[_claude_api_error_stream(500), _claude_api_error_stream(500)],
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
        reviewer_role=ResolvedRole(agent="claude"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.error is not None
    assert result.error.startswith("API Error: 500")


@pytest.mark.asyncio
async def test_fix_run_stall_returns_fix_run_failed(tmp_path: Path) -> None:
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- [Major] bug\n{VERDICT_CHANGES_REQUESTED_MARKER}"
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=log_dir,
        head_sha_provider=head_sha,
    )
    assert result.outcome == LoopOutcome.FIX_RUN_FAILED
    assert result.error == "fix-run stalled"
    assert (log_dir / "fix-0.out.log").read_text(encoding="utf-8") == ""
    assert (log_dir / "fix-0.err.log").read_text(encoding="utf-8") == ""


def _claude_result_stream(text: str) -> list[RunnerEvent]:
    """A claude fix-run stream ending in a `result` event (final message)."""
    return [
        RunnerEvent(kind="stdout", line=json.dumps({"type": "result", "result": text})),
        RunnerEvent(kind="exit", returncode=0),
    ]


@pytest.mark.asyncio
async def test_blocked_fix_run_halts_session_as_blocked(tmp_path: Path) -> None:
    """A fix-run that exits 0 but politely asks for human authorization
    (no marker, HEAD did not advance) is classified blocked via the SYM-101
    completion gate. The session halts as FIX_RUN_BLOCKED — no further review
    pass — so the orchestrator parks it for the operator instead of pushing."""
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- [Major] bug in foo.py:10\n{VERDICT_CHANGES_REQUESTED_MARKER}"
            ),
            _claude_result_stream(
                "I need you to authorize the Supabase OAuth URL before I can "
                "regenerate the types. I cannot proceed without your approval."
            ),
        ]
    )

    # HEAD never advances: the fixer made no commit because it stalled on a
    # human action. Same SHA before and after the fix-run.
    async def head_sha(_: Path) -> str:
        return "sha-stuck"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-blocked",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.FIX_RUN_BLOCKED
    assert result.iterations == 1
    # Only the first reviewer + the fixer ran; no second review pass.
    assert len(runner.specs) == 2
    assert runner.specs[1].stage == "local_review_fix"
    # The human-action ask is surfaced verbatim for the operator.
    assert result.error is not None
    assert "authorize the Supabase OAuth URL" in result.error


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
            Path(last_path).write_text(f"file-source\n{VERDICT_APPROVED_MARKER}", encoding="utf-8")

            async def gen() -> AsyncIterator[RunnerEvent]:
                yield RunnerEvent(kind="stdout", line='{"type":"turn.completed"}')
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
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
                    f"## Findings\n- [Major] real-bug\n{VERDICT_CHANGES_REQUESTED_MARKER}",
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=1,
        stall_secs=300,
        last_message_dir=last_dir,
        head_sha_provider=head_sha,
    )
    # If the stale file had leaked through we'd have gotten APPROVED.
    assert result.outcome == LoopOutcome.STUCK_LOOP
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
    finder_text = "## Findings\n- [Major] suspicion at foo.py:1"
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
        reviewer_role=ResolvedRole(
            agent=reviewer_agent, model="gpt-5.1-codex" if reviewer_agent == "codex" else None
        ),
        verifier_role=ResolvedRole(
            agent=implementer_agent, model="gpt-5.1-codex" if implementer_agent == "codex" else None
        ),
        fixer_role=ResolvedRole(
            agent=implementer_agent, model="gpt-5.1-codex" if implementer_agent == "codex" else None
        ),
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
async def test_two_pass_retry_reuses_finder_and_preserves_incomplete_verifier_notes(
    tmp_path: Path,
) -> None:
    """A no-verdict verifier retry must not redo pass 1 or lose discoveries.

    Production BENCH-59 confirmed a SQLite overflow, then ended without the
    required verdict marker. The retry rebuilt its prompt from a new finder
    run and silently dropped that confirmed defect.
    """

    class _RetryRunner:
        def __init__(self) -> None:
            self.specs: list[RunnerSpec] = []
            self.find_calls = 0
            self.verify_calls = 0

        def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
            self.specs.append(spec)

            async def gen() -> AsyncIterator[RunnerEvent]:
                if spec.run_id.endswith("-find"):
                    self.find_calls += 1
                    events = _codex_message_stream(
                        "## Findings\n- [Major] recursive decoder is slow"
                    )
                else:
                    self.verify_calls += 1
                    if self.verify_calls == 1:
                        events = [
                            *_codex_message_stream(
                                "Confirmed SQLite OverflowError on oversized event id"
                            )[:-1],
                            *_codex_message_stream("I ran out of time before emitting a verdict"),
                        ]
                    else:
                        events = _codex_message_stream(
                            f"rechecked prior notes\n{VERDICT_APPROVED_MARKER}"
                        )
                for event in events:
                    yield event

            return gen()

        async def kill(self, run_id: str) -> None:
            pass

    async def head_sha(_: Path) -> str:
        return "sha-1"

    async def diff_size(_: Path) -> DiffSize:
        return DiffSize(changed_lines=500, changed_files=10)

    runner = _RetryRunner()
    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-retry-notes",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        fixer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        cap=1,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.APPROVED
    assert runner.find_calls == 1
    assert runner.verify_calls == 2
    retry_prompt = [spec.command[-1] for spec in runner.specs if spec.run_id.endswith("-verify")][1]
    assert "Confirmed SQLite OverflowError" in retry_prompt
    assert (tmp_path / "last/review-0-verify.out.log").exists()
    assert (tmp_path / "last/review-0-verify-attempt-2.out.log").exists()


@pytest.mark.asyncio
async def test_two_pass_retry_reruns_an_error_only_finder(tmp_path: Path) -> None:
    api_error = "The server encountered a temporary problem. Please retry."
    finder_text = "## Findings\n- [Major] suspicion at foo.py:1"
    runner = _ScriptedRunner(
        scripts=[
            _turn_failed_stream(api_error),
            _message_stream("codex", finder_text),
            _message_stream("codex", f"held\n{VERDICT_APPROVED_MARKER}"),
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
        parent_run_id="run-finder-retry",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        fixer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        cap=1,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.APPROVED
    assert [spec.run_id for spec in runner.specs].count("run-finder-retry-rev-0-find") == 2
    assert (tmp_path / "last/review-0-find.out.log").exists()
    assert (tmp_path / "last/review-0-find-attempt-2.out.log").exists()


@pytest.mark.asyncio
async def test_two_pass_finder_and_verifier_reflect_per_role_model_effort(
    tmp_path: Path,
) -> None:
    """review_find and review_verify each drive their own pass's argv — model
    and reasoning effort per role (SYM-192)."""
    finder_text = "## Findings\n- [Major] suspicion at foo.py:1"
    verifier_text = f"held\n{VERDICT_APPROVED_MARKER}"
    runner = _ScriptedRunner(
        scripts=[
            _message_stream("codex", finder_text),
            _message_stream("claude", verifier_text),
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex", effort="low"),
        verifier_role=ResolvedRole(agent="claude", model="opus", effort="high"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.APPROVED
    finder_spec, verifier_spec = runner.specs
    # Finder (review_find): codex model + reasoning effort.
    assert finder_spec.command[0] == "codex"
    assert finder_spec.command[finder_spec.command.index("--model") + 1] == "gpt-5.1-codex"
    assert 'model_reasoning_effort="low"' in finder_spec.command
    # Verifier (review_verify): claude model + effort.
    assert verifier_spec.command[0] == "claude"
    assert verifier_spec.command[verifier_spec.command.index("--model") + 1] == "opus"
    assert verifier_spec.command[verifier_spec.command.index("--effort") + 1] == "high"


@pytest.mark.asyncio
async def test_two_pass_finder_stream_error_surfaces_without_verifier(
    tmp_path: Path,
) -> None:
    """A pass-1 finder that exits 0 with only a `turn.failed` (API error)
    produces no findings; the session must surface REVIEWER_FAILED with the
    real error and never run the verifier (which could APPROVE empty findings
    and mask the failure)."""
    api_error = (
        "The 'gpt-5.1-codex' model is not supported when using Codex with a ChatGPT account."
    )
    # Both attempts (the loop retries the reviewer once) fail in pass 1; the
    # verifier-approve script must never be reached.
    runner = _ScriptedRunner(
        scripts=[
            _turn_failed_stream(api_error),
            _turn_failed_stream(api_error),
            _message_stream("claude", f"approved\n{VERDICT_APPROVED_MARKER}"),
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
        parent_run_id="run-2pass-err",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="claude"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.error == api_error
    # Only finder passes ran; the verifier was never spawned.
    assert all(spec.run_id.endswith("-find") for spec in runner.specs)


@pytest.mark.asyncio
async def test_two_pass_finder_with_findings_and_stray_error_still_verifies(
    tmp_path: Path,
) -> None:
    """A finder that produced usable findings is not dropped just because the
    stream also carried an error event — only an error-only (empty-findings)
    finder fails. The verifier still runs."""
    finder_text = "## Findings\n- [Major] suspicion at foo.py:1"
    # A stray error event early in the stream, then the finder's real findings
    # as the final agent message — extract_last_agent_message still returns them.
    finder_stream = [
        RunnerEvent(
            kind="stdout",
            line=json.dumps({"type": "error", "message": "transient blip"}),
        ),
        RunnerEvent(
            kind="stdout",
            line=json.dumps({"type": "result", "result": finder_text}),
        ),
        RunnerEvent(kind="exit", returncode=0),
    ]
    runner = _ScriptedRunner(
        scripts=[
            finder_stream,
            _message_stream("claude", f"held\n{VERDICT_APPROVED_MARKER}"),
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
        parent_run_id="run-2pass-findings",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="claude"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.APPROVED
    assert len(runner.specs) == 2
    # The findings reached the verifier prompt despite the stray error event.
    assert "suspicion at foo.py:1" in runner.specs[1].command[-1]


@pytest.mark.asyncio
async def test_two_pass_finder_401_survives_merge_when_verifier_stalls(
    tmp_path: Path,
) -> None:
    """A codex finder that hits a typed 401 but still produces usable findings
    proceeds to the verifier (per the guard above); when that claude verifier
    then stalls (its own `api_error` stays None), the merged `ReviewerOutput`
    must still carry the *finder's* `api_error`/`api_error_agent` so the loop
    surfaces + expires codex — not silently drop it because the verifier
    doesn't carry one of its own. Regression for the merge's `replace()`
    folding only cost counters and losing `finder_out.api_error`."""
    finder_stream = [
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {"type": "turn.failed", "error": {"message": "unauthorized", "status": 401}}
            ),
        ),
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "i",
                        "type": "agent_message",
                        "text": "## Findings\n- [Major] x",
                    },
                }
            ),
        ),
        RunnerEvent(kind="exit", returncode=0),
    ]
    verifier_stall = [RunnerEvent(kind="stall_timeout")]
    runner = _ScriptedRunner(scripts=[finder_stream, verifier_stall, verifier_stall])

    async def head_sha(_: Path) -> str:
        return "sha-1"

    async def diff_size(_: Path) -> DiffSize:
        return DiffSize(changed_lines=500, changed_files=10)

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-2pass-merge-401",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.api_error is not None and result.api_error.status == 401
    # Attributed to the finder's provider (codex), not the stalled verifier's
    # (claude).
    assert result.api_error_agent == "codex"


@pytest.mark.asyncio
async def test_finder_uses_sonnet_verifier_stays_on_opus(
    tmp_path: Path,
) -> None:
    """`local_review_claude_model` routes ONLY the pass-1 finder; the
    pass-2 verifier keeps the CLI default (Opus) unless its own override
    is set. Finder argv carries `--model <sonnet>`; verifier argv has no
    `--model`."""
    finder_text = "## Findings\n- [Major] suspicion at foo.py:1"
    verifier_text = f"tried to break it, held\n{VERDICT_APPROVED_MARKER}"
    runner = _ScriptedRunner(
        scripts=[
            _message_stream("claude", finder_text),
            _message_stream("claude", verifier_text),
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
        parent_run_id="run-split",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="claude", model="claude-sonnet-4-6"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.APPROVED
    finder_spec, verifier_spec = runner.specs
    finder_argv = finder_spec.command
    assert finder_argv[finder_argv.index("--model") + 1] == "claude-sonnet-4-6"
    assert "--model" not in verifier_spec.command


@pytest.mark.asyncio
async def test_verifier_claude_model_override_runs_verifier_on_it(
    tmp_path: Path,
) -> None:
    """The verifier override is independently selectable: when set, the
    pass-2 verifier argv carries it while the finder keeps its own model."""
    finder_text = "## Findings\n- [Major] suspicion at foo.py:1"
    verifier_text = f"held\n{VERDICT_APPROVED_MARKER}"
    runner = _ScriptedRunner(
        scripts=[
            _message_stream("claude", finder_text),
            _message_stream("claude", verifier_text),
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
        parent_run_id="run-vovr",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="claude", model="claude-sonnet-4-6"),
        verifier_role=ResolvedRole(agent="claude", model="claude-opus-4-8"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.APPROVED
    finder_spec, verifier_spec = runner.specs
    finder_argv = finder_spec.command
    verifier_argv = verifier_spec.command
    assert finder_argv[finder_argv.index("--model") + 1] == "claude-sonnet-4-6"
    assert verifier_argv[verifier_argv.index("--model") + 1] == "claude-opus-4-8"


@pytest.mark.asyncio
async def test_two_pass_merged_verdict_is_pass_twos(tmp_path: Path) -> None:
    """The loop receives pass-2's merged findings, not pass-1's raw
    suspicions. Pass 2 requests changes, so the loop dispatches a fixer
    with pass-2's findings as the trigger."""
    finder_text = "## Findings\n- [Major] suspicion at foo.py:1"
    verifier_text = (
        f"## Findings\n- [Major] confirmed bug at foo.py:1\n{VERDICT_CHANGES_REQUESTED_MARKER}"
    )
    runner = _ScriptedRunner(
        scripts=[
            _message_stream("codex", finder_text),  # pass 1 (reviewer)
            _message_stream("claude", verifier_text),  # pass 2 (implementer)
            _ok_fix_stream(),  # fixer dispatched on CHANGES_REQUESTED
            _message_stream("codex", finder_text),
            _message_stream("claude", f"fixed and rechecked\n{VERDICT_APPROVED_MARKER}"),
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=1,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.APPROVED
    # First two specs are the reviewer passes; third is the fixer. The final
    # two specs are the mandatory closure review after the last permitted fix.
    assert runner.specs[0].run_id == "run-merge-rev-0-find"
    assert runner.specs[1].run_id == "run-merge-rev-0-verify"
    assert runner.specs[2].stage == "local_review_fix"
    # Merged verdict is pass-2's, not pass-1's.
    assert "confirmed bug at foo.py:1" in result.verdicts[0].findings
    assert "suspicion at foo.py:1" not in result.verdicts[0].findings
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
            _message_stream("codex", "## Findings\n- [Major] s at a.py:1"),
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
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
    runner = _ScriptedRunner(scripts=[_message_stream("codex", f"ok\n{VERDICT_APPROVED_MARKER}")])

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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
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
    finder_text = "## Findings\n- [Major] suspicion at foo.py:1"
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
        reviewer_role=ResolvedRole(
            agent=reviewer_agent, model="gpt-5.1-codex" if reviewer_agent == "codex" else None
        ),
        verifier_role=ResolvedRole(
            agent=implementer_agent, model="gpt-5.1-codex" if implementer_agent == "codex" else None
        ),
        fixer_role=ResolvedRole(
            agent=implementer_agent, model="gpt-5.1-codex" if implementer_agent == "codex" else None
        ),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )
    assert result.outcome == LoopOutcome.APPROVED
    finder_argv = runner.specs[0].command
    verifier_argv = runner.specs[1].command

    # Pass 1 (finder, reviewer family) is read-only. codex's sandbox is bypassed
    # (container is the boundary), so read-only intent is prompt-carried, not a
    # `--sandbox` flag.
    if reviewer_agent == "codex":
        assert "--dangerously-bypass-approvals-and-sandbox" in finder_argv
        assert "--sandbox" not in finder_argv
    else:
        assert "Write" not in finder_argv[finder_argv.index("--tools") + 1]

    # Pass 2 (verifier, implementer family) gets Tier B grants. For codex the
    # write/execute intent is prompt-carried; the argv carries no `--sandbox`.
    if implementer_agent == "codex":
        assert "--dangerously-bypass-approvals-and-sandbox" in verifier_argv
        assert "--sandbox" not in verifier_argv
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
            "item": {"id": "i", "type": "agent_message", "text": "## Findings\n- [Major] s"},
        }
    )
    verifier_text = (
        f"## Findings\n- [Major] confirmed bug at foo.py:1 (test failed)\n"
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
                    yield RunnerEvent(kind="stdout", line='{"type":"turn.completed"}')
                yield RunnerEvent(kind="exit", returncode=0)

            return gen()

        async def kill(self, run_id: str) -> None:
            pass

    async def head_sha(_: Path) -> str:
        return "sha-1"

    async def diff_size(_: Path) -> DiffSize:
        return DiffSize(changed_lines=500, changed_files=10)

    async def scrubber(ws: Path, target_sha: str) -> None:
        events.append(("scrub", target_sha))
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=1,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
        workspace_scrubber=scrubber,
    )

    assert result.outcome == LoopOutcome.STUCK_LOOP
    kinds = [e[0] for e in events]
    assert ("verify_wrote", True) in events
    assert "scrub" in kinds
    # The scrub resets to the pre-review HEAD (discarding reviewer commits too).
    assert ("scrub", "sha-1") in events
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
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )
    # The derived run_id stays log-filename safe.
    assert runner.specs[0].run_id == "weird-id-with-chars-rev-0"


def test_reviewer_stream_error_unwraps_codex_turn_failed() -> None:
    """A codex `turn.failed` wraps the real cause one JSON level deep; the
    extractor surfaces the status-prefixed message so a no-verdict run reports it."""
    from symphony.pipeline.local_review import classify_stream_api_error

    inner = json.dumps(
        {
            "type": "error",
            "status": 400,
            "error": {
                "type": "invalid_request_error",
                "message": (
                    "The 'gpt-5.1-codex' model is not supported when using "
                    "Codex with a ChatGPT account."
                ),
            },
        }
    )
    stdout = "\n".join(
        [
            json.dumps({"type": "thread.started", "thread_id": "t"}),
            json.dumps({"type": "turn.started"}),
            json.dumps({"type": "error", "message": inner}),
            json.dumps({"type": "turn.failed", "error": {"message": inner}}),
        ]
    )
    err = classify_stream_api_error(stdout)
    assert err is not None
    assert err.message == (
        "API Error: 400 The 'gpt-5.1-codex' model is not supported when using "
        "Codex with a ChatGPT account."
    )


def test_reviewer_stream_error_none_on_clean_stream() -> None:
    """A normal stream with no error/turn.failed event yields None."""
    from symphony.pipeline.local_review import classify_stream_api_error

    stdout = "\n".join(
        [
            json.dumps({"type": "thread.started", "thread_id": "t"}),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"id": "i", "type": "agent_message", "text": "ok"},
                }
            ),
            json.dumps({"type": "turn.completed"}),
        ]
    )
    assert classify_stream_api_error(stdout) is None


@pytest.mark.asyncio
async def test_wall_clock_secs_wired_to_specs_and_distinguishes_error(
    tmp_path: Path,
) -> None:
    """wall_clock_secs must be threaded into every RunnerSpec (reviewer and
    fixer), and a wall_clock_timeout terminal event must produce
    "fix-run exceeded wall-clock cap" — not the stall message."""
    changes_text = f"## Findings\n- [Major] bug found\n{VERDICT_CHANGES_REQUESTED_MARKER}"
    runner = _ScriptedRunner(
        scripts=[
            _message_stream("codex", changes_text),
            [RunnerEvent(kind="wall_clock_timeout")],
        ]
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-wc",
        issue_title="Test",
        issue_body="Test",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        wall_clock_secs=2,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.FIX_RUN_FAILED
    assert result.error == "fix-run exceeded wall-clock cap"
    reviewer_spec, fixer_spec = runner.specs
    assert reviewer_spec.wall_clock_secs == 2
    assert fixer_spec.wall_clock_secs == 2


@pytest.mark.asyncio
async def test_two_pass_verifier_401_tags_verifier_agent(tmp_path: Path) -> None:
    """A two-pass review whose verifier runs a different provider than the
    finder and returns a 401 must attribute the failure to the *verifier's*
    agent — a codex finder must not expire a claude verifier's provider (and
    vice versa). Regression for `api_error_agent` only ever being the finder."""
    finder_text = "## Findings\n- [Major] suspicion at foo.py:1"
    # Finder (codex) succeeds with findings; verifier (claude) exits 0 with only
    # a 401 and no verdict. The loop retries the whole two-pass once.
    runner = _ScriptedRunner(
        scripts=[
            _message_stream("codex", finder_text),
            _claude_api_error_stream(401),
            _message_stream("codex", finder_text),
            _claude_api_error_stream(401),
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
        parent_run_id="run-verify-401",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.api_error is not None and result.api_error.status == 401
    # Attributed to the verifier's provider, not the finder's (codex).
    assert result.api_error_agent == "claude"


@pytest.mark.asyncio
async def test_fixer_deterministic_401_surfaces_as_fix_run_failed(tmp_path: Path) -> None:
    """A fixer that exits 0 emitting a deterministic 401 (unauthorized) and
    makes no commit must surface FIX_RUN_FAILED carrying the fixer's api_error
    tagged with its agent — not a silent ok=True that leaves the row connected.
    Only transient statuses were preserved before; a 401 was dropped."""
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- [Major] bug\n{VERDICT_CHANGES_REQUESTED_MARKER}"
            ),
            _claude_api_error_stream(401),
        ]
    )

    async def head_sha(_: Path) -> str:
        # HEAD never advances: the fixer made no commit before the 401.
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-fix-401",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.FIX_RUN_FAILED
    assert result.api_error is not None and result.api_error.status == 401
    assert result.api_error_agent == "claude"


@pytest.mark.asyncio
async def test_single_pass_reviewer_plaintext_auth_failure_tags_only_reviewer_agent(
    tmp_path: Path,
) -> None:
    """A single-pass (small-diff) claude reviewer that dies on a pre-stream,
    plaintext "Not logged in" line (no JSONL, so `classify_stream_api_error`
    sees nothing) must attribute the failure to claude only — never to the
    verifier's agent (codex), which never ran this iteration. Regression for
    the combined-log scrape misattributing a claude pre-stream auth failure to
    an uninvolved codex verifier."""
    plaintext_auth_failure = [
        RunnerEvent(kind="stderr", line="Not logged in. Please run /login."),
        RunnerEvent(kind="exit", returncode=1),
    ]
    runner = _ScriptedRunner(scripts=[plaintext_auth_failure, plaintext_auth_failure])

    async def head_sha(_: Path) -> str:
        return "sha-1"

    async def diff_size(_: Path) -> DiffSize:
        return DiffSize(changed_lines=1, changed_files=1)

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-plaintext-401",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="claude"),
        verifier_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=1,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
        diff_size_provider=diff_size,
    )

    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.api_error is not None and result.api_error.status == 401
    assert result.api_error_agent == "claude"


@pytest.mark.asyncio
async def test_reviewer_spawn_failure_with_auth_stderr_tags_reviewer_agent(
    tmp_path: Path,
) -> None:
    """A reviewer whose process fails to spawn at all (e.g. a codex refresh
    failure before the CLI even starts) can still print a plaintext auth line
    on stderr. `_run_reviewer_pass` must classify it and tag `api_error_agent`
    with the reviewer's own agent — never the verifier's (claude), which never
    ran this iteration. Regression for the ambiguous combined-log fallback,
    which had no per-agent scoping and could misattribute a codex spawn-time
    401 to an uninvolved claude verifier."""
    spawn_auth_failure = [
        RunnerEvent(kind="stderr", line="401 Unauthorized: refresh token expired"),
        RunnerEvent(kind="spawn_failed", error="codex auth refresh failed"),
    ]
    runner = _ScriptedRunner(scripts=[spawn_auth_failure, spawn_auth_failure])

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-spawn-401",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.api_error is not None and result.api_error.status == 401
    assert result.api_error_agent == "codex"


@pytest.mark.asyncio
async def test_fixer_stall_with_auth_stderr_tags_fixer_agent(tmp_path: Path) -> None:
    """A fix-run that stalls after printing a plaintext auth line on stderr
    must surface `FixerOutput.api_error` so `LoopResult.api_error_agent` is
    the fixer's own agent (codex) — not the claude reviewer/verifier that
    already succeeded this iteration."""
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- [Major] bug\n{VERDICT_CHANGES_REQUESTED_MARKER}"
            ),
            [
                RunnerEvent(kind="stderr", line="Not logged in. Please run /login."),
                RunnerEvent(kind="stall_timeout"),
            ],
        ],
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-fix-stall-401",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.FIX_RUN_FAILED
    assert result.api_error is not None and result.api_error.status == 401
    assert result.api_error_agent == "claude"


@pytest.mark.asyncio
async def test_fixer_nonzero_exit_with_auth_stderr_tags_fixer_agent(tmp_path: Path) -> None:
    """A fix-run that prints a plaintext auth line on stderr and exits
    non-zero (the normal shape: claude prints "Not logged in" and exits 1)
    must surface `FixerOutput.api_error` so `LoopResult.api_error_agent` is
    the fixer's own agent, not left `None`. Regression: the rc!=0 branch
    returned a plain FIX_RUN_FAILED with no api_error, so the connection
    never expired."""
    runner = _ScriptedRunner(
        scripts=[
            _codex_message_stream(
                f"## Findings\n- [Major] bug\n{VERDICT_CHANGES_REQUESTED_MARKER}"
            ),
            [
                RunnerEvent(kind="stderr", line="Not logged in. Please run /login."),
                RunnerEvent(kind="exit", returncode=1),
            ],
        ],
    )

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-fix-nonzero-401",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.FIX_RUN_FAILED
    assert result.api_error is not None and result.api_error.status == 401
    assert result.api_error_agent == "claude"


@pytest.mark.asyncio
async def test_reviewer_stall_after_auth_prose_does_not_flag_auth_failure(
    tmp_path: Path,
) -> None:
    """A reviewer reviewing an auth-related diff streams findings that quote
    auth wording ("401 Unauthorized", "refresh token expired") on stdout, then
    stalls with no verdict. That prose is the reviewer's own review text — the
    credential is fine — so `LoopResult.api_error` must stay None and the
    caller must not expire the provider. Regression for the per-pass plaintext
    scan reading the pass's whole stdout, where a review *about* auth code
    looked identical to a real pre-stream auth failure."""
    auth_prose_then_stall = [
        RunnerEvent(
            kind="stdout",
            line=json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "i",
                        "type": "agent_message",
                        "text": (
                            "## Findings\n- [Major] The refresh path returns 200 where it should "
                            "return 401 Unauthorized once the refresh token expired."
                        ),
                    },
                }
            ),
        ),
        RunnerEvent(kind="stall_timeout"),
    ]
    runner = _ScriptedRunner(scripts=[auth_prose_then_stall, auth_prose_then_stall])

    async def head_sha(_: Path) -> str:
        return "sha-1"

    result = await run_local_review_session(
        runner=runner,
        workspace_path=tmp_path / "ws",
        base_branch="main",
        parent_run_id="run-auth-prose",
        issue_title="t",
        issue_body="b",
        labels=[],
        reviewer_role=ResolvedRole(agent="codex", model="gpt-5.1-codex"),
        verifier_role=ResolvedRole(agent="claude"),
        fixer_role=ResolvedRole(agent="claude"),
        cap=5,
        stall_secs=300,
        last_message_dir=tmp_path / "last",
        head_sha_provider=head_sha,
    )

    assert result.outcome == LoopOutcome.REVIEWER_FAILED
    assert result.api_error is None
    assert result.api_error_agent is None
