"""Local-reviewer building blocks: prompt, command builder, parser."""

from __future__ import annotations

import json

import pytest

from symphony.pipeline.local_review import (
    SMALL_DIFF_MAX_FILES,
    SMALL_DIFF_MAX_LINES,
    VERDICT_APPROVED_MARKER,
    VERDICT_CHANGES_REQUESTED_MARKER,
    DiffSize,
    LocalVerdictKind,
    build_local_review_command,
    default_reviewer_agent,
    extract_last_agent_message,
    is_small_diff,
    local_review_finder_prompt,
    local_review_prompt,
    local_review_verifier_prompt,
    parse_diff_numstat,
    parse_local_review_output,
)

# --- prompt --------------------------------------------------------------


def test_local_review_prompt_mentions_base_branch_and_markers() -> None:
    prompt = local_review_prompt(
        issue_title="t",
        issue_body="b",
        labels=["x"],
        base_branch="main",
    )
    assert "origin/main" in prompt
    assert VERDICT_APPROVED_MARKER in prompt
    assert VERDICT_CHANGES_REQUESTED_MARKER in prompt
    # The prompt must forbid edits so the reviewer doesn't touch files.
    assert "Do NOT modify" in prompt


def test_local_review_prompt_is_deterministic() -> None:
    kwargs = {"issue_title": "t", "issue_body": "b", "labels": ["x"], "base_branch": "main"}
    assert local_review_prompt(**kwargs) == local_review_prompt(**kwargs)


def test_local_review_prompt_demands_actionable_findings() -> None:
    """Smoke runs (iter 6) showed vague findings hurt downstream
    fix-runs. The prompt must explicitly require file:line citations."""
    prompt = local_review_prompt(
        issue_title="t", issue_body="b", labels=[], base_branch="main"
    )
    # path:line example present.
    assert "file:line" in prompt or "foo.py:42" in prompt
    # Junior-engineer framing forces concrete-edit-only findings.
    assert "junior engineer" in prompt.lower()


def test_local_review_prompt_is_adversarial_and_lensed() -> None:
    """SYM-89: the single reviewer must dig harder — adversarial stance,
    explicit lenses applied to the touched files, and an asymmetric
    approval bar that forbids a bare 'looks good'."""
    prompt = local_review_prompt(
        issue_title="t", issue_body="b", labels=[], base_branch="main"
    )
    lower = prompt.lower()
    # 1. Adversarial stance: assume a bug exists; try to break it.
    assert "assume a bug exists" in lower
    assert "break it" in lower
    # 2. The three lenses, applied per touched files.
    assert "data" in lower and "sql" in lower
    assert "run_model_usage" in prompt
    assert "test quality" in lower
    assert "querykey" in lower  # frontend data-flow lens
    assert "relevant to the" in lower or "files the diff touches" in lower
    # 3. Asymmetric approval bar: enumerate what you tried to break.
    assert "tried to break" in lower
    # 4. Style nits stay explicitly non-blocking.
    assert "nit" in lower and "not blocking" in lower


def test_local_review_prompt_handles_missing_origin_ref() -> None:
    """Iter 6 smoke: scratch repos without `origin` made the reviewer
    narrate "I used main...HEAD instead" in findings. The prompt now
    instructs a silent fallback."""
    prompt = local_review_prompt(
        issue_title="t", issue_body="b", labels=[], base_branch="trunk"
    )
    # Fallback ref documented.
    assert "trunk...HEAD" in prompt
    # And instructed not to narrate.
    assert "Do not narrate" in prompt or "do not narrate" in prompt


# --- command builder -----------------------------------------------------


def test_build_local_review_command_codex_uses_plain_exec_read_only() -> None:
    argv = build_local_review_command(
        agent="codex",
        prompt="please review",
        base_branch="main",
        codex_model="gpt-5.1-codex",
        last_message_path="/tmp/last.txt",
    )
    # Plain `codex exec` — NOT `exec review`. The `review` subcommand
    # imposes its own output schema and ignores our verdict-marker
    # contract; see build_local_review_command docstring.
    assert argv[:2] == ["codex", "exec"]
    assert "review" not in argv[:3]
    # Read-only sandbox: reviewer can `git diff` but cannot write files.
    assert "--sandbox" in argv and argv[argv.index("--sandbox") + 1] == "read-only"
    # `--base` not forwarded; base branch lives in the prompt body.
    assert "--base" not in argv
    assert "--json" in argv
    assert "--model" in argv and argv[argv.index("--model") + 1] == "gpt-5.1-codex"
    assert "-o" in argv and argv[argv.index("-o") + 1] == "/tmp/last.txt"
    assert "--strict-mcp-config" not in argv
    assert "--mcp-config" not in argv
    assert "--permission-mode" not in argv
    assert "--disallowedTools" not in argv
    assert "--bare" not in argv
    assert "--tools" not in argv
    assert "--allowedTools" not in argv
    # The prompt must be the last positional argument so codex picks it up.
    assert argv[-1] == "please review"


def test_build_local_review_command_codex_omits_output_file_when_unset() -> None:
    argv = build_local_review_command(
        agent="codex",
        prompt="p",
        base_branch="main",
    )
    assert "-o" not in argv


def test_build_local_review_command_claude_uses_print_stream_json() -> None:
    argv = build_local_review_command(
        agent="claude",
        prompt="please review",
        base_branch="main",
    )
    assert argv[0] == "claude"
    assert "--print" in argv
    assert argv[argv.index("--output-format") + 1] == "stream-json"
    assert argv[-2:] == ["--", "please review"]
    assert argv[-1] == "please review"


def test_build_local_review_command_claude_isolates_reviewer_environment() -> None:
    argv = build_local_review_command(
        agent="claude",
        prompt="please review",
        base_branch="main",
    )
    assert "--strict-mcp-config" in argv
    assert "--mcp-config" not in argv
    assert "--permission-mode" in argv
    assert argv[argv.index("--permission-mode") + 1] == "default"
    assert "--disable-slash-commands" in argv
    assert "--setting-sources" in argv
    assert argv[argv.index("--setting-sources") + 1] == ""
    assert "--settings" in argv
    settings = json.loads(argv[argv.index("--settings") + 1])
    assert settings["autoMemoryEnabled"] is False
    assert settings["claudeMdExcludes"] == ["**/CLAUDE.md", "**/CLAUDE.local.md"]
    assert settings["disableAllHooks"] is True
    assert "--bare" not in argv
    assert "--tools" in argv
    assert argv[argv.index("--tools") + 1] == "Bash,Read,Grep,Glob,LS"
    assert "--allowedTools" in argv
    assert (
        argv[argv.index("--allowedTools") + 1]
        == "Bash(git diff *),Read,Grep,Glob,LS"
    )
    assert "--disallowedTools" in argv
    disallowed_tools = argv[argv.index("--disallowedTools") + 1].split(",")
    assert {"Edit", "Write", "MultiEdit", "WebFetch", "Task"}.issubset(
        disallowed_tools
    )
    # Read-only repo search is now part of the reviewer surface, not banned.
    assert {"Grep", "Glob", "LS"}.isdisjoint(disallowed_tools)
    tools = argv[argv.index("--tools") + 1].split(",")
    allowed_tools = argv[argv.index("--allowedTools") + 1].split(",")
    assert {"Grep", "Glob", "LS"}.issubset(tools)
    assert {"Grep", "Glob", "LS"}.issubset(allowed_tools)
    assert "Bash" not in disallowed_tools
    assert "Read" not in disallowed_tools
    assert argv.index("--disallowedTools") < argv.index("--tools")
    assert argv.index("--tools") < argv.index("--allowedTools")
    assert argv.index("--settings") < argv.index("--tools")
    assert argv[argv.index("--allowedTools") + 2] == "--"
    assert argv[-2:] == ["--", "please review"]


def test_build_local_review_command_claude_omits_model_when_unset() -> None:
    """`claude_model=None` (default) → no `--model` flag, identical to today."""
    argv = build_local_review_command(
        agent="claude",
        prompt="please review",
        base_branch="main",
    )
    assert "--model" not in argv


def test_build_local_review_command_claude_injects_model_when_set() -> None:
    """`claude_model` set → `--model <value>` injected into the claude argv.

    Covers the finder and the single-pass reviewer; the pass-2 verifier
    routes through this same builder (see pass_two variant below).
    """
    argv = build_local_review_command(
        agent="claude",
        prompt="please review",
        base_branch="main",
        claude_model="claude-sonnet-4-6",
    )
    assert "--model" in argv
    assert argv[argv.index("--model") + 1] == "claude-sonnet-4-6"
    # Prompt stays the trailing positional after the `--` separator.
    assert argv[-2:] == ["--", "please review"]


def test_build_local_review_command_claude_pass_two_injects_model_when_set() -> None:
    """Pass-2 verifier routes through the same builder; `--model` applies."""
    argv = build_local_review_command(
        agent="claude",
        prompt="verify",
        base_branch="main",
        claude_model="claude-sonnet-4-6",
        pass_two=True,
    )
    assert argv[argv.index("--model") + 1] == "claude-sonnet-4-6"


def test_build_local_review_command_codex_ignores_claude_model() -> None:
    """Codex branch keeps using `codex_model`; `claude_model` is inert."""
    argv = build_local_review_command(
        agent="codex",
        prompt="p",
        base_branch="main",
        codex_model="gpt-5.1-codex",
        claude_model="claude-sonnet-4-6",
    )
    assert argv[argv.index("--model") + 1] == "gpt-5.1-codex"
    assert "claude-sonnet-4-6" not in argv


def test_build_local_review_command_codex_pass_two_uses_workspace_write() -> None:
    """Tier B (pass 2 only): codex switches to a writable sandbox so the
    verifier can write a throwaway test and run it."""
    argv = build_local_review_command(
        agent="codex",
        prompt="verify",
        base_branch="main",
        codex_model="gpt-5.1-codex",
        last_message_path="/tmp/last.txt",
        pass_two=True,
    )
    assert argv[:2] == ["codex", "exec"]
    assert "--sandbox" in argv
    assert argv[argv.index("--sandbox") + 1] == "workspace-write"
    assert "read-only" not in argv


def test_build_local_review_command_claude_pass_two_grants_tier_b_tools() -> None:
    """Tier B (pass 2 only): claude gets Write/Edit plus a narrow set of
    test-runner Bash commands so it can prove a finding by running code."""
    argv = build_local_review_command(
        agent="claude",
        prompt="verify",
        base_branch="main",
        pass_two=True,
    )
    tools = argv[argv.index("--tools") + 1].split(",")
    allowed = argv[argv.index("--allowedTools") + 1]
    disallowed = argv[argv.index("--disallowedTools") + 1].split(",")
    # Write/Edit are now in-surface.
    assert {"Write", "Edit"}.issubset(tools)
    assert "Write" in allowed and "Edit" in allowed
    # Targeted test-runner commands are allowed; whole-suite is bounded by
    # the prompt, not the allowlist.
    assert "Bash(uv run pytest *)" in allowed
    assert "Bash(npm test *)" in allowed
    assert "Bash(tsc *)" in allowed
    # Write/Edit must NOT be disallowed this pass.
    assert "Write" not in disallowed
    assert "Edit" not in disallowed


def test_build_local_review_command_pass_one_and_single_pass_stay_read_only() -> None:
    """Pass 1 and the single-pass fallback (pass_two=False) keep the
    read-only surface: no exec/write for codex or claude."""
    codex_argv = build_local_review_command(
        agent="codex", prompt="p", base_branch="main"
    )
    assert codex_argv[codex_argv.index("--sandbox") + 1] == "read-only"
    assert "workspace-write" not in codex_argv

    claude_argv = build_local_review_command(
        agent="claude", prompt="p", base_branch="main"
    )
    tools = claude_argv[claude_argv.index("--tools") + 1].split(",")
    disallowed = claude_argv[claude_argv.index("--disallowedTools") + 1].split(",")
    assert {"Write", "Edit"}.isdisjoint(tools)
    assert {"Write", "Edit"}.issubset(disallowed)
    allowed = claude_argv[claude_argv.index("--allowedTools") + 1]
    assert "pytest" not in allowed


def test_build_local_review_command_unknown_agent_raises() -> None:
    with pytest.raises(ValueError, match="unknown reviewer agent"):
        build_local_review_command(
            agent="aider",  # type: ignore[arg-type]
            prompt="p",
            base_branch="main",
        )


def test_default_reviewer_agent_pairs_against_implementer() -> None:
    assert default_reviewer_agent("claude") == "codex"
    assert default_reviewer_agent("codex") == "claude"
    with pytest.raises(ValueError):
        default_reviewer_agent("unknown")


# --- message extraction --------------------------------------------------


def _codex_jsonl_with_final_message(text: str) -> str:
    events = [
        {"type": "thread.started", "thread_id": "t1"},
        {"type": "turn.started"},
        {
            "type": "item.completed",
            "item": {"id": "item_0", "type": "command_execution", "exit_code": 0},
        },
        {"type": "item.completed", "item": {"id": "item_1", "type": "agent_message", "text": text}},
        {"type": "turn.completed", "usage": {}},
    ]
    return "\n".join(json.dumps(e) for e in events)


def test_extract_last_agent_message_codex_picks_last_agent_message() -> None:
    stdout = _codex_jsonl_with_final_message("final reviewer text")
    assert (
        extract_last_agent_message(agent="codex", stdout=stdout)
        == "final reviewer text"
    )


def test_extract_last_agent_message_prefers_last_message_file() -> None:
    stdout = _codex_jsonl_with_final_message("stdout text")
    assert (
        extract_last_agent_message(
            agent="codex", stdout=stdout, last_message_file="file text"
        )
        == "file text"
    )


def test_extract_last_agent_message_claude_picks_result_event() -> None:
    events = [
        {"type": "system", "subtype": "init"},
        {
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "thinking aloud"}]},
        },
        {"type": "result", "result": "the final result"},
    ]
    stdout = "\n".join(json.dumps(e) for e in events)
    assert (
        extract_last_agent_message(agent="claude", stdout=stdout)
        == "the final result"
    )


def test_extract_last_agent_message_claude_falls_back_to_last_assistant() -> None:
    events = [
        {
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "first"}]},
        },
        {
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "second"}]},
        },
    ]
    stdout = "\n".join(json.dumps(e) for e in events)
    assert (
        extract_last_agent_message(agent="claude", stdout=stdout)
        == "second"
    )


def test_extract_last_agent_message_skips_garbage_lines() -> None:
    events = [
        "not-json garbage line",
        json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "i", "type": "agent_message", "text": "ok"},
            }
        ),
    ]
    assert (
        extract_last_agent_message(agent="codex", stdout="\n".join(events))
        == "ok"
    )


# --- verdict classification ---------------------------------------------


def test_parse_local_review_output_approved() -> None:
    message = f"Looks good.\n\n{VERDICT_APPROVED_MARKER}\n"
    stdout = _codex_jsonl_with_final_message(message)
    verdict = parse_local_review_output(
        agent="codex", stdout=stdout, head_sha="abc123"
    )
    assert verdict.kind == LocalVerdictKind.APPROVED
    assert verdict.findings == ""
    assert verdict.trigger_signature == "local_approved:abc123"
    assert verdict.raw_message == message


def test_parse_local_review_output_changes_requested_extracts_findings() -> None:
    message = (
        "Reviewed.\n\n"
        "## Findings\n"
        "- `a.py:42` swallows the wrong exception.\n"
        "- Missing test for the empty-input case.\n\n"
        f"{VERDICT_CHANGES_REQUESTED_MARKER}\n"
    )
    stdout = _codex_jsonl_with_final_message(message)
    verdict = parse_local_review_output(
        agent="codex", stdout=stdout, head_sha="abc123"
    )
    assert verdict.kind == LocalVerdictKind.CHANGES_REQUESTED
    assert "a.py:42" in verdict.findings
    assert "empty-input" in verdict.findings
    assert verdict.trigger_signature.startswith("local_review:abc123:")
    assert verdict.findings_signature.startswith("local_review_findings:")


def test_changes_requested_signature_changes_with_head_sha() -> None:
    message = f"## Findings\n- bug\n\n{VERDICT_CHANGES_REQUESTED_MARKER}"
    stdout = _codex_jsonl_with_final_message(message)
    v_a = parse_local_review_output(agent="codex", stdout=stdout, head_sha="aaa")
    v_b = parse_local_review_output(agent="codex", stdout=stdout, head_sha="bbb")
    assert v_a.trigger_signature != v_b.trigger_signature


def test_changes_requested_signature_changes_with_findings() -> None:
    head = "abc"
    a = parse_local_review_output(
        agent="codex",
        stdout=_codex_jsonl_with_final_message(
            f"## Findings\n- one\n\n{VERDICT_CHANGES_REQUESTED_MARKER}"
        ),
        head_sha=head,
    )
    b = parse_local_review_output(
        agent="codex",
        stdout=_codex_jsonl_with_final_message(
            f"## Findings\n- two\n\n{VERDICT_CHANGES_REQUESTED_MARKER}"
        ),
        head_sha=head,
    )
    assert a.trigger_signature != b.trigger_signature
    assert a.findings_signature != b.findings_signature


def test_parse_local_review_output_unparseable_when_no_marker() -> None:
    stdout = _codex_jsonl_with_final_message(
        "I think this is fine but I forgot to emit the marker."
    )
    verdict = parse_local_review_output(
        agent="codex", stdout=stdout, head_sha="abc123"
    )
    assert verdict.kind == LocalVerdictKind.UNPARSEABLE
    assert verdict.findings == ""
    assert verdict.trigger_signature == ""


def test_parse_local_review_output_unparseable_when_empty_message() -> None:
    stdout = ""
    verdict = parse_local_review_output(
        agent="codex", stdout=stdout, head_sha="abc"
    )
    assert verdict.kind == LocalVerdictKind.UNPARSEABLE


def test_parse_local_review_uses_last_marker_if_quoted_earlier() -> None:
    # The prompt itself quotes the markers; the operative one is the
    # final occurrence at the end of the message.
    message = (
        f"Per instructions I may emit {VERDICT_APPROVED_MARKER} or "
        f"{VERDICT_CHANGES_REQUESTED_MARKER}. "
        f"My actual verdict: {VERDICT_APPROVED_MARKER}"
    )
    stdout = _codex_jsonl_with_final_message(message)
    verdict = parse_local_review_output(
        agent="codex", stdout=stdout, head_sha="abc"
    )
    assert verdict.kind == LocalVerdictKind.APPROVED


def test_parse_local_review_findings_falls_back_when_no_heading() -> None:
    # If the agent forgets the `## Findings` heading, we still feed the
    # body before the marker into the next fix-run prompt.
    message = (
        "The new function ignores the timeout argument.\n"
        f"{VERDICT_CHANGES_REQUESTED_MARKER}"
    )
    stdout = _codex_jsonl_with_final_message(message)
    verdict = parse_local_review_output(
        agent="codex", stdout=stdout, head_sha="abc"
    )
    assert verdict.kind == LocalVerdictKind.CHANGES_REQUESTED
    assert "timeout" in verdict.findings


# --- two-pass prompts ----------------------------------------------------


def test_finder_prompt_lists_findings_without_a_verdict_marker() -> None:
    """Pass 1 enumerates suspicions but must not emit a verdict marker —
    the verifier pass owns the verdict."""
    prompt = local_review_finder_prompt(
        issue_title="t", issue_body="b", labels=[], base_branch="main"
    )
    lower = prompt.lower()
    # Shared adversarial stance + lenses are carried over.
    assert "assume a bug exists" in lower
    assert "run_model_usage" in prompt
    # Findings section is requested...
    assert "## Findings" in prompt
    # ...but the literal verdict markers must NOT appear as a response
    # instruction, and the model is told explicitly not to emit one.
    assert VERDICT_APPROVED_MARKER not in prompt
    assert VERDICT_CHANGES_REQUESTED_MARKER not in prompt
    assert "do not emit" in lower and "verdict marker" in lower


def test_verifier_prompt_injects_pass_one_findings_and_emits_marker() -> None:
    """Pass 2 receives pass-1 findings, is told to refute them, and emits
    the final verdict marker the parser reads."""
    pass_one = "- suspicious null deref at src/foo.py:42"
    prompt = local_review_verifier_prompt(
        issue_title="t",
        issue_body="b",
        labels=[],
        base_branch="main",
        pass_one_findings=pass_one,
    )
    lower = prompt.lower()
    # Pass-1 findings are injected verbatim for the verifier to check.
    assert pass_one in prompt
    # Adversarial refutation framing.
    assert "refute" in lower
    # Emits the same marker contract as the single-pass reviewer.
    assert VERDICT_APPROVED_MARKER in prompt
    assert VERDICT_CHANGES_REQUESTED_MARKER in prompt


def test_verifier_prompt_is_finding_triggered_and_caps_runs() -> None:
    """Pass 2 may PROVE a finding by running code, but only when it has a
    concrete hypothesis, with throwaway targeted tests capped at three."""
    prompt = local_review_verifier_prompt(
        issue_title="t",
        issue_body="b",
        labels=[],
        base_branch="main",
        pass_one_findings="- suspicion",
    )
    lower = prompt.lower()
    # Finding-triggered: only with a concrete hypothesis.
    assert "concrete hypothesis" in lower
    # Throwaway failing test, run targeted (not the whole suite).
    assert "throwaway" in lower
    assert "uv run pytest" in lower
    # Hard cap of three targeted runs.
    assert "three" in lower or "3" in prompt
    # Proven findings carry their failing-test evidence into Findings.
    assert "evidence" in lower
    # The verifier is NOT told to keep the tree read-only (it has write
    # access this pass); the no-mutation line is single-pass / finder only.
    assert "Do NOT modify any files" not in prompt


def test_single_pass_and_finder_prompts_stay_read_only() -> None:
    """Only pass 2 may write/execute; the single-pass reviewer and pass-1
    finder must keep the explicit no-mutation instruction."""
    single = local_review_prompt(
        issue_title="t", issue_body="b", labels=[], base_branch="main"
    )
    finder = local_review_finder_prompt(
        issue_title="t", issue_body="b", labels=[], base_branch="main"
    )
    assert "Do NOT modify any files" in single
    assert "Do NOT modify any files" in finder
    # No execution-grounding language leaks into the read-only roles.
    assert "throwaway" not in single.lower()
    assert "throwaway" not in finder.lower()


def test_verifier_prompt_handles_empty_pass_one_findings() -> None:
    prompt = local_review_verifier_prompt(
        issue_title="t",
        issue_body="b",
        labels=[],
        base_branch="main",
        pass_one_findings="   ",
    )
    assert VERDICT_APPROVED_MARKER in prompt
    # No findings still produces a usable verifier prompt.
    assert "no findings" in prompt.lower()


# --- diff-size measurement -----------------------------------------------


def test_parse_diff_numstat_sums_lines_and_counts_files() -> None:
    out = "10\t5\tsrc/a.py\n3\t0\tsrc/b.py\n"
    size = parse_diff_numstat(out)
    assert size == DiffSize(changed_lines=18, changed_files=2)


def test_parse_diff_numstat_handles_binary_and_blank_lines() -> None:
    # Binary files report `-` for added/deleted; they still count as a
    # changed file but contribute zero counted lines.
    out = "-\t-\tassets/logo.png\n\n7\t2\tsrc/c.py\n"
    size = parse_diff_numstat(out)
    assert size == DiffSize(changed_lines=9, changed_files=2)


def test_parse_diff_numstat_empty_is_zero() -> None:
    assert parse_diff_numstat("") == DiffSize(changed_lines=0, changed_files=0)


def test_small_diff_thresholds() -> None:
    assert SMALL_DIFF_MAX_LINES == 150
    assert SMALL_DIFF_MAX_FILES == 3


@pytest.mark.parametrize(
    "lines,files,small",
    [
        (150, 3, True),  # both at the inclusive boundary
        (150, 1, True),
        (1, 3, True),
        (0, 0, True),
        (151, 3, False),  # one line over
        (150, 4, False),  # one file over
        (151, 4, False),
        (1000, 9, False),
    ],
)
def test_is_small_diff_boundaries(lines: int, files: int, small: bool) -> None:
    assert is_small_diff(DiffSize(changed_lines=lines, changed_files=files)) is small
