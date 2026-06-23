"""Local-reviewer pieces: prompt, command builder, output parser.

This module is the building block for the local-review flow described in
`docs/local-review-flow.md`. It is intentionally pure: no subprocess, no
git, no network. The orchestrator owns the side effects; this module
owns the contract with the reviewer agent.

Contract with the reviewer agent
--------------------------------
The reviewer is asked to end its final message with exactly one of:

    <<<VERDICT:APPROVED>>>
    <<<VERDICT:CHANGES_REQUESTED>>>

A structured marker is more robust than parsing the free-form body for
phrases like "Didn't find any major issues" (which is what we have to do
for the remote `@codex` bot, see `review_classifier`). When the marker
is `CHANGES_REQUESTED`, the agent emits a `## Findings` section above
the marker — that text becomes the `trigger` passed to the next
`review_comment_fix_prompt`.

If neither marker is present (model failed to follow instructions, or
the run was killed before the final message), the verdict is
`UNPARSEABLE` and the caller decides whether to retry or escalate.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from ..agent.codex_models import DEFAULT_CODEX_MODEL

VERDICT_APPROVED_MARKER = "<<<VERDICT:APPROVED>>>"
VERDICT_CHANGES_REQUESTED_MARKER = "<<<VERDICT:CHANGES_REQUESTED>>>"

ReviewerAgent = Literal["claude", "codex"]

# Read-only repo search (Grep/Glob/LS) is in-surface so the reviewer can find
# callers, sibling implementations, and existing tests of changed code. Write
# and exec tools stay forbidden; the reviewer remains single-pass and read-only.
_CLAUDE_REVIEWER_TOOLS = "Bash,Read,Grep,Glob,LS"
_CLAUDE_REVIEWER_ALLOWED_TOOLS = "Bash(git diff *),Read,Grep,Glob,LS"
_CLAUDE_REVIEWER_DISALLOWED_TOOLS = ",".join(
    (
        "Edit",
        "Write",
        "MultiEdit",
        "NotebookRead",
        "NotebookEdit",
        "WebFetch",
        "WebSearch",
        "TodoWrite",
        "Task",
    )
)
_CLAUDE_REVIEWER_SETTINGS = json.dumps(
    {
        "autoMemoryEnabled": False,
        "claudeMdExcludes": ["**/CLAUDE.md", "**/CLAUDE.local.md"],
        "disableAllHooks": True,
    },
    sort_keys=True,
    separators=(",", ":"),
)
_CLAUDE_REVIEWER_SETTING_SOURCES = ""

# Tier B (pass-2 verifier only): on top of the read-only surface, the verifier
# may PROVE a finding by writing a throwaway failing test and running it
# targeted. Write/Edit join the in-surface tools, and a narrow set of
# test-runner Bash commands join the allowlist. Pass 1 and the single-pass
# fallback never see these — they stay strictly read-only.
_CLAUDE_VERIFIER_TOOLS = "Bash,Read,Grep,Glob,LS,Write,Edit"
_CLAUDE_VERIFIER_ALLOWED_TOOLS = ",".join(
    (
        "Bash(git diff *)",
        "Bash(uv run pytest *)",
        "Bash(npm test *)",
        "Bash(tsc *)",
        "Read",
        "Grep",
        "Glob",
        "LS",
        "Write",
        "Edit",
    )
)
_CLAUDE_VERIFIER_DISALLOWED_TOOLS = ",".join(
    (
        "MultiEdit",
        "NotebookRead",
        "NotebookEdit",
        "WebFetch",
        "WebSearch",
        "TodoWrite",
        "Task",
    )
)


# Provider API statuses we treat as transient: a retry can plausibly clear
# them (server overload / rate limit / gateway), unlike a deterministic 4xx
# such as 400 (bad request) or 404. Used to tell a genuine transient apart
# from a clean no-verdict stream so downstream retry logic only retries the
# former.
TRANSIENT_API_STATUSES: frozenset[int] = frozenset({429, 500, 502, 503, 529})

_API_ERROR_STATUS_RE = re.compile(r"API Error:\s*(\d{3})")


@dataclass(frozen=True)
class StreamApiError:
    """A provider API error recovered from an agent's terminal JSONL stream.

    `message` is the human-readable cause (e.g. "API Error: 500 …"); `status`
    is the HTTP-ish status when the stream carried one. `transient` is True
    when `status` is one a retry can plausibly clear — the typed signal
    downstream retry logic gates on. A clean no-verdict stream classifies to
    `None` (not a non-transient error), so it is never mistaken for a retryable
    failure.
    """

    message: str
    status: int | None = None

    @property
    def transient(self) -> bool:
        return self.status in TRANSIENT_API_STATUSES


def _status_from_text(text: str) -> int | None:
    match = _API_ERROR_STATUS_RE.search(text)
    return int(match.group(1)) if match else None


def _unwrap_codex_error(text: str) -> tuple[str | None, int | None]:
    """Dig the real message + status out of codex's one-level-nested error.

    A `turn.failed` carries `error.message` whose value is itself a JSON string
    like `{"type":"error","status":500,"error":{"message":"…"}}`. Returns
    `(message, status)`, either of which may be None when absent.
    """
    try:
        obj = json.loads(text)
    except (ValueError, TypeError):
        return None, None
    if not isinstance(obj, dict):
        return None, None
    status = obj.get("status") if isinstance(obj.get("status"), int) else None
    message: str | None = None
    nested = obj.get("error")
    if isinstance(nested, dict):
        if isinstance(nested.get("message"), str):
            message = nested["message"].strip() or None
        if status is None and isinstance(nested.get("status"), int):
            status = nested["status"]
    if message is None and isinstance(obj.get("message"), str):
        message = obj["message"].strip() or None
    return message, status


def _claude_result_api_error(event: dict) -> StreamApiError | None:
    """A claude terminal `result` with `is_error: true` (e.g. `api_error_status`
    500 + an `API Error: …` result text)."""
    status = event.get("api_error_status")
    status = status if isinstance(status, int) else None
    text = event.get("result")
    message = text.strip() if isinstance(text, str) and text.strip() else None
    # Gate: require a recognized API error indicator — either api_error_status
    # is present or the result text matches "API Error: <3-digit status>".
    if status is None and (message is None or not _API_ERROR_STATUS_RE.search(message)):
        return None
    if message is None:
        message = f"API Error: {status}"
    if status is None:
        status = _status_from_text(message)
    return StreamApiError(message=message, status=status)


def _claude_synthetic_api_error(event: dict) -> StreamApiError | None:
    """A claude `model:"<synthetic>"` assistant message whose text reads
    `API Error: <status> …` (the placeholder claude emits in place of the model
    turn when the provider call fails)."""
    message = event.get("message")
    if not isinstance(message, dict) or message.get("model") != "<synthetic>":
        return None
    parts: list[str] = []
    for block in message.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    if not parts:
        return None
    text = "\n".join(parts)
    return StreamApiError(message=text, status=_status_from_text(text))


def _codex_event_api_error(event: dict) -> StreamApiError | None:
    """A codex `error` / `turn.failed` event (the real cause is one JSON level
    deep in `error.message`)."""
    err = event.get("error")
    raw: str | None = None
    status: int | None = None
    if isinstance(err, dict):
        if isinstance(err.get("message"), str):
            raw = err["message"]
        if isinstance(err.get("status"), int):
            status = err["status"]
    elif isinstance(err, str):
        raw = err
    if raw is None and isinstance(event.get("message"), str):
        raw = event["message"]
    if status is None and isinstance(event.get("status"), int):
        status = event["status"]
    if raw is None or not raw.strip():
        return None
    inner_msg, inner_status = _unwrap_codex_error(raw)
    if inner_status is not None:
        status = inner_status
    msg = inner_msg or raw.strip()
    if status is not None and not msg.startswith("API Error:"):
        msg = f"API Error: {status} {msg}"
    return StreamApiError(message=msg, status=status)


def classify_stream_api_error(stdout: str) -> StreamApiError | None:
    """Recover a provider API error from an agent's terminal JSONL stream.

    Both providers can exit 0 carrying only an error and no verdict / completion
    marker:

    * claude emits a terminal `result` with `is_error: true` +
      `api_error_status` (e.g. 500) and a synthetic `model:"<synthetic>"`
      assistant message whose text is `"API Error: 500 …"`.
    * codex emits an `error` / `turn.failed` event whose (one-level-nested)
      payload carries the message and a `status`.

    Returns the last such error with its real message + status, or None for a
    clean stream. Callers read `.transient` to gate retries; because a clean
    no-verdict stream returns None (not a non-transient error), it never retries.
    """
    found: StreamApiError | None = None
    for line in stdout.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            continue
        if not isinstance(event, dict):
            continue
        etype = event.get("type")
        if etype == "result" and event.get("is_error") is True:
            err = _claude_result_api_error(event)
        elif etype == "assistant":
            err = _claude_synthetic_api_error(event)
        elif etype in ("error", "turn.failed"):
            err = _codex_event_api_error(event)
        else:
            err = None
        if err is not None:
            found = err
    return found


class LocalVerdictKind(StrEnum):
    APPROVED = "approved"
    CHANGES_REQUESTED = "changes_requested"
    UNPARSEABLE = "unparseable"


@dataclass(frozen=True)
class LocalVerdict:
    kind: LocalVerdictKind
    findings: str = ""
    trigger_signature: str = ""
    raw_message: str = ""
    # Findings-only digest used by the local loop to detect non-convergence.
    # This deliberately ignores HEAD so unresolved findings after a fix commit
    # still trip STUCK_LOOP instead of burning another identical fix-run.
    findings_signature: str = ""


# --- shared prompt blocks ------------------------------------------------
# The single-pass reviewer and the two-pass finder/verifier share the same
# stance, reading instructions, priorities, and lenses; only the intro and
# the "how to respond" section differ per role. Factoring keeps the three
# variants in lockstep so a lens tweak lands everywhere at once.

_STANCE_BLOCK = (
    "# Stance\n\n"
    "Be adversarial: assume a bug exists and your job is to find it. "
    "Actively try to break the change — feed it the edge cases, the "
    "empty inputs, the concurrent calls, the malformed rows the author "
    "didn't consider. An empty 'looks good' is only allowed AFTER a "
    "genuine attempt to break it; a bare approval with no attempt is a "
    "review failure.\n\n"
)

_WHAT_TO_LOOK_FOR_BLOCK = (
    "# What to look for (in priority order)\n\n"
    "1. The change satisfies the stated issue, including any "
    "explicit acceptance criteria.\n"
    "2. Correctness bugs: missing edge cases, off-by-one, incorrect "
    "error types, swallowed exceptions, races, broken invariants.\n"
    "3. Test coverage: new behavior has tests; tests would actually "
    "fail without the change.\n"
    "4. No unrelated edits, no dead code, no leftover scaffolding "
    "(stale TODO/FIXME comments inserted by the implementer count).\n\n"
)

_LENSES_BLOCK = (
    "# Lenses\n\n"
    "On top of correctness/intent/tests above, apply the lenses below "
    "that are relevant to the files the diff touches (skip the ones that "
    "don't apply):\n\n"
    "- **Data & query correctness** (SQL/SQLite changes): WHERE-clause "
    "composition, parameter binding (no string interpolation, right "
    "number of params), NULL/empty handling, and the "
    "`run_model_usage`↔`runs` token invariants — per-model usage rows "
    "must reconcile with the run-level token totals.\n"
    "- **Test quality** (test changes): would the test actually FAIL "
    "without the production change? Or does it pass against the old code "
    "too (asserting nothing)? Watch for stale fixtures, over-mocking that "
    "hides the real path, and assertions on incidental values.\n"
    "- **Frontend data-flow** (React/TS changes): react-query queryKey "
    "collisions and `enabled` gating, `useEffect` dependency arrays "
    "(missing or over-broad deps), and debounce/race conditions on "
    "rapidly-changing inputs.\n\n"
    "Be strict but practical. Style nits are NOT blocking — do not "
    "demand stylistic churn the fixer has to chase. Spend the depth on "
    "correctness, and flag only issues a careful human reviewer would "
    "block merge on.\n\n"
)

# Final-message contract: enumerate-then-approve, or `## Findings` + marker.
# Used by the single-pass reviewer and the two-pass verifier — the two roles
# that own the verdict. The trailing no-mutation line is appended only for the
# read-only single-pass role; the pass-2 verifier swaps in an execution block.
_VERDICT_CONTRACT_BLOCK = (
    "# How to respond\n\n"
    "End your final message with EXACTLY ONE of these markers on a "
    "line by itself:\n\n"
    f"    {VERDICT_APPROVED_MARKER}\n"
    f"    {VERDICT_CHANGES_REQUESTED_MARKER}\n\n"
    "To emit `APPROVED`, you must first enumerate — above the marker — "
    "what you tried to break and why each attempt held (the edge cases, "
    "inputs, and invariants you probed). No 'what I tried to break' "
    "list, no approval.\n\n"
    "If `CHANGES_REQUESTED`, write a `## Findings` section above the "
    "marker. Each finding is a bullet with:\n"
    "  - the file:line where it applies (e.g. `src/foo.py:42`),\n"
    "  - one sentence on what's wrong,\n"
    "  - one sentence on the fix.\n\n"
    "The findings text is fed verbatim into the next fix-run prompt, "
    "so vague findings produce vague fixes. Pretend you're writing a "
    "PR review for a junior engineer who will edit only the lines "
    "you cite.\n\n"
)

_NO_MUTATION_BLOCK = (
    "Do NOT modify any files. Do NOT run git commit, git push, or "
    "any command that mutates the working tree.\n\n"
)

_HOW_TO_RESPOND_VERDICT_BLOCK = _VERDICT_CONTRACT_BLOCK + _NO_MUTATION_BLOCK

# Pass-2 only: the verifier may PROVE a finding by running code. This block
# replaces the read-only no-mutation line. It is finding-triggered (only with
# a concrete hypothesis), bounds itself to throwaway targeted tests, and caps
# at three runs. The orchestrator scrubs the working tree after this pass, so
# nothing the verifier writes here reaches the diff the fixer sees.
_PASS_TWO_EXECUTION_BLOCK = (
    "# Proving a finding by execution\n\n"
    "You have write and test-run access this pass. Use it ONLY to prove a "
    "concrete hypothesis — not to explore. When you have a concrete "
    "hypothesis that a specific bug exists, write a throwaway failing test "
    "that would pass if the code were correct, and run it TARGETED — a "
    "single test, never the whole suite (e.g. `uv run pytest "
    "path/to/test.py::test_name`, `npm test -- -t name`). Cap yourself at "
    "THREE targeted test runs total for the whole pass.\n\n"
    "If a run confirms the bug, the finding is a blocker: quote the failing "
    "assertion / error output in that finding's `## Findings` bullet as "
    "evidence. If you cannot reproduce it after a genuine attempt, drop the "
    "finding or mark it explicitly unproven — do not pad findings with "
    "hypotheses you could not confirm.\n\n"
    "Your throwaway tests and any scratch edits are scrubbed from the working "
    "tree after this pass — do NOT git commit/push them and do not rely on "
    "them persisting. Cite real `file:line` locations in the production code, "
    "not in your throwaway test.\n\n"
)

# Pass-1 contract: list every suspicion, emit NO verdict marker. The verifier
# pass refutes/confirms these and owns the verdict.
_HOW_TO_RESPOND_FINDER_BLOCK = (
    "# How to respond\n\n"
    "Write a single `## Findings` section listing every suspicion you "
    "have — one bullet each, with:\n"
    "  - the file:line where it applies (e.g. `src/foo.py:42`),\n"
    "  - one sentence on what you suspect is wrong,\n"
    "  - one sentence on the fix you'd expect.\n\n"
    "Do NOT self-censor: list even low-confidence suspicions so the "
    "verifier can refute or confirm them — a missed bug is worse than a "
    "false alarm the next pass discards. If you genuinely find nothing "
    "after a real attempt to break the change, say so explicitly.\n\n"
    "Do NOT emit any verdict marker (no `<<<VERDICT:...>>>` line). The "
    "verifier pass owns the verdict; your findings are its input.\n\n"
    "Do NOT modify any files. Do NOT run git commit, git push, or "
    "any command that mutates the working tree.\n\n"
)


def _what_to_read_block(base_branch: str) -> str:
    return (
        "# What to read\n\n"
        f"1. Run `git diff origin/{base_branch}...HEAD`. If the `origin/"
        f"{base_branch}` ref does not exist in this checkout, fall back "
        f"to `{base_branch}...HEAD`. Read the full diff. (Do not narrate "
        "which ref you used — just review.)\n"
        "2. Read the changed files in context where the diff alone is "
        "ambiguous.\n"
        "3. For every changed symbol (function, class, constant), use "
        "`Grep`/`Glob`/`LS` to read the surrounding repo: find the callers "
        "of changed code, sibling implementations that should change in "
        "lockstep, and the existing tests that cover the touched code. The "
        "diff alone hides interaction bugs between changed and unchanged "
        "code — go look.\n"
        "4. Re-read the Linear issue below and check that the change "
        "actually satisfies it — not just that it compiles.\n\n"
    )


def _issue_block(issue_title: str, issue_body: str, labels: list[str]) -> str:
    label_line = ", ".join(labels) if labels else "(no labels)"
    body = issue_body.strip() if issue_body else "(no description)"
    return (
        "# Issue\n\n"
        f"## Title\n{issue_title}\n\n"
        f"## Labels\n{label_line}\n\n"
        f"## Description\n{body}\n"
    )


def local_review_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    base_branch: str,
) -> str:
    """Instructions for the single-pass local reviewer agent.

    The reviewer's job is narrow: read `git diff origin/<base>...HEAD`,
    decide if the branch is mergeable, emit a structured verdict. This is
    the small-diff fallback for the two-pass stage (see
    `local_review_finder_prompt` / `local_review_verifier_prompt`).

    Prompt-quality notes from real-CLI smoke runs (iter 6) drove some
    specific phrasings:
      - Tell the reviewer to fall back to `<base>...HEAD` if origin is
        missing, but *not* to narrate which ref it used (kept findings
        focused on actual issues, not git plumbing).
      - Demand `path:line` citations explicitly; smoke runs showed
        without prompting the reviewer often gave vague locations.
      - Force the marker to be on its own line — easier for the parser
        and harder for the model to "almost" emit.
    """
    return (
        "You are Symphony's local-review agent. Your only job is to "
        "produce a verdict on the current branch — not to fix anything.\n\n"
        + _STANCE_BLOCK
        + _what_to_read_block(base_branch)
        + _WHAT_TO_LOOK_FOR_BLOCK
        + _LENSES_BLOCK
        + _HOW_TO_RESPOND_VERDICT_BLOCK
        + _issue_block(issue_title, issue_body, labels)
    )


def local_review_finder_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    base_branch: str,
) -> str:
    """Pass-1 (finder) instructions: list every suspicion, no verdict.

    The finder runs opposite the implementer's family and feeds its
    `## Findings` into the pass-2 verifier. It deliberately emits no
    verdict marker — the verifier owns the final verdict.
    """
    return (
        "You are Symphony's local-review FINDER — pass 1 of a two-pass "
        "review. Your job is to surface every suspicion about the current "
        "branch so a second-pass verifier (a different model family) can "
        "refute or confirm each one. You do NOT decide the verdict.\n\n"
        + _STANCE_BLOCK
        + _what_to_read_block(base_branch)
        + _WHAT_TO_LOOK_FOR_BLOCK
        + _LENSES_BLOCK
        + _HOW_TO_RESPOND_FINDER_BLOCK
        + _issue_block(issue_title, issue_body, labels)
    )


def local_review_verifier_prompt(
    *,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    base_branch: str,
    pass_one_findings: str,
) -> str:
    """Pass-2 (verifier) instructions: refute pass-1, add misses, verdict.

    The verifier runs in the implementer's family (model diversity vs the
    finder), tries to refute each pass-1 finding, adds anything pass 1
    missed, and emits the final `## Findings` + verdict marker that the
    loop parses.
    """
    findings = pass_one_findings.strip() or "(pass 1 reported no findings)"
    pass_one_block = (
        "# Pass-1 findings to verify\n\n"
        "Treat each item below as a claim to disprove, not a fact. For "
        "each one, try to REFUTE it — read the surrounding code and prove "
        "it is a false alarm. Keep only the findings that survive a "
        "genuine refutation attempt and drop the rest silently. Then add "
        "any real issue pass 1 missed.\n\n"
        f"{findings}\n\n"
    )
    return (
        "You are Symphony's local-review VERIFIER — pass 2 of a two-pass "
        "review. A first-pass finder has listed its suspicions below. "
        "Refute the weak ones, confirm the real ones, add anything it "
        "missed, and emit the final verdict.\n\n"
        + _STANCE_BLOCK
        + _what_to_read_block(base_branch)
        + _WHAT_TO_LOOK_FOR_BLOCK
        + _LENSES_BLOCK
        + pass_one_block
        + _VERDICT_CONTRACT_BLOCK
        + _PASS_TWO_EXECUTION_BLOCK
        + _issue_block(issue_title, issue_body, labels)
    )


def build_local_review_command(
    *,
    agent: ReviewerAgent,
    prompt: str,
    base_branch: str,
    codex_model: str = DEFAULT_CODEX_MODEL,
    claude_model: str | None = None,
    last_message_path: str | None = None,
    pass_two: bool = False,
) -> list[str]:
    """argv for the local reviewer subprocess.

    `pass_two` grants Tier B execution/write so the verifier can prove a
    finding by writing a throwaway test and running it: codex switches to
    `--sandbox workspace-write`, and claude gains `Write`/`Edit` plus a
    narrow set of test-runner Bash commands. Pass 1 and the single-pass
    fallback (`pass_two=False`) stay strictly read-only.

    `codex` uses plain `codex exec --sandbox read-only [PROMPT]`. We
    intentionally do NOT use the `codex exec review` subcommand: it
    imposes its own opinionated output schema (a `[P1] title — path:line —
    body` review-comment format) and ignores instructions to emit our
    verdict marker. Plain `exec` lets us drive the output format from
    the prompt. The `read-only` sandbox keeps the reviewer from
    modifying the working tree.

    `base_branch` is threaded into the prompt body, not forwarded as a
    flag; the parameter is kept in the signature because callers and
    tests use it.

    `claude` runs through `--print` with the same prompt. It uses explicit
    non-bare isolation controls so auth still loads, while user/project/local
    filesystem settings, MCP servers, hooks, skills, auto memory, CLAUDE.md,
    and tools outside the reviewer's read-only surface are kept out of the
    subprocess.
    """
    _ = base_branch
    if agent == "codex":
        command = [
            "codex",
            "exec",
            "--sandbox",
            "workspace-write" if pass_two else "read-only",
            "--json",
            "--model",
            codex_model,
        ]
        if last_message_path is not None:
            command.extend(["-o", last_message_path])
        command.append(prompt)
        return command
    if agent == "claude":
        if pass_two:
            disallowed = _CLAUDE_VERIFIER_DISALLOWED_TOOLS
            tools = _CLAUDE_VERIFIER_TOOLS
            allowed = _CLAUDE_VERIFIER_ALLOWED_TOOLS
        else:
            disallowed = _CLAUDE_REVIEWER_DISALLOWED_TOOLS
            tools = _CLAUDE_REVIEWER_TOOLS
            allowed = _CLAUDE_REVIEWER_ALLOWED_TOOLS
        command = [
            "claude",
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-mode",
            "default",
            "--strict-mcp-config",
            "--disable-slash-commands",
            "--setting-sources",
            _CLAUDE_REVIEWER_SETTING_SOURCES,
            "--settings",
            _CLAUDE_REVIEWER_SETTINGS,
            "--disallowedTools",
            disallowed,
            "--tools",
            tools,
            "--allowedTools",
            allowed,
        ]
        if claude_model is not None:
            command.extend(["--model", claude_model])
        command.extend(["--", prompt])
        return command
    raise ValueError(f"unknown reviewer agent {agent!r}")


# --- diff-size measurement -----------------------------------------------
# Above these thresholds the reviewer splits into finder→verifier; at or
# below, a single direct review runs to save a second subprocess. "Small"
# requires BOTH to hold (few lines AND few files).
SMALL_DIFF_MAX_LINES = 150
SMALL_DIFF_MAX_FILES = 3


@dataclass(frozen=True)
class DiffSize:
    changed_lines: int
    changed_files: int


def parse_diff_numstat(numstat_output: str) -> DiffSize:
    """Parse `git diff --numstat` output into a `DiffSize`.

    Each non-empty line is `<added>\\t<deleted>\\t<path>`. Binary files
    report `-` for added/deleted; they still count as a changed file but
    contribute zero counted lines. Malformed lines are skipped.
    """
    changed_lines = 0
    changed_files = 0
    for raw in numstat_output.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        changed_files += 1
        added, deleted = parts[0], parts[1]
        if added.isdigit():
            changed_lines += int(added)
        if deleted.isdigit():
            changed_lines += int(deleted)
    return DiffSize(changed_lines=changed_lines, changed_files=changed_files)


def is_small_diff(diff_size: DiffSize) -> bool:
    """Whether a diff is small enough to skip the two-pass split.

    Small = at or below BOTH thresholds, so a single direct review is
    worth the cost saving over spawning two reviewer subprocesses.
    """
    return (
        diff_size.changed_lines <= SMALL_DIFF_MAX_LINES
        and diff_size.changed_files <= SMALL_DIFF_MAX_FILES
    )


def default_reviewer_agent(implementer_agent: str) -> ReviewerAgent:
    """Pair the reviewer against the implementer for a second opinion.

    A reviewer that shares the implementer's blind spots is less useful;
    defaulting to the opposite family is the cheapest way to keep them
    independent. Operators can still override via `reviewer_agent` on
    the binding.
    """
    if implementer_agent == "claude":
        return "codex"
    if implementer_agent == "codex":
        return "claude"
    raise ValueError(f"unknown implementer agent {implementer_agent!r}")


_VERDICT_LINE_RE = re.compile(
    rf"({re.escape(VERDICT_APPROVED_MARKER)}|"
    rf"{re.escape(VERDICT_CHANGES_REQUESTED_MARKER)})"
)
_FINDINGS_HEADING_RE = re.compile(r"(?im)^\s*#{1,6}\s*findings\b\s*$")


def extract_last_agent_message(
    *, agent: ReviewerAgent, stdout: str, last_message_file: str | None = None
) -> str:
    """Extract the reviewer's final message text from the runner output.

    `codex exec review --json` emits JSONL with terminal event
    `item.completed` containing an `agent_message`. The same text is
    also written to `-o <file>` so we prefer that file when present —
    it's the authoritative single-message form and survives even when
    stdout is truncated.

    `claude --print --output-format stream-json` emits a sequence of
    events terminated by `{"type":"result","result":"..."}`.
    """
    if last_message_file:
        text = last_message_file.strip()
        if text:
            return text
    if agent == "codex":
        return _codex_last_agent_message(stdout)
    if agent == "claude":
        return _claude_last_result_text(stdout)
    raise ValueError(f"unknown reviewer agent {agent!r}")


def _codex_last_agent_message(stdout: str) -> str:
    last = ""
    for line in stdout.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") != "item.completed":
            continue
        item = event.get("item") or {}
        if item.get("type") == "agent_message":
            text = item.get("text")
            if isinstance(text, str) and text:
                last = text
    return last


def _claude_last_result_text(stdout: str) -> str:
    last = ""
    for line in stdout.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        # The terminal "result" event carries the final text once.
        if event.get("type") == "result":
            text = event.get("result")
            if isinstance(text, str) and text:
                return text
        # Fall back to the last assistant message if no result event lands.
        if event.get("type") == "assistant":
            content = (event.get("message") or {}).get("content") or []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text")
                    if isinstance(text, str) and text:
                        last = text
    return last


def parse_local_review_output(
    *,
    agent: ReviewerAgent,
    stdout: str,
    head_sha: str,
    last_message_file: str | None = None,
) -> LocalVerdict:
    """Turn the reviewer's final message into a `LocalVerdict`.

    `head_sha` ties the trigger signature to the commit that was reviewed.
    Stuck-loop detection uses a separate findings-only signature so repeated
    unresolved findings after a fix commit still terminate as STUCK_LOOP.
    """
    message = extract_last_agent_message(
        agent=agent, stdout=stdout, last_message_file=last_message_file
    )
    return _classify_message(message=message, head_sha=head_sha)


def _classify_message(*, message: str, head_sha: str) -> LocalVerdict:
    if not message.strip():
        return LocalVerdict(kind=LocalVerdictKind.UNPARSEABLE, raw_message=message)

    matches = list(_VERDICT_LINE_RE.finditer(message))
    if not matches:
        return LocalVerdict(kind=LocalVerdictKind.UNPARSEABLE, raw_message=message)

    # The agent may quote the marker in its instructions section earlier
    # in the message; the last match is the operative one.
    marker = matches[-1].group(1)
    if marker == VERDICT_APPROVED_MARKER:
        return LocalVerdict(
            kind=LocalVerdictKind.APPROVED,
            findings="",
            trigger_signature=f"local_approved:{head_sha}",
            raw_message=message,
        )
    findings = _extract_findings(message=message, verdict_index=matches[-1].start())
    digest = _stable_digest(findings)
    return LocalVerdict(
        kind=LocalVerdictKind.CHANGES_REQUESTED,
        findings=findings,
        trigger_signature=f"local_review:{head_sha}:{digest}",
        raw_message=message,
        findings_signature=f"local_review_findings:{digest}",
    )


def _extract_findings(*, message: str, verdict_index: int) -> str:
    """Pull the `## Findings` section preceding the verdict marker.

    Falls back to "everything before the marker" if the agent skipped
    the heading — we still want *something* concrete to feed into the
    next fix-run prompt.
    """
    head = message[:verdict_index]
    m = _FINDINGS_HEADING_RE.search(head)
    if m is None:
        return head.strip()
    return head[m.end():].strip()


def _stable_digest(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode("utf-8"))
    return h.hexdigest()[:16]


__all__ = [
    "SMALL_DIFF_MAX_FILES",
    "SMALL_DIFF_MAX_LINES",
    "TRANSIENT_API_STATUSES",
    "DiffSize",
    "LocalVerdict",
    "LocalVerdictKind",
    "ReviewerAgent",
    "StreamApiError",
    "VERDICT_APPROVED_MARKER",
    "VERDICT_CHANGES_REQUESTED_MARKER",
    "build_local_review_command",
    "classify_stream_api_error",
    "default_reviewer_agent",
    "extract_last_agent_message",
    "is_small_diff",
    "local_review_finder_prompt",
    "local_review_prompt",
    "local_review_verifier_prompt",
    "parse_diff_numstat",
    "parse_local_review_output",
]
