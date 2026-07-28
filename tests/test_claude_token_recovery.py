"""SYM-236: an implement or fix run survives a token rotation instead of dying.

Three things are pinned here, at two seams.

At the CLI seam, that a dispatch armed for recovery is armed *completely*: the
prompt moved to stdin, the environment that makes the CLI ask at all, and a
handler to answer. Two of the three is a run that hangs or a run that never
asks, and neither failure announces itself.

At the orchestrator seam, that a run whose token is rejected mid-flight
finishes the work it had already done — and that everything held back (local
review, acceptance, the merge pass, codex, a deployment on ambient auth, a
binding bringing its own token) keeps the one-directional shape it has today.

And throughout, that the recovery is *counted*. There is no config flag behind
this mechanism, so the tally in the log is the only way to tell a working
recovery from one that has never fired.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
import respx

from symphony import db
from symphony.agent.claude_cli import (
    CLAUDE_CONTROL_CHANNEL_ENTRYPOINT,
    claude_control_channel_argv,
    claude_control_channel_env,
)
from symphony.agent.control_channel import ControlRequest, Decline
from symphony.agent.runner import RunnerEvent
from symphony.claude_login import CLAUDE_OAUTH_TOKEN_URL
from symphony.claude_token_dispenser import (
    ClaudeTokenDispenser,
    TokenGrant,
    TokenRefusal,
    TokenResponse,
)
from symphony.claude_token_recovery import OAUTH_TOKEN_REFRESH, ClaudeTokenRecovery
from symphony.config import Config, LinearStates, RepoBinding, ResolvedRole
from symphony.credentials import CredentialWriteBack
from symphony.crypto import CredentialCipher
from symphony.linear.client import Issue as LinearIssue
from symphony.orchestrator.poll import build_runner_command
from tests.harness import Harness

ENC_KEY = "deployment-secret"
CLAUDE_TOKEN_ENV = "CLAUDE_CODE_OAUTH_TOKEN"
PROMPT = "implement the thing"


# --- the CLI seam ---------------------------------------------------------


def test_a_conversation_run_takes_its_prompt_on_stdin() -> None:
    """The prompt leaves the argv and the CLI is told to read messages instead.

    Everything else the builder decided — model, effort, the deny-hook, the
    allowlist — has to survive: this mode changes how the prompt is delivered,
    not what the run is allowed to do."""
    built = build_runner_command("claude", PROMPT, claude_model="opus", effort="high")
    argv = claude_control_channel_argv(built, PROMPT)
    assert argv is not None
    assert PROMPT not in argv
    assert "--" not in argv
    assert argv[argv.index("--input-format") + 1] == "stream-json"
    # Read alongside `--output-format stream-json`: the input format is only
    # meaningful because the output already is one.
    assert argv[argv.index("--output-format") + 1] == "stream-json"
    assert argv[: len(built) - 2] == built[:-2]


def test_an_argv_that_no_longer_ends_in_the_prompt_is_left_alone() -> None:
    """The transformation checks itself against the prompt it was handed.

    If the builder ever stops putting the prompt last behind `--`, the cost is
    a run that keeps today's shape — never a run dispatched with its prompt
    silently dropped from both the argv and stdin."""
    built = build_runner_command("claude", PROMPT)
    assert claude_control_channel_argv(built, "a different prompt") is None
    assert claude_control_channel_argv([*built, "--extra"], PROMPT) is None
    assert claude_control_channel_argv(["claude"], PROMPT) is None


def test_a_codex_argv_is_left_alone(tmp_path: Path) -> None:
    """Only the claude CLI speaks this protocol."""
    built = build_runner_command("codex", PROMPT, workspace_path=tmp_path)
    assert claude_control_channel_argv(built, PROMPT) is None


def test_the_environment_arms_the_clis_own_recovery() -> None:
    """All three switches, or the CLI never asks.

    The entrypoint is the easiest of the three to get wrong: it looks like
    telemetry, and the CLI's own default value silently disarms mid-run
    recovery."""
    env = claude_control_channel_env()
    assert env["CLAUDE_CODE_SDK_HAS_OAUTH_REFRESH"] == "1"
    assert env["CLAUDE_CODE_ENTRYPOINT"] == CLAUDE_CONTROL_CHANNEL_ENTRYPOINT
    assert CLAUDE_CONTROL_CHANNEL_ENTRYPOINT in ("claude-desktop", "local-agent", "claude-vscode")
    # The CLI holds the rejected request open for this long waiting on us; the
    # dispenser's own budget is ~20s, so anything under it answers too late.
    assert int(env["CLAUDE_CODE_OAUTH_401_WAIT_MS"]) >= 25_000


# --- the recovery handler -------------------------------------------------


class _FakeDispenser:
    """Stands in for the dispenser: records the generations it was complained
    to about, answers from a script, and reports what the shared row holds."""

    def __init__(self, *responses: TokenResponse, stored: int | None = None) -> None:
        self._responses = list(responses)
        self.asked: list[int] = []
        # What `snapshot()` reports. None means "whatever this run last got",
        # i.e. nobody else has rotated — the ordinary case.
        self.stored = stored
        self._last_granted: int | None = None

    async def request(self, generation: int) -> TokenResponse:
        self.asked.append(generation)
        served = self._responses.pop(0)
        if isinstance(served, TokenGrant):
            self._last_granted = served.generation
        return served

    async def snapshot(self) -> tuple[str, int] | None:
        current = self.stored if self.stored is not None else self._last_granted
        return None if current is None else ("blob", current)


@dataclass
class _Stamps:
    seen: list[int]

    async def __call__(self, generation: int) -> None:
        self.seen.append(generation)


class _Clock:
    """A hand-wound monotonic clock, so a test can say how much later the
    agent asked again without waiting for it."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, secs: float) -> None:
        self.now += secs


def _recovery(
    dispenser: object, *, generation: int = 4, clock: _Clock | None = None
) -> tuple[ClaudeTokenRecovery, _Stamps]:
    stamps = _Stamps(seen=[])
    recovery = ClaudeTokenRecovery(
        dispenser,  # type: ignore[arg-type]
        run_id="run-1",
        generation=generation,
        restamp=stamps,
        now=clock or _Clock(),
    )
    return recovery, stamps


def _asked(subtype: str = OAUTH_TOKEN_REFRESH) -> ControlRequest:
    return ControlRequest(request_id="req-1", subtype=subtype, request={"subtype": subtype})


@pytest.mark.asyncio
async def test_a_rejected_token_is_replaced_and_the_run_restamped() -> None:
    """The run is handed a fresh token and re-stamped with the generation it
    now holds — the stamp is what a later, independent complaint names."""
    dispenser = _FakeDispenser(TokenGrant(token="tok-v5", generation=5, rotated=True))
    recovery, stamps = _recovery(dispenser, generation=4)

    assert await recovery(_asked()) == {"accessToken": "tok-v5"}
    assert dispenser.asked == [4]
    assert stamps.seen == [5]


@pytest.mark.asyncio
async def test_the_second_complaint_names_the_generation_the_run_now_holds() -> None:
    """A run rejected again much later must not name the generation it was
    dispatched with: the dispenser would read it as "nobody has rotated yet"
    and burn a rotation handing back the token that just failed."""
    dispenser = _FakeDispenser(
        TokenGrant(token="tok-v5", generation=5, rotated=True),
        TokenGrant(token="tok-v6", generation=6, rotated=True),
    )
    clock = _Clock()
    recovery, stamps = _recovery(dispenser, generation=4, clock=clock)

    await recovery(_asked())
    clock.advance(3600)
    assert await recovery(_asked()) == {"accessToken": "tok-v6"}
    assert dispenser.asked == [4, 5]
    assert stamps.seen == [5, 6]


@pytest.mark.asyncio
async def test_one_401s_burst_of_questions_costs_one_rotation() -> None:
    """The cascade this would otherwise recreate inside a single run.

    One 401 asked three times in ~1.2s in the SYM-232 spike. Each answer
    advances the generation this run names, so asking the dispenser again would
    find it naming the current one and rotate — and every rotation invalidates
    the token the previous one just handed over."""
    dispenser = _FakeDispenser(TokenGrant(token="tok-v5", generation=5, rotated=True))
    clock = _Clock()
    recovery, stamps = _recovery(dispenser, generation=4, clock=clock)

    answers = []
    for _ in range(3):
        answers.append(await recovery(_asked()))
        clock.advance(0.4)

    assert answers == [{"accessToken": "tok-v5"}] * 3
    assert dispenser.asked == [4]
    assert stamps.seen == [5]


@pytest.mark.asyncio
async def test_a_rotation_by_someone_else_ends_the_burst_early() -> None:
    """The window says a repeat is too soon to be a new rejection. It stops
    saying that once someone else has moved underneath us.

    Another run's rotation, or an operator reconnect, landing inside the window
    means the token this run holds really is superseded — so the repeat is a
    genuine second 401, and replaying the cached token would cost the run the
    recovery this ticket exists to give it. Naming an older generation gets a
    hand-out, so asking again is free."""
    dispenser = _FakeDispenser(
        TokenGrant(token="tok-v5", generation=5, rotated=True),
        TokenGrant(token="tok-v7", generation=7, rotated=False),
    )
    clock = _Clock()
    recovery, _ = _recovery(dispenser, generation=4, clock=clock)

    assert await recovery(_asked()) == {"accessToken": "tok-v5"}
    dispenser.stored = 7  # somebody else rotated while this run was adopting
    clock.advance(0.4)

    assert await recovery(_asked()) == {"accessToken": "tok-v7"}
    assert dispenser.asked == [4, 5]


@pytest.mark.asyncio
async def test_a_refusal_stays_a_refusal() -> None:
    """A connection nobody can rotate is not recoverable, and pretending
    otherwise costs the run its whole retry window before it dies anyway."""
    dispenser = _FakeDispenser(TokenRefusal("the claude connection is expired", permanent=True))
    recovery, stamps = _recovery(dispenser)

    assert await recovery(_asked()) is None
    assert stamps.seen == []
    # Permanently refused: the dispenser has already armed the reconnect gate,
    # so this run parks for an operator rather than coming straight back.
    assert not recovery.refused_retryably


@pytest.mark.asyncio
async def test_a_refusal_that_could_clear_on_its_own_is_marked_for_requeue() -> None:
    """A busy dispenser or an unreachable token endpoint leaves the connection
    untouched and still believed good. The run has to come back, not park —
    that is what it does today, and this ticket removes a cost, not a
    guarantee."""
    dispenser = _FakeDispenser(TokenRefusal("the claude token dispenser is busy", permanent=False))
    recovery, _ = _recovery(dispenser)

    assert await recovery(_asked()) is None
    assert recovery.refused_retryably


@pytest.mark.asyncio
async def test_a_question_this_host_does_not_answer_is_declined_not_fatal() -> None:
    """The CLI's control vocabulary is far wider than the one question this
    host advertises. An unrecognised one gets a proper error and the run
    carries on — killing a healthy implement run over a question would be a
    worse bug than the one this ticket fixes."""
    dispenser = _FakeDispenser()
    recovery, _ = _recovery(dispenser)

    assert isinstance(await recovery(_asked("can_use_tool")), Decline)
    assert dispenser.asked == []
    assert not recovery.refused_retryably


@pytest.mark.asyncio
async def test_recoveries_are_counted_and_said_out_loud(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without these lines there is no way to tell a mechanism that works from
    one that has never been exercised — there is no config flag to read."""
    dispenser = _FakeDispenser(
        TokenGrant(token="tok-v5", generation=5, rotated=True),
        TokenGrant(token="tok-v6", generation=6, rotated=True),
    )
    clock = _Clock()
    recovery, _ = _recovery(dispenser, clock=clock)
    with caplog.at_level(logging.INFO, logger="symphony.claude_token_recovery"):
        await recovery(_asked())
        clock.advance(3600)
        await recovery(_asked())
        recovery.log_tally()
    assert "recovery #1" in caplog.text
    assert "recovery #2" in caplog.text
    assert "generation 6" in caplog.text
    assert "finished after 2 mid-run claude token recovery(ies)" in caplog.text


@pytest.mark.asyncio
async def test_a_run_that_never_needed_a_token_says_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The common case is an armed run that is never at risk. Tallying those
    would drown the signal the tally exists to carry."""
    recovery, _ = _recovery(_FakeDispenser())
    with caplog.at_level(logging.INFO, logger="symphony.claude_token_recovery"):
        recovery.log_tally()
    assert caplog.text == ""


# --- the orchestrator seam ------------------------------------------------


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(
            ready="Todo", in_progress="In Progress", code_review="Needs Approval"
        ),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="",
        url="https://linear.app/x/issue/ENG-1",
        state_id="state-progress",
        state_name="In Progress",
        state_type="started",
        team_key="ENG",
        labels=["symphony"],
    )


def _cred(token: str) -> str:
    return json.dumps(
        {
            "claudeAiOauth": {
                "accessToken": token,
                "refreshToken": "rt-1",
                "expiresAt": 4102444800000,
            }
        }
    )


def _mints(token: str) -> respx.Route:
    """Let the shared connection rotate once, for real, through the dispenser."""
    return respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(
        return_value=httpx.Response(
            200, json={"access_token": token, "refresh_token": "rt-2", "expires_in": 28800}
        )
    )


def _control_request(
    request_id: str = "req-1", *, subtype: str = OAUTH_TOKEN_REFRESH
) -> RunnerEvent:
    return RunnerEvent(
        kind="stdout",
        line=json.dumps(
            {
                "type": "control_request",
                "request_id": request_id,
                "request": {"subtype": subtype},
            }
        ),
    )


def _done() -> list[RunnerEvent]:
    return [
        RunnerEvent(kind="stdout", line=json.dumps({"type": "result", "result": "SYMPHONY_DONE"})),
        RunnerEvent(kind="exit", returncode=0),
    ]


def _config(tmp_path: Path) -> Config:
    return Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        symphony_encryption_key=ENC_KEY,
        repos=[_binding()],
    )


async def _daemon(tmp_path: Path, *, connected: bool = True) -> Harness:
    """A daemon with a fake runner, a Claude connection to rotate, and one
    implement run already claimed — the state a dispatch starts from."""
    harness = await Harness.create(tmp_path, config=_config(tmp_path))
    if connected:
        await db.oauth_connections.set_connection(
            harness.conn,
            provider="claude",
            credential=_cred("tok-v1"),
            cipher=CredentialCipher(ENC_KEY),
        )
    await db.issues.upsert(
        harness.conn, id="iss-1", identifier="ENG-1", title="Add auth", team_key="ENG"
    )
    await db.runs.create(
        harness.conn,
        id="run-1",
        issue_id="iss-1",
        stage="implement",
        status="running",
        pid=None,
        started_at="2026-07-28T00:00:00+00:00",
    )
    harness.config.workspace_root.mkdir(parents=True, exist_ok=True)
    return harness


async def _dispatch(
    harness: Harness, *, agent: str = "claude", prompt: str | None = PROMPT
) -> tuple[str, int | None]:
    """One implement dispatch through the real run loop, on the fake runner."""
    workspace = harness.config.workspace_root
    _, final_kind, returncode = await harness.orch._run_stage_command(  # noqa: SLF001
        binding=_binding(),
        issue=_issue(),
        command=build_runner_command(agent, PROMPT, workspace_path=workspace),
        run_id="run-1",
        workspace_path=workspace,
        stage="implement",
        role=ResolvedRole(agent=agent),
        prior_total=0.0,
        prompt=prompt,
    )
    return final_kind, returncode


@pytest.mark.asyncio
async def test_an_implement_run_is_dispatched_as_a_conversation(tmp_path: Path) -> None:
    """The armed shape, end to end: prompt on stdin, the CLI told to read it,
    and the environment that makes a rejected token askable about."""
    harness = await _daemon(tmp_path)
    try:
        harness.runner.enqueue([RunnerEvent(kind="started", pid=1), *_done()])
        await _dispatch(harness)

        spec = harness.runner.specs[0]
        assert spec.conversation is not None
        assert spec.conversation.prompt == PROMPT
        assert PROMPT not in spec.command
        assert "--input-format" in spec.command
        assert spec.env["CLAUDE_CODE_SDK_HAS_OAUTH_REFRESH"] == "1"
        assert spec.env[CLAUDE_TOKEN_ENV] == "tok-v1"
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_run_whose_token_is_rejected_mid_flight_finishes(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The symptom this whole chain exists to remove: a run that asks for a
    replacement token mid-flight completes, instead of throwing away the work
    it had already done."""
    _mints("tok-v2")
    harness = await _daemon(tmp_path)
    try:
        harness.runner.enqueue([RunnerEvent(kind="started", pid=1), _control_request(), *_done()])
        with caplog.at_level(logging.INFO):
            final_kind, returncode = await _dispatch(harness)

        assert (final_kind, returncode) == ("exit", 0)
        # It rotated: the shared connection advanced a generation, and the run
        # re-stamped itself with the one it now holds. Without the re-stamp a
        # later, independent rejection would name a superseded generation.
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None
        assert await db.runs.claude_token_generation(harness.conn, "run-1") == status.generation
        assert "recovery #1" in caplog.text
        assert "finished after 1 mid-run claude token recovery" in caplog.text
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_control_traffic_never_reaches_the_runs_log(tmp_path: Path) -> None:
    """Control frames share a pipe with the agent's own output. A run that
    forwarded them would corrupt completion markers, cost accounting and
    verdict parsing at once — and it would not look like an auth bug."""
    _mints("tok-v2")
    harness = await _daemon(tmp_path)
    try:
        harness.runner.enqueue([RunnerEvent(kind="started", pid=1), _control_request(), *_done()])
        await _dispatch(harness)

        written = (harness.config.log_root / "run-1.log").read_text()
        assert "control_request" not in written
        assert "control_response" not in written
        assert "SYMPHONY_DONE" in written
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_run_that_cannot_recover_keeps_todays_ending(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A rotation the token endpoint rejects outright is unrecoverable. The
    run dies on auth and the connection is gated for an operator, exactly as it
    does today: this ticket removes a cost, not a guarantee."""
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(400, json={}))
    harness = await _daemon(tmp_path)
    try:
        harness.runner.enqueue(
            [
                RunnerEvent(kind="started", pid=1),
                _control_request(),
                RunnerEvent(kind="exit", returncode=1),
            ]
        )
        with caplog.at_level(logging.INFO):
            final_kind, returncode = await _dispatch(harness)

        # The run's own ending survives — a refusal must not overwrite what the
        # agent reported, or the state machine reads a refusal as a plain crash.
        assert (final_kind, returncode) == ("exit", 1)
        # And the escalation the operator acts on is the one that already
        # existed: the shared connection is gated until they reconnect.
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None
        assert status.status == "expired"
        assert "could not recover its claude token" in caplog.text
        # Nothing to tally: a refused run recovered nothing.
        assert "mid-run claude token recovery" not in caplog.text
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_bookkeeping_failure_does_not_cost_the_run_its_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The re-stamp is bookkeeping; the token is the point. A failed write must
    not turn a recoverable run into a dead one."""
    _mints("tok-v2")
    harness = await _daemon(tmp_path)
    try:
        real = db.runs.stamp_claude_token_generation
        calls = 0

        async def flaky(*args: object, **kwargs: object) -> None:
            # The dispatch stamp has to land — that is what arms the run at all.
            # It is the re-stamp *after* a grant that fails here.
            nonlocal calls
            calls += 1
            if calls > 1:
                raise RuntimeError("db is having a moment")
            await real(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(db.runs, "stamp_claude_token_generation", flaky)
        harness.runner.enqueue([RunnerEvent(kind="started", pid=1), _control_request(), *_done()])

        assert await _dispatch(harness) == ("exit", 0)
        assert calls == 2
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_a_codex_run_is_not_turned_into_a_conversation(tmp_path: Path) -> None:
    """Only the claude CLI speaks this protocol; codex keeps its own shape."""
    harness = await _daemon(tmp_path)
    try:
        harness.runner.enqueue([RunnerEvent(kind="started", pid=1), *_done()])
        await _dispatch(harness, agent="codex")
        assert harness.runner.specs[0].conversation is None
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_a_run_with_no_connection_to_rotate_stays_one_directional(tmp_path: Path) -> None:
    """A deployment on ambient host auth has no generation to name and no
    connection to rotate. Arming it would only add a question nobody can
    answer."""
    harness = await _daemon(tmp_path, connected=False)
    try:
        harness.runner.enqueue([RunnerEvent(kind="started", pid=1), *_done()])
        await _dispatch(harness)
        spec = harness.runner.specs[0]
        assert spec.conversation is None
        assert PROMPT in spec.command
        assert "CLAUDE_CODE_SDK_HAS_OAUTH_REFRESH" not in spec.env
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_a_caller_that_passes_no_prompt_is_untouched(tmp_path: Path) -> None:
    """How local review, acceptance and the merge pass stay out of this: they
    never pass a prompt, so their dispatch is byte-for-byte what it was.
    Holding them back is the point — the mode proves itself first."""
    harness = await _daemon(tmp_path)
    try:
        harness.runner.enqueue([RunnerEvent(kind="started", pid=1), *_done()])
        await _dispatch(harness, prompt=None)
        spec = harness.runner.specs[0]
        assert spec.conversation is None
        assert spec.command[-2:] == ["--", PROMPT]
        assert "CLAUDE_CODE_SDK_HAS_OAUTH_REFRESH" not in spec.env
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_retryably_refused_run_is_marked_for_requeue(tmp_path: Path) -> None:
    """The failure path the spec insists must not change: a run the dispenser
    could not serve *for now* comes back on its own.

    Armed, the channel answers with an error and the runner ends the run within
    seconds — usually before the agent has printed anything the log classifier
    would recognise as an auth failure. Without an explicit verdict the issue
    would park for an operator, where today it simply requeues."""
    # A token endpoint that never answers cleanly: the dispenser exhausts its
    # budget and refuses retryably, leaving the connection untouched.
    respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(503, json={}))
    harness = await _daemon(tmp_path)
    try:
        harness.orch.claude_token_dispenser = ClaudeTokenDispenser(
            harness.conn,
            CredentialCipher(ENC_KEY),
            CredentialWriteBack(harness.conn, CredentialCipher(ENC_KEY)),
            budget_secs=0.01,
            retry_backoff_secs=0.01,
        )
        harness.runner.enqueue(
            [
                RunnerEvent(kind="started", pid=1),
                _control_request(),
                RunnerEvent(kind="exit", returncode=1),
            ]
        )
        assert await _dispatch(harness) == ("exit", 1)

        # The connection is untouched — a flaky endpoint must not black out the
        # fleet — and this run is flagged to come back.
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None
        assert status.status == "connected"
        assert await harness.orch._claude_auth_requeue_signal(  # noqa: SLF001
            "claude", None, run_id="run-1"
        )
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_a_question_this_host_does_not_answer_does_not_kill_the_run(
    tmp_path: Path,
) -> None:
    """End to end: an unrecognised control request is answered with an error
    and the run finishes normally, with nothing tallied and nothing requeued."""
    harness = await _daemon(tmp_path)
    try:
        harness.runner.enqueue(
            [
                RunnerEvent(kind="started", pid=1),
                _control_request(subtype="can_use_tool"),
                *_done(),
            ]
        )
        assert await _dispatch(harness) == ("exit", 0)
        assert not await harness.orch._claude_auth_requeue_signal(  # noqa: SLF001
            "claude", None, run_id="run-1"
        )
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_a_binding_that_brings_its_own_token_is_left_alone(tmp_path: Path) -> None:
    """A binding supplying `CLAUDE_CODE_OAUTH_TOKEN` through `env:` is not
    authenticating as the UI-connected account — and `binding.env` wins the
    merge. Arming that run would let a mid-flight rejection rotate an account
    it never used and hand it a stranger's token."""
    harness = await _daemon(tmp_path)
    try:
        binding = _binding().model_copy(update={"env": {CLAUDE_TOKEN_ENV: "byo-token"}})
        harness.runner.enqueue([RunnerEvent(kind="started", pid=1), *_done()])
        await harness.orch._run_stage_command(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            command=build_runner_command(
                "claude", PROMPT, workspace_path=harness.config.workspace_root
            ),
            run_id="run-1",
            workspace_path=harness.config.workspace_root,
            stage="implement",
            role=ResolvedRole(agent="claude"),
            prior_total=0.0,
            prompt=PROMPT,
        )
        spec = harness.runner.specs[0]
        assert spec.conversation is None
        assert spec.env[CLAUDE_TOKEN_ENV] == "byo-token"
    finally:
        await harness.close()


@pytest.mark.asyncio
@respx.mock
async def test_a_retryable_refusal_stops_the_tail_re_validating(tmp_path: Path) -> None:
    """The regression arming would otherwise introduce, and the reason the
    verdict is recorded before the runner tail reads the log.

    A refused run does print a 401 — that is why the agent asked in the first
    place. Left alone, the tail would classify it and re-validate: a second
    exchange against the endpoint the dispenser has just found unreachable,
    moments later, failing the same way — and the tail expires the shared row
    on that, gating every other run. One blip, fleet-wide outage: the SYM-234
    cascade, re-entered through the back door."""
    endpoint = respx.post(CLAUDE_OAUTH_TOKEN_URL).mock(return_value=httpx.Response(503, json={}))
    harness = await _daemon(tmp_path)
    try:
        harness.orch.claude_token_dispenser = ClaudeTokenDispenser(
            harness.conn,
            CredentialCipher(ENC_KEY),
            CredentialWriteBack(harness.conn, CredentialCipher(ENC_KEY)),
            budget_secs=0.01,
            retry_backoff_secs=0.01,
        )
        harness.runner.enqueue(
            [
                RunnerEvent(kind="started", pid=1),
                _control_request(),
                RunnerEvent(
                    kind="stdout",
                    line=json.dumps(
                        {"type": "result", "is_error": True, "result": "Not logged in"}
                    ),
                ),
                RunnerEvent(kind="exit", returncode=1),
            ]
        )
        assert await _dispatch(harness) == ("exit", 1)

        # One exchange, the dispenser's own — no second one from the tail. The
        # connection is untouched, and the run comes back rather than parking
        # for an operator who has nothing to fix.
        assert endpoint.call_count == 1
        status = await db.oauth_connections.get_status(harness.conn, "claude")
        assert status is not None
        assert status.status == "connected"
        assert await harness.orch._claude_auth_requeue_signal(  # noqa: SLF001
            "claude", None, run_id="run-1"
        )
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_a_binding_that_overrides_a_control_variable_is_left_alone(
    tmp_path: Path,
) -> None:
    """The three control variables are only correct together, and `binding.env`
    wins the merge. A binding zeroing the 401 wait would leave a run armed on
    this side — prompt on stdin, handler attached — and deaf on the other,
    losing recovery silently. Silence is the problem: a run that never asks
    looks exactly like a run that never needed to."""
    harness = await _daemon(tmp_path)
    try:
        binding = _binding().model_copy(update={"env": {"CLAUDE_CODE_OAUTH_401_WAIT_MS": "0"}})
        harness.runner.enqueue([RunnerEvent(kind="started", pid=1), *_done()])
        await harness.orch._run_stage_command(  # noqa: SLF001
            binding=binding,
            issue=_issue(),
            command=build_runner_command(
                "claude", PROMPT, workspace_path=harness.config.workspace_root
            ),
            run_id="run-1",
            workspace_path=harness.config.workspace_root,
            stage="implement",
            role=ResolvedRole(agent="claude"),
            prior_total=0.0,
            prompt=PROMPT,
        )
        spec = harness.runner.specs[0]
        assert spec.conversation is None
        assert PROMPT in spec.command
        assert "CLAUDE_CODE_SDK_HAS_OAUTH_REFRESH" not in spec.env
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_a_binding_with_its_own_anthropic_key_is_left_alone(tmp_path: Path) -> None:
    """The CLI prefers `ANTHROPIC_API_KEY` over the OAuth token, and the runner
    deliberately preserves a binding's own copy. Such a run is not the
    UI-connected account, so a 401 against that private credential must not
    rotate the shared one and answer with a token from it."""
    for key in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"):
        harness = await _daemon(tmp_path / key)
        try:
            binding = _binding().model_copy(update={"env": {key: "sk-private"}})
            harness.runner.enqueue([RunnerEvent(kind="started", pid=1), *_done()])
            await harness.orch._run_stage_command(  # noqa: SLF001
                binding=binding,
                issue=_issue(),
                command=build_runner_command(
                    "claude", PROMPT, workspace_path=harness.config.workspace_root
                ),
                run_id="run-1",
                workspace_path=harness.config.workspace_root,
                stage="implement",
                role=ResolvedRole(agent="claude"),
                prior_total=0.0,
                prompt=PROMPT,
            )
            spec = harness.runner.specs[0]
            assert spec.conversation is None, key
            assert PROMPT in spec.command, key
        finally:
            await harness.close()
