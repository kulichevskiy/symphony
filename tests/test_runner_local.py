"""Light tests for LocalRunner.

We exercise the happy path (echo command exits 0 with stdout) and the
stall path (sleep longer than stall_secs is killed). Real agent JSON
parsing is exercised in iteration 4+ once the prompt + parser exist.
"""

from __future__ import annotations

import asyncio
import json
import os.path
import sys
from pathlib import Path

import pytest

from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.agent.runners.local import LocalRunner
from symphony.credentials import RunCredentials


def _path_exists(path: Path) -> bool:
    # Sync helper so the filesystem check isn't flagged inside an async test.
    return os.path.exists(path)


@pytest.mark.asyncio
async def test_runner_streams_stdout_and_exits_clean(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r1",
        workspace_path=tmp_path,
        command=["sh", "-c", "printf 'hello\\nworld\\n'; exit 0"],
        stall_secs=10,
    )
    events = [ev async for ev in runner.run(spec)]
    started = [e for e in events if e.kind == "started"]
    assert started and started[0].pid is not None
    stdout = [e.line for e in events if e.kind == "stdout"]
    assert stdout == ["hello", "world"]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_drains_tail_output_after_process_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_read = asyncio.StreamReader.read

    async def delayed_read(self: asyncio.StreamReader, n: int = -1) -> bytes:
        await asyncio.sleep(0.4)
        return await original_read(self, n)

    monkeypatch.setattr(asyncio.StreamReader, "read", delayed_read)
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-tail",
        workspace_path=tmp_path,
        command=["sh", "-c", "printf 'tail\\n'"],
        stall_secs=10,
    )
    events = [ev async for ev in runner.run(spec)]
    stdout = [e.line for e in events if e.kind == "stdout"]
    assert stdout == ["tail"]


@pytest.mark.asyncio
async def test_runner_exits_when_child_holds_pipe_open(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-held-pipe",
        workspace_path=tmp_path,
        command=[
            sys.executable,
            "-c",
            ("import subprocess; print('tail', flush=True); subprocess.Popen(['sleep', '30'])"),
        ],
        stall_secs=60,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=10)
    stdout = [e.line for e in events if e.kind == "stdout"]
    assert stdout == ["tail"]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_waits_when_process_closes_stdio_before_exit(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-closed-stdio",
        workspace_path=tmp_path,
        command=["sh", "-c", "exec >/dev/null 2>/dev/null; sleep 30"],
        stall_secs=1,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=10)
    assert events[-1].kind == "stall_timeout"
    assert not any(e.kind == "exit" and e.returncode is None for e in events)


@pytest.mark.asyncio
async def test_runner_kills_on_stall(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r2",
        workspace_path=tmp_path,
        command=["sh", "-c", "sleep 30"],
        stall_secs=1,
    )
    kinds: list[str] = []
    async for ev in runner.run(spec):
        kinds.append(ev.kind)
        if ev.kind in ("exit", "stall_timeout"):
            break
    assert "stall_timeout" in kinds


@pytest.mark.asyncio
async def test_runner_materializes_credentials_into_private_home_torn_down_after(
    tmp_path: Path,
) -> None:
    # A run with resolved credentials gets a private, torn-down credential home:
    # GH_TOKEN + a git credential store are visible to the subprocess for the
    # duration of the run, then removed — never a persistent volume file.
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-creds",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            'printf "%s\\n" "$GH_TOKEN"; printf "%s\\n" "$GIT_CONFIG_GLOBAL"; '
            'cat "$GIT_CONFIG_GLOBAL"',
        ],
        stall_secs=10,
        credentials=RunCredentials(github_token="gho_run", linear_token="lin_run"),
    )
    events = [ev async for ev in runner.run(spec)]
    stdout = [e.line for e in events if e.kind == "stdout"]
    assert stdout[0] == "gho_run"
    gitconfig = Path(stdout[1])
    assert "helper = store" in "\n".join(stdout[2:])
    # Torn down after the run — the private home no longer exists on disk.
    assert not _path_exists(gitconfig)
    assert not _path_exists(gitconfig.parent)


@pytest.mark.asyncio
async def test_runner_binding_gh_token_overrides_materialized_git_credential_helper(
    tmp_path: Path,
) -> None:
    # A binding that supplies its own GH_TOKEN via `env:` must win for *every*
    # consumer, not just env-reading ones like `gh` — including the git
    # credential helper materialized from the DB/volume-resolved token. Not
    # materializing anything would leave `git push` on whatever the ambient
    # host state already provides, silently ignoring the binding's override;
    # instead the binding's own token gets materialized (SYM-199 review fix).
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-binding-token",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            'printf "%s\\n" "$GH_TOKEN"; printf "%s\\n" "${GIT_CONFIG_GLOBAL:-<unset>}"; '
            'cat "$(dirname "$GIT_CONFIG_GLOBAL")/.git-credentials"',
        ],
        stall_secs=10,
        env={"GH_TOKEN": "binding_own_token"},
        credentials=RunCredentials(github_token="gho_run", linear_token="lin_run"),
    )
    events = [ev async for ev in runner.run(spec)]
    stdout = [e.line for e in events if e.kind == "stdout"]
    assert stdout[0] == "binding_own_token"
    assert stdout[1] != "<unset>"
    assert "binding_own_token" in stdout[2]
    assert "gho_run" not in stdout[2]


@pytest.mark.asyncio
async def test_runner_preserves_global_git_identity_with_materialized_creds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Materializing creds replaces GIT_CONFIG_GLOBAL for the run. If that
    # doesn't also preserve the pre-existing global config, the container's
    # `user.name`/`user.email` (set --global in the Dockerfile, no auto-detect
    # in the headless container) is silently dropped and `git commit` either
    # mis-attributes or fails outright the moment a GitHub connection exists.
    fake_home = tmp_path / "home"
    fake_home.mkdir()
    (fake_home / ".gitconfig").write_text(
        "[user]\n\tname = Symphony\n\temail = symphony@localhost\n", encoding="utf-8"
    )
    monkeypatch.setenv("HOME", str(fake_home))

    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "file.txt").write_text("hi", encoding="utf-8")

    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-identity",
        workspace_path=repo,
        command=[
            "sh",
            "-c",
            "git init -q && git add file.txt && git commit -q -m test "
            "&& git log -1 --format='%an <%ae>'",
        ],
        stall_secs=10,
        credentials=RunCredentials(github_token="gho_run"),
    )
    events = [ev async for ev in runner.run(spec)]
    stdout = [e.line for e in events if e.kind == "stdout"]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0
    assert stdout[-1] == "Symphony <symphony@localhost>"


@pytest.mark.asyncio
async def test_runner_cleans_up_cred_home_when_materialization_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # If materializing creds raises (disk full, permission error), the
    # plaintext temp dir must not be left on disk and the run must not
    # silently proceed without creds — the exception propagates.
    import symphony.agent.runners.local as local_mod

    created_homes: list[str] = []
    real_mkdtemp = local_mod.tempfile.mkdtemp

    def _tracking_mkdtemp(*args: object, **kwargs: object) -> str:
        home = real_mkdtemp(*args, **kwargs)  # type: ignore[arg-type]
        created_homes.append(home)
        return home

    def _boom(*args: object, **kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(local_mod.tempfile, "mkdtemp", _tracking_mkdtemp)
    monkeypatch.setattr(local_mod, "materialize_credentials", _boom)

    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-creds-fail",
        workspace_path=tmp_path,
        command=["true"],
        stall_secs=10,
        credentials=RunCredentials(github_token="gho_run"),
    )
    with pytest.raises(OSError, match="disk full"):
        async for _ in runner.run(spec):
            pass

    assert len(created_homes) == 1
    assert not _path_exists(Path(created_homes[0]))


@pytest.mark.asyncio
async def test_runner_does_not_stall_while_command_in_flight(tmp_path: Path) -> None:
    # Agent emits item.started for a command_execution, then is silent past
    # stall_secs while its tool runs, then emits item.completed and exits.
    # The in-flight command must extend the deadline to command_secs so the
    # healthy run is not killed as a false-positive stall (the 2026-05-26
    # incident: a broad rg ran >stall_secs with no agent output).
    started = '{"type":"item.started","item":{"id":"c1","type":"command_execution"}}'
    completed = '{"type":"item.completed","item":{"id":"c1","type":"command_execution"}}'
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-inflight",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            f"printf '%s\\n' '{started}'; sleep 3; printf '%s\\n' '{completed}'; exit 0",
        ],
        stall_secs=1,
        command_secs=30,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=15)
    kinds = [e.kind for e in events]
    assert "stall_timeout" not in kinds
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_delivers_large_command_completion_line(tmp_path: Path) -> None:
    script = """
import json
import sys
import time

started = {"type": "item.started", "item": {"id": "c-large", "type": "command_execution"}}
completed = {
    "type": "item.completed",
    "item": {
        "id": "c-large",
        "type": "command_execution",
        "aggregated_output": "x" * (17 * 1024 * 1024),
    },
}
for event in (started, completed):
    sys.stdout.write(json.dumps(event) + "\\n")
    sys.stdout.flush()
time.sleep(4.0)
"""
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-large-completion",
        workspace_path=tmp_path,
        command=[sys.executable, "-c", script],
        stall_secs=5,
        command_secs=3,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=20)

    stdout_lines = [e.line for e in events if e.kind == "stdout"]
    completed_lines = [
        line
        for line in stdout_lines
        if (payload := json.loads(line)).get("type") == "item.completed"
        and payload.get("item", {}).get("id") == "c-large"
    ]
    assert completed_lines
    assert len(completed_lines[0].encode()) > 16 * 1024 * 1024
    assert "stall_timeout" not in [e.kind for e in events]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_recovers_when_stream_reports_oversized_line(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _ValueErrorThenPayloadStream:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload
            self._offset = 0
            self._raised = False

        async def read(self, n: int = -1) -> bytes:
            if not self._raised:
                self._raised = True
                raise ValueError("Separator is not found, and chunk exceed the limit")
            if self._offset >= len(self._payload):
                return b""
            if n < 0:
                n = len(self._payload) - self._offset
            chunk = self._payload[self._offset : self._offset + n]
            self._offset += len(chunk)
            return chunk

    class _FakeProcess:
        stdout = _ValueErrorThenPayloadStream(b"x" * (4 * 1024 * 1024 + 1) + b"\nnormal\n")
        stderr = None
        pid = None
        returncode: int | None = None

        async def wait(self) -> int:
            self.returncode = 0
            return 0

    async def fake_create_subprocess_exec(*_args: object, **_kwargs: object) -> _FakeProcess:
        return _FakeProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-overrun",
        workspace_path=tmp_path,
        command=["fake-agent"],
        stall_secs=10,
    )

    with caplog.at_level("WARNING", logger="symphony.agent.runners.local"):
        events = await asyncio.wait_for(_collect_events(runner, spec), timeout=10)

    assert [e.line for e in events if e.kind == "stdout"] == ["normal"]
    assert "stall_timeout" not in [e.kind for e in events]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0
    assert "skipping oversized stdout line" in caplog.text


@pytest.mark.asyncio
async def test_runner_resets_command_heartbeat_for_skipped_oversized_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = {
        "type": "item.started",
        "item": {"id": "c-overrun", "type": "command_execution"},
    }
    completed = {
        "type": "item.completed",
        "item": {
            "id": "c-overrun",
            "type": "command_execution",
            "aggregated_output": "x" * (4 * 1024 * 1024 + 1),
        },
    }

    class _CommandCompletionOverrunStream:
        def __init__(self) -> None:
            self._payload = json.dumps(completed).encode() + b"\nafter\n"
            self._offset = 0
            self._read_started = False
            self._raised = False

        async def read(self, n: int = -1) -> bytes:
            if not self._read_started:
                self._read_started = True
                return json.dumps(started).encode() + b"\n"
            if not self._raised:
                self._raised = True
                raise ValueError("Separator is not found, and chunk exceed the limit")
            if self._offset >= len(self._payload):
                return b""
            if n < 0:
                n = len(self._payload) - self._offset
            chunk = self._payload[self._offset : self._offset + n]
            self._offset += len(chunk)
            return chunk

    class _FakeProcess:
        def __init__(self) -> None:
            self.stdout = _CommandCompletionOverrunStream()
            self.stderr = None
            self.pid = 123456
            self.returncode: int | None = None
            self._wait_task: asyncio.Task[int] | None = None

        async def wait(self) -> int:
            if self._wait_task is None:
                self._wait_task = asyncio.create_task(self._complete())
            return await self._wait_task

        async def _complete(self) -> int:
            await asyncio.sleep(1.4)
            self.returncode = 0
            return 0

    async def fake_create_subprocess_exec(*_args: object, **_kwargs: object) -> _FakeProcess:
        return _FakeProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr("symphony.agent.runners.local._pid_alive", lambda _pid: True)
    monkeypatch.setattr("symphony.agent.runners.local._terminate_process_group", lambda _pid: None)

    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-overrun-completion",
        workspace_path=tmp_path,
        command=["fake-agent"],
        stall_secs=10,
        command_secs=0.2,
    )

    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=10)

    assert [e.line for e in events if e.kind == "stdout"] == [
        json.dumps(started),
        "after",
    ]
    assert "stall_timeout" not in [e.kind for e in events]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_recognises_legacy_command_execution_shape(tmp_path: Path) -> None:
    # Older codex builds emit `item_type` (on the item or on the event) instead
    # of `item.type`. activity.parse_codex_activity_line already accepts that;
    # the watchdog must too, or a legacy-shape tool call falls back to
    # stall_secs and gets killed by the very false-positive this PR fixes.
    started = '{"type":"item.started","item":{"id":"c1","item_type":"command_execution"}}'
    completed = '{"type":"item.completed","item":{"id":"c1","item_type":"command_execution"}}'
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-legacy",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            f"printf '%s\\n' '{started}'; sleep 3; printf '%s\\n' '{completed}'; exit 0",
        ],
        stall_secs=1,
        command_secs=30,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=15)
    kinds = [e.kind for e in events]
    assert "stall_timeout" not in kinds
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_stalls_when_command_exceeds_command_secs(tmp_path: Path) -> None:
    # The in-flight extension is bounded: a command that never completes
    # within command_secs is still killed (backstop against a deadlocked
    # agent that emitted item.started but hangs forever).
    #
    # command_secs < stall_secs so the cap, not the stall window, must be
    # the binding deadline. With the prior `max(stall, command)` formula
    # this test passed only because `sleep 30` outran stall_secs too.
    started = '{"type":"item.started","item":{"id":"c1","type":"command_execution"}}'
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-inflight-cap",
        workspace_path=tmp_path,
        command=["sh", "-c", f"printf '%s\\n' '{started}'; sleep 8"],
        stall_secs=30,
        command_secs=1,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=15)
    assert events[-1].kind == "stall_timeout"


@pytest.mark.asyncio
async def test_runner_kills_on_wall_clock_cap_despite_fresh_output(tmp_path: Path) -> None:
    # A confused-but-chatty agent keeps emitting output, so the silence-based
    # stall watchdog never trips and no command is in flight to hit command_secs.
    # The absolute wall-clock backstop must still terminate it (SYM-153 /
    # incident SYM-148: a review_fix agent looped for 28+ minutes).
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-wallclock",
        workspace_path=tmp_path,
        command=["sh", "-c", "while true; do printf 'tick\\n'; sleep 0.2; done"],
        stall_secs=30,
        command_secs=30,
        wall_clock_secs=2,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=20)
    kinds = [e.kind for e in events]
    assert "wall_clock_timeout" in kinds
    assert "stall_timeout" not in kinds
    # The agent was chatty right up to the kill — fresh output kept the
    # heartbeat alive, which is exactly why the stall watchdog couldn't help.
    assert any(e.kind == "stdout" and e.line == "tick" for e in events)


@pytest.mark.asyncio
async def test_runner_wall_clock_cap_disabled_by_default(tmp_path: Path) -> None:
    # Unset (0) means no absolute cap: a short clean run exits normally.
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-no-wallclock",
        workspace_path=tmp_path,
        command=["sh", "-c", "printf 'hi\\n'; exit 0"],
        stall_secs=10,
    )
    events = await asyncio.wait_for(_collect_events(runner, spec), timeout=10)
    assert "wall_clock_timeout" not in [e.kind for e in events]
    exits = [e for e in events if e.kind == "exit"]
    assert exits and exits[0].returncode == 0


@pytest.mark.asyncio
async def test_runner_kill_terminates(tmp_path: Path) -> None:
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r3",
        workspace_path=tmp_path,
        command=["sh", "-c", "sleep 30"],
        stall_secs=60,
    )

    async def consume() -> list[str]:
        out: list[str] = []
        async for ev in runner.run(spec):
            out.append(ev.kind)
            if ev.kind in ("exit", "stall_timeout"):
                break
        return out

    consumer = asyncio.create_task(consume())
    # Give the subprocess a moment to actually spawn.
    await asyncio.sleep(0.2)
    await runner.kill("r3")
    kinds = await asyncio.wait_for(consumer, timeout=10)
    assert "exit" in kinds


async def _collect_events(runner: LocalRunner, spec: RunnerSpec) -> list[RunnerEvent]:
    return [ev async for ev in runner.run(spec)]


@pytest.mark.asyncio
async def test_runner_scrubs_symphony_env_from_agent(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Deployment flags (SYMPHONY_*) must not leak into agent subprocesses: an
    agent working on this repo would otherwise inherit e.g.
    SYMPHONY_REQUIRE_AUTH0=1 from the Coolify daemon and its own test runs of
    unauthenticated UI mode would fail. Per-binding spec.env still wins."""
    monkeypatch.setenv("SYMPHONY_REQUIRE_AUTH0", "1")
    monkeypatch.setenv("SYMPHONY_DEPLOY_FLAG", "x")
    monkeypatch.setenv("UNRELATED_VAR", "keep-me")
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-env",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            'printf "%s|%s|%s|%s\\n" "${SYMPHONY_REQUIRE_AUTH0:-unset}" '
            '"${SYMPHONY_DEPLOY_FLAG:-unset}" "${UNRELATED_VAR:-unset}" '
            '"${FROM_SPEC:-unset}"',
        ],
        stall_secs=10,
        env={"FROM_SPEC": "spec-wins"},
    )
    events = [ev async for ev in runner.run(spec)]
    lines = [ev.line for ev in events if ev.kind == "stdout"]
    assert lines == ["unset|unset|keep-me|spec-wins"]


@pytest.mark.asyncio
async def test_runner_scrubs_ambient_claude_auth_when_config_dir_materialized(
    tmp_path: Path,
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """When a run carries a materialized CLAUDE_CONFIG_DIR (a DB-resolved Claude
    credential), the daemon host's ambient ANTHROPIC_API_KEY / CLAUDE_CODE_OAUTH_TOKEN
    must NOT leak into the child — Claude Code prefers an ambient token over the
    on-disk credential, so the host key would silently win over the UI-connected
    account (the SYM-206 hazard). GitHub/Linear env is unaffected (SYM-215 scope)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "host-key")
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "host-oauth")
    monkeypatch.setenv("LINEAR_API_KEY", "keep-linear")
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-claude-scrub",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            'printf "%s|%s|%s|%s\\n" "${ANTHROPIC_API_KEY:-unset}" '
            '"${CLAUDE_CODE_OAUTH_TOKEN:-unset}" "${LINEAR_API_KEY:-unset}" '
            '"${CLAUDE_CONFIG_DIR:-unset}"',
        ],
        stall_secs=10,
        env={"CLAUDE_CONFIG_DIR": str(tmp_path / "cfg")},
    )
    events = [ev async for ev in runner.run(spec)]
    lines = [ev.line for ev in events if ev.kind == "stdout"]
    assert lines == [f"unset|unset|keep-linear|{tmp_path / 'cfg'}"]


@pytest.mark.asyncio
async def test_runner_scrubs_ambient_codex_auth_when_codex_home_materialized(
    tmp_path: Path,
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """CODEX_HOME materialized → host OPENAI_API_KEY must not leak; codex prefers
    the ambient API key over auth.json in CODEX_HOME."""
    monkeypatch.setenv("OPENAI_API_KEY", "host-openai")
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-codex-scrub",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            'printf "%s|%s\\n" "${OPENAI_API_KEY:-unset}" "${CODEX_HOME:-unset}"',
        ],
        stall_secs=10,
        env={"CODEX_HOME": str(tmp_path / "codex")},
    )
    events = [ev async for ev in runner.run(spec)]
    lines = [ev.line for ev in events if ev.kind == "stdout"]
    assert lines == [f"unset|{tmp_path / 'codex'}"]


@pytest.mark.asyncio
async def test_runner_keeps_ambient_agent_auth_without_materialized_cred(
    tmp_path: Path,
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """No materialized agent credential (no CLAUDE_CONFIG_DIR / CODEX_HOME in
    spec.env) → ambient agent-auth env is left untouched. The scrub is scoped to
    UI-backed runs; a binding running purely on ambient host auth still works."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "host-key")
    monkeypatch.setenv("OPENAI_API_KEY", "host-openai")
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-no-mat",
        workspace_path=tmp_path,
        command=[
            "sh",
            "-c",
            'printf "%s|%s\\n" "${ANTHROPIC_API_KEY:-unset}" "${OPENAI_API_KEY:-unset}"',
        ],
        stall_secs=10,
    )
    events = [ev async for ev in runner.run(spec)]
    lines = [ev.line for ev in events if ev.kind == "stdout"]
    assert lines == ["host-key|host-openai"]


@pytest.mark.asyncio
async def test_runner_binding_agent_auth_override_survives_scrub(
    tmp_path: Path,
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """A binding that sets ANTHROPIC_API_KEY explicitly via `env:` still wins even
    when CLAUDE_CONFIG_DIR is materialized — the scrub only strips *inherited*
    ambient auth, never a per-binding override."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "host-key")
    runner = LocalRunner()
    spec = RunnerSpec(
        run_id="r-binding-override",
        workspace_path=tmp_path,
        command=["sh", "-c", 'printf "%s\\n" "${ANTHROPIC_API_KEY:-unset}"'],
        stall_secs=10,
        env={
            "CLAUDE_CONFIG_DIR": str(tmp_path / "cfg"),
            "ANTHROPIC_API_KEY": "binding-key",
        },
    )
    events = [ev async for ev in runner.run(spec)]
    lines = [ev.line for ev in events if ev.kind == "stdout"]
    assert lines == ["binding-key"]
