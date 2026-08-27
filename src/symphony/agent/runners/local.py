"""LocalRunner — subprocess on the orchestrator host.

Mirrors the load-bearing parts of the Rust `agent/process.rs`:

- Separate stdout / stderr pumps that notify a stall watchdog on every line.
- PID-based liveness for the watchdog (status fields lie during fix-runs;
  PIDs don't — see docs/python-port-research.md §13.1).
- SIGTERM on stall, then SIGKILL after a short grace period. The Rust code
  only sends SIGTERM; in Python we add the grace+kill because asyncio
  doesn't always reap zombies cleanly otherwise.
- `kill()` is callable from another coroutine for `$stop` and shutdown.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
from collections.abc import AsyncIterator
from contextlib import suppress
from pathlib import Path
from typing import Any

from ...credentials import RunCredentials, materialize_credentials
from ..control_channel import ControlChannel
from ..runner import RunnerEvent, RunnerSpec

_STREAM_DRAIN_SECS = 2.0
# How long a refused run gets to end its own turn before it is terminated.
_REFUSAL_GRACE_SECS = 2.0
# How long a terminated process group gets before SIGKILL.
_TERMINATE_GRACE_SECS = 5.0
_WATCHDOG_POLL_SECS = 1.0
_SUBPROCESS_BUFFER_LIMIT = 4 * 1024 * 1024
_STREAM_READ_CHUNK_BYTES = 64 * 1024
_OVERSIZED_LINE_PREFIX_BYTES = 64 * 1024
_JSON_ID_RE = re.compile(r'"(?:id|item_id)"\s*:\s*"([^"]+)"')

# When a run is backed by a DB-materialized agent credential (a claude bearer
# token in `CLAUDE_CODE_OAUTH_TOKEN`, or a private CODEX_HOME, both written
# from `oauth_connections`), the daemon host's own ambient agent-auth env must
# NOT leak into the child: Claude Code and codex both prefer an ambient
# API-key env var over the credential we hand them, so a stray host
# `ANTHROPIC_API_KEY` would silently win over the UI-connected account (the
# SYM-206 hazard). We scrub these from the *inherited* env only — a binding
# that sets one explicitly via `env:` (landing in `spec.env`) still overrides,
# as always.
#
# The marker for claude is the token itself (SYM-233): the run no longer gets a
# config dir to key off, and the token env var it does get is the one thing an
# inherited value could shadow — which the merge already settles, since
# `spec.env` wins.
# Named separately because the dispatch path needs the same list: a binding
# that sets one of these is authenticating the run as something other than the
# UI-connected account, which decides whether mid-run token recovery can be
# armed at all (SYM-236).
CLAUDE_AMBIENT_AUTH_ENV: tuple[str, ...] = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")

_AGENT_AMBIENT_AUTH_ENV: dict[str, tuple[str, ...]] = {
    "CLAUDE_CODE_OAUTH_TOKEN": CLAUDE_AMBIENT_AUTH_ENV,
    "CODEX_HOME": ("OPENAI_API_KEY", "CODEX_API_KEY"),
}


def _scrub_ambient_agent_auth(inherited: dict[str, str], spec_env: dict[str, str]) -> None:
    """Drop host ambient agent-auth vars from `inherited` in place when the
    run carries a materialized agent credential, so the DB credential wins.

    A key that `spec_env` supplies itself is left for the later merge to
    override — this only strips values inherited from the daemon process."""
    for marker, ambient_keys in _AGENT_AMBIENT_AUTH_ENV.items():
        if marker not in spec_env:
            continue
        for key in ambient_keys:
            if key not in spec_env:
                inherited.pop(key, None)


log = logging.getLogger(__name__)


class _Heartbeat:
    """Tracks runner liveness for the stall watchdog.

    `last_line` is the monotonic time of the most recent stdout/stderr line.
    `_cmd_starts` maps an in-flight codex `command_execution` item id to the
    time it started, so the watchdog can extend its deadline while a tool
    call is genuinely running (the agent emits no output in that window).
    """

    def __init__(self, last_line: float) -> None:
        self.last_line = last_line
        self._cmd_starts: dict[str, float] = {}

    def observe(self, line: str) -> None:
        """Parse one codex JSON-stream line and track command_execution spans.

        Accepts both the canonical shape (`item.type == "command_execution"`)
        and the legacy shape that puts `item_type` on the item or the outer
        event — same fields `activity.parse_codex_activity_line` recognises.
        """
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            return
        if not isinstance(event, dict):
            return
        kind = event.get("type")
        if kind not in ("item.started", "item.completed"):
            return
        raw_item = event.get("item")
        item: dict[str, object] = raw_item if isinstance(raw_item, dict) else {}
        item_type = item.get("type") or item.get("item_type") or event.get("item_type")
        if item_type != "command_execution":
            return
        item_id = item.get("id") or event.get("item_id") or event.get("id")
        if not isinstance(item_id, str):
            return
        if kind == "item.started":
            self._cmd_starts.setdefault(item_id, self.last_line)
        else:  # item.completed
            self._cmd_starts.pop(item_id, None)

    def observe_oversized_stdout(self, prefix: bytes) -> None:
        """Best-effort command completion tracking for a skipped stdout line."""
        if not self._cmd_starts:
            return
        text = prefix.decode(errors="replace")
        if '"item.completed"' not in text or '"command_execution"' not in text:
            return
        matched = False
        for item_id in _JSON_ID_RE.findall(text):
            if item_id in self._cmd_starts:
                self._cmd_starts.pop(item_id, None)
                matched = True
        if not matched and len(self._cmd_starts) == 1:
            self._cmd_starts.clear()

    def deadline(self, now: float, stall_secs: float, command_secs: float) -> float:
        """Effective time by which fresh activity must have occurred.

        While at least one `command_execution` is in flight, `command_secs`
        is the hard outer cap on that single command — measured from its
        own start, not from `last_line`. The stall window only applies in
        the gaps between commands.
        """
        if self._cmd_starts:
            oldest = min(self._cmd_starts.values())
            return oldest + command_secs
        return self.last_line + stall_secs


def _prepare_run_environment(spec: RunnerSpec) -> tuple[dict[str, str], str | None]:
    inherited = {key: value for key, value in os.environ.items() if not key.startswith("SYMPHONY_")}
    _scrub_ambient_agent_auth(inherited, spec.env)
    env = {**inherited, **spec.env}
    credentials = spec.credentials
    if credentials is not None and "GH_TOKEN" in spec.env:
        credentials = RunCredentials(
            github_token=spec.env["GH_TOKEN"],
            linear_token=credentials.linear_token,
        )
    if credentials is None or credentials.is_empty:
        return env, None

    cred_home = tempfile.mkdtemp(prefix="symphony-run-creds-")
    try:
        os.chmod(cred_home, 0o700)
        prior_gitconfig = Path(env["GIT_CONFIG_GLOBAL"]) if "GIT_CONFIG_GLOBAL" in env else None
        cred_env = materialize_credentials(
            credentials,
            Path(cred_home),
            prior_gitconfig=prior_gitconfig,
            github_host=spec.github_host,
        )
    except Exception:
        _remove_cred_home(cred_home)
        raise
    return {**env, **cred_env, **spec.env}, cred_home


async def _drain_runner_events(
    *,
    proc: asyncio.subprocess.Process,
    wait_task: asyncio.Task[int],
    stdout_task: asyncio.Task[None],
    stderr_task: asyncio.Task[None],
    events: asyncio.Queue[RunnerEvent],
    loop: asyncio.AbstractEventLoop,
) -> AsyncIterator[RunnerEvent]:
    drain_deadline: float | None = None
    cleaned_process_group = False
    while True:
        try:
            event = await asyncio.wait_for(events.get(), timeout=0.25)
        except TimeoutError:
            process_done = (
                wait_task.done() or proc.returncode is not None or not _pid_alive(proc.pid)
            )
            if not process_done:
                drain_deadline = None
                yield RunnerEvent(kind="tick")
                continue
            if not cleaned_process_group:
                with suppress(ProcessLookupError):
                    _terminate_process_group(proc.pid)
                cleaned_process_group = True
            if stdout_task.done() and stderr_task.done():
                return
            if drain_deadline is None:
                drain_deadline = loop.time() + _STREAM_DRAIN_SECS
            if loop.time() >= drain_deadline:
                return
            continue
        yield event


class LocalRunner:
    """Runs agent CLIs as subprocesses on this host.

    One instance is shared across the process; per-run state lives in
    `_active` keyed by `run_id` so `kill(run_id)` can find the right
    process.
    """

    def __init__(self) -> None:
        self._active: dict[str, asyncio.subprocess.Process] = {}
        self._pending_kills: set[str] = set()

    async def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        # Inherit the daemon's env minus SYMPHONY_*: those are deployment
        # flags (e.g. SYMPHONY_REQUIRE_AUTH0 in the Coolify stack), and an
        # agent working on this very repo must not have its tests/verification
        # inherit the host deployment's posture. spec.env (the per-binding
        # allowlist) still overrides.
        # Materialize DB-resolved credentials into a private, per-run home.
        env, cred_home = _prepare_run_environment(spec)
        # Control-channel mode (SYM-235): the prompt travels on stdin and stdin
        # stays open so the agent can ask questions mid-run. Every other spawn
        # site keeps /dev/null, where no control traffic is possible.
        conversation = spec.conversation
        try:
            proc = await asyncio.create_subprocess_exec(
                *spec.command,
                cwd=spec.workspace_path,
                env=env,
                start_new_session=True,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.PIPE
                if conversation is not None
                else asyncio.subprocess.DEVNULL,
                # Buffer watermark only. The pump below frames JSONL itself
                # so one codex line may exceed this without being dropped.
                limit=_SUBPROCESS_BUFFER_LIMIT,
            )
        except (OSError, FileNotFoundError) as e:
            _remove_cred_home(cred_home)
            yield RunnerEvent(kind="spawn_failed", error=f"{type(e).__name__}: {e}")
            return

        self._active[spec.run_id] = proc
        channel: ControlChannel | None = None
        if conversation is not None and proc.stdin is not None:
            channel = ControlChannel(proc.stdin, conversation, run_id=spec.run_id)
        stalled = asyncio.Event()
        wall_clock_hit = asyncio.Event()
        events: asyncio.Queue[RunnerEvent] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        hb = _Heartbeat(last_line=loop.time())
        run_started = loop.time()
        wall_clock_deadline = (
            run_started + spec.wall_clock_secs if spec.wall_clock_secs > 0 else None
        )

        async def pump(stream: asyncio.StreamReader | None, kind: str) -> None:
            if stream is None:
                return
            pending = bytearray()

            async def publish(raw_line: bytes) -> None:
                line = raw_line.decode(errors="replace")
                hb.last_line = loop.time()
                if kind == "stdout":
                    # Protocol traffic is activity (it refreshed the
                    # heartbeat above) but it is not agent output; see
                    # `control_channel` for why forwarding it is the hazard.
                    if channel is not None and channel.intercept(line):
                        return
                    hb.observe(line)
                await events.put(RunnerEvent(kind=kind, line=line))  # type: ignore[arg-type]

            async def process_chunk(chunk: bytes) -> None:
                if b"\n" not in chunk:
                    pending.extend(chunk)
                    return
                parts = chunk.split(b"\n")
                if pending:
                    pending.extend(parts[0])
                    await publish(bytes(pending))
                    pending.clear()
                else:
                    await publish(parts[0])
                for raw_line in parts[1:-1]:
                    await publish(raw_line)
                pending.extend(parts[-1])

            async def drain_oversized_line() -> tuple[bytes | None, bytes]:
                pending.clear()
                prefix = bytearray()

                def remember_prefix(data: bytes) -> None:
                    remaining = _OVERSIZED_LINE_PREFIX_BYTES - len(prefix)
                    if remaining > 0:
                        prefix.extend(data[:remaining])

                while True:
                    chunk = await stream.read(_STREAM_READ_CHUNK_BYTES)
                    if not chunk:
                        return None, bytes(prefix)
                    newline_at = chunk.find(b"\n")
                    if newline_at < 0:
                        remember_prefix(chunk)
                        continue
                    remember_prefix(chunk[:newline_at])
                    return chunk[newline_at + 1 :], bytes(prefix)

            while True:
                try:
                    chunk = await stream.read(_STREAM_READ_CHUNK_BYTES)
                except (asyncio.LimitOverrunError, ValueError) as e:
                    if not _is_stream_limit_overrun(e):
                        break
                    log.warning(
                        "skipping oversized %s line for run_id=%s after stream reader "
                        "limit overrun: %s",
                        kind,
                        spec.run_id,
                        e,
                    )
                    try:
                        remainder, skipped_prefix = await drain_oversized_line()
                    except Exception:  # noqa: BLE001 — stream is no longer recoverable
                        break
                    if skipped_prefix:
                        hb.last_line = loop.time()
                        if kind == "stdout":
                            hb.observe_oversized_stdout(skipped_prefix)
                    if remainder is None:
                        break
                    if remainder:
                        await process_chunk(remainder)
                    continue
                except Exception:  # noqa: BLE001 — pump must not crash the run
                    break
                if not chunk:
                    break
                await process_chunk(chunk)
            if pending:
                await publish(bytes(pending))

        async def watchdog() -> None:
            # Poll-based: a run is "alive" if the agent printed a line within
            # `stall_secs`, OR it has a tool call in flight that started less
            # than `command_secs` ago. The second clause is what keeps a long
            # innocent subprocess (broad rg, pnpm install) from tripping the
            # stall — the agent emits no stdout while waiting on its own tool.
            while True:
                await asyncio.sleep(_WATCHDOG_POLL_SECS)
                if proc.returncode is not None:
                    return
                now = loop.time()
                # Absolute wall-clock backstop takes precedence over the
                # heartbeat: it fires even while output is fresh or a command
                # is in flight (the heartbeat clauses can't catch a chatty but
                # wedged agent — incident SYM-148).
                wall_clock_breached = wall_clock_deadline is not None and now >= wall_clock_deadline
                if not wall_clock_breached:
                    deadline = hb.deadline(now, spec.stall_secs, spec.command_secs)
                    if now < deadline:
                        continue
                # PID-based liveness: don't trust status fields here.
                if not _pid_alive(proc.pid):
                    return
                if wall_clock_breached:
                    wall_clock_hit.set()
                else:
                    stalled.set()
                _terminate_process_group(proc.pid)
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except TimeoutError:
                    with suppress(ProcessLookupError):
                        _kill_process_group(proc.pid)
                    with suppress(Exception):
                        await proc.wait()
                return

        async def refusal_guard(channel: ControlChannel) -> None:
            # A refused request is unrecoverable for this run: the agent would
            # otherwise sit out its own retry window before dying. The channel
            # has already answered with an error and closed stdin, so give the
            # agent a moment to end its turn and report — a run that dies on
            # SIGTERM loses its final result and cost frame, and looks to the
            # state machine like a plain crash rather than a refusal.
            await channel.refused.wait()
            log.warning("ending run_id=%s: a control request was refused", spec.run_id)
            with suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(wait_task), timeout=_REFUSAL_GRACE_SECS)
            # An agent that catches SIGTERM would otherwise live to the stall
            # deadline — minutes — which is the exact wait this path avoids.
            await _stop_process_group(proc)

        stdout_task = asyncio.create_task(pump(proc.stdout, "stdout"))
        stderr_task = asyncio.create_task(pump(proc.stderr, "stderr"))
        wait_task = asyncio.create_task(proc.wait())
        refusal_task = asyncio.create_task(refusal_guard(channel)) if channel is not None else None
        watch_task = asyncio.create_task(watchdog())
        try:
            if spec.run_id in self._pending_kills:
                self._pending_kills.discard(spec.run_id)
                with suppress(ProcessLookupError):
                    _terminate_process_group(proc.pid)
            yield RunnerEvent(kind="started", pid=proc.pid)
            # The prompt goes out only now: after the pumps and the watchdog,
            # so a prompt bigger than the pipe buffer has something draining
            # stdout and a timer able to end the run while it blocks in
            # `drain()`; and inside this region, so a cancellation mid-write
            # still tears down the child, the tasks and the credential home.
            if channel is not None and conversation is not None:
                await channel.send_prompt(conversation.prompt)
            async for event in _drain_runner_events(
                proc=proc,
                wait_task=wait_task,
                stdout_task=stdout_task,
                stderr_task=stderr_task,
                events=events,
                loop=loop,
            ):
                yield event

            for task in (stdout_task, stderr_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            if not wait_task.done():
                with suppress(TimeoutError):
                    await asyncio.wait_for(wait_task, timeout=0.25)
            if not wait_task.done():
                wait_task.cancel()
                with suppress(asyncio.CancelledError):
                    await wait_task
            watch_task.cancel()
            with suppress(asyncio.CancelledError):
                await watch_task

            if wall_clock_hit.is_set():
                yield RunnerEvent(kind="wall_clock_timeout")
            elif stalled.is_set():
                yield RunnerEvent(kind="stall_timeout")
            else:
                yield RunnerEvent(kind="exit", returncode=proc.returncode)
        finally:
            # Reached on the normal path, where the tasks are already done, and
            # on every exit that skips it: a cancellation, a closed generator,
            # an exception. A caller that stops reading right after `started`
            # used to leave the child running, the run registered and the
            # credential home on disk. Each step is guarded and repeatable.
            # Stopping the child first is what lets `wait_task` finish and reap
            # it: cancelling that task with the process still up leaves a
            # zombie for as long as the daemon lives.
            await _stop_process_group(proc)
            spawned: tuple[asyncio.Task[Any] | None, ...] = (
                stdout_task,
                stderr_task,
                watch_task,
                wait_task,
                refusal_task,
            )
            for spawned_task in spawned:
                if spawned_task is not None and not spawned_task.done():
                    spawned_task.cancel()
            await asyncio.gather(*(t for t in spawned if t is not None), return_exceptions=True)
            if channel is not None:
                await channel.aclose()
            self._active.pop(spec.run_id, None)
            _remove_cred_home(cred_home)

    async def kill(self, run_id: str) -> None:
        proc = self._active.get(run_id)
        if proc is None:
            self._pending_kills.add(run_id)
            return
        await _stop_process_group(proc)


async def _stop_process_group(proc: asyncio.subprocess.Process) -> None:
    """End a run's process group and wait for the child to be reaped.

    SIGTERM, then SIGKILL if it is ignored — the escalation every caller here
    wants, in one place. Returning only once the child is reaped matters as
    much as the signals: a caller that signals and walks away leaves a zombie
    behind for as long as the daemon lives."""
    if proc.returncode is not None:
        return
    with suppress(ProcessLookupError):
        _terminate_process_group(proc.pid)
    try:
        await asyncio.wait_for(proc.wait(), timeout=_TERMINATE_GRACE_SECS)
    except TimeoutError:
        with suppress(ProcessLookupError):
            _kill_process_group(proc.pid)
        with suppress(Exception):
            await proc.wait()


def _remove_cred_home(cred_home: str | None) -> None:
    """Tear down a run's materialized credential home. Best-effort — a cleanup
    hiccup must never propagate out of a finished run."""
    if cred_home is not None:
        shutil.rmtree(cred_home, ignore_errors=True)


def _pid_alive(pid: int | None) -> bool:
    """POSIX liveness check; returns False if the PID is unknown or zombie."""
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _is_stream_limit_overrun(exc: BaseException) -> bool:
    if isinstance(exc, asyncio.LimitOverrunError):
        return True
    message = str(exc).lower()
    return "separator" in message and "limit" in message


def _terminate_process_group(pid: int | None) -> None:
    if pid is None:
        return
    os.killpg(pid, 15)


def _kill_process_group(pid: int | None) -> None:
    if pid is None:
        return
    os.killpg(pid, 9)
