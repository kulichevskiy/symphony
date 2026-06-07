"""Codex activity-event parsing and digest formatting.

The raw Codex JSONL stream still belongs in the per-run log. This module
extracts only the small command/file activity surface needed for rate-limited
Linear comments.
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

ActivityEventKind = Literal["command_started", "command_completed", "file_changed"]
ActivityPublishReason = Literal["interval", "threshold", "heartbeat", "final"]

_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY|KEY)[A-Z0-9_]*)=([^ \t\n]+)"
)
_URL_CREDENTIAL_RE = re.compile(r"://([^/\s:@]+):([^/\s@]+)@")
_AUTH_TOKEN_RE = re.compile(r"(?i)\b(bearer|token|api[_-]?key)\s+([A-Za-z0-9._~+/=-]{8,})")


@dataclass(frozen=True)
class ActivitySettings:
    enabled: bool = True
    interval_secs: int = 300
    min_interval_secs: int = 120
    event_threshold: int = 20
    long_running_secs: int = 300
    long_running_repeat_secs: int = 600
    include_failed_output_lines: int = 2


@dataclass(frozen=True)
class ActivityEvent:
    kind: ActivityEventKind
    item_id: str
    command: str = ""
    exit_code: int | None = None
    output_lines: tuple[str, ...] = ()
    file_path: str = ""
    file_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class RunningCommand:
    item_id: str
    command: str
    started_at: datetime


@dataclass(frozen=True)
class RunningCommandDigest:
    command: str
    duration_secs: int


@dataclass(frozen=True)
class FailedCommandDigest:
    command: str
    exit_code: int | None
    output_lines: tuple[str, ...]


@dataclass(frozen=True)
class ActivityDigest:
    run_id: str
    stage: str
    reason: ActivityPublishReason
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0
    running_commands: tuple[RunningCommandDigest, ...] = ()
    completed_command_count: int = 0
    completed_command_examples: tuple[str, ...] = ()
    failed_commands: tuple[FailedCommandDigest, ...] = ()
    changed_files: tuple[str, ...] = ()


@dataclass
class ActivitySession:
    """In-memory activity window for one live run.

    SQLite stores the publish marks and heartbeat marks; this object holds the
    current live command/file window so digests do not require replaying the
    full raw stream.
    """

    settings: ActivitySettings
    run_id: str
    stage: str
    workspace_path: Path
    active_commands: dict[str, RunningCommand] = field(default_factory=dict)
    pending_event_count: int = 0
    first_unpublished_at: datetime | None = None
    completed_command_count: int = 0
    completed_command_examples: list[str] = field(default_factory=list)
    failed_commands: list[FailedCommandDigest] = field(default_factory=list)
    changed_files: OrderedDict[str, None] = field(default_factory=OrderedDict)
    heartbeat_marks_loaded: bool = False
    last_heartbeat_at_by_item: dict[str, datetime] = field(default_factory=dict)

    def record_line(self, line: str, now: datetime) -> bool:
        event = parse_codex_activity_line(line, self.workspace_path)
        if event is None:
            return False
        self.record_event(event, now)
        return True

    def record_event(self, event: ActivityEvent, now: datetime) -> None:
        if self.first_unpublished_at is None:
            self.first_unpublished_at = now
        self.pending_event_count += 1

        if event.kind == "command_started":
            command = sanitize_text(
                event.command or "(command)",
                workspace_path=self.workspace_path,
            )
            self.active_commands[event.item_id] = RunningCommand(
                item_id=event.item_id,
                command=command,
                started_at=now,
            )
            return

        if event.kind == "command_completed":
            running = self.active_commands.pop(event.item_id, None)
            raw_command = event.command or (running.command if running is not None else "(command)")
            command = sanitize_text(raw_command, workspace_path=self.workspace_path)
            self.completed_command_count += 1
            if len(self.completed_command_examples) < 3:
                self.completed_command_examples.append(command)
            if event.exit_code is not None and event.exit_code != 0:
                output_lines = tuple(
                    sanitize_text(
                        line,
                        workspace_path=self.workspace_path,
                        limit=180,
                    )
                    for line in event.output_lines
                    if line
                )
                self.failed_commands.append(
                    FailedCommandDigest(
                        command=command,
                        exit_code=event.exit_code,
                        output_lines=output_lines[: self.settings.include_failed_output_lines],
                    )
                )
            return

        if event.kind == "file_changed":
            paths = event.file_paths or ((event.file_path,) if event.file_path else ())
            for path in paths:
                self.changed_files[path] = None

    def due_reason(
        self, now: datetime, *, last_posted_at: datetime | None
    ) -> ActivityPublishReason | None:
        if self.pending_event_count <= 0:
            return None
        anchor = self.first_unpublished_at or last_posted_at or now
        elapsed = max((now - anchor).total_seconds(), 0.0)
        if (
            self.pending_event_count >= self.settings.event_threshold
            and elapsed >= self.settings.min_interval_secs
        ):
            return "threshold"
        if elapsed >= self.settings.interval_secs:
            return "interval"
        return None

    def heartbeat_due_item_ids(
        self,
        now: datetime,
        *,
        last_heartbeat_at_by_item: Mapping[str, datetime] | None = None,
    ) -> tuple[str, ...]:
        marks = (
            self.last_heartbeat_at_by_item
            if last_heartbeat_at_by_item is None
            else last_heartbeat_at_by_item
        )
        due: list[str] = []
        for command in sorted(
            self.active_commands.values(),
            key=lambda c: (c.started_at, c.item_id),
        ):
            age = max((now - command.started_at).total_seconds(), 0.0)
            if age < self.settings.long_running_secs:
                continue
            last = marks.get(command.item_id)
            if last is None:
                due.append(command.item_id)
                continue
            since_last = max((now - last).total_seconds(), 0.0)
            if since_last >= self.settings.long_running_repeat_secs:
                due.append(command.item_id)
        return tuple(due)

    def has_heartbeat_candidate(self, now: datetime) -> bool:
        for command in self.active_commands.values():
            age = max((now - command.started_at).total_seconds(), 0.0)
            if age >= self.settings.long_running_secs:
                return True
        return False

    def needs_heartbeat_mark_lookup(self, now: datetime) -> bool:
        return self.has_heartbeat_candidate(now) and not self.heartbeat_marks_loaded

    def cache_heartbeat_marks(self, marks: Mapping[str, datetime]) -> None:
        self.last_heartbeat_at_by_item = dict(marks)
        self.heartbeat_marks_loaded = True

    def mark_heartbeat_posted(self, item_ids: Sequence[str], posted_at: datetime) -> None:
        self.heartbeat_marks_loaded = True
        for item_id in item_ids:
            self.last_heartbeat_at_by_item[item_id] = posted_at

    def has_unpublished_events(self) -> bool:
        return self.pending_event_count > 0

    def build_digest(
        self,
        *,
        reason: ActivityPublishReason,
        now: datetime,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cache_write_tokens: int = 0,
        cache_read_tokens: int = 0,
    ) -> ActivityDigest:
        running = sorted(
            self.active_commands.values(),
            key=lambda c: ((now - c.started_at).total_seconds(), c.item_id),
            reverse=True,
        )
        running_digest = tuple(
            RunningCommandDigest(
                command=c.command,
                duration_secs=int(max((now - c.started_at).total_seconds(), 0.0)),
            )
            for c in running[:3]
        )
        return ActivityDigest(
            run_id=self.run_id,
            stage=self.stage,
            reason=reason,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_write_tokens=cache_write_tokens,
            cache_read_tokens=cache_read_tokens,
            running_commands=running_digest,
            completed_command_count=self.completed_command_count,
            completed_command_examples=tuple(self.completed_command_examples[:3]),
            failed_commands=tuple(self.failed_commands[:3]),
            changed_files=tuple(self.changed_files.keys())[:5],
        )

    def mark_published(self) -> None:
        self.pending_event_count = 0
        self.first_unpublished_at = None
        self.completed_command_count = 0
        self.completed_command_examples.clear()
        self.failed_commands.clear()
        self.changed_files.clear()


def parse_codex_activity_line(line: str, workspace_path: Path) -> ActivityEvent | None:
    if not line:
        return None
    try:
        raw = json.loads(line)
    except (TypeError, ValueError):
        return None
    if not isinstance(raw, dict):
        return None
    event_type = raw.get("type")
    if event_type not in {"item.started", "item.completed"}:
        return None
    item = _as_mapping(raw.get("item"))
    item_type = str(item.get("type") or item.get("item_type") or raw.get("item_type") or "")
    if item_type == "command_execution":
        return _command_activity_event(
            raw=raw,
            item=item,
            event_type=str(event_type),
            workspace_path=workspace_path,
        )
    if item_type == "file_change":
        return _file_activity_event(
            raw=raw,
            item=item,
            workspace_path=workspace_path,
        )
    return None


def format_activity_digest(digest: ActivityDigest) -> str:
    title_stage = digest.stage.replace("_", " ").title()
    total_tokens = (
        digest.input_tokens
        + digest.output_tokens
        + digest.cache_write_tokens
        + digest.cache_read_tokens
    )
    lines = [
        f"📡 **Activity digest — {title_stage}**",
        "",
        f"- Run ID: `{digest.run_id}`",
        (
            f"- Tokens: in {digest.input_tokens} · out {digest.output_tokens} · "
            f"cache w {digest.cache_write_tokens} / r {digest.cache_read_tokens} · "
            f"total {total_tokens}"
        ),
    ]
    if digest.running_commands:
        running = ", ".join(
            f"`{cmd.command}` ({_format_duration(cmd.duration_secs)})"
            for cmd in digest.running_commands
        )
        lines.append(f"- Running commands: {running}")
    if digest.completed_command_count:
        examples = ", ".join(f"`{cmd}`" for cmd in digest.completed_command_examples)
        suffix = f" ({examples})" if examples else ""
        lines.append(f"- Completed commands: **{digest.completed_command_count}**{suffix}")
    if digest.failed_commands:
        lines.append("- Failed commands:")
        for failed in digest.failed_commands:
            code = failed.exit_code if failed.exit_code is not None else "unknown"
            lines.append(f"  - `{failed.command}` exited `{code}`")
            for output in failed.output_lines:
                lines.append(f"    - `{output}`")
    if digest.changed_files:
        files = ", ".join(f"`{path}`" for path in digest.changed_files)
        lines.append(f"- Changed files: {files}")
    if len(lines) == 4:
        lines.append("- Activity: no unpublished command or file events")
    return "\n".join(lines) + "\n"


def digest_fingerprint(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _command_activity_event(
    *,
    raw: Mapping[str, object],
    item: Mapping[str, object],
    event_type: str,
    workspace_path: Path,
) -> ActivityEvent | None:
    command = _extract_command(item, workspace_path)
    item_id = _item_id(raw, item, fallback=command)
    if event_type == "item.started":
        return ActivityEvent(
            kind="command_started",
            item_id=item_id,
            command=command,
        )

    exit_code = _extract_exit_code(item)
    return ActivityEvent(
        kind="command_completed",
        item_id=item_id,
        command=command,
        exit_code=exit_code,
        output_lines=_extract_output_lines(item, workspace_path),
    )


def _file_activity_event(
    *,
    raw: Mapping[str, object],
    item: Mapping[str, object],
    workspace_path: Path,
) -> ActivityEvent | None:
    paths = _extract_file_paths(item, workspace_path)
    if not paths:
        return None
    return ActivityEvent(
        kind="file_changed",
        item_id=_item_id(raw, item, fallback=paths[0]),
        file_path=paths[0],
        file_paths=paths if len(paths) > 1 else (),
    )


def _as_mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    return {}


def _item_id(raw: Mapping[str, object], item: Mapping[str, object], *, fallback: str) -> str:
    raw_id = item.get("id") or raw.get("item_id") or raw.get("id")
    if raw_id is not None and str(raw_id):
        return str(raw_id)
    digest = hashlib.sha256(fallback.encode("utf-8")).hexdigest()[:16]
    return f"item-{digest}"


def _extract_command(item: Mapping[str, object], workspace_path: Path) -> str:
    for key in ("command", "cmd", "argv", "args"):
        value = item.get(key)
        command = _command_value_to_text(value)
        if command:
            return sanitize_text(command, workspace_path=workspace_path)
    result = _as_mapping(item.get("result"))
    for key in ("command", "cmd", "argv", "args"):
        value = result.get(key)
        command = _command_value_to_text(value)
        if command:
            return sanitize_text(command, workspace_path=workspace_path)
    return "(command)"


def _command_value_to_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str | int | float | bool):
                parts.append(shlex.quote(str(item)))
        return " ".join(parts)
    return ""


def _extract_exit_code(item: Mapping[str, object]) -> int | None:
    for source in (item, _as_mapping(item.get("result"))):
        for key in ("exit_code", "returncode", "return_code", "code"):
            value = source.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                return value
            if isinstance(value, str):
                try:
                    return int(value)
                except ValueError:
                    continue
        status = str(source.get("status") or "").lower()
        if status in {"failed", "failure", "error"}:
            return 1
        if status in {"succeeded", "success", "completed"}:
            return 0
    return None


def _extract_output_lines(item: Mapping[str, object], workspace_path: Path) -> tuple[str, ...]:
    lines: list[str] = []
    for source in (item, _as_mapping(item.get("result"))):
        for key in ("stderr", "stdout", "output", "aggregated_output"):
            lines.extend(_output_value_to_lines(source.get(key), workspace_path))
            if len(lines) >= 5:
                return tuple(lines[:5])
    return tuple(lines)


def _output_value_to_lines(value: object, workspace_path: Path) -> list[str]:
    raw_lines: list[str] = []
    if isinstance(value, str):
        raw_lines = value.splitlines()
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        raw_lines = [str(item) for item in value if item is not None]
    out: list[str] = []
    for line in raw_lines:
        cleaned = sanitize_text(line.strip(), workspace_path=workspace_path, limit=180)
        if cleaned:
            out.append(cleaned)
    return out


def _extract_file_paths(item: Mapping[str, object], workspace_path: Path) -> tuple[str, ...]:
    paths: OrderedDict[str, None] = OrderedDict()

    def add(raw_path: object) -> None:
        if not isinstance(raw_path, str):
            return
        normalized = normalize_workspace_path(raw_path, workspace_path)
        if normalized is not None:
            paths[normalized] = None

    for key in ("path", "file_path", "file"):
        add(item.get(key))
    for key in ("files", "paths"):
        value = item.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
            for entry in value:
                add(entry)
    changes = item.get("changes")
    if isinstance(changes, Sequence) and not isinstance(changes, (bytes, bytearray, str)):
        for change in changes:
            if not isinstance(change, Mapping):
                continue
            add(change.get("path"))
    return tuple(paths.keys())


def normalize_workspace_path(raw_path: str, workspace_path: Path) -> str | None:
    if not raw_path:
        return None
    path = Path(raw_path)
    if path.is_absolute():
        workspace = workspace_path.resolve(strict=False)
        try:
            rel = path.resolve(strict=False).relative_to(workspace)
        except ValueError:
            return None
    else:
        rel = path
    parts = rel.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        return None
    return rel.as_posix()


def sanitize_text(
    text: str,
    *,
    workspace_path: Path | None = None,
    limit: int = 160,
) -> str:
    out = text.replace("\x00", " ")
    if workspace_path is not None:
        candidates = {str(workspace_path), str(workspace_path.resolve(strict=False))}
        for candidate in sorted(candidates, key=len, reverse=True):
            if candidate:
                out = out.replace(candidate, ".")
    out = _SECRET_ASSIGNMENT_RE.sub(r"\1=[redacted]", out)
    out = _URL_CREDENTIAL_RE.sub("://[redacted]@", out)
    out = _AUTH_TOKEN_RE.sub(r"\1 [redacted]", out)
    out = " ".join(out.split())
    if len(out) <= limit:
        return out
    if limit <= 1:
        return out[:limit]
    return out[: limit - 1].rstrip() + "…"


def _format_duration(seconds: int) -> str:
    seconds = max(seconds, 0)
    minutes, secs = divmod(seconds, 60)
    hours, mins = divmod(minutes, 60)
    if hours:
        return f"{hours}h {mins}m"
    if mins:
        return f"{mins}m {secs}s"
    return f"{secs}s"
