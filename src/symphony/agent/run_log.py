"""Run-log appends plus per-line Symphony receipt times.

The raw agent JSONL in ``{log_root}/{run_id}.log`` is a contract: final-message
extraction, usage accounting and error classification all re-read it verbatim.
So receipt times live *beside* it, in ``{run_id}.log.ts`` — one
``<end_offset> <iso8601>`` line per appended log line, keyed by the log byte
offset just past that line (the same boundary the live stream resumes from).

Logs written before the sidecar existed simply have none; lookups then return
``None`` and the UI renders no time rather than inventing one.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import IO

RECEIPTS_SUFFIX = ".ts"


def _wall_clock() -> datetime:
    return datetime.now(UTC)


def receipts_path(log_path: Path) -> Path:
    """Sidecar path for a run log (``run-1.log`` → ``run-1.log.ts``)."""
    return log_path.with_name(log_path.name + RECEIPTS_SUFFIX)


class RunLogWriter:
    """Append lines to a run log, recording each line's receipt time.

    Both files are flushed per line so a live tail sees output (and its time)
    while the run is still in progress.
    """

    def __init__(self, log_path: Path, now: Callable[[], datetime] | None = None) -> None:
        self._log_path = log_path
        # Wall clock by default, and that is what callers want: a receipt time
        # is the real instant Symphony saw the line, so the orchestrator does
        # *not* hand over its (test-scriptable) clock here. `now` exists so
        # this module's own tests can pin the stamp.
        self._now = now if now is not None else _wall_clock
        # Tracked rather than read back via `tell()`, which is an opaque
        # cookie for text-mode handles.
        self._offset = log_path.stat().st_size if log_path.exists() else 0
        self._log: IO[str] | None = None
        self._receipts: IO[str] | None = None

    def open(self) -> RunLogWriter:
        """Open both files for append. Also the `with`-statement entry point."""
        self._log = self._log_path.open("a", encoding="utf-8")
        self._receipts = receipts_path(self._log_path).open("a", encoding="utf-8")
        return self

    def close(self) -> None:
        for handle in (self._log, self._receipts):
            if handle is not None:
                handle.close()
        self._log = None
        self._receipts = None

    def __enter__(self) -> RunLogWriter:
        return self.open()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def write(self, line: str) -> None:
        """Append one line (no trailing newline) and stamp its receipt time."""
        if self._log is None or self._receipts is None:
            raise RuntimeError("RunLogWriter used outside its context")
        data = line + "\n"
        self._log.write(data)
        self._log.flush()
        self._offset += len(data.encode("utf-8"))
        self._receipts.write(f"{self._offset} {self._now().isoformat()}\n")
        self._receipts.flush()


class ReceiptTimes:
    """Reader for a run log's receipt-time sidecar, keyed by line end offset.

    Re-reads the sidecar's tail when a lookup misses and the file has grown,
    so a live tail resolves times for lines appended after the first read.
    """

    def __init__(self, log_path: Path) -> None:
        self._path = receipts_path(log_path)
        self._times: dict[int, str] = {}
        self._pos = 0
        self._buffer = b""
        self._loaded = False

    def get(self, end_offset: int) -> str | None:
        if not self._loaded:
            self._refresh()
        if end_offset not in self._times:
            self._refresh()
        return self._times.get(end_offset)

    def _refresh(self) -> None:
        try:
            size = self._path.stat().st_size
        except OSError:
            return
        if self._loaded and size <= self._pos:
            return
        self._loaded = True
        try:
            with self._path.open("rb") as handle:
                handle.seek(self._pos)
                self._buffer += handle.read()
                self._pos = handle.tell()
        except OSError:
            return
        while b"\n" in self._buffer:
            raw, self._buffer = self._buffer.split(b"\n", 1)
            offset, _, ts = raw.decode(errors="replace").partition(" ")
            if not ts.strip():
                continue
            try:
                self._times[int(offset)] = ts.strip()
            except ValueError:
                continue


__all__ = ["ReceiptTimes", "RECEIPTS_SUFFIX", "RunLogWriter", "receipts_path"]
