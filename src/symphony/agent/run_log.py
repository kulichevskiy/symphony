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

import logging
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import IO

log = logging.getLogger(__name__)

RECEIPTS_SUFFIX = ".ts"


def _wall_clock() -> datetime:
    return datetime.now(UTC)


def receipts_path(log_path: Path) -> Path:
    """Sidecar path for a run log (``run-1.log`` → ``run-1.log.ts``)."""
    return log_path.with_name(log_path.name + RECEIPTS_SUFFIX)


class RunLogWriter:
    """Append lines to a run log, recording each line's receipt time.

    Both files are flushed per line so a live tail sees output (and its time)
    while the run is still in progress. The log is the contract other
    consumers replay; the receipts sidecar is best-effort, so a sidecar that
    can't be opened or fails on a later write (its directory disallows new
    files, the process is out of file descriptors, a transient filesystem
    error, ...) does not stop the log itself from being written — the run
    just gets no receipt times for that sidecar's lines from that point on.
    """

    def __init__(self, log_path: Path) -> None:
        self._log_path = log_path
        # Wall clock: a receipt time is the real instant Symphony saw the
        # line, so the orchestrator does not hand over its (test-scriptable)
        # clock here.
        self._now = _wall_clock
        # Tracked rather than read back via `tell()`, which is an opaque
        # cookie for text-mode handles.
        self._offset = log_path.stat().st_size if log_path.exists() else 0
        self._log: IO[str] | None = None
        self._receipts: IO[str] | None = None

    def open(self) -> RunLogWriter:
        """Open both files for append. Also the `with`-statement entry point.

        A failure opening the receipts sidecar is logged and swallowed
        rather than raised: the log itself is still appendable, and losing
        receipt times for one run is far cheaper than losing the run log.
        """
        self._log = self._log_path.open("a", encoding="utf-8")
        try:
            self._receipts = receipts_path(self._log_path).open("a", encoding="utf-8")
        except OSError:
            log.warning(
                "receipts sidecar unavailable for %s; run log continues without receipt times",
                self._log_path,
                exc_info=True,
            )
            self._receipts = None
        return self

    def close(self) -> None:
        """Close both handles. A receipts-close failure is logged and
        swallowed, not raised: the sidecar is best-effort, so a delayed
        writeback failure on it (e.g. a network filesystem) must not stop
        the run log's own handle from closing or escape to the caller.
        """
        if self._log is not None:
            self._log.close()
        if self._receipts is not None:
            try:
                self._receipts.close()
            except OSError:
                log.warning(
                    "receipts sidecar close failed for %s",
                    self._log_path,
                    exc_info=True,
                )
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
        """Append one line (no trailing newline) and stamp its receipt time.

        The receipts line is written and flushed *before* the log line, not
        after: a reader tails the log, not the sidecar, so the moment the log
        line becomes visible is the moment its receipt time must already be
        on disk. A sidecar entry for a not-yet-visible log line is harmless;
        a visible log line with no receipt entry yet is a permanent `ts:
        null`, since the stream emits each event once and never re-asks.

        If the sidecar failed to open (see `open`), `self._receipts` is
        `None` and this simply skips the receipt line — the log write below
        still happens. A sidecar that opened fine but fails on a later write
        or flush (transient filesystem error, descriptor limit, ...) is
        handled the same way: the failure is logged and the sidecar is
        disabled for the rest of the run rather than raised, since losing
        receipt times must never stop the run log itself from being written.
        """
        if self._log is None:
            raise RuntimeError("RunLogWriter used outside its context")
        data = line + "\n"
        offset = self._offset + len(data.encode("utf-8"))
        if self._receipts is not None:
            try:
                self._receipts.write(f"{offset} {self._now().isoformat()}\n")
                self._receipts.flush()
            except OSError:
                log.warning(
                    "receipts sidecar write failed for %s; disabling receipt times for this run",
                    self._log_path,
                    exc_info=True,
                )
                self._receipts = None
        self._log.write(data)
        self._log.flush()
        self._offset = offset


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

    def snapshot(self) -> dict[int, str]:
        """Refresh once and return the full offset -> receipt-time mapping.

        For a one-shot scan of an already-fully-read log (the history page),
        every line's receipt entry is already on disk by the time the line
        itself is readable (see `RunLogWriter.write`), so a single refresh
        here covers every lookup — unlike `get`, which re-stats per miss to
        keep up with a sidecar still growing under a live tail.
        """
        self._refresh()
        return self._times

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
